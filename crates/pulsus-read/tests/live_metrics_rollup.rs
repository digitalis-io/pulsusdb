//! Issue #18 part B: reading the rollup levels, against a live ClickHouse.
//!
//! Each case builds or leases a database with `run_init`, every level view
//! stopped right after (part A's driving runs). Samples land through
//! `metric_landing`; the first level is built by the first-level view's own
//! statement with its `plan` scalar replaced by a literal interval, an hour
//! a statement; every later level by its own view's statement run as an
//! insert, a day at most a statement, until one writes no run row. Cases
//! that only read a corpus share one database per corpus. Two engines read
//! it over one label cache: one with the rollup levels (routing on) and one
//! without (routing off).
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
#[path = "query_log_wait/mod.rs"]
mod query_log_wait;

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
use query_log_wait::settle_query_log;

const MIN: i64 = 60_000;
const HOUR: i64 = 3_600_000;
const DAY: i64 = 86_400_000;
const SCRAPE: i64 = 15_000;

static DB_B3: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_b3");
static DB_B4: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_b4");
static DB_B5: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_b5");
static DB_B5B: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_b5b");
static DB_B1F: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_b1f");
static DB_B7: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_b7");
static DB_B10: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_b10");
static DB_S2: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_s2");
static DB_B2E: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_b2e");
static DB_T10: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_t10");
static DB_H1: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_h1");
static DB_L1: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_l1");
static DB_L7: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_l7");
static DB_P5: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_p5");
static DB_P22: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_p22");
static DB_P64: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_p64");
static DB_P31: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_p31");
static DB_P33: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_p33");

/// The default levels and their switch points: 60 s first, the resolution
/// from 3 h; a shorter range reads raw (owner, 2026-10-31).
const DEFAULTS: (&[u32], &[u32]) = (
    pulsus_schema::DEFAULT_ROLLUP_LEVELS,
    pulsus_schema::DEFAULT_ROLLUP_RANGES,
);

/// The five levels most of these cases were written for, 5 s first, with
/// the switch points that make each the resolution from 5 s, 1 h, 24 h, 7
/// days and 90 days.
const FIVE: (&[u32], &[u32]) = (
    &[5, 60, 720, 7200, 86400],
    &[5, 3600, 86400, 604800, 7776000],
);

/// Part B's levels, 60 s first, with the switch points that make each the
/// resolution from 60 s, 24 h, 7 days and 90 days.
const PART_B: (&[u32], &[u32]) = (&[60, 720, 7200, 86400], &[60, 86400, 604800, 7776000]);

/// The shortest query range that selects the level `secs` of [`FIVE`].
fn band(secs: i64) -> i64 {
    match secs {
        5 => 5_000,
        60 => HOUR,
        720 => DAY,
        7200 => 7 * DAY,
        _ => 90 * DAY,
    }
}

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

/// Runs level `r`'s view until a run writes no run row. A level after the
/// first catches up 10 of its buckets a run, so the 60 s level built from
/// the 5 s level takes 144 runs a day: 15 days is 2,160.
async fn until_idle(client: &ChClient, db: &str, r: u32) {
    let view = format!("{db}.metric_samples_{r}s_mv");
    let mut before = run_count(client, db, r).await;
    for _ in 0..4000 {
        for verb in ["START", "REFRESH", "WAIT", "STOP"] {
            exec(client, &format!("SYSTEM {verb} VIEW {view}")).await;
        }
        let now = run_count(client, db, r).await;
        if now == before {
            return;
        }
        before = now;
    }
    panic!("level {r}s still writes run rows after 4000 runs");
}

/// Every level after the first, in order, each by its view until idle.
async fn build_later_by_view(client: &ChClient, ctx: &RenderCtx) {
    for r in &ctx.rollup_levels[1..] {
        until_idle(client, &ctx.db, *r).await;
    }
}

/// Level `r`'s view statement (a level after the first) as an insert: the
/// view's own plan, watermark and hole rule, a run capped at the end of the
/// UTC day its first bucket ends in instead of 10 buckets on.
fn later_level_insert(ctx: &RenderCtx, r: u32) -> String {
    let head = format!("CREATE MATERIALIZED VIEW {}.metric_samples_{r}s_mv", ctx.db);
    let view = pulsus_schema::rendered_statements(ctx)
        .into_iter()
        .find(|s| s.starts_with(&head))
        .expect("the level's view");
    let body = &view[view.find("AS WITH ").expect("AS WITH") + 3..];
    let cap = format!(", b + 9 * {r}000)");
    assert_eq!(
        body.matches(&cap).count(),
        1,
        "the 10-bucket cap, once:\n{body}"
    );
    format!(
        "INSERT INTO {}.metric_samples_{r}s\n{}",
        ctx.db,
        body.replacen(&cap, ", (intDiv(b - 1, 86400000) + 1) * 86400000)", 1)
    )
}

/// Every level after the first, in order: its statement until one writes no run row.
async fn build_later_by_statement(client: &ChClient, ctx: &RenderCtx) {
    for &r in &ctx.rollup_levels[1..] {
        let sql = later_level_insert(ctx, r);
        let mut before = run_count(client, &ctx.db, r).await;
        let mut idle = false;
        for _ in 0..1000 {
            exec(client, &sql).await;
            let now = run_count(client, &ctx.db, r).await;
            if now == before {
                idle = true;
                break;
            }
            before = now;
        }
        assert!(
            idle,
            "level {r}s still writes run rows after 1000 statements"
        );
    }
}

/// Removes level `r`'s coverage of `(a, b]`: the run rows overlapping it
/// are deleted and their parts outside it written back. Data rows stay.
async fn cut_runs(client: &ChClient, db: &str, r: u32, a: i64, b: i64) {
    // The builder writes run rows only on bucket ends, so a cut anywhere
    // else leaves coverage that no level row stands for.
    assert!(
        a.rem_euclid(i64::from(r) * 1000) == 0 && b.rem_euclid(i64::from(r) * 1000) == 0,
        "a cut ends on bucket ends of the {r} s level: ({a}, {b}]"
    );
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
    /// The shared corpus this database belongs to, if leased.
    lease: Option<&'static Shared>,
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

    /// Issue #579 part 4: the engine with the levels and the pushed nodes.
    async fn pushed_engine(&self, comment: Option<&str>) -> MetricsEngine {
        let mut e = MetricsEngine::new(
            self.client().await,
            Arc::clone(&self.cache),
            engine_config(self.db(), true),
        )
        .with_rollup_levels(Arc::clone(&self.slot));
        if let Some(c) = comment {
            e = e.with_query_log_comment(c);
        }
        e
    }

    async fn finish(self) {
        let Some(shared) = self.lease else {
            drop_database(&self.bootstrap, &self.ctx.db).await;
            return;
        };
        let mut state = shared.state.lock().await;
        let writes = writes_since(&self.admin, &self.ctx.db, state.since_us).await;
        assert!(
            writes.is_empty(),
            "a member of the shared corpus {} wrote to it:\n{}",
            self.ctx.db,
            writes.join("\n")
        );
        state.released += 1;
        if state.released == shared.members.len() {
            drop_database(&self.bootstrap, &self.ctx.db).await;
            *state = SharedState::EMPTY;
        }
    }
}

/// Creates `db` with `levels`, runs `land` in it and builds the first level
/// over `(lo, hi]` one hour at a time: the bootstrap and admin clients and
/// the database's context.
async fn first_only(
    db: &str,
    (levels, ranges): (&[u32], &[u32]),
    land: &[String],
    lo: i64,
    hi: i64,
) -> (ChClient, ChClient, RenderCtx) {
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let ctx = RenderCtx {
        rollup_levels: levels.to_vec(),
        rollup_ranges: ranges.to_vec(),
        ..RenderCtx::for_tests(db, 30)
    };
    fresh(&bootstrap, &ctx).await;
    let admin = ChClient::new(test_config(db)).await.expect("connect");
    for sql in land {
        exec(&admin, sql).await;
    }
    build_first(&admin, &ctx, lo, hi, HOUR).await;
    (bootstrap, admin, ctx)
}

/// Creates `db` with `levels`, runs `land` in it, builds the first level
/// over `(lo, hi]` one hour at a time and every later level until idle,
/// and warms one label cache for `tenants` (the empty tenant always).
async fn build(
    db: &str,
    levels: (&[u32], &[u32]),
    land: &[String],
    lo: i64,
    hi: i64,
    tenants: &[Tenant],
) -> Built {
    let (bootstrap, admin, ctx) = first_only(db, levels, land, lo, hi).await;
    build_later_by_statement(&admin, &ctx).await;
    warm(bootstrap, admin, ctx, tenants).await
}

/// The label cache and the level slot over a database already built.
async fn warm(bootstrap: ChClient, admin: ChClient, ctx: RenderCtx, tenants: &[Tenant]) -> Built {
    let mut cfg = cache_config(&ctx.db);
    // Wide enough for S2's oldest 86400 s window, 13 days back.
    cfg.window_ms = 16 * DAY;
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
        lease: None,
    }
}

/// Corpus D's landing statements over days `days` after `day0`.
fn corpus_d_land(db: &str, day0: i64, days: std::ops::Range<i64>) -> Vec<String> {
    let mut land = vec![corpus_d_series(db)];
    land.push(corpus_d_registrations(
        db,
        day0 + days.start * DAY,
        (days.end - days.start) * 24,
    ));
    for d in days {
        land.push(corpus_d_day(db, day0, d));
    }
    land
}

/// Corpus D over days `days` after `day0`, built at `levels`.
async fn corpus_d(
    db: &str,
    levels: (&[u32], &[u32]),
    day0: i64,
    days: std::ops::Range<i64>,
) -> Built {
    let land = corpus_d_land(db, day0, days.clone());
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

// ------------------------------------------------------ shared corpora

/// A corpus only its `members` read: built once per test process by the
/// first member to lease it, dropped when every member has finished.
struct Shared {
    db: pulsus_testkit::TestDb,
    corpus: Corpus,
    members: &'static [&'static str],
    state: tokio::sync::Mutex<SharedState>,
}

struct SharedState {
    /// The database's context and its anchor.
    built: Option<(RenderCtx, i64)>,
    released: usize,
    /// The server's clock, in µs, when the build finished.
    since_us: i64,
}

impl SharedState {
    const EMPTY: Self = Self {
        built: None,
        released: 0,
        since_us: 0,
    };
}

enum Corpus {
    /// `corpus_d` at these levels over these days, `day0 = today() − 8 d`.
    D((&'static [u32], &'static [u32]), std::ops::Range<i64>),
    /// `corpus_p(db, p_end())`.
    P,
    /// `corpus_p2(db, p_end())`.
    P2,
}

impl Corpus {
    /// The corpus built into `db`, and its anchor.
    async fn build(&self, db: &str) -> (Built, i64) {
        match self {
            Corpus::D(levels, days) => {
                let day0 = today() - 8 * DAY;
                (corpus_d(db, *levels, day0, days.clone()).await, day0)
            }
            Corpus::P => {
                let end = p_end();
                (corpus_p(db, end).await, end)
            }
            Corpus::P2 => {
                let end = p_end();
                (corpus_p2(db, end).await, end)
            }
        }
    }
}

impl Shared {
    const fn new(
        db: pulsus_testkit::TestDb,
        corpus: Corpus,
        members: &'static [&'static str],
    ) -> Self {
        Self {
            db,
            corpus,
            members,
            state: tokio::sync::Mutex::const_new(SharedState::EMPTY),
        }
    }

    /// The corpus for member `test`, built by the first member to ask, and
    /// its anchor. Under the process-per-test harness each member builds a
    /// private database instead.
    async fn lease(&'static self, test: &str) -> (Built, i64) {
        let k = self
            .members
            .iter()
            .position(|m| *m == test)
            .unwrap_or_else(|| panic!("{test} is not a member of {}", self.db.name()));
        if std::env::var("NEXTEST_EXECUTION_MODE").as_deref() == Ok("process-per-test") {
            let db = format!("{}_{k}", self.db.name());
            println!("{test}: private database {db}");
            return self.corpus.build(&db).await;
        }
        let (ctx, anchor) = {
            let mut state = self.state.lock().await;
            if state.built.is_none() {
                let (built, anchor) = self.corpus.build(self.db.name()).await;
                state.since_us = server_now_us(&built.admin).await;
                state.released = 0;
                state.built = Some((built.ctx.clone(), anchor));
            }
            state.built.clone().expect("built above")
        };
        let bootstrap = ChClient::new(test_config("default"))
            .await
            .expect("connect (bootstrap)");
        let admin = ChClient::new(test_config(&ctx.db)).await.expect("connect");
        let mut built = warm(bootstrap, admin, ctx, &[]).await;
        built.lease = Some(self);
        (built, anchor)
    }
}

static D5_8: Shared = Shared::new(
    pulsus_testkit::TestDb::new("pulsus_read_it_rollup_d5_8"),
    Corpus::D(FIVE, 0..8),
    &[
        "each_served_function_is_within_its_distance",
        "a_week_reads_two_hour_rows",
        "a_raw_only_query_keeps_its_grid",
        "a_pushed_aggregate_beside_a_routed_selector",
    ],
);
static D60_8: Shared = Shared::new(
    pulsus_testkit::TestDb::new("pulsus_read_it_rollup_d60_8"),
    Corpus::D(DEFAULTS, 0..8),
    &[
        "a_week_and_three_hours_fill_from_their_levels",
        "a_rounded_range_is_within_its_distance",
    ],
);
static D60_2: Shared = Shared::new(
    pulsus_testkit::TestDb::new("pulsus_read_it_rollup_d60_2"),
    Corpus::D(DEFAULTS, 6..8),
    &[
        "a_range_below_the_first_level_reads_raw",
        "the_lattice_reads_no_finer_level_where_its_own_covers",
        "queries_off_the_lattice_keep_the_request_grid",
        "a_query_that_reads_the_step_time_keeps_its_steps",
    ],
);
static P: Shared = Shared::new(
    pulsus_testkit::TestDb::new("pulsus_read_it_rollup_p"),
    Corpus::P,
    &[
        "pushdown_on_the_request_grid_and_when_filled",
        "a_level_rate_aggregate_answers_as_in_process",
        "the_panel_sends_groups_not_level_rows",
        "a_vector_match_under_an_aggregate_answers_as_in_process",
        "unpushed_shapes_take_todays_route",
        "a_split_level_node_reads_its_prefix",
        "a_filled_request_pushes_both_reads",
    ],
);
static P2: Shared = Shared::new(
    pulsus_testkit::TestDb::new("pulsus_read_it_rollup_p2"),
    Corpus::P2,
    &[
        "a_coarse_level_aggregate_answers_as_in_process",
        "a_week_reads_the_two_hour_level_pushed",
        "a_pushed_node_reads_no_more_than_in_process",
        "a_side_the_cache_cannot_answer_keeps_its_series_statement",
        "the_lag_guard_reaches_every_statement",
        "a_statement_at_the_cap_declines_and_one_below_pushes",
    ],
);

/// The five groups, by their statics' names.
fn groups() -> [(&'static str, &'static Shared); 5] {
    [
        ("D5_8", &D5_8),
        ("D60_8", &D60_8),
        ("D60_2", &D60_2),
        ("P", &P),
        ("P2", &P2),
    ]
}

/// The server's clock, in µs.
async fn server_now_us(client: &ChClient) -> i64 {
    ints(client, "SELECT toUnixTimestamp64Micro(now64(6)) AS v").await[0]
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct LoggedWrite {
    kind: String,
    query_kind: String,
    query: String,
}

/// Every statement other than a `Select` that touched `db` after
/// `since_us` on the server's clock, each of its query-log rows, start
/// rows included, once the log has every one of them that has started.
async fn writes_since(admin: &ChClient, db: &str, since_us: i64) -> Vec<String> {
    let cutoff_us = server_now_us(admin).await;
    let filter = format!(
        "has(databases, '{db}') AND query_kind != 'Select' \
         AND client_name != 'refreshable materialized view' \
         AND toUnixTimestamp64Micro(query_start_time_microseconds) > {since_us} \
         AND toUnixTimestamp64Micro(query_start_time_microseconds) <= {cutoff_us}"
    );
    settle_query_log(&[admin], &filter, 0).await;
    let sql = format!(
        "SELECT DISTINCT toString(type) AS kind, toString(query_kind) AS query_kind, query \
         FROM system.query_log \
         WHERE {filter} AND query_kind != 'Select' AND query NOT LIKE '%system.query_log%'"
    );
    let mut stream = admin
        .query_stream::<LoggedWrite>(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\nSQL:\n{sql}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        let row = row.expect("decode");
        out.push(format!("{} {}: {}", row.kind, row.query_kind, row.query));
    }
    out
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

    /// Issue #579 part 4: the statements of the pushed nodes, without the
    /// declines that share their stage name.
    fn pushed(&self) -> Vec<&str> {
        self.explain
            .stages
            .iter()
            .filter(|s| s.name == "pushed_aggregate" && !s.sql.starts_with("declined:"))
            .map(|s| s.sql.as_str())
            .collect()
    }

    /// Issue #579 part 4: every `declined: …` the pushed nodes report.
    fn declines(&self) -> Vec<&str> {
        self.explain
            .stages
            .iter()
            .filter(|s| s.name == "pushed_aggregate" && s.sql.starts_with("declined:"))
            .map(|s| s.sql.as_str())
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

/// Plan section 9's `d` of one series and step: 0 when both are NaN, 1
/// when only one is (plan r9 section 5).
fn d_of(a: Option<&f64>, b: Option<&f64>) -> Option<f64> {
    match (a, b) {
        (Some(x), Some(y)) if x.is_nan() && y.is_nan() => Some(0.0),
        (Some(x), Some(y)) if x.is_nan() || y.is_nan() => Some(1.0),
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

/// `read_rows` per `log_comment`, for the comments `prefix` starts, once
/// every statement under them has finished in the log.
async fn read_rows(admin: &ChClient, prefix: &str) -> BTreeMap<String, u64> {
    settle_query_log(&[admin], &format!("startsWith(log_comment, '{prefix}')"), 1).await;
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

/// The bound of plan section 9 for `function` on `kind` at the level of
/// `secs` (5 s, 60 s, 720 s, 7200 s, 86400 s); `None` for bit-equal. At
/// 5 s, on corpus E, a bucket holds one sample: 1e-12 for every function
/// that is not bit-equal.
fn bound(function: &str, kind: &str, secs: i64) -> Option<f64> {
    if secs == 5 {
        return match (function, kind) {
            ("rate" | "increase" | "sum_over_time", _) | ("avg_over_time", "gauge") => Some(1e-12),
            _ => None,
        };
    }
    let li = match secs {
        60 => 0,
        720 => 1,
        7200 => 2,
        _ => 3,
    };
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
/// own query-log comment, both on the request's grid.
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

/// One level's column of B2: per function and range, the query over
/// `(start, end]` with routing on and off; the points at or after
/// `from` are compared.
struct Column<'a> {
    built: &'a Built,
    secs: i64,
    start: i64,
    end: i64,
    from: i64,
    run: &'a str,
}

/// B2's query pairs in flight at once: half the 16 cores of the database
/// machine the suite runs on, each pair two statements.
const B2_PAIRS: usize = 8;

/// Runs one B2 column, pushing its failures, its report lines and its
/// query-log comments.
async fn b2_column(
    c: Column<'_>,
    failures: &mut Vec<String>,
    report: &mut Vec<String>,
    comments: &mut Vec<(String, i64, String, String)>,
) {
    let secs = c.secs;
    let p = range(c.start, c.end, secs * 1000);
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
    }
    let kept = |x: BTreeMap<(String, i64), f64>| -> BTreeMap<(String, i64), f64> {
        x.into_iter().filter(|((_, t), _)| *t >= c.from).collect()
    };
    let mut answers = futures::stream::iter(queries.iter().enumerate())
        .map(|(qi, (_, _, query))| {
            let on_c = format!("{}_{secs}_{qi}_on", c.run);
            let off_c = format!("{}_{secs}_{qi}_off", c.run);
            let p = &p;
            async move {
                let ab = both(c.built, query, p, Some((&on_c, &off_c))).await;
                (on_c, off_c, ab)
            }
        })
        .buffered(B2_PAIRS);
    let mut each = queries.iter();
    while let Some((on_c, off_c, (a, b))) = answers.next().await {
        let (function, kind, query) = each.next().expect("one answer a query");
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
        let (pa, pb) = (kept(a.points()), kept(b.points()));
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
        match bound(function, kind, secs) {
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

/// Corpus E (issue #18 follow-up): corpus D's formulas at a 5 s scrape,
/// over the 2 hours ending at `end`, the phase `100 + (7919 s) mod 4800`
/// and `c_stop` stopping 1 h 30 min 7.5 s after the start.
fn corpus_e(db: &str, end: i64) -> Vec<String> {
    let start = end - 2 * HOUR;
    let stop = start + HOUR + 30 * MIN + 7_500;
    vec![
        corpus_d_series(db),
        corpus_d_registrations(db, start, 2),
        format!(
            "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
             SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 0, metric, fp, t, v FROM ( \
               SELECT s, metric, fp, toInt64(n.number) AS i, \
                      toInt64({start}) + 5000 * i + (100 + (7919 * s) % 4800) + (toInt64((7 * s + 13 * i) % 201) - 100) AS t, \
                      {CORPUS_D_VALUE} AS v \
               FROM {db}.rd_series, numbers({}) AS n) \
             WHERE NOT (metric = 'c_stop' AND t > {stop}) \
               AND NOT (metric = 'c_sparse' AND (3 * s + 7 * i) % 10 < 3)",
            2 * HOUR / 5_000
        ),
    ]
}

/// **B2 (issue #18 part B; follow-up): each served function is within its
/// distance from raw.** Corpus D at every level; per level `res`, a query
/// range inside that level's band: `rate` and `increase` over `5 × res` on
/// each counter metric, `avg_over_time` on the gauges, every other served
/// function on every metric; each sent with routing on and off, compared
/// at the steps from `day0 + 5 res`. The 5 s column runs on corpus E,
/// over 30 minutes. The routed explain names the level and reads
/// `exactness` `"rollup"`; each function is within the bound of plan
/// section 9 (bit-equal where it says so).
#[tokio::test(flavor = "multi_thread")]
async fn each_served_function_is_within_its_distance() {
    skip_unless_live!();
    let (built, day0) = D5_8
        .lease("each_served_function_is_within_its_distance")
        .await;
    let end = day0 + 8 * DAY;
    let levels = built.slot.get().expect("the levels are read");
    assert_eq!(levels.levels.len(), 5, "{levels:?}");
    let e_land = corpus_e(DB_B2E.name(), end);
    let built_e = build(DB_B2E.name(), FIVE, &e_land, end - 2 * HOUR, end, &[]).await;

    let mut failures = Vec::new();
    let mut report = Vec::new();
    let mut comments = Vec::new();
    // Each run's statements carry their own comment, so the query log's
    // rows of an earlier run are not summed into this one's.
    let run = format!("b2_{}_{}", built.db(), now_ms());
    b2_column(
        Column {
            built: &built_e,
            secs: 5,
            start: end - 30 * MIN,
            end,
            from: end - 30 * MIN,
            run: &run,
        },
        &mut failures,
        &mut report,
        &mut comments,
    )
    .await;
    for secs in [60i64, 720, 7200, 86400] {
        let span = match secs {
            60 => DAY - HOUR,
            720 => 3 * DAY,
            7200 => 8 * DAY,
            _ => band(secs),
        };
        b2_column(
            Column {
                built: &built,
                secs,
                start: end - span,
                end,
                from: day0 + 5 * secs * 1000,
                run: &run,
            },
            &mut failures,
            &mut report,
            &mut comments,
        )
        .await;
    }
    let mut rows = read_rows(&built.admin, &format!("{run}_")).await;
    rows.extend(read_rows(&built_e.admin, &format!("{run}_")).await);
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
    built_e.finish().await;
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
/// its level.** Corpus D's last two days; the 720 s level's coverage cut after
/// `B`, the 60 s and 5 s levels' after `B + 30 m`, `B` a 720 s boundary
/// three hours before the data's end. A 24-hour `last_over_time` (the
/// 720 s level's band) at step 12 m lists the 720 s piece up to `B`, the
/// 60 s piece `(B, B + 30 m]`, then raw to the end, in that order, and its
/// last step equals the raw route's.
#[tokio::test(flavor = "multi_thread")]
async fn a_window_reads_the_finer_levels_and_raw_after_its_level() {
    skip_unless_live!();
    let day0 = today() - 8 * DAY;
    let end = day0 + 8 * DAY;
    let built = corpus_d(DB_B3.name(), FIVE, day0, 6..8).await;
    let b = end - 3 * HOUR;
    // Each level is cut from its last bucket end at or before `b`.
    for (r, from) in [(86_400, end - DAY), (7_200, end - 4 * HOUR), (720, b)] {
        cut_runs(&built.admin, built.db(), r, from, end + DAY).await;
    }
    for r in [60, 5] {
        cut_runs(&built.admin, built.db(), r, b + 30 * MIN, end + DAY).await;
    }

    let p = range(end - DAY, end, 12 * MIN);
    let query = "last_over_time(c_normal[12m])";
    let a = ask(&built.engine(true, None).await, &no_tenant(), query, &p).await;
    let raw = ask(&built.engine(false, None).await, &no_tenant(), query, &p).await;
    // A level whose coverage the cuts removed serves nothing; its stage
    // names no interval.
    let pieces: Vec<(String, i64)> = a
        .stages("rollup_piece")
        .iter()
        .filter(|n| !n.ends_with(" served nothing"))
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
/// last two days at every level, with `c_stale`, `c_mixed`, `h` and `c_iv`
/// beside it; `W = day0 + 7 d 18 h`. (i) to (ii) and (v), over ranges in
/// the 60 s level's band, with the 60 s and 5 s levels' coverage cut after
/// `W`; (iii) and (iv), over 24 hours (the 720 s level's band), with the
/// 720 s level's cut after `W` too, against the same step read before
/// that cut.
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
        corpus_d_registrations(db, day - DAY, 48),
        corpus_d_day(db, day0, 6),
        corpus_d_day(db, day0, 7),
    ];
    land.extend(b4_land(db, day0, w, &floats, &hists));
    let built = build(db, FIVE, &land, day - DAY, end, &[]).await;
    for r in [60, 5] {
        cut_runs(&built.admin, built.db(), r, w, end + DAY).await;
    }

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
            let limit = bound(f, kind, 60).expect("a counter kind");
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

            // From `W + 1 m`: the grid starts at `start`, and the window
            // at `W` itself lies wholly in the level's coverage.
            let (a, b) = both(&built, &query, &range(w + MIN, w + HOUR + MIN, MIN), None).await;
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
    let p720 = range(w + 2 * HOUR - DAY, w + 2 * HOUR, 12 * MIN);
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
        let limit = bound("rate", kind, 720).expect("a counter kind");
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
    let (a, b) = both(
        &built,
        query,
        &range(w + HOUR - DAY, w + HOUR, 12 * MIN),
        None,
    )
    .await;
    assert!(
        a.stages("rollup_piece")
            .iter()
            .any(|n| n.starts_with("raw (")),
        "{:?}",
        a.stages("rollup_piece")
    );
    // The steps from `W − 1 h`, part B's range: an earlier window holds
    // `c_iv`'s first sample, which part B never compared.
    let kept = |x: BTreeMap<(String, i64), f64>| -> BTreeMap<(String, i64), f64> {
        x.into_iter().filter(|((_, t), _)| *t >= w - HOUR).collect()
    };
    let (pa, pb) = (kept(a.points()), kept(b.points()));
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
/// first level's coverage of one hour removed before the later levels are
/// built, so no level covers it. `sum_over_time` over 1 h, over 22 hours
/// (the 60 s level's band), lists a raw piece over that hour and equals
/// the raw route's within 1e-12.
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
    cut_runs(&admin, db, ctx.rollup_levels[0], hole, hole + HOUR).await;
    build_later_by_statement(&admin, &ctx).await;
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
/// Corpus D's last two days; the 720 s level's coverage of one hour in the
/// middle of a 24-hour window (the 720 s level's band) removed, the window
/// ending before the stopping counters stop. `rate` over 1 h at step 12 m
/// reads the 720 s level, the 60 s level for the hour, the 720 s level
/// again, and stays within the 720 s bounds.
#[tokio::test(flavor = "multi_thread")]
async fn a_hole_in_the_middle_keeps_the_counter() {
    skip_unless_live!();
    let day0 = today() - 8 * DAY;
    let end = day0 + 8 * DAY;
    let built = corpus_d(DB_B5B.name(), FIVE, day0, 6..8).await;
    let hole = end - 14 * HOUR;
    cut_runs(&built.admin, built.db(), 720, hole, hole + HOUR).await;

    let p = range(end - 32 * HOUR, end - 8 * HOUR, 12 * MIN);
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
        let limit = bound("rate", kind, 720).expect("a counter kind");
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
/// and one under `histogram_count`: no `rollup_route` stage and bit-equal
/// to routing off. A selector the cache cannot answer (`SqlFallback`) is
/// routed and answers as the warm cache's route does (issue #18
/// follow-up). A metric with one float and one native-histogram series
/// reads its float series from a level (`exactness` `"rollup"`) and its
/// histogram series bit-equal to routing off; a histogram-only metric is
/// routed but `"raw-exact"`.
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
    let built = build(db, FIVE, &land, lo, end, &[]).await;
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
    let warm = ask(&routed, &no_tenant(), "rate(rollup_f[1h])", &p).await;
    let b = ask(&cold_raw, &no_tenant(), "rate(rollup_f[1h])", &p).await;
    assert!(!a.stages("rollup_route").is_empty(), "SqlFallback: routed");
    assert!(!canonical(&b.result).is_empty(), "SqlFallback: an answer");
    assert_eq!(
        canonical(&a.result),
        canonical(&warm.result),
        "SqlFallback: the warm cache's route"
    );

    // The mixed metric: the float series from the level, the histogram
    // series raw.
    let a = ask(&routed, &no_tenant(), "rate(rollup_mixed[1h])", &p).await;
    let b = ask(&raw, &no_tenant(), "rate(rollup_mixed[1h])", &p).await;
    assert!(a.explain.rollup, "the float series is served from a level");
    assert!(
        a.stages("rollup_piece")
            .iter()
            .any(|n| n.starts_with("metric_samples_60s.last, inc served")),
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
/// built with the levels 60, 300, 7200 and 86400 and the ranges 60, 3600,
/// 86400 and 7776000: `max_over_time` over 5 m on a 6-hour range reads
/// `metric_samples_300s`.
#[tokio::test(flavor = "multi_thread")]
async fn a_level_added_later_is_used() {
    skip_unless_live!();
    let day0 = today() - 8 * DAY;
    let end = day0 + 8 * DAY;
    let built = corpus_d(
        DB_B7.name(),
        (&[60, 300, 7200, 86400], &[60, 3600, 86400, 7776000]),
        day0,
        7..8,
    )
    .await;
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
/// steps.** Part B's levels, 60 s first; the first level built by 180
/// separate one-minute runs; `increase` over 5 m, over 2 h 50 min (the
/// 60 s level's band), is within the sparse 60 s bound, per tenant.
#[tokio::test(flavor = "multi_thread")]
async fn a_sparse_counter_keeps_its_steps() {
    skip_unless_live!();
    let db = DB_B10.name();
    let t0 = (now_ms() / HOUR) * HOUR - 4 * HOUR;
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect");
    let ctx = RenderCtx {
        rollup_levels: PART_B.0.to_vec(),
        rollup_ranges: PART_B.1.to_vec(),
        ..RenderCtx::for_tests(db, 30)
    };
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

/// The stale marker's bits.
const STALE_BITS: u64 = 9_218_868_437_227_405_314;
/// Two ordinary NaNs with different payloads.
const NAN_A: u64 = 9_221_120_237_041_090_561;
const NAN_B: u64 = 9_221_120_237_041_090_562;

/// S2's samples for one level `res`: `(metric, t, value bits)`, each
/// series labelled with the level. Bucket `B = (b − res, b]`, `b = E −
/// 3 res`, holds S1's shapes, the stale marker in its last 5 minutes;
/// every gauge resumes two buckets later. `s2_c` is a counter rising by
/// 10 a scrape from `b − 11 res`, with a stale marker in `B` and its next
/// sample 15 s after it, 30 higher: S1's `c1` shape.
fn s2_series(e: i64, res: i64) -> Vec<(&'static str, i64, u64)> {
    let b = e - 3 * res;
    let st = b - (res / 3).min(150_000);
    let resume = b + 2 * res - res / 2;
    let mut out = vec![
        ("s2_g1", st - 15_000, 5f64.to_bits()),
        ("s2_g1", st, STALE_BITS),
        ("s2_g2", st, STALE_BITS),
        ("s2_g3", st - 15_000, NAN_A),
        ("s2_g3", st, NAN_B),
        ("s2_g4", st - 30_000, 3f64.to_bits()),
        ("s2_g4", st - 15_000, f64::NAN.to_bits()),
        ("s2_g4", st, 7f64.to_bits()),
    ];
    for g in ["s2_g1", "s2_g2", "s2_g3", "s2_g4"] {
        out.push((g, resume, 9f64.to_bits()));
    }
    let mut t = b - 11 * res + 7_000;
    let mut v = 0.0f64;
    while t < st {
        out.push(("s2_c", t, v.to_bits()));
        v += 10.0;
        t += SCRAPE;
    }
    out.push(("s2_c", st, STALE_BITS));
    let mut v = v - 10.0 + 30.0;
    let mut t = st + SCRAPE;
    while t <= e {
        out.push(("s2_c", t, v.to_bits()));
        v += 10.0;
        t += SCRAPE;
    }
    out
}

/// **S2 (issue #18 part B, plan r9 to r11): stale markers and NaN values
/// read as raw.** Per level, S1's shapes in one of its buckets: the
/// functions that are bit-equal stay bit-equal, NaN bits and presence
/// included; `sum_` and `avg_over_time` are NaN where raw is; a counter
/// resuming after a stale marker stays within the normal-counter bound;
/// `c4`'s step of more than a day reads each route's exact value. Each
/// level is asked over the shortest range of its band, ending at `E`, and
/// `c4` over the same range ending at `D`; the earlier times read nothing
/// on either route (issue #18 follow-up).
#[tokio::test(flavor = "multi_thread")]
async fn stale_and_nan_read_as_raw() {
    skip_unless_live!();
    let db = DB_S2.name();
    let e = today();
    let d = e - DAY;
    let levels = [60i64, 720, 7200, 86400];
    let mut land = vec![format!(
        "CREATE TABLE {db}.rd_s2 ENGINE = Memory AS \
         SELECT metric, lvl, concat('{{\"lvl\":\"', lvl, '\"}}') AS labels, \
                concat(metric, unhex('FF'), 'lvl', unhex('FF'), lvl, unhex('FF')) AS buf, \
                bitOr(bitShiftLeft(toUInt128(bitShiftRight(cityHash64(metric), 32)), 96), \
                      bitAnd(bitOr(bitShiftLeft(toUInt128(cityHash64(buf)), 64), toUInt128(xxHash64(buf))), \
                             toUInt128('79228162514264337593543950335'))) AS fp \
         FROM (SELECT arrayJoin(['s2_g1', 's2_g2', 's2_g3', 's2_g4', 's2_c']) AS metric) \
         ARRAY JOIN ['60', '720', '7200', '86400', 'c4'] AS lvl \
         WHERE NOT (lvl = 'c4' AND metric != 's2_c')"
    )];
    // Registrations: every hour of the 15 days.
    land.push(format!(
        "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) \
         SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 2, metric, fp, \
                toInt64({}) + toInt64(n.number) * {HOUR} + 1000, labels, 0 \
         FROM {db}.rd_s2, numbers({}) AS n",
        e - 15 * DAY,
        15 * 24
    ));
    let mut samples: Vec<(String, String, i64, u64)> = Vec::new();
    for res in levels {
        for (m, t, v) in s2_series(e, res * 1000) {
            samples.push((m.to_string(), (res).to_string(), t, v));
        }
    }
    // `c4`: 100 at `D − 86,430,000`, a stale marker at `D − 15 s`, 130 at `D`.
    for (t, v) in [
        (d - 86_430_000, 100f64.to_bits()),
        (d - 15_000, STALE_BITS),
        (d, 130f64.to_bits()),
    ] {
        samples.push(("s2_c".to_string(), "c4".to_string(), t, v));
    }
    for chunk in samples.chunks(2_000) {
        let values: Vec<String> = chunk
            .iter()
            .map(|(m, l, t, v)| format!("('{m}', '{l}', {t}, {v})"))
            .collect();
        land.push(format!(
            "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
             SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 0, r.metric, r.fp, x.t, reinterpretAsFloat64(x.v) \
             FROM (SELECT * FROM values('m String, l String, t Int64, v UInt64', {})) AS x \
             JOIN {db}.rd_s2 AS r ON r.metric = x.m AND r.lvl = x.l",
            values.join(", ")
        ));
    }
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect");
    // The switch points this case was written for: the 60 s level from
    // 1 h, so `band(60)` reads it (issue #18 moved the default to 3 h).
    let ctx = RenderCtx {
        rollup_ranges: vec![3600, 86400, 604800, 7776000],
        ..RenderCtx::for_tests(db, 30)
    };
    fresh(&bootstrap, &ctx).await;
    let admin = ChClient::new(test_config(db)).await.expect("connect");
    for sql in &land {
        exec(&admin, sql).await;
    }
    // The first level by one run over the whole corpus.
    build_first(&admin, &ctx, e - 15 * DAY, e, 15 * DAY).await;
    build_later_by_statement(&admin, &ctx).await;
    let built = warm(bootstrap, admin, ctx, &[]).await;

    let mut failures = Vec::new();
    for secs in levels {
        let res = secs * 1000;
        let b = e - 3 * res;
        let p = range(e - band(secs), e, res);
        let sel = |m: &str| format!("{m}{{lvl=\"{secs}\"}}");
        for g in ["s2_g1", "s2_g2", "s2_g3", "s2_g4"] {
            let mut bit_equal_queries = vec![sel(g), format!("timestamp({})", sel(g))];
            for f in [
                "last_over_time",
                "present_over_time",
                "absent_over_time",
                "max_over_time",
                "min_over_time",
            ] {
                for r in [secs, 5 * secs] {
                    bit_equal_queries.push(format!("{f}({}[{r}s])", sel(g)));
                }
            }
            for q in &bit_equal_queries {
                let (a, raw) = both(&built, q, &p, None).await;
                if a.stages("rollup_route").is_empty() {
                    failures.push(format!("{q} at {secs}s: not routed"));
                }
                let (ca, cb) = (vector_or_matrix(&a), vector_or_matrix(&raw));
                if ca != cb {
                    failures.push(format!("{q} at {secs}s: routed {ca:?}, raw {cb:?}"));
                }
            }
            for f in ["sum_over_time", "avg_over_time"] {
                for r in [secs, 5 * secs] {
                    let q = format!("{f}({}[{r}s])", sel(g));
                    let (a, raw) = both(&built, &q, &p, None).await;
                    let (pa, pb) = (a.points(), raw.points());
                    let limit = if f == "sum_over_time" {
                        1e-12
                    } else {
                        bound("avg_over_time", "gauge", secs).expect("avg bound")
                    };
                    let keys: std::collections::BTreeSet<_> = pa.keys().chain(pb.keys()).collect();
                    for k in keys {
                        let dk = d_of(pa.get(k), pb.get(k)).unwrap_or(0.0);
                        if dk > limit {
                            failures.push(format!(
                                "{q} at {secs}s {k:?}: routed {:?}, raw {:?}",
                                pa.get(k),
                                pb.get(k)
                            ));
                        }
                    }
                }
            }
        }
        let q = format!("increase({}[{}s])", sel("s2_c"), 5 * secs);
        let (a, raw) = both(&built, &q, &p, None).await;
        // The steps from `b − 5 res`, part B's range: an earlier window
        // holds the counter's first sample, which part B never compared.
        let from = b - 5 * res;
        let kept = |x: BTreeMap<(String, i64), f64>| -> BTreeMap<(String, i64), f64> {
            x.into_iter().filter(|((_, t), _)| *t >= from).collect()
        };
        let (pa, pb) = (kept(a.points()), kept(raw.points()));
        let dist = distances(&pa, &pb);
        assert!(!dist.is_empty(), "{q}: no points on either route");
        let (p99, max) = p99_max(&dist);
        let limit = bound("increase", "normal", secs).expect("a counter bound");
        println!(
            "S2 {q}: n {} p99 {p99:e} max {max:e} bound {limit:e}",
            dist.len()
        );
        let keys: std::collections::BTreeSet<_> = pa.keys().chain(pb.keys()).collect();
        for k in keys {
            if d_of(pa.get(k), pb.get(k)).is_some_and(|x| x > limit) {
                println!(
                    "S2 {q} step b{:+}s: routed {:?}, raw {:?}",
                    (k.1 - b) / 1000,
                    pa.get(k),
                    pb.get(k)
                );
            }
        }
        if p99 > limit {
            failures.push(format!("{q}: p99 {p99:e} above {limit:e}"));
        }
        if secs == 60 {
            // The step whose window holds the first sample after the
            // marker counts the 30 step, as raw does.
            let st = b - (res / 3).min(150_000);
            let t = ((st + SCRAPE - 1).div_euclid(res) + 1) * res;
            let at = pa.iter().find(|((_, s), _)| *s == t).map(|(_, v)| *v);
            let raw_at = pb.iter().find(|((_, s), _)| *s == t).map(|(_, v)| *v);
            println!("S2 60 s step holding the resumed sample: routed {at:?}, raw {raw_at:?}");
            let close = match (at, raw_at) {
                (Some(x), Some(y)) => (x - y).abs() / y.abs() <= limit && x >= 30.0,
                _ => false,
            };
            if !close {
                failures.push(format!(
                    "{q}: at the resumed step routed {at:?}, raw {raw_at:?}"
                ));
            }
        }
        // `c4`, at `D`, the last time of a range in the level's band.
        let q = format!("increase(s2_c{{lvl=\"c4\"}}[{}s])", 5 * secs);
        let (a, raw) = both(&built, &q, &range(d - band(secs), d, res), None).await;
        let value = |x: &Answer| {
            x.points()
                .into_iter()
                .find(|((_, t), _)| *t == d)
                .map(|(_, v)| v.to_bits())
        };
        let want: (Option<u64>, Option<u64>) = match secs {
            60 => (None, None),
            720 | 7200 => (Some(0f64.to_bits()), None),
            _ => (Some(0f64.to_bits()), Some(45f64.to_bits())),
        };
        let got = (value(&a), value(&raw));
        println!(
            "S2 c4 at {secs}s: routed {:?}, raw {:?}",
            got.0.map(f64::from_bits),
            got.1.map(f64::from_bits)
        );
        if got != want {
            failures.push(format!(
                "{q}: routed {:?}, raw {:?}, want {:?}",
                got.0.map(f64::from_bits),
                got.1.map(f64::from_bits),
                (want.0.map(f64::from_bits), want.1.map(f64::from_bits))
            ));
        }
    }
    // The size of the flags column, plan r9 section 6.
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
    struct ColRow {
        table: String,
        name: String,
        compressed: u64,
        uncompressed: u64,
    }
    for r in [60, 720, 7200, 86400] {
        exec(
            &built.admin,
            &format!(
                "CREATE TABLE {db}.size_{r} ENGINE = MergeTree ORDER BY (org_id, fingerprint, t_end) \
                 SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0 \
                 AS SELECT * FROM {db}.metric_samples_{r}s"
            ),
        )
        .await;
    }
    let mut stream = built
        .admin
        .query_stream::<ColRow>(
            &format!(
                "SELECT table, column AS name, toUInt64(sum(column_data_compressed_bytes)) AS compressed, \
                        toUInt64(sum(column_data_uncompressed_bytes)) AS uncompressed \
                 FROM system.parts_columns WHERE database = '{db}' AND active \
                 AND match(table, '^size_[0-9]+$') AND column IN ('flags', 'inc', 'last') \
                 GROUP BY table, column ORDER BY table, column"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("columns");
    while let Some(row) = stream.next().await {
        let row = row.expect("decode");
        println!(
            "S2 column {}.{}: {} compressed, {} uncompressed bytes",
            row.table, row.name, row.compressed, row.uncompressed
        );
    }
    drop(stream);
    built.finish().await;
    assert!(failures.is_empty(), "S2 failures:\n{}", failures.join("\n"));
}

/// An answer's cells, a vector's samples at time 0, floats as their bits.
fn vector_or_matrix(x: &Answer) -> BTreeMap<(String, i64), String> {
    match &x.result {
        QueryResult::Vector(v) => v
            .iter()
            .map(|s| {
                let mut l = s.labels.clone();
                l.sort();
                ((format!("{l:?}"), 0), format!("{:016x}", s.value.to_bits()))
            })
            .collect(),
        _ => x.cells(),
    }
}

/// **T4 (issue #18 follow-up, plan r16): a week reads two-hour rows.**
/// Corpus D at every level; a label cache whose window is 24 hours, so a
/// 7-day selector resolves by SQL. `end` 2 hours before the corpus's end
/// and `start` 7 days before it, both on 2 h boundaries, asked at step
/// 2 h: `max_over_time(g[2h])` and `rate(c_normal[10h])` have 85 points
/// per series at `start + k × 2 h`; explain resolves the series by SQL,
/// reads their labels by the `rollup_series` statement and routes to
/// `metric_samples_7200s` at step 2 h. `max_over_time` is bit-equal and
/// `rate` within its level's bound against the unrouted engine asked at
/// step 2 h from the same `start`. With both ends 5 minutes later, the
/// same 85 points read `metric_samples_7200s` at its bucket ends: the
/// first 84 take the unrouted engine at the bucket end that holds each,
/// the 85th the unrouted engine at `end`.
#[tokio::test(flavor = "multi_thread")]
async fn a_week_reads_two_hour_rows() {
    skip_unless_live!();
    let (built, day0) = D5_8.lease("a_week_reads_two_hour_rows").await;
    let db = built.db().to_string();
    let mut cfg = cache_config(&db);
    cfg.window_ms = DAY;
    let cache = Arc::new(LabelCache::new(built.client().await, cfg));
    cache.refresh().await.expect("refresh");
    assert!(cache.is_warm());
    let run = format!("t4_{db}_{}", now_ms());
    let engine = |routed: bool, comment: String| {
        let cache = Arc::clone(&cache);
        let slot = Arc::clone(&built.slot);
        let db = db.clone();
        async move {
            let mut e = MetricsEngine::new(
                ChClient::new(test_config(&db)).await.expect("connect"),
                cache,
                engine_config(&db, false),
            )
            .with_query_log_comment(&comment);
            if routed {
                e = e.with_rollup_levels(slot);
            }
            e
        }
    };

    let res = 2 * HOUR;
    // Both ends on 2 h boundaries, then both 5 minutes later: the times are
    // `start + k × 2 h` either way; the second reads the 60 s level (G2).
    for (shift, table, level) in [
        (0, "metric_samples_7200s", 7200),
        (5 * MIN, "metric_samples_7200s", 7200),
    ] {
        let end = day0 + 8 * DAY - 2 * HOUR + shift;
        let start = end - 7 * DAY;
        let p = range(start, end, res);
        let times: Vec<i64> = (0..85).map(|k| start + k * res).collect();
        for (qi, (query, kind)) in [
            ("max_over_time(g[2h])", None),
            ("rate(c_normal[10h])", Some("normal")),
        ]
        .into_iter()
        .enumerate()
        {
            let a = ask(
                &engine(true, format!("{run}_{shift}_{qi}_on")).await,
                &no_tenant(),
                query,
                &p,
            )
            .await;
            let series = per_series(&a);
            assert!(!series.is_empty(), "{query} +{shift}: no series");
            for (l, ts) in &series {
                assert_eq!(ts, &times, "{query} +{shift} {l}: start + k × 2 h");
            }
            let stages = &a.explain.stages;
            let resolved = stages
                .iter()
                .find(|s| s.name == "series_resolution")
                .expect("series_resolution");
            assert!(
                resolved.sql.starts_with("SELECT"),
                "{query}: the series resolve by SQL: {resolved:?}"
            );
            let metric = if kind.is_some() { "c_normal" } else { "g" };
            let hydrate = pulsus_read::metrics::sql::series_labels_by_fingerprint(
                &no_tenant(),
                &engine_config(&db, false).labels_table,
                &[metric.to_string()],
                &resolved.sql,
            );
            let hydrated = stages
                .iter()
                .find(|s| s.name == "rollup_series")
                .unwrap_or_else(|| panic!("{query}: no rollup_series stage: {stages:?}"));
            assert_eq!(
                hydrated.sql, hydrate,
                "{query}: the rollup_series statement"
            );
            let route = a.stages("rollup_route");
            assert!(
                route
                    .iter()
                    .any(|n| n.starts_with(&format!("{table},")) && n.contains(", step 2h,")),
                "{query} +{shift}: {route:?}"
            );

            let off = engine(false, format!("{run}_{shift}_{qi}_off")).await;
            let pa = a.points();
            let pb = if shift == 0 {
                ask(&off, &no_tenant(), query, &range(start, end, res))
                    .await
                    .points()
            } else {
                let ends = ask(
                    &off,
                    &no_tenant(),
                    query,
                    &range(start - shift + res, end - shift, res),
                )
                .await;
                let mut want = spread(&ends.points(), &times[..84], res, 1.0);
                want.extend(
                    ask(&off, &no_tenant(), query, &range(end, end, res))
                        .await
                        .points(),
                );
                want
            };
            match kind {
                None => assert!(bit_equal(&pa, &pb), "{query} +{shift}: bit-equal"),
                Some(kind) => {
                    let d = distances(&pa, &pb);
                    let (p99, max) = p99_max(&d);
                    let limit = bound("rate", kind, level).expect("a counter kind");
                    println!("T4 {query} +{shift}: n {} p99 {p99:e} max {max:e}", d.len());
                    assert!(
                        p99 <= limit,
                        "{query} +{shift}: p99 {p99:e} above {limit:e}"
                    );
                }
            }
        }
    }
    for (comment, rows) in read_rows(&built.admin, &format!("{run}_")).await {
        println!("T4 read_rows {comment}: {rows}");
    }
    built.finish().await;
}

/// The answer's points per series label set.
fn per_series(x: &Answer) -> BTreeMap<String, Vec<i64>> {
    let mut out: BTreeMap<String, Vec<i64>> = BTreeMap::new();
    for (l, t) in x.points().keys() {
        out.entry(l.clone()).or_default().push(*t);
    }
    out
}

/// **T9 (issue #18 follow-up, r11): a raw-only query keeps its grid.**
/// Corpus D at every level; `irate(c_normal[5m])` and `deriv(g[10m])`
/// over 7 days at step 5 m from `day0 + 10 min + 7.123 s`, an engine with
/// the levels against the same engine without them: bit-equal, 2,017
/// points for each of the 100 series.
#[tokio::test(flavor = "multi_thread")]
async fn a_raw_only_query_keeps_its_grid() {
    skip_unless_live!();
    let (built, day0) = D5_8.lease("a_raw_only_query_keeps_its_grid").await;
    assert!(built.slot.get().is_some_and(|l| l.levels.len() == 5));
    let start = day0 + 10 * MIN + 7_123;
    let p = range(start, start + 7 * DAY, 5 * MIN);
    for query in ["irate(c_normal[5m])", "deriv(g[10m])"] {
        let a = ask(&built.engine(true, None).await, &no_tenant(), query, &p).await;
        let b = ask(&built.engine(false, None).await, &no_tenant(), query, &p).await;
        assert!(a.stages("rollup_route").is_empty(), "{query}: no route");
        let series = per_series(&a);
        assert_eq!(series.len(), 100, "{query}: the series");
        for (l, ts) in &series {
            assert_eq!(ts.len(), 2_017, "{query} {l}: the points");
            assert_eq!(ts[0], start, "{query} {l}: the request's first time");
        }
        assert_eq!(a.cells(), b.cells(), "{query}: bit-equal to no levels");
    }
    built.finish().await;
}

/// **T10 (issue #18 follow-up, plan r16): a grid from `start` routes
/// where its times fit.** Corpus D at every level, against the engine
/// without levels. `rate(c_normal[5m] offset 1s)` over 1 h at step 5 m
/// from `day0 + 7 d + 1 s`: `S` = 300 s = the step, so the request's own
/// 13 times; each time less 1 s is a minute boundary, so it reads
/// `metric_samples_60s`, within the 60 s bound. The instant
/// `rate(c_normal[5m])` at `day0 + 7 d` reads the 5 s level. The range
/// query with `start = end` is shorter than the 5 s level's switch point,
/// so it keeps its own one-point grid and reads raw (issue #746).
#[tokio::test(flavor = "multi_thread")]
async fn a_grid_from_start_routes_where_the_times_fit() {
    skip_unless_live!();
    let day0 = today() - 8 * DAY;
    let built = corpus_d(DB_T10.name(), FIVE, day0, 6..8).await;
    let t = day0 + 7 * DAY;
    let routed = built.engine(true, None).await;
    let raw = built.engine(false, None).await;
    // Every value within `limit` of the engine without levels, by the
    // 99th percentile, with every value over it printed.
    let close = |what: &str, a: &Answer, b: &Answer, limit: f64| {
        let (pa, pb) = (a.points(), b.points());
        let d = distances(&pa, &pb);
        let (p99, max) = p99_max(&d);
        let over: Vec<String> = pa
            .iter()
            .filter_map(|(k, x)| {
                let y = pb.get(k)?;
                (d_of(Some(x), Some(y))? > limit).then(|| format!("{} {}: {x} vs {y}", k.0, k.1))
            })
            .collect();
        println!(
            "T10 {what}: n {} p99 {p99:e} max {max:e}; over {limit:e}: {over:?}",
            d.len()
        );
        assert!(p99 <= limit, "{what}: p99 {p99:e} above {limit:e}");
    };

    let query = "rate(c_normal[5m] offset 1s)";
    let p = range(t + 1_000, t + 1_000 + HOUR, 5 * MIN);
    let a = ask(&routed, &no_tenant(), query, &p).await;
    let b = ask(&raw, &no_tenant(), query, &p).await;
    assert!(
        a.stages("rollup_route")
            .iter()
            .any(|n| n.starts_with("metric_samples_60s,")),
        "range: {:?}",
        a.stages("rollup_route")
    );
    let series = per_series(&a);
    assert_eq!(series.len(), 100, "range: the series");
    let want: Vec<i64> = (0..13).map(|k| p.start_ms + k * 300_000).collect();
    for (l, ts) in &series {
        assert_eq!(ts, &want, "range {l}: the request's times");
    }
    close(
        "range",
        &a,
        &b,
        bound("rate", "normal", 60).expect("a counter bound"),
    );

    let instant = range(t, t, 0);
    let a = ask(&routed, &no_tenant(), "rate(c_normal[5m])", &instant).await;
    let b = ask(&raw, &no_tenant(), "rate(c_normal[5m])", &instant).await;
    assert!(
        a.stages("rollup_route")
            .iter()
            .any(|n| n.starts_with("metric_samples_5s,")),
        "instant: {:?}",
        a.stages("rollup_route")
    );
    assert_eq!(a.points().len(), 100, "instant: the series");
    close("instant", &a, &b, 1e-12);

    let equal = range(t + 1_000, t + 1_000, 5 * MIN);
    let a = ask(&routed, &no_tenant(), query, &equal).await;
    let b = ask(&raw, &no_tenant(), query, &equal).await;
    assert!(
        a.stages("rollup_route").is_empty(),
        "equal bounds: raw {:?}",
        a.stages("rollup_route")
    );
    let series = per_series(&a);
    assert_eq!(series.len(), 100, "equal bounds: the series");
    for (l, ts) in &series {
        assert_eq!(ts, &vec![t + 1_000], "equal bounds {l}: one point at start");
    }
    assert_eq!(a.cells(), b.cells(), "equal bounds: bit-equal to no levels");
    built.finish().await;
}

/// **R1 (issue #746): a range below the first level's switch point reads
/// raw.** Corpus D over two days at the default levels, 60 s first from
/// 3 h. `rate(c_normal[5m])` at step 60 s from a minute boundary: over
/// 15 minutes, and over 3 h less 1 ms, the query keeps its own grid, no
/// selector routes, and every value is bit-equal to the engine without
/// levels; over exactly 3 h the resolution is 60 s and the selector reads
/// `metric_samples_60s`.
#[tokio::test(flavor = "multi_thread")]
async fn a_range_below_the_first_level_reads_raw() {
    skip_unless_live!();
    let (built, day0) = D60_2.lease("a_range_below_the_first_level_reads_raw").await;
    let found = built.slot.get().expect("the levels");
    let first = &found.levels[0];
    assert_eq!(
        (first.res_ms, first.from_ms, found.levels.len()),
        (60_000, 10_800_000, 4),
        "the first default level and its switch point: {found:?}"
    );
    let t = day0 + 7 * DAY;
    let routed = built.engine(true, None).await;
    let raw = built.engine(false, None).await;
    let query = "rate(c_normal[5m])";
    for span in [15 * MIN, 3 * HOUR - 1] {
        let p = range(t, t + span, MIN);
        let a = ask(&routed, &no_tenant(), query, &p).await;
        let b = ask(&raw, &no_tenant(), query, &p).await;
        assert!(
            a.stages("rollup_route").is_empty(),
            "{span} ms: raw {:?}",
            a.stages("rollup_route")
        );
        assert_eq!(per_series(&a).len(), 100, "{span} ms: the series");
        assert_eq!(a.cells(), b.cells(), "{span} ms: bit-equal to no levels");
    }
    let p = range(t, t + 3 * HOUR, MIN);
    let a = ask(&routed, &no_tenant(), query, &p).await;
    assert!(
        a.stages("rollup_route")
            .iter()
            .any(|n| n.starts_with("metric_samples_60s,")),
        "3 h: reads the first level {:?}",
        a.stages("rollup_route")
    );
    built.finish().await;
}

/// **T14 (issue #18 follow-up, plan r16): a pushed aggregate beside a
/// routed selector.** Corpus D at every level.
/// `count(g) + scalar(max_over_time(g{s="160"}[1h] @ end()))` over 7 days
/// less 1 ms at step 5 m: the resolution is 720 s; the `@ end()` selector
/// routes, `count(g)` cannot (`start` is 1 ms off every boundary) and is
/// pushed. `end` is the corpus's end, so the first point's lookback holds
/// samples. 2,016 points per series at `start + k × 5 m`, explain holding
/// the routed selector's `rollup_route` and the pushed aggregate's
/// statement, every value bit-equal to the engine without levels on the
/// same request.
#[tokio::test(flavor = "multi_thread")]
async fn a_pushed_aggregate_beside_a_routed_selector() {
    skip_unless_live!();
    let (built, day0) = D5_8
        .lease("a_pushed_aggregate_beside_a_routed_selector")
        .await;
    let end = day0 + 8 * DAY;
    let start = end - 7 * DAY + 1;
    let query = r#"count(g) + scalar(max_over_time(g{s="160"}[1h] @ end()))"#;
    let mut e = MetricsEngine::new(
        built.client().await,
        Arc::clone(&built.cache),
        engine_config(built.db(), true),
    );
    e = e.with_rollup_levels(Arc::clone(&built.slot));
    let raw = MetricsEngine::new(
        built.client().await,
        Arc::clone(&built.cache),
        engine_config(built.db(), true),
    );
    let a = ask(&e, &no_tenant(), query, &range(start, end, 5 * MIN)).await;
    let series = per_series(&a);
    assert_eq!(series.len(), 1, "one series: {series:?}");
    let times: Vec<i64> = (0..2016).map(|k| start + k * 300_000).collect();
    for (l, ts) in &series {
        assert_eq!(ts, &times, "{l}: start + k × 5 m");
    }
    assert!(
        a.stages("rollup_route")
            .iter()
            .any(|n| n.starts_with("metric_samples_720s,")),
        "the @ end() selector routes: {:?}",
        a.stages("rollup_route")
    );
    // The pushed aggregate's statement, on the request's grid.
    let pushed: Vec<&pulsus_read::logql::ExplainStage> = a
        .explain
        .stages
        .iter()
        .filter(|s| {
            s.name == "grouped_fetch"
                && s.sql
                    .contains(&format!("{start} AS grid_start, 300000 AS grid_step"))
        })
        .collect();
    assert!(
        !pushed.is_empty(),
        "count(g) is pushed: {:?}",
        a.explain.stages
    );
    let b = ask(&raw, &no_tenant(), query, &range(start, end, 5 * MIN)).await;
    assert!(
        b.stages("rollup_route").is_empty(),
        "the engine without levels routes nothing"
    );
    assert_eq!(a.cells(), b.cells(), "bit-equal to no levels");
    built.finish().await;
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct StatementRow {
    query: String,
    rows: u64,
}

/// Every finished statement sent under `comment`, with its `result_rows`,
/// once each has finished in the log ([`settle_query_log`]).
async fn statements(admin: &ChClient, comment: &str) -> Vec<(String, u64)> {
    settle_query_log(&[admin], &format!("log_comment = '{comment}'"), 1).await;
    let mut stream = admin
        .query_stream::<StatementRow>(
            &format!(
                "SELECT query, toUInt64(result_rows) AS rows FROM system.query_log \
                 WHERE type = 'QueryFinish' AND log_comment = '{comment}'"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("query_log");
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        let row = row.expect("decode");
        out.push((row.query, row.rows));
    }
    out
}

/// The raw sample statements among `statements`.
fn raw_statements(statements: &[(String, u64)]) -> Vec<&(String, u64)> {
    statements
        .iter()
        .filter(|(q, _)| q.starts_with("SELECT fingerprint, unix_milli, value\n"))
        .collect()
}

/// The samples of series `s` in each interval `(a, b]` of `within`.
fn samples_within(day0: i64, s: std::ops::RangeInclusive<i64>, within: &[(i64, i64)]) -> u64 {
    s.map(|s| {
        within
            .iter()
            .map(|&(a, b)| samples_in(day0, s, a + 1, b).len() as u64)
            .sum::<u64>()
    })
    .sum()
}

/// **H1 (issue #18): finer levels and raw read only what is not
/// covered.** Corpus D's last two days at the default levels, with a head
/// `Hd` and a tail `Tl` cut from the levels' coverage, and 65 one-minute
/// holes cut from the 60 s level. Each statement names the intervals the
/// coarser levels left, never their hull, and the raw statements return
/// only the samples of those intervals; a rounded range inside `Hd` reads
/// raw.
#[tokio::test(flavor = "multi_thread")]
async fn finer_levels_and_raw_read_only_what_is_not_covered() {
    skip_unless_live!();
    let day0 = today() - 8 * DAY;
    let end = day0 + 8 * DAY;
    let built = corpus_d(DB_H1.name(), DEFAULTS, day0, 6..8).await;
    let db = built.db().to_string();
    let run = format!("h1_{db}_{}", now_ms());
    let st = end - DAY;
    let hd = (end - 9 * DAY, st + 2 * HOUR);
    let tl720 = (end - 36 * MIN, end + DAY);
    let tl60 = (end - 30 * MIN, end + DAY);
    let raw = built.engine(false, None).await;

    // (C) The 720 s level cut at both ends: the 60 s statement names the
    // two intervals.
    for (a, b) in [hd, tl720] {
        cut_runs(&built.admin, &db, 720, a, b).await;
    }
    let p = range(st, end, 12 * MIN);
    let query = "max_over_time(g[12m])";
    let on = ask(&built.engine(true, None).await, &no_tenant(), query, &p).await;
    let off = ask(&raw, &no_tenant(), query, &p).await;
    assert!(
        on.stages("rollup_route")
            .iter()
            .any(|n| n.starts_with("metric_samples_720s,")),
        "(C): {:?}",
        on.stages("rollup_route")
    );
    let sixty: Vec<&str> = on
        .explain
        .stages
        .iter()
        .filter(|s| s.name == "rollup_piece" && s.sql.contains("FROM metric_samples_60s\n"))
        .map(|s| s.sql.as_str())
        .collect();
    let want = format!(
        "((t_end > {} AND t_end <= {}) OR (t_end > {} AND t_end <= {end}))",
        st - 1_020_000,
        st + 7_200_000,
        end - 2_160_000
    );
    assert!(
        !sixty.is_empty() && sixty.iter().all(|s| s.contains(&want)),
        "(C): the 60 s statement names {want}: {sixty:?}"
    );
    assert_eq!(on.cells(), off.cells(), "(C): bit-equal to no levels");

    // (D) 65 one-minute holes in the 60 s level: two raw statements, 64
    // intervals and one. Over 3 hours, the 60 s level's switch point.
    let t0 = day0 + 6 * DAY + 6 * HOUR;
    let holes: Vec<(i64, i64)> = (0..=64)
        .map(|k| (t0 + 2 * k * MIN, t0 + (2 * k + 1) * MIN))
        .collect();
    for &(a, b) in &holes {
        cut_runs(&built.admin, &db, 60, a, b).await;
    }
    let comment = format!("{run}_h1_d_on");
    let p = range(t0, t0 + 3 * HOUR, MIN);
    let query = "max_over_time(g[1m])";
    let on = ask(
        &built.engine(true, Some(&comment)).await,
        &no_tenant(),
        query,
        &p,
    )
    .await;
    let off = ask(&raw, &no_tenant(), query, &p).await;
    let sent = statements(&built.admin, &comment).await;
    let raws = raw_statements(&sent);
    assert_eq!(raws.len(), 2, "(D): two raw statements: {raws:?}");
    let got: u64 = raws.iter().map(|(_, n)| n).sum();
    assert_eq!(
        got,
        samples_within(day0, 160..=259, &holes),
        "(D): raw rows"
    );
    assert_eq!(on.cells(), off.cells(), "(D): bit-equal to no levels");

    // The 60 s level cut at both ends, so (E) reads raw.
    for (a, b) in [hd, tl60] {
        cut_runs(&built.admin, &db, 60, a, b).await;
    }

    // (E) A rounded range read wholly from raw: 15 hours, on the 60 s
    // level's own steps, inside `Hd`.
    let p = range(st - DAY, st - DAY + 15 * HOUR, 120_000);
    let on = ask(
        &built.engine(true, None).await,
        &no_tenant(),
        "rate(c_normal[2m15s])",
        &p,
    )
    .await;
    let off = ask(&raw, &no_tenant(), "rate(c_normal[5m])", &p).await;
    assert!(
        on.stages("rollup_route")
            .iter()
            .any(|n| n.ends_with("range 5m (asked 2m15s)")),
        "(E): {:?}",
        on.stages("rollup_route")
    );
    assert!(on.explain.rollup, "(E): exactness is rollup");
    assert_eq!(on.cells(), off.cells(), "(E): rate(c_normal[5m]) from raw");
    built.finish().await;
}

/// `t` rounded up to a whole number of `w`.
fn ceil_to(t: i64, w: i64) -> i64 {
    let down = t.div_euclid(w) * w;
    if down == t { t } else { down + w }
}

/// The request's times, `start + k × step` up to `end`.
fn times_of(p: &MetricQueryParams) -> Vec<i64> {
    (0..)
        .map(|k| p.start_ms + k * p.step_ms)
        .take_while(|&t| t <= p.end_ms)
        .collect()
}

/// The points of `x` whose time `keep` accepts.
fn only(
    x: &BTreeMap<(String, i64), f64>,
    keep: impl Fn(i64) -> bool,
) -> BTreeMap<(String, i64), f64> {
    x.iter()
        .filter(|((_, t), _)| keep(*t))
        .map(|(k, v)| (k.clone(), *v))
        .collect()
}

/// `b`, an answer at the bucket ends of width `w`, drawn at every time of
/// `times` the bucket holds, each value times `scale`.
fn spread(
    b: &BTreeMap<(String, i64), f64>,
    times: &[i64],
    w: i64,
    scale: f64,
) -> BTreeMap<(String, i64), f64> {
    let mut held: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    for &t in times {
        held.entry(ceil_to(t, w)).or_default().push(t);
    }
    let mut out = BTreeMap::new();
    for ((l, tau), v) in b {
        for &t in held.get(tau).map(Vec::as_slice).unwrap_or(&[]) {
            out.insert((l.clone(), t), v * scale);
        }
    }
    out
}

/// How a query's answer is compared: bit-equal, within `f64` relative
/// distance at every point, or within it by the 99th percentile.
#[derive(Clone, Copy, Debug)]
enum Cmp {
    Bits,
    Max(f64),
    P99(f64),
}

/// `a` against `b` by `cmp`, a failure line under `what` if not.
fn compare(
    what: &str,
    a: &BTreeMap<(String, i64), f64>,
    b: &BTreeMap<(String, i64), f64>,
    cmp: Cmp,
) -> Option<String> {
    if b.is_empty() {
        return Some(format!("{what}: nothing to compare against"));
    }
    match cmp {
        Cmp::Bits => (!bit_equal(a, b)).then(|| {
            let first = b
                .iter()
                .find(|(k, v)| a.get(*k).map(|x| x.to_bits()) != Some(v.to_bits()))
                .map(|(k, v)| format!("{k:?}: want {v}, got {:?}", a.get(k)));
            format!(
                "{what}: not bit-equal ({} against {} points); first {first:?}",
                a.len(),
                b.len()
            )
        }),
        Cmp::Max(limit) | Cmp::P99(limit) => {
            let d = distances(a, b);
            let (p99, max) = p99_max(&d);
            println!("{what}: n {} p99 {p99:e} max {max:e}", d.len());
            let got = if matches!(cmp, Cmp::Max(_)) { max } else { p99 };
            (got > limit).then(|| format!("{what}: {got:e} above {limit:e} ({cmp:?})"))
        }
    }
}

/// **L1 (issue #18): a panel reads the level its range selects.** Corpus D
/// at the default levels; a 2-day panel at step 2 m from `end − 2 d +
/// 6 min`, on 2 minutes and off 12: the resolution is 720 s, the bucket
/// ends run to `gl = end − 12 min`, and the three steps after `gl` read
/// the 60 s level, cut after `end − 6 min`, then raw. Every series has
/// every time; each step to `gl` takes the value of the engine without
/// levels at the bucket end that holds it, over 12 minutes; each later
/// step the engine without levels at its own time and range, read from the
/// 60 s level at step 2 m.
#[tokio::test(flavor = "multi_thread")]
async fn a_panel_reads_the_level_its_range_selects() {
    skip_unless_live!();
    let day0 = today() - 8 * DAY;
    let end = day0 + 8 * DAY;
    let built = corpus_d(DB_L1.name(), DEFAULTS, day0, 0..8).await;
    let db = built.db().to_string();
    cut_runs(&built.admin, &db, 720, end - 12 * MIN, end + DAY).await;
    cut_runs(&built.admin, &db, 60, end - 6 * MIN, end + DAY).await;
    let s = end - 2 * DAY + 6 * MIN;
    let p = range(s, end - 6 * MIN, 2 * MIN);
    let width = 720_000;
    let gl = end - 12 * MIN;
    let times = times_of(&p);
    assert_eq!(times.len(), 1_435, "the request's times");
    let routed = built.engine(true, None).await;
    let raw = built.engine(false, None).await;
    let t = no_tenant();
    let lattice = range(s + 6 * MIN, gl, 12 * MIN);
    let newest = range(gl + 2 * MIN, end - 6 * MIN, 2 * MIN);
    let counter = 0.0211;
    // (query, the query at the bucket ends, (iii), (iv), scale of (iii)).
    let cases: [(&str, &str, Cmp, Cmp, f64); 10] = [
        (
            "max_over_time(g[2m])",
            "max_over_time(g[12m])",
            Cmp::Bits,
            Cmp::Bits,
            1.0,
        ),
        (
            "min_over_time(g[2m])",
            "min_over_time(g[12m])",
            Cmp::Bits,
            Cmp::Bits,
            1.0,
        ),
        (
            "avg_over_time(g[2m])",
            "avg_over_time(g[12m])",
            Cmp::Max(1e-12),
            Cmp::Max(1e-12),
            1.0,
        ),
        (
            "last_over_time(g[2m])",
            "last_over_time(g[12m])",
            Cmp::Bits,
            Cmp::Bits,
            1.0,
        ),
        (
            "present_over_time(g[2m])",
            "present_over_time(g[12m])",
            Cmp::Bits,
            Cmp::Bits,
            1.0,
        ),
        (
            "absent_over_time(c_stop[2m])",
            "absent_over_time(c_stop[12m])",
            Cmp::Bits,
            Cmp::Bits,
            1.0,
        ),
        ("g", "g", Cmp::Bits, Cmp::Bits, 1.0),
        (
            "rate(c_normal[2m15s])",
            "rate(c_normal[12m])",
            Cmp::P99(counter),
            Cmp::P99(0.0706),
            1.0,
        ),
        (
            "increase(c_normal[2m15s])",
            "increase(c_normal[12m])",
            Cmp::P99(counter),
            Cmp::P99(0.0706),
            135.0 / 720.0,
        ),
        (
            "sum(rate(c_normal[2m15s]))",
            "sum(rate(c_normal[12m]))",
            Cmp::P99(counter),
            Cmp::P99(0.0706),
            1.0,
        ),
    ];
    let mut failures = Vec::new();
    for (query, at_ends, early, late, scale) in cases {
        let a = ask(&routed, &t, query, &p).await;
        // (i) every time of the request.
        let series = per_series(&a);
        let want_series = match query {
            "absent_over_time(c_stop[2m])" | "sum(rate(c_normal[2m15s]))" => 1,
            _ => 100,
        };
        if series.len() != want_series {
            failures.push(format!("{query} (i): {} series", series.len()));
        }
        for (l, ts) in &series {
            let ok = if query.starts_with("absent_over_time") {
                !ts.is_empty() && ts[..] == times[times.len() - ts.len()..]
            } else {
                ts == &times
            };
            if !ok {
                failures.push(format!(
                    "{query} (i) {l}: {} of {} times",
                    ts.len(),
                    times.len()
                ));
            }
        }
        // (ii) the bucket ends' stage and route.
        let lattice_stage = a.explain.stages.iter().any(|s| s.name == "rollup_lattice");
        let route = a.stages("rollup_route");
        let head = if query == "g" {
            "metric_samples_720s, use Last, step 12m, range none".to_string()
        } else if query.contains("2m15s") {
            "metric_samples_720s, use Counter, step 12m, range 12m (asked 2m15s)".to_string()
        } else {
            "metric_samples_720s, use".to_string()
        };
        let routed_ok = route
            .iter()
            .any(|n| n.starts_with(&head) && (query == "g" || n.contains("step 12m, range 12m")));
        if !lattice_stage || !routed_ok {
            failures.push(format!(
                "{query} (ii): rollup_lattice {lattice_stage}, routes {route:?}"
            ));
        }
        // (iii) each step to `gl` from the bucket that holds it.
        let ends = ask(&raw, &t, at_ends, &lattice).await;
        let points = a.points();
        let want = spread(&ends.points(), &times, width, scale);
        failures.extend(compare(
            &format!("L1 (iii) {query}"),
            &only(&points, |x| x <= gl),
            &only(&want, |x| x <= gl),
            early,
        ));
        // (iv) each later step at its own time.
        let own = ask(&raw, &t, query, &newest).await;
        failures.extend(compare(
            &format!("L1 (iv) {query}"),
            &only(&points, |x| x > gl),
            &own.points(),
            late,
        ));
        // ... from the second read, at the request's resolution: the 60 s
        // level at step 2 m (the bucket ends' notes say step 12m).
        let second = match query {
            "rate(c_normal[2m15s])" | "increase(c_normal[2m15s])" => {
                Some("metric_samples_60s, use Counter, step 2m, range 5m (asked 2m15s)")
            }
            "max_over_time(g[2m])" => Some("metric_samples_60s, use Max, step 2m, range 2m"),
            _ => None,
        };
        if let Some(head) = second
            && !route.iter().any(|n| n.starts_with(head))
        {
            failures.push(format!("{query} (iv): no route `{head}`: {route:?}"));
        }
        // (v) the answer is a rollup's.
        if !a.explain.rollup {
            failures.push(format!("{query} (v): exactness is not rollup"));
        }
    }
    built.finish().await;
    assert!(failures.is_empty(), "L1:\n{}", failures.join("\n"));
}

/// **L2 (issue #18): the bucket ends read no finer level where their own
/// covers.** Corpus D's last two days; `rate(c_normal[2m15s])` over the
/// L1 request with no cut. The 60 s statements return only the newest
/// steps' windows, about 14 minutes, and their run rows; the 720 s
/// statement every bucket end.
#[tokio::test(flavor = "multi_thread")]
async fn the_lattice_reads_no_finer_level_where_its_own_covers() {
    skip_unless_live!();
    let (built, day0) = D60_2
        .lease("the_lattice_reads_no_finer_level_where_its_own_covers")
        .await;
    let end = day0 + 8 * DAY;
    let comment = format!("{}_l2_on_{}", built.db(), now_ms());
    let p = range(end - 2 * DAY + 6 * MIN, end - 6 * MIN, 2 * MIN);
    let a = ask(
        &built.engine(true, Some(&comment)).await,
        &no_tenant(),
        "rate(c_normal[2m15s])",
        &p,
    )
    .await;
    assert!(
        a.explain.stages.iter().any(|s| s.name == "rollup_lattice"),
        "read at the bucket ends: {:?}",
        a.explain.stages
    );
    let sent = statements(&built.admin, &comment).await;
    let rows = |table: &str| -> Vec<u64> {
        sent.iter()
            .filter(|(q, _)| q.contains(&format!("FROM {table}\n")))
            .map(|(_, n)| *n)
            .collect()
    };
    let (sixty, twelve) = (rows("metric_samples_60s"), rows("metric_samples_720s"));
    built.finish().await;
    let sixty_rows: u64 = sixty.iter().sum();
    assert!(
        sixty_rows <= 100 * 20 + 20,
        "the 60 s statements return {sixty_rows} rows: {sixty:?}"
    );
    let twelve_rows: u64 = twelve.iter().sum();
    assert!(
        twelve_rows >= 100 * 238,
        "the 720 s statements return {twelve_rows} rows: {twelve:?}"
    );
}

/// **L3 (issue #18): a week and three hours fill from their levels.**
/// Corpus D at the default levels. A 7-day request at step 10 m reads the
/// 7200 s level at its bucket ends; a 3-hour request at step 15 s the 60 s
/// level.
#[tokio::test(flavor = "multi_thread")]
async fn a_week_and_three_hours_fill_from_their_levels() {
    skip_unless_live!();
    let (built, day0) = D60_8
        .lease("a_week_and_three_hours_fill_from_their_levels")
        .await;
    let end = day0 + 8 * DAY;
    let routed = built.engine(true, None).await;
    let raw = built.engine(false, None).await;
    let t = no_tenant();
    let mut failures = Vec::new();

    let query = "rate(c_normal[10m15s])";
    let p = range(end - 7 * DAY - 10 * MIN, end - 10 * MIN, 10 * MIN);
    let (a, b) = futures::join!(ask(&routed, &t, query, &p), ask(&raw, &t, query, &p));
    for (l, ts) in &per_series(&a) {
        if ts.len() != 1_009 {
            failures.push(format!("7 d {l}: {} times", ts.len()));
        }
    }
    let note = "metric_samples_7200s, use Counter, step 2h, range 2h (asked 10m15s)";
    if !a.stages("rollup_route").iter().any(|n| n == note) {
        failures.push(format!("7 d: routes {:?}", a.stages("rollup_route")));
    }
    let gl = end - 2 * HOUR;
    failures.extend(compare(
        "L3 7 d",
        &only(&a.points(), |x| x <= gl),
        &only(&b.points(), |x| x <= gl),
        Cmp::P99(0.0254),
    ));

    let query = "max_over_time(g[1m])";
    let p = range(end - 3 * HOUR - 45_000, end - 15_000, 15_000);
    let a = ask(&routed, &t, query, &p).await;
    let times = times_of(&p);
    for (l, ts) in &per_series(&a) {
        if ts.len() != 723 || ts != &times {
            failures.push(format!("3 h {l}: {} times", ts.len()));
        }
    }
    if !a
        .stages("rollup_route")
        .iter()
        .any(|n| n.starts_with("metric_samples_60s,"))
    {
        failures.push(format!("3 h: routes {:?}", a.stages("rollup_route")));
    }
    let gl = end - MIN;
    let ends = ask(&raw, &t, query, &range(end - 3 * HOUR, gl, MIN)).await;
    failures.extend(compare(
        "L3 3 h",
        &only(&a.points(), |x| x <= gl),
        &only(&spread(&ends.points(), &times, MIN, 1.0), |x| x <= gl),
        Cmp::Bits,
    ));
    built.finish().await;
    assert!(failures.is_empty(), "L3:\n{}", failures.join("\n"));
}

/// **L7 (issue #18): a day level fills a week.** Corpus D with the 86400 s
/// level the resolution from 6 days; a request of 7 days less 1 hour at
/// step 1 h reads it at its bucket ends, every step at or before `end`.
#[tokio::test(flavor = "multi_thread")]
async fn a_day_level_fills_a_week() {
    skip_unless_live!();
    let day0 = today() - 8 * DAY;
    let end = day0 + 8 * DAY;
    let levels: (&[u32], &[u32]) = (
        pulsus_schema::DEFAULT_ROLLUP_LEVELS,
        &[10800, 57600, 259200, 518400],
    );
    let built = corpus_d(DB_L7.name(), levels, day0, 0..8).await;
    let routed = built.engine(true, None).await;
    let raw = built.engine(false, None).await;
    let t = no_tenant();
    let start = end - 7 * DAY + HOUR;
    let p = range(start, end, HOUR);
    let times = times_of(&p);
    let mut failures = Vec::new();
    for (query, at_ends, cmp) in [
        ("rate(c_normal[1h0m15s])", None, Cmp::P99(0.00308)),
        ("rate(c_sparse[1h0m15s])", None, Cmp::P99(0.0116)),
        (
            "max_over_time(g[1h])",
            Some("max_over_time(g[1d])"),
            Cmp::Bits,
        ),
    ] {
        let a = ask(&routed, &t, query, &p).await;
        let series = per_series(&a);
        if series.is_empty() {
            failures.push(format!("{query}: no series"));
        }
        for (l, ts) in &series {
            if ts != &times {
                failures.push(format!(
                    "{query} {l}: {} of {} times",
                    ts.len(),
                    times.len()
                ));
            }
        }
        if !a
            .stages("rollup_route")
            .iter()
            .any(|n| n.starts_with("metric_samples_86400s,"))
        {
            failures.push(format!("{query}: routes {:?}", a.stages("rollup_route")));
        }
        let want = match at_ends {
            Some(q) => {
                let ends = ask(&raw, &t, q, &range(end - 6 * DAY, end, DAY)).await;
                spread(&ends.points(), &times, DAY, 1.0)
            }
            None => ask(&raw, &t, query, &p).await.points(),
        };
        failures.extend(compare(&format!("L7 {query}"), &a.points(), &want, cmp));
    }
    built.finish().await;
    assert!(failures.is_empty(), "L7:\n{}", failures.join("\n"));
}

/// **L5 (issue #18): pushdown on the request's steps, and when filled.**
/// Corpus P with the pushed engine against the engine with the levels and
/// no pushdown. (a) A 6-hour request on its own minute steps pushes
/// through the 60 s level; (b) one at 30 s off the minute is filled, and
/// its bucket ends' read pushes on the minute (issue #18 part 2b); (c) a
/// day at step 2 m is filled with no newest steps: `rollup_lattice` first,
/// then the bucket ends' read with its pushed statement; (d) a week at 1 h
/// is the same, pushed at 7200 s.
#[tokio::test(flavor = "multi_thread")]
async fn pushdown_on_the_request_grid_and_when_filled() {
    skip_unless_live!();
    let (built, end) = P
        .lease("pushdown_on_the_request_grid_and_when_filled")
        .await;
    let on = built.pushed_engine(None).await;
    let off = built.engine(true, None).await;
    let t = no_tenant();
    let q = "sum by (instance) (rate(p_rx_total[1m]))";
    let mut failures = Vec::new();

    // (a)
    let p = range(end - 6 * HOUR, end, MIN);
    let (a, b) = futures::join!(ask(&on, &t, q, &p), ask(&off, &t, q, &p));
    let grid = format!("{} AS grid_start, 60000 AS grid_step", end - 6 * HOUR);
    if !a.pushed().iter().any(|s| s.contains(&grid)) {
        failures.push(format!(
            "(a): no pushed statement on {grid}; declines {:?}",
            a.declines()
        ));
    }
    failures.extend(compare("L5 (a)", &a.points(), &b.points(), Cmp::P99(1e-12)));

    // (b)
    let p = range(end - 6 * HOUR + 30_000, end - 30_000, 30_000);
    let (a, b) = futures::join!(ask(&on, &t, q, &p), ask(&off, &t, q, &p));
    if !a.declines().is_empty() || !a.pushed().iter().any(|s| s.contains("60000 AS grid_step")) {
        failures.push(format!(
            "(b): declines {:?}, pushed {:?}",
            a.declines(),
            a.pushed()
        ));
    }
    let times = times_of(&p);
    for (l, ts) in &per_series(&a) {
        if ts != &times {
            failures.push(format!("(b) {l}: {} of {} times", ts.len(), times.len()));
        }
    }
    failures.extend(compare("L5 (b)", &a.points(), &b.points(), Cmp::P99(1e-12)));

    // (c)
    let q = "sum by (instance) (rate(p_rx_total[2m15s]))";
    let a = ask(&on, &t, q, &range(end - DAY, end, 2 * MIN)).await;
    let names: Vec<(&str, &str)> = a
        .explain
        .stages
        .iter()
        .map(|s| (s.name, s.sql.as_str()))
        .collect();
    let in_order = names.first().map(|s| s.0) == Some("rollup_lattice")
        && a.declines().is_empty()
        && a.pushed().iter().any(|s| s.contains("720000 AS grid_step"));
    if !in_order {
        failures.push(format!("(c): stages {names:?}"));
    }

    // (d) A week at 1 h, filled at 7200 s: the bucket ends' read pushes at
    // 7200 s.
    let q = "sum by (instance) (rate(p_rx_total[5m]))";
    let a = ask(&on, &t, q, &range(end - 7 * DAY, end, HOUR)).await;
    let names: Vec<(&str, &str)> = a
        .explain
        .stages
        .iter()
        .map(|s| (s.name, s.sql.as_str()))
        .collect();
    let in_order = names.first().map(|s| s.0) == Some("rollup_lattice")
        && a.declines().is_empty()
        && a.pushed()
            .iter()
            .any(|s| s.contains("7200000 AS grid_step"));
    if !in_order {
        failures.push(format!("(d): stages {names:?}"));
    }
    built.finish().await;
    assert!(failures.is_empty(), "L5:\n{}", failures.join("\n"));
}

/// **L4 (issue #18): queries off the lattice keep the request's steps.**
/// Corpus D's last two days, a 2-day request at step 2 m off the 12-minute
/// buckets: a query with a selector no level serves, one with an `@`, and
/// one whose offset no level's buckets end at are read at the request's
/// own steps, every time present.
#[tokio::test(flavor = "multi_thread")]
async fn queries_off_the_lattice_keep_the_request_grid() {
    skip_unless_live!();
    let (built, day0) = D60_2
        .lease("queries_off_the_lattice_keep_the_request_grid")
        .await;
    let end = day0 + 8 * DAY;
    let p = range(end - 2 * DAY + 6 * MIN, end, 2 * MIN);
    let times = times_of(&p);
    let routed = built.engine(true, None).await;
    let at = format!("rate(c_normal[5m] @ {})", (end - HOUR) / 1000);
    let mut failures = Vec::new();
    for query in [
        "rate(c_normal[5m]) / irate(c_normal[5m])",
        at.as_str(),
        "rate(c_normal[5m] offset 90s)",
    ] {
        let a = ask(&routed, &no_tenant(), query, &p).await;
        if a.explain.stages.iter().any(|s| s.name == "rollup_lattice") {
            failures.push(format!("{query}: read at the bucket ends"));
        }
        let series = per_series(&a);
        if series.len() != 100 {
            failures.push(format!("{query}: {} series", series.len()));
        }
        for (l, ts) in &series {
            if ts != &times {
                failures.push(format!(
                    "{query} {l}: {} of {} times",
                    ts.len(),
                    times.len()
                ));
            }
        }
        if query.contains("irate")
            && !a
                .stages("rollup_route")
                .iter()
                .any(|n| n.starts_with("metric_samples_60s, use Counter, step 2m, range 5m"))
        {
            failures.push(format!("{query}: routes {:?}", a.stages("rollup_route")));
        }
    }
    built.finish().await;
    assert!(failures.is_empty(), "L4:\n{}", failures.join("\n"));
}

/// **L8 (issue #18): a query that reads the step time keeps its steps.**
/// L4's corpus and request: `g * 0 + time()` is read at the request's own
/// steps, each value its time in seconds; `hour(g)` reads only values and
/// is read at the bucket ends.
#[tokio::test(flavor = "multi_thread")]
async fn a_query_that_reads_the_step_time_keeps_its_steps() {
    skip_unless_live!();
    let (built, day0) = D60_2
        .lease("a_query_that_reads_the_step_time_keeps_its_steps")
        .await;
    let end = day0 + 8 * DAY;
    let p = range(end - 2 * DAY + 6 * MIN, end, 2 * MIN);
    let routed = built.engine(true, None).await;
    let lattice = |a: &Answer| a.explain.stages.iter().any(|s| s.name == "rollup_lattice");
    let a = ask(&routed, &no_tenant(), "g * 0 + time()", &p).await;
    let b = ask(&routed, &no_tenant(), "hour(g)", &p).await;
    built.finish().await;
    assert!(!lattice(&a), "g * 0 + time(): {:?}", a.explain.stages);
    let points = a.points();
    assert_eq!(
        points.len(),
        100 * times_of(&p).len(),
        "every time of every series"
    );
    for ((l, t), v) in &points {
        assert_eq!(
            v.to_bits(),
            (*t as f64 / 1000.0).to_bits(),
            "{l} at {t}: its time in seconds"
        );
    }
    assert!(lattice(&b), "hour(g): {:?}", b.explain.stages);
}

/// One row of RR: the case, its request, the range asked, the level and
/// the last bucket end, and the p99 per counter kind.
type RoundedCase = (
    &'static str,
    MetricQueryParams,
    &'static str,
    i64,
    i64,
    [f64; 4],
);

/// **RR (issue #18): a rounded range is within its distance.** Corpus D
/// over eight days at the default levels. The three measured cases, each
/// `start` on its step and off the level's buckets: `rate` and `increase`
/// on each counter kind against no levels at the range asked, over the
/// times to the last bucket end. The route note names the level, and the
/// p99 distance is at most the measured one rounded up.
#[tokio::test(flavor = "multi_thread")]
async fn a_rounded_range_is_within_its_distance() {
    skip_unless_live!();
    let (built, day0) = D60_8.lease("a_rounded_range_is_within_its_distance").await;
    let end = day0 + 8 * DAY;
    let rows: [RoundedCase; 3] = [
        (
            "6 h",
            range(end - 6 * HOUR + 20_000, end, 20_000),
            "1m",
            60,
            end,
            [0.0618, 0.00363, 1.22, 1.01],
        ),
        (
            "2 d",
            range(end - 2 * DAY + 2 * MIN, end, 2 * MIN),
            "2m15s",
            720,
            end,
            [0.125, 0.0212, 0.129, 0.216],
        ),
        (
            "7 d",
            range(end - 7 * DAY - 10 * MIN, end - 10 * MIN, 10 * MIN),
            "10m15s",
            7200,
            end - 2 * HOUR,
            [0.0254, 0.00219, 1.00, 0.0571],
        ),
    ];
    let kinds = ["c_normal", "c_fast", "c_stop", "c_sparse"];
    let mut table = Vec::new();
    let mut over = Vec::new();
    for (case, p, asked, level, gl, limits) in rows {
        for (metric, limit) in kinds.into_iter().zip(limits) {
            for f in ["rate", "increase"] {
                let query = format!("{f}({metric}[{asked}])");
                let (a, b) = both(&built, &query, &p, None).await;
                let route = a.stages("rollup_route");
                let head = format!("metric_samples_{level}s, use Counter, ");
                assert!(
                    route.iter().any(|n| n.starts_with(&head)),
                    "{case} {query}: {route:?}"
                );
                let d = distances(
                    &only(&a.points(), |x| x <= gl),
                    &only(&b.points(), |x| x <= gl),
                );
                let (p99, max) = p99_max(&d);
                table.push(format!(
                    "{case} {query}: n {} p99 {p99:e} max {max:e} (at most {limit:e})",
                    d.len()
                ));
                if p99 > limit {
                    over.push(format!("{case} {query}: p99 {p99:e} above {limit:e}"));
                }
            }
        }
    }
    for line in &table {
        println!("RR {line}");
    }
    built.finish().await;
    assert!(over.is_empty(), "{over:?}");
}

static DB_KEPT: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_kept");

/// **B4 (issue #743 part B): a level older than raw answers.** Levels `60,
/// 720`, `R` = 2, the levels kept `2` and `10` days. Corpus D's series
/// registered for the UTC day `d0`, five days back, and hand-made 720 s
/// rows for its `g` series over that day with one run row, all after
/// `OPTIMIZE … FINAL`. `avg_over_time(g[12m])` over the day at step 12 m
/// reads the 720 s level and answers each row's `sum ÷ count`; the series
/// resolve by SQL, the label cache's window being one day.
#[tokio::test(flavor = "multi_thread")]
async fn a_level_older_than_raw_answers() {
    skip_unless_live!();
    let db = DB_KEPT.name();
    let bootstrap = ChClient::new(test_config("default"))
        .await
        .expect("connect (bootstrap)");
    let ctx = RenderCtx {
        rollup_levels: vec![60, 720],
        rollup_ranges: vec![60, 86400],
        rollup_retention_days: vec![2, 10],
        ..RenderCtx::for_tests(db, 2)
    };
    fresh(&bootstrap, &ctx).await;
    let admin = ChClient::new(test_config(db)).await.expect("connect");
    let d0 = today() - 5 * DAY;
    exec(&admin, &corpus_d_series(db)).await;
    exec(&admin, &corpus_d_registrations(db, d0, 24)).await;
    exec(
        &admin,
        &format!(
            "INSERT INTO {db}.metric_samples_720s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags, built_at) \
             SELECT 0, '', fp, {d0} + toInt64(j.number + 1) * 720000, {d0} + toInt64(j.number + 1) * 720000 - 1000, \
                    toFloat64(s), toFloat64(s), toFloat64(s), toFloat64(s * 1000 + j.number + 1), \
                    toUInt32(1 + (s + j.number + 1) % 4), 0, 0, 0 \
             FROM {db}.rd_series, numbers(120) AS j WHERE metric = 'g'"
        ),
    )
    .await;
    exec(
        &admin,
        &format!(
            "INSERT INTO {db}.metric_samples_720s (kind, org_id, fingerprint, t_end, t_last, built_at) \
             VALUES (1, '', 0, {}, {d0}, 0)",
            d0 + DAY
        ),
    )
    .await;
    for table in ["metric_samples_720s", "metric_series", "metric_labels"] {
        exec(&admin, &format!("OPTIMIZE TABLE {db}.{table} FINAL")).await;
    }

    let mut cfg = cache_config(db);
    cfg.window_ms = DAY;
    let cache = Arc::new(LabelCache::new(
        ChClient::new(test_config(db)).await.expect("connect"),
        cfg,
    ));
    cache.refresh().await.expect("refresh");
    let slot = Arc::new(LevelSlot::new());
    refresh_levels(&slot, &admin, db, &ctx.dist_suffix, None)
        .await
        .expect("level discovery");
    let engine = MetricsEngine::new(
        ChClient::new(test_config(db)).await.expect("connect"),
        cache,
        engine_config(db, false),
    )
    .with_rollup_levels(slot);
    let a = ask(
        &engine,
        &no_tenant(),
        "avg_over_time(g[12m])",
        &range(d0, d0 + DAY, 12 * MIN),
    )
    .await;

    let mut want = BTreeMap::new();
    for s in 160..260i64 {
        for j in 1..=120i64 {
            let labels = vec![("s".to_string(), s.to_string())];
            let v = (s * 1000 + j) as f64 / (1 + (s + j) % 4) as f64;
            want.insert(
                (format!("{labels:?}"), d0 + j * 720_000),
                format!("{:016x}", v.to_bits()),
            );
        }
    }
    assert_eq!(want.len(), 12_000);
    assert_eq!(
        a.cells(),
        want,
        "each 720 s row's sum ÷ count, no point at d0"
    );
    let routes = a.stages("rollup_route");
    assert!(
        routes.iter().any(|n| n.starts_with("metric_samples_720s,")),
        "the 720 s level answers: {routes:?}"
    );
    let resolved = a
        .explain
        .stages
        .iter()
        .find(|s| s.name == "series_resolution")
        .expect("series_resolution");
    assert!(
        resolved.sql.starts_with("SELECT"),
        "the series resolve by SQL: {resolved:?}"
    );
    drop_database(&bootstrap, db).await;
}

// ------------------------------------------ issue #579 part 4, corpus P

/// The panel part 4 was measured on: shape C over two counters read from
/// the 60 s level.
const PANEL: &str = "sum by (instance) (rate(p_rx_total[2m15s]) + rate(p_tx_total[2m15s]))";

/// Corpus P's last time: a whole hour, an hour ago.
fn p_end() -> i64 {
    (now_ms() / HOUR) * HOUR - HOUR
}

/// Corpus P (part 4, plan section 12) ending at `end`: `p_rx_total` and
/// `p_tx_total` over `instance` × `device` = 4 × 6, less tx `{i3,d5}` and rx
/// `{i2,d4}`, scraped every 15 s with corpus D's jitter for 50 hours, and
/// the series e1 to e13 of that table. A series' `key` is `rx`/`tx`, its
/// instance and its device: `rxi0d1`.
///
/// Issue #18 part 2a: `p2` (corpus P2 only) resets rx `{i3,d1}` to 0 at
/// `end − 22 h` and at `end − 13 h 50 min`, and adds rx `{i9,d0}`, one sample a 720 s bucket at
/// its start plus 1 ms, valued `−1e6 + 7k` at the `k`-th: each 720 s row's
/// `t_last` is its start point's time, and `c − inc < c` while their bits
/// order the other way.
fn corpus_p_land(db: &str, end: i64, p2: bool) -> Vec<String> {
    let lo = end - 50 * HOUR;
    let stale = "reinterpretAsFloat64(toUInt64(9218868437227405314))";
    let series = format!(
        "CREATE TABLE {db}.rp_series ENGINE = Memory AS \
         SELECT metric, inst, dev, key, seed, labels, fp FROM ( \
           SELECT metric, concat('i', toString(ii)) AS inst, concat('d', toString(di)) AS dev, \
                  concat(if(metric = 'p_rx_total', 'rx', 'tx'), inst, dev) AS key, \
                  toUInt32(if(key = 'txi2d0', 12, if(metric = 'p_rx_total', 0, 24) + ii * 6 + di)) AS seed, \
                  if(ii = 3, concat('{{\"__unit__\":\"bytes\",\"device\":\"', dev, '\",\"instance\":\"', inst, '\"}}'), \
                     concat('{{\"device\":\"', dev, '\",\"instance\":\"', inst, '\"}}')) AS labels, \
                  concat(metric, unhex('FF'), labels, unhex('FF')) AS buf, \
                  bitOr(bitShiftLeft(toUInt128(bitShiftRight(cityHash64(metric), 32)), 96), \
                        bitAnd(bitOr(bitShiftLeft(toUInt128(cityHash64(buf)), 64), toUInt128(xxHash64(buf))), \
                               toUInt128('79228162514264337593543950335'))) AS fp \
           FROM (SELECT arrayJoin(['p_rx_total', 'p_tx_total']) AS metric) \
           ARRAY JOIN [0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3] AS ii, \
                      [0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5] AS di) \
         WHERE key NOT IN ('txi3d5', 'rxi2d4')"
    );
    // Issue #18 part 2a: rx `{i9,d0}`, by the same ID formula.
    let i9 = format!(
        "INSERT INTO {db}.rp_series \
         SELECT metric, inst, dev, key, seed, labels, fp FROM ( \
           SELECT 'p_rx_total' AS metric, 'i9' AS inst, 'd0' AS dev, 'rxi9d0' AS key, toUInt32(48) AS seed, \
                  concat('{{\"device\":\"', dev, '\",\"instance\":\"', inst, '\"}}') AS labels, \
                  concat(metric, unhex('FF'), labels, unhex('FF')) AS buf, \
                  bitOr(bitShiftLeft(toUInt128(bitShiftRight(cityHash64(metric), 32)), 96), \
                        bitAnd(bitOr(bitShiftLeft(toUInt128(cityHash64(buf)), 64), toUInt128(xxHash64(buf))), \
                               toUInt128('79228162514264337593543950335'))) AS fp)"
    );
    let registrations = format!(
        "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) \
         SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 2, metric, fp, \
                toInt64({lo}) + toInt64(n.number) * {HOUR} + 1000, labels, 0 \
         FROM {db}.rp_series, numbers(50) AS n"
    );
    // Each counter: `base` until its first reset, then 0, plus the
    // increments before each sample in its segment. e5 steps 0.1 to 0.4
    // near 1e15; e10 is constant; e9 is rx `{i2,d0}`'s twin by its seed.
    let samples = format!(
        "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
         SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 0, metric, fp, t, \
                if(nan_here, nan, base * (seg = 0) + sum(inc) OVER (PARTITION BY fp, seg ORDER BY i \
                   ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS v \
         FROM ( \
           SELECT metric, fp, key, i, t, \
             multiIf(key IN ('rxi0d1', 'rxi1d5'), 1e9, key = 'rxi1d1', 1e15, key IN ('rxi0d0', 'rxi1d3'), 0., \
                     key = 'txi2d1', 500., 1000. * (seed + 1)) AS base, \
             multiIf(key = 'rxi1d1', 0.1 * toFloat64(1 + cityHash64(seed, i) % 4), key = 'txi2d1', 0., \
                     toFloat64(1 + cityHash64(seed, i) % 10)) AS inc, \
             multiIf(key = 'rxi0d1', toUInt8(t >= {e2}), key = 'rxi1d5', toUInt8(t >= {e12}), \
                     key = 'rxi1d2', toUInt8(t >= {e6}) + toUInt8(t >= {e6b}), {reset}toUInt8(0)) AS seg, \
             (key = 'rxi0d3' AND i = {i4}) OR (key = 'rxi1d0' AND i = {i4b}) AS nan_here \
           FROM ( \
             SELECT s.metric AS metric, s.fp AS fp, s.key AS key, s.seed AS seed, toInt64(n.number) AS i, \
                    toInt64({lo}) + 15000 * i + (100 + (7919 * seed) % 14800) + (toInt64((7 * seed + 13 * i) % 201) - 100) AS t \
             FROM {db}.rp_series AS s, numbers({n}) AS n) \
           WHERE t <= {end} \
             AND NOT (key = 'rxi0d0' AND t < {e1}) \
             AND NOT (key = 'rxi0d2' AND t > {e3} - 60000 AND t <= {e3}) \
             AND NOT (key = 'rxi1d0' AND i < {i4b}) \
             AND NOT (key = 'rxi1d3' AND t < {e7}) \
             AND NOT (key = 'rxi1d4' AND t >= {e8}) \
             AND NOT (key = 'rxi3d4' AND t > {e13} - 240000){no_i9})",
        e1 = end - 20 * HOUR,
        e2 = end - DAY - 4 * MIN - 30_000,
        e3 = end - 15 * HOUR,
        i4 = (end - 12 * HOUR - lo) / SCRAPE,
        i4b = (end - DAY - 9 * MIN - lo) / SCRAPE,
        e6 = end - 9 * HOUR,
        e6b = end - 9 * HOUR + MIN,
        e7 = end - 5 * MIN,
        e8 = end - 3 * HOUR,
        e12 = end - 15 * HOUR + 351 * MIN + 30_000,
        e13 = end - DAY - 8 * MIN,
        n = 50 * HOUR / SCRAPE,
        reset = if p2 {
            format!(
                "key = 'rxi3d1', toUInt8(t >= {}) + toUInt8(t >= {}), ",
                end - 22 * HOUR,
                end - 13 * HOUR - 50 * MIN
            )
        } else {
            String::new()
        },
        no_i9 = if p2 { " AND key != 'rxi9d0'" } else { "" },
    );
    // e3's minute holds only a stale marker; e8 and e13 end on one.
    let markers = format!(
        "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
         SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 0, s.metric, s.fp, x.t, {stale} \
         FROM (SELECT * FROM values('k String, t Int64', ('rxi0d2', {}), ('rxi1d4', {}), ('rxi3d4', {}))) AS x \
         JOIN {db}.rp_series AS s ON s.key = x.k",
        end - 15 * HOUR - 30_000,
        end - 3 * HOUR,
        end - DAY - 8 * MIN,
    );
    if !p2 {
        return vec![series, registrations, samples, markers];
    }
    let i9_samples = format!(
        "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
         SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 0, s.metric, s.fp, \
                toInt64({lo}) + toInt64(n.number) * 720000 + 1, -1e6 + 7 * toFloat64(n.number) \
         FROM {db}.rp_series AS s, numbers({}) AS n WHERE s.key = 'rxi9d0'",
        50 * HOUR / 720_000
    );
    vec![series, i9, registrations, samples, markers, i9_samples]
}

/// Corpus P built at the default levels to `end − 10 min`, its 60 s
/// coverage cut over `(end − 6 h, end − 5 h 30 m]`.
async fn corpus_p(db: &str, end: i64) -> Built {
    let built = build(
        db,
        DEFAULTS,
        &corpus_p_land(db, end, false),
        end - 50 * HOUR,
        end - 10 * MIN,
        &[],
    )
    .await;
    cut_runs(
        &built.admin,
        db,
        60,
        end - 6 * HOUR,
        end - 5 * HOUR - 30 * MIN,
    )
    .await;
    built
}

/// The two engines' answers to `query`, pushed first, and a failure line
/// unless they are equal as bits and the pushed one pushed a node: one
/// reading every level table the in-process route's `rollup_piece` stages
/// say served it, and then with no `rollup_piece`.
async fn pushed_against_in_process(
    on: &MetricsEngine,
    off: &MetricsEngine,
    query: &str,
    p: &MetricQueryParams,
) -> (Answer, Answer, Vec<String>) {
    let t = no_tenant();
    let (a, b) = futures::join!(ask(on, &t, query, p), ask(off, &t, query, p));
    let mut failures = Vec::new();
    let (ca, cb) = (vector_or_matrix(&a), vector_or_matrix(&b));
    if cb.is_empty() {
        failures.push(format!("{query} at {p:?}: the in-process answer is empty"));
    }
    if ca != cb {
        let first = cb
            .iter()
            .find(|(k, v)| ca.get(*k) != Some(*v))
            .map(|(k, v)| format!("{k:?}: in process {v}, pushed {:?}", ca.get(k)));
        failures.push(format!(
            "{query} at {p:?}: {} cells pushed, {} in process; first difference {first:?}",
            ca.len(),
            cb.len()
        ));
    }
    let pushed = a.pushed();
    if pushed.is_empty() {
        failures.push(format!(
            "{query} at {p:?}: no pushed_aggregate statement; declines {:?}",
            a.declines()
        ));
    }
    if !b.stages("rollup_route").is_empty() {
        let tables = served_tables(&b);
        if tables.is_empty() {
            failures.push(format!("{query} at {p:?}: in process, no level served"));
        }
        for table in tables {
            if !pushed
                .iter()
                .any(|s| s.contains(&format!("FROM {table}\n")))
            {
                failures.push(format!(
                    "{query} at {p:?}: the pushed statement does not read {table}"
                ));
            }
        }
        if !a.stages("rollup_piece").is_empty() {
            failures.push(format!("{query} at {p:?}: rollup pieces beside the push"));
        }
    }
    (a, b, failures)
}

/// Issue #18 part 2a: the level tables an answer's `rollup_piece` notes
/// say served something (`metric_samples_720s.inc served (a, b]`).
fn served_tables(x: &Answer) -> std::collections::BTreeSet<String> {
    x.stages("rollup_piece")
        .iter()
        .filter_map(|n| n.split_once(" served (").map(|(head, _)| head))
        .filter_map(|head| head.split_once('.').map(|(t, _)| t.to_string()))
        .collect()
}

/// **T1 (issue #579 part 4): shape A over the 60 s level answers as in
/// process.** Every aggregation, `rate` and `increase`, three groupings and
/// two ranges, over 15 h at 2 m, 6 h at 1 m and an instant at `end − 1 h`.
#[tokio::test(flavor = "multi_thread")]
async fn a_level_rate_aggregate_answers_as_in_process() {
    skip_unless_live!();
    let (built, end) = P
        .lease("a_level_rate_aggregate_answers_as_in_process")
        .await;
    let on = built.pushed_engine(None).await;
    let off = built.engine(true, None).await;
    let windows = [
        range(end - 15 * HOUR, end, 2 * MIN),
        range(end - 6 * HOUR, end, MIN),
        range(end - HOUR, end - HOUR, 0),
    ];
    let mut failures = Vec::new();
    for agg in ["sum", "avg", "count", "min", "max"] {
        for f in ["rate", "increase"] {
            for g in ["by (instance) ", "without (device) ", ""] {
                for r in ["2m15s", "5m"] {
                    let q = format!("{agg} {g}({f}(p_rx_total[{r}]))");
                    for p in &windows {
                        failures.extend(pushed_against_in_process(&on, &off, &q, p).await.2);
                    }
                }
            }
        }
    }
    built.finish().await;
    assert!(failures.is_empty(), "T1:\n{}", failures.join("\n"));
}

/// **T2 (issue #579 part 4): the panel sends groups, not level rows.** No
/// level fetch; one coverage statement; one data statement (issue #18
/// part 2c: with no replica check), returning at most a row per instance
/// per step and the histogram count.
#[tokio::test(flavor = "multi_thread")]
async fn the_panel_sends_groups_not_level_rows() {
    skip_unless_live!();
    let (built, end) = P.lease("the_panel_sends_groups_not_level_rows").await;
    let db = &built.db().to_string();
    // Unique to this run: the log keeps every earlier run's statements
    // under the same database name.
    let comment = format!("{db}_panel_{}", now_ms());
    let e = built.pushed_engine(Some(&comment)).await;
    e.query(
        &no_tenant(),
        &parse(PANEL).expect("parse"),
        &range(end - 15 * HOUR, end, 2 * MIN),
    )
    .await
    .expect("the panel");
    let sent = statements(&built.admin, &comment).await;
    let brief: Vec<String> = sent
        .iter()
        .map(|(q, rows)| format!("{rows} rows: {}", q.lines().next().unwrap_or("")))
        .collect();
    built.finish().await;
    assert!(
        !sent
            .iter()
            .any(|(q, _)| q.starts_with("SELECT toUInt32(1) AS shard, kind, fingerprint, t_end")),
        "a level fetch was sent: {brief:#?}"
    );
    assert!(
        sent.iter()
            .any(|(q, _)| q.starts_with("SELECT toUInt32(1) AS shard, t_last, t_end")),
        "no coverage statement: {brief:#?}"
    );
    let data: Vec<&(String, u64)> = sent
        .iter()
        .filter(|(q, _)| q.starts_with("WITH "))
        .collect();
    assert_eq!(data.len(), 1, "one data statement: {brief:#?}");
    assert!(
        data[0].1 <= 4 * 451 + 1,
        "the data statement returned {} rows: {brief:#?}",
        data[0].1
    );
}

/// **T3 (issue #579 part 4): a vector match under an aggregate answers as
/// in process.** Every operator part 4 pushes, under every aggregation,
/// both sides at the level; one side `irate`, read raw, on the left and on
/// the right. Corpus P expresses e11: in process, a filtering comparison
/// keeps `__unit__` to the end and `+` drops it.
#[tokio::test(flavor = "multi_thread")]
async fn a_vector_match_under_an_aggregate_answers_as_in_process() {
    skip_unless_live!();
    let (built, end) = P
        .lease("a_vector_match_under_an_aggregate_answers_as_in_process")
        .await;
    let on = built.pushed_engine(None).await;
    let off = built.engine(true, None).await;
    let p = range(end - 15 * HOUR, end, 2 * MIN);
    let mut failures = Vec::new();
    let mut ops: Vec<String> = ["+", "-", "*", "/", "==", "!=", ">", "<", ">=", "<="]
        .iter()
        .map(|o| (*o).to_string())
        .collect();
    for o in ["==", "!=", ">", "<", ">=", "<="] {
        ops.push(format!("{o} bool"));
    }
    for agg in ["sum", "avg", "count", "min", "max"] {
        for g in ["by (instance) ", "without (device) "] {
            for op in &ops {
                let q = format!("{agg} {g}(rate(p_rx_total[5m]) {op} rate(p_tx_total[5m]))");
                let (a, b, f) = pushed_against_in_process(&on, &off, &q, &p).await;
                failures.extend(f);
                if b.stages("rollup_route").len() != 2 {
                    failures.push(format!("{q}: in process, both sides do not read the level"));
                }
                if a.pushed()
                    .iter()
                    .any(|s| s.matches("FROM metric_samples_60s\n").count() != 2)
                {
                    failures.push(format!(
                        "{q}: the pushed statement does not read the level twice"
                    ));
                }
            }
        }
    }
    for q in [
        "sum(irate(p_rx_total[5m]) + rate(p_tx_total[5m]))",
        "sum(rate(p_rx_total[5m]) + irate(p_tx_total[5m]))",
    ] {
        failures.extend(pushed_against_in_process(&on, &off, q, &p).await.2);
    }
    let unit = |a: &Answer| {
        a.cells()
            .keys()
            .any(|(l, _)| l.contains("(\"__unit__\", \"bytes\")"))
    };
    let t = no_tenant();
    let gt = ask(
        &off,
        &t,
        "sum without (device) (rate(p_rx_total[5m]) > rate(p_tx_total[5m]))",
        &p,
    )
    .await;
    let plus = ask(
        &off,
        &t,
        "sum without (device) (rate(p_rx_total[5m]) + rate(p_tx_total[5m]))",
        &p,
    )
    .await;
    built.finish().await;
    assert!(unit(&gt), "e11: `>` keeps __unit__ in process");
    assert!(!unit(&plus), "e11: `+` drops __unit__ in process");
    assert!(failures.is_empty(), "T3:\n{}", failures.join("\n"));
}

/// **T4 (issue #579 part 4): the shapes part 4 does not push take today's
/// route**, name their reason where section 4 names one, and answer as in
/// process. Issue #18 part 2b: the week's three, filled at 7200 s, are
/// pushed now (T30 (c)), so they are not here.
#[tokio::test(flavor = "multi_thread")]
async fn unpushed_shapes_take_todays_route() {
    skip_unless_live!();
    let (built, end) = P.lease("unpushed_shapes_take_todays_route").await;
    let db = &built.db().to_string();
    // `fill` is an experimental modifier.
    let engine = |push: bool| {
        let mut cfg = engine_config(db, push);
        cfg.experimental_functions = true;
        cfg
    };
    let on = MetricsEngine::new(built.client().await, Arc::clone(&built.cache), engine(true))
        .with_rollup_levels(Arc::clone(&built.slot));
    let off = MetricsEngine::new(
        built.client().await,
        Arc::clone(&built.cache),
        engine(false),
    )
    .with_rollup_levels(Arc::clone(&built.slot));
    let day = range(end - 15 * HOUR, end, 2 * MIN);
    let cases: [(&str, &MetricQueryParams, Option<&str>); 11] = [
        (
            "sum(rate(p_rx_total[5m]) + on (instance, device) rate(p_tx_total[5m]))",
            &day,
            Some("Matching"),
        ),
        (
            "sum(rate(p_rx_total[5m]) + ignoring (__unit__) rate(p_tx_total[5m]))",
            &day,
            Some("Matching"),
        ),
        (
            "sum(rate(p_rx_total[5m]) * on (instance, device) group_left rate(p_tx_total[5m]))",
            &day,
            Some("Matching"),
        ),
        (
            "sum(rate(p_rx_total[5m]) * on (instance, device) group_right rate(p_tx_total[5m]))",
            &day,
            Some("Matching"),
        ),
        (
            "sum(rate(p_rx_total[5m]) % rate(p_tx_total[5m]))",
            &day,
            Some("Operator"),
        ),
        (
            "sum(rate(p_rx_total[5m]) ^ rate(p_tx_total[5m]))",
            &day,
            Some("Operator"),
        ),
        (
            "sum(rate(p_rx_total[5m]) atan2 rate(p_tx_total[5m]))",
            &day,
            Some("Operator"),
        ),
        (
            "sum(rate(p_rx_total[5m]) + fill(0) rate(p_tx_total[5m]))",
            &day,
            Some("Matching"),
        ),
        ("rate(p_rx_total[5m]) + rate(p_tx_total[5m])", &day, None),
        (
            "sum(rate({__name__=~\"p_r._total\"}[5m]) + rate(p_tx_total[5m]))",
            &day,
            Some("Selector"),
        ),
        (
            "sum(rate(p_rx_total[5m] offset 1m) + rate(p_tx_total[5m]))",
            &day,
            Some("Selector"),
        ),
    ];
    let mut failures = Vec::new();
    let t = no_tenant();
    for (q, p, reason) in cases {
        let (a, b) = futures::join!(ask(&on, &t, q, p), ask(&off, &t, q, p));
        if !a.pushed().is_empty() {
            failures.push(format!("{q}: pushed {:?}", a.pushed()));
        }
        if let Some(r) = reason
            && !a.declines().contains(&format!("declined: {r}").as_str())
        {
            failures.push(format!("{q}: declines {:?}, want {r}", a.declines()));
        }
        if vector_or_matrix(&a) != vector_or_matrix(&b) {
            failures.push(format!("{q}: answers differ"));
        }
        if vector_or_matrix(&b).is_empty() {
            failures.push(format!("{q}: empty"));
        }
    }
    built.finish().await;
    assert!(failures.is_empty(), "T4:\n{}", failures.join("\n"));
}

/// **T5 (issue #579 part 4): a histogram sample on one side falls back.**
#[tokio::test(flavor = "multi_thread")]
async fn a_histogram_side_falls_back() {
    skip_unless_live!();
    let end = p_end();
    let db = DB_P5.name();
    let built = corpus_p(db, end).await;
    exec(
        &built.admin,
        &format!(
            "INSERT INTO {db}.metric_hist_samples (org_id, fingerprint, unix_milli, schema, zero_threshold, zero_count, \
                                                   count, sum, pos_span_offsets, pos_span_lengths, pos_bucket_deltas, \
                                                   neg_span_offsets, neg_span_lengths, neg_bucket_deltas, custom_values) \
             SELECT '', fp, {}, 0, 0., 0, 4, 5., [0], [3], [1, 1, -1], [], [], [], [] \
             FROM {db}.rp_series WHERE key = 'txi0d0'",
            end - 2 * HOUR - 7_000
        ),
    )
    .await;
    let on = built.pushed_engine(None).await;
    let off = built.engine(true, None).await;
    let p = range(end - 15 * HOUR, end, 2 * MIN);
    let t = no_tenant();
    let (a, b) = futures::join!(ask(&on, &t, PANEL, &p), ask(&off, &t, PANEL, &p));
    built.finish().await;
    assert!(
        a.declines().contains(&"declined: HistogramSamples"),
        "declines {:?}",
        a.declines()
    );
    assert!(!b.cells().is_empty());
    assert_eq!(a.cells(), b.cells(), "the fallback answers as in process");
}

/// **T6 (issue #579 part 4): a node split by time reads its whole prefix**
/// in every chunk: at a cap of 8,192 series-steps the panel is 3
/// statements and `sum` over one metric 2, and e12's reset 4 minutes 30 seconds
/// before the panel's second chunk counts in both.
#[tokio::test(flavor = "multi_thread")]
async fn a_split_level_node_reads_its_prefix() {
    skip_unless_live!();
    let (built, end) = P.lease("a_split_level_node_reads_its_prefix").await;
    let db = &built.db().to_string();
    // Unique to this run: the log keeps every earlier run's statements
    // under the same database name.
    let comment = format!("{db}_split_{}", now_ms());
    let on = built
        .pushed_engine(Some(&comment))
        .await
        .with_pushed_series_steps_cap(8_192);
    let off = built.engine(true, None).await;
    let p = range(end - 15 * HOUR, end, 2 * MIN);
    let mut failures = Vec::new();
    for (q, more) in [
        (PANEL, "(+2 more statements like this one, split by time)"),
        (
            "sum(rate(p_rx_total[2m15s]))",
            "(+1 more statements like this one, split by time)",
        ),
    ] {
        let (a, _, f) = pushed_against_in_process(&on, &off, q, &p).await;
        failures.extend(f);
        let notes: Vec<String> = a
            .explain
            .stages
            .iter()
            .filter(|s| s.name == "pushed_aggregate" && !s.sql.starts_with("declined:"))
            .map(|s| s.note.clone().unwrap_or_default())
            .collect();
        if notes != [more] {
            failures.push(format!("{q}: notes {notes:?}, want {more}"));
        }
    }
    // The panel's chunks start where e12 was placed for: 46 series, 8,192 /
    // 46 = 178 steps a chunk, the second at end − 15 h + 356 min.
    let mut starts: Vec<i64> = statements(&built.admin, &comment)
        .await
        .iter()
        // The panel's statements: the join's pairing names the left
        // member's rank (a known side's members carry no metric name).
        .filter(|(q, _)| q.starts_with("WITH ") && q.contains("p.rank AS rank, l.gi AS gi"))
        .filter_map(|(q, _)| {
            q.strip_prefix("WITH ")?
                .split_once(" AS grid_start")?
                .0
                .parse()
                .ok()
        })
        .collect();
    starts.sort_unstable();
    built.finish().await;
    assert!(failures.is_empty(), "T6:\n{}", failures.join("\n"));
    assert_eq!(
        starts.get(1).copied(),
        Some(end - 15 * HOUR + 356 * MIN),
        "the panel's second chunk starts at end − 15 h + 356 min: {starts:?}"
    );
}

// ------------------------------------------ issue #18 part 2a, corpus P2

/// Corpus P2 (issue #18 part 2a, plan section 11): corpus P with its `p2`
/// series, built at the default levels to `end − 10 min`, with P's 60 s
/// cut over `(end − 6 h, end − 5 h 30 m]`, a 720 s cut over `(end − 20 h,
/// end − 19 h]` that the 60 s level fills, and both cut over `(end − 14 h,
/// end − 13 h 36 m]`, which raw fills inside one counter.
async fn corpus_p2(db: &str, end: i64) -> Built {
    corpus_p2_with(db, end, Vec::new()).await
}

/// [`corpus_p2`] with `more` landed beside it, before the build.
async fn corpus_p2_with(db: &str, end: i64, more: Vec<String>) -> Built {
    let mut land = corpus_p_land(db, end, true);
    land.extend(more);
    let built = build(db, DEFAULTS, &land, end - 50 * HOUR, end - 10 * MIN, &[]).await;
    let admin = &built.admin;
    cut_runs(admin, db, 60, end - 6 * HOUR, end - 5 * HOUR - 30 * MIN).await;
    cut_runs(admin, db, 720, end - 20 * HOUR, end - 19 * HOUR).await;
    for r in [720, 60] {
        cut_runs(admin, db, r, end - 14 * HOUR, end - 13 * HOUR - 36 * MIN).await;
    }
    built
}

/// **T20 (issue #18 part 2a): an aggregate over the 720 s level answers as
/// in process.** Shape A over every aggregation, `rate` and `increase`, three
/// groupings and three ranges over 24 h at 12 min and two over 40 h at 24
/// min; shape C under `sum` and `max` over 24 h. In process, the 24 h
/// windows read the 720 s level, the 60 s level and raw.
#[tokio::test(flavor = "multi_thread")]
async fn a_coarse_level_aggregate_answers_as_in_process() {
    skip_unless_live!();
    let (built, end) = P2
        .lease("a_coarse_level_aggregate_answers_as_in_process")
        .await;
    let on = built.pushed_engine(None).await;
    let off = built.engine(true, None).await;
    let w24 = range(end - DAY, end, 12 * MIN);
    let w40 = range(end - 40 * HOUR, end, 24 * MIN);
    let cases = [
        ("12m", &w24, true),
        ("1h", &w24, true),
        ("2m15s", &w24, true),
        ("24m", &w40, false),
        ("2h", &w40, false),
    ];
    let mut failures = Vec::new();
    let check = |q: &str, b: &Answer, w24: bool, failures: &mut Vec<String>| {
        let notes = b.stages("rollup_piece");
        let names = |prefix: &str| notes.iter().any(|n| n.starts_with(prefix));
        if w24 && !(names("metric_samples_720s.") && names("metric_samples_60s.") && names("raw ("))
        {
            failures.push(format!(
                "{q}: in process, the 24 h window does not read 720 s, 60 s and raw: {notes:?}"
            ));
        }
    };
    for agg in ["sum", "avg", "count", "min", "max"] {
        for f in ["rate", "increase"] {
            for g in ["by (instance) ", "without (device) ", ""] {
                for (r, p, is24) in &cases {
                    let q = format!("{agg} {g}({f}(p_rx_total[{r}]))");
                    let (_, b, f) = pushed_against_in_process(&on, &off, &q, p).await;
                    failures.extend(f);
                    check(&q, &b, *is24, &mut failures);
                }
            }
        }
    }
    for agg in ["sum", "max"] {
        for g in ["by (instance) ", "without (device) "] {
            for op in ["+", "/", ">", "> bool"] {
                let q = format!("{agg} {g}(rate(p_rx_total[12m]) {op} rate(p_tx_total[12m]))");
                let (_, b, f) = pushed_against_in_process(&on, &off, &q, &w24).await;
                failures.extend(f);
                check(&q, &b, true, &mut failures);
            }
        }
    }
    built.finish().await;
    assert!(failures.is_empty(), "T20:\n{}", failures.join("\n"));
}

/// **T21 (issue #18 part 2a): a week reads the two-hour level pushed.**
#[tokio::test(flavor = "multi_thread")]
async fn a_week_reads_the_two_hour_level_pushed() {
    skip_unless_live!();
    let (built, end) = P2.lease("a_week_reads_the_two_hour_level_pushed").await;
    let on = built.pushed_engine(None).await;
    let off = built.engine(true, None).await;
    let e2h = end.div_euclid(2 * HOUR) * 2 * HOUR;
    let wk = range(e2h - 7 * DAY, e2h, 2 * HOUR);
    let mut failures = Vec::new();
    for q in [
        "sum by (instance) (rate(p_rx_total[2h]))",
        "sum by (instance) (increase(p_rx_total[2h]))",
        "sum by (instance) (rate(p_rx_total[2h]) / rate(p_tx_total[2h]))",
    ] {
        let (a, _, f) = pushed_against_in_process(&on, &off, q, &wk).await;
        failures.extend(f);
        if !a
            .pushed()
            .iter()
            .any(|s| s.contains("FROM metric_samples_7200s\n"))
        {
            failures.push(format!(
                "{q}: the pushed statement does not read the 7200 s level"
            ));
        }
    }
    built.finish().await;
    assert!(failures.is_empty(), "T21:\n{}", failures.join("\n"));
}

/// **T22 (issue #18 part 2a): a 60 s level over a configured 5 s level
/// answers as in process.** Corpus P at the five levels, a 60 s cut that
/// the 5 s level fills: over 6 h at 1 m the 60 s route is pushed and reads
/// both levels; over 30 min at 15 s the 5 s route, pushed before, still
/// answers as in process.
#[tokio::test(flavor = "multi_thread")]
async fn a_level_over_a_finer_level_answers_as_in_process() {
    skip_unless_live!();
    let end = p_end();
    let db = DB_P22.name();
    let built = build(
        db,
        FIVE,
        &corpus_p_land(db, end, false),
        end - 50 * HOUR,
        end - 10 * MIN,
        &[],
    )
    .await;
    cut_runs(
        &built.admin,
        db,
        60,
        end - 4 * HOUR,
        end - 3 * HOUR - 30 * MIN,
    )
    .await;
    let on = built.pushed_engine(None).await;
    let off = built.engine(true, None).await;
    let queries = [
        "sum by (instance) (rate(p_rx_total[5m]))",
        "avg without (device) (increase(p_rx_total[2m15s]))",
        "sum(rate(p_rx_total[5m]) - rate(p_tx_total[5m]))",
    ];
    let mut failures = Vec::new();
    let six = range(end - 6 * HOUR, end, MIN);
    for q in queries {
        let (a, _, f) = pushed_against_in_process(&on, &off, q, &six).await;
        failures.extend(f);
        for table in ["metric_samples_60s", "metric_samples_5s"] {
            if !a
                .pushed()
                .iter()
                .any(|s| s.contains(&format!("FROM {table}\n")))
            {
                failures.push(format!(
                    "{q} over 6 h: the pushed statement does not read {table}"
                ));
            }
        }
    }
    let half = range(end - 30 * MIN, end, 15_000);
    for q in queries {
        failures.extend(pushed_against_in_process(&on, &off, q, &half).await.2);
    }
    built.finish().await;
    assert!(failures.is_empty(), "T22:\n{}", failures.join("\n"));
}

// ------------------------------------------ issue #18 part 2b, filled

/// Every pushed statement of `a` on the grid `start AS grid_start, step AS
/// grid_step`.
fn on_grid(a: &Answer, start: Option<i64>, step: i64) -> Vec<&str> {
    let grid = match start {
        Some(s) => format!("{s} AS grid_start, {step} AS grid_step"),
        None => format!(" AS grid_start, {step} AS grid_step"),
    };
    a.pushed()
        .into_iter()
        .filter(|s| s.contains(&grid))
        .collect()
}

/// A native-histogram sample on rx `{i0,d0}` at `t`.
async fn one_histogram(built: &Built, t: i64) {
    let db = built.db();
    exec(
        &built.admin,
        &format!(
            "INSERT INTO {db}.metric_hist_samples (org_id, fingerprint, unix_milli, schema, zero_threshold, zero_count, \
                                                   count, sum, pos_span_offsets, pos_span_lengths, pos_bucket_deltas, \
                                                   neg_span_offsets, neg_span_lengths, neg_bucket_deltas, custom_values) \
             SELECT '', fp, {t}, 0, 0., 0, 4, 5., [0], [3], [1, 1, -1], [], [], [], [] \
             FROM {db}.rp_series WHERE key = 'rxi0d0'"
        ),
    )
    .await;
}

/// **T30 (issue #18 part 2b): a filled request pushes both reads.** (a)
/// 6 h at 30 s off the minute: the bucket ends' read at 60 s and the
/// newest step's read at 30 s, both pushed; (b) a day at 2 m, filled from
/// 720 s, no newest step; (c) a week at 1 h from 7200 s; (d) shape C over a
/// day.
#[tokio::test(flavor = "multi_thread")]
async fn a_filled_request_pushes_both_reads() {
    skip_unless_live!();
    let (built, end) = P.lease("a_filled_request_pushes_both_reads").await;
    let on = built.pushed_engine(None).await;
    let off = built.engine(true, None).await;
    let t = no_tenant();
    let mut failures = Vec::new();
    let cases: [(&str, &str, MetricQueryParams); 4] = [
        (
            "(a)",
            "sum by (instance) (rate(p_rx_total[1m]))",
            range(end - 6 * HOUR + 30_000, end - 30_000, 30_000),
        ),
        (
            "(b)",
            "sum by (instance) (rate(p_rx_total[2m15s]))",
            range(end - DAY, end, 2 * MIN),
        ),
        (
            "(c)",
            "sum by (instance) (rate(p_rx_total[5m]))",
            range(end - 7 * DAY, end, HOUR),
        ),
        (
            "(d)",
            "sum by (instance) (rate(p_rx_total[2m15s]) + rate(p_tx_total[2m15s]))",
            range(end - DAY, end, 2 * MIN),
        ),
    ];
    for (case, q, p) in &cases {
        let (a, b) = futures::join!(ask(&on, &t, q, p), ask(&off, &t, q, p));
        if !a.declines().is_empty() {
            failures.push(format!("{case} {q}: declines {:?}", a.declines()));
        }
        if vector_or_matrix(&b).is_empty() {
            failures.push(format!("{case} {q}: in process, empty"));
        }
        if vector_or_matrix(&a) != vector_or_matrix(&b) {
            failures.push(format!("{case} {q}: cells differ from push off"));
        }
        let pushed = a.pushed();
        match *case {
            "(a)" => {
                let first = -(-p.start_ms).div_euclid(MIN) * MIN;
                if pushed.len() != 2
                    || on_grid(&a, Some(first), 60_000).len() != 1
                    || on_grid(&a, Some(end - 30_000), 30_000).len() != 1
                {
                    failures.push(format!(
                        "(a): pushed statements {}, want one on ({first}, 60000) and one on ({}, 30000)",
                        pushed.len(),
                        end - 30_000
                    ));
                }
                let times = times_of(p);
                for (l, ts) in &per_series(&a) {
                    if ts != &times {
                        failures.push(format!("(a) {l}: {} of {} times", ts.len(), times.len()));
                    }
                }
            }
            "(b)" => {
                if pushed.len() != 1
                    || !pushed[0].contains("FROM metric_samples_720s\n")
                    || on_grid(&a, None, 720_000).len() != 1
                {
                    failures.push(format!(
                        "(b): {} pushed statements, want one at 720 s",
                        pushed.len()
                    ));
                }
            }
            "(c)" => {
                // The bucket ends' read at 7200 s and, when `end` is not on
                // a 2-hour bucket end, the newest step's read at 1 h.
                let level = on_grid(&a, None, 7_200_000);
                let newest = usize::from(end.rem_euclid(2 * HOUR) != 0);
                if level.len() != 1
                    || !level[0].contains("FROM metric_samples_7200s\n")
                    || on_grid(&a, Some(end), HOUR).len() != newest
                    || pushed.len() != 1 + newest
                {
                    failures.push(format!(
                        "(c): {} pushed statements, want one at 7200 s and {newest} at 1 h",
                        pushed.len()
                    ));
                }
            }
            _ => {
                if pushed.is_empty() {
                    failures.push("(d): not pushed".to_string());
                }
            }
        }
    }
    built.finish().await;
    assert!(failures.is_empty(), "T30:\n{}", failures.join("\n"));
}

/// **T31 (issue #18 part 2b): a filled request mixes a pushed read and an
/// evaluated one.** A histogram sample inside the newest step's read only:
/// the bucket ends' read pushes, the newest step's read declines.
#[tokio::test(flavor = "multi_thread")]
async fn a_filled_request_mixes_a_pushed_and_an_evaluated_read() {
    skip_unless_live!();
    let end = p_end();
    let built = corpus_p(DB_P31.name(), end).await;
    one_histogram(&built, end - 40_000).await;
    let on = built.pushed_engine(None).await;
    let off = built.engine(true, None).await;
    let t = no_tenant();
    let q = "sum by (instance) (rate(p_rx_total[1m]))";
    let p = range(end - 6 * HOUR + 30_000, end - 30_000, 30_000);
    let (a, b) = futures::join!(ask(&on, &t, q, &p), ask(&off, &t, q, &p));
    let mut failures = Vec::new();
    let first = -(-p.start_ms).div_euclid(MIN) * MIN;
    if on_grid(&a, Some(first), 60_000).len() != 1 {
        failures.push(format!(
            "the bucket ends' read is not pushed: {:?}",
            a.pushed().len()
        ));
    }
    if a.declines() != ["declined: HistogramSamples"] {
        failures.push(format!("declines {:?}", a.declines()));
    }
    if vector_or_matrix(&b).is_empty() {
        failures.push("in process, empty".to_string());
    }
    if vector_or_matrix(&a) != vector_or_matrix(&b) {
        failures.push("cells differ from push off".to_string());
    }
    built.finish().await;
    assert!(failures.is_empty(), "T31:\n{}", failures.join("\n"));
}

/// **T33 (issue #18 part 2b): a filled request pushes only its second
/// read.** A histogram sample inside the bucket ends' read only: that read
/// declines and fetches its levels, the newest step's read pushes.
#[tokio::test(flavor = "multi_thread")]
async fn a_filled_request_pushes_only_its_second_read() {
    skip_unless_live!();
    let end = p_end();
    let built = corpus_p(DB_P33.name(), end).await;
    one_histogram(&built, end - 21 * MIN).await;
    let on = built.pushed_engine(None).await;
    let off = built.engine(true, None).await;
    let t = no_tenant();
    let q = "sum by (instance) (rate(p_rx_total[1m]))";
    let p = range(end - 6 * HOUR + 30_000, end - 30_000, 30_000);
    let (a, b) = futures::join!(ask(&on, &t, q, &p), ask(&off, &t, q, &p));
    let mut failures = Vec::new();
    if a.declines() != ["declined: HistogramSamples"] {
        failures.push(format!("declines {:?}", a.declines()));
    }
    if a.stages("rollup_piece").is_empty() {
        failures.push("the bucket ends' read fetched no level".to_string());
    }
    if on_grid(&a, Some(end - 30_000), 30_000).len() != 1 {
        failures.push(format!(
            "the second read is not pushed: {} pushed",
            a.pushed().len()
        ));
    }
    if vector_or_matrix(&b).is_empty() {
        failures.push("in process, empty".to_string());
    }
    if vector_or_matrix(&a) != vector_or_matrix(&b) {
        failures.push("cells differ from push off".to_string());
    }
    built.finish().await;
    assert!(failures.is_empty(), "T33:\n{}", failures.join("\n"));
}

// ------------------------------------------- issue #18 part 2c, T60-T65

/// One logged statement: its text, `read_rows`, the tables it read and
/// its replica delay setting.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct LoggedRead {
    query: String,
    read_rows: u64,
    tables: Vec<String>,
    delay: String,
}

/// Every statement that finished under `comment`, in start order.
async fn logged(admin: &ChClient, comment: &str) -> Vec<LoggedRead> {
    settle_query_log(&[admin], &format!("log_comment = '{comment}'"), 1).await;
    let mut stream = admin
        .query_stream::<LoggedRead>(
            &format!(
                "SELECT query, toUInt64(read_rows) AS read_rows, tables, \
                        Settings['max_replica_delay_for_distributed_queries'] AS delay \
                 FROM system.query_log \
                 WHERE type = 'QueryFinish' AND log_comment = '{comment}' \
                 ORDER BY query_start_time_microseconds"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("query_log");
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.expect("decode"));
    }
    out
}

/// `query` on `engine` without explain: its cells.
async fn quiet_cells(
    engine: &MetricsEngine,
    query: &str,
    p: &MetricQueryParams,
    explain_of: &Answer,
) -> BTreeMap<(String, i64), String> {
    let (result, _) = engine
        .query(&no_tenant(), &parse(query).expect("parse"), p)
        .await
        .unwrap_or_else(|e| panic!("{query}: {e:?}"));
    // Only the result is read; the explain is any answer's.
    vector_or_matrix(&Answer {
        result,
        explain: explain_of.explain.clone(),
    })
}

/// The 2c parity queries: `(name, query, window)`.
fn t60_queries(end: i64) -> Vec<(&'static str, &'static str, MetricQueryParams)> {
    let w24 = range(end - DAY, end, 12 * MIN);
    vec![
        ("a24", "sum by (instance) (rate(p_rx_total[12m]))", w24),
        (
            "c24",
            "sum by (instance) (rate(p_rx_total[12m]) + rate(p_tx_total[12m]))",
            w24,
        ),
        (
            "a6",
            "sum by (instance) (rate(p_rx_total[12m]))",
            range(end - 6 * HOUR, end, MIN),
        ),
    ]
}

/// A coverage statement's first line, single node.
const COVERAGE: &str = "SELECT toUInt32(1) AS shard, t_last, t_end";

/// The engine's default lookback, which every fetch window subtracts.
const LOOKBACK: i64 = 5 * MIN;

/// The intervals a coverage statement names, `t_end > a AND t_last < b`
/// each, in order.
fn t60_intervals(sql: &str) -> Vec<(i64, i64)> {
    let one = regex::Regex::new(r"t_end > (-?\d+) AND t_last < (-?\d+)").expect("regex");
    one.captures_iter(sql)
        .map(|c| (c[1].parse().expect("a"), c[2].parse().expect("b")))
        .collect()
}

/// One run row of a level table, as T60 (e) reads it.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct Run {
    shard: u32,
    t_last: i64,
    t_end: i64,
}

/// T60 (e): what the 720 s level leaves of `window`, `(lo, hi]`, computed
/// here. Its run rows overlapping the window, by shard: each shard's
/// union, the intersection over shards, and the window less that.
async fn t60_uncovered(admin: &ChClient, db: &str, window: (i64, i64)) -> Vec<(i64, i64)> {
    let (lo, hi) = window;
    let mut stream = admin
        .query_stream::<Run>(
            &format!(
                "SELECT toUInt32(1) AS shard, t_last, t_end FROM {db}.metric_samples_720s \
                 WHERE kind = 1 AND org_id = '' AND fingerprint = 0 AND t_end > {lo} AND t_last < {hi}"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("run rows");
    let mut by_shard: BTreeMap<u32, Vec<(i64, i64)>> = BTreeMap::new();
    while let Some(row) = stream.next().await {
        let r = row.expect("decode");
        by_shard
            .entry(r.shard)
            .or_default()
            .push((r.t_last, r.t_end));
    }
    let merge = |mut v: Vec<(i64, i64)>| -> Vec<(i64, i64)> {
        v.sort_unstable();
        let mut out: Vec<(i64, i64)> = Vec::new();
        for (a, b) in v {
            match out.last_mut() {
                Some(l) if a <= l.1 => l.1 = l.1.max(b),
                _ => out.push((a, b)),
            }
        }
        out
    };
    let mut covered: Option<Vec<(i64, i64)>> = None;
    for runs in by_shard.into_values() {
        let mine = merge(runs);
        covered = Some(match covered {
            None => mine,
            Some(acc) => {
                let mut both = Vec::new();
                for &(a, b) in &acc {
                    for &(c, d) in &mine {
                        let (x, y) = (a.max(c), b.min(d));
                        if x < y {
                            both.push((x, y));
                        }
                    }
                }
                merge(both)
            }
        });
    }
    let mut left = Vec::new();
    let mut at = lo;
    for (a, b) in covered.unwrap_or_default() {
        if b <= at || a >= hi {
            continue;
        }
        if a > at {
            left.push((at, a));
        }
        at = at.max(b);
    }
    if at < hi {
        left.push((at, hi));
    }
    left
}

/// **T60 (issue #18 part 2c): a pushed node reads no more than in
/// process.** Corpus P2, the cache warm: no pushed statement reads the
/// series tables, the data statements read at most the in-process level
/// and raw fetches plus three a statement (`system.one`), no coverage
/// statement names a series, and the 60 s coverage is sent over exactly
/// what the 720 s level leaves (plan r12 to r14).
#[tokio::test(flavor = "multi_thread")]
async fn a_pushed_node_reads_no_more_than_in_process() {
    skip_unless_live!();
    let (built, end) = P2
        .lease("a_pushed_node_reads_no_more_than_in_process")
        .await;
    let db = &built.db().to_string();
    let probe = ask(
        &built.engine(true, None).await,
        &no_tenant(),
        "sum(rate(p_rx_total[12m]))",
        &range(end - DAY, end, 12 * MIN),
    )
    .await;
    let series_in = regex::Regex::new(r"fingerprint\s+(GLOBAL\s+)?IN").expect("regex");
    let fp_eq = regex::Regex::new(r"fingerprint\s*=\s*(\S+)").expect("regex");
    let mut failures = Vec::new();
    for (name, q, p) in t60_queries(end) {
        let stamp = now_ms();
        let (c_on, c_off) = (
            format!("{db}_t60{name}_on_{stamp}"),
            format!("{db}_t60{name}_off_{stamp}"),
        );
        let on = built.pushed_engine(Some(&c_on)).await;
        let off = built.engine(true, Some(&c_off)).await;
        let (a, b) = (
            quiet_cells(&on, q, &p, &probe).await,
            quiet_cells(&off, q, &p, &probe).await,
        );
        if a != b || b.is_empty() {
            failures.push(format!(
                "{name} {q}: (c) {} cells pushed, {} in process",
                a.len(),
                b.len()
            ));
        }
        let (son, soff) = (
            logged(&built.admin, &c_on).await,
            logged(&built.admin, &c_off).await,
        );
        for r in &son {
            if r.tables
                .iter()
                .any(|t| t == &format!("{db}.metric_series") || t == &format!("{db}.metric_labels"))
            {
                failures.push(format!(
                    "{name}: (a) a pushed statement reads {:?}: {}",
                    r.tables,
                    r.query.lines().next().unwrap_or("")
                ));
            }
        }
        // (b1), plan r12: the data statements read no more than the
        // in-process level and raw fetches, plus three a statement.
        let data: Vec<&LoggedRead> = son
            .iter()
            .filter(|r| r.query.starts_with("WITH "))
            .collect();
        let with = data.len() as u64;
        let fetches: Vec<&LoggedRead> = soff
            .iter()
            .filter(|r| {
                r.query
                    .starts_with("SELECT toUInt32(1) AS shard, kind, fingerprint")
                    || r.tables
                        .iter()
                        .any(|t| t == &format!("{db}.metric_samples"))
            })
            .collect();
        let (ron, roff) = (
            data.iter().map(|r| r.read_rows).sum::<u64>(),
            fetches.iter().map(|r| r.read_rows).sum::<u64>(),
        );
        eprintln!(
            "T60 {name}: pushed data {ron} rows read over {with} statements; in process {roff} over {} fetches; \
             pushed all {} rows over {} statements, in process all {} over {}",
            fetches.len(),
            son.iter().map(|r| r.read_rows).sum::<u64>(),
            son.len(),
            soff.iter().map(|r| r.read_rows).sum::<u64>(),
            soff.len()
        );
        if with == 0 || fetches.is_empty() || ron > roff + 3 * with {
            failures.push(format!(
                "{name}: (b1) pushed data statements read {ron} rows, in process fetches {roff}, \
                 {with} data statements, {} fetches",
                fetches.len()
            ));
        }
        // (b2), plan r13 and r14: no coverage statement names a series.
        let covering: Vec<&LoggedRead> = son
            .iter()
            .filter(|r| r.query.starts_with(COVERAGE))
            .collect();
        if covering.is_empty() {
            failures.push(format!("{name}: (b2) no coverage statement logged"));
        }
        for r in &covering {
            let named = series_in.is_match(&r.query)
                || r.query.contains("FROM metric_series")
                || r.query.contains("FROM metric_labels")
                || fp_eq.captures_iter(&r.query).any(|c| &c[1] != "0");
            if named {
                failures.push(format!(
                    "{name}: (b2) a coverage statement names series: {}",
                    r.query
                ));
            }
        }
        // (e), plan r12: the 60 s statement is sent exactly when the 720 s
        // level leaves part of the window, over exactly what it leaves.
        if name != "a6" {
            let window = (p.start_ms - 12 * MIN - LOOKBACK, p.end_ms);
            let left = t60_uncovered(&built.admin, db, window).await;
            let sixty: Vec<Vec<(i64, i64)>> = covering
                .iter()
                .filter(|r| r.query.contains("FROM metric_samples_60s\n"))
                .map(|r| t60_intervals(&r.query))
                .collect();
            let want: Vec<Vec<(i64, i64)>> = if left.is_empty() {
                vec![]
            } else {
                vec![left.clone()]
            };
            eprintln!(
                "T60 {name}: (e) the 720 s level leaves {left:?}; 60 s statements over {sixty:?}"
            );
            if sixty != want {
                failures.push(format!(
                    "{name}: (e) 60 s coverage statements over {sixty:?}, the 720 s level leaves {left:?}"
                ));
            }
        }
        if name != "c24" {
            continue;
        }
        // (d), issue #18 part 2c r10 and r11: the two sides share each
        // level's coverage statement — one a level the in-process route
        // reads — and explain shows one stage for each statement sent.
        let coverage = |rows: &[LoggedRead]| -> Vec<String> {
            rows.iter()
                .filter(|r| r.query.starts_with(COVERAGE))
                .map(|r| r.query.clone())
                .collect()
        };
        let sent = coverage(&son);
        let distinct: std::collections::BTreeSet<&String> = sent.iter().collect();
        let explained_off = ask(&built.engine(true, None).await, &no_tenant(), q, &p).await;
        let levels = served_tables(&explained_off);
        if sent.len() != levels.len() || distinct.len() != sent.len() {
            failures.push(format!(
                "(d) {} coverage statements ({} distinct) for {} levels {levels:?}",
                sent.len(),
                distinct.len(),
                levels.len()
            ));
        }
        let c_explain = format!("{db}_t60{name}_explain_{stamp}");
        let explained = ask(
            &built.pushed_engine(Some(&c_explain)).await,
            &no_tenant(),
            q,
            &p,
        )
        .await;
        let stages: Vec<String> = explained
            .explain
            .stages
            .iter()
            .filter(|s| s.name == "pushed_coverage")
            .map(|s| s.sql.clone())
            .collect();
        let logged_coverage = coverage(&logged(&built.admin, &c_explain).await);
        let logged_distinct: std::collections::BTreeSet<&String> = logged_coverage.iter().collect();
        if stages != logged_coverage || logged_distinct.len() != logged_coverage.len() {
            failures.push(format!(
                "(d) {} pushed_coverage stages against {} coverage statements logged ({} distinct)",
                stages.len(),
                logged_coverage.len(),
                logged_distinct.len()
            ));
        }
    }
    built.finish().await;
    assert!(failures.is_empty(), "T60:\n{}", failures.join("\n"));
}

/// **T61 (issue #18 part 2c): a side the cache cannot answer keeps its
/// series statement.**
#[tokio::test(flavor = "multi_thread")]
async fn a_side_the_cache_cannot_answer_keeps_its_series_statement() {
    skip_unless_live!();
    let (built, end) = P2
        .lease("a_side_the_cache_cannot_answer_keeps_its_series_statement")
        .await;
    let db = &built.db().to_string();
    // A label cache that was never refreshed: every resolution falls back
    // to SQL.
    let cold = Arc::new(LabelCache::new(
        ChClient::new(test_config(db)).await.expect("connect"),
        cache_config(db),
    ));
    let on = MetricsEngine::new(built.client().await, cold, engine_config(db, true))
        .with_rollup_levels(Arc::clone(&built.slot));
    let off = built.engine(true, None).await;
    let t = no_tenant();
    let q = "sum by (instance) (rate(p_rx_total[12m]))";
    let p = range(end - DAY, end, 12 * MIN);
    let (a, b) = futures::join!(ask(&on, &t, q, &p), ask(&off, &t, q, &p));
    let mut failures = Vec::new();
    let resolution: Vec<&str> = a
        .explain
        .stages
        .iter()
        .filter(|s| s.name == "series_resolution")
        .map(|s| s.sql.as_str())
        .collect();
    if resolution.is_empty() || resolution.iter().any(|s| s.starts_with("label cache:")) {
        failures.push(format!("the cold cache answered: {resolution:?}"));
    }
    let pushed = a.pushed();
    if !pushed
        .iter()
        .any(|s| s.starts_with("WITH ") && s.contains("FROM metric_series"))
    {
        failures.push(format!(
            "no pushed statement reads metric_series: {} pushed",
            pushed.len()
        ));
    }
    if vector_or_matrix(&a) != vector_or_matrix(&b) || vector_or_matrix(&b).is_empty() {
        failures.push("cells differ from push off".to_string());
    }
    built.finish().await;
    assert!(failures.is_empty(), "T61:\n{}", failures.join("\n"));
}

/// **T62 (issue #18 part 2c): the lag guard reaches every statement**:
/// both routes' statements and an in-process read's carry the delay, in
/// `system.query_log`.
#[tokio::test(flavor = "multi_thread")]
async fn the_lag_guard_reaches_every_statement() {
    skip_unless_live!();
    let (built, end) = P2.lease("the_lag_guard_reaches_every_statement").await;
    let db = &built.db().to_string();
    let probe = ask(
        &built.engine(true, None).await,
        &no_tenant(),
        "sum(rate(p_rx_total[12m]))",
        &range(end - DAY, end, 12 * MIN),
    )
    .await;
    let mut runs: Vec<(String, String, MetricQueryParams, bool)> = t60_queries(end)
        .into_iter()
        .flat_map(|(n, q, p)| {
            [
                (n.to_string(), q.to_string(), p, true),
                (n.to_string(), q.to_string(), p, false),
            ]
        })
        .collect();
    runs.push((
        "raw".to_string(),
        "rate(p_rx_total[5m])".to_string(),
        range(end - HOUR, end, MIN),
        false,
    ));
    let mut failures = Vec::new();
    for (name, q, p, push) in runs {
        let comment = format!("{db}_t62{name}_{push}_{}", now_ms());
        let engine = if push {
            built.pushed_engine(Some(&comment)).await
        } else {
            built.engine(true, Some(&comment)).await
        };
        quiet_cells(&engine, &q, &p, &probe).await;
        let sent = logged(&built.admin, &comment).await;
        if sent.is_empty() {
            failures.push(format!("{name} push {push}: no statement logged"));
        }
        for r in &sent {
            // The two pins equal to the server's default (1) are not in
            // `Settings`; U2 checks them in the builders (plan r8).
            if r.delay != "5" {
                failures.push(format!(
                    "{name} push {push}: delay {:?}: {}",
                    r.delay,
                    r.query.lines().next().unwrap_or("")
                ));
            }
        }
    }
    built.finish().await;
    assert!(failures.is_empty(), "T62:\n{}", failures.join("\n"));
}

/// `q_wide_total`, 500 series `{k="k0"}` … `{k="k499"}`, scraped every
/// 15 s over corpus P's 50 hours, registered every hour.
fn q_wide_land(db: &str, end: i64) -> Vec<String> {
    let lo = end - 50 * HOUR;
    let series = format!(
        "CREATE TABLE {db}.rw_series ENGINE = Memory AS \
         SELECT metric, labels, seed, \
                bitOr(bitShiftLeft(toUInt128(bitShiftRight(cityHash64(metric), 32)), 96), \
                      bitAnd(bitOr(bitShiftLeft(toUInt128(cityHash64(buf)), 64), toUInt128(xxHash64(buf))), \
                             toUInt128('79228162514264337593543950335'))) AS fp \
         FROM (SELECT 'q_wide_total' AS metric, concat('{{\"k\":\"k', toString(number), '\"}}') AS labels, \
                      toUInt32(200 + number) AS seed, concat(metric, unhex('FF'), labels, unhex('FF')) AS buf \
               FROM numbers(500))"
    );
    let registrations = format!(
        "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) \
         SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 2, metric, fp, \
                toInt64({lo}) + toInt64(n.number) * {HOUR} + 1000, labels, 0 \
         FROM {db}.rw_series, numbers(50) AS n"
    );
    let samples = format!(
        "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
         SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 0, s.metric, s.fp, \
                toInt64({lo}) + 15000 * toInt64(n.number) + (100 + (7919 * s.seed) % 14800), \
                toFloat64(n.number * (1 + s.seed % 7)) \
         FROM {db}.rw_series AS s, numbers({}) AS n",
        50 * HOUR / SCRAPE
    );
    vec![series, registrations, samples]
}

/// The late `q_wide_total{k="late"}`: registered over the last hour and
/// scraped every 15 s, its values `scale` times its scrape index.
fn q_wide_late(db: &str, end: i64, scale: f64, offset_ms: i64) -> Vec<String> {
    let lo = end - HOUR;
    let fp = "bitOr(bitShiftLeft(toUInt128(bitShiftRight(cityHash64('q_wide_total'), 32)), 96), \
              bitAnd(bitOr(bitShiftLeft(toUInt128(cityHash64(concat('q_wide_total', unhex('FF'), '{\"k\":\"late\"}', unhex('FF')))), 64), \
                           toUInt128(xxHash64(concat('q_wide_total', unhex('FF'), '{\"k\":\"late\"}', unhex('FF'))))), \
                     toUInt128('79228162514264337593543950335')))";
    vec![
        format!(
            "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) \
             SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 2, 'q_wide_total', {fp}, \
                    toInt64({lo}) + 1000, '{{\"k\":\"late\"}}', 0"
        ),
        format!(
            "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
             SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 0, 'q_wide_total', {fp}, \
                    toInt64({lo}) + 15000 * toInt64(number) + {offset_ms}, {scale} * toFloat64(number) \
             FROM numbers({})",
            HOUR / SCRAPE
        ),
    ]
}

/// **T64 (issue #18 part 2c): a series registered after the sweep is left
/// out by both routes.** 500 `q_wide_total` series in the cache; one more,
/// registered and landed after the sweep, inside the cache's recency gate.
/// At 499 series (a matcher) and at 500, the pushed answer equals the
/// in-process one, and neither moves when the late series' samples change.
#[tokio::test(flavor = "multi_thread")]
async fn a_series_registered_after_the_sweep_is_left_out_by_both_routes() {
    skip_unless_live!();
    let end = p_end();
    let db = DB_P64.name();
    let built = corpus_p2_with(db, end, q_wide_land(db, end)).await;
    let on = built.pushed_engine(None).await;
    let off = built.engine(true, None).await;
    let p = range(end - 6 * HOUR, end, MIN);
    let queries = [
        "sum(rate(q_wide_total{k!=\"k0\"}[5m]))",
        "sum(rate(q_wide_total[5m]))",
    ];
    for sql in q_wide_late(db, end, 1.0, 0) {
        exec(&built.admin, &sql).await;
    }
    let mut failures = Vec::new();
    let mut first = Vec::new();
    for q in queries {
        let (a, _, f) = pushed_against_in_process(&on, &off, q, &p).await;
        failures.extend(f);
        first.push(vector_or_matrix(&a));
    }
    // The late series' samples change: twice as many, twice as steep.
    for sql in q_wide_late(db, end, 2.0, 7_000).into_iter().skip(1) {
        exec(&built.admin, &sql).await;
    }
    for (q, before) in queries.iter().zip(&first) {
        let (a, _, f) = pushed_against_in_process(&on, &off, q, &p).await;
        failures.extend(f);
        if &vector_or_matrix(&a) != before {
            failures.push(format!("{q}: the answer moved with the late series"));
        }
    }
    built.finish().await;
    assert!(failures.is_empty(), "T64:\n{}", failures.join("\n"));
}

/// **T65 (issue #18 part 2c): a statement at the cap declines, and one
/// byte below it pushes.**
#[tokio::test(flavor = "multi_thread")]
async fn a_statement_at_the_cap_declines_and_one_below_pushes() {
    skip_unless_live!();
    let (built, end) = P2
        .lease("a_statement_at_the_cap_declines_and_one_below_pushes")
        .await;
    let off = built.engine(true, None).await;
    let t = no_tenant();
    let q = "sum by (instance) (rate(p_rx_total[12m]))";
    let p = range(end - DAY, end, 12 * MIN);
    let free = ask(&built.pushed_engine(None).await, &t, q, &p).await;
    let text = free
        .pushed()
        .first()
        .map(|s| s.to_string())
        .expect("a pushed statement");
    // The dispatcher's own measure: its text with each `?` doubled.
    let escaped = |s: &str| s.replace('?', "??").len() as u64;
    let n = escaped(&text);
    let b = ask(&off, &t, q, &p).await;
    let mut failures = Vec::new();
    let at = ask(
        &built.pushed_engine(None).await.with_statement_cap(n),
        &t,
        q,
        &p,
    )
    .await;
    if at.declines() != ["declined: StatementTooLarge"] || !at.pushed().is_empty() {
        failures.push(format!(
            "at the cap: declines {:?}, {} pushed",
            at.declines(),
            at.pushed().len()
        ));
    }
    if vector_or_matrix(&at) != vector_or_matrix(&b) {
        failures.push("at the cap: cells differ from push off".to_string());
    }
    let below = ask(
        &built.pushed_engine(None).await.with_statement_cap(n + 1),
        &t,
        q,
        &p,
    )
    .await;
    if !below.declines().is_empty() || below.pushed().first().map(|s| escaped(s)) != Some(n) {
        failures.push(format!(
            "one byte below: declines {:?}, {} pushed",
            below.declines(),
            below.pushed().len()
        ));
    }
    if vector_or_matrix(&below) != vector_or_matrix(&b) {
        failures.push("one byte below: cells differ from push off".to_string());
    }
    built.finish().await;
    assert!(failures.is_empty(), "T65:\n{}", failures.join("\n"));
}

// ------------------------------------- the setup build against the view

static DB_SAME_D60_VIEW: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_read_it_rollup_same_d60_view");
static DB_SAME_D60_SET: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_read_it_rollup_same_d60_set");
static DB_SAME_D5_VIEW: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_read_it_rollup_same_d5_view");
static DB_SAME_D5_SET: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_read_it_rollup_same_d5_set");

/// A level's data row, the floats other than `sum` and `inc` as their bits.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct LevelRow {
    org_id: String,
    fingerprint: u128,
    t_end: i64,
    t_last: i64,
    last_bits: u64,
    min_bits: u64,
    max_bits: u64,
    sum: f64,
    count: u32,
    inc: f64,
    flags: u8,
    built_at: i64,
}

/// A level's run row.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, Copy)]
struct RunRow {
    t_last: i64,
    t_end: i64,
    built_at: i64,
}

/// Level `r`'s data rows in key order and its run rows.
async fn level_rows(client: &ChClient, db: &str, r: u32) -> (Vec<LevelRow>, Vec<RunRow>) {
    let sql = format!(
        "SELECT org_id, fingerprint, t_end, t_last, \
                reinterpretAsUInt64(last) AS last_bits, reinterpretAsUInt64(min) AS min_bits, \
                reinterpretAsUInt64(max) AS max_bits, sum, count, inc, flags, built_at \
         FROM {db}.metric_samples_{r}s WHERE kind = 0 ORDER BY org_id, fingerprint, t_end"
    );
    let mut stream = client
        .query_stream::<LevelRow>(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\nSQL:\n{sql}"));
    let mut data = Vec::new();
    while let Some(row) = stream.next().await {
        data.push(row.expect("decode"));
    }
    let sql =
        format!("SELECT t_last, t_end, built_at FROM {db}.metric_samples_{r}s WHERE kind = 1");
    let mut stream = client
        .query_stream::<RunRow>(&sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\nSQL:\n{sql}"));
    let mut runs = Vec::new();
    while let Some(row) = stream.next().await {
        runs.push(row.expect("decode"));
    }
    (data, runs)
}

/// The union of run rows' `(t_last, t_end]`, adjacent intervals merged.
fn union_of(runs: &[RunRow]) -> Vec<(i64, i64)> {
    let mut v: Vec<(i64, i64)> = runs.iter().map(|r| (r.t_last, r.t_end)).collect();
    v.sort_unstable();
    let mut out: Vec<(i64, i64)> = Vec::new();
    for (lo, hi) in v {
        match out.last_mut() {
            Some(last) if lo <= last.1 => last.1 = last.1.max(hi),
            _ => out.push((lo, hi)),
        }
    }
    out
}

/// `a` and `b` as bits, or within a relative 1e-9.
fn close(a: f64, b: f64) -> bool {
    a.to_bits() == b.to_bits() || (a - b).abs() <= 1e-9 * a.abs().max(b.abs())
}

/// Two data rows equal on every column the level fetch reads and `org_id`,
/// `sum` and `inc` within [`close`]; `built_at` not compared.
fn same_row(a: &LevelRow, b: &LevelRow) -> bool {
    a.org_id == b.org_id
        && a.fingerprint == b.fingerprint
        && a.t_end == b.t_end
        && a.t_last == b.t_last
        && a.last_bits == b.last_bits
        && a.min_bits == b.min_bits
        && a.max_bits == b.max_bits
        && close(a.sum, b.sum)
        && a.count == b.count
        && close(a.inc, b.inc)
        && a.flags == b.flags
}

/// One case of N1 and N2: two databases built alike to their first level,
/// its coverage cut, then the later levels built by the view in one and by
/// the setup statement in the other.
struct SameCase {
    view_db: &'static str,
    set_db: &'static str,
    levels: (&'static [u32], &'static [u32]),
    days: std::ops::Range<i64>,
    day0: i64,
    first: (i64, i64),
    cut: (u32, i64, i64),
    /// Per later level, the expected union of its run rows.
    unions: Vec<(u32, Vec<(i64, i64)>)>,
    /// The later levels that hold at least one data row; the others hold no row.
    with_rows: &'static [u32],
}

async fn same_build(case: SameCase) {
    let mut read = Vec::new();
    for (db, by_view) in [(case.view_db, true), (case.set_db, false)] {
        let land = corpus_d_land(db, case.day0, case.days.clone());
        let (bootstrap, admin, ctx) =
            first_only(db, case.levels, &land, case.first.0, case.first.1).await;
        cut_runs(&admin, db, case.cut.0, case.cut.1, case.cut.2).await;
        if by_view {
            build_later_by_view(&admin, &ctx).await;
        } else {
            build_later_by_statement(&admin, &ctx).await;
        }
        let mut levels = BTreeMap::new();
        for (r, _) in &case.unions {
            levels.insert(*r, level_rows(&admin, db, *r).await);
        }
        read.push((db, levels));
        drop_database(&bootstrap, db).await;
    }
    let (view, set) = (&read[0].1, &read[1].1);
    for (r, want) in &case.unions {
        let res = i64::from(*r) * 1000;
        let (vd, vr) = &view[r];
        let (sd, sr) = &set[r];
        // (a) each database's run-row union.
        assert_eq!(&union_of(vr), want, "(a) level {r}s, the view's database");
        assert_eq!(&union_of(sr), want, "(a) level {r}s, the setup's database");
        // (b) the data rows of the two, pairwise.
        let first_diff = vd.iter().zip(sd).position(|(a, b)| !same_row(a, b));
        assert!(
            vd.len() == sd.len() && first_diff.is_none(),
            "(b) level {r}s: {} rows by the view, {} by the setup; first difference at {:?}:\n\
             view  {:?}\nsetup {:?}",
            vd.len(),
            sd.len(),
            first_diff,
            first_diff.map(|i| &vd[i]),
            first_diff.map(|i| &sd[i]),
        );
        for (name, data, runs) in [("view", vd, vr), ("setup", sd, sr)] {
            // (c) each data row's bucket inside the expected union.
            for row in data {
                assert!(
                    want.iter()
                        .any(|&(lo, hi)| lo <= row.t_end - res && row.t_end <= hi),
                    "(c) level {r}s, the {name}'s database: a row outside the union {want:?}: {row:?}"
                );
            }
            // (d) at least one data row, or no row at all.
            if case.with_rows.contains(r) {
                assert!(
                    !data.is_empty(),
                    "(d) level {r}s, the {name}'s database: no data row"
                );
            } else {
                assert!(
                    data.is_empty() && runs.is_empty(),
                    "(d) level {r}s, the {name}'s database: {} data rows, {} run rows",
                    data.len(),
                    runs.len()
                );
            }
            // (e) each data row stamped as the run row covering its bucket.
            for row in data {
                let run = runs
                    .iter()
                    .find(|u| u.t_last <= row.t_end - res && row.t_end <= u.t_end);
                assert_eq!(
                    run.map(|u| u.built_at),
                    Some(row.built_at),
                    "(e) level {r}s, the {name}'s database: the run row covering {row:?}: {run:?}"
                );
            }
            for u in runs {
                if name == "view" {
                    assert!(
                        u.t_end - u.t_last <= 10 * res,
                        "(e) level {r}s, the view's database: a run row over more than 10 buckets: {u:?}"
                    );
                } else {
                    assert_eq!(
                        u.t_last.div_euclid(DAY),
                        (u.t_end - 1).div_euclid(DAY),
                        "(e) level {r}s, the setup's database: a run row across midnight: {u:?}"
                    );
                }
            }
        }
    }
}

/// **N1 (issue #781): the setup build equals the view build at the default
/// levels.** Corpus D over days 6 and 7, the 60 s level built by its
/// statement and cut over `(d7 + 5 h 3 min, d7 + 6 h 7 min]`; the later
/// levels built by their views in one database and by the setup statement
/// in the other. Per level, each database's run rows cover the union the
/// cut leaves, the data rows of the two are equal (`sum` and `inc` within
/// a relative 1e-9), each lies inside that union and carries its run row's
/// `built_at`, a view run spans at most 10 buckets and a setup run one day.
#[tokio::test(flavor = "multi_thread")]
async fn the_setup_build_equals_the_view_build_at_the_default_levels() {
    skip_unless_live!();
    let day0 = today() - 8 * DAY;
    let d = |n: i64| day0 + n * DAY;
    same_build(SameCase {
        view_db: DB_SAME_D60_VIEW.name(),
        set_db: DB_SAME_D60_SET.name(),
        levels: DEFAULTS,
        days: 6..8,
        day0,
        first: (d(6), d(8)),
        cut: (60, d(7) + 5 * HOUR + 3 * MIN, d(7) + 6 * HOUR + 7 * MIN),
        unions: vec![
            (
                720,
                vec![(d(6), d(7) + 5 * HOUR), (d(7) + 6 * HOUR + 12 * MIN, d(8))],
            ),
            (7200, vec![(d(6), d(7) + 4 * HOUR), (d(7) + 8 * HOUR, d(8))]),
            (86400, vec![(d(6), d(7))]),
        ],
        with_rows: &[720, 7200, 86400],
    })
    .await;
}

/// **N2 (issue #781): the setup build equals the view build from a 5 s
/// level.** As N1 at the five levels from 5 s, corpus D's day 7, the 5 s
/// level over `(d7 + 8 h, d7 + 14 h]` cut over `(d7 + 11 h 1 min 5 s, d7 +
/// 11 h 7 min 35 s]`; the 86400 s level holds no row in either database.
#[tokio::test(flavor = "multi_thread")]
async fn the_setup_build_equals_the_view_build_from_a_5s_level() {
    skip_unless_live!();
    let day0 = today() - 8 * DAY;
    let d7 = day0 + 7 * DAY;
    same_build(SameCase {
        view_db: DB_SAME_D5_VIEW.name(),
        set_db: DB_SAME_D5_SET.name(),
        levels: FIVE,
        days: 7..8,
        day0,
        first: (d7 + 8 * HOUR, d7 + 14 * HOUR),
        cut: (
            5,
            d7 + 11 * HOUR + MIN + 5_000,
            d7 + 11 * HOUR + 7 * MIN + 35_000,
        ),
        unions: vec![
            (
                60,
                vec![
                    (d7 + 8 * HOUR, d7 + 11 * HOUR + MIN),
                    (d7 + 11 * HOUR + 8 * MIN, d7 + 14 * HOUR),
                ],
            ),
            (
                720,
                vec![
                    (d7 + 8 * HOUR, d7 + 11 * HOUR),
                    (d7 + 11 * HOUR + 12 * MIN, d7 + 14 * HOUR),
                ],
            ),
            (
                7200,
                vec![
                    (d7 + 8 * HOUR, d7 + 10 * HOUR),
                    (d7 + 12 * HOUR, d7 + 14 * HOUR),
                ],
            ),
            (86400, vec![]),
        ],
        with_rows: &[60, 720, 7200],
    })
    .await;
}

/// **N3 (issue #781): every shared member leases its corpus.** No test is
/// in two groups, and each member's body leases from its group by its own
/// name.
#[test]
fn every_shared_member_leases_its_corpus() {
    let text = include_str!("live_metrics_rollup.rs");
    let mut seen = std::collections::BTreeSet::new();
    for (name, group) in groups() {
        for member in group.members {
            assert!(seen.insert(*member), "{member} is in two groups");
            let head = format!("async fn {member}() {{");
            let at = text
                .find(&head)
                .unwrap_or_else(|| panic!("{name}: no test `{member}` in the file"));
            let body = &text[at..];
            let body = &body[..body.find("\n}\n").expect("the end of the test")];
            // The call as written, whatever lines the formatter breaks it over.
            let body: String = body.chars().filter(|c| !c.is_whitespace()).collect();
            let call = format!("{name}.lease(\"{member}\")");
            assert!(
                body.contains(&call),
                "{name}: `{member}` does not lease its corpus with `{call}`"
            );
        }
    }
}
