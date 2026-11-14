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

static DB_B1F: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_b1f");
static DB_B7: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_b7");
static DB_B10: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_b10");
static DB_S2: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_s2");
static DB_B2E: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_b2e");
static DB_T10: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_t10");
static DB_L1: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_l1");
static DB_L7: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_l7");
static DB_P5: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_p5");
static DB_P22: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_p22");
static DB_P64: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_p64");
static DB_P31: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_p31");
static DB_P33: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_p33");
static DB_Q12: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_q12");
static DB_Q13: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_q13");
static DB_Q14: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_q14");
static DB_Q15: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_q15");
static DB_Q18: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_q18");

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

/// Builds `ctx` fresh with every level view stopped and its table and its
/// stage, where it has one, emptied.
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
        // Issue #800: the level's stage holds the creation run's run row.
        exec(
            bootstrap,
            &format!(
                "TRUNCATE TABLE IF EXISTS {}.metric_samples_{r}s_stage",
                ctx.db
            ),
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
    /// `corpus_h(db, p_end())` (issue #810).
    H,
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
            Corpus::H => {
                let end = p_end();
                (corpus_h(db, end).await, end)
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
        "each_served_function_is_within_its_distance_at_60s",
        "each_served_function_is_within_its_distance_at_720s",
        "each_served_function_is_within_its_distance_at_7200s",
        "each_served_function_is_within_its_distance_at_86400s",
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
        "a_panel_at_a_level_is_one_statement",
        "an_in_process_routed_selector_is_one_statement",
        "a_node_the_cache_cannot_list_is_one_statement",
        "a_listed_and_an_unlisted_side_are_gated_over_the_cap",
    ],
);
static H: Shared = Shared::new(
    pulsus_testkit::TestDb::new("pulsus_read_it_rollup_h810"),
    Corpus::H,
    &[
        "a_query_over_native_histograms_reads_raw",
        "the_histogram_count_spans_the_query",
        "a_pushed_node_over_histograms_reads_raw",
        "a_raw_tail_answers_its_tail",
        "a_count_past_the_text_cap_drops_other_lists",
        "a_kept_node_is_read_again_raw",
        "a_kept_node_is_read_again_over_its_tail",
        "an_unrouted_join_over_histograms_falls_back",
        "a_mixed_join_falls_back_on_its_raw_side",
        "a_level_node_is_read_again_on_another_selectors_histograms",
        "a_mixed_node_is_read_again_one_statement_a_side",
        "a_raw_only_node_stays_pushed_in_a_raw_query",
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
fn groups() -> [(&'static str, &'static Shared); 6] {
    [
        ("D5_8", &D5_8),
        ("D60_8", &D60_8),
        ("D60_2", &D60_2),
        ("P", &P),
        ("P2", &P2),
        ("H", &H),
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

/// B2's pairs in flight across every level test in this process: the
/// level tests run side by side, and 8 pairs each overran the CI runner's
/// 4 cores, so the client stopped reading a body for the server's 30 s.
static B2_SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(B2_PAIRS);

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
                let _slot = B2_SLOTS
                    .acquire()
                    .await
                    .expect("the semaphore is never closed");
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
/// section 9 (bit-equal where it says so). One test per level, so the
/// levels run side by side and a failure names its level.
///
/// Runs one level's column on `built` over `(start, end]`, comparing the
/// steps from `from`; prints the report and each query's rows read, drops
/// or releases the corpus, and asserts the column's failures.
async fn b2_level(built: Built, secs: i64, start: i64, end: i64, from: i64) {
    let mut failures = Vec::new();
    let mut report = Vec::new();
    let mut comments = Vec::new();
    // Each run's statements carry their own comment, so the query log's
    // rows of an earlier run, or of another level's test running beside
    // this one on the same corpus, are not summed into this one's.
    let run = format!("b2_{}_{secs}_{}", built.db(), now_ms());
    b2_column(
        Column {
            built: &built,
            secs,
            start,
            end,
            from,
            run: &run,
        },
        &mut failures,
        &mut report,
        &mut comments,
    )
    .await;
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
    assert!(
        failures.is_empty(),
        "B2 failures at {secs}s:\n{}",
        failures.join("\n")
    );
}

/// B2's column on corpus D5_8 at level `secs`, over the range inside its
/// band that ends at `day0 + 8 d`.
async fn b2_d5_8(built: Built, day0: i64, secs: i64) {
    let end = day0 + 8 * DAY;
    let span = match secs {
        60 => DAY - HOUR,
        720 => 3 * DAY,
        7200 => 8 * DAY,
        _ => band(secs),
    };
    b2_level(built, secs, end - span, end, day0 + 5 * secs * 1000).await;
}

/// B2 at 5 s: corpus E over the 2 hours ending at D5_8's end,
/// `today() = day0 + 8 d`, compared over the last 30 minutes.
#[tokio::test(flavor = "multi_thread")]
async fn each_served_function_is_within_its_distance_at_5s() {
    skip_unless_live!();
    let end = today();
    let e_land = corpus_e(DB_B2E.name(), end);
    let built_e = build(DB_B2E.name(), FIVE, &e_land, end - 2 * HOUR, end, &[]).await;
    b2_level(built_e, 5, end - 30 * MIN, end, end - 30 * MIN).await;
}

/// B2 at 60 s, on corpus D5_8.
#[tokio::test(flavor = "multi_thread")]
async fn each_served_function_is_within_its_distance_at_60s() {
    skip_unless_live!();
    let (built, day0) = D5_8
        .lease("each_served_function_is_within_its_distance_at_60s")
        .await;
    let levels = built.slot.get().expect("the levels are read");
    assert_eq!(levels.levels.len(), 5, "{levels:?}");
    b2_d5_8(built, day0, 60).await;
}

/// B2 at 720 s, on corpus D5_8.
#[tokio::test(flavor = "multi_thread")]
async fn each_served_function_is_within_its_distance_at_720s() {
    skip_unless_live!();
    let (built, day0) = D5_8
        .lease("each_served_function_is_within_its_distance_at_720s")
        .await;
    let levels = built.slot.get().expect("the levels are read");
    assert_eq!(levels.levels.len(), 5, "{levels:?}");
    b2_d5_8(built, day0, 720).await;
}

/// B2 at 7200 s, on corpus D5_8.
#[tokio::test(flavor = "multi_thread")]
async fn each_served_function_is_within_its_distance_at_7200s() {
    skip_unless_live!();
    let (built, day0) = D5_8
        .lease("each_served_function_is_within_its_distance_at_7200s")
        .await;
    let levels = built.slot.get().expect("the levels are read");
    assert_eq!(levels.levels.len(), 5, "{levels:?}");
    b2_d5_8(built, day0, 7200).await;
}

/// B2 at 86400 s, on corpus D5_8.
#[tokio::test(flavor = "multi_thread")]
async fn each_served_function_is_within_its_distance_at_86400s() {
    skip_unless_live!();
    let (built, day0) = D5_8
        .lease("each_served_function_is_within_its_distance_at_86400s")
        .await;
    let levels = built.slot.get().expect("the levels are read");
    assert_eq!(levels.levels.len(), 5, "{levels:?}");
    b2_d5_8(built, day0, 86400).await;
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

    // The mixed metric (issue #810, D9): a routed selector meets a
    // native-histogram sample, so the whole query is read raw.
    let a = ask(&routed, &no_tenant(), "rate(rollup_mixed[1h])", &p).await;
    let b = ask(&raw, &no_tenant(), "rate(rollup_mixed[1h])", &p).await;
    assert!(!a.explain.rollup, "the mixed metric is read raw");
    assert!(
        a.stages("rollup_piece")
            .iter()
            .any(|n| n.starts_with("native histograms: read raw")),
        "{:?}",
        a.stages("rollup_piece")
    );
    assert!(!canonical(&b.result).is_empty());
    assert_eq!(
        canonical(&a.result),
        canonical(&b.result),
        "the whole answer"
    );

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

    // T11 (issue #810, D9): a pushed node over the mixed metric is its own
    // statement, which reads no level, then one statement read raw.
    let comment = format!("{db}_t11_{}", now_ms());
    let pushed = MetricsEngine::new(
        built.client().await,
        Arc::clone(&built.cache),
        engine_config(db, true),
    )
    .with_rollup_levels(Arc::clone(&built.slot))
    .with_query_log_comment(&comment);
    let q = "sum by (s) (rate(rollup_mixed[1h]))";
    let a = ask(&pushed, &no_tenant(), q, &p).await;
    let b = ask(&raw, &no_tenant(), q, &p).await;
    let sent = logged(&built.admin, &comment).await;
    let tables: Vec<&Vec<String>> = sent.iter().map(|r| &r.tables).collect();
    assert_eq!(sent.len(), 2, "T11: statements {tables:?}");
    assert!(
        sent[1].tables.contains(&format!("{db}.metric_samples"))
            && sent[1]
                .tables
                .contains(&format!("{db}.metric_hist_samples")),
        "T11: the second reads raw: {tables:?}"
    );
    assert!(!vector_or_matrix(&b).is_empty(), "T11: an answer");
    assert_eq!(vector_or_matrix(&a), vector_or_matrix(&b), "T11: as raw");
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
/// reads their labels in its one `rollup_piece` statement and routes to
/// `metric_samples_7200s` at step 2 h. `max_over_time` is bit-equal and
/// `rate` within its level's bound against the unrouted engine asked at
/// step 2 h from the same `start`. With both ends 5 minutes later, the
/// points read `metric_samples_7200s` at its bucket ends: the first 84
/// take the unrouted engine at the bucket end that holds each; the 85th
/// has no value (issue #810: nothing after the last step is read).
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
            // Issue #810 (§3): shifted, the 85th point is in the 2 h bucket
            // after `end`, which nothing after the last step reads:
            // `max_over_time(g[2h])` has no built bucket in its window there
            // and no value; `rate(c_normal[10h])` has, and a value.
            let want_times = if shift == 0 || kind.is_some() {
                &times[..]
            } else {
                &times[..84]
            };
            for (l, ts) in &series {
                assert_eq!(ts, want_times, "{query} +{shift} {l}: start + k × 2 h");
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
            // Issue #810 (D11): the one `rollup_piece` statement reads the
            // series and their labels itself.
            let hydrated = stages
                .iter()
                .find(|s| s.name == "rollup_piece")
                .unwrap_or_else(|| panic!("{query}: no rollup_piece stage: {stages:?}"));
            assert!(
                hydrated.sql.contains(&hydrate),
                "{query}: the rollup_piece statement reads the labels: {}",
                hydrated.sql
            );
            let route = a.stages("rollup_route");
            assert!(
                route
                    .iter()
                    .any(|n| n.starts_with(&format!("{table},")) && n.contains(", step 2h,")),
                "{query} +{shift}: {route:?}"
            );

            let off = engine(false, format!("{run}_{shift}_{qi}_off")).await;
            // Shifted, the 85th point is the newest step answered: no step
            // after the newest built bucket is
            // (`a_panel_at_a_level_ends_at_the_newest_built_bucket`).
            let pa = only(&a.points(), |x| shift == 0 || x <= times[83]);
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
                spread(&ends.points(), &times[..84], res, 1.0)
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
    let counter = 0.0211;
    // (query, the query at the bucket ends, (iii), scale of (iii)).
    let cases: [(&str, &str, Cmp, f64); 10] = [
        (
            "max_over_time(g[2m])",
            "max_over_time(g[12m])",
            Cmp::Bits,
            1.0,
        ),
        (
            "min_over_time(g[2m])",
            "min_over_time(g[12m])",
            Cmp::Bits,
            1.0,
        ),
        (
            "avg_over_time(g[2m])",
            "avg_over_time(g[12m])",
            Cmp::Max(1e-12),
            1.0,
        ),
        (
            "last_over_time(g[2m])",
            "last_over_time(g[12m])",
            Cmp::Bits,
            1.0,
        ),
        (
            "present_over_time(g[2m])",
            "present_over_time(g[12m])",
            Cmp::Bits,
            1.0,
        ),
        (
            "absent_over_time(c_stop[2m])",
            "absent_over_time(c_stop[12m])",
            Cmp::Bits,
            1.0,
        ),
        ("g", "g", Cmp::Bits, 1.0),
        (
            "rate(c_normal[2m15s])",
            "rate(c_normal[12m])",
            Cmp::P99(counter),
            1.0,
        ),
        (
            "increase(c_normal[2m15s])",
            "increase(c_normal[12m])",
            Cmp::P99(counter),
            135.0 / 720.0,
        ),
        (
            "sum(rate(c_normal[2m15s]))",
            "sum(rate(c_normal[12m]))",
            Cmp::P99(counter),
            1.0,
        ),
    ];
    let mut failures = Vec::new();
    for (query, at_ends, early, scale) in cases {
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
        // Issue #836: no step after the newest built bucket `gl` is
        // answered, `absent_over_time` included.
        let built_times: Vec<i64> = times.iter().copied().filter(|x| *x <= gl).collect();
        for (l, ts) in &series {
            let ok = if query.starts_with("absent_over_time") {
                !ts.is_empty() && ts[..] == built_times[built_times.len() - ts.len()..]
            } else {
                ts == &built_times
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
        // (iv) each later step: no value (issue #836).
        let late_points = only(&points, |x| x > gl);
        let late_ok = late_points.is_empty();
        if !late_ok {
            failures.push(format!("{query} (iv): after {gl}: {late_points:?}"));
        }
        // (v) the answer is a rollup's.
        if !a.explain.rollup {
            failures.push(format!("{query} (v): exactness is not rollup"));
        }
    }
    built.finish().await;
    assert!(failures.is_empty(), "L1:\n{}", failures.join("\n"));
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
    // Issue #810 (§3): nothing after the last step is read, so the steps
    // after the last 2 h bucket ending by it, `gl`, have one point or none
    // in their window and no value.
    let gl = end - 2 * HOUR;
    let to_gl = times_of(&p).into_iter().filter(|x| *x <= gl).count();
    for (l, ts) in &per_series(&a) {
        if ts.len() != to_gl || ts.iter().any(|x| *x > gl) {
            failures.push(format!("7 d {l}: {} times, want {to_gl}", ts.len()));
        }
    }
    let note = "metric_samples_7200s, use Counter, step 2h, range 2h (asked 10m15s)";
    if !a.stages("rollup_route").iter().any(|n| n == note) {
        failures.push(format!("7 d: routes {:?}", a.stages("rollup_route")));
    }
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
    // Issue #810 (§3): the last three steps are in the minute bucket after
    // the last step, which is not read; their windows hold no built bucket.
    let to_gl: Vec<i64> = times.iter().copied().filter(|x| *x <= end - MIN).collect();
    for (l, ts) in &per_series(&a) {
        if ts != &to_gl {
            failures.push(format!("3 h {l}: {} times, want {}", ts.len(), to_gl.len()));
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
    // Issue #810 (§3): corpus P's 60 s level is built to `end − 10 min`;
    // every step up to it has a value, and no later step
    // (`a_panel_at_a_level_ends_at_the_newest_built_bucket`).
    let built_to = end - 10 * MIN;
    let times: Vec<i64> = times_of(&p)
        .into_iter()
        .filter(|x| *x <= built_to)
        .collect();
    for (l, ts) in &per_series(&a) {
        if !times.iter().all(|x| ts.contains(x)) {
            failures.push(format!(
                "(b) {l}: {} times, not every one of the {} to {built_to}",
                ts.len(),
                times.len()
            ));
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
/// level fetch; no coverage statement (issue #810); one data statement,
/// returning at most a row per instance per step and the two counts.
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
    // Issue #810 (D2): no coverage statement.
    assert!(
        !sent
            .iter()
            .any(|(q, _)| q.starts_with("SELECT toUInt32(1) AS shard, t_last, t_end")),
        "a coverage statement: {brief:#?}"
    );
    let data: Vec<&(String, u64)> = sent
        .iter()
        .filter(|(q, _)| q.starts_with("WITH "))
        .collect();
    assert_eq!(data.len(), 1, "one data statement: {brief:#?}");
    assert_eq!(sent.len(), 1, "one statement: {brief:#?}");
    // A row per instance per step, the histogram count and `hist_n`.
    assert!(
        data[0].1 <= 4 * 451 + 2,
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
/// pushed now (T30 (c)), so they are not here. Issue #579 part 5: its
/// `on`, `ignoring` and `group_*` rows are pushed now (T12 b1-b3).
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
    let cases: [(&str, &MetricQueryParams, Option<&str>); 7] = [
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
    // Issue #810 (D1): the 24 h window reads the 720 s level alone.
    let check = |q: &str, b: &Answer, w24: bool, failures: &mut Vec<String>| {
        let notes = b.stages("rollup_piece");
        let alone =
            !notes.is_empty() && notes.iter().all(|n| n.starts_with("metric_samples_720s."));
        if w24 && !alone {
            failures.push(format!(
                "{q}: in process, the 24 h window does not read the 720 s level alone: {notes:?}"
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
/// answers as in process.** Corpus P at the five levels, a 60 s cut: over
/// 6 h at 1 m the 60 s route is pushed and reads the 60 s level alone
/// (issue #810); over 30 min at 15 s the 5 s route, pushed before, still
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
        // Issue #810 (D1): the route's level alone, no finer level.
        let reads = |table: &str| {
            a.pushed()
                .iter()
                .any(|s| s.contains(&format!("FROM {table}\n")))
        };
        if !reads("metric_samples_60s") || reads("metric_samples_5s") {
            failures.push(format!(
                "{q} over 6 h: the pushed statement does not read the 60 s level alone"
            ));
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

/// **T30 (issue #18 part 2b): a filled request pushes its read.** Issue
/// #810: one read, one pushed statement. (a) 6 h at 30 s off the minute:
/// the bucket ends' read at 60 s; (b) a day at 2 m, filled from 720 s; (c)
/// a week at 1 h from 7200 s; (d) shape C over a day.
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
                if pushed.len() != 1 || on_grid(&a, Some(first), 60_000).len() != 1 {
                    failures.push(format!(
                        "(a): pushed statements {}, want one on ({first}, 60000)",
                        pushed.len(),
                    ));
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
                // The bucket ends' read at 7200 s, and no other.
                let level = on_grid(&a, None, 7_200_000);
                if level.len() != 1
                    || !level[0].contains("FROM metric_samples_7200s\n")
                    || pushed.len() != 1
                {
                    failures.push(format!(
                        "(c): {} pushed statements, want one at 7200 s",
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

/// **T33 (issue #18 part 2b): a filled request over a histogram sample
/// falls back.** Issue #810: one read, which declines and is read in
/// process.
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
    if a.pushed().len() != 1 {
        failures.push(format!(
            "one pushed statement, declined: {} pushed",
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

/// **T60 (issue #18 part 2c): a pushed node reads no more than in
/// process.** Corpus P2, the cache warm: no pushed statement reads the
/// series tables but for `hist_n` (issue #810), the data statements read at
/// most the in-process statements plus three a statement (`system.one`),
/// and no coverage statement is sent (issue #810, D2).
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
        // (a) Issue #810: the series and labels are read by `hist_n`'s
        // condition alone; the groups name the cache's own series.
        for r in &son {
            let after = r
                .query
                .split_once(") AS hist_n")
                .map_or(r.query.as_str(), |(_, rest)| rest);
            if after.contains("FROM metric_series") || after.contains("FROM metric_labels") {
                failures.push(format!(
                    "{name}: (a) a pushed statement reads the series beside hist_n: {}",
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
        // (b2) Issue #810 (D2): no coverage statement.
        if son.iter().any(|r| r.query.starts_with(COVERAGE)) {
            failures.push(format!("{name}: (b2) a coverage statement logged"));
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
            // (e) issue #800: data rows carry no clock, run rows the run's.
            for row in data {
                assert_eq!(
                    row.built_at, 0,
                    "(e) level {r}s, the {name}'s database: a data row with a clock: {row:?}"
                );
            }
            for u in runs {
                assert!(
                    u.built_at > 0,
                    "(e) level {r}s, the {name}'s database: a run row without the run's clock: {u:?}"
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
/// a relative 1e-9), each lies inside that union and carries `built_at` 0
/// while each run row carries its run's clock (issue #800), a view run spans
/// at most 10 buckets and a setup run one day.
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

// ------------------------------------------- issue #579 part 5, corpus Q

/// Every statement sent under `comment`, finished or failed, in start
/// order: `(query, failed)`.
async fn statements_in_order(admin: &ChClient, comment: &str) -> Vec<(String, bool)> {
    #[derive(Row, serde::Serialize, serde::Deserialize)]
    struct Logged {
        query: String,
        failed: u8,
    }
    settle_query_log(&[admin], &format!("log_comment = '{comment}'"), 1).await;
    let mut stream = admin
        .query_stream::<Logged>(
            &format!(
                "SELECT query, toUInt8(type = 'ExceptionWhileProcessing') AS failed \
                 FROM system.query_log \
                 WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing') \
                   AND log_comment = '{comment}' \
                 ORDER BY query_start_time_microseconds"
            ),
            &QuerySettings::new(),
        )
        .await
        .expect("query_log");
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        let row = row.expect("decode");
        out.push((row.query, row.failed == 1));
    }
    out
}

/// Corpus Q's series (part 5, plan section 12): `(metric, key, labels,
/// seed)`, the labels canonical JSON with sorted keys.
const CORPUS_Q: [(&str, &str, &str, u32); 27] = [
    (
        "q_many_total",
        "mi0d0",
        r#"{"device":"d0","instance":"i0"}"#,
        100,
    ),
    (
        "q_many_total",
        "mi0d1",
        r#"{"device":"d1","instance":"i0"}"#,
        101,
    ),
    (
        "q_many_total",
        "mi0d2",
        r#"{"device":"d2","instance":"i0"}"#,
        102,
    ),
    (
        "q_many_total",
        "mi1d0",
        r#"{"device":"d0","instance":"i1"}"#,
        103,
    ),
    (
        "q_many_total",
        "mi1d1",
        r#"{"device":"d1","instance":"i1"}"#,
        104,
    ),
    (
        "q_many_total",
        "mi1d2",
        r#"{"device":"d2","instance":"i1"}"#,
        105,
    ),
    (
        "q_many_total",
        "mi2d0",
        r#"{"device":"d0","instance":"i2","role":"m"}"#,
        106,
    ),
    (
        "q_many_total",
        "mi2d1",
        r#"{"device":"d1","instance":"i2","role":"m"}"#,
        107,
    ),
    (
        "q_many_total",
        "mi2d2",
        r#"{"device":"d2","instance":"i2","role":"m"}"#,
        108,
    ),
    (
        "q_many_total",
        "mi3d0",
        r#"{"__unit__":"bytes","device":"d0","instance":"i3"}"#,
        109,
    ),
    (
        "q_many_total",
        "mi3d1",
        r#"{"__unit__":"bytes","device":"d1","instance":"i3"}"#,
        110,
    ),
    (
        "q_many_total",
        "mi3d2",
        r#"{"__unit__":"bytes","device":"d2","instance":"i3"}"#,
        111,
    ),
    (
        "q_one_total",
        "oi0a",
        r#"{"instance":"i0","role":"a"}"#,
        112,
    ),
    (
        "q_one_total",
        "oi0a2",
        r#"{"instance":"i0","role":"a2"}"#,
        113,
    ),
    (
        "q_one_total",
        "oi1b",
        r#"{"instance":"i1","role":"b"}"#,
        114,
    ),
    (
        "q_one_total",
        "oi1b2",
        r#"{"instance":"i1","role":"b2"}"#,
        115,
    ),
    ("q_one_total", "oi2", r#"{"instance":"i2"}"#, 116),
    (
        "q_one_total",
        "oi3",
        r#"{"__unit__":"bytes","instance":"i3","role":"c"}"#,
        117,
    ),
    (
        "q_peer_total",
        "pi0",
        r#"{"instance":"i0","zone":"z0"}"#,
        118,
    ),
    (
        "q_peer_total",
        "pi1",
        r#"{"instance":"i1","zone":"z1"}"#,
        119,
    ),
    (
        "q_peer_total",
        "pi2",
        r#"{"instance":"i2","zone":"z2"}"#,
        120,
    ),
    (
        "q_peer_total",
        "pi3",
        r#"{"__unit__":"bytes","instance":"i3","zone":"z3"}"#,
        121,
    ),
    ("q_glob_total", "g", "{}", 122),
    ("q_dup_total", "di0", r#"{"instance":"i0"}"#, 123),
    (
        "q_dup_total",
        "di9a",
        r#"{"instance":"i9","role":"a"}"#,
        124,
    ),
    (
        "q_dup_total",
        "di9b",
        r#"{"instance":"i9","role":"b"}"#,
        125,
    ),
    (
        "q_empty_total",
        "ei1",
        r#"{"instance":"i1","role":""}"#,
        126,
    ),
];

/// Corpus Q's other three series, beside [`CORPUS_Q`].
const CORPUS_Q_MORE: [(&str, &str, &str, u32); 3] = [
    (
        "q_empty_total",
        "ei2e",
        r#"{"instance":"i2","role":""}"#,
        127,
    ),
    ("q_empty_total", "ei2", r#"{"instance":"i2"}"#, 128),
    (
        "q_emptyb_total",
        "bi1",
        r#"{"instance":"i1","role":""}"#,
        129,
    ),
];

/// Corpus Q (part 5, plan section 12) beside corpus P, ending at `end`:
/// the series of [`CORPUS_Q`], registered every hour of the 50 and
/// scraped every 15 s with corpus P's jitter, each `q_one_total` rising by
/// about 1e6 a scrape, `q_emptyb_total` by 0.1, the others by 1 to 10.
fn corpus_q_land(db: &str, end: i64) -> Vec<String> {
    let lo = end - 50 * HOUR;
    let rows = CORPUS_Q
        .iter()
        .chain(&CORPUS_Q_MORE)
        .map(|(m, k, l, seed)| format!("('{m}', '{k}', '{}', {seed})", l.replace('\'', "\\'")))
        .collect::<Vec<_>>()
        .join(", ");
    let series = format!(
        "CREATE TABLE {db}.rq_series ENGINE = Memory AS \
         SELECT metric, key, labels, seed, \
                bitOr(bitShiftLeft(toUInt128(bitShiftRight(cityHash64(metric), 32)), 96), \
                      bitAnd(bitOr(bitShiftLeft(toUInt128(cityHash64(buf)), 64), toUInt128(xxHash64(buf))), \
                             toUInt128('79228162514264337593543950335'))) AS fp \
         FROM (SELECT metric, key, labels, seed, concat(metric, unhex('FF'), labels, unhex('FF')) AS buf \
               FROM values('metric String, key String, labels String, seed UInt32', {rows}))"
    );
    let registrations = format!(
        "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) \
         SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 2, metric, fp, \
                toInt64({lo}) + toInt64(n.number) * {HOUR} + 1000, labels, 0 \
         FROM {db}.rq_series, numbers(50) AS n"
    );
    let samples = format!(
        "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
         SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 0, metric, fp, t, \
                1000. * (seed + 1) + sum(inc) OVER (PARTITION BY fp ORDER BY i \
                   ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS v \
         FROM ( \
           SELECT metric, fp, key, seed, i, t, \
             multiIf(metric = 'q_one_total', 1e6 + toFloat64(cityHash64(seed, i) % 10), \
                     metric = 'q_emptyb_total', 0.1, toFloat64(1 + cityHash64(seed, i) % 10)) AS inc \
           FROM ( \
             SELECT s.metric AS metric, s.fp AS fp, s.key AS key, s.seed AS seed, toInt64(n.number) AS i, \
                    toInt64({lo}) + 15000 * i + (100 + (7919 * seed) % 14800) + (toInt64((7 * seed + 13 * i) % 201) - 100) AS t \
             FROM {db}.rq_series AS s, numbers({n}) AS n) \
           WHERE t <= {end} \
             AND NOT (metric = 'q_many_total' AND t > {gap_lo} AND t <= {gap_hi}) \
             AND NOT (key = 'oi0a' AND t >= {a_end}) \
             AND NOT (key = 'oi0a2' AND t < {a2_from}) \
             AND NOT (key = 'oi1b' AND t >= {b_end}) \
             AND NOT (key = 'oi1b2' AND t < {b2_from}))",
        n = 50 * HOUR / SCRAPE,
        gap_lo = end - 12 * HOUR - 20 * MIN,
        gap_hi = end - 11 * HOUR - 40 * MIN,
        a_end = end - 12 * HOUR + 2 * MIN,
        a2_from = end - 12 * HOUR - 2 * MIN,
        b_end = end - 8 * HOUR,
        b2_from = end - 8 * HOUR - 3 * MIN,
    );
    let markers = format!(
        "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
         SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 0, s.metric, s.fp, x.t, \
                reinterpretAsFloat64(toUInt64(9218868437227405314)) \
         FROM (SELECT * FROM values('k String, t Int64', ('oi0a', {}), ('oi1b', {}))) AS x \
         JOIN {db}.rq_series AS s ON s.key = x.k",
        end - 12 * HOUR + 2 * MIN,
        end - 8 * HOUR,
    );
    vec![series, registrations, samples, markers]
}

/// Corpora P and Q in one build at the default levels, P's 60 s hole cut.
async fn corpus_pq(db: &str, end: i64) -> Built {
    let mut land = corpus_p_land(db, end, false);
    land.extend(corpus_q_land(db, end));
    let built = build(db, DEFAULTS, &land, end - 50 * HOUR, end - 10 * MIN, &[]).await;
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

/// The group label sets of an answer, each sorted.
fn groups_of(x: &Answer) -> std::collections::BTreeSet<String> {
    vector_or_matrix(x).keys().map(|(l, _)| l.clone()).collect()
}

/// **T12 (issue #579 part 5): a matched binary under an aggregate answers
/// as in process**, for every row of the plan's T12 table, and the
/// in-process answers express what the rows pin.
#[tokio::test(flavor = "multi_thread")]
async fn a_matched_binary_under_an_aggregate_answers_as_in_process() {
    skip_unless_live!();
    let end = p_end();
    let built = corpus_pq(DB_Q12.name(), end).await;
    let on = built.pushed_engine(None).await;
    let off = built.engine(true, None).await;
    let t = no_tenant();
    let v = range(end - 6 * HOUR, end, MIN);
    let d = range(end - 15 * HOUR, end, 2 * MIN);
    let i = range(end - HOUR, end - HOUR, 0);
    let five = ["sum", "avg", "count", "min", "max"];
    let mut cases: Vec<(String, MetricQueryParams)> = Vec::new();
    let mut add =
        |aggs: &[&str], groupings: &[&str], binary: &str, windows: &[&MetricQueryParams]| {
            for agg in aggs {
                for g in groupings {
                    for w in windows {
                        cases.push((format!("{agg} {g}({binary})"), **w));
                    }
                }
            }
        };
    let three = ["by (instance) ", "without (device) ", ""];
    // b1, b1c
    for op in ["+", "/", ">", "> bool"] {
        let b = format!("rate(p_rx_total[5m]) {op} on (instance, device) rate(p_tx_total[5m])");
        add(&five, &three, &b, &[&d, &i]);
    }
    for op in [
        "==", "!=", "<", ">=", "<=", "== bool", "!= bool", "> bool", "< bool", ">= bool", "<= bool",
    ] {
        let b = format!("rate(p_rx_total[5m]) {op} on (instance, device) rate(p_tx_total[5m])");
        add(&["sum"], &three, &b, &[&d]);
    }
    // b2, b3
    let two = ["by (instance) ", "without (device) "];
    for op in ["-", "<="] {
        let b =
            format!("rate(p_rx_total[5m]) {op} ignoring (__name__, __unit__) rate(p_tx_total[5m])");
        add(&["sum"], &two, &b, &[&d]);
    }
    for b in [
        "rate(p_rx_total[5m]) * on (instance, device) group_left rate(p_tx_total[5m])",
        "rate(p_rx_total[5m]) > on (instance, device) group_right rate(p_tx_total[5m])",
    ] {
        add(&["sum"], &two, b, &[&d]);
    }
    // b4, b5, b6
    for op in ["*", "/", "<", "== bool"] {
        let b = format!(
            "rate(q_many_total[5m]) {op} on (instance) group_left (role) rate(q_one_total[5m])"
        );
        add(
            &five,
            &["by (instance) ", "without (device) ", "by (role) ", ""],
            &b,
            &[&v, &i],
        );
    }
    add(
        &["sum"],
        &["without (device) ", "by (role) "],
        "rate(q_many_total[5m]) - on (instance) group_left rate(q_one_total[5m])",
        &[&v],
    );
    for op in ["/", ">"] {
        let b =
            format!("rate(q_one_total[5m]) {op} on (instance) group_right rate(q_many_total[5m])");
        add(&five, &three, &b, &[&v, &i]);
    }
    // b7, b8, b10
    for b in [
        "rate(q_one_total[5m]) - on (instance) rate(q_peer_total[5m])",
        "rate(q_one_total[5m]) > ignoring (role, zone) rate(q_peer_total[5m])",
    ] {
        add(&["sum"], &["by (instance) ", "without (role) "], b, &[&v]);
    }
    add(
        &["sum"],
        &two,
        "rate(q_many_total[5m]) * on () group_left rate(q_glob_total[5m])",
        &[&v],
    );
    add(
        &["sum"],
        &["by (instance) ", "without (role) "],
        "rate(q_one_total[5m]) + on (__name__, instance) rate(q_one_total[5m])",
        &[&v],
    );
    // b12, b13, b11, b9
    add(
        &["sum"],
        &["by (role) ", "without (instance) "],
        "rate(q_empty_total{instance=\"i1\"}[5m]) * on (instance) group_left rate(q_one_total[5m])",
        &[&v],
    );
    add(
        &["sum"],
        &["by (role) "],
        "rate(q_empty_total{instance=\"i1\"}[5m]) > rate(q_emptyb_total[5m])",
        &[&v],
    );
    add(
        &["sum"],
        &["without (device) ", "by (role) "],
        "rate(q_many_total[5m]) < on (instance) group_left (__name__, role) rate(q_one_total[5m])",
        &[&v],
    );
    for b in [
        "irate(q_many_total[5m]) + on (instance) group_left (role) rate(q_one_total[5m])",
        "rate(q_one_total[5m]) + on (instance) group_right (role) irate(q_many_total[5m])",
    ] {
        add(&["sum"], &["by (role) "], b, &[&v]);
    }
    let mut failures = Vec::new();
    for (q, p) in &cases {
        failures.extend(pushed_against_in_process(&on, &off, q, p).await.2);
    }
    // Two names under `on (__name__, …)`: nothing matches.
    let q =
        "sum(rate(q_many_total[5m]) * on (__name__, instance) group_left rate(q_one_total[5m]))";
    let (a, b) = futures::join!(ask(&on, &t, q, &v), ask(&off, &t, q, &v));
    if !vector_or_matrix(&a).is_empty() || !vector_or_matrix(&b).is_empty() || a.pushed().is_empty()
    {
        failures.push(format!(
            "{q}: pushed {} cells, in process {}, pushed statements {}",
            vector_or_matrix(&a).len(),
            vector_or_matrix(&b).len(),
            a.pushed().len()
        ));
    }
    // In process only: what the rows pin.
    let s1 = ask(
        &off,
        &t,
        "sum without (device) (rate(p_rx_total[5m]) > on (instance, device) rate(p_tx_total[5m]))",
        &d,
    )
    .await;
    if groups_of(&s1).iter().any(|g| g.contains("__unit__")) {
        failures.push(format!("s1: a group with __unit__: {:?}", groups_of(&s1)));
    }
    let s2 = ask(
        &off,
        &t,
        "sum by (role) (rate(q_many_total[5m]) * on (instance) group_left (role) rate(q_one_total[5m]))",
        &v,
    )
    .await;
    let want: std::collections::BTreeSet<String> = [
        r#"[("role", "a2")]"#,
        r#"[("role", "b2")]"#,
        r#"[("role", "c")]"#,
        "[]",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    if groups_of(&s2) != want {
        failures.push(format!("s2: groups {:?}", groups_of(&s2)));
    }
    let s3 = ask(
        &off,
        &t,
        "sum without (device) (rate(q_many_total[5m]) * on (instance) group_left rate(q_one_total[5m]))",
        &v,
    )
    .await;
    let g3 = groups_of(&s3);
    if !g3
        .iter()
        .any(|g| g.contains(r#"("instance", "i2")"#) && g.contains(r#"("role", "m")"#))
        || g3
            .iter()
            .any(|g| g.contains(r#"("instance", "i3")"#) && g.contains("__unit__"))
    {
        failures.push(format!("s3: groups {g3:?}"));
    }
    let b12 = ask(
        &off,
        &t,
        "sum by (role) (rate(q_empty_total{instance=\"i1\"}[5m]) * on (instance) group_left rate(q_one_total[5m]))",
        &v,
    )
    .await;
    if groups_of(&b12) != ["[]".to_string()].into_iter().collect() {
        failures.push(format!("b12: in process, groups {:?}", groups_of(&b12)));
    }
    let b13 = ask(
        &off,
        &t,
        "sum by (role) (rate(q_empty_total{instance=\"i1\"}[5m]) > rate(q_emptyb_total[5m]))",
        &v,
    )
    .await;
    if groups_of(&b13) != ["[]".to_string()].into_iter().collect() {
        failures.push(format!("b13: in process, groups {:?}", groups_of(&b13)));
    }
    built.finish().await;
    assert!(
        failures.is_empty(),
        "T12 ({} cases):\n{}",
        cases.len(),
        failures.join("\n")
    );
}

/// The text of `query`'s error on `engine`, or a line saying it answered.
async fn error_of(engine: &MetricsEngine, query: &str, p: &MetricQueryParams) -> String {
    match engine
        .query(&no_tenant(), &parse(query).expect("parse"), p)
        .await
    {
        Ok(_) => "answered".to_string(),
        Err(e) => e.to_string(),
    }
}

/// Whether `sent` holds a statement satisfying `first`, and after it at
/// least two level fetches of today's route.
fn then_two_level_fetches(
    sent: &[(String, bool)],
    first: impl Fn(&(String, bool)) -> bool,
) -> bool {
    sent.iter().position(first).is_some_and(|at| {
        sent[at + 1..]
            .iter()
            // Issue #810: today's route for a routed selector is its one
            // statement (`routed_fetch`).
            .filter(|(q, _)| {
                q.starts_with("WITH ifNull((SELECT count() FROM") && q.contains("toUInt8(0) AS arm")
            })
            .count()
            >= 2
    })
}

/// **T13 (issue #579 part 5): a matching error takes today's route**: both
/// engines fail with the same text, and the pushed engine sent S3c, then
/// today's level fetches.
#[tokio::test(flavor = "multi_thread")]
async fn a_matching_error_takes_todays_route() {
    skip_unless_live!();
    let end = p_end();
    let db = DB_Q13.name();
    let built = corpus_pq(db, end).await;
    let off = built.engine(true, None).await;
    let v = range(end - 6 * HOUR, end, MIN);
    let d = range(end - 15 * HOUR, end, 2 * MIN);
    let rows: [(&str, &str, &MetricQueryParams, &[&str]); 9] = [
        (
            "a",
            "sum(rate(q_one_total[5m]) + on (instance) rate(q_many_total[5m]))",
            &v,
            &[
                "found duplicate series for the match group",
                "right hand-side",
            ],
        ),
        (
            "b",
            "sum(rate(q_many_total[5m]) + on (instance) rate(q_one_total[5m]))",
            &v,
            &["many-to-one matching must be explicit"],
        ),
        (
            "c",
            "sum(rate(q_many_total[5m]) * on (instance) group_left rate(q_one_total[5m]))",
            &d,
            &["{instance=\"i1\"} on the right hand-side"],
        ),
        (
            "d",
            "sum(rate(q_one_total[5m]) / on (instance) group_right rate(q_many_total[5m]))",
            &d,
            &["{instance=\"i1\"} on the left hand-side"],
        ),
        (
            "e",
            "sum(rate(q_many_total[5m]) * on (instance) group_left rate(q_dup_total[5m]))",
            &v,
            &["{instance=\"i9\"} on the right hand-side"],
        ),
        (
            "f",
            "sum(rate(q_many_total[5m]) * on (instance) group_left (device) rate(q_one_total[5m]))",
            &v,
            &["grouping labels must ensure unique matches"],
        ),
        (
            "g",
            "sum(rate(q_many_total[5m]) > on (instance) group_left (device) rate(q_one_total[5m]))",
            &v,
            &["grouping labels must ensure unique matches"],
        ),
        (
            "h",
            "sum(rate(q_many_total[5m]) > on (instance) rate(q_one_total[5m]))",
            &v,
            &["many-to-one matching must be explicit"],
        ),
        (
            "i",
            "sum by (role) (rate(q_empty_total{instance=\"i2\"}[5m]) * on (instance) group_left rate(q_one_total[5m]))",
            &v,
            &["grouping labels must ensure unique matches"],
        ),
    ];
    let mut failures = Vec::new();
    for (row, q, p, fragments) in rows {
        let comment = format!("{db}_t13{row}_{}", now_ms());
        let on = built.pushed_engine(Some(&comment)).await;
        let (a, b) = (error_of(&on, q, p).await, error_of(&off, q, p).await);
        if a != b {
            failures.push(format!("({row}): push on {a:?}, push off {b:?}"));
        }
        for f in fragments {
            if !b.contains(f) {
                failures.push(format!("({row}): {b:?} lacks {f:?}"));
            }
        }
        let sent = statements_in_order(&built.admin, &comment).await;
        if !then_two_level_fetches(&sent, |(q, _)| q.contains("CAST([('', 'match')]")) {
            let brief: Vec<String> = sent
                .iter()
                .map(|(q, failed)| format!("{failed}: {}", q.lines().next().unwrap_or("")))
                .collect();
            failures.push(format!(
                "({row}): no S3c then two level fetches: {brief:#?}"
            ));
        }
    }
    built.finish().await;
    assert!(failures.is_empty(), "T13:\n{}", failures.join("\n"));
}

/// The query of T14 and T15: `i0`'s two one-side series overlap where
/// the many side has no sample.
const T14_QUERY: &str = "sum by (instance) (rate(q_many_total[5m]) * on (instance) group_left (role) rate(q_one_total[5m]))";

/// **T14 (issue #579 part 5): a duplicate beside an empty side is
/// pushed.**
#[tokio::test(flavor = "multi_thread")]
async fn a_duplicate_beside_an_empty_side_is_pushed() {
    skip_unless_live!();
    let end = p_end();
    let built = corpus_pq(DB_Q14.name(), end).await;
    let on = built.pushed_engine(None).await;
    let off = built.engine(true, None).await;
    let t = no_tenant();
    let b = range(end - 14 * HOUR + MIN, end - 10 * HOUR, MIN);
    let (a, x, mut failures) = pushed_against_in_process(&on, &off, T14_QUERY, &b).await;
    if !a.declines().is_empty() {
        failures.push(format!("declines {:?}", a.declines()));
    }
    let count = ask(&off, &t, "count by (instance) (rate(q_one_total[5m]))", &b).await;
    let two = count
        .points()
        .iter()
        .any(|((l, _), v)| l.contains(r#"("instance", "i0")"#) && *v == 2.0);
    if !two {
        failures.push("in process, i0 never has two one-side series".to_string());
    }
    let near = x
        .points()
        .keys()
        .any(|(_, ts)| (ts - (end - 12 * HOUR)).abs() <= 10 * MIN);
    if near {
        failures.push("the binary answers within 10 minutes of end − 12 h".to_string());
    }
    built.finish().await;
    assert!(failures.is_empty(), "T14:\n{}", failures.join("\n"));
}

/// **T15 (issue #579 part 5, after #18 part 2b): a filled request pushes
/// its match node** in the bucket ends' read.
#[tokio::test(flavor = "multi_thread")]
async fn a_filled_request_pushes_its_match_node() {
    skip_unless_live!();
    let end = p_end();
    let built = corpus_pq(DB_Q15.name(), end).await;
    let on = built.pushed_engine(None).await;
    let off = built.engine(true, None).await;
    let t = no_tenant();
    let p = range(end - 6 * HOUR + 30_000, end - 30_000, 30_000);
    let (a, b) = futures::join!(ask(&on, &t, T14_QUERY, &p), ask(&off, &t, T14_QUERY, &p));
    let mut failures = Vec::new();
    if !a.declines().is_empty() {
        failures.push(format!("declines {:?}", a.declines()));
    }
    if !a
        .pushed()
        .iter()
        .any(|s| s.contains("60000 AS grid_step") && s.contains("CAST([('', 'match')]"))
    {
        failures.push(format!(
            "no S3c on the bucket ends: {} pushed",
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
    assert!(failures.is_empty(), "T15:\n{}", failures.join("\n"));
}

/// **T18 (issue #579 part 5, review r1): a match whose only include label
/// is `__name__` answers as in process**, under `group_left` and
/// `group_right`, arithmetic and comparison.
#[tokio::test(flavor = "multi_thread")]
async fn a_name_only_include_answers_as_in_process() {
    skip_unless_live!();
    let end = p_end();
    let built = corpus_pq(DB_Q18.name(), end).await;
    let on = built.pushed_engine(None).await;
    let off = built.engine(true, None).await;
    let v = range(end - 6 * HOUR, end, MIN);
    let mut failures = Vec::new();
    for b in [
        "rate(q_many_total[5m]) * on (instance) group_left (__name__) rate(q_one_total[5m])",
        "rate(q_one_total[5m]) * on (instance) group_right (__name__) rate(q_many_total[5m])",
        "rate(q_many_total[5m]) < on (instance) group_left (__name__) rate(q_one_total[5m])",
        "rate(q_one_total[5m]) > on (instance) group_right (__name__) rate(q_many_total[5m])",
    ] {
        for g in ["", "by (instance) ", "without (device) "] {
            let q = format!("sum {g}({b})");
            failures.extend(pushed_against_in_process(&on, &off, &q, &v).await.2);
        }
    }
    built.finish().await;
    assert!(failures.is_empty(), "T18:\n{}", failures.join("\n"));
}

// ---------------------------------------------------------------- #810
//
// Issue #810 part 1: a query read at a rollup level sends one statement per
// read, and that statement reads only the level.

static DB_T2: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_t810_2");
static DB_T8: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_t810_8");
static DB_T14: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_read_it_rollup_t810_14");

/// A per-test comment, unique to this run.
fn comment_for(db: &str, what: &str) -> String {
    format!("{db}_{what}_{}", now_ms())
}

/// Whether a logged statement read `table`.
fn reads_table(r: &LoggedRead, table: &str) -> bool {
    r.tables.iter().any(|t| t == table)
}

/// The tables of every logged statement, one line each.
fn brief(sent: &[LoggedRead]) -> Vec<String> {
    sent.iter()
        .map(|r| {
            format!(
                "{} rows, {:?}: {}",
                r.read_rows,
                r.tables,
                r.query.lines().next().unwrap_or("")
            )
        })
        .collect()
}

/// The text of `sql` with every gate condition of issue #810 replaced by
/// `1`: the cap gate, the level reads' `hist_n = 0` and the raw arms'
/// `hist_n > 0`.
fn ungated(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut rest = sql;
    let gate = "series_n * grid_n <= ";
    while let Some(at) = rest.find(gate) {
        out.push_str(&rest[..at]);
        out.push('1');
        let tail = &rest[at + gate.len()..];
        let digits = tail.chars().take_while(|c| c.is_ascii_digit()).count();
        rest = &tail[digits..];
    }
    out.push_str(rest);
    out.replace("hist_n = 0", "1").replace("hist_n > 0", "1")
}

/// The rows `sql` reads, sent on its own under the settings a routed
/// statement carries, from its `system.query_log` row.
async fn rows_read(admin: &ChClient, sql: &str, what: &str) -> u64 {
    // Unique across the tests that run at once over a shared database.
    let comment = format!("t810-gate-{what}-{}", uuid::Uuid::new_v4().simple());
    // The client binds `?`: written `??`, as the dispatcher escapes it.
    admin
        .execute(
            &sql.replace('?', "??"),
            &QuerySettings::new()
                .set("max_memory_usage", 4_294_967_296u64)
                .set("join_algorithm", "hash")
                .set("max_bytes_before_external_group_by", 0u64)
                .set("log_comment", comment.as_str()),
            Idempotency::Idempotent,
        )
        .await
        .unwrap_or_else(|e| panic!("{what}: {e}\nSQL:\n{sql}"));
    let sent = logged(admin, &comment).await;
    assert_eq!(sent.len(), 1, "{what}: one statement");
    sent[0].read_rows
}

/// The columns of each table a gate holds back, as the routed statements
/// read them.
fn gated_columns(table: &str) -> &'static str {
    if table == "metric_samples" {
        "org_id String, fingerprint UInt128, unix_milli Int64, value Float64"
    } else {
        "kind UInt8, org_id String, fingerprint UInt128, t_end Int64, t_last Int64, \
         last Float64, min Float64, max Float64, sum Float64, count UInt32, inc Float64, \
         flags UInt8"
    }
}

/// The rows `sql` reads from `{db}.{table}`: its rows read, less those of
/// the same text with that table replaced by an empty table of its
/// columns. The empty table is in a database of its own, so a shared
/// fixture database is never written to.
async fn rows_from(admin: &ChClient, db: &str, sql: &str, table: &str, what: &str) -> u64 {
    let scratch = format!("{db}_gate_{}", uuid::Uuid::new_v4().simple());
    exec(admin, &format!("CREATE DATABASE {scratch}")).await;
    exec(
        admin,
        &format!(
            "CREATE TABLE {scratch}.empty ({}) ENGINE = MergeTree ORDER BY tuple()",
            gated_columns(table)
        ),
    )
    .await;
    let from = format!("FROM {table}\n");
    assert!(
        sql.contains(&from),
        "{what}: the statement reads {table}:\n{sql}"
    );
    let swapped = sql.replace(&from, &format!("FROM {scratch}.empty\n"));
    let all = rows_read(admin, sql, what).await;
    let rest = rows_read(admin, &swapped, &format!("{what}-without")).await;
    exec(admin, &format!("DROP DATABASE {scratch} SYNC")).await;
    all.saturating_sub(rest)
}

/// The gate check (issue #810, plan section 6, as built): the rows `sql` as
/// sent reads from `{db}.{table}`, none, and the rows the same text with
/// every gate replaced by `1` reads from it, at least `x`, the rows the
/// gates hold back. Each statement is sent on its own, and again with the
/// table replaced by an empty copy, so the figures are that table's. The
/// two figures, and the failures.
async fn gate_check(
    admin: &ChClient,
    db: &str,
    sql: &str,
    table: &str,
    x: u64,
) -> (u64, u64, Vec<String>) {
    let gated = rows_from(admin, db, sql, table, "gated").await;
    let open = rows_from(admin, db, &ungated(sql), table, "ungated").await;
    let mut failures = Vec::new();
    if gated != 0 {
        failures.push(format!("{table}: {gated} rows read under the gate"));
    }
    if open < x {
        failures.push(format!(
            "{table}: {open} rows read without the gate, under the {x} it holds back"
        ));
    }
    println!(
        "gate check {db}.{table}: gated {gated} rows, ungated {open} rows, rows held back {x}"
    );
    (gated, open, failures)
}

/// One count from `sql`.
async fn count_of(admin: &ChClient, sql: &str) -> u64 {
    let v = ints(admin, &format!("SELECT toInt64(ifNull(({sql}), 0)) AS v")).await;
    u64::try_from(v[0]).expect("a count")
}

/// The data rows of `table` of the series of `metric` matching `extra`
/// with `t_end` in `(lo, hi]`.
async fn level_rows_of(
    admin: &ChClient,
    db: &str,
    table: &str,
    metric: &str,
    extra: &str,
    (lo, hi): (i64, i64),
) -> u64 {
    count_of(
        admin,
        &format!(
            "SELECT count() FROM {db}.{table} WHERE kind = 0 AND t_end > {lo} AND t_end <= {hi} \
             AND fingerprint IN (SELECT fingerprint FROM {db}.metric_labels \
                                 WHERE metric_name = '{metric}'{extra})"
        ),
    )
    .await
}

/// The raw samples of `metric` with `unix_milli` in `(lo, hi]`.
async fn raw_rows_of(admin: &ChClient, db: &str, metric: &str, (lo, hi): (i64, i64)) -> u64 {
    count_of(
        admin,
        &format!(
            "SELECT count() FROM {db}.metric_samples WHERE unix_milli > {lo} AND unix_milli <= {hi} \
             AND fingerprint IN (SELECT fingerprint FROM {db}.metric_labels \
                                 WHERE metric_name = '{metric}')"
        ),
    )
    .await
}

/// **T1 (issue #810): a panel at a level is one statement**, reading the
/// route's level and no raw sample.
#[tokio::test(flavor = "multi_thread")]
async fn a_panel_at_a_level_is_one_statement() {
    skip_unless_live!();
    let (built, end) = P.lease("a_panel_at_a_level_is_one_statement").await;
    let db = built.db().to_string();
    let mut failures = Vec::new();
    for (what, q, p, level, not) in [
        (
            "a",
            "sum by (instance) (rate(p_rx_total[1m]))",
            range(end - 3 * HOUR - 30_000, end - 30_000, SCRAPE),
            "metric_samples_60s",
            vec!["metric_samples"],
        ),
        (
            "b",
            "sum by (instance) (rate(p_rx_total[2m15s]))",
            range(end - DAY, end, 2 * MIN),
            "metric_samples_720s",
            vec!["metric_samples_60s", "metric_samples"],
        ),
    ] {
        let comment = comment_for(&db, &format!("t1{what}"));
        let e = built.pushed_engine(Some(&comment)).await;
        e.query(&no_tenant(), &parse(q).expect("parse"), &p)
            .await
            .expect("the panel");
        let sent = logged(&built.admin, &comment).await;
        if sent.len() != 1 {
            failures.push(format!(
                "({what}) {} statements: {:#?}",
                sent.len(),
                brief(&sent)
            ));
            continue;
        }
        if !reads_table(&sent[0], &format!("{db}.{level}")) {
            failures.push(format!("({what}) {level} not read: {:#?}", brief(&sent)));
        }
        for t in not {
            if reads_table(&sent[0], &format!("{db}.{t}")) {
                failures.push(format!("({what}) {t} read: {:#?}", brief(&sent)));
            }
        }
    }
    built.finish().await;
    assert!(failures.is_empty(), "T1:\n{}", failures.join("\n"));
}

/// Issue #810: one counter `c`, sampled every 15 s on the boundary over
/// `(end − 4 h, end]`, valued `1e6 +` the seconds since `end − 4 h`, its
/// first level built to `built_to`. The default levels; only the first is
/// built.
async fn one_counter(db: &str, end: i64, built_to: i64) -> Built {
    let lo = end - 4 * HOUR;
    let fp = "toUInt128('24000000000000000000000000000000810001')";
    let land = vec![
        format!(
            "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) \
             SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 2, 'c', {fp}, \
                    toInt64({lo}) + toInt64(number) * {HOUR} + 1000, '{{\"job\":\"c\"}}', 0 FROM numbers(5)"
        ),
        format!(
            "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
             SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 0, 'c', {fp}, \
                    toInt64({lo}) + toInt64(number) * {SCRAPE}, 1e6 + toFloat64(number) * 15 \
             FROM numbers(1, {})",
            4 * HOUR / SCRAPE
        ),
    ];
    let (bootstrap, admin, ctx) = first_only(db, DEFAULTS, &land, lo, built_to).await;
    warm(bootstrap, admin, ctx, &[]).await
}

/// The anchor of [`one_counter`]'s cases: a whole minute, an hour ago.
fn minute_end() -> i64 {
    (now_ms() / HOUR) * HOUR - HOUR
}

/// **E1 (issue #836): a panel read at a level ends at the newest built
/// bucket.** A counter rising 1 a second over 4 h, the first level built to
/// `W = end - 10 min`; `sum(rate(c[1m]))` over 8 h at 15 s, pushed and in
/// process: every step with its window inside the data and at or before
/// `W` is 1.0, every later step absent, one statement each.
#[tokio::test(flavor = "multi_thread")]
async fn a_panel_at_a_level_ends_at_the_newest_built_bucket() {
    skip_unless_live!();
    let end = minute_end();
    let w = end - 10 * MIN;
    let built = one_counter(DB_T2.name(), end, w).await;
    let db = built.db().to_string();
    let q = "sum(rate(c[1m]))";
    let p = range(end - 8 * HOUR, end, SCRAPE);
    // The data starts 4 h before `end`; a window wholly inside it.
    let first = end - 4 * HOUR + 10 * MIN;
    let t = no_tenant();
    let mut failures = Vec::new();
    for (name, pushed) in [("pushed", true), ("in process", false)] {
        let comment = comment_for(&db, &format!("e1{}", u8::from(pushed)));
        let e = if pushed {
            built.pushed_engine(Some(&comment)).await
        } else {
            built.engine(true, Some(&comment)).await
        };
        let points = ask(&e, &t, q, &p).await.points();
        for step in times_of(&p) {
            let got = points
                .iter()
                .find(|((_, at), _)| *at == step)
                .map(|(_, v)| *v);
            let ok = if step > w {
                got.is_none()
            } else if step >= first {
                got.is_some_and(|v| (v - 1.0).abs() <= 1e-12)
            } else {
                true
            };
            if !ok {
                failures.push(format!("{name} at {step} (W {w}): got {got:?}"));
            }
        }
        let sent = logged(&built.admin, &comment).await;
        if sent.len() != 1 {
            failures.push(format!("{name}: {} statements", sent.len()));
        }
    }
    built.finish().await;
    assert!(
        failures.is_empty(),
        "E1: {} failures, first 20:\n{}",
        failures.len(),
        failures
            .iter()
            .take(20)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}

static DB_E3: pulsus_testkit::TestDb = pulsus_testkit::TestDb::new("pulsus_read_it_rollup_t836_e3");

/// **E3 (issue #836): an instant query reads the level as of its newest
/// built bucket.** A gauge at 1 every 15 s over 9 h, one sample of 100 at
/// `T - 8 h - 2 min`, the first level built to `W = T - 4 min`; instant
/// `max_over_time(g[8h])` at `T`. The spike is inside the window ending at
/// `W` and outside the one ending at `T`: the answer is the routing-off
/// value at `W`, 100, with the request's time; one statement.
#[tokio::test(flavor = "multi_thread")]
async fn an_instant_query_reads_the_level_as_of_its_built_bucket() {
    skip_unless_live!();
    let at = minute_end();
    let w = at - 4 * MIN;
    let lo = at - 9 * HOUR;
    let spike = at - 8 * HOUR - 2 * MIN;
    let fp = "toUInt128('24000000000000000000000000000000836003')";
    let land = vec![
        format!(
            "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) \
             SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 2, 'g', {fp}, \
                    toInt64({lo}) + toInt64(number) * {HOUR} + 1000, '{{\"job\":\"g\"}}', 0 FROM numbers(10)",
            db = DB_E3.name()
        ),
        format!(
            "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
             SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 0, 'g', {fp}, \
                    toInt64({lo}) + toInt64(number) * {SCRAPE} AS ts, if(ts = {spike}, 100., 1.) \
             FROM numbers(1, {})",
            9 * HOUR / SCRAPE,
            db = DB_E3.name()
        ),
    ];
    let (bootstrap, admin, ctx) = first_only(DB_E3.name(), DEFAULTS, &land, lo, w).await;
    let built = warm(bootstrap, admin, ctx, &[]).await;
    let db = built.db().to_string();
    let q = "max_over_time(g[8h])";
    let t = no_tenant();
    let instant = |at: i64| MetricQueryParams {
        start_ms: at,
        end_ms: at,
        step_ms: 0,
    };
    let comment = comment_for(&db, "e3");
    let on = built.engine(true, Some(&comment)).await;
    let a = ask(&on, &t, q, &instant(at)).await;
    let sent = logged(&built.admin, &comment).await;
    let off = built.engine(false, None).await;
    let at_w = ask(&off, &t, q, &instant(w)).await.points();
    let at_t = ask(&off, &t, q, &instant(at)).await.points();
    built.finish().await;
    let mut failures = Vec::new();
    if !matches!(a.result, QueryResult::Vector(_)) {
        failures.push(format!(
            "not an instant vector, answered at the request's time: {:?}",
            a.result
        ));
    }
    let got: Vec<f64> = a.points().values().copied().collect();
    let want: Vec<f64> = at_w.values().copied().collect();
    if want != vec![100.0] || got != want {
        failures.push(format!("got {got:?}, routing off at W {want:?}"));
    }
    if at_t.values().copied().collect::<Vec<f64>>() != vec![1.0] {
        failures.push(format!(
            "routing off at T {at_t:?}: the case does not tell the two apart"
        ));
    }
    if sent.len() != 1 {
        failures.push(format!("{} statements", sent.len()));
    }
    assert!(failures.is_empty(), "E3:\n{}", failures.join("\n"));
}

/// **T3 (issue #810, D11): an in-process routed selector is one statement.**
#[tokio::test(flavor = "multi_thread")]
async fn an_in_process_routed_selector_is_one_statement() {
    skip_unless_live!();
    let (built, end) = P
        .lease("an_in_process_routed_selector_is_one_statement")
        .await;
    let db = built.db().to_string();
    let t = no_tenant();
    let mut failures = Vec::new();
    let q = "max_over_time(p_rx_total[1m])";
    let p = range(end - 6 * HOUR, end, MIN);
    let cold = Arc::new(LabelCache::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache_config(&db),
    ));
    let mut answers = Vec::new();
    for (what, q, p, warm_cache) in [
        ("a", q, p, true),
        ("b", q, p, false),
        (
            "c",
            "max_over_time(p_rx_total[3h])",
            range(end - 30 * MIN, end - 30 * MIN, 0),
            true,
        ),
    ] {
        let comment = comment_for(&db, &format!("t3{what}"));
        let e = if warm_cache {
            built.engine(true, Some(&comment)).await
        } else {
            MetricsEngine::new(
                built.client().await,
                Arc::clone(&cold),
                engine_config(&db, false),
            )
            .with_rollup_levels(Arc::clone(&built.slot))
            .with_query_log_comment(&comment)
        };
        let (result, _) = e
            .query(&t, &parse(q).expect("parse"), &p)
            .await
            .unwrap_or_else(|e| panic!("({what}): {e:?}"));
        answers.push(Answer {
            result,
            explain: PlanExplain::new("metrics"),
        });
        let sent = logged(&built.admin, &comment).await;
        if sent.len() != 1 {
            failures.push(format!(
                "({what}) {} statements: {:#?}",
                sent.len(),
                brief(&sent)
            ));
            continue;
        }
        let s = &sent[0];
        if !reads_table(s, &format!("{db}.metric_samples_60s")) {
            failures.push(format!(
                "({what}) the 60 s level not read: {:#?}",
                brief(&sent)
            ));
        }
        // Issue #836: the run rows read are those overlapping the window,
        // in the level arm's own scan.
        if !s
            .query
            .contains("OR kind = 1 AND org_id = '' AND fingerprint = 0 AND t_end > ")
        {
            failures.push(format!(
                "({what}) the run rows not read with the level's rows"
            ));
        }
        if what == "b" {
            if !reads_table(s, &format!("{db}.metric_labels")) {
                failures.push(format!("(b) the labels not read: {:#?}", brief(&sent)));
            }
        } else {
            // The fetch window: the range and the lookback before the
            // first step.
            let lo = if p.step_ms == 0 {
                p.start_ms - 3 * HOUR - LOOKBACK
            } else {
                p.start_ms - MIN - LOOKBACK
            };
            let x = raw_rows_of(&built.admin, &db, "p_rx_total", (lo, p.end_ms)).await;
            let (_, _, f) = gate_check(&built.admin, &db, &s.query, "metric_samples", x).await;
            failures.extend(f.into_iter().map(|f| format!("({what}) {f}")));
        }
    }
    if answers[0].cells() != answers[1].cells() || answers[0].cells().is_empty() {
        failures.push("(b) differs from (a)".to_string());
    }
    built.finish().await;
    assert!(failures.is_empty(), "T3:\n{}", failures.join("\n"));
}

/// **T8 (issue #810, D6): a level with no rows reads as `rollup`.** Only the
/// 60 s level is built; a 16 h range at 2 m is routed to 720 s.
#[tokio::test(flavor = "multi_thread")]
async fn a_level_with_no_rows_reads_as_rollup() {
    skip_unless_live!();
    let end = minute_end();
    let built = one_counter(DB_T8.name(), end, end - 10 * MIN).await;
    let p = range(end - 16 * HOUR, end, 2 * MIN);
    let t = no_tenant();
    let mut failures = Vec::new();
    for (name, e) in [
        ("pushed", built.pushed_engine(None).await),
        ("in process", built.engine(true, None).await),
    ] {
        for q in ["sum(rate(c[2m15s]))", "max_over_time(c[2m])"] {
            let a = ask(&e, &t, q, &p).await;
            if !a.points().is_empty() {
                failures.push(format!("{name} {q}: {} points", a.points().len()));
            }
            if !a.explain.rollup {
                failures.push(format!("{name} {q}: not marked rollup"));
            }
        }
    }
    built.finish().await;
    assert!(failures.is_empty(), "T8:\n{}", failures.join("\n"));
}

/// **T10 (issue #810, D10): a node the cache cannot list is one statement**,
/// and over the cap the first statement returns only the count, its level
/// reads gated off.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_the_cache_cannot_list_is_one_statement() {
    skip_unless_live!();
    let (built, end) = P
        .lease("a_node_the_cache_cannot_list_is_one_statement")
        .await;
    let db = built.db().to_string();
    let t = no_tenant();
    let q = "sum(rate(p_rx_total[2m15s]))";
    let p = range(end - 15 * HOUR, end, 2 * MIN);
    let warm = vector_or_matrix(&ask(&built.pushed_engine(None).await, &t, q, &p).await);
    let cold = Arc::new(LabelCache::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cache_config(&db),
    ));
    let mut failures = Vec::new();
    for (what, cap, want) in [("a", None, 1usize), ("b", Some(8_192usize), 3)] {
        let comment = comment_for(&db, &format!("t10{what}"));
        let mut e = MetricsEngine::new(
            built.client().await,
            Arc::clone(&cold),
            engine_config(&db, true),
        )
        .with_rollup_levels(Arc::clone(&built.slot))
        .with_query_log_comment(&comment);
        if let Some(cap) = cap {
            e = e.with_pushed_series_steps_cap(cap);
        }
        let a = ask(&e, &t, q, &p).await;
        if vector_or_matrix(&a) != warm || warm.is_empty() {
            failures.push(format!("({what}) differs from the warm pushed engine"));
        }
        let sent = logged(&built.admin, &comment).await;
        if sent.len() != want {
            failures.push(format!(
                "({what}) {} statements: {:#?}",
                sent.len(),
                brief(&sent)
            ));
            continue;
        }
        if what == "b" {
            // The fetch window: the 2 m15 s range rounded to 3 m (60 s × 3)
            // and the lookback, before the first step.
            let l = level_rows_of(
                &built.admin,
                &db,
                "metric_samples_60s",
                "p_rx_total",
                "",
                (p.start_ms - 3 * MIN - LOOKBACK, p.end_ms),
            )
            .await;
            let (_, _, f) =
                gate_check(&built.admin, &db, &sent[0].query, "metric_samples_60s", l).await;
            failures.extend(f.into_iter().map(|f| format!("(b) {f}")));
        }
    }
    built.finish().await;
    assert!(failures.is_empty(), "T10:\n{}", failures.join("\n"));
}

/// The fingerprint literal of issue #810's fixture `H`, series `n`.
fn h_fp(n: u32) -> String {
    format!("toUInt128('24000000000000000000000000000000810{n:03}')")
}

/// Corpus H (issue #810) ending at `end`, every 15 s over `(end − 4 h,
/// end]`: metric `c` with a float counter `{s="f"}`, a native-histogram
/// series `{s="h"}`, and `{s="m"}` with histogram samples over `(end − 4 h,
/// end − 2 h]` and float samples after; metrics `d` and `e`, two float
/// counters each, `{s="f"}` and `{s="h"}`.
fn corpus_h_land(db: &str, end: i64) -> Vec<String> {
    let lo = end - 4 * HOUR;
    let n = 4 * HOUR / SCRAPE;
    let mut land = Vec::new();
    let float = |fp: &str, name: &str, rate: f64, from: i64| {
        format!(
            "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
             SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 0, '{name}', {fp}, \
                    toInt64({lo}) + toInt64(number) * {SCRAPE}, 1e6 + toFloat64(number) * {rate} \
             FROM numbers({from}, {})",
            n + 1 - from
        )
    };
    let hist = |fp: &str, upto: i64| {
        format!(
            "INSERT INTO {db}.metric_hist_samples (org_id, fingerprint, unix_milli, schema, zero_threshold, zero_count, \
                                                   count, sum, pos_span_offsets, pos_span_lengths, pos_bucket_deltas, \
                                                   neg_span_offsets, neg_span_lengths, neg_bucket_deltas, custom_values) \
             SELECT '', {fp}, toInt64({lo}) + toInt64(number) * {SCRAPE}, 0, 0., 0, 4 + number, \
                    5. + toFloat64(number), [0], [3], [1, 1, -1], [], [], [], [] \
             FROM numbers(1, {upto})"
        )
    };
    for (k, (name, s)) in [
        ("c", "f"),
        ("c", "h"),
        ("c", "m"),
        ("d", "f"),
        ("d", "h"),
        ("e", "f"),
        ("e", "h"),
    ]
    .into_iter()
    .enumerate()
    {
        let fp = h_fp(k as u32 + 1);
        land.push(format!(
            "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) \
             SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 2, '{name}', {fp}, \
                    toInt64({lo}) + toInt64(number) * {HOUR} + 1000, '{{\"s\":\"{s}\"}}', 0 FROM numbers(5)"
        ));
        let rate = 15.0 * (k as f64 + 1.0);
        match (name, s) {
            ("c", "h") => land.push(hist(&fp, n)),
            ("c", "m") => {
                land.push(hist(&fp, n / 2));
                land.push(float(&fp, name, rate, n / 2 + 1));
            }
            _ => land.push(float(&fp, name, rate, 1)),
        }
    }
    land
}

/// Corpus H built at the default levels, its first level to `end`.
async fn corpus_h(db: &str, end: i64) -> Built {
    let (bootstrap, admin, ctx) =
        first_only(db, DEFAULTS, &corpus_h_land(db, end), end - 4 * HOUR, end).await;
    warm(bootstrap, admin, ctx, &[]).await
}

/// T16's range over corpus H: 3 h at 15 s, off the minute.
fn h_range(end: i64) -> MetricQueryParams {
    range(
        end - 3 * HOUR - 30 * MIN + SCRAPE,
        end - 30 * MIN + SCRAPE,
        SCRAPE,
    )
}

/// **T16 (issue #810, D9): a query over native histograms reads raw.**
#[tokio::test(flavor = "multi_thread")]
async fn a_query_over_native_histograms_reads_raw() {
    skip_unless_live!();
    let (built, end) = H.lease("a_query_over_native_histograms_reads_raw").await;
    let db = built.db().to_string();
    let p = h_range(end);
    let t = no_tenant();
    let off = built.engine(false, None).await;
    let mut failures = Vec::new();
    for (k, q) in [
        "rate(c[1m])",
        "c",
        "last_over_time(c[1m])",
        "absent_over_time(c{s=\"h\"}[1m])",
        "absent(c)",
        "sum by (s) (c)",
        "rate(d[1m])",
    ]
    .into_iter()
    .enumerate()
    {
        let comment = comment_for(&db, &format!("t16_{k}"));
        let on = built.engine(true, Some(&comment)).await;
        let (a, b) = futures::join!(ask(&on, &t, q, &p), ask(&off, &t, q, &p));
        let sent = logged(&built.admin, &comment).await;
        if sent.len() != 1 {
            failures.push(format!(
                "{q}: {} statements: {:#?}",
                sent.len(),
                brief(&sent)
            ));
        }
        if q.starts_with("rate(d") {
            if !a.explain.rollup {
                failures.push(format!("{q}: not marked rollup"));
            }
            if !a
                .stages("rollup_piece")
                .iter()
                .any(|n| n.contains("metric_samples_60s"))
            {
                failures.push(format!("{q}: rollup_piece {:?}", a.stages("rollup_piece")));
            }
            continue;
        }
        if a.explain.rollup {
            failures.push(format!("{q}: marked rollup"));
        }
        // `absent` and `absent_over_time` of a present series answer
        // nothing on either route.
        if vector_or_matrix(&a) != vector_or_matrix(&b)
            || (!q.starts_with("absent") && vector_or_matrix(&b).is_empty())
        {
            failures.push(format!("{q}: differs from routing off"));
        }
    }
    built.finish().await;
    assert!(failures.is_empty(), "T16:\n{}", failures.join("\n"));
}

/// **T17 (issue #810, D9): the histogram count spans the query.**
#[tokio::test(flavor = "multi_thread")]
async fn the_histogram_count_spans_the_query() {
    skip_unless_live!();
    let (built, end) = H.lease("the_histogram_count_spans_the_query").await;
    let db = built.db().to_string();
    let p = h_range(end);
    let t = no_tenant();
    let q = "last_over_time(c[1m]) or last_over_time(d[1m])";
    let comment = comment_for(&db, "t17");
    let on = built.engine(true, Some(&comment)).await;
    let off = built.engine(false, None).await;
    let (a, b) = futures::join!(ask(&on, &t, q, &p), ask(&off, &t, q, &p));
    let sent = logged(&built.admin, &comment).await;
    built.finish().await;
    let mut failures = Vec::new();
    if sent.len() != 2 {
        failures.push(format!("{} statements: {:#?}", sent.len(), brief(&sent)));
    }
    let pieces = a.stages("rollup_piece");
    if pieces.len() != 2 || !pieces.iter().all(|n| n.contains("read raw")) {
        failures.push(format!("rollup_piece {pieces:?}"));
    }
    if vector_or_matrix(&a) != vector_or_matrix(&b) || vector_or_matrix(&b).is_empty() {
        failures.push("differs from routing off".to_string());
    }
    assert!(failures.is_empty(), "T17:\n{}", failures.join("\n"));
}

/// **T18 (issue #810, D9): a pushed node over histograms reads raw.**
#[tokio::test(flavor = "multi_thread")]
async fn a_pushed_node_over_histograms_reads_raw() {
    skip_unless_live!();
    let (built, end) = H.lease("a_pushed_node_over_histograms_reads_raw").await;
    let db = built.db().to_string();
    let p = h_range(end);
    let t = no_tenant();
    let q = "sum by (s) (rate(c[1m]))";
    let comment = comment_for(&db, "t18");
    let on = built.pushed_engine(Some(&comment)).await;
    let off = built.engine(false, None).await;
    let (a, b) = futures::join!(ask(&on, &t, q, &p), ask(&off, &t, q, &p));
    let sent = logged(&built.admin, &comment).await;
    let mut failures = Vec::new();
    if sent.len() != 2 {
        failures.push(format!("{} statements: {:#?}", sent.len(), brief(&sent)));
    } else {
        // The lattice's fetch window: 5 m (the counter's five buckets) and
        // the lookback before the first bucket end, to the last step.
        let lo = ceil_to(p.start_ms, MIN) - 5 * MIN - LOOKBACK;
        let k = level_rows_of(
            &built.admin,
            &db,
            "metric_samples_60s",
            "c",
            "",
            (lo, p.end_ms),
        )
        .await;
        let (_, _, f) =
            gate_check(&built.admin, &db, &sent[0].query, "metric_samples_60s", k).await;
        failures.extend(f);
    }
    if vector_or_matrix(&a) != vector_or_matrix(&b) || vector_or_matrix(&b).is_empty() {
        failures.push("differs from routing off".to_string());
    }
    built.finish().await;
    assert!(failures.is_empty(), "T18:\n{}", failures.join("\n"));
}

/// **T21 (issue #810, addendum r6): a raw tail answers its tail.** A tail
/// over native histograms is read raw over the tail alone, in one statement.
#[tokio::test(flavor = "multi_thread")]
async fn a_raw_tail_answers_its_tail() {
    skip_unless_live!();
    let (built, end) = H.lease("a_raw_tail_answers_its_tail").await;
    let db = built.db().to_string();
    let whole = h_range(end);
    let tail = range(whole.end_ms - 10 * MIN, whole.end_ms, SCRAPE);
    let t = no_tenant();
    let q = "rate(c[1m])";
    let comment = comment_for(&db, "t21");
    let on = built.engine(true, Some(&comment)).await.with_request(whole);
    let expr = parse(q).expect("parse");
    let (result, _, facts) = on
        .query_with_facts(&t, &expr, &tail)
        .await
        .expect("the tail");
    let off = ask(&built.engine(false, None).await, &t, q, &tail).await;
    let sent = logged(&built.admin, &comment).await;
    built.finish().await;
    let mut failures = Vec::new();
    if sent.len() != 1 {
        failures.push(format!("{} statements: {:#?}", sent.len(), brief(&sent)));
    }
    if !facts.raw {
        failures.push("not read raw".to_string());
    }
    let a = Answer {
        result,
        explain: PlanExplain::new("metrics"),
    };
    if a.cells() != off.cells() || off.cells().is_empty() {
        failures.push("differs from routing off over the tail".to_string());
    }
    assert!(failures.is_empty(), "T21:\n{}", failures.join("\n"));
}

/// The four-hour range at 60 s of T25 and T27, on the minute.
fn h_four_hours(end: i64) -> MetricQueryParams {
    range(end - 4 * HOUR, end, MIN)
}

/// **T24 (issue #810): an unrouted join over histograms falls back.**
#[tokio::test(flavor = "multi_thread")]
async fn an_unrouted_join_over_histograms_falls_back() {
    skip_unless_live!();
    let (built, end) = H.lease("an_unrouted_join_over_histograms_falls_back").await;
    let p = range(end - HOUR, end, SCRAPE);
    let t = no_tenant();
    let q = "sum(rate(c[5m]) + rate(d[5m]))";
    let on = built.pushed_engine(None).await;
    let off = built.engine(false, None).await;
    let (a, b) = futures::join!(ask(&on, &t, q, &p), ask(&off, &t, q, &p));
    built.finish().await;
    assert!(
        a.declines().contains(&"declined: HistogramSamples"),
        "T24: declines {:?}",
        a.declines()
    );
    assert!(!vector_or_matrix(&b).is_empty(), "T24: an answer");
    assert_eq!(vector_or_matrix(&a), vector_or_matrix(&b), "T24");
}

/// **T25 (issue #810): a mixed join falls back on its raw side.**
#[tokio::test(flavor = "multi_thread")]
async fn a_mixed_join_falls_back_on_its_raw_side() {
    skip_unless_live!();
    let (built, end) = H.lease("a_mixed_join_falls_back_on_its_raw_side").await;
    let p = h_four_hours(end);
    let t = no_tenant();
    let q = "sum(rate(d[5m]) + irate(c[5m]))";
    let on = built.pushed_engine(None).await;
    let off = built.engine(true, None).await;
    let (a, b) = futures::join!(ask(&on, &t, q, &p), ask(&off, &t, q, &p));
    built.finish().await;
    assert!(
        a.declines().contains(&"declined: HistogramSamples"),
        "T25: declines {:?}",
        a.declines()
    );
    assert!(a.explain.rollup, "T25: d is read at its level");
    assert!(!vector_or_matrix(&b).is_empty(), "T25: an answer");
    assert_eq!(vector_or_matrix(&a), vector_or_matrix(&b), "T25");
}

/// A routed selector read again raw: the raw arms alone, no count.
fn is_raw_again(sql: &str) -> bool {
    sql.starts_with("SELECT arm, fingerprint, t, t_last") && !sql.contains("AS hist_n")
}

/// **T26 (issue #810, D9; code review r1): a level node is read again on
/// another selector's histograms.** `d`'s node counts the listed `c`'s
/// histograms beside its own series, none of its own: it keeps its answer,
/// which is discarded, and `d` is read again raw in one statement. Three
/// statements: the node, `c`'s, `d` again.
#[tokio::test(flavor = "multi_thread")]
async fn a_level_node_is_read_again_on_another_selectors_histograms() {
    skip_unless_live!();
    let (built, end) = H
        .lease("a_level_node_is_read_again_on_another_selectors_histograms")
        .await;
    let db = built.db().to_string();
    let p = h_range(end);
    let t = no_tenant();
    let q = "sum(rate(d[1m])) + count(last_over_time(c[1m]))";
    let comment = comment_for(&db, "t26");
    let on = built.pushed_engine(Some(&comment)).await;
    let off = built.engine(false, None).await;
    let (a, b) = futures::join!(ask(&on, &t, q, &p), ask(&off, &t, q, &p));
    let sent = logged(&built.admin, &comment).await;
    built.finish().await;
    let mut failures = Vec::new();
    if sent.len() != 3 {
        failures.push(format!("{} statements: {:#?}", sent.len(), brief(&sent)));
    }
    if sent.iter().filter(|r| is_raw_again(&r.query)).count() != 1 {
        failures.push(format!("not one raw re-read: {:#?}", brief(&sent)));
    }
    if a.declines().contains(&"declined: HistogramSamples") {
        failures.push("the node fell back".to_string());
    }
    if a.explain.rollup {
        failures.push("marked rollup".to_string());
    }
    if vector_or_matrix(&a) != vector_or_matrix(&b) || vector_or_matrix(&b).is_empty() {
        failures.push("differs from routing off".to_string());
    }
    assert!(failures.is_empty(), "T26:\n{}", failures.join("\n"));
}

/// **T42 (issue #810, code review r1): a mixed node is read again one
/// statement a side.** `d` at its level and `e` raw in one node; the listed
/// `c` holds the histograms, the node's own sides none. The node keeps its
/// answer, discarded; `d` and `e` are each read again raw in one statement
/// (`e`'s by its ID statement, as the node read it). Four statements: the
/// node, `c`'s, `d` again, `e` again.
#[tokio::test(flavor = "multi_thread")]
async fn a_mixed_node_is_read_again_one_statement_a_side() {
    skip_unless_live!();
    let (built, end) = H
        .lease("a_mixed_node_is_read_again_one_statement_a_side")
        .await;
    let db = built.db().to_string();
    let p = h_four_hours(end);
    let t = no_tenant();
    let q = "sum(rate(d[5m]) + irate(e[5m])) + count(last_over_time(c[1m]))";
    let comment = comment_for(&db, "t42");
    let on = built.pushed_engine(Some(&comment)).await;
    let off = built.engine(false, None).await;
    let (a, b) = futures::join!(ask(&on, &t, q, &p), ask(&off, &t, q, &p));
    let sent = logged(&built.admin, &comment).await;
    built.finish().await;
    let mut failures = Vec::new();
    if sent.len() != 4 {
        failures.push(format!("{} statements: {:#?}", sent.len(), brief(&sent)));
    }
    let again: Vec<&LoggedRead> = sent.iter().filter(|r| is_raw_again(&r.query)).collect();
    if again.len() != 2 {
        failures.push(format!("{} raw re-reads: {:#?}", again.len(), brief(&sent)));
    }
    // `e`'s re-read names its series by its ID statement, with the labels arm.
    if !again
        .iter()
        .any(|r| r.query.contains("toUInt8(2) AS arm") && r.query.contains("'e'"))
    {
        failures.push(format!(
            "no re-read of e by its ID statement: {:#?}",
            again.iter().map(|r| &r.query).collect::<Vec<_>>()
        ));
    }
    if a.declines().contains(&"declined: HistogramSamples") {
        failures.push("the node fell back".to_string());
    }
    if vector_or_matrix(&a) != vector_or_matrix(&b) || vector_or_matrix(&b).is_empty() {
        failures.push("differs from routing off".to_string());
    }
    assert!(failures.is_empty(), "T42:\n{}", failures.join("\n"));
}

/// **T27 (issue #810, D9): a raw-only node stays pushed in a raw query.**
#[tokio::test(flavor = "multi_thread")]
async fn a_raw_only_node_stays_pushed_in_a_raw_query() {
    skip_unless_live!();
    let (built, end) = H.lease("a_raw_only_node_stays_pushed_in_a_raw_query").await;
    let p = h_four_hours(end);
    let t = no_tenant();
    let q = "last_over_time(c[5m]) or sum(irate(d[5m]) + irate(e[5m]))";
    let on = built.pushed_engine(None).await;
    let off = built.engine(false, None).await;
    let (a, b) = futures::join!(ask(&on, &t, q, &p), ask(&off, &t, q, &p));
    built.finish().await;
    assert!(a.declines().is_empty(), "T27: declines {:?}", a.declines());
    assert!(!a.pushed().is_empty(), "T27: the join is pushed");
    assert!(!a.explain.rollup, "T27: read raw");
    assert!(!vector_or_matrix(&b).is_empty(), "T27: an answer");
    assert_eq!(vector_or_matrix(&a), vector_or_matrix(&b), "T27");
}

/// **T14 (issue #810, D3): nothing after the last step is read.**
#[tokio::test(flavor = "multi_thread")]
async fn nothing_after_the_last_step_is_read() {
    skip_unless_live!();
    let end = minute_end();
    let built = one_counter(DB_T14.name(), end, end).await;
    let db = built.db().to_string();
    let e_last = end - 30 * MIN + SCRAPE;
    let p = range(e_last - 3 * HOUR, e_last, SCRAPE);
    let q = "rate(c[1m])";
    let comment = comment_for(&db, "t14");
    let on = built.engine(true, Some(&comment)).await;
    let t = no_tenant();
    let a = ask(&on, &t, q, &p).await;
    let sent = logged(&built.admin, &comment).await;
    exec(
        &built.admin,
        &format!(
            "ALTER TABLE {db}.metric_samples_60s UPDATE inc = inc + 1000, last = last + 1000 \
             WHERE kind = 0 AND t_end = {} SETTINGS mutations_sync = 2",
            e_last + 45_000
        ),
    )
    .await;
    let b = ask(&built.engine(true, None).await, &t, q, &p).await;
    built.finish().await;
    let mut failures = Vec::new();
    if sent.len() != 1 {
        failures.push(format!("{} statements: {:#?}", sent.len(), brief(&sent)));
    } else if !sent[0].query.contains(&format!("t_end <= {e_last}")) {
        failures.push(format!(
            "the level arm does not end at {e_last}:\n{}",
            sent[0].query
        ));
    }
    if a.cells() != b.cells() {
        failures.push("A and B differ".to_string());
    }
    if !a.points().keys().any(|(_, at)| *at == e_last) {
        failures.push(format!("no point at {e_last}"));
    }
    assert!(failures.is_empty(), "T14:\n{}", failures.join("\n"));
}

/// **T15 (issue #810, D10): a listed and an unlisted side are gated over
/// the cap.**
#[tokio::test(flavor = "multi_thread")]
async fn a_listed_and_an_unlisted_side_are_gated_over_the_cap() {
    skip_unless_live!();
    let (built, end) = P
        .lease("a_listed_and_an_unlisted_side_are_gated_over_the_cap")
        .await;
    let db = built.db().to_string();
    let mut cfg = cache_config(&db);
    cfg.window_ms = 16 * DAY;
    cfg.cache_max_series = 20;
    let cache = Arc::new(LabelCache::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cfg,
    ));
    cache.refresh().await.expect("refresh");
    let comment = comment_for(&db, "t15");
    let on = MetricsEngine::new(built.client().await, cache, engine_config(&db, true))
        .with_rollup_levels(Arc::clone(&built.slot))
        .with_pushed_series_steps_cap(8_192)
        .with_query_log_comment(&comment);
    let q = "sum by (instance) (rate(p_rx_total[2m15s]) + rate(p_tx_total{instance=~\"i0|i1\"}[2m15s]))";
    let p = range(end - 15 * HOUR, end, 2 * MIN);
    let t = no_tenant();
    let off = built.engine(true, None).await;
    let (a, b) = futures::join!(ask(&on, &t, q, &p), ask(&off, &t, q, &p));
    let sent = logged(&built.admin, &comment).await;
    let mut failures = Vec::new();
    if sent.len() != 3 {
        failures.push(format!("{} statements: {:#?}", sent.len(), brief(&sent)));
    } else {
        let k = level_rows_of(
            &built.admin,
            &db,
            "metric_samples_60s",
            "p_tx_total",
            " AND JSONExtractString(labels, 'instance') IN ('i0', 'i1')",
            (p.start_ms - 3 * MIN - LOOKBACK, p.end_ms),
        )
        .await;
        let (_, _, f) =
            gate_check(&built.admin, &db, &sent[0].query, "metric_samples_60s", k).await;
        failures.extend(f);
    }
    if vector_or_matrix(&a) != vector_or_matrix(&b) || vector_or_matrix(&b).is_empty() {
        failures.push("differs from the in-process engine".to_string());
    }
    built.finish().await;
    assert!(failures.is_empty(), "T15:\n{}", failures.join("\n"));
}

// ------------------------------------------------- #810, addendum r6

static DB_T32: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_read_it_rollup_t810_32");

/// The `fingerprint IN (…)` list of a routed statement's level arm.
fn level_arm_line(sql: &str) -> &str {
    sql.lines()
        .find(|l| l.contains("WHERE kind = 0"))
        .unwrap_or("")
}

/// The text of a routed statement's `hist_n` clause.
fn hist_n_clause(sql: &str) -> &str {
    sql.split_once(") AS hist_n").map_or("", |(head, _)| head)
}

/// **T31 (issue #810, addendum r6): a count past the text cap drops the
/// other lists.** `d`'s statement fits the cap only without `c`'s list: it
/// counts its own series alone and reads its level; `c`'s counts its
/// histograms; `d` is read again raw.
#[tokio::test(flavor = "multi_thread")]
async fn a_count_past_the_text_cap_drops_other_lists() {
    skip_unless_live!();
    let (built, end) = H.lease("a_count_past_the_text_cap_drops_other_lists").await;
    let db = built.db().to_string();
    let p = h_range(end);
    let t = no_tenant();
    let q = "last_over_time(d[1m]) or last_over_time(c[1m])";
    // `d`'s statement as an engine with the default cap renders it.
    let measure = comment_for(&db, "t31m");
    ask(&built.engine(true, Some(&measure)).await, &t, q, &p).await;
    let first = logged(&built.admin, &measure).await;
    let d_len = first
        .iter()
        .find(|r| level_arm_line(&r.query).contains("810004'"))
        .map(|r| r.query.len() as u64)
        .expect("d's statement");
    let comment = comment_for(&db, "t31");
    let on = built
        .engine(true, Some(&comment))
        .await
        .with_statement_cap(d_len);
    let off = built.engine(false, None).await;
    let (a, b) = futures::join!(ask(&on, &t, q, &p), ask(&off, &t, q, &p));
    let sent = logged(&built.admin, &comment).await;
    built.finish().await;
    let mut failures = Vec::new();
    if sent.len() != 3 {
        failures.push(format!("{} statements: {:#?}", sent.len(), brief(&sent)));
    }
    let d_first: Vec<&LoggedRead> = sent
        .iter()
        .filter(|r| level_arm_line(&r.query).contains("810004'"))
        .collect();
    if d_first.len() != 1
        || d_first[0].query.contains("810001'")
        || d_first[0].query.contains("FROM metric_series")
    {
        failures.push(format!(
            "d's statement names c, or its ID statement: {:#?}",
            d_first.iter().map(|r| &r.query).collect::<Vec<_>>()
        ));
    }
    if sent
        .iter()
        .filter(|r| !r.query.contains("toUInt8(0) AS arm"))
        .count()
        != 1
    {
        failures.push(format!("no one raw re-read: {:#?}", brief(&sent)));
    }
    if vector_or_matrix(&a) != vector_or_matrix(&b) || vector_or_matrix(&b).is_empty() {
        failures.push("differs from routing off".to_string());
    }
    assert!(failures.is_empty(), "T31:\n{}", failures.join("\n"));
}

/// **T32 (issue #810, addendum r6): one resolution serves the count and the
/// read.** A series of `c` lands, and the label cache lists it, after the
/// read resolved its selectors: the statement's read and its count name the
/// same list, the one resolved, without it.
#[tokio::test(flavor = "multi_thread")]
async fn one_resolution_serves_count_and_read() {
    skip_unless_live!();
    let end = p_end();
    let built = corpus_h(DB_T32.name(), end).await;
    let db = built.db().to_string();
    let lo = end - 4 * HOUR;
    let fp = h_fp(9);
    let land = vec![
        format!(
            "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, labels, value_type) \
             SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 2, 'c', {fp}, \
                    toInt64({lo}) + toInt64(number) * {HOUR} + 1000, '{{\"s\":\"n\"}}', 0 FROM numbers(5)"
        ),
        format!(
            "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
             SELECT '', toInt64(toUnixTimestamp64Milli(now64(3))), 0, 'c', {fp}, \
                    toInt64({lo}) + toInt64(number) * {SCRAPE}, toFloat64(number) \
             FROM numbers(1, {})",
            4 * HOUR / SCRAPE
        ),
    ];
    let cache = Arc::clone(&built.cache);
    let config = test_config(&db);
    let hook: pulsus_read::metrics::exec::AfterResolution = Arc::new(move || {
        let (cache, config, land) = (Arc::clone(&cache), config.clone(), land.clone());
        Box::pin(async move {
            let c = ChClient::new(config).await.expect("connect");
            for sql in &land {
                exec(&c, sql).await;
            }
            cache.refresh().await.expect("refresh");
        })
    });
    let comment = comment_for(&db, "t32");
    let on = built
        .engine(true, Some(&comment))
        .await
        .with_after_resolution(hook);
    let p = h_range(end);
    ask(&on, &no_tenant(), "last_over_time(c[1m])", &p).await;
    let sent = logged(&built.admin, &comment).await;
    let later = comment_for(&db, "t32later");
    ask(
        &built.engine(true, Some(&later)).await,
        &no_tenant(),
        "last_over_time(c[1m])",
        &p,
    )
    .await;
    let after = logged(&built.admin, &later).await;
    built.finish().await;
    let mut failures = Vec::new();
    if sent.len() != 1 {
        failures.push(format!("{} statements: {:#?}", sent.len(), brief(&sent)));
    } else {
        let sql = &sent[0].query;
        let arm = level_arm_line(sql);
        let list = arm
            .split_once("fingerprint IN (")
            .and_then(|(_, rest)| rest.split_once(") AND hist_n"))
            .map(|(l, _)| l)
            .unwrap_or("");
        let count = hist_n_clause(sql);
        if list.is_empty() || !count.contains(&format!("fingerprint IN ({list})")) {
            failures.push(format!("the count does not name the read's list:\n{sql}"));
        }
        if count.contains("FROM metric_series") {
            failures.push("the count reads an ID statement".to_string());
        }
        if sql.contains("810009'") {
            failures.push("the series landed after the resolution is named".to_string());
        }
    }
    if !after.iter().any(|r| r.query.contains("810009'")) {
        failures.push("the landed series never reached the label cache".to_string());
    }
    assert!(failures.is_empty(), "T32:\n{}", failures.join("\n"));
}

/// T37's engine: corpus H's database with a label cache that lists at most
/// two series a selector, so `d` (2) is listed and `c` (3) is not.
async fn two_series_engine(built: &Built, comment: &str) -> MetricsEngine {
    let db = built.db().to_string();
    let mut cfg = cache_config(&db);
    cfg.window_ms = 16 * DAY;
    cfg.cache_max_series = 2;
    let cache = Arc::new(LabelCache::new(
        ChClient::new(test_config(&db)).await.expect("connect"),
        cfg,
    ));
    cache.refresh().await.expect("refresh");
    MetricsEngine::new(built.client().await, cache, engine_config(&db, true))
        .with_rollup_levels(Arc::clone(&built.slot))
        .with_query_log_comment(comment)
}

const T37_QUERY: &str = "sum(rate(d[1m])) + count(last_over_time(c[1m]))";

/// **T37 (issue #810, addendum r6): a kept node is read again raw.** `d`'s
/// node counts `d` alone and keeps its level answer; `c`'s statement counts
/// its histograms; `d` is read again raw.
#[tokio::test(flavor = "multi_thread")]
async fn a_kept_node_is_read_again_raw() {
    skip_unless_live!();
    let (built, end) = H.lease("a_kept_node_is_read_again_raw").await;
    let db = built.db().to_string();
    let p = h_range(end);
    let t = no_tenant();
    let comment = comment_for(&db, "t37");
    let on = two_series_engine(&built, &comment).await;
    let off = built.engine(false, None).await;
    let (a, b) = futures::join!(ask(&on, &t, T37_QUERY, &p), ask(&off, &t, T37_QUERY, &p));
    let sent = logged(&built.admin, &comment).await;
    built.finish().await;
    let mut failures = Vec::new();
    if sent.len() != 3 {
        failures.push(format!("{} statements: {:#?}", sent.len(), brief(&sent)));
    }
    if a.explain.rollup {
        failures.push("marked rollup".to_string());
    }
    if vector_or_matrix(&a) != vector_or_matrix(&b) || vector_or_matrix(&b).is_empty() {
        failures.push("differs from routing off".to_string());
    }
    assert!(failures.is_empty(), "T37:\n{}", failures.join("\n"));
}

/// **T38 (issue #810, addendum r6): a kept node is read again over its
/// tail.** T37's engine with its request; the tail of its last 10 minutes.
#[tokio::test(flavor = "multi_thread")]
async fn a_kept_node_is_read_again_over_its_tail() {
    skip_unless_live!();
    let (built, end) = H.lease("a_kept_node_is_read_again_over_its_tail").await;
    let db = built.db().to_string();
    let whole = h_range(end);
    let tail = range(whole.end_ms - 10 * MIN, whole.end_ms, SCRAPE);
    let t = no_tenant();
    let comment = comment_for(&db, "t38");
    let on = two_series_engine(&built, &comment)
        .await
        .with_request(whole);
    let expr = parse(T37_QUERY).expect("parse");
    let (result, _, facts) = on
        .query_with_facts(&t, &expr, &tail)
        .await
        .expect("the tail");
    let off = ask(&built.engine(false, None).await, &t, T37_QUERY, &tail).await;
    let sent = logged(&built.admin, &comment).await;
    built.finish().await;
    let mut failures = Vec::new();
    if sent.len() != 3 {
        failures.push(format!("{} statements: {:#?}", sent.len(), brief(&sent)));
    }
    if !facts.raw {
        failures.push("not read raw".to_string());
    }
    let a = Answer {
        result,
        explain: PlanExplain::new("metrics"),
    };
    if vector_or_matrix(&a) != vector_or_matrix(&off) || vector_or_matrix(&off).is_empty() {
        failures.push("differs from routing off over the tail".to_string());
    }
    assert!(failures.is_empty(), "T38:\n{}", failures.join("\n"));
}
