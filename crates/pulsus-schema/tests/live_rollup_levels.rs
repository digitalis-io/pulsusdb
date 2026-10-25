//! Issue #18 part A: the rollup levels, built by their timed views, on one
//! node. Each case builds its own database, then drives the views itself:
//! after the build every level view's creation run is waited for, the view
//! stopped and its table emptied, and a run is `SYSTEM START VIEW`,
//! `SYSTEM REFRESH VIEW`, `SYSTEM WAIT VIEW`, `SYSTEM STOP VIEW` (a stopped
//! view ignores a manual refresh). Starting a view whose scheduled time has
//! passed also runs it, so one such run may be two; every assertion below
//! holds either way, because a second run starts from the first one's
//! watermark.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, like `live_schema.rs`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-schema --test live_rollup_levels
//! ```
//!
//! The expected rows are computed here, in plain Rust, from the samples the
//! server stores (the first level) or the rows of the level below (every
//! other level). Floats are compared as bits, except `sum` and `inc`, which
//! differ in addition order and are compared within 1e-9 relative.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, ChRow, Idempotency, QuerySettings, Row};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const MINUTE: i64 = 60_000;
const HOUR: i64 = 3_600_000;
const DAY: i64 = 86_400_000;
/// The first level's lookback for each series' previous sample.
const LOOKBACK: i64 = 120_000;

static DB_AGGREGATES: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_levels_aggregates");
static DB_EMPTY: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_levels_empty");
static DB_HOLE_LANDING: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_levels_hole_landing");
static DB_HOLE_SOURCE: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_levels_hole_source");
static DB_WINDOW: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_levels_window");
static DB_WINDOW_UP: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_levels_window_up");
static DB_WAITS: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_levels_waits");
static DB_MIDNIGHT: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_levels_midnight");
static DB_EXPIRY: pulsus_testkit::TestDb =
    pulsus_testkit::TestDb::new("pulsus_schema_it_levels_expiry");

/// `true` when the gated half of this suite should run. Skips cleanly on a
/// developer machine with no container; **panics** rather than skipping when
/// the gate is absent in a live CI job (issue #320).
fn should_run() -> bool {
    pulsus_testkit::live_clickhouse_enabled()
}

fn test_config() -> ChConnConfig {
    ChConnConfig {
        server: std::env::var("PULSUS_TEST_CH_HOST").unwrap_or_else(|_| "localhost".to_string()),
        http_port: std::env::var("PULSUS_TEST_CH_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(19123),
        database: std::env::var("PULSUS_TEST_CH_DATABASE")
            .unwrap_or_else(|_| "default".to_string()),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(60),
        ..ChConnConfig::default()
    }
}

macro_rules! skip_unless_live {
    () => {
        if !should_run() {
            eprintln!(
                "skipping: set PULSUS_TEST_CLICKHOUSE=1 with a live ClickHouse to run this test \
                 (see crates/pulsus-schema/tests/live_rollup_levels.rs)"
            );
            return;
        }
    };
}

async fn exec(client: &ChClient, sql: &str) {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await
        .unwrap_or_else(|e| panic!("execute failed: {e}\nSQL:\n{sql}"));
}

async fn rows<R: ChRow>(client: &ChClient, sql: &str) -> Vec<R> {
    let mut stream = client
        .query_stream::<R>(sql, &QuerySettings::new())
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\nSQL:\n{sql}"));
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row.unwrap_or_else(|e| panic!("decode failed: {e}\nSQL:\n{sql}")));
    }
    out
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct IntRow {
    v: i64,
}

async fn int(client: &ChClient, sql: &str) -> i64 {
    rows::<IntRow>(client, sql)
        .await
        .first()
        .unwrap_or_else(|| panic!("no row: {sql}"))
        .v
}

/// The server's clock, in milliseconds.
async fn now_ms(client: &ChClient) -> i64 {
    int(
        client,
        "SELECT toInt64(toUnixTimestamp64Milli(now64())) AS v",
    )
    .await
}

async fn drop_db(client: &ChClient, db: &str) {
    exec(client, &format!("DROP DATABASE IF EXISTS {db} SYNC")).await;
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct NameRow {
    name: String,
}

/// Builds `ctx` fresh and readies its levels for the test's own runs: the
/// level views must exist, each one's creation run is waited for, the view
/// is stopped, and its table emptied.
async fn fresh(client: &ChClient, ctx: &RenderCtx) {
    let db = &ctx.db;
    drop_db(client, db).await;
    run_init(client, ctx).await.expect("run_init");
    let got: Vec<String> = rows::<NameRow>(
        client,
        &format!(
            "SELECT name FROM system.tables WHERE database = '{db}' \
             AND match(name, '^metric_samples_[0-9]+s_mv$') ORDER BY name"
        ),
    )
    .await
    .into_iter()
    .map(|r| r.name)
    .collect();
    let mut want: Vec<String> = ctx
        .rollup_levels
        .iter()
        .map(|r| format!("metric_samples_{r}s_mv"))
        .collect();
    want.sort();
    assert_eq!(got, want, "one view per rollup level");
    for r in &ctx.rollup_levels {
        exec(
            client,
            &format!("SYSTEM WAIT VIEW {db}.metric_samples_{r}s_mv"),
        )
        .await;
        exec(
            client,
            &format!("SYSTEM STOP VIEW {db}.metric_samples_{r}s_mv"),
        )
        .await;
    }
    for r in &ctx.rollup_levels {
        exec(client, &format!("TRUNCATE TABLE {db}.metric_samples_{r}s")).await;
    }
}

/// One run of level `r`'s view.
async fn run(client: &ChClient, db: &str, r: u32) {
    let view = format!("{db}.metric_samples_{r}s_mv");
    exec(client, &format!("SYSTEM START VIEW {view}")).await;
    exec(client, &format!("SYSTEM REFRESH VIEW {view}")).await;
    exec(client, &format!("SYSTEM WAIT VIEW {view}")).await;
    exec(client, &format!("SYSTEM STOP VIEW {view}")).await;
}

#[derive(
    Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord,
)]
struct RunRow {
    t_last: i64,
    t_end: i64,
    built_at: i64,
}

/// Level `r`'s run rows, oldest first.
async fn run_rows(client: &ChClient, db: &str, r: u32) -> Vec<RunRow> {
    rows(
        client,
        &format!(
            "SELECT t_last, t_end, built_at FROM {db}.metric_samples_{r}s \
             WHERE kind = 1 ORDER BY t_last, t_end, built_at"
        ),
    )
    .await
}

/// The union of `(t_last, t_end]` intervals, adjacent ones merged.
fn union(intervals: impl IntoIterator<Item = (i64, i64)>) -> Vec<(i64, i64)> {
    let mut all: Vec<(i64, i64)> = intervals.into_iter().collect();
    all.sort();
    let mut out: Vec<(i64, i64)> = Vec::new();
    for (lo, hi) in all {
        match out.last_mut() {
            Some(last) if lo <= last.1 => last.1 = last.1.max(hi),
            _ => out.push((lo, hi)),
        }
    }
    out
}

/// What one run of a level did: its run-row count and largest `t_end`
/// after the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AfterRun {
    run_rows: usize,
    watermark: i64,
}

/// Runs level `r` until a run leaves its count of run rows unchanged, at
/// most `max` times, and returns what each run left.
async fn until_idle(client: &ChClient, db: &str, r: u32, max: usize) -> Vec<AfterRun> {
    let mut before = run_rows(client, db, r).await.len();
    let mut history = Vec::new();
    for _ in 0..max {
        run(client, db, r).await;
        let now = run_rows(client, db, r).await;
        let after = AfterRun {
            run_rows: now.len(),
            watermark: now.iter().map(|x| x.t_end).max().unwrap_or(0),
        };
        history.push(after);
        if after.run_rows == before {
            return history;
        }
        before = after.run_rows;
    }
    panic!("level {r}s still writes run rows after {max} runs: {history:?}");
}

/// One data row of a level, floats as bits where they compare as bits.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
struct LevelRow {
    fingerprint: u128,
    t_end: i64,
    t_last: i64,
    last_bits: u64,
    min_bits: u64,
    max_bits: u64,
    sum: f64,
    count: u32,
    inc: f64,
}

/// Level `r`'s data rows, by series and bucket.
async fn data_rows(client: &ChClient, db: &str, r: u32) -> Vec<LevelRow> {
    rows(
        client,
        &format!(
            "SELECT fingerprint, t_end, t_last, reinterpretAsUInt64(last) AS last_bits, \
             reinterpretAsUInt64(min) AS min_bits, reinterpretAsUInt64(max) AS max_bits, \
             sum, count, inc FROM {db}.metric_samples_{r}s WHERE kind = 0 \
             ORDER BY fingerprint, t_end"
        ),
    )
    .await
}

/// `a` and `b` within 1e-9 of the larger.
fn close(a: f64, b: f64) -> bool {
    a == b || (a - b).abs() <= 1e-9 * a.abs().max(b.abs())
}

/// `got` against `want`, row by row: every field as bits, `sum` and `inc`
/// within 1e-9 relative.
fn assert_rows_match(got: &[LevelRow], want: &[LevelRow], what: &str) {
    let key = |r: &LevelRow| (r.fingerprint, r.t_end);
    let got_keys: Vec<_> = got.iter().map(key).collect();
    let want_keys: Vec<_> = want.iter().map(key).collect();
    assert_eq!(got_keys, want_keys, "{what}: the series and buckets");
    for (g, w) in got.iter().zip(want) {
        let same = g.t_last == w.t_last
            && g.last_bits == w.last_bits
            && g.min_bits == w.min_bits
            && g.max_bits == w.max_bits
            && g.count == w.count
            && close(g.sum, w.sum)
            && close(g.inc, w.inc);
        assert!(same, "{what}: got {g:?}, want {w:?}");
    }
}

/// One stored sample: series, time, value bits.
type Sample = (u128, i64, u64);

/// The first level's rows for `(lo, hi]`, from `samples`: exact repeats
/// dropped; each series sorted by `(time, value bits)`; per bucket of
/// `level_ms`, `count`, `sum`, `min` and `max` over the non-NaN values,
/// `last` the last value and `t_last` its time, and `inc` the sum over the
/// bucket's samples of `v − q` when `v ≥ q`, else `v`, where `q` is the
/// series' previous sample, taken only when it is no more than 120,000 ms
/// before `lo` (no term for the first sample of a window otherwise).
fn expected_first(samples: &[Sample], lo: i64, hi: i64, level_ms: i64) -> Vec<LevelRow> {
    let distinct: BTreeSet<Sample> = samples.iter().copied().collect();
    let mut by_series: BTreeMap<u128, Vec<(i64, u64)>> = BTreeMap::new();
    for (fp, t, bits) in distinct {
        by_series.entry(fp).or_default().push((t, bits));
    }
    let mut out = Vec::new();
    for (fp, mut series) in by_series {
        series.sort();
        let mut buckets: BTreeMap<i64, LevelRow> = BTreeMap::new();
        for (i, &(t, bits)) in series.iter().enumerate() {
            if t <= lo || t > hi {
                continue;
            }
            let v = f64::from_bits(bits);
            let q = i
                .checked_sub(1)
                .map(|p| series[p])
                .filter(|&(tq, _)| tq > lo - LOOKBACK)
                .map(|(_, b)| f64::from_bits(b));
            let d = match q {
                Some(q) if !v.is_nan() && !q.is_nan() => {
                    if v >= q {
                        v - q
                    } else {
                        v
                    }
                }
                _ => 0.0,
            };
            let t_end = ((t - 1).div_euclid(level_ms) + 1) * level_ms;
            let row = buckets.entry(t_end).or_insert(LevelRow {
                fingerprint: fp,
                t_end,
                t_last: t,
                last_bits: bits,
                min_bits: 0f64.to_bits(),
                max_bits: 0f64.to_bits(),
                sum: 0.0,
                count: 0,
                inc: 0.0,
            });
            row.t_last = t;
            row.last_bits = bits;
            row.inc += d;
            if !v.is_nan() {
                if row.count == 0 {
                    row.min_bits = bits;
                    row.max_bits = bits;
                } else {
                    if v < f64::from_bits(row.min_bits) {
                        row.min_bits = bits;
                    }
                    if v > f64::from_bits(row.max_bits) {
                        row.max_bits = bits;
                    }
                }
                row.sum += v;
                row.count += 1;
            }
        }
        out.extend(buckets.into_values());
    }
    out
}

/// A coarser level's rows from `rows` of the level below: per series and
/// bucket of `level_ms`, `count`, `sum` and `inc` summed, `min` and `max`
/// over the rows with `count > 0`, and `last` of the row with the largest
/// `t_last`.
fn expected_up(rows: &[LevelRow], level_ms: i64) -> Vec<LevelRow> {
    let mut buckets: BTreeMap<(u128, i64), LevelRow> = BTreeMap::new();
    for r in rows {
        let t_end = ((r.t_end - 1).div_euclid(level_ms) + 1) * level_ms;
        let row = buckets.entry((r.fingerprint, t_end)).or_insert(LevelRow {
            fingerprint: r.fingerprint,
            t_end,
            t_last: r.t_last,
            last_bits: r.last_bits,
            min_bits: 0f64.to_bits(),
            max_bits: 0f64.to_bits(),
            sum: 0.0,
            count: 0,
            inc: 0.0,
        });
        if r.t_last >= row.t_last {
            row.t_last = r.t_last;
            row.last_bits = r.last_bits;
        }
        if r.count > 0 {
            let (mn, mx) = (f64::from_bits(r.min_bits), f64::from_bits(r.max_bits));
            let first = row.count == 0;
            if first || mn < f64::from_bits(row.min_bits) {
                row.min_bits = r.min_bits;
            }
            if first || mx > f64::from_bits(row.max_bits) {
                row.max_bits = r.max_bits;
            }
        }
        row.sum += r.sum;
        row.count += r.count;
        row.inc += r.inc;
    }
    buckets.into_values().collect()
}

/// The samples `metric_landing` stores, as the first level reads them.
async fn landed(client: &ChClient, db: &str) -> Vec<Sample> {
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct LandedRow {
        fingerprint: u128,
        unix_milli: i64,
        bits: u64,
    }
    rows::<LandedRow>(
        client,
        &format!(
            "SELECT fingerprint, unix_milli, reinterpretAsUInt64(value) AS bits \
             FROM {db}.metric_landing WHERE kind = 0"
        ),
    )
    .await
    .into_iter()
    .map(|r| (r.fingerprint, r.unix_milli, r.bits))
    .collect()
}

/// One `metric_landing` insert of `samples`, each received five seconds
/// after its time. The value goes in as its bits, so it lands exactly.
fn landing_insert(db: &str, samples: &[Sample]) -> String {
    let tuples: Vec<String> = samples
        .iter()
        .map(|(fp, t, bits)| format!("({fp}, {t}, {bits})"))
        .collect();
    format!(
        "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
         SELECT '', t + 5000, 0, '', fp, t, reinterpretAsFloat64(b) \
         FROM values('fp UInt128, t Int64, b UInt64', {})",
        tuples.join(", ")
    )
}

/// Hand-made rows into level `r`: data rows as given, floats as bits, and
/// run rows `(t_last, t_end]`, in statements of at most 300 rows, each well
/// inside the server's query size.
async fn insert_level(client: &ChClient, db: &str, r: u32, data: &[LevelRow], runs: &[(i64, i64)]) {
    let mut tuples: Vec<String> = data
        .iter()
        .map(|d| {
            format!(
                "(0, {}, {}, {}, {}, {}, {}, {}, {}, {}, 0)",
                d.fingerprint,
                d.t_end,
                d.t_last,
                d.last_bits,
                d.min_bits,
                d.max_bits,
                d.sum.to_bits(),
                d.count,
                d.inc.to_bits()
            )
        })
        .collect();
    tuples.extend(
        runs.iter()
            .map(|(lo, hi)| format!("(1, 0, {hi}, {lo}, 0, 0, 0, 0, 0, 0, 0)")),
    );
    for chunk in tuples.chunks(300) {
        exec(
            client,
            &format!(
                "INSERT INTO {db}.metric_samples_{r}s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, built_at) \
                 SELECT k, '', fp, te, tl, reinterpretAsFloat64(lb), reinterpretAsFloat64(nb), reinterpretAsFloat64(xb), \
                        reinterpretAsFloat64(sb), c, reinterpretAsFloat64(ib), ba \
                 FROM values('k UInt8, fp UInt128, te Int64, tl Int64, lb UInt64, nb UInt64, xb UInt64, sb UInt64, c UInt32, ib UInt64, ba Int64', {})",
                chunk.join(", ")
            ),
        )
        .await;
    }
}

/// A plain data row: `count` samples of `v` ending at `t_end`.
fn flat_row(fp: u128, t_end: i64, v: f64, count: u32) -> LevelRow {
    LevelRow {
        fingerprint: fp,
        t_end,
        t_last: t_end - 1_000,
        last_bits: v.to_bits(),
        min_bits: v.to_bits(),
        max_bits: v.to_bits(),
        sum: v * f64::from(count),
        count,
        inc: 1.0,
    }
}

/// Four series' rows for every bucket of level `r` in `(lo, hi]`, and one
/// run row per bucket.
fn covered(r: u32, lo: i64, hi: i64) -> (Vec<LevelRow>, Vec<(i64, i64)>) {
    let w = i64::from(r) * 1000;
    let mut data = Vec::new();
    let mut runs = Vec::new();
    let mut t = lo + w;
    while t <= hi {
        for fp in 1..=4u128 {
            data.push(flat_row(fp, t, (fp as f64) * 2.0, 4));
        }
        runs.push((t - w, t));
        t += w;
    }
    (data, runs)
}

/// Samples every 15 s over `(start, start + minutes]` for four kinds of
/// series: a counter resetting every 7 samples (fingerprint 1), a gauge
/// (2), and two counters of other periods (3 and 4). Values are small
/// multiples of a quarter, so each is exact as text and as a double.
fn day_samples(start: i64, minutes: i64) -> Vec<Sample> {
    let mut out = Vec::new();
    for k in 1..=minutes * 4 {
        let t = start + k * 15_000;
        let values = [
            (1u128, (k % 7) as f64 * 3.0 + 1.0),
            (2, ((k * 37) % 101) as f64 / 4.0),
            (3, (k % 13) as f64 * 0.5 + 2.0),
            (4, (k % 29) as f64 * 1.25),
        ];
        for (fp, v) in values {
            out.push((fp, t, v.to_bits()));
        }
    }
    out
}

/// **A4 (issue #18 part A): a level is its aggregates.** (i) Four series
/// over 40 minutes ending 3 minutes ago land at 15 s: a counter resetting
/// every 7 samples, a gauge, a series stored twice (a second insert of the
/// same rows) and a series with a second value at one millisecond every
/// 10th sample. With a first-level run row seeded at the window start, the
/// 60 s level runs until idle: its rows equal [`expected_first`] over the
/// runs' window, and its run rows' union is that window. (ii) For the UTC
/// day `T−2`, 60 s rows the test builds from its own 15 s samples and one
/// run row for the day are inserted by hand; the 720 s, 7200 s and 86400 s
/// levels then run until idle, and every row of each equals
/// [`expected_up`] of the level below in its bucket, with every bucket of
/// the day built for every series.
#[tokio::test]
async fn a_level_is_its_aggregates() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = DB_AGGREGATES.name();
    let ctx = RenderCtx::for_tests(db, 7);
    fresh(&client, &ctx).await;

    // (i)
    let now = now_ms(&client).await;
    let end = now / MINUTE * MINUTE - 3 * MINUTE;
    let start = end - 40 * MINUTE;
    let mut first = Vec::new();
    let mut twice = Vec::new();
    for k in 1..=160i64 {
        let t = start + k * 15_000;
        let counter = (k % 7) as f64 * 2.0 + 1.0;
        let gauge = ((k * 53) % 97) as f64 / 4.0;
        let doubled = (k % 11) as f64 + 0.5;
        let pair = (k % 5) as f64 * 1.5 + 3.0;
        first.push((11u128, t, counter.to_bits()));
        first.push((12, t, gauge.to_bits()));
        first.push((13, t, doubled.to_bits()));
        twice.push((13, t, doubled.to_bits()));
        first.push((14, t, pair.to_bits()));
        if k % 10 == 0 {
            first.push((14, t, (pair + 0.25).to_bits()));
        }
    }
    exec(&client, &landing_insert(db, &first)).await;
    exec(&client, &landing_insert(db, &twice)).await;
    let doubled = int(
        &client,
        &format!("SELECT toInt64(count()) AS v FROM {db}.metric_landing WHERE fingerprint = 13"),
    )
    .await;
    assert_eq!(doubled, 320, "the doubled series is stored twice");
    insert_level(&client, db, 60, &[], &[(start - MINUTE, start)]).await;

    until_idle(&client, db, 60, 20).await;
    let runs = run_rows(&client, db, 60).await;
    let window = union(runs.iter().map(|r| (r.t_last, r.t_end)));
    let hi = window.last().expect("run rows").1;
    assert_eq!(
        window,
        vec![(start - MINUTE, hi)],
        "the 60 s run rows cover the seed and every run after it, without a gap"
    );
    assert!(hi >= end, "the runs reached the last sample: {hi} < {end}");
    let samples = landed(&client, db).await;
    let want = expected_first(&samples, start, hi, MINUTE);
    assert_eq!(want.len(), 4 * 40, "every series has a row per minute");
    let got = data_rows(&client, db, 60).await;
    assert_rows_match(&got, &want, "(i) the 60 s level");

    // (ii)
    let day = (now / DAY - 2) * DAY;
    let generated = day_samples(day, 1440);
    let built = expected_first(&generated, day, day + DAY, MINUTE);
    assert_eq!(built.len(), 4 * 1440);
    insert_level(&client, db, 60, &built, &[]).await;
    insert_level(&client, db, 60, &[], &[(day, day + DAY)]).await;
    for (r, max) in [(720u32, 30usize), (7200, 30), (86400, 30)] {
        until_idle(&client, db, r, max).await;
    }
    let mut below = data_rows(&client, db, 60).await;
    for (r, per_day) in [(720u32, 120usize), (7200, 12), (86400, 1)] {
        let level_ms = i64::from(r) * 1000;
        let got = data_rows(&client, db, r).await;
        let up: BTreeMap<(u128, i64), LevelRow> = expected_up(&below, level_ms)
            .into_iter()
            .map(|x| ((x.fingerprint, x.t_end), x))
            .collect();
        let want: Vec<LevelRow> = got
            .iter()
            .map(|g| {
                up.get(&(g.fingerprint, g.t_end))
                    .unwrap_or_else(|| panic!("{r}s: a row with no rows below it: {g:?}"))
                    .clone()
            })
            .collect();
        assert_rows_match(&got, &want, &format!("(ii) the {r}s level"));
        for fp in 1..=4u128 {
            let in_day = got
                .iter()
                .filter(|g| g.fingerprint == fp && g.t_end > day && g.t_end <= day + DAY)
                .count();
            assert_eq!(in_day, per_day, "{r}s: series {fp}'s buckets of T−2");
        }
        below = got;
    }

    drop_db(&client, db).await;
}

/// **A5 (issue #18 part A): an empty window moves the watermark.** No
/// samples; a first-level run row seeded 3 minutes before the run's
/// `ready`; one 60 s run. Its run rows cover `(seed, ready]` and it writes
/// no data row.
#[tokio::test]
async fn an_empty_window_moves_the_watermark() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = DB_EMPTY.name();
    fresh(&client, &RenderCtx::for_tests(db, 7)).await;
    let ready = |now: i64| (now - 150_000).div_euclid(MINUTE) * MINUTE;
    let before = ready(now_ms(&client).await);
    let seed = before - 3 * MINUTE;
    insert_level(&client, db, 60, &[], &[(seed - MINUTE, seed)]).await;
    run(&client, db, 60).await;
    let after = ready(now_ms(&client).await);
    let runs = run_rows(&client, db, 60).await;
    let new = union(
        runs.iter()
            .filter(|r| r.t_end > seed)
            .map(|r| (r.t_last, r.t_end)),
    );
    assert_eq!(new.len(), 1, "one interval after the seed: {runs:?}");
    let (lo, hi) = new[0];
    assert_eq!(lo, seed, "the run starts at the seed: {runs:?}");
    assert!(
        hi >= before && hi <= after,
        "the run ends at its ready, {before} to {after}: {runs:?}"
    );
    let data = int(
        &client,
        &format!("SELECT toInt64(count()) AS v FROM {db}.metric_samples_60s WHERE kind = 0"),
    )
    .await;
    assert_eq!(data, 0, "an empty window writes no data row");
    drop_db(&client, db).await;
}

/// **A6 (issue #18 part A): a hole is skipped at every level.** (i) With
/// landing kept one hour and the first level's run row seeded two hours
/// ago, one 60 s run starts no earlier than landing allows, at
/// `⌈now − 3600 s + 720 s⌉` to the minute, and leaves the time after the
/// seed without a run row. (ii) With levels `60, 720, 7200` and the 60 s
/// level covering `(B, B + 40 min]` and `(B + 60 min, B + 3 h]` by hand,
/// the 720 s level builds up to `B + 36 min`, skips the buckets the hole
/// touches, resumes with the first bucket wholly covered after it and
/// reaches `B + 3 h`; every run moved its watermark or wrote nothing, and
/// the last wrote nothing. The 7200 s level, whose buckets the 720 s run
/// rows never wholly cover here, writes nothing.
#[tokio::test]
async fn a_hole_is_skipped_at_every_level() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");

    // (i)
    let db = DB_HOLE_LANDING.name();
    let ctx = RenderCtx {
        metrics_landing_retention_hours: 1,
        ..RenderCtx::for_tests(db, 7)
    };
    fresh(&client, &ctx).await;
    let oldest = |now: i64| (now - HOUR + 12 * MINUTE + MINUTE - 1).div_euclid(MINUTE) * MINUTE;
    let now = now_ms(&client).await;
    let seed = now / MINUTE * MINUTE - 2 * HOUR;
    insert_level(&client, db, 60, &[], &[(seed - MINUTE, seed)]).await;
    let before = oldest(now_ms(&client).await);
    run(&client, db, 60).await;
    let after = oldest(now_ms(&client).await);
    let runs = run_rows(&client, db, 60).await;
    let new: Vec<&RunRow> = runs.iter().filter(|r| r.t_end > seed).collect();
    let starts = new
        .iter()
        .map(|r| r.t_last)
        .min()
        .expect("the run wrote a run row");
    assert!(
        starts >= before && starts <= after,
        "the run starts at ⌈now − 3600 s + 720 s⌉, {before} to {after}: {runs:?}"
    );
    assert!(
        new.iter().all(|r| r.t_last >= starts),
        "(seed, {starts}] has no run row: {runs:?}"
    );
    drop_db(&client, db).await;

    // (ii)
    let db = DB_HOLE_SOURCE.name();
    let ctx = RenderCtx {
        rollup_levels: vec![60, 720, 7200],
        ..RenderCtx::for_tests(db, 7)
    };
    fresh(&client, &ctx).await;
    let now = now_ms(&client).await;
    let b = (now - 4 * HOUR).div_euclid(2 * HOUR) * 2 * HOUR;
    for (lo, hi) in [(b, b + 40 * MINUTE), (b + 60 * MINUTE, b + 3 * HOUR)] {
        let (data, runs) = covered(60, lo, hi);
        insert_level(&client, db, 60, &data, &runs).await;
    }
    let history = until_idle(&client, db, 720, 20).await;
    let mut previous = AfterRun {
        run_rows: 0,
        watermark: 0,
    };
    for after in &history {
        assert!(
            after.run_rows == previous.run_rows || after.watermark > previous.watermark,
            "a 720 s run wrote a run row without moving its watermark: {history:?}"
        );
        previous = *after;
    }
    let runs = run_rows(&client, db, 720).await;
    assert_eq!(
        union(runs.iter().map(|r| (r.t_last, r.t_end))),
        vec![(b, b + 36 * MINUTE), (b + 60 * MINUTE, b + 3 * HOUR)],
        "the 720 s level skips the buckets the hole touches and resumes after it: {runs:?}"
    );
    let in_hole = int(
        &client,
        &format!(
            "SELECT toInt64(count()) AS v FROM {db}.metric_samples_720s \
             WHERE kind = 0 AND t_end > {} AND t_end <= {}",
            b + 36 * MINUTE,
            b + 60 * MINUTE
        ),
    )
    .await;
    assert_eq!(in_hole, 0, "no 720 s data row in the hole");
    until_idle(&client, db, 7200, 20).await;
    let written = int(
        &client,
        &format!("SELECT toInt64(count()) AS v FROM {db}.metric_samples_7200s"),
    )
    .await;
    assert_eq!(
        written, 0,
        "no 7200 s bucket is wholly covered by the 720 s run rows"
    );
    drop_db(&client, db).await;
}

/// **A7 (issue #18 part A): each run reads its window.** (i) In its own
/// database, 400 series at 15 s over 40 minutes ending 3 minutes ago land
/// (64,000 rows), a first-level run row is seeded at the window start, and
/// the 60 s level runs until idle. Every first-level run reads fewer rows
/// than `metric_landing` holds: its window and lookback span a few
/// 5-minute blocks, and the block condition is what lets it read only
/// those. (ii) In another database, one UTC day `T−2` of 60 s rows for 400
/// series and one run row for the day, built by the test; the 720 s, 7200 s
/// and 86400 s levels run until idle. Every 720 s and 7200 s run that wrote
/// data read its source through the source's `by_parent` projection, at
/// least 12 and 2 of them. The 86400 s level's source has no projection (a
/// day-long run reads its source's day partition), and its data run reads
/// at most `S + Σ_p min(rows_p, 1024) + O + 1`, taken from `system.parts`
/// before it: the 7200 s table's rows in the day's partition, one granule
/// of run rows per source part, its own table's watermark scan, and the
/// run-row branch.
#[tokio::test]
async fn each_run_reads_its_window() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = DB_WINDOW.name();
    fresh(&client, &RenderCtx::for_tests(db, 7)).await;
    let now = now_ms(&client).await;
    let end = now / MINUTE * MINUTE - 3 * MINUTE;
    let start = end - 40 * MINUTE;
    exec(
        &client,
        &format!(
            "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
             SELECT '', t + 5000, 0, '', toUInt128(number % 400 + 1), t, toFloat64(number % 97) \
             FROM (SELECT number, {start} + (intDiv(number, 400) + 1) * 15000 AS t FROM numbers(64000))"
        ),
    )
    .await;
    let landing = int(
        &client,
        &format!("SELECT toInt64(count()) AS v FROM {db}.metric_landing"),
    )
    .await;
    assert_eq!(landing, 64_000);
    insert_level(&client, db, 60, &[], &[(start - MINUTE, start)]).await;
    until_idle(&client, db, 60, 20).await;
    let built = int(
        &client,
        &format!("SELECT toInt64(count()) AS v FROM {db}.metric_samples_60s WHERE kind = 0"),
    )
    .await;
    assert_eq!(built, 400 * 40, "every series has a row per minute");

    exec(&client, "SYSTEM FLUSH LOGS").await;
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct ReadLog {
        read_rows: u64,
        written_rows: u64,
    }
    let logged = rows::<ReadLog>(
        &client,
        &format!(
            "SELECT read_rows, written_rows FROM system.query_log \
             WHERE type = 'QueryFinish' AND event_date >= yesterday() \
             AND query LIKE 'INSERT INTO {db}.metric_samples_60s (%' \
             AND query LIKE '%builder%'"
        ),
    )
    .await;
    assert!(
        logged.iter().filter(|l| l.written_rows > 1).count() >= 8,
        "the runs that built 40 minutes are in the query log: {logged:?}"
    );
    for l in &logged {
        assert!(
            l.read_rows < landing as u64,
            "a first-level run read {} rows of the {landing} landing holds: {logged:?}",
            l.read_rows
        );
    }
    drop_db(&client, db).await;

    // (ii)
    let db = DB_WINDOW_UP.name();
    fresh(&client, &RenderCtx::for_tests(db, 7)).await;
    let day = (now_ms(&client).await / DAY - 2) * DAY;
    exec(
        &client,
        &format!(
            "INSERT INTO {db}.metric_samples_60s (kind, org_id, fingerprint, t_end, t_last, last, min, max, sum, count, inc, built_at) \
             SELECT 0, '', toUInt128(number % 400 + 1), {day} + (intDiv(number, 400) + 1) * 60000 AS te, te - 1000, \
                    toFloat64(number % 89), toFloat64(number % 89), toFloat64(number % 89), toFloat64(number % 89) * 4, 4, 1., 0 \
             FROM numbers(400 * 1440)"
        ),
    )
    .await;
    insert_level(&client, db, 60, &[], &[(day, day + DAY)]).await;
    until_idle(&client, db, 720, 30).await;
    until_idle(&client, db, 7200, 30).await;
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct PartRow {
        rows: u64,
    }
    let parts = rows::<PartRow>(
        &client,
        &format!(
            "SELECT rows FROM system.parts WHERE database = '{db}' AND table = 'metric_samples_7200s' \
             AND active AND partition = toString(toDate(fromUnixTimestamp64Milli({day}), 'UTC'))"
        ),
    )
    .await;
    let own = int(
        &client,
        &format!("SELECT toInt64(count()) AS v FROM {db}.metric_samples_86400s"),
    )
    .await as u64;
    let ceiling = parts.iter().map(|p| p.rows).sum::<u64>()
        + parts.iter().map(|p| p.rows.min(1024)).sum::<u64>()
        + own
        + 1;
    until_idle(&client, db, 86400, 30).await;
    let built = int(
        &client,
        &format!(
            "SELECT toInt64(count()) AS v FROM {db}.metric_samples_86400s \
             WHERE kind = 0 AND t_end = {}",
            day + DAY
        ),
    )
    .await;
    assert_eq!(
        built, 400,
        "the 86400 s level built the day for every series"
    );

    exec(&client, "SYSTEM FLUSH LOGS").await;
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct RunLog {
        read_rows: u64,
        written_rows: u64,
        projections: Vec<String>,
    }
    let runs_of = |r: u32| {
        format!(
            "SELECT read_rows, written_rows, projections FROM system.query_log \
             WHERE type = 'QueryFinish' AND event_date >= yesterday() \
             AND query LIKE 'INSERT INTO {db}.metric_samples_{r}s (%' \
             AND query LIKE '%builder%' AND written_rows > 1"
        )
    };
    for (r, source, at_least) in [(720u32, 60u32, 12usize), (7200, 720, 2)] {
        let logged = rows::<RunLog>(&client, &runs_of(r)).await;
        assert!(
            logged.len() >= at_least,
            "{r}s: {} runs wrote data, want at least {at_least}: {logged:?}",
            logged.len()
        );
        let projection = format!("{db}.metric_samples_{source}s.by_parent");
        for l in &logged {
            assert!(
                l.projections.contains(&projection),
                "{r}s: a run that wrote {} rows read without {projection}: {logged:?}",
                l.written_rows
            );
        }
    }
    let logged = rows::<RunLog>(&client, &runs_of(86400)).await;
    assert!(
        !logged.is_empty(),
        "86400s: the run that wrote the day is in the query log"
    );
    for l in &logged {
        assert!(
            l.read_rows <= ceiling,
            "86400s: a run read {} rows, above the day partition's {ceiling}: {logged:?}",
            l.read_rows
        );
    }
    drop_db(&client, db).await;
}

/// **A8 (issue #18 part A): a level waits for its source.** Levels `60,
/// 720, 7200`; the 720 s level covers `(B, B + 2 h]` and the 60 s level
/// `(B + 2 h, B + 4 h]`, with no 720 s rows there. Two 7200 s runs build
/// `(B, B + 2 h]` and nothing after it.
#[tokio::test]
async fn a_level_waits_for_its_source() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = DB_WAITS.name();
    let ctx = RenderCtx {
        rollup_levels: vec![60, 720, 7200],
        ..RenderCtx::for_tests(db, 7)
    };
    fresh(&client, &ctx).await;
    let now = now_ms(&client).await;
    let b = (now - 6 * HOUR).div_euclid(2 * HOUR) * 2 * HOUR;
    let (data, runs) = covered(720, b, b + 2 * HOUR);
    insert_level(&client, db, 720, &data, &runs).await;
    let (data, runs) = covered(60, b + 2 * HOUR, b + 4 * HOUR);
    insert_level(&client, db, 60, &data, &runs).await;
    run(&client, db, 7200).await;
    run(&client, db, 7200).await;
    let runs = run_rows(&client, db, 7200).await;
    assert_eq!(
        union(runs.iter().map(|r| (r.t_last, r.t_end))),
        vec![(b, b + 2 * HOUR)],
        "the 7200 s run rows end where the 720 s ones do: {runs:?}"
    );
    drop_db(&client, db).await;
}

/// **A9 (issue #18 part A): runs split at midnight and expire by day.** (i)
/// Levels `60, 720`; the 60 s level covers `(M − 1 h, M + 2 h]`, `M`
/// yesterday's midnight; one 720 s run writes two run rows, `(M − 1 h, M]`
/// and `(M, M + 1 h]`, one per day. (ii) Retention rendered 4; rows of every
/// level for the UTC days `T−4` and `T−2`; after `OPTIMIZE … FINAL`, no row
/// of `T−4` is left and every row of `T−2` is.
#[tokio::test]
async fn runs_split_at_midnight_and_expire_by_day() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");

    // (i)
    let db = DB_MIDNIGHT.name();
    let ctx = RenderCtx {
        rollup_levels: vec![60, 720],
        ..RenderCtx::for_tests(db, 7)
    };
    fresh(&client, &ctx).await;
    let m = (now_ms(&client).await / DAY - 1) * DAY;
    let (data, runs) = covered(60, m - HOUR, m + 2 * HOUR);
    insert_level(&client, db, 60, &data, &runs).await;
    run(&client, db, 720).await;
    let runs = run_rows(&client, db, 720).await;
    let first_run = runs.iter().map(|r| r.built_at).min().expect("a run row");
    let rows_of_first: Vec<(i64, i64)> = runs
        .iter()
        .filter(|r| r.built_at == first_run)
        .map(|r| (r.t_last, r.t_end))
        .collect();
    assert_eq!(
        rows_of_first,
        vec![(m - HOUR, m), (m, m + HOUR)],
        "one run across midnight writes one run row per day: {runs:?}"
    );
    drop_db(&client, db).await;

    // (ii)
    let db = DB_EXPIRY.name();
    let ctx = RenderCtx::for_tests(db, 4);
    fresh(&client, &ctx).await;
    let t = now_ms(&client).await / DAY;
    for r in ctx.rollup_levels.clone() {
        for day in [t - 4, t - 2] {
            let (data, _) = covered(r, day * DAY, (day + 1) * DAY);
            insert_level(&client, db, r, &data, &[(day * DAY, (day + 1) * DAY)]).await;
        }
        exec(
            &client,
            &format!("OPTIMIZE TABLE {db}.metric_samples_{r}s FINAL"),
        )
        .await;
        let per_day = |day: i64| {
            format!(
                "SELECT toInt64(count()) AS v FROM {db}.metric_samples_{r}s \
                 WHERE intDiv(t_end - 1, {DAY}) = {day}"
            )
        };
        let kept = 4 * (86_400 / i64::from(r)) + 1;
        assert_eq!(int(&client, &per_day(t - 4)).await, 0, "{r}s: T−4 is gone");
        assert_eq!(
            int(&client, &per_day(t - 2)).await,
            kept,
            "{r}s: T−2 is kept"
        );
    }
    drop_db(&client, db).await;
}
