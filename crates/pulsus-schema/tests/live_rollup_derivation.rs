//! Issue #18 part 1: `metric_rollup_mv` derives the 5m and 1h bucket
//! summaries of `metric_samples` and `metric_hist_samples`, and coverage
//! rows for every settled day from the day before the oldest raw partition,
//! empty days included. Single node.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`, like `live_schema.rs`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 cargo test -p pulsus-schema --test live_rollup_derivation
//! ```
//!
//! **Not near midnight UTC.** `T` is the server's UTC day when a test
//! starts, and a test that crosses midnight reads the wrong days.
//!
//! The expected rows come from `common::rollup_summary`, a summarizer
//! written in plain Rust from the design's tables, over the raw samples the
//! server stores. Floats are compared as bits.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use common::rollup_summary::{
    FloatSample, HistSample, RES_1H, RES_5M, STALE_BITS, Summary, bucket_end, summarize,
};
use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, ChRow, Idempotency, QuerySettings, Row};
use pulsus_schema::RenderCtx;
use pulsus_schema_testkit::run_init;

const DAY: i64 = 86_400_000;
const RETENTION_DAYS: u32 = 40;
/// The largest fingerprint: the fingerprint space is `[0, 2^128 − 1]`.
const FP_MAX: u128 = u128::MAX;

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
                 (see crates/pulsus-schema/tests/live_rollup_derivation.rs)"
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

/// The planning view, then the derivation that depends on it.
const VIEWS: [&str; 2] = ["metric_rollup_plan_mv", "metric_rollup_mv"];

/// One refresh of `view`, waited for. A stopped view ignores it.
async fn refresh_one(client: &ChClient, db: &str, view: &str) {
    exec(client, &format!("SYSTEM REFRESH VIEW {db}.{view}")).await;
    exec(client, &format!("SYSTEM WAIT VIEW {db}.{view}")).await;
}

/// One refresh: the planner, then the derivation, each waited for.
async fn refresh(client: &ChClient, db: &str) {
    for view in VIEWS {
        refresh_one(client, db, view).await;
    }
}

/// `SYSTEM STOP VIEW` or `SYSTEM START VIEW` on both views, planner first.
async fn views(client: &ChClient, db: &str, verb: &str) {
    for view in VIEWS {
        exec(client, &format!("SYSTEM {verb} VIEW {db}.{view}")).await;
    }
}

/// One data row of `metric_rollup`, floats as bits. Positional: the field
/// order is the `SELECT`'s in [`data_rows`].
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct RollupRow {
    res: u32,
    org_id: String,
    fingerprint: u128,
    t_end: i64,
    n: u32,
    t_first: i64,
    v_first: u64,
    t_last: i64,
    v_last: u64,
    t_prev: i64,
    v_prev: u64,
    v_min: u64,
    v_max: u64,
    changes: u32,
    n_resets: u32,
    resets: Vec<u64>,
    t_any: i64,
    any_stale: u8,
    has_hist: u8,
    expires_ms: i64,
}

impl RollupRow {
    fn summary(&self) -> Summary {
        Summary {
            res: self.res,
            org_id: self.org_id.clone(),
            fingerprint: self.fingerprint,
            t_end: self.t_end,
            n: self.n,
            t_first: self.t_first,
            v_first: self.v_first,
            t_last: self.t_last,
            v_last: self.v_last,
            t_prev: self.t_prev,
            v_prev: self.v_prev,
            v_min: self.v_min,
            v_max: self.v_max,
            changes: self.changes,
            n_resets: self.n_resets,
            resets: self.resets.clone(),
            t_any: self.t_any,
            any_stale: self.any_stale,
            has_hist: self.has_hist,
        }
    }
}

/// Every data row (`row_kind = 0`) of `table`, after `FINAL`.
async fn data_rows(client: &ChClient, table: &str) -> Vec<RollupRow> {
    let sql = format!(
        "SELECT res, org_id, fingerprint, t_end, n, t_first, reinterpretAsUInt64(v_first) AS v_first, \
         t_last, reinterpretAsUInt64(v_last) AS v_last, t_prev, reinterpretAsUInt64(v_prev) AS v_prev, \
         reinterpretAsUInt64(v_min) AS v_min, reinterpretAsUInt64(v_max) AS v_max, changes, n_resets, \
         arrayMap(x -> reinterpretAsUInt64(x), resets) AS resets, t_any, any_stale, has_hist, expires_ms \
         FROM {table} FINAL WHERE row_kind = 0"
    );
    rows(client, &sql).await
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CoverageRow {
    day: i64,
    res: u32,
    lo: u128,
    hi: u128,
    expires_ms: i64,
}

/// Every coverage row (`row_kind = 1`) of `table`.
async fn coverage_rows(client: &ChClient, table: &str) -> Vec<CoverageRow> {
    let sql = format!(
        "SELECT intDiv(t_end - 1, {DAY}) AS day, res, fingerprint AS lo, chunk_last AS hi, expires_ms \
         FROM {table} WHERE row_kind = 1"
    );
    rows(client, &sql).await
}

/// Whether the chunks of `(day, res)` together cover `[lo, hi]` with no gap.
fn covers(cov: &[CoverageRow], day: i64, res: u32, lo: u128, hi: u128) -> bool {
    let mut chunks: Vec<(u128, u128)> = cov
        .iter()
        .filter(|c| c.day == day && c.res == res)
        .map(|c| (c.lo, c.hi))
        .collect();
    chunks.sort();
    // `next` is the first fingerprint not yet covered; `None` once `hi` is.
    let mut next = Some(lo);
    for (a, b) in chunks {
        if let Some(n) = next
            && a <= n
            && b >= n
        {
            next = if b >= hi { None } else { Some(b + 1) };
        }
    }
    next.is_none()
}

/// Whether every day in `days` is covered at both tiers over `[lo, hi]`.
fn all_covered(cov: &[CoverageRow], days: &[i64], lo: u128, hi: u128) -> bool {
    days.iter()
        .all(|&d| [RES_5M, RES_1H].iter().all(|&r| covers(cov, d, r, lo, hi)))
}

/// One view's refreshes in the query log (1-h).
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct IdleRow {
    view: String,
    refreshes: u64,
    most_written: u64,
    cpu_us: u64,
    analysis_us: u64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct FloatRow {
    org_id: String,
    fingerprint: u128,
    unix_milli: i64,
    bits: u64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct HistRow {
    org_id: String,
    fingerprint: u128,
    unix_milli: i64,
}

/// The raw samples the server stores, read through `samples` and `hists`.
async fn raw_samples(
    client: &ChClient,
    samples: &str,
    hists: &str,
) -> (Vec<FloatSample>, Vec<HistSample>) {
    let floats = rows::<FloatRow>(
        client,
        &format!(
            "SELECT org_id, fingerprint, unix_milli, reinterpretAsUInt64(value) AS bits FROM {samples}"
        ),
    )
    .await
    .into_iter()
    .map(|r| (r.org_id, r.fingerprint, r.unix_milli, r.bits))
    .collect();
    let hists = rows::<HistRow>(
        client,
        &format!("SELECT org_id, fingerprint, unix_milli FROM {hists}"),
    )
    .await
    .into_iter()
    .map(|r| (r.org_id, r.fingerprint, r.unix_milli))
    .collect();
    (floats, hists)
}

/// The view's rows against the summarizer's: `(missing, extra)`, each a
/// sorted list of summaries.
fn diff(got: &[RollupRow], want: &[Summary]) -> (Vec<Summary>, Vec<Summary>) {
    let got: BTreeSet<Summary> = got.iter().map(RollupRow::summary).collect();
    let want: BTreeSet<Summary> = want.iter().cloned().collect();
    (
        want.difference(&got).cloned().collect(),
        got.difference(&want).cloned().collect(),
    )
}

/// The design's eight kinds of series on one UTC day, two series of each
/// (fingerprints 1 to 16, kind = fingerprint mod 8), from its generator.
/// Sample `j` of the day sits at `day start + j × interval ± jitter` for
/// `j ≥ 1`, so every sample falls strictly inside the day.
fn kinds_insert(db: &str, day: i64) -> Vec<String> {
    [
        ("k != 4", 15_000, 1_000, 5_760),
        ("k = 4", 10_000, 3_000, 8_640),
    ]
    .into_iter()
    .map(|(kinds, iv, jit, per_day)| {
        format!(
            "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
             SELECT '', ts, 0, '', toUInt128(fp), ts, if(rep = 3, v + 1, v) FROM ( \
               SELECT fp, i, k, \
                 toInt64({start} + j * {iv} + (toInt64(cityHash64(fp, i) % (2 * {jit} + 1)) - {jit})) AS ts, \
                 multiIf( \
                   k IN (0, 4, 5), toFloat64((i % (2000 + cityHash64(fp) % 18000)) * (3 + fp % 5)) * 0.25 + toFloat64(cityHash64(fp, intDiv(i, 2000 + cityHash64(fp) % 18000)) % 7), \
                   k = 1, if(cityHash64(fp, i, 1) % 997 = 0, nan, sin(i / 50.0 + fp) * 100 + (cityHash64(fp, i) % 1000) / 1000.0), \
                   k = 2, toFloat64(intDiv(i % (30 + fp % 20), 3) + cityHash64(fp, intDiv(i, 30 + fp % 20)) % 4), \
                   k = 3, if(i % 2000 = 1000, reinterpretAsFloat64(toUInt64({STALE_BITS})), sin(i / 50.0 + fp) * 100), \
                   k = 6, if(i % 7 = 0, -0., if(i % 5 = 0, 1., 0.)), \
                   sin(i / 70.0 + fp) * 10) AS v \
               FROM ( \
                 SELECT (number % 16) + 1 AS fp, intDiv(number, 16) + 1 AS j, {day} * {per_day} + j AS i, fp % 8 AS k \
                 FROM numbers(16 * ({per_day} - 1)) \
               ) \
               WHERE {kinds} AND NOT (k = 3 AND i % 2000 > 1000 AND i % 2000 < 1040) \
             ) \
             ARRAY JOIN if(k = 5, [1, 2], if(k = 7 AND i % 1500 = 0, [1, 3], [1])) AS rep",
            start = day * DAY,
        )
    })
    .collect()
}

/// One landing row per `(fingerprint, unix_milli, value)`. A value of
/// `None` is the stale marker; `hist` makes every row a native-histogram
/// sample instead, and ignores the value.
fn landing_values(db: &str, samples: &[(u128, i64, Option<f64>)], hist: bool) -> String {
    let kind = if hist { 1 } else { 0 };
    let values: Vec<String> = samples
        .iter()
        .map(|(fp, t, v)| {
            let v = match v {
                _ if hist => "0".to_string(),
                Some(v) => format!("{v:?}"),
                None => format!("reinterpretAsFloat64(toUInt64({STALE_BITS}))"),
            };
            format!("('', {t}, {kind}, '', {fp}, {t}, {v})")
        })
        .collect();
    format!(
        "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
         VALUES {}",
        values.join(", ")
    )
}

/// **1-b, 1-b2, 1-b3, then 1-f.** The view derives every day exactly, from
/// the day before the oldest raw partition, and covers the days with no
/// partition; then a sample landing at an older day is derived, and the
/// empty days after it are covered.
#[tokio::test]
async fn the_view_derives_every_day_exactly_and_covers_empty_days() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_it_rollup_derive");
    drop_db(&client, db).await;
    run_init(&client, &RenderCtx::for_tests(db, RETENTION_DAYS))
        .await
        .expect("run_init");
    // No scheduled refresh may run while the days are only partly
    // written: the refreshes below are the test's own.
    views(&client, db, "STOP").await;

    let t = now_ms(&client).await / DAY;

    // 1-b: the eight kinds on T−6, T−3 and T−2; T−5 has no partition.
    for day in [t - 6, t - 3, t - 2] {
        for sql in kinds_insert(db, day) {
            exec(&client, &sql).await;
        }
    }
    // The 16- and 17-reset hours (fingerprints 401 and 402), ending T−3 01:00.
    for (fp, values) in [
        (
            401,
            "if(number < 225, toFloat64(number % 14) + 1, toFloat64(number) + 1000)",
        ),
        (402, "toFloat64(number % 14) + 1"),
    ] {
        exec(
            &client,
            &format!(
                "INSERT INTO {db}.metric_landing (org_id, received_ms, kind, metric_name, fingerprint, unix_milli, value) \
                 SELECT '', ts, 0, '', toUInt128({fp}), ts, {values} FROM ( \
                   SELECT number, toInt64({} + (number + 1) * 15000) AS ts FROM numbers(240))",
                (t - 3) * DAY
            ),
        )
        .await;
    }
    // A counter with 5 resets in the 5m bucket ending T−2 10:05
    // (fingerprint 403).
    let reset5_start = (t - 2) * DAY + 10 * 3_600_000;
    let reset5: Vec<(u128, i64, Option<f64>)> = [5., 1., 6., 1., 7., 1., 8., 1., 9., 1., 10., 11.]
        .iter()
        .enumerate()
        .map(|(k, v)| (403, reset5_start + 15_000 * (k as i64 + 1), Some(*v)))
        .collect();
    exec(&client, &landing_values(db, &reset5, false)).await;
    // One native-histogram sample on T−2 (fingerprint 404).
    let hist_t = (t - 2) * DAY + 12 * 3_600_000 + 150_000;
    exec(&client, &landing_values(db, &[(404, hist_t, None)], true)).await;

    // 1-b2: T−4 holds one sample, at its start (fingerprint 405).
    exec(
        &client,
        &landing_values(db, &[(405, (t - 4) * DAY, Some(42.0))], false),
    )
    .await;

    // 1-b3: one series per order of a float (F), a stale-only (S) and a
    // histogram-only (H) bucket in T−6's first hour (fingerprints 501 to
    // 506), each sample at its bucket's start + 150 s.
    let hour0 = (t - 6) * DAY;
    let orders = ["FSH", "FHS", "SFH", "SHF", "HFS", "HSF"];
    for (i, order) in orders.iter().enumerate() {
        let fp = 501 + i as u128;
        let mut floats = Vec::new();
        let mut hists = Vec::new();
        for (b, c) in order.chars().enumerate() {
            let at = hour0 + b as i64 * 300_000 + 150_000;
            match c {
                'F' => floats.push((fp, at, Some(1.5))),
                'S' => floats.push((fp, at, None)),
                _ => hists.push((fp, at, None)),
            }
        }
        exec(&client, &landing_values(db, &floats, false)).await;
        exec(&client, &landing_values(db, &hists, true)).await;
    }

    views(&client, db, "START").await;
    let table = format!("{db}.metric_rollup");
    let days: Vec<i64> = (t - 7..=t - 2).collect();
    let mut refreshes = 0;
    let mut cov = coverage_rows(&client, &table).await;
    while !all_covered(&cov, &days, 0, FP_MAX) {
        assert!(
            refreshes < 200,
            "T−7 to T−2 are not covered after 200 refreshes"
        );
        refresh(&client, db).await;
        refreshes += 1;
        cov = coverage_rows(&client, &table).await;
    }

    // Every data row equals the summarizer's.
    let (floats, hists) = raw_samples(
        &client,
        &format!("{db}.metric_samples"),
        &format!("{db}.metric_hist_samples"),
    )
    .await;
    let want = summarize(&floats, &hists);
    let got = data_rows(&client, &table).await;
    let (missing, extra) = diff(&got, &want);
    assert!(
        missing.is_empty() && extra.is_empty(),
        "{} of {} expected rows missing, {} unexpected rows; first missing: {:?}; first unexpected: {:?}",
        missing.len(),
        want.len(),
        extra.len(),
        missing.iter().take(3).collect::<Vec<_>>(),
        extra.iter().take(3).collect::<Vec<_>>()
    );
    assert_eq!(got.len(), want.len(), "no row is stored twice");

    // Expiry: the row's UTC day start + the retention.
    for r in &got {
        assert_eq!(
            r.expires_ms,
            (r.t_end - 1).div_euclid(DAY) * DAY + i64::from(RETENTION_DAYS) * DAY,
            "expiry of {:?}",
            r.summary()
        );
    }
    for c in &cov {
        assert_eq!(
            c.expires_ms,
            c.day * DAY + i64::from(RETENTION_DAYS) * DAY,
            "expiry of the coverage row {c:?}"
        );
    }

    let find = |fp: u128, res: u32, t_end: i64| -> &RollupRow {
        got.iter()
            .find(|r| r.fingerprint == fp && r.res == res && r.t_end == t_end)
            .unwrap_or_else(|| panic!("no row for fingerprint {fp}, res {res}, t_end {t_end}"))
    };

    // has_hist marks exactly the buckets holding a histogram sample.
    let hist_buckets: BTreeSet<(u32, u128, i64)> = hists
        .iter()
        .flat_map(|(_, fp, at)| [RES_5M, RES_1H].map(|r| (r, *fp, bucket_end(*at, r))))
        .collect();
    let marked: BTreeSet<(u32, u128, i64)> = got
        .iter()
        .filter(|r| r.has_hist == 1)
        .map(|r| (r.res, r.fingerprint, r.t_end))
        .collect();
    assert_eq!(
        marked, hist_buckets,
        "has_hist marks exactly the histogram buckets"
    );
    assert!(hist_buckets.contains(&(RES_5M, 404, bucket_end(hist_t, RES_5M))));

    // The 16-reset hour stores 16 values; the 17-reset hour overflows.
    let h401 = find(401, RES_1H, (t - 3) * DAY + 3_600_000);
    assert_eq!((h401.n_resets, h401.resets.len()), (16, 16), "16 resets");
    let h402 = find(402, RES_1H, (t - 3) * DAY + 3_600_000);
    assert_eq!((h402.n_resets, h402.resets.len()), (17, 0), "17 resets");
    // The 5-reset bucket and its hour are overflowed.
    let b403 = find(403, RES_5M, reset5_start + 300_000);
    assert_eq!((b403.n_resets, b403.resets.len()), (5, 0), "5m, 5 resets");
    let h403 = find(403, RES_1H, reset5_start + 3_600_000);
    assert_eq!(
        (h403.n_resets, h403.resets.len()),
        (5, 0),
        "1h holding an overflowed 5m bucket"
    );

    // 1-b2: the sample at T−4's start is in T−5's last bucket, both tiers,
    // and T−5 holds no other data row.
    for res in [RES_5M, RES_1H] {
        assert_eq!(find(405, res, (t - 4) * DAY).n, 1, "1-b2 at res {res}");
    }
    let t5: Vec<(u32, u128, i64)> = got
        .iter()
        .filter(|r| (r.t_end - 1).div_euclid(DAY) == t - 5)
        .map(|r| (r.res, r.fingerprint, r.t_end))
        .collect();
    assert_eq!(
        t5.len(),
        2,
        "T−5's data rows are the boundary sample's: {t5:?}"
    );

    // 1-b3: the hour's t_any is its last non-histogram sample's time.
    let want_any: [(i64, u8); 6] = [
        (450_000, 1),
        (750_000, 1),
        (450_000, 0),
        (750_000, 0),
        (750_000, 1),
        (750_000, 0),
    ];
    for (i, order) in orders.iter().enumerate() {
        let h = find(501 + i as u128, RES_1H, hour0 + 3_600_000);
        assert_eq!(
            (h.t_any - hour0, h.any_stale),
            want_any[i],
            "{order}: (t_any offset, any_stale)"
        );
    }

    // 1-h: with everything covered, five refreshes write nothing. Their
    // CPU is printed as evidence and not asserted on.
    let t0 = now_ms(&client).await;
    for _ in 0..5 {
        refresh(&client, db).await;
    }
    exec(&client, "SYSTEM FLUSH LOGS").await;
    let idle = rows::<IdleRow>(
        &client,
        &format!(
            "SELECT if(query LIKE 'INSERT INTO {db}.metric_rollup (%', 'derivation', 'planner') AS view, \
             toUInt64(count()) AS refreshes, toUInt64(max(written_rows)) AS most_written, \
             toUInt64(quantile(0.5)(ProfileEvents['OSCPUVirtualTimeMicroseconds'])) AS cpu_us, \
             toUInt64(quantile(0.5)(ProfileEvents['QueryAnalysisMicroseconds'])) AS analysis_us \
             FROM system.query_log WHERE has(databases, '{db}') AND query_kind = 'Insert' \
             AND type = 'QueryFinish' AND event_time_microseconds >= fromUnixTimestamp64Milli({t0}) \
             GROUP BY view ORDER BY view"
        ),
    )
    .await;
    eprintln!("1-h idle refreshes: {idle:?}");
    let derivation = idle
        .iter()
        .find(|r| r.view == "derivation")
        .expect("the derivation's refreshes are in the query log");
    assert!(
        derivation.refreshes >= 5,
        "five refreshes of the derivation: {idle:?}"
    );
    assert_eq!(
        derivation.most_written, 0,
        "an idle refresh writes nothing: {idle:?}"
    );

    // 1-f: one sample lands at T−15 12:00. Within 3 refreshes T−16 to T−8
    // are covered, and only T−15 has data rows.
    let late = (t - 15) * DAY + 12 * 3_600_000;
    exec(
        &client,
        &landing_values(db, &[(406, late, Some(7.0))], false),
    )
    .await;
    let old_days: Vec<i64> = (t - 16..=t - 8).collect();
    let mut refreshes = 0;
    loop {
        refresh(&client, db).await;
        refreshes += 1;
        cov = coverage_rows(&client, &table).await;
        if all_covered(&cov, &old_days, 0, FP_MAX) {
            break;
        }
        assert!(
            refreshes < 3,
            "T−16 to T−8 are not covered after 3 refreshes"
        );
    }
    let got = data_rows(&client, &table).await;
    let old: BTreeMap<i64, usize> = got
        .iter()
        .map(|r| (r.t_end - 1).div_euclid(DAY))
        .filter(|d| old_days.contains(d))
        .fold(BTreeMap::new(), |mut m, d| {
            *m.entry(d).or_default() += 1;
            m
        });
    assert_eq!(
        old.keys().copied().collect::<Vec<_>>(),
        vec![t - 15],
        "only T−15 has data rows among T−16 to T−8"
    );
    let b406 = got
        .iter()
        .find(|r| r.fingerprint == 406 && r.res == RES_5M)
        .expect("the late sample's 5m row");
    assert_eq!((b406.n, b406.t_end), (1, bucket_end(late, RES_5M)));

    drop_db(&client, db).await;
}

/// **1-c.** An expired day leaves at both tiers whole, and no part ever
/// holds two days, so the expiry drops whole parts.
#[tokio::test]
async fn an_expired_day_drops_whole_parts_at_both_tiers() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_it_rollup_expiry");
    drop_db(&client, db).await;
    run_init(&client, &RenderCtx::for_tests(db, RETENTION_DAYS))
        .await
        .expect("run_init");
    let now = now_ms(&client).await;
    let t = now / DAY;
    for day in [t - 3, t - 2] {
        exec(
            &client,
            &format!(
                "INSERT INTO {db}.metric_rollup (row_kind, org_id, res, fingerprint, t_end, n, derived_ms, expires_ms) \
                 SELECT 0, '', r, toUInt128(number + 1), {} + r, 1, 1, {} \
                 FROM numbers(100) ARRAY JOIN [toUInt32(300000), toUInt32(3600000)] AS r",
                day * DAY,
                now + DAY
            ),
        )
        .await;
    }
    exec(&client, &format!("OPTIMIZE TABLE {db}.metric_rollup FINAL")).await;
    let mixed = int(
        &client,
        &format!(
            "SELECT toInt64(count()) AS v FROM (SELECT _part FROM {db}.metric_rollup \
             GROUP BY _part HAVING uniqExact(intDiv(t_end - 1, {DAY})) > 1)"
        ),
    )
    .await;
    assert_eq!(mixed, 0, "an active part holds rows of two days");

    exec(
        &client,
        &format!(
            "ALTER TABLE {db}.metric_rollup UPDATE expires_ms = {} \
             WHERE intDiv(t_end - 1, {DAY}) = {} SETTINGS mutations_sync = 2",
            now - 3_600_000,
            t - 3
        ),
    )
    .await;
    exec(&client, &format!("OPTIMIZE TABLE {db}.metric_rollup FINAL")).await;
    let per_day = |day: i64| {
        format!(
            "SELECT toInt64(count()) AS v FROM {db}.metric_rollup WHERE intDiv(t_end - 1, {DAY}) = {day}"
        )
    };
    assert_eq!(int(&client, &per_day(t - 3)).await, 0, "T−3 is gone");
    assert_eq!(int(&client, &per_day(t - 2)).await, 200, "T−2 remains");
    drop_db(&client, db).await;
}

/// **1-d.** A day is not derived until settle has passed since it ended.
/// Settle is rendered so that T−2 is settled and T−1 is not.
#[tokio::test]
async fn an_unsettled_day_is_not_derived() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_it_rollup_settle");
    drop_db(&client, db).await;
    let now = now_ms(&client).await;
    let t = now / DAY;
    // T−1 ended at T's start; settle is an hour more than has passed since.
    let settle = now - t * DAY + 3_600_000;
    let ctx = RenderCtx {
        rollup_settle_ms: u64::try_from(settle).expect("positive"),
        ..RenderCtx::for_tests(db, RETENTION_DAYS)
    };
    run_init(&client, &ctx).await.expect("run_init");
    views(&client, db, "STOP").await;
    let samples: Vec<(u128, i64, Option<f64>)> = [t - 2, t - 1]
        .iter()
        .flat_map(|d| (1..=10).map(move |k| (1, d * DAY + 6 * 3_600_000 + k * 15_000, Some(1.0))))
        .collect();
    exec(&client, &landing_values(db, &samples, false)).await;
    views(&client, db, "START").await;
    for _ in 0..5 {
        refresh(&client, db).await;
    }
    let cov = coverage_rows(&client, &format!("{db}.metric_rollup")).await;
    assert!(
        all_covered(&cov, &[t - 2], 0, FP_MAX),
        "T−2 has settled and is covered"
    );
    let unsettled: Vec<&CoverageRow> = cov.iter().filter(|c| c.day == t - 1).collect();
    assert!(
        unsettled.is_empty(),
        "T−1 has not settled and has coverage: {unsettled:?}"
    );
    drop_db(&client, db).await;
}

/// **1-g.** With no raw partition there is no work: a fresh database's
/// refreshes write nothing.
#[tokio::test]
async fn a_database_with_no_samples_derives_nothing() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let db = &pulsus_testkit::test_db("pulsus_it_rollup_empty");
    drop_db(&client, db).await;
    run_init(&client, &RenderCtx::for_tests(db, RETENTION_DAYS))
        .await
        .expect("run_init");
    for _ in 0..3 {
        refresh(&client, db).await;
    }
    let n = int(
        &client,
        &format!("SELECT toInt64(count()) AS v FROM {db}.metric_rollup"),
    )
    .await;
    assert_eq!(n, 0, "a database with no samples has rollup rows");
    drop_db(&client, db).await;
}

/// A fresh database with both views stopped, and `T`.
async fn stopped_db(client: &ChClient, name: &str) -> (String, i64) {
    let db = pulsus_testkit::test_db(name);
    drop_db(client, &db).await;
    run_init(client, &RenderCtx::for_tests(&db, RETENTION_DAYS))
        .await
        .expect("run_init");
    views(client, &db, "STOP").await;
    let t = now_ms(client).await / DAY;
    (db, t)
}

/// Replaces this replica's plan row with a run of empty days over the
/// whole fingerprint space: the row the planner wrote before a backfill.
async fn plan_empty_run(client: &ChClient, db: &str, run: &[i64]) {
    exec(client, &format!("TRUNCATE TABLE {db}.metric_rollup_plan")).await;
    let days: Vec<String> = run.iter().map(i64::to_string).collect();
    exec(
        client,
        &format!(
            "INSERT INTO {db}.metric_rollup_plan SELECT {}, toUInt256(0), toUInt256('{FP_MAX}'), 0, \
             CAST([{}] AS Array(Int64)), toUInt256(0), toUInt256('{FP_MAX}')",
            run[0],
            days.join(", ")
        ),
    )
    .await;
}

/// The data rows (both tiers) of UTC day `day`, by `t_end − 1`.
fn rows_of_day(rows: &[RollupRow], day: i64) -> usize {
    rows.iter()
        .filter(|r| (r.t_end - 1).div_euclid(DAY) == day)
        .count()
}

/// **1-j, 1-j2, 1-j3.** A plan made before a backfill names a run of empty
/// days, and one of them has since gained a sample. A refresh of the
/// derivation alone, on that plan, covers the days that are still empty and
/// not the one that gained the sample. `backfill` lands the sample; `run`
/// is the plan's run; `gained` is the day the sample belongs to.
async fn a_stale_plan_does_not_certify_a_backfilled_day(
    name: &str,
    run: fn(i64) -> Vec<i64>,
    backfill: fn(&str, i64) -> String,
    gained: fn(i64) -> i64,
    then_derive: bool,
) {
    let client = ChClient::new(test_config()).await.expect("connect");
    let (db, t) = stopped_db(&client, name).await;
    let db = db.as_str();
    let ordinary: Vec<(u128, i64, Option<f64>)> = [t - 7, t - 2]
        .iter()
        .map(|d| (1, d * DAY + 6 * 3_600_000, Some(1.0)))
        .collect();
    exec(&client, &landing_values(db, &ordinary, false)).await;
    let run = run(t);
    plan_empty_run(&client, db, &run).await;
    exec(&client, &backfill(db, t)).await;
    let gained = gained(t);

    exec(&client, &format!("SYSTEM START VIEW {db}.metric_rollup_mv")).await;
    refresh_one(&client, db, "metric_rollup_mv").await;
    let table = format!("{db}.metric_rollup");
    let cov = coverage_rows(&client, &table).await;
    for &d in run.iter().filter(|&&d| d != gained) {
        assert!(
            all_covered(&cov, &[d], 0, FP_MAX),
            "day {d} of the stale plan's run is still empty and is covered"
        );
    }
    let wrong: Vec<&CoverageRow> = cov.iter().filter(|c| c.day == gained).collect();
    assert!(
        wrong.is_empty(),
        "the backfilled day {gained} was covered on a plan made before the backfill: {wrong:?}"
    );
    assert_eq!(
        rows_of_day(&data_rows(&client, &table).await, gained),
        0,
        "the backfilled day has no data rows yet"
    );

    if then_derive {
        // The next plan derives it as a data day.
        exec(
            &client,
            &format!("SYSTEM START VIEW {db}.metric_rollup_plan_mv"),
        )
        .await;
        let mut n = 0;
        loop {
            refresh(&client, db).await;
            n += 1;
            let cov = coverage_rows(&client, &table).await;
            if all_covered(&cov, &[gained], 0, FP_MAX) {
                break;
            }
            assert!(
                n < 20,
                "the backfilled day is not covered after 20 refreshes"
            );
        }
        let got = data_rows(&client, &table).await;
        assert_eq!(
            [RES_5M, RES_1H].map(|r| got
                .iter()
                .filter(|x| x.res == r && (x.t_end - 1).div_euclid(DAY) == gained)
                .count()),
            [1, 1],
            "the backfilled sample's 5m and 1h rows"
        );
    }
    drop_db(&client, db).await;
}

/// **1-j.** The backfill is one float sample at `T−5` 12:00.
#[tokio::test]
async fn a_stale_plan_does_not_certify_a_day_backfilled_with_a_float() {
    skip_unless_live!();
    a_stale_plan_does_not_certify_a_backfilled_day(
        "pulsus_it_rollup_stale_float",
        |t| vec![t - 6, t - 5, t - 4],
        |db, t| landing_values(db, &[(2, (t - 5) * DAY + 12 * 3_600_000, Some(3.0))], false),
        |t| t - 5,
        true,
    )
    .await;
}

/// **1-j2.** The backfill is one native-histogram sample at `T−5` 12:00.
#[tokio::test]
async fn a_stale_plan_does_not_certify_a_day_backfilled_with_a_histogram() {
    skip_unless_live!();
    a_stale_plan_does_not_certify_a_backfilled_day(
        "pulsus_it_rollup_stale_hist",
        |t| vec![t - 6, t - 5, t - 4],
        |db, t| landing_values(db, &[(2, (t - 5) * DAY + 12 * 3_600_000, None)], true),
        |t| t - 5,
        false,
    )
    .await;
}

/// **1-j3.** The backfill is one float sample at exactly `T−4`'s start,
/// which is in `T−4`'s partition and belongs to `T−5`'s last bucket.
#[tokio::test]
async fn a_stale_plan_does_not_certify_a_day_whose_last_bucket_was_backfilled() {
    skip_unless_live!();
    a_stale_plan_does_not_certify_a_backfilled_day(
        "pulsus_it_rollup_stale_edge",
        |t| vec![t - 6, t - 5],
        |db, t| landing_values(db, &[(2, (t - 4) * DAY, Some(3.0))], false),
        |t| t - 5,
        false,
    )
    .await;
}

/// **1-k.** With no plan row the derivation writes nothing.
#[tokio::test]
async fn an_empty_plan_table_derives_nothing() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let (db, t) = stopped_db(&client, "pulsus_it_rollup_no_plan").await;
    let db = db.as_str();
    exec(
        &client,
        &landing_values(db, &[(1, (t - 7) * DAY + 6 * 3_600_000, Some(1.0))], false),
    )
    .await;
    exec(&client, &format!("TRUNCATE TABLE {db}.metric_rollup_plan")).await;
    exec(&client, &format!("SYSTEM START VIEW {db}.metric_rollup_mv")).await;
    refresh_one(&client, db, "metric_rollup_mv").await;
    let n = int(
        &client,
        &format!("SELECT toInt64(count()) AS v FROM {db}.metric_rollup"),
    )
    .await;
    assert_eq!(n, 0, "a derivation with no plan row wrote rows");
    drop_db(&client, db).await;
}

/// **1-l.** Both views start after a backfill landed under a stale plan.
/// At no moment does the backfilled day have coverage without its data
/// rows, and it ends with both. `planner_first` is the start order.
async fn starting_the_views_never_shows_a_backfilled_day_as_empty(name: &str, planner_first: bool) {
    let client = ChClient::new(test_config()).await.expect("connect");
    let (db, t) = stopped_db(&client, name).await;
    let db = db.as_str();
    let ordinary: Vec<(u128, i64, Option<f64>)> = [t - 7, t - 2]
        .iter()
        .map(|d| (1, d * DAY + 6 * 3_600_000, Some(1.0)))
        .collect();
    exec(&client, &landing_values(db, &ordinary, false)).await;
    plan_empty_run(&client, db, &[t - 6, t - 5, t - 4]).await;
    exec(
        &client,
        &landing_values(db, &[(2, (t - 5) * DAY + 12 * 3_600_000, Some(3.0))], false),
    )
    .await;
    let order = if planner_first {
        VIEWS
    } else {
        [VIEWS[1], VIEWS[0]]
    };
    for view in order {
        exec(&client, &format!("SYSTEM START VIEW {db}.{view}")).await;
    }
    let table = format!("{db}.metric_rollup");
    let mut wrong = 0;
    for _ in 0..75 {
        let cov = coverage_rows(&client, &table).await;
        let covered = cov.iter().any(|c| c.day == t - 5);
        if covered && rows_of_day(&data_rows(&client, &table).await, t - 5) == 0 {
            wrong += 1;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    assert_eq!(
        wrong, 0,
        "polls at which T−5 had coverage and no data rows, of 75"
    );
    let mut n = 0;
    loop {
        refresh(&client, db).await;
        n += 1;
        if all_covered(&coverage_rows(&client, &table).await, &[t - 5], 0, FP_MAX) {
            break;
        }
        assert!(n < 20, "T−5 is not covered after 20 refreshes");
    }
    assert!(
        rows_of_day(&data_rows(&client, &table).await, t - 5) > 0,
        "T−5 ends with its data rows"
    );
    drop_db(&client, db).await;
}

/// **1-l, order A**: the planner starts first.
#[tokio::test]
async fn starting_the_planner_first_never_shows_a_backfilled_day_as_empty() {
    skip_unless_live!();
    starting_the_views_never_shows_a_backfilled_day_as_empty("pulsus_it_rollup_start_a", true)
        .await;
}

/// **1-l, order B**: the derivation starts first.
#[tokio::test]
async fn starting_the_derivation_first_never_shows_a_backfilled_day_as_empty() {
    skip_unless_live!();
    starting_the_views_never_shows_a_backfilled_day_as_empty("pulsus_it_rollup_start_b", false)
        .await;
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct DerivedRow {
    derived_ms: i64,
}

/// **1-m.** A row's `derived_ms` is the derivation's own clock, never the
/// plan's: a derivation run 3 s after its plan stamps the time it read.
#[tokio::test]
async fn a_row_is_stamped_with_the_derivations_clock() {
    skip_unless_live!();
    let client = ChClient::new(test_config()).await.expect("connect");
    let (db, t) = stopped_db(&client, "pulsus_it_rollup_stamp").await;
    let db = db.as_str();
    exec(
        &client,
        &landing_values(db, &[(1, (t - 3) * DAY + 6 * 3_600_000, Some(1.0))], false),
    )
    .await;
    // One planning refresh, which plans a data day, then the planner stops.
    exec(
        &client,
        &format!("SYSTEM START VIEW {db}.metric_rollup_plan_mv"),
    )
    .await;
    refresh_one(&client, db, "metric_rollup_plan_mv").await;
    exec(
        &client,
        &format!("SYSTEM STOP VIEW {db}.metric_rollup_plan_mv"),
    )
    .await;
    let day_rows = int(
        &client,
        &format!("SELECT toInt64(any(day_rows)) AS v FROM {db}.metric_rollup_plan"),
    )
    .await;
    assert!(day_rows > 0, "the plan names a data day");

    let t0 = now_ms(&client).await;
    while now_ms(&client).await < t0 + 3000 {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let t1 = now_ms(&client).await;
    exec(&client, &format!("SYSTEM START VIEW {db}.metric_rollup_mv")).await;
    refresh_one(&client, db, "metric_rollup_mv").await;
    let t2 = now_ms(&client).await;
    let stamps = rows::<DerivedRow>(
        &client,
        &format!("SELECT derived_ms FROM {db}.metric_rollup"),
    )
    .await;
    assert!(!stamps.is_empty(), "the derivation wrote rows");
    for s in &stamps {
        assert!(
            t1 <= s.derived_ms && s.derived_ms <= t2,
            "derived_ms {} is outside the derivation's run [{t1}, {t2}] (plan at {t0})",
            s.derived_ms
        );
    }
    drop_db(&client, db).await;
}
