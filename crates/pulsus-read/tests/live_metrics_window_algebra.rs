//! Issue #18 part 3: the window algebra and the canonical raw order,
//! against a live ClickHouse.
//!
//! Two series of each of the design's eight corpus kinds, two UTC days
//! of samples, landed through `metric_landing`. The test keeps what it
//! wrote: **E**, per series, is the written samples sorted by `(time,
//! value bits)` with exact repeats dropped.
//!
//! 1. Each series' `sample_fetch` read, de-duplicated within each
//!    millisecond as the raw route does, is E, bit for bit. Kind 7 writes
//!    its equal-millisecond pairs over two inserts, the higher-bits value
//!    first for one series and second for the other, so a read in storage
//!    order fails here.
//! 2. For every 5m-aligned window of the design's range shapes, every
//!    finisher over `window` of the window's 5-minute `segment_of` pieces
//!    equals the base revision's function over the window's samples of
//!    E, as bits.
//! 3. The same with each piece passed through `cap(…, 4)`, the 5m tier's
//!    cap: a window holding an overflowed piece answers nothing under
//!    `rate` and `increase`, and every other answer is unchanged.
//!
//! Gated behind `PULSUS_TEST_CLICKHOUSE=1`:
//!
//! ```text
//! PULSUS_TEST_CLICKHOUSE=1 PULSUS_TEST_CH_HTTP_PORT=<port> \
//!   PULSUS_TEST_CH_DATABASE_PREFIX=<yours> \
//!   cargo test -p pulsus-read --test live_metrics_window_algebra
//! ```

#[path = "pushed_rate_corpus/mod.rs"]
mod pushed_rate_corpus;

/// The base revision's window functions, verbatim.
#[allow(dead_code)]
mod legacy {
    use pulsus_promql::Sample;
    include!("../../pulsus-promql/src/eval/functions_legacy.rs");
}

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::StreamExt;
use pulsus_clickhouse::{QuerySettings, Row};
use pulsus_model::{Fingerprint, STALE_NAN_BITS};
use pulsus_promql::Sample;
use pulsus_promql::eval::window::{
    Segment, WindowSummary, cap, finish_extrapolated, finish_idelta, finish_irate, segment_of,
    window,
};
use pulsus_read::metrics::sample_sql;
use pulsus_write::{MetricLandingRow, MetricPoint};
use pushed_rate_corpus::{DAY_MS, Rng, drop_database, fresh_database, now_ms};

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

const METRIC: &str = "window_algebra";
const BUCKET_MS: i64 = 300_000;
const HOUR_MS: i64 = 3_600_000;
const BUCKETS: i64 = 2 * DAY_MS / BUCKET_MS;

/// One written series: its fingerprint, the first insert's samples and
/// the second's.
struct Written {
    kind: u32,
    fp: u128,
    first: Vec<(i64, f64)>,
    second: Vec<(i64, f64)>,
}

/// Timestamps `i · scrape ± jitter` from `day0`, for `i` in `1..=n`.
fn times(rng: &mut Rng, day0: i64, scrape: i64, jitter: u64, n: i64) -> Vec<i64> {
    (1..=n)
        .map(|i| day0 + i * scrape + rng.upto(2 * jitter) - jitter as i64)
        .collect()
}

/// The design's eight kinds, two series each, over the two days after
/// `day0`.
fn corpus(day0: i64) -> Vec<Written> {
    let stale = f64::from_bits(STALE_NAN_BITS);
    let per_day = DAY_MS / 15_000;
    let mut out = Vec::new();
    for kind in 0..8u32 {
        for j in 0..2u32 {
            let mut rng = Rng::new(0x0018_0300 + u64::from(kind * 2 + j));
            let fp = 0x1803_0000_0000_0000_0000_0000_0000_0000 | u128::from(kind * 2 + j);
            let mut first = Vec::new();
            let mut second = Vec::new();
            let mut v = 0.0_f64;
            match kind {
                // A counter that resets every 2,000 to 19,999 samples.
                0 => {
                    let mut next = 2_000 + rng.upto(17_999);
                    for (i, t) in times(&mut rng, day0, 15_000, 1_000, 2 * per_day)
                        .into_iter()
                        .enumerate()
                    {
                        if i as i64 == next {
                            v = 0.0;
                            next += 2_000 + rng.upto(17_999);
                        } else {
                            v += rng.uniform(0.0, 10.0);
                        }
                        first.push((t, v));
                    }
                }
                // A gauge with an ordinary NaN about once in 997 samples.
                1 => {
                    for t in times(&mut rng, day0, 15_000, 1_000, 2 * per_day) {
                        let v = if rng.upto(996) == 0 {
                            f64::NAN
                        } else {
                            rng.uniform(-100.0, 100.0)
                        };
                        first.push((t, v));
                    }
                }
                // A counter of small values that resets every 30 to 49
                // samples.
                2 => {
                    let mut next = 30 + rng.upto(19);
                    for (i, t) in times(&mut rng, day0, 15_000, 1_000, 2 * per_day)
                        .into_iter()
                        .enumerate()
                    {
                        if i as i64 == next {
                            v = 0.0;
                            next += 30 + rng.upto(19);
                        } else {
                            v += rng.upto(3) as f64;
                        }
                        first.push((t, v));
                    }
                }
                // A gauge with a stale marker, then a 39-sample gap, every
                // 2,000 samples.
                3 => {
                    let ts = times(&mut rng, day0, 15_000, 1_000, 2 * per_day);
                    let mut i = 0;
                    while i < ts.len() {
                        if i > 0 && i % 2_000 == 0 {
                            first.push((ts[i], stale));
                            i += 40;
                            continue;
                        }
                        first.push((ts[i], rng.uniform(0.0, 50.0)));
                        i += 1;
                    }
                }
                // A counter scraped every 10 s with ±3 s of jitter.
                4 => {
                    for t in times(&mut rng, day0, 10_000, 3_000, 2 * DAY_MS / 10_000) {
                        v += rng.uniform(0.0, 5.0);
                        first.push((t, v));
                    }
                }
                // A counter whose every sample is stored twice.
                5 => {
                    for t in times(&mut rng, day0, 15_000, 1_000, 2 * per_day) {
                        v += rng.uniform(0.0, 3.0);
                        first.push((t, v));
                        first.push((t, v));
                    }
                }
                // 0.0, -0.0 and 1.0.
                6 => {
                    for t in times(&mut rng, day0, 15_000, 1_000, 2 * per_day) {
                        first.push((t, [0.0, -0.0, 1.0][rng.upto(2) as usize]));
                    }
                }
                // A gauge with a second value at the same millisecond every
                // 1,500 samples. The pair is split over the two inserts:
                // series 0 writes the higher-bits value first, series 1
                // second.
                _ => {
                    for (i, t) in times(&mut rng, day0, 15_000, 1_000, 2 * per_day)
                        .into_iter()
                        .enumerate()
                    {
                        let v = rng.uniform(1.0, 100.0);
                        if i > 0 && i % 1_500 == 0 {
                            let (lo, hi) = (v, v + 0.5);
                            let (a, b) = if j == 0 { (hi, lo) } else { (lo, hi) };
                            first.push((t, a));
                            second.push((t, b));
                        } else {
                            first.push((t, v));
                        }
                    }
                }
            }
            out.push(Written {
                kind,
                fp,
                first,
                second,
            });
        }
    }
    out
}

/// E: the written samples sorted by `(time, value bits)`, exact repeats
/// dropped.
fn canonical(w: &Written) -> Vec<(i64, u64)> {
    let mut e: Vec<(i64, u64)> = w
        .first
        .iter()
        .chain(&w.second)
        .map(|(t, v)| (*t, v.to_bits()))
        .collect();
    e.sort_unstable();
    e.dedup();
    e
}

async fn land(client: &pulsus_clickhouse::ChClient, rows: Vec<(u128, i64, f64)>) {
    let org: Arc<str> = Arc::from("");
    let name: Arc<str> = Arc::from(METRIC);
    let received = now_ms();
    let landed: Vec<MetricLandingRow> = rows
        .into_iter()
        .map(|(fp, t, v)| {
            MetricLandingRow::float_sample(
                &org,
                received,
                &MetricPoint {
                    metric_name: Arc::clone(&name),
                    fingerprint: Fingerprint::from_raw(fp),
                    unix_milli: t,
                    value: v,
                },
            )
        })
        .collect();
    client
        .insert_block("metric_landing", &landed)
        .await
        .expect("insert into metric_landing");
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug)]
struct FetchedRow {
    fingerprint: Fingerprint,
    unix_milli: i64,
    value: f64,
}

/// The fetched rows of one series, de-duplicated within each millisecond
/// as the raw route does: each distinct value-bit identity once, in the
/// order read.
fn dedup_runs(rows: &[(i64, u64)]) -> Vec<(i64, u64)> {
    let mut out: Vec<(i64, u64)> = Vec::new();
    let mut run_start = 0;
    for &(t, b) in rows {
        if out.last().is_none_or(|(lt, _)| *lt != t) {
            run_start = out.len();
        }
        if !out[run_start..].iter().any(|(_, ob)| *ob == b) {
            out.push((t, b));
        }
    }
    out
}

fn bits(v: Option<f64>) -> Option<u64> {
    v.map(f64::to_bits)
}

const FINISHERS: [&str; 9] = [
    "rate", "increase", "delta", "irate", "idelta", "resets", "changes", "min", "max",
];

/// Every finisher over a window summary.
fn from_summary(w: Option<&WindowSummary>, range: i64, lo: i64, hi: i64) -> [Option<u64>; 9] {
    let Some(w) = w else {
        return [None; 9];
    };
    [
        bits(finish_extrapolated(w, range, lo, hi, true, true)),
        bits(finish_extrapolated(w, range, lo, hi, true, false)),
        bits(finish_extrapolated(w, range, lo, hi, false, false)),
        bits(finish_irate(w)),
        bits(finish_idelta(w)),
        Some(f64::from(w.resets).to_bits()),
        Some(f64::from(w.changes).to_bits()),
        Some(w.min.to_bits()),
        Some(w.max.to_bits()),
    ]
}

/// Every finisher's base-revision function over a window's samples.
fn from_legacy(samples: &[Sample], range: i64, lo: i64, hi: i64) -> [Option<u64>; 9] {
    let nonempty = |f: &dyn Fn(&[Sample]) -> f64| (!samples.is_empty()).then(|| f(samples));
    [
        bits(legacy::eval_extrapolated(
            samples, range, lo, hi, true, true, None,
        )),
        bits(legacy::eval_extrapolated(
            samples, range, lo, hi, true, false, None,
        )),
        bits(legacy::eval_extrapolated(
            samples, range, lo, hi, false, false, None,
        )),
        bits(legacy::eval_irate(samples, None)),
        bits(legacy::eval_idelta(samples)),
        bits(nonempty(&legacy::eval_resets)),
        bits(nonempty(&legacy::eval_changes)),
        bits(nonempty(&|s| {
            legacy::eval_extremum_over_time(s, legacy::WhichExtremum::Min)
        })),
        bits(nonempty(&|s| {
            legacy::eval_extremum_over_time(s, legacy::WhichExtremum::Max)
        })),
    ]
}

/// The tallies of one series' window comparisons.
#[derive(Default, Debug)]
struct Tally {
    windows: usize,
    compared: usize,
    overflowed: usize,
    capped_with_corrections: usize,
}

/// (ii) and (iii) over one series' E.
fn check_windows(w: &Written, e: &[(i64, u64)], day0: i64, tally: &mut Tally) {
    let samples: Vec<Sample> = e
        .iter()
        .map(|(t, b)| Sample::float(*t, f64::from_bits(*b)))
        .collect();
    // Bucket k is (day0 + (k-1)·5m, day0 + k·5m], for k in 1..=BUCKETS.
    let bucket_of = |t: i64| (t - day0 + BUCKET_MS - 1) / BUCKET_MS;
    let mut buckets: Vec<Vec<Sample>> = vec![Vec::new(); BUCKETS as usize + 1];
    for s in &samples {
        let k = bucket_of(s.t_ms);
        if (1..=BUCKETS).contains(&k) {
            buckets[k as usize].push(s.clone());
        }
    }
    let pieces: Vec<Option<Segment>> = buckets.iter().map(|b| segment_of(b)).collect();
    let capped: Vec<Option<Segment>> = pieces
        .iter()
        .map(|p| p.clone().map(|s| cap(s, 4)))
        .collect();

    // The design's range shapes whose window is 5m-aligned: (step, range).
    let shapes: [(i64, i64); 6] = [
        (BUCKET_MS, BUCKET_MS),
        (BUCKET_MS, HOUR_MS),
        (BUCKET_MS, 3 * HOUR_MS),
        (HOUR_MS, HOUR_MS),
        (HOUR_MS, 3 * HOUR_MS),
        (HOUR_MS, DAY_MS),
    ];
    for (step, range) in shapes {
        let mut end = day0 + step;
        while end <= day0 + 2 * DAY_MS {
            let lo = end - range;
            let k_hi = bucket_of(end);
            let k_lo = (k_hi - range / BUCKET_MS + 1).max(1);
            let segs = |from: &[Option<Segment>]| -> Vec<Segment> {
                from[k_lo as usize..=k_hi as usize]
                    .iter()
                    .flatten()
                    .cloned()
                    .collect()
            };
            let from = samples.partition_point(|s| s.t_ms <= lo);
            let to = samples.partition_point(|s| s.t_ms <= end);
            let raw: Vec<Sample> = samples[from..to]
                .iter()
                .filter(|s| !s.is_stale())
                .cloned()
                .collect();
            let want = from_legacy(&raw, range, lo, end);

            // (ii) uncapped pieces.
            let got = from_summary(window(&segs(&pieces)).as_ref(), range, lo, end);
            for (i, name) in FINISHERS.iter().enumerate() {
                assert_eq!(
                    got[i], want[i],
                    "kind {} series {:#x}: {name} over ({lo}, {end}], uncapped pieces",
                    w.kind, w.fp
                );
            }

            // (iii) the 5m tier's cap.
            let summary = window(&segs(&capped));
            let got = from_summary(summary.as_ref(), range, lo, end);
            let overflowed = summary.as_ref().is_some_and(|s| s.overflow);
            for (i, name) in FINISHERS.iter().enumerate() {
                let expect = if overflowed && (*name == "rate" || *name == "increase") {
                    None
                } else {
                    want[i]
                };
                assert_eq!(
                    got[i], expect,
                    "kind {} series {:#x}: {name} over ({lo}, {end}], capped pieces",
                    w.kind, w.fp
                );
            }
            tally.windows += 1;
            tally.compared += 2 * FINISHERS.len();
            tally.overflowed += usize::from(overflowed);
            tally.capped_with_corrections += usize::from(
                summary
                    .as_ref()
                    .is_some_and(|s| !s.overflow && s.corrections.iter().any(|c| *c != 0.0)),
            );
            end += step;
        }
    }
}

/// 3-g.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_window_algebra_answers_as_the_raw_functions_over_the_fetched_order() {
    skip_unless_live!();
    let day0 = (now_ms() / DAY_MS - 2) * DAY_MS;
    let written = corpus(day0);
    let db = pulsus_testkit::test_db("pulsus_read_it_window_algebra");
    let (bootstrap, client) = fresh_database(&db).await;

    // Two inserts: everything, then kind 7's second values.
    land(
        &client,
        written
            .iter()
            .flat_map(|w| w.first.iter().map(move |(t, v)| (w.fp, *t, *v)))
            .collect(),
    )
    .await;
    land(
        &client,
        written
            .iter()
            .flat_map(|w| w.second.iter().map(move |(t, v)| (w.fp, *t, *v)))
            .collect(),
    )
    .await;

    // (i) the fetched order is E.
    let fps: Vec<_> = written
        .iter()
        .map(|w| Fingerprint::from_raw(w.fp).sql_literal())
        .collect();
    let sql = sample_sql::sample_fetch(
        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        "metric_samples",
        &fps,
        day0,
        day0 + 2 * DAY_MS + 10_000,
    );
    let mut fetched: BTreeMap<Fingerprint, Vec<(i64, u64)>> = BTreeMap::new();
    let mut stream = client
        .query_stream::<FetchedRow>(&sql, &QuerySettings::new())
        .await
        .expect("sample_fetch");
    while let Some(row) = stream.next().await {
        let row = row.expect("decode");
        fetched
            .entry(row.fingerprint)
            .or_default()
            .push((row.unix_milli, row.value.to_bits()));
    }
    drop(stream);

    let mut pairs_read = 0;
    for w in &written {
        let e = canonical(w);
        let got = dedup_runs(
            fetched
                .get(&Fingerprint::from_raw(w.fp))
                .map_or(&[][..], Vec::as_slice),
        );
        assert_eq!(
            got.len(),
            e.len(),
            "kind {} series {:#x}: rows",
            w.kind,
            w.fp
        );
        let first_difference = got.iter().zip(&e).position(|(a, b)| a != b);
        assert_eq!(
            first_difference,
            None,
            "kind {} series {:#x}: the fetched order is not (time, value bits); at that row \
             it read {:?} where E holds {:?}",
            w.kind,
            w.fp,
            first_difference.map(|i| got[i]),
            first_difference.map(|i| e[i]),
        );
        pairs_read += e.windows(2).filter(|p| p[0].0 == p[1].0).count();
    }

    // (ii) and (iii), over what the test wrote.
    let mut tally = Tally::default();
    for w in &written {
        check_windows(w, &canonical(w), day0, &mut tally);
    }
    eprintln!("equal-millisecond pairs read: {pairs_read}; {tally:?}");
    assert_eq!(
        pairs_read, 14,
        "kind 7's equal-millisecond pairs, seven a series"
    );
    assert!(
        tally.overflowed > 0,
        "some window holds an overflowed 5m piece"
    );
    assert!(
        tally.capped_with_corrections > 0,
        "some capped window keeps its corrections"
    );
    drop_database(&bootstrap, &db).await;
}
