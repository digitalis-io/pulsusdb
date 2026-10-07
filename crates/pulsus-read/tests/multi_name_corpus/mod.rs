//! Issue #579 part 2: the multi-name corpus, shared by
//! `live_metrics_pushed_rate.rs` and `query_log_gates.rs` through
//! `#[path]`, beside `pushed_rate_corpus/mod.rs`, whose seeding and harness
//! it uses.
//!
//! 280 names `node_m0` … `node_m279` with the demo's series-per-name
//! distribution — 205 names with one series, 35 with two, … one with 128:
//! 1,012 series, labels `instance`, `job` and `k`. One sample per 15 s with
//! 0-900 ms jitter for the last 7 h 5 min. 16 series end with a stale
//! marker 3 h before the end, 10 start 2 h before it, 5 have a 10-minute
//! gap 4 h before it.
//!
//! Beside them, `node_nan_a` and `node_nan_b`, two series each, whose
//! samples are NaN payloads (never the stale marker) for one hour. The
//! name prefix of `node_nan_a` sorts above `node_nan_b`'s, so ascending
//! fingerprint order and `(metric_name, fingerprint)` order put a
//! different series last.
//!
//! And `hmix_a`, `hmix_b`, two float counters, the second holding one
//! histogram sample, for the multi-name histogram fallback.

#![allow(dead_code)]

use pulsus_model::{LabelSet, STALE_NAN_BITS, raw_cityhash64, series_fingerprint};

use crate::pushed_rate_corpus::{SCRAPE_MS, SeedSeries};

/// Scrapes in the corpus: the last 7 h 5 min.
pub const SCRAPES: i64 = 1_700;
pub const HOUR_MS: i64 = 3_600_000;

/// The Series panel's query.
pub const SERIES_PANEL: &str = "count(count by (__name__)({__name__=~\"node_.+\"}))";

/// Series per name, as `(series, names)`, ascending: the demo's
/// distribution.
const DISTRIBUTION: [(u32, u32); 14] = [
    (1, 205),
    (2, 35),
    (3, 4),
    (4, 1),
    (8, 1),
    (9, 7),
    (13, 1),
    (15, 1),
    (16, 16),
    (17, 2),
    (22, 2),
    (32, 2),
    (48, 2),
    (128, 1),
];

fn labels(k: u32) -> Vec<(String, String)> {
    vec![
        ("instance".to_string(), "host-1:9100".to_string()),
        ("job".to_string(), "node".to_string()),
        ("k".to_string(), k.to_string()),
    ]
}

fn fp(name: &str, l: &[(String, String)]) -> u128 {
    series_fingerprint(name, &LabelSet::from_verbatim(l.to_vec()))
        .sql_literal()
        .to_string()
        .trim_start_matches("toUInt128('")
        .trim_end_matches("')")
        .parse()
        .expect("a decimal ID")
}

/// A fixed jitter in `[0, 900)` ms, from the series and scrape index.
fn jitter(s: u32, i: i64) -> i64 {
    let mut b = s.to_le_bytes().to_vec();
    b.extend(i.to_le_bytes());
    (raw_cityhash64(&b) % 900) as i64
}

/// The corpus, anchored so the range ends at `t_end`.
pub fn multi_name_corpus(t_end: i64) -> Vec<SeedSeries> {
    let stale = f64::from_bits(STALE_NAN_BITS);
    let first = t_end - (SCRAPES - 1) * SCRAPE_MS;
    let mut out = Vec::new();
    let mut idx = 0u32;
    let mut s = 0u32;
    for (per, names) in DISTRIBUTION {
        for _ in 0..names {
            let name = format!("node_m{idx}");
            idx += 1;
            for k in 0..per {
                let l = labels(k);
                let life = if s % 67 == 5 {
                    1
                } else if s % 101 == 7 {
                    2
                } else if s % 211 == 11 {
                    3
                } else {
                    0
                };
                let mut samples = Vec::new();
                for i in 0..SCRAPES {
                    let ts = first + i * SCRAPE_MS + jitter(s, i);
                    if ts > t_end {
                        continue;
                    }
                    let stop = t_end - 3 * HOUR_MS;
                    if life == 1 && ts >= stop {
                        if ts < stop + SCRAPE_MS {
                            samples.push((ts, stale));
                        }
                        continue;
                    }
                    if life == 2 && ts < t_end - 2 * HOUR_MS {
                        continue;
                    }
                    if life == 3 && ts >= t_end - 4 * HOUR_MS && ts < t_end - 4 * HOUR_MS + 600_000
                    {
                        continue;
                    }
                    samples.push((ts, i as f64 * 3.5 + f64::from(s)));
                }
                out.push(SeedSeries {
                    fp: fp(&name, &l),
                    metric: name.clone(),
                    labels: l,
                    samples,
                    hist: Vec::new(),
                });
                s += 1;
            }
        }
    }
    assert_eq!(idx, 280);
    assert_eq!(s, 1_012);

    // The NaN pair: a distinct NaN payload per series for the hour before
    // the last 30 minutes, ordinary values elsewhere.
    assert!(
        raw_cityhash64(b"node_nan_a") >> 32 > raw_cityhash64(b"node_nan_b") >> 32,
        "fingerprint order must put node_nan_a last, name order node_nan_b"
    );
    let mut payload = 0u64;
    for name in ["node_nan_a", "node_nan_b"] {
        for k in 0..2 {
            payload += 1;
            let l = labels(k);
            let samples = (0..SCRAPES)
                .map(|i| {
                    let ts = first + i * SCRAPE_MS;
                    let in_hour = ts > t_end - 90 * 60_000 && ts <= t_end - 30 * 60_000;
                    let v = if in_hour {
                        f64::from_bits(0x7FF8_0000_0000_0000 | payload)
                    } else {
                        i as f64 + payload as f64
                    };
                    (ts, v)
                })
                .collect();
            out.push(SeedSeries {
                fp: fp(name, &l),
                metric: name.to_string(),
                labels: l,
                samples,
                hist: Vec::new(),
            });
        }
    }

    // The histogram pair.
    for name in ["hmix_a", "hmix_b"] {
        let l = labels(0);
        out.push(SeedSeries {
            fp: fp(name, &l),
            metric: name.to_string(),
            labels: l,
            samples: (0..SCRAPES)
                .map(|i| (first + i * SCRAPE_MS, i as f64 * 2.0))
                .collect(),
            hist: if name == "hmix_b" {
                vec![t_end - 20 * SCRAPE_MS + 7_000]
            } else {
                Vec::new()
            },
        });
    }
    out
}

/// §5's queries of the design, the Series panel first.
pub const QUERIES: [&str; 15] = [
    SERIES_PANEL,
    "count by (__name__)({__name__=~\"node_.+\"})",
    "group by (__name__)({job=\"node\"})",
    "min by (job)({__name__=~\"node_m1.*\"})",
    "max({__name__=~\"node_m2.*\"})",
    "count without (k)({__name__=~\"node_m(1|2)[0-9]\"})",
    "sum by (k)(rate({__name__=~\"node_m1[0-9]\"}[5m]))",
    "avg(irate({__name__=~\"node_m(0|5|9).*\"}[5m]))",
    "max without (k)(increase({__name__=~\"node_m2.*\"}[5m]))",
    "sum(rate({__name__=~\"node_.+\"}[5m]))",
    "avg(irate({__name__=~\"node_m27.*\"}[5m]))",
    "avg by (k)(rate({__name__=~\"node_m(1|2).*\"}[5m]))",
    "sum by (k)(increase({__name__=~\"node_.+\"}[5m]))",
    "min(rate({__name__=~\"node_m2.*\"}[5m]))",
    "count(rate({__name__=~\"node_.+\"}[5m]))",
];
