//! The bucket summaries `metric_rollup` must hold, computed in plain Rust
//! from the stored raw samples (issue #18 part 1).
//!
//! Written from the design's tables, not from the view's statement:
//!
//! - A bucket of resolution `res` ending at `t_end` holds one series'
//!   samples with `t_end − res < unix_milli ≤ t_end`.
//! - **a** is the bucket's float samples sorted by `(unix_milli, value
//!   bits)` with exact repeats dropped; **s** is **a** without stale
//!   markers.
//! - `n = len(s)`; first, last and prev are `s[1]`, `s[n]` and `s[n − 1]`,
//!   each `0` when absent.
//! - min and max fold `m = s[1]; x: if x < m or isNaN(m) then x else m`,
//!   and the same with `>`.
//! - `changes` counts consecutive pairs with `y != p and not (isNaN(y) and
//!   isNaN(p))`; `n_resets` counts pairs with `y < p`, and `resets` lists
//!   each such `p` in time order, or is empty when `n_resets` exceeds the
//!   tier's cap (5m: 4, 1h: 16). A 1h row is also empty when any of its 5m
//!   rows overflowed.
//! - `t_any` and `any_stale` are **a**'s last sample's time and whether it
//!   is a stale marker; both `0` when **a** is empty.
//! - `has_hist` is 1 when the series has a native-histogram sample in the
//!   bucket. A bucket holding any sample has a row.
//!
//! Floats are carried as their bits throughout, so a comparison is exact
//! for NaN payloads and signed zeros.

use std::collections::{BTreeMap, BTreeSet};

/// The bits of the stale marker (`crates/pulsus-promql/src/eval/mod.rs`).
pub const STALE_BITS: u64 = 0x7FF0_0000_0000_0002;

pub const RES_5M: u32 = 300_000;
pub const RES_1H: u32 = 3_600_000;

/// One float sample: `(org_id, fingerprint, unix_milli, value bits)`.
pub type FloatSample = (String, u128, i64, u64);
/// One native-histogram sample: `(org_id, fingerprint, unix_milli)`.
pub type HistSample = (String, u128, i64);

/// One data row of `metric_rollup`, floats as bits. `derived_ms` is not
/// part of the summary; `expires_ms` is computed by the caller from the
/// retention.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Summary {
    pub res: u32,
    pub org_id: String,
    pub fingerprint: u128,
    pub t_end: i64,
    pub n: u32,
    pub t_first: i64,
    pub v_first: u64,
    pub t_last: i64,
    pub v_last: u64,
    pub t_prev: i64,
    pub v_prev: u64,
    pub v_min: u64,
    pub v_max: u64,
    pub changes: u32,
    pub n_resets: u32,
    pub resets: Vec<u64>,
    pub t_any: i64,
    pub any_stale: u8,
    pub has_hist: u8,
}

/// The end of the `res` bucket holding `t`: buckets are `(end − res, end]`.
pub fn bucket_end(t: i64, res: u32) -> i64 {
    let res = i64::from(res);
    (t - 1).div_euclid(res) * res + res
}

/// Every 5m and 1h summary the samples produce, sorted.
pub fn summarize(floats: &[FloatSample], hists: &[HistSample]) -> Vec<Summary> {
    let hist_keys = |res: u32| -> BTreeSet<(String, u128, i64)> {
        hists
            .iter()
            .map(|(org, fp, t)| (org.clone(), *fp, bucket_end(*t, res)))
            .collect()
    };
    let five = summarize_tier(
        &buckets(floats, hists, RES_5M),
        &hist_keys(RES_5M),
        RES_5M,
        4,
    );
    // A 1h row is overflowed when any of its 5m rows is.
    let overflowed: BTreeSet<(String, u128, i64)> = five
        .iter()
        .filter(|s| s.n_resets > 4)
        .map(|s| (s.org_id.clone(), s.fingerprint, bucket_end(s.t_end, RES_1H)))
        .collect();
    let mut hours = summarize_tier(
        &buckets(floats, hists, RES_1H),
        &hist_keys(RES_1H),
        RES_1H,
        16,
    );
    for h in &mut hours {
        if overflowed.contains(&(h.org_id.clone(), h.fingerprint, h.t_end)) {
            h.resets.clear();
        }
    }
    let mut out = five;
    out.extend(hours);
    out.sort();
    out
}

/// Each bucket's **a** as a set of `(unix_milli, bits)`: sorted, with exact
/// repeats gone. A bucket holding only histogram samples is present and
/// empty.
type Buckets = BTreeMap<(String, u128, i64), BTreeSet<(i64, u64)>>;

fn buckets(floats: &[FloatSample], hists: &[HistSample], res: u32) -> Buckets {
    let mut a: Buckets = BTreeMap::new();
    for (org, fp, t, bits) in floats {
        a.entry((org.clone(), *fp, bucket_end(*t, res)))
            .or_default()
            .insert((*t, *bits));
    }
    for (org, fp, t) in hists {
        a.entry((org.clone(), *fp, bucket_end(*t, res)))
            .or_default();
    }
    a
}

fn summarize_tier(
    buckets: &Buckets,
    hist: &BTreeSet<(String, u128, i64)>,
    res: u32,
    cap: usize,
) -> Vec<Summary> {
    let mut out = Vec::new();
    for ((org, fp, t_end), a) in buckets {
        let a: Vec<(i64, u64)> = a.iter().copied().collect();
        let s: Vec<(i64, u64)> = a.iter().copied().filter(|z| z.1 != STALE_BITS).collect();
        let ys: Vec<f64> = s.iter().map(|z| f64::from_bits(z.1)).collect();
        let n = s.len();
        let at = |i: Option<usize>| i.and_then(|i| s.get(i)).copied().unwrap_or((0, 0));
        let first = at(Some(0));
        let last = at(n.checked_sub(1));
        let prev = at(n.checked_sub(2));
        let fold = |less: fn(f64, f64) -> bool| -> u64 {
            let Some(&m0) = ys.first() else { return 0 };
            ys.iter()
                .fold(m0, |m, &x| if less(x, m) || m.is_nan() { x } else { m })
                .to_bits()
        };
        let mut changes = 0u32;
        let mut resets = Vec::new();
        for w in ys.windows(2) {
            let (p, y) = (w[0], w[1]);
            if y != p && !(y.is_nan() && p.is_nan()) {
                changes += 1;
            }
            if y < p {
                resets.push(p.to_bits());
            }
        }
        let n_resets = resets.len();
        if n_resets > cap {
            resets.clear();
        }
        let (t_any, any_stale) = match a.last() {
            Some(&(t, bits)) => (t, u8::from(bits == STALE_BITS)),
            None => (0, 0),
        };
        out.push(Summary {
            res,
            org_id: org.clone(),
            fingerprint: *fp,
            t_end: *t_end,
            n: n as u32,
            t_first: first.0,
            v_first: first.1,
            t_last: last.0,
            v_last: last.1,
            t_prev: prev.0,
            v_prev: prev.1,
            v_min: fold(|x, m| x < m),
            v_max: fold(|x, m| x > m),
            changes,
            n_resets: n_resets as u32,
            resets,
            t_any,
            any_stale,
            has_hist: u8::from(hist.contains(&(org.clone(), *fp, *t_end))),
        });
    }
    out
}
