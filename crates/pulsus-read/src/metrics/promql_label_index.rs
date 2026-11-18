//! The PromQL label cache's per-tenant index (issue #880). Tests first:
//! the type exists, its behaviour does not yet.
#![allow(dead_code, unused_variables)]

use std::collections::HashMap;

use pulsus_model::{ACTIVITY_BUCKET_MS, Fingerprint, LabelSet};
use regex::Regex;

use super::labels::{FallbackReason, RegexCache};
use super::matcher::{LabelMatcher, MatchOp};

pub(crate) type SeriesId = u32;

#[derive(Debug, Default)]
struct Symbols;
impl Symbols {
    fn lookup(&self, s: &str) -> Option<u32> {
        None
    }
}

#[derive(Debug, Default)]
struct SegVec;
impl SegVec {
    fn get(&self, i: usize) -> u32 {
        0
    }
}

pub(crate) fn intersect(a: &[SeriesId], b: &[SeriesId]) -> Vec<SeriesId> {
    Vec::new()
}

#[derive(Debug, Default)]
pub struct LabelIndex {
    symbols: Symbols,
    last_hour: SegVec,
    by_metric: HashMap<u32, Vec<SeriesId>>,
    postings: HashMap<u64, Vec<SeriesId>>,
}

impl LabelIndex {
    pub fn len(&self) -> usize {
        0
    }
    pub(crate) fn find(&self, fp: Fingerprint) -> Option<SeriesId> {
        None
    }
    pub(crate) fn apply<F: FnOnce() -> LabelSet>(
        &mut self,
        fp: Fingerprint,
        metric_name: &str,
        bucket_ms: i64,
        lower_bound_ms: i64,
        labels: F,
    ) {
    }
    pub(crate) fn fingerprint(&self, id: SeriesId) -> Fingerprint {
        Fingerprint::from_raw(0)
    }
    pub(crate) fn label_set(&self, id: SeriesId) -> LabelSet {
        LabelSet::default()
    }
    pub(crate) fn select(
        &self,
        metric_name: &str,
        matchers: &[LabelMatcher],
        regex_cache: &RegexCache,
    ) -> Result<Vec<SeriesId>, FallbackReason> {
        Ok(Vec::new())
    }
    pub(crate) fn age_out(&mut self, lower_bound_ms: i64) -> usize {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: i64 = ACTIVITY_BUCKET_MS;

    fn labels(pairs: &[(&str, &str)]) -> LabelSet {
        LabelSet::from_verbatim(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<Vec<_>>(),
        )
    }

    fn put(ix: &mut LabelIndex, fp: u128, metric: &str, bucket_ms: i64, ls: &LabelSet) {
        ix.apply(Fingerprint::from_raw(fp), metric, bucket_ms, 0, || ls.clone());
    }

    fn materialise(ix: &LabelIndex, ids: &[SeriesId]) -> Vec<(Fingerprint, LabelSet)> {
        let mut out: Vec<(Fingerprint, LabelSet)> = ids
            .iter()
            .map(|&id| (ix.fingerprint(id), ix.label_set(id)))
            .collect();
        out.sort_by_key(|(fp, _)| *fp);
        out
    }

    /// A small xorshift generator: the fixture and the selectors are the
    /// same on every run.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    fn matcher(key: &str, op: MatchOp, value: &str) -> LabelMatcher {
        LabelMatcher {
            key: key.to_string(),
            op,
            value: value.to_string(),
        }
    }

    /// The brute-force verdict: every matcher on `get(key).unwrap_or("")`,
    /// a regex anchored as the resolver anchors it (`compiled[i]` is
    /// matcher `i`'s).
    fn brute_matches(ls: &LabelSet, matchers: &[LabelMatcher], compiled: &[Option<Regex>]) -> bool {
        matchers.iter().zip(compiled).all(|(m, re)| {
            let v = ls.get(&m.key).unwrap_or("");
            match (m.op, re) {
                (MatchOp::Eq, _) => v == m.value,
                (MatchOp::Neq, _) => v != m.value,
                (op, Some(re)) => re.is_match(v) == (op == MatchOp::Re),
                (_, None) => unreachable!("every regex matcher is compiled"),
            }
        })
    }

    /// **T1.** 20,000 seeded series and 400 seeded selectors: the index's
    /// answer equals a scan of the generator's list.
    #[test]
    fn index_answers_equal_a_brute_force_scan() {
        let mut rng = Rng(0x880);
        let names: Vec<String> = (0..300).map(|i| format!("m_{i:03}")).collect();
        let keys = [
            "container", "endpoint", "instance", "job", "namespace", "node", "pod", "service",
            "image", "id", "cpu", "device", "le", "scope", "zone", "opt",
        ];
        let mut series: Vec<(String, Fingerprint, LabelSet)> = Vec::with_capacity(20_000);
        for i in 0..20_000u64 {
            let metric = names[rng.below(300) as usize].clone();
            let n = 12 + rng.below(5) as usize;
            let mut pairs: Vec<(String, String)> = Vec::new();
            for k in keys.iter().take(n) {
                if *k == "opt" && i % 3 == 0 {
                    // Absent on a third of the series.
                    continue;
                }
                let v = match *k {
                    // A stored empty value.
                    "zone" if i % 7 == 0 => String::new(),
                    // A value equal to another series' key, and one equal
                    // to a metric name.
                    "zone" if i % 7 == 1 => "pod".to_string(),
                    "zone" if i % 7 == 2 => "m_007".to_string(),
                    _ => format!("{k}-{}", rng.below(12)),
                };
                pairs.push((k.to_string(), v));
            }
            if i % 5 == 0 {
                pairs.push(("opt".to_string(), String::new()));
            }
            let fp = Fingerprint::from_raw((u128::from(rng.next()) << 64) | u128::from(i));
            series.push((metric, fp, LabelSet::from_verbatim(pairs)));
        }
        // One label set under two metric names.
        let twin = series[0].2.clone();
        series.push(("m_001".to_string(), Fingerprint::from_raw(7), twin.clone()));
        series.push(("m_002".to_string(), Fingerprint::from_raw(8), twin));

        let mut ix = LabelIndex::default();
        // 2 % of the series first arrive an hour earlier, age out, and
        // arrive again.
        let h = 500_000 * H;
        for (i, (m, fp, ls)) in series.iter().enumerate() {
            let bucket = if i % 50 == 0 { h - H } else { h };
            ix.apply(*fp, m, bucket, 0, || ls.clone());
        }
        let before = ix.len();
        assert_eq!(ix.age_out(h), series.len().div_ceil(50));
        assert_eq!(ix.len() + series.len().div_ceil(50), before);
        for (i, (m, fp, ls)) in series.iter().enumerate() {
            if i % 50 == 0 {
                ix.apply(*fp, m, h, h, || ls.clone());
            }
        }
        assert_eq!(ix.len(), series.len());

        let ops = [MatchOp::Eq, MatchOp::Neq, MatchOp::Re, MatchOp::Nre];
        let regexes = ["", ".*", "pod-1.*", "(pod|m_007)", "x?", "job-[0-3]", "a|"];
        let regex_cache = RegexCache::new(4_096);
        let mut non_empty = 0;
        for _ in 0..400 {
            let (metric, _, sample) = &series[rng.below(series.len() as u64) as usize];
            let mut matchers = Vec::new();
            for _ in 0..rng.below(4) {
                let key = keys[rng.below(keys.len() as u64) as usize];
                let op = ops[rng.below(4) as usize];
                let value = match (op, rng.below(4)) {
                    (MatchOp::Eq | MatchOp::Neq, 0) => String::new(),
                    (MatchOp::Eq | MatchOp::Neq, _) => {
                        sample.get(key).unwrap_or("absent-value").to_string()
                    }
                    (_, _) => regexes[rng.below(regexes.len() as u64) as usize].to_string(),
                };
                matchers.push(matcher(key, op, &value));
            }
            let got = materialise(
                &ix,
                &ix.select(metric, &matchers, &regex_cache)
                    .expect("every test pattern compiles"),
            );
            let compiled: Vec<Option<Regex>> = matchers
                .iter()
                .map(|m| {
                    matches!(m.op, MatchOp::Re | MatchOp::Nre).then(|| {
                        Regex::new(&format!("^(?:{})$", m.value)).expect("a test pattern")
                    })
                })
                .collect();
            let mut want: Vec<(Fingerprint, LabelSet)> = series
                .iter()
                .filter(|(m, _, ls)| m == metric && brute_matches(ls, &matchers, &compiled))
                .map(|(_, fp, ls)| (*fp, ls.clone()))
                .collect();
            want.sort_by_key(|(fp, _)| *fp);
            if !want.is_empty() {
                non_empty += 1;
            }
            assert_eq!(got, want, "{metric} {matchers:?}");
        }
        assert!(non_empty > 200, "the selectors must mostly match: {non_empty} of 400");
    }

    /// **T2.**
    #[test]
    fn age_out_removes_exactly_the_series_behind_the_edge() {
        let h = 400_000 * H;
        let mut ix = LabelIndex::default();
        let old = labels(&[("job", "api"), ("only", "the-oldest")]);
        let mid = labels(&[("job", "api"), ("pod", "p1")]);
        let new = labels(&[("job", "web"), ("pod", "p2")]);
        put(&mut ix, 1, "up", h - 2 * H, &old);
        put(&mut ix, 2, "up", h - H, &mid);
        put(&mut ix, 3, "up", h, &new);
        assert_eq!(ix.len(), 3);

        assert_eq!(ix.age_out(h - H), 1);
        assert_eq!(ix.len(), 2);
        for list in ix.postings.values().chain(ix.by_metric.values()) {
            assert!(list.iter().all(|&id| (id as usize) < ix.len()), "{list:?}");
            assert!(list.windows(2).all(|w| w[0] < w[1]), "{list:?}");
        }
        assert_eq!(ix.symbols.lookup("the-oldest"), None);
        assert_eq!(ix.symbols.lookup("only"), None);
        assert_eq!(ix.find(Fingerprint::from_raw(1)), None);
        let survivors: Vec<(Fingerprint, LabelSet)> = [2u128, 3]
            .iter()
            .map(|&fp| {
                let id = ix.find(Fingerprint::from_raw(fp)).expect("a survivor");
                (ix.fingerprint(id), ix.label_set(id))
            })
            .collect();
        assert_eq!(
            survivors,
            vec![
                (Fingerprint::from_raw(2), mid),
                (Fingerprint::from_raw(3), new)
            ]
        );
        let regex_cache = RegexCache::new(16);
        assert_eq!(
            ix.select("up", &[matcher("job", MatchOp::Eq, "api")], &regex_cache)
                .map(|ids| ids.len()),
            Ok(1)
        );
    }

    /// **T3.**
    #[test]
    fn a_touch_never_lowers_the_hour_and_a_row_behind_the_edge_is_ignored() {
        let h = 400_000 * H;
        let mut ix = LabelIndex::default();
        let ls = labels(&[("job", "api")]);
        ix.apply(Fingerprint::from_raw(1), "up", h, h - H, || ls.clone());
        ix.apply(Fingerprint::from_raw(1), "up", h - H, h - H, || ls.clone());
        assert_eq!(ix.len(), 1, "one series, touched once");
        let id = ix.find(Fingerprint::from_raw(1)).expect("resident");
        assert_eq!(i64::from(ix.last_hour.get(id as usize)) * H, h);
        ix.apply(Fingerprint::from_raw(2), "up", h - 2 * H, h - H, || ls.clone());
        assert_eq!(ix.len(), 1);
        assert_eq!(ix.find(Fingerprint::from_raw(2)), None);
    }

    /// **T10.**
    #[test]
    fn intersect_finds_an_element_at_the_galloping_probe() {
        assert_eq!(intersect(&[1, 2, 5], &[1, 2, 3, 4, 5, 6]), vec![1, 2, 5]);
        assert_eq!(intersect(&[1, 2, 3, 4, 5, 6], &[1, 2, 5]), vec![1, 2, 5]);
    }
}
