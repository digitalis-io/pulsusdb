//! `sum/avg/min/max/count/group/topk/bottomk` with `by`/`without`
//! grouping. `sum`/`avg` use [`KahanSum`] (Neumaier-compensated
//! summation), **accumulation order pinned to the input vector's own
//! order** — which is itself pinned to ascending-fingerprint order all the
//! way back to the fetch layer's `ORDER BY fingerprint, unix_milli`
//! (docs/schemas.md §2.3) and never reshuffled by a `HashMap` in between
//! (every grouping step here accumulates into a per-group [`KahanSum`] in
//! the same relative order the input vector arrives in, regardless of the
//! `HashMap`'s own bucket iteration order). Exact last-ULP parity with
//! Prometheus's own series-storage accumulation order is a #33
//! differential concern (architect plan Open Q1), not assumed here.

use std::cmp::Ordering;
use std::collections::HashMap;

use crate::error::PromqlError;
use crate::math::KahanSum;
use crate::plan::{AggOp, Grouping};
use crate::value::{InstantSample, Labels};

fn group_key(labels: &Labels, grouping: Option<&Grouping>) -> Labels {
    match grouping {
        None => Labels::default(),
        Some(g) if g.without => labels.without(&g.labels),
        Some(g) => labels.only(&g.labels),
    }
}

/// `sum/avg/min/max/count/group/topk/bottomk`. `param` is `topk`/
/// `bottomk`'s `k` (already evaluated to a scalar by the caller).
pub fn aggregate(
    op: AggOp,
    vector: &[InstantSample],
    grouping: Option<&Grouping>,
    param: Option<f64>,
) -> Result<Vec<InstantSample>, PromqlError> {
    match op {
        AggOp::Topk | AggOp::Bottomk => aggregate_topk(op, vector, grouping, param),
        _ => Ok(aggregate_reduce(op, vector, grouping)),
    }
}

struct Acc {
    kahan: KahanSum,
    min: f64,
    max: f64,
    count: f64,
    t_ms: i64,
}

fn aggregate_reduce(
    op: AggOp,
    vector: &[InstantSample],
    grouping: Option<&Grouping>,
) -> Vec<InstantSample> {
    let mut groups: HashMap<Labels, Acc> = HashMap::new();
    for s in vector {
        let key = group_key(&s.labels, grouping);
        let acc = groups.entry(key).or_insert_with(|| Acc {
            kahan: KahanSum::new(),
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            count: 0.0,
            t_ms: s.t_ms,
        });
        acc.kahan.add(s.v);
        acc.min = acc.min.min(s.v);
        acc.max = acc.max.max(s.v);
        acc.count += 1.0;
    }

    let mut out: Vec<InstantSample> = groups
        .into_iter()
        .map(|(labels, acc)| {
            let v = match op {
                AggOp::Sum => acc.kahan.value(),
                AggOp::Avg => acc.kahan.value() / acc.count,
                AggOp::Min => acc.min,
                AggOp::Max => acc.max,
                AggOp::Count => acc.count,
                AggOp::Group => 1.0,
                AggOp::Topk | AggOp::Bottomk => unreachable!("handled by aggregate_topk"),
            };
            InstantSample {
                labels,
                t_ms: acc.t_ms,
                v,
            }
        })
        .collect();
    // Deterministic output order (HashMap iteration order is not stable) —
    // not a correctness requirement, but keeps callers/tests from having
    // to sort themselves.
    out.sort_by(|a, b| a.labels.cmp(&b.labels));
    out
}

fn aggregate_topk(
    op: AggOp,
    vector: &[InstantSample],
    grouping: Option<&Grouping>,
    param: Option<f64>,
) -> Result<Vec<InstantSample>, PromqlError> {
    let k = param.ok_or_else(|| PromqlError::BadMatching {
        detail: "topk/bottomk require a k parameter".to_string(),
    })?;
    if !k.is_finite() || k < 1.0 {
        return Ok(Vec::new());
    }
    let k = k as usize;

    let mut groups: HashMap<Labels, Vec<InstantSample>> = HashMap::new();
    for s in vector {
        let key = group_key(&s.labels, grouping);
        groups.entry(key).or_default().push(s.clone());
    }

    let mut group_keys: Vec<Labels> = groups.keys().cloned().collect();
    group_keys.sort();

    let mut out = Vec::new();
    for key in group_keys {
        let mut members = groups.remove(&key).expect("key came from groups.keys()");
        match op {
            AggOp::Topk => members.sort_by(|a, b| b.v.partial_cmp(&a.v).unwrap_or(Ordering::Equal)),
            AggOp::Bottomk => {
                members.sort_by(|a, b| a.v.partial_cmp(&b.v).unwrap_or(Ordering::Equal))
            }
            _ => unreachable!("only called for Topk/Bottomk"),
        }
        out.extend(members.into_iter().take(k));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(labels: &[(&str, &str)], v: f64) -> InstantSample {
        InstantSample {
            labels: Labels::new(labels.iter().map(|(k, v)| (k.to_string(), v.to_string()))),
            t_ms: 0,
            v,
        }
    }

    fn grouping(without: bool, labels: &[&str]) -> Grouping {
        Grouping {
            without,
            labels: labels.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn sum_with_no_grouping_reduces_to_one_series() {
        let vector = vec![sample(&[("job", "a")], 1.0), sample(&[("job", "b")], 2.0)];
        let out = aggregate(AggOp::Sum, &vector, None, None).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].v, 3.0);
        assert!(out[0].labels.is_empty());
    }

    #[test]
    fn sum_by_groups_and_sums_per_group() {
        let vector = vec![
            sample(&[("job", "a"), ("inst", "1")], 1.0),
            sample(&[("job", "a"), ("inst", "2")], 2.0),
            sample(&[("job", "b"), ("inst", "1")], 5.0),
        ];
        let g = grouping(false, &["job"]);
        let out = aggregate(AggOp::Sum, &vector, Some(&g), None).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].labels.get("job"), Some("a"));
        assert_eq!(out[0].v, 3.0);
        assert_eq!(out[1].labels.get("job"), Some("b"));
        assert_eq!(out[1].v, 5.0);
    }

    #[test]
    fn sum_uses_kahan_summation_on_an_ordering_a_naive_sum_gets_wrong() {
        let vector = vec![
            sample(&[("s", "1")], 1e100),
            sample(&[("s", "2")], 1.0),
            sample(&[("s", "3")], -1e100),
        ];
        let out = aggregate(AggOp::Sum, &vector, None, None).unwrap();
        assert_eq!(out[0].v, 1.0);
    }

    #[test]
    fn without_excludes_the_named_labels_from_the_group_key() {
        let vector = vec![
            sample(&[("job", "a"), ("inst", "1")], 1.0),
            sample(&[("job", "a"), ("inst", "2")], 2.0),
        ];
        let g = grouping(true, &["inst"]);
        let out = aggregate(AggOp::Sum, &vector, Some(&g), None).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].v, 3.0);
    }

    #[test]
    fn avg_divides_by_group_member_count() {
        let vector = vec![sample(&[("job", "a")], 2.0), sample(&[("job", "a")], 4.0)];
        let out = aggregate(AggOp::Avg, &vector, None, None).unwrap();
        assert_eq!(out[0].v, 3.0);
    }

    #[test]
    fn min_and_max() {
        let vector = vec![
            sample(&[("job", "a")], 5.0),
            sample(&[("job", "a")], 1.0),
            sample(&[("job", "a")], 3.0),
        ];
        assert_eq!(
            aggregate(AggOp::Min, &vector, None, None).unwrap()[0].v,
            1.0
        );
        assert_eq!(
            aggregate(AggOp::Max, &vector, None, None).unwrap()[0].v,
            5.0
        );
    }

    #[test]
    fn count_counts_group_members() {
        let vector = vec![
            sample(&[("job", "a")], 1.0),
            sample(&[("job", "a")], 1.0),
            sample(&[("job", "b")], 1.0),
        ];
        let g = grouping(false, &["job"]);
        let out = aggregate(AggOp::Count, &vector, Some(&g), None).unwrap();
        assert_eq!(out[0].v, 2.0);
        assert_eq!(out[1].v, 1.0);
    }

    #[test]
    fn group_always_yields_one() {
        let vector = vec![sample(&[("job", "a")], 42.0)];
        let out = aggregate(AggOp::Group, &vector, None, None).unwrap();
        assert_eq!(out[0].v, 1.0);
    }

    #[test]
    fn topk_keeps_the_largest_k_values_per_group() {
        let vector = vec![
            sample(&[("s", "1")], 5.0),
            sample(&[("s", "2")], 1.0),
            sample(&[("s", "3")], 3.0),
        ];
        let out = aggregate(AggOp::Topk, &vector, None, Some(2.0)).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].v, 5.0);
        assert_eq!(out[1].v, 3.0);
    }

    #[test]
    fn bottomk_keeps_the_smallest_k_values_per_group() {
        let vector = vec![
            sample(&[("s", "1")], 5.0),
            sample(&[("s", "2")], 1.0),
            sample(&[("s", "3")], 3.0),
        ];
        let out = aggregate(AggOp::Bottomk, &vector, None, Some(2.0)).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].v, 1.0);
        assert_eq!(out[1].v, 3.0);
    }

    #[test]
    fn topk_retains_full_original_labels_not_the_grouping_key() {
        let vector = vec![sample(&[("job", "a"), ("inst", "1")], 5.0)];
        let out = aggregate(AggOp::Topk, &vector, None, Some(1.0)).unwrap();
        assert_eq!(out[0].labels.get("inst"), Some("1"));
    }

    #[test]
    fn topk_without_a_k_parameter_is_bad_matching() {
        let vector = vec![sample(&[("s", "1")], 1.0)];
        let err = aggregate(AggOp::Topk, &vector, None, None).unwrap_err();
        assert!(matches!(err, PromqlError::BadMatching { .. }));
    }

    #[test]
    fn topk_by_groups_independently() {
        let vector = vec![
            sample(&[("job", "a")], 1.0),
            sample(&[("job", "a")], 2.0),
            sample(&[("job", "b")], 9.0),
        ];
        let g = grouping(false, &["job"]);
        let out = aggregate(AggOp::Topk, &vector, Some(&g), Some(1.0)).unwrap();
        assert_eq!(out.len(), 2);
        let vals: Vec<f64> = out.iter().map(|s| s.v).collect();
        assert!(vals.contains(&2.0));
        assert!(vals.contains(&9.0));
    }

    #[test]
    fn an_empty_vector_aggregates_to_an_empty_result() {
        assert!(aggregate(AggOp::Sum, &[], None, None).unwrap().is_empty());
    }
}
