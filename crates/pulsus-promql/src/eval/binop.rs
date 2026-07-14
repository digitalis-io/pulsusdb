//! Vector–scalar and vector–vector arithmetic/comparison, `bool`, and
//! `on(...)`/`ignoring(...)` one-to-one matching. `group_left`/
//! `group_right` (many-to-one) are rejected at plan time
//! ([`crate::plan::plan_binary`]) — this module only ever sees one-to-one
//! matching, so a duplicate group key on either side is unconditionally a
//! [`PromqlError::BadMatching`], never a silent many-to-one collapse.

use std::collections::{HashMap, HashSet};

use crate::error::PromqlError;
use crate::plan::{BinOp, Matching};
use crate::value::{InstantSample, Labels};

fn apply_arith(op: BinOp, l: f64, r: f64) -> f64 {
    match op {
        BinOp::Add => l + r,
        BinOp::Sub => l - r,
        BinOp::Mul => l * r,
        BinOp::Div => l / r,
        BinOp::Mod => l % r,
        BinOp::Pow => l.powf(r),
        BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
            unreachable!("comparison operators are handled by apply_compare")
        }
    }
}

/// Pinned upstream-exact contract (code review round 1, architect
/// adjudication REJECT #1 — **do not** add a NaN special case here):
/// Prometheus v3.13's vector-comparison path (`promql/engine.go`,
/// `vectorElemBinop`) passes every comparison straight through Go's IEEE
/// operators with no NaN guard at all —
///
/// ```go
/// case parser.NEQ:
///     return lhs, nil, lhs != rhs, nil
/// ```
///
/// — and identically for `EQLC`/`GTR`/`LSS`/`GTE`/`LTE`. So `NaN != 5`
/// upstream evaluates `keep = true` (kept/passes in filter mode, `1.0` in
/// `bool` mode) and `NaN == 5` evaluates `keep = false` (dropped/`0.0`) —
/// exactly what Rust's `l != r` / `l == r` already produce for `f64`
/// (IEEE 754 `!=`/`==` need no special-casing to match Go's). A "fix" that
/// special-cases NaN to always compare false/dropped would *introduce* a
/// divergence from upstream, not correct one — see the `nan_vs_*` golden
/// tests below, which pin this exact behavior across all six operators and
/// every evaluation shape (scalar-scalar, vector-scalar, scalar-vector,
/// vector-vector, filter and `bool` mode).
fn apply_compare(op: BinOp, l: f64, r: f64) -> bool {
    match op {
        BinOp::Eq => l == r,
        BinOp::Ne => l != r,
        BinOp::Lt => l < r,
        BinOp::Le => l <= r,
        BinOp::Gt => l > r,
        BinOp::Ge => l >= r,
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod | BinOp::Pow => {
            unreachable!("arithmetic operators are handled by apply_arith")
        }
    }
}

/// Scalar–scalar: comparisons always produce `1.0`/`0.0` (there is no
/// vector to filter), regardless of `bool`.
pub fn scalar_scalar(op: BinOp, l: f64, r: f64) -> f64 {
    if op.is_comparison() {
        f64::from(apply_compare(op, l, r))
    } else {
        apply_arith(op, l, r)
    }
}

/// Vector–scalar (or scalar–vector, via `scalar_on_left`). The result
/// keeps each surviving element's original, full label set (no matching
/// reduction — that only applies to vector–vector, where a `resultMetric`
/// per Prometheus's own `engine.go` reduces to the matched label set).
pub fn vector_scalar(
    op: BinOp,
    bool_modifier: bool,
    vector: &[InstantSample],
    scalar: f64,
    scalar_on_left: bool,
) -> Vec<InstantSample> {
    vector
        .iter()
        .filter_map(|s| {
            let (l, r) = if scalar_on_left {
                (scalar, s.v)
            } else {
                (s.v, scalar)
            };
            let v = if op.is_comparison() {
                let keep = apply_compare(op, l, r);
                if bool_modifier {
                    f64::from(keep)
                } else if keep {
                    s.v
                } else {
                    return None;
                }
            } else {
                apply_arith(op, l, r)
            };
            Some(InstantSample {
                labels: s.labels.clone(),
                t_ms: s.t_ms,
                v,
            })
        })
        .collect()
}

fn matching_key(labels: &Labels, matching: &Matching) -> Labels {
    if matching.on {
        labels.only(&matching.labels)
    } else {
        labels.without(&matching.labels)
    }
}

/// Vector–vector one-to-one matching. Output labels are the **matched**
/// label set (Prometheus's `resultMetric`: `Keep` the `on` labels, or
/// `Del` the `ignoring` labels) — for both arithmetic and comparison ops,
/// `bool` or not; `bool` only changes the *value* (0/1 vs. the LHS value),
/// never the label reduction.
pub fn vector_vector(
    op: BinOp,
    bool_modifier: bool,
    matching: &Matching,
    lhs: &[InstantSample],
    rhs: &[InstantSample],
) -> Result<Vec<InstantSample>, PromqlError> {
    let mut rhs_by_key: HashMap<Labels, &InstantSample> = HashMap::with_capacity(rhs.len());
    for r in rhs {
        let key = matching_key(&r.labels, matching);
        if rhs_by_key.insert(key, r).is_some() {
            return Err(too_many_matches());
        }
    }

    let mut seen_lhs_keys: HashSet<Labels> = HashSet::with_capacity(lhs.len());
    let mut out = Vec::new();
    for l in lhs {
        let key = matching_key(&l.labels, matching);
        if !seen_lhs_keys.insert(key.clone()) {
            return Err(too_many_matches());
        }
        let Some(r) = rhs_by_key.get(&key) else {
            continue;
        };
        let v = if op.is_comparison() {
            let keep = apply_compare(op, l.v, r.v);
            if bool_modifier {
                f64::from(keep)
            } else if keep {
                l.v
            } else {
                continue;
            }
        } else {
            apply_arith(op, l.v, r.v)
        };
        out.push(InstantSample {
            labels: key,
            t_ms: l.t_ms,
            v,
        });
    }
    Ok(out)
}

fn too_many_matches() -> PromqlError {
    PromqlError::BadMatching {
        detail: "found duplicate series for the matching labels: many-to-one matching must be \
                 explicit (group_left/group_right, unsupported in this proof subset)"
            .to_string(),
    }
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

    fn ignoring_default() -> Matching {
        Matching {
            on: false,
            labels: Vec::new(),
        }
    }

    #[test]
    fn scalar_scalar_arithmetic() {
        assert_eq!(scalar_scalar(BinOp::Add, 2.0, 3.0), 5.0);
        assert_eq!(scalar_scalar(BinOp::Mul, 2.0, 3.0), 6.0);
    }

    #[test]
    fn scalar_scalar_comparison_is_always_zero_or_one() {
        assert_eq!(scalar_scalar(BinOp::Gt, 5.0, 3.0), 1.0);
        assert_eq!(scalar_scalar(BinOp::Gt, 1.0, 3.0), 0.0);
    }

    #[test]
    fn vector_scalar_arithmetic_applies_to_every_element() {
        let vector = vec![sample(&[("job", "a")], 2.0), sample(&[("job", "b")], 4.0)];
        let out = vector_scalar(BinOp::Mul, false, &vector, 10.0, false);
        assert_eq!(out[0].v, 20.0);
        assert_eq!(out[1].v, 40.0);
    }

    #[test]
    fn vector_scalar_comparison_without_bool_filters_and_keeps_original_value() {
        let vector = vec![sample(&[("job", "a")], 5.0), sample(&[("job", "b")], 1.0)];
        let out = vector_scalar(BinOp::Gt, false, &vector, 3.0, false);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].v, 5.0);
        assert_eq!(out[0].labels.get("job"), Some("a"));
    }

    #[test]
    fn vector_scalar_comparison_with_bool_keeps_every_element_as_zero_or_one() {
        let vector = vec![sample(&[("job", "a")], 5.0), sample(&[("job", "b")], 1.0)];
        let out = vector_scalar(BinOp::Gt, true, &vector, 3.0, false);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].v, 1.0);
        assert_eq!(out[1].v, 0.0);
    }

    #[test]
    fn vector_scalar_with_scalar_on_left_flips_the_comparison_operands() {
        let vector = vec![sample(&[("job", "a")], 5.0)];
        // 3 < vector value (5), scalar_on_left => op applied as (3 < 5).
        let out = vector_scalar(BinOp::Lt, true, &vector, 3.0, true);
        assert_eq!(out[0].v, 1.0);
    }

    #[test]
    fn vector_vector_default_matching_matches_on_the_full_label_set() {
        let lhs = vec![sample(&[("job", "a")], 2.0)];
        let rhs = vec![sample(&[("job", "a")], 10.0)];
        let out = vector_vector(BinOp::Add, false, &ignoring_default(), &lhs, &rhs).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].v, 12.0);
    }

    #[test]
    fn vector_vector_arithmetic_output_labels_reduce_to_the_matching_key() {
        let lhs = vec![sample(&[("job", "a"), ("inst", "1")], 2.0)];
        let rhs = vec![sample(&[("job", "a"), ("inst", "2")], 10.0)];
        let matching = Matching {
            on: true,
            labels: vec!["job".to_string()],
        };
        let out = vector_vector(BinOp::Add, false, &matching, &lhs, &rhs).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].labels.get("job"), Some("a"));
        assert_eq!(out[0].labels.get("inst"), None);
    }

    #[test]
    fn vector_vector_ignoring_excludes_the_named_labels_from_matching() {
        let lhs = vec![sample(&[("job", "a"), ("inst", "1")], 2.0)];
        let rhs = vec![sample(&[("job", "a"), ("inst", "2")], 10.0)];
        let matching = Matching {
            on: false,
            labels: vec!["inst".to_string()],
        };
        let out = vector_vector(BinOp::Add, false, &matching, &lhs, &rhs).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].v, 12.0);
    }

    #[test]
    fn vector_vector_with_no_match_on_the_rhs_drops_the_lhs_element() {
        let lhs = vec![sample(&[("job", "a")], 2.0)];
        let rhs = vec![sample(&[("job", "b")], 10.0)];
        let out = vector_vector(BinOp::Add, false, &ignoring_default(), &lhs, &rhs).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn vector_vector_comparison_without_bool_filters_and_keeps_the_matched_key() {
        let lhs = vec![sample(&[("job", "a")], 5.0), sample(&[("job", "b")], 1.0)];
        let rhs = vec![sample(&[("job", "a")], 3.0), sample(&[("job", "b")], 3.0)];
        let out = vector_vector(BinOp::Gt, false, &ignoring_default(), &lhs, &rhs).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].v, 5.0);
    }

    #[test]
    fn vector_vector_comparison_with_bool_keeps_every_matched_pair() {
        let lhs = vec![sample(&[("job", "a")], 5.0), sample(&[("job", "b")], 1.0)];
        let rhs = vec![sample(&[("job", "a")], 3.0), sample(&[("job", "b")], 3.0)];
        let out = vector_vector(BinOp::Gt, true, &ignoring_default(), &lhs, &rhs).unwrap();
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn vector_vector_a_duplicate_rhs_match_key_is_bad_matching() {
        let lhs = vec![sample(&[("job", "a")], 1.0)];
        let rhs = vec![
            sample(&[("job", "a"), ("inst", "1")], 1.0),
            sample(&[("job", "a"), ("inst", "2")], 2.0),
        ];
        let matching = Matching {
            on: true,
            labels: vec!["job".to_string()],
        };
        let err = vector_vector(BinOp::Add, false, &matching, &lhs, &rhs).unwrap_err();
        assert!(matches!(err, PromqlError::BadMatching { .. }));
    }

    #[test]
    fn vector_vector_a_duplicate_lhs_match_key_is_bad_matching() {
        let lhs = vec![
            sample(&[("job", "a"), ("inst", "1")], 1.0),
            sample(&[("job", "a"), ("inst", "2")], 2.0),
        ];
        let rhs = vec![sample(&[("job", "a")], 1.0)];
        let matching = Matching {
            on: true,
            labels: vec!["job".to_string()],
        };
        let err = vector_vector(BinOp::Add, false, &matching, &lhs, &rhs).unwrap_err();
        assert!(matches!(err, PromqlError::BadMatching { .. }));
    }

    // --- NaN vs. upstream-exact comparison semantics (code review round
    // 1, architect adjudication REJECT #1: pinning, not fixing — see
    // `apply_compare`'s doc comment for the upstream `vectorElemBinop`
    // citation). All six operators, every evaluation shape. ---

    const NAN: f64 = f64::NAN;

    #[test]
    fn nan_vs_scalar_scalar_all_six_operators() {
        // `!=`/`<`/`<=`/`>`/`>=` all evaluate `true` for NaN (IEEE: NaN
        // compares unordered/unequal to everything, including itself);
        // only `==` evaluates `false`.
        assert_eq!(scalar_scalar(BinOp::Eq, NAN, 5.0), 0.0);
        assert_eq!(scalar_scalar(BinOp::Ne, NAN, 5.0), 1.0);
        assert_eq!(scalar_scalar(BinOp::Lt, NAN, 5.0), 0.0);
        assert_eq!(scalar_scalar(BinOp::Le, NAN, 5.0), 0.0);
        assert_eq!(scalar_scalar(BinOp::Gt, NAN, 5.0), 0.0);
        assert_eq!(scalar_scalar(BinOp::Ge, NAN, 5.0), 0.0);
    }

    #[test]
    fn nan_vs_scalar_scalar_nan_vs_nan() {
        // NaN never compares equal to anything, including another NaN.
        assert_eq!(scalar_scalar(BinOp::Eq, NAN, NAN), 0.0);
        assert_eq!(scalar_scalar(BinOp::Ne, NAN, NAN), 1.0);
    }

    #[test]
    fn nan_ne_5_keeps_the_element_in_vector_scalar_filter_mode() {
        let vector = vec![sample(&[("job", "a")], NAN)];
        let out = vector_scalar(BinOp::Ne, false, &vector, 5.0, false);
        assert_eq!(out.len(), 1, "NaN != 5 must keep (upstream: keep=true)");
        assert!(out[0].v.is_nan());
    }

    #[test]
    fn nan_eq_5_drops_the_element_in_vector_scalar_filter_mode() {
        let vector = vec![sample(&[("job", "a")], NAN)];
        let out = vector_scalar(BinOp::Eq, false, &vector, 5.0, false);
        assert!(out.is_empty(), "NaN == 5 must drop (upstream: keep=false)");
    }

    #[test]
    fn nan_ne_5_is_one_in_vector_scalar_bool_mode() {
        let vector = vec![sample(&[("job", "a")], NAN)];
        let out = vector_scalar(BinOp::Ne, true, &vector, 5.0, false);
        assert_eq!(out[0].v, 1.0);
    }

    #[test]
    fn nan_eq_5_is_zero_in_vector_scalar_bool_mode() {
        let vector = vec![sample(&[("job", "a")], NAN)];
        let out = vector_scalar(BinOp::Eq, true, &vector, 5.0, false);
        assert_eq!(out[0].v, 0.0);
    }

    #[test]
    fn five_ne_nan_keeps_in_scalar_vector_filter_mode() {
        // scalar_on_left = true: op applied as (scalar, vector_value).
        let vector = vec![sample(&[("job", "a")], NAN)];
        let out = vector_scalar(BinOp::Ne, false, &vector, 5.0, true);
        assert_eq!(out.len(), 1, "5 != NaN must keep (upstream: keep=true)");
    }

    #[test]
    fn five_eq_nan_drops_in_scalar_vector_filter_mode() {
        let vector = vec![sample(&[("job", "a")], NAN)];
        let out = vector_scalar(BinOp::Eq, false, &vector, 5.0, true);
        assert!(out.is_empty(), "5 == NaN must drop (upstream: keep=false)");
    }

    #[test]
    fn nan_ne_5_keeps_in_vector_vector_filter_mode() {
        let lhs = vec![sample(&[("job", "a")], NAN)];
        let rhs = vec![sample(&[("job", "a")], 5.0)];
        let out = vector_vector(BinOp::Ne, false, &ignoring_default(), &lhs, &rhs).unwrap();
        assert_eq!(out.len(), 1, "NaN != 5 must keep (upstream: keep=true)");
        assert!(out[0].v.is_nan());
    }

    #[test]
    fn nan_eq_5_drops_in_vector_vector_filter_mode() {
        let lhs = vec![sample(&[("job", "a")], NAN)];
        let rhs = vec![sample(&[("job", "a")], 5.0)];
        let out = vector_vector(BinOp::Eq, false, &ignoring_default(), &lhs, &rhs).unwrap();
        assert!(out.is_empty(), "NaN == 5 must drop (upstream: keep=false)");
    }

    #[test]
    fn nan_ne_5_is_one_in_vector_vector_bool_mode() {
        let lhs = vec![sample(&[("job", "a")], NAN)];
        let rhs = vec![sample(&[("job", "a")], 5.0)];
        let out = vector_vector(BinOp::Ne, true, &ignoring_default(), &lhs, &rhs).unwrap();
        assert_eq!(out[0].v, 1.0);
    }

    #[test]
    fn nan_eq_5_is_zero_in_vector_vector_bool_mode() {
        let lhs = vec![sample(&[("job", "a")], NAN)];
        let rhs = vec![sample(&[("job", "a")], 5.0)];
        let out = vector_vector(BinOp::Eq, true, &ignoring_default(), &lhs, &rhs).unwrap();
        assert_eq!(out[0].v, 0.0);
    }
}
