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
            // Issue #37: a filter-mode comparison (no `bool`) passes the
            // matched element's value — and `__name__` — through verbatim
            // (captured: `up > 0` keeps `__name__`); arithmetic and
            // `bool`-mode comparisons both **compute** a new value, so
            // both drop `__name__` (captured: `up * 2`, `up > bool 0`).
            let (v, metric_name) = if op.is_comparison() {
                let keep = apply_compare(op, l, r);
                if bool_modifier {
                    (f64::from(keep), None)
                } else if keep {
                    (s.v, s.metric_name.clone())
                } else {
                    return None;
                }
            } else {
                (apply_arith(op, l, r), None)
            };
            Some(InstantSample {
                labels: s.labels.clone(),
                metric_name,
                t_ms: s.t_ms,
                v,
            })
        })
        .collect()
}

/// The vector-vector **pairing** key: the ordinary-label reduction
/// (`Labels` — never contains `__name__`, per that type's own invariant)
/// plus an optional name component.
///
/// **Issue #37 code-review round 3 [medium] fix:** `on(__name__, ...)`
/// must pair series **only when their actual metric names are equal** —
/// upstream's `signatureFunc` (`promql/engine.go`): for `on=true`,
/// `__name__` participates in the pairing hash *iff* it is explicitly
/// among the `on(...)` names, looking up the real `__name__` label value.
/// `Labels` structurally cannot carry that value (see its own doc, and
/// `name_kept`'s), so the name component is threaded alongside it here
/// rather than smuggled into `Labels` itself — every other `Labels`-keyed
/// `HashMap`/grouping computation in this crate (`aggregation.rs`, and
/// `matching_key`'s own ordinary-label reduction below) relies on
/// `Labels` never containing `__name__`; violating that to encode a name
/// match here would risk breaking those invariants by proximity.
/// `ignoring(...)` mode's name component is *always* `None`, regardless
/// of whether `__name__` is explicitly `ignoring`-ed: upstream's
/// `ignoring`/`without` signature path always excludes `__name__` from
/// the hash, listed or not (`hashWithoutLabels` drops `MetricName`
/// unconditionally, then additionally drops the `ignoring` list) — so two
/// differently-named series with otherwise-matching ordinary labels pair
/// up under `ignoring(...)` in every case, `ignoring(__name__)` included.
/// This was already this code's *accidental* behavior before this fix
/// (`Labels` never carried `__name__` to strip in the first place) — now
/// pinned as intentional, with tests covering both directions.
type MatchKey = (Labels, Option<String>);

/// Builds one side's [`MatchKey`] for [`vector_vector`]. See that type's
/// own doc for the `__name__`-participation rule.
fn matching_key(labels: &Labels, metric_name: &Option<String>, matching: &Matching) -> MatchKey {
    let ordinary = if matching.on {
        labels.only(&matching.labels)
    } else {
        labels.without(&matching.labels)
    };
    let name_participates = matching.on && matching.labels.iter().any(|l| l == "__name__");
    let name_component = name_participates.then(|| metric_name.clone().unwrap_or_default());
    (ordinary, name_component)
}

/// Issue #37 code-review finding 3 (CONFIRM): whether `__name__` survives
/// the **same** label reduction `matching_key` applies to the ordinary
/// labels — upstream v3.13's `resultMetric` (`promql/engine.go`) applies
/// `enh.resultMetric`'s `Keep`/`Del` to the *whole* metric (name
/// included), not just the ordinary labels: for `CardOneToOne`, `on(...)`
/// -> `lb.Keep(matching.MatchingLabels...)` (drops everything **not**
/// named, `__name__` included, unless `__name__` itself is explicitly
/// `on`-listed — an edge case, but the rule is general); `ignoring(...)`
/// -> `lb.Del(matching.MatchingLabels...)` (drops only the named labels,
/// so `__name__` survives unless it is itself explicitly `ignoring`-ed).
/// `group_left`/`group_right` (`CardManyToOne`/`CardOneToMany`) are
/// `PromqlError::Unsupported` (`plan.rs`), so `CardOneToOne` is the only
/// reachable case — this fn is total for it. Only meaningful for
/// filter-mode comparisons (the sole case that ever keeps a name at all —
/// arithmetic and `bool`-mode always drop, per `vector_vector`'s own
/// callers below).
fn name_kept(matching: &Matching) -> bool {
    let name_listed = matching.labels.iter().any(|l| l == "__name__");
    if matching.on {
        name_listed
    } else {
        !name_listed
    }
}

/// Vector–vector one-to-one matching. Output labels are the **matched**
/// label set (Prometheus's `resultMetric`: `Keep` the `on` labels, or
/// `Del` the `ignoring` labels) — for both arithmetic and comparison ops,
/// `bool` or not; `bool` only changes the *value* (0/1 vs. the LHS value),
/// never the label reduction. `__name__`'s own keep/drop verdict follows
/// the identical reduction — see [`name_kept`].
pub fn vector_vector(
    op: BinOp,
    bool_modifier: bool,
    matching: &Matching,
    lhs: &[InstantSample],
    rhs: &[InstantSample],
) -> Result<Vec<InstantSample>, PromqlError> {
    let mut rhs_by_key: HashMap<MatchKey, &InstantSample> = HashMap::with_capacity(rhs.len());
    for r in rhs {
        let key = matching_key(&r.labels, &r.metric_name, matching);
        if rhs_by_key.insert(key, r).is_some() {
            return Err(too_many_matches());
        }
    }

    let mut seen_lhs_keys: HashSet<MatchKey> = HashSet::with_capacity(lhs.len());
    let mut out = Vec::new();
    for l in lhs {
        let key = matching_key(&l.labels, &l.metric_name, matching);
        if !seen_lhs_keys.insert(key.clone()) {
            return Err(too_many_matches());
        }
        let Some(r) = rhs_by_key.get(&key) else {
            continue;
        };
        // Issue #37: a filter-mode comparison (no `bool`) passes the LHS
        // element's value through, and keeps `__name__` **iff it survives
        // the same `on`/`ignoring` reduction the ordinary labels go
        // through** — see [`name_kept`]'s doc (code-review finding 3,
        // upstream `engine.go` citation there; captured: `up == up`,
        // `up == ignoring(instance) up` keep, `up == on(job) up` drops).
        // Arithmetic and `bool`-mode comparisons both unconditionally drop
        // it (captured: `up + on(job) up`, `up > bool up`) — Prometheus's
        // `shouldDropMetricName` is `true` for every op except a
        // non-`bool` comparison.
        let (v, metric_name) = if op.is_comparison() {
            let keep = apply_compare(op, l.v, r.v);
            if bool_modifier {
                (f64::from(keep), None)
            } else if keep {
                let name = if name_kept(matching) {
                    l.metric_name.clone()
                } else {
                    None
                };
                (l.v, name)
            } else {
                continue;
            }
        } else {
            (apply_arith(op, l.v, r.v), None)
        };
        out.push(InstantSample {
            labels: key.0,
            metric_name,
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
            metric_name: Some("test_metric".to_string()),
            t_ms: 0,
            v,
        }
    }

    /// Like [`sample`], but with an explicit `metric_name` — needed for
    /// the `on(__name__)`/`ignoring(__name__)` tests below, which must
    /// distinguish two *differently*-named series.
    fn named_sample(name: &str, labels: &[(&str, &str)], v: f64) -> InstantSample {
        InstantSample {
            labels: Labels::new(labels.iter().map(|(k, v)| (k.to_string(), v.to_string()))),
            metric_name: Some(name.to_string()),
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

    // --- issue #37: `__name__` keep/drop rule ---

    #[test]
    fn vector_scalar_arithmetic_drops_metric_name() {
        let vector = vec![sample(&[("job", "a")], 2.0)];
        let out = vector_scalar(BinOp::Mul, false, &vector, 10.0, false);
        assert_eq!(out[0].metric_name, None);
    }

    #[test]
    fn vector_scalar_filter_comparison_keeps_metric_name() {
        let vector = vec![sample(&[("job", "a")], 5.0)];
        let out = vector_scalar(BinOp::Gt, false, &vector, 3.0, false);
        assert_eq!(out[0].metric_name.as_deref(), Some("test_metric"));
    }

    #[test]
    fn vector_scalar_bool_comparison_drops_metric_name() {
        let vector = vec![sample(&[("job", "a")], 5.0)];
        let out = vector_scalar(BinOp::Gt, true, &vector, 3.0, false);
        assert_eq!(out[0].metric_name, None);
    }

    #[test]
    fn vector_vector_arithmetic_drops_metric_name() {
        let lhs = vec![sample(&[("job", "a")], 2.0)];
        let rhs = vec![sample(&[("job", "a")], 10.0)];
        let out = vector_vector(BinOp::Add, false, &ignoring_default(), &lhs, &rhs).unwrap();
        assert_eq!(out[0].metric_name, None);
    }

    #[test]
    fn vector_vector_filter_comparison_keeps_the_lhs_metric_name() {
        let lhs = vec![sample(&[("job", "a")], 5.0)];
        let rhs = vec![sample(&[("job", "a")], 3.0)];
        let out = vector_vector(BinOp::Gt, false, &ignoring_default(), &lhs, &rhs).unwrap();
        assert_eq!(out[0].metric_name.as_deref(), Some("test_metric"));
    }

    #[test]
    fn vector_vector_bool_comparison_drops_metric_name() {
        let lhs = vec![sample(&[("job", "a")], 5.0)];
        let rhs = vec![sample(&[("job", "a")], 3.0)];
        let out = vector_vector(BinOp::Gt, true, &ignoring_default(), &lhs, &rhs).unwrap();
        assert_eq!(out[0].metric_name, None);
    }

    // --- issue #37 code-review finding 3: `name_kept` (the exact
    // upstream `on`/`ignoring` `__name__`-reduction rule), both
    // directions — captured/pinned against real Prometheus v3.13 as
    // `query.name_comparison_on_drops_get.json` /
    // `query.name_comparison_plain_keeps_get.json` (see PROVENANCE.md).

    #[test]
    fn name_kept_is_true_for_ignoring_with_an_empty_or_unrelated_list() {
        assert!(name_kept(&ignoring_default()));
        assert!(name_kept(&Matching {
            on: false,
            labels: vec!["instance".to_string()],
        }));
    }

    #[test]
    fn name_kept_is_false_for_ignoring_that_explicitly_names_dunder_name() {
        assert!(!name_kept(&Matching {
            on: false,
            labels: vec!["__name__".to_string()],
        }));
    }

    #[test]
    fn name_kept_is_false_for_on_that_does_not_list_dunder_name() {
        assert!(!name_kept(&Matching {
            on: true,
            labels: vec!["job".to_string()],
        }));
    }

    #[test]
    fn name_kept_is_true_for_on_that_explicitly_lists_dunder_name() {
        assert!(name_kept(&Matching {
            on: true,
            labels: vec!["__name__".to_string()],
        }));
    }

    /// `up == on(job) up` drops `__name__` (`Keep(job)` — `on(...)`
    /// retains only the named labels, `__name__` not among them).
    #[test]
    fn vector_vector_filter_comparison_with_on_drops_metric_name() {
        let lhs = vec![sample(&[("job", "a"), ("instance", "1")], 5.0)];
        let rhs = vec![sample(&[("job", "a"), ("instance", "2")], 5.0)];
        let matching = Matching {
            on: true,
            labels: vec!["job".to_string()],
        };
        let out = vector_vector(BinOp::Eq, false, &matching, &lhs, &rhs).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_name, None);
    }

    /// `up == ignoring(instance) up` keeps `__name__` (`Del(instance)` —
    /// `ignoring(...)` drops only the named label, `__name__` survives).
    #[test]
    fn vector_vector_filter_comparison_with_ignoring_keeps_metric_name() {
        let lhs = vec![sample(&[("job", "a"), ("instance", "1")], 5.0)];
        let rhs = vec![sample(&[("job", "a"), ("instance", "2")], 5.0)];
        let matching = Matching {
            on: false,
            labels: vec!["instance".to_string()],
        };
        let out = vector_vector(BinOp::Eq, false, &matching, &lhs, &rhs).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_name.as_deref(), Some("test_metric"));
    }

    // --- issue #37 code-review round 3 [medium]: `on(__name__)` must
    // pair series only when their actual metric names are equal (not an
    // empty-key "everything pairs" bug); `ignoring(__name__)` always
    // excludes the name from pairing, so differently-named series with
    // matching ordinary labels still pair — see `MatchKey`'s own doc for
    // the upstream `signatureFunc` citation.

    #[test]
    fn vector_vector_on_dunder_name_between_different_names_does_not_match() {
        let lhs = vec![named_sample("foo", &[("job", "a")], 1.0)];
        let rhs = vec![named_sample("bar", &[("job", "a")], 2.0)];
        let matching = Matching {
            on: true,
            labels: vec!["__name__".to_string()],
        };
        let out = vector_vector(BinOp::Add, false, &matching, &lhs, &rhs).unwrap();
        assert!(
            out.is_empty(),
            "on(__name__) must not pair series with different metric names: {out:?}"
        );
    }

    #[test]
    fn vector_vector_on_dunder_name_between_the_same_name_matches() {
        let lhs = vec![named_sample("foo", &[("job", "a")], 1.0)];
        let rhs = vec![named_sample("foo", &[("job", "b")], 2.0)];
        let matching = Matching {
            on: true,
            labels: vec!["__name__".to_string()],
        };
        let out = vector_vector(BinOp::Add, false, &matching, &lhs, &rhs).unwrap();
        assert_eq!(
            out.len(),
            1,
            "on(__name__) must pair series with the same metric name regardless of other labels"
        );
        assert_eq!(out[0].v, 3.0);
    }

    /// `on(__name__)` filter-mode comparison between same-named series
    /// keeps `__name__` (it is explicitly `on`-listed — `name_kept`'s own
    /// rule) with the correct, real name (not an empty/default string).
    #[test]
    fn vector_vector_on_dunder_name_filter_comparison_keeps_the_real_name() {
        let lhs = vec![named_sample("foo", &[("job", "a")], 5.0)];
        let rhs = vec![named_sample("foo", &[("job", "b")], 5.0)];
        let matching = Matching {
            on: true,
            labels: vec!["__name__".to_string()],
        };
        let out = vector_vector(BinOp::Eq, false, &matching, &lhs, &rhs).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_name.as_deref(), Some("foo"));
    }

    #[test]
    fn vector_vector_ignoring_dunder_name_between_different_names_still_matches() {
        let lhs = vec![named_sample("foo", &[("job", "a")], 1.0)];
        let rhs = vec![named_sample("bar", &[("job", "a")], 2.0)];
        let matching = Matching {
            on: false,
            labels: vec!["__name__".to_string()],
        };
        let out = vector_vector(BinOp::Add, false, &matching, &lhs, &rhs).unwrap();
        assert_eq!(
            out.len(),
            1,
            "ignoring(__name__) must pair series regardless of metric name: {out:?}"
        );
        assert_eq!(out[0].v, 3.0);
    }

    /// `ignoring(__name__)` behaves identically to plain `ignoring()`
    /// (empty list) — upstream always excludes `__name__` from `ignoring`
    /// pairing, listed or not.
    #[test]
    fn vector_vector_ignoring_dunder_name_matches_plain_ignoring_behavior() {
        let lhs = vec![named_sample("foo", &[("job", "a")], 1.0)];
        let rhs = vec![named_sample("bar", &[("job", "a")], 2.0)];
        let explicit = Matching {
            on: false,
            labels: vec!["__name__".to_string()],
        };
        let plain = ignoring_default();
        let out_explicit = vector_vector(BinOp::Add, false, &explicit, &lhs, &rhs).unwrap();
        let out_plain = vector_vector(BinOp::Add, false, &plain, &lhs, &rhs).unwrap();
        assert_eq!(out_explicit.len(), out_plain.len());
        assert_eq!(out_explicit[0].v, out_plain[0].v);
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
