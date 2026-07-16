//! Vector–scalar and vector–vector arithmetic/comparison (incl. `atan2`),
//! `bool`, `on(...)`/`ignoring(...)` matching, the `and`/`or`/`unless`
//! set operators, `group_left`/`group_right` many-to-one matching with
//! include-label copying, and the experimental `fill`/`fill_left`/
//! `fill_right` modifiers (issue #70, M6-07). [`vector_vector`] is a
//! faithful port of upstream `promql/engine.go`'s `VectorBinop` +
//! `resultMetric` at the pinned v3.13.0 SHA (40af9c2), [`set_op`] of its
//! `VectorAnd`/`VectorOr`/`VectorUnless` — the vendored corpus
//! (`fill-modifier.test`) and the `proof/m6_07_operator_matrix.test`
//! proof file are the oracles.

use std::collections::{HashMap, HashSet};

use super::quote::go_quote;
use crate::error::PromqlError;
use crate::plan::{BinOp, FillValues, Group, Matching, SetOp};
use crate::value::{InstantSample, Labels};

fn apply_arith(op: BinOp, l: f64, r: f64) -> f64 {
    match op {
        BinOp::Add => l + r,
        BinOp::Sub => l - r,
        BinOp::Mul => l * r,
        BinOp::Div => l / r,
        BinOp::Mod => l % r,
        BinOp::Pow => l.powf(r),
        // Issue #70 (M6-07): arithmetic-class per upstream
        // `changesMetricSchema` — computes, never filters, drops
        // `__name__`.
        BinOp::Atan2 => l.atan2(r),
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
        BinOp::Add
        | BinOp::Sub
        | BinOp::Mul
        | BinOp::Div
        | BinOp::Mod
        | BinOp::Pow
        | BinOp::Atan2 => {
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
/// `group_left`/`group_right` (`CardManyToOne`/`CardOneToMany`) skip this
/// reduction entirely — the many side's labels pass through whole (see
/// [`vector_vector`]'s `resultMetric` port). Only meaningful for
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

/// The `and`/`or`/`unless` set operators (issue #70, M6-07) — upstream
/// `VectorAnd`/`VectorOr`/`VectorUnless` (engine.go @ 40af9c2), verbatim:
/// membership is on the [`matching_key`] signature (default `ignoring()`
/// drops `__name__` from the signature; `on(__name__)` includes it), and
/// every surviving element is copied **unchanged** — full labels,
/// `__name__`, and value. `or` = every lhs element (in order) then each
/// rhs element whose signature is absent from the lhs. No duplicate
/// checks of any kind (set ops are many-to-many by definition).
pub fn set_op(
    op: SetOp,
    matching: &Matching,
    lhs: &[InstantSample],
    rhs: &[InstantSample],
) -> Vec<InstantSample> {
    let key_of = |s: &InstantSample| matching_key(&s.labels, &s.metric_name, matching);
    match op {
        SetOp::And => {
            let rhs_sigs: HashSet<MatchKey> = rhs.iter().map(key_of).collect();
            lhs.iter()
                .filter(|l| rhs_sigs.contains(&key_of(l)))
                .cloned()
                .collect()
        }
        SetOp::Unless => {
            let rhs_sigs: HashSet<MatchKey> = rhs.iter().map(key_of).collect();
            lhs.iter()
                .filter(|l| !rhs_sigs.contains(&key_of(l)))
                .cloned()
                .collect()
        }
        SetOp::Or => {
            let lhs_sigs: HashSet<MatchKey> = lhs.iter().map(key_of).collect();
            lhs.iter()
                .cloned()
                .chain(
                    rhs.iter()
                        .filter(|r| !lhs_sigs.contains(&key_of(r)))
                        .cloned(),
                )
                .collect()
        }
    }
}

/// The per-call context [`emit_pair`] needs — bundled so the helper stays
/// under clippy's argument-count threshold.
struct PairCtx<'a> {
    op: BinOp,
    bool_modifier: bool,
    matching: &'a Matching,
    /// `Some(include labels)` for `group_left`/`group_right` (post-swap:
    /// the include labels are always copied from the "one" side = the
    /// post-swap rhs); `None` for one-to-one.
    include: Option<&'a [String]>,
    /// `true` when the operand sides were swapped (`group_right`): the
    /// *value* computation restores source order (upstream's
    /// `fl, fr = fr, fl` swap-back), while labels/duplicate identity stay
    /// post-swap (the many side).
    swapped: bool,
}

/// Match-signature bookkeeping + output accumulator for one
/// [`vector_vector`] call. Mirrors upstream `VectorBinop`'s
/// `matchedSigsPresent` (one-to-one) / `matchedSigs` (many-to-one, a set
/// of output identities per signature — hashed on the full
/// `(Labels, metric_name)` identity, plan v2 D2).
struct MatchState {
    one_to_one_matched: HashSet<MatchKey>,
    many_matched: HashMap<MatchKey, HashSet<(Labels, Option<String>)>>,
    out: Vec<InstantSample>,
}

impl MatchState {
    /// Whether `key` was consumed by the main (many-side) loop — the
    /// fill-LHS pass skips such signatures (upstream: `matchedSigsPresent
    /// [sigOrd]` / `len(matchedSigs[sigOrd]) > 0`). Registration happens
    /// in [`emit_pair`] *before* the keep filter, so a filtered-out
    /// comparison still blocks filling — upstream-exact.
    fn already_matched(&self, one_to_one: bool, key: &MatchKey) -> bool {
        if one_to_one {
            self.one_to_one_matched.contains(key)
        } else {
            self.many_matched.get(key).is_some_and(|s| !s.is_empty())
        }
    }
}

/// One matched (or filled) pair — the port of upstream `VectorBinop`'s
/// `doBinOp` + `resultMetric` (engine.go @ 40af9c2). `ls` is always the
/// post-swap lhs (the "many" side under `group_*`), `rs` the post-swap
/// rhs (the "one" side); `key` is their shared match signature.
fn emit_pair(
    ctx: &PairCtx<'_>,
    state: &mut MatchState,
    ls: &InstantSample,
    rs: &InstantSample,
    key: &MatchKey,
) -> Result<(), PromqlError> {
    // Restore source operand order for the value (upstream swap-back).
    let (vl, vr) = if ctx.swapped {
        (rs.v, ls.v)
    } else {
        (ls.v, rs.v)
    };
    let (value, keep) = if ctx.op.is_comparison() {
        let keep = apply_compare(ctx.op, vl, vr);
        if ctx.bool_modifier {
            (f64::from(keep), true)
        } else {
            // Filter mode passes the source-lhs value through.
            (vl, keep)
        }
    } else {
        (apply_arith(ctx.op, vl, vr), true)
    };

    // resultMetric: `dropMetricName || changesMetricSchema(op)` deletes
    // the name FIRST (plan v2 D2: `!is_comparison(op) || bool` — a filter
    // comparison keeps the many-side name), then the one-to-one
    // `on`/`ignoring` reduction (one-to-one ONLY — the many side's labels
    // pass through whole under `group_*`), then the include-label copy
    // from the one side (which may re-set `__name__` via the metric-name
    // channel).
    let drop_name = !ctx.op.is_comparison() || ctx.bool_modifier;
    let one_to_one = ctx.include.is_none();
    let (mut labels, mut metric_name) = if one_to_one {
        // `Keep(on)`/`Del(ignoring)` over the ls labels == the signature's
        // ordinary-label reduction; `__name__` survives iff it survives
        // the same reduction (`name_kept`).
        let name = if drop_name || !name_kept(ctx.matching) {
            None
        } else {
            ls.metric_name.clone()
        };
        (key.0.clone(), name)
    } else {
        let name = if drop_name {
            None
        } else {
            ls.metric_name.clone()
        };
        (ls.labels.clone(), name)
    };
    if let Some(include) = ctx.include {
        for ln in include {
            if ln == "__name__" {
                // Plan v2 D2: `group_left(__name__)` copies the one
                // side's name through the metric-name channel, never a
                // `Labels` entry (that type's own invariant).
                metric_name = rs.metric_name.clone().filter(|n| !n.is_empty());
            } else {
                match rs.labels.get(ln) {
                    // Upstream `rhs.Get(ln)` treats an empty value as
                    // absent (`if v != ""`).
                    Some(v) if !v.is_empty() => labels.set(ln.clone(), v.to_string()),
                    _ => labels = labels.without(std::slice::from_ref(ln)),
                }
            }
        }
    }

    // Duplicate detection — BEFORE the keep filter (upstream registers
    // the signature/identity even for a filtered-out comparison).
    if one_to_one {
        if !state.one_to_one_matched.insert(key.clone()) {
            return Err(PromqlError::BadMatching {
                detail: "multiple matches for labels: many-to-one matching must be explicit \
                         (group_left/group_right)"
                    .to_string(),
            });
        }
    } else {
        let inserted = state
            .many_matched
            .entry(key.clone())
            .or_default()
            .insert((labels.clone(), metric_name.clone()));
        if !inserted {
            return Err(PromqlError::BadMatching {
                detail: "multiple matches for labels: grouping labels must ensure unique matches"
                    .to_string(),
            });
        }
    }

    if keep {
        state.out.push(InstantSample {
            labels,
            metric_name,
            t_ms: ls.t_ms,
            v: value,
        });
    }
    Ok(())
}

/// Formats a full metric identity Go `labels.Labels.String()`-style —
/// `{__name__="foo", a="b"}`, sorted by label name, values Go-quoted
/// ([`go_quote`], the exact `strconv.Quote`/`strconv.IsPrint` port —
/// #70 review round 2) — for the duplicate-one-side error text. An EMPTY name
/// component is skipped, not rendered as `__name__=""`: upstream's
/// `MatchLabels` simply never carries an absent `__name__`, so a nameless
/// `on(__name__)` match group renders `{}`.
fn fmt_metric(labels: &Labels, metric_name: &Option<String>) -> String {
    let mut pairs: Vec<(&str, &str)> = labels
        .0
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    if let Some(name) = metric_name
        && !name.is_empty()
    {
        pairs.push(("__name__", name.as_str()));
    }
    pairs.sort();
    let body: Vec<String> = pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={}", go_quote(v)))
        .collect();
    format!("{{{}}}", body.join(", "))
}

/// The upstream duplicate-"one"-side error (verbatim message shape,
/// engine.go @ 40af9c2): two one-side samples sharing a match signature
/// is many-to-many. `swapped` selects the side wording — the one side is
/// the source rhs normally, the source lhs under `group_right`.
fn duplicate_one_side(
    key: &MatchKey,
    swapped: bool,
    sample: &InstantSample,
    duplicate: &InstantSample,
) -> PromqlError {
    let one_side = if swapped { "left" } else { "right" };
    PromqlError::BadMatching {
        detail: format!(
            "found duplicate series for the match group {} on the {} hand-side of the \
             operation: [{}, {}];many-to-many matching not allowed: matching labels must be \
             unique on one side",
            fmt_metric(&key.0, &key.1),
            one_side,
            fmt_metric(&sample.labels, &sample.metric_name),
            fmt_metric(&duplicate.labels, &duplicate.metric_name),
        ),
    }
}

/// Builds the synthetic sample a fill value stands in for (upstream:
/// `Sample{Metric: other.Metric.MatchLabels(on, matchingLabels), F: *fill}`)
/// — the *other* side's match-group identity with the fill value.
/// `__name__` participates only when explicitly `on`-listed (the
/// [`MatchKey`] name component).
fn synthetic_fill_sample(key: &MatchKey, other: &InstantSample, fill: f64) -> InstantSample {
    let name_participates = key.1.is_some();
    InstantSample {
        labels: key.0.clone(),
        metric_name: if name_participates {
            other.metric_name.clone()
        } else {
            None
        },
        t_ms: other.t_ms,
        v: fill,
    }
}

/// Vector–vector matching — one-to-one and (issue #70, M6-07)
/// `group_left`/`group_right` many-to-one, with the experimental fill
/// modifiers. A faithful port of upstream `VectorBinop` (engine.go @
/// 40af9c2):
///
/// - **one-to-one** output labels are the matched reduction (`Keep` the
///   `on` labels / `Del` the `ignoring` labels) of the lhs, `__name__`
///   per [`name_kept`]; a second lhs match for one signature is
///   `multiple matches for labels: many-to-one matching must be explicit`;
/// - **`group_right`** swaps the operand sides up front (the fill values
///   stay positional — corpus-pinned, see the swap comment in the body),
///   so the loop below always sees lhs = the "many" side, rhs = the
///   "one" side; values swap back for the arithmetic;
/// - a duplicate signature on the **one** side is the verbatim
///   `found duplicate series for the match group …` error, regardless of
///   cardinality;
/// - **many-to-one** output = the many side's full labels (name-dropped
///   per D2's `!is_comparison || bool` rule) + include labels copied from
///   the one side (deleted when absent); a duplicate output identity per
///   signature — hashed on `(Labels, metric_name)` — is
///   `multiple matches for labels: grouping labels must ensure unique
///   matches`;
/// - **fill** (plan v2 D1, asymmetric): a many-side element with no one-
///   side match uses a synthetic one side (the many element's match-group
///   identity, fill value) — output keeps the real many labels, include
///   labels delete; an unmatched one-side element (when the many-side
///   fill value is set) uses a synthetic many side (the one element's
///   match-group identity) — output is that identity, include labels copy
///   from the real one side.
pub fn vector_vector(
    op: BinOp,
    bool_modifier: bool,
    matching: &Matching,
    group: &Group,
    fill: &FillValues,
    lhs: &[InstantSample],
    rhs: &[InstantSample],
) -> Result<Vec<InstantSample>, PromqlError> {
    // Upstream short-circuit, at the upstream-equivalent position —
    // BEFORE the one-side signature map is built: when either operand is
    // empty and no fill value is set, nothing can match, and a duplicate
    // one-side signature that could never pair must NOT surface as a
    // spurious error. (Source-side lengths/fills, like upstream — the
    // condition is symmetric, so it is `group_right`-swap-invariant.)
    if (lhs.is_empty() && rhs.is_empty())
        || ((lhs.is_empty() || rhs.is_empty()) && fill.lhs.is_none() && fill.rhs.is_none())
    {
        return Ok(Vec::new());
    }

    // Operand swap: the control flow below handles one-to-one and
    // many-to-one; `group_right` (one-to-many) swaps sides. The fill
    // values are NOT swapped with the operands — upstream reads
    // `FillValues.RHS` for a missing post-swap rhs and `FillValues.LHS`
    // in the post-swap fill pass, so under `group_right` the source
    // `fill_left` value ends up filling the source-RHS (many) side —
    // `fill-modifier.test`'s `group_right fill_left(1)` case
    // (`{instance="c"} 300`) pins exactly this.
    let (many, one, include, swapped) = match group {
        Group::OneToOne => (lhs, rhs, None, false),
        Group::Left(inc) => (lhs, rhs, Some(inc.as_slice()), false),
        Group::Right(inc) => (rhs, lhs, Some(inc.as_slice()), true),
    };
    let (fill_many, fill_one) = (fill.lhs, fill.rhs);
    let one_to_one = include.is_none();

    // All samples from the one side, hashed by match signature — a
    // duplicate signature here is many-to-many, an error for every
    // cardinality.
    let mut one_by_key: HashMap<MatchKey, &InstantSample> = HashMap::with_capacity(one.len());
    for r in one {
        let key = matching_key(&r.labels, &r.metric_name, matching);
        if let Some(duplicate) = one_by_key.insert(key.clone(), r) {
            return Err(duplicate_one_side(&key, swapped, r, duplicate));
        }
    }

    let ctx = PairCtx {
        op,
        bool_modifier,
        matching,
        include,
        swapped,
    };
    let mut state = MatchState {
        one_to_one_matched: HashSet::new(),
        many_matched: HashMap::new(),
        out: Vec::new(),
    };

    // Main pass: every many-side sample against its one-side match (or
    // the one-side fill value when unmatched).
    for l in many {
        let key = matching_key(&l.labels, &l.metric_name, matching);
        match one_by_key.get(&key) {
            Some(r) => emit_pair(&ctx, &mut state, l, r, &key)?,
            None => {
                let Some(fill_value) = fill_one else { continue };
                let synthetic = synthetic_fill_sample(&key, l, fill_value);
                emit_pair(&ctx, &mut state, l, &synthetic, &key)?;
            }
        }
    }

    // Fill pass: any one-side sample whose signature was never matched
    // pairs against a synthetic many side, when the many-side fill value
    // is set.
    if let Some(fill_value) = fill_many {
        for r in one {
            let key = matching_key(&r.labels, &r.metric_name, matching);
            if state.already_matched(one_to_one, &key) {
                continue;
            }
            let synthetic = synthetic_fill_sample(&key, r, fill_value);
            emit_pair(&ctx, &mut state, &synthetic, r, &key)?;
        }
    }

    Ok(state.out)
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

    /// One-to-one, no fill — the M2 shape every pre-#70 test exercises.
    fn vv(
        op: BinOp,
        bool_modifier: bool,
        matching: &Matching,
        lhs: &[InstantSample],
        rhs: &[InstantSample],
    ) -> Result<Vec<InstantSample>, PromqlError> {
        vector_vector(
            op,
            bool_modifier,
            matching,
            &Group::OneToOne,
            &FillValues::default(),
            lhs,
            rhs,
        )
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
        let out = vv(BinOp::Add, false, &ignoring_default(), &lhs, &rhs).unwrap();
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
        let out = vv(BinOp::Add, false, &matching, &lhs, &rhs).unwrap();
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
        let out = vv(BinOp::Add, false, &matching, &lhs, &rhs).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].v, 12.0);
    }

    #[test]
    fn vector_vector_with_no_match_on_the_rhs_drops_the_lhs_element() {
        let lhs = vec![sample(&[("job", "a")], 2.0)];
        let rhs = vec![sample(&[("job", "b")], 10.0)];
        let out = vv(BinOp::Add, false, &ignoring_default(), &lhs, &rhs).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn vector_vector_comparison_without_bool_filters_and_keeps_the_matched_key() {
        let lhs = vec![sample(&[("job", "a")], 5.0), sample(&[("job", "b")], 1.0)];
        let rhs = vec![sample(&[("job", "a")], 3.0), sample(&[("job", "b")], 3.0)];
        let out = vv(BinOp::Gt, false, &ignoring_default(), &lhs, &rhs).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].v, 5.0);
    }

    #[test]
    fn vector_vector_comparison_with_bool_keeps_every_matched_pair() {
        let lhs = vec![sample(&[("job", "a")], 5.0), sample(&[("job", "b")], 1.0)];
        let rhs = vec![sample(&[("job", "a")], 3.0), sample(&[("job", "b")], 3.0)];
        let out = vv(BinOp::Gt, true, &ignoring_default(), &lhs, &rhs).unwrap();
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
        let err = vv(BinOp::Add, false, &matching, &lhs, &rhs).unwrap_err();
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
        let err = vv(BinOp::Add, false, &matching, &lhs, &rhs).unwrap_err();
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
        let out = vv(BinOp::Add, false, &ignoring_default(), &lhs, &rhs).unwrap();
        assert_eq!(out[0].metric_name, None);
    }

    #[test]
    fn vector_vector_filter_comparison_keeps_the_lhs_metric_name() {
        let lhs = vec![sample(&[("job", "a")], 5.0)];
        let rhs = vec![sample(&[("job", "a")], 3.0)];
        let out = vv(BinOp::Gt, false, &ignoring_default(), &lhs, &rhs).unwrap();
        assert_eq!(out[0].metric_name.as_deref(), Some("test_metric"));
    }

    #[test]
    fn vector_vector_bool_comparison_drops_metric_name() {
        let lhs = vec![sample(&[("job", "a")], 5.0)];
        let rhs = vec![sample(&[("job", "a")], 3.0)];
        let out = vv(BinOp::Gt, true, &ignoring_default(), &lhs, &rhs).unwrap();
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
        let out = vv(BinOp::Eq, false, &matching, &lhs, &rhs).unwrap();
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
        let out = vv(BinOp::Eq, false, &matching, &lhs, &rhs).unwrap();
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
        let out = vv(BinOp::Add, false, &matching, &lhs, &rhs).unwrap();
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
        let out = vv(BinOp::Add, false, &matching, &lhs, &rhs).unwrap();
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
        let out = vv(BinOp::Eq, false, &matching, &lhs, &rhs).unwrap();
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
        let out = vv(BinOp::Add, false, &matching, &lhs, &rhs).unwrap();
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
        let out_explicit = vv(BinOp::Add, false, &explicit, &lhs, &rhs).unwrap();
        let out_plain = vv(BinOp::Add, false, &plain, &lhs, &rhs).unwrap();
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
        let out = vv(BinOp::Ne, false, &ignoring_default(), &lhs, &rhs).unwrap();
        assert_eq!(out.len(), 1, "NaN != 5 must keep (upstream: keep=true)");
        assert!(out[0].v.is_nan());
    }

    #[test]
    fn nan_eq_5_drops_in_vector_vector_filter_mode() {
        let lhs = vec![sample(&[("job", "a")], NAN)];
        let rhs = vec![sample(&[("job", "a")], 5.0)];
        let out = vv(BinOp::Eq, false, &ignoring_default(), &lhs, &rhs).unwrap();
        assert!(out.is_empty(), "NaN == 5 must drop (upstream: keep=false)");
    }

    #[test]
    fn nan_ne_5_is_one_in_vector_vector_bool_mode() {
        let lhs = vec![sample(&[("job", "a")], NAN)];
        let rhs = vec![sample(&[("job", "a")], 5.0)];
        let out = vv(BinOp::Ne, true, &ignoring_default(), &lhs, &rhs).unwrap();
        assert_eq!(out[0].v, 1.0);
    }

    #[test]
    fn nan_eq_5_is_zero_in_vector_vector_bool_mode() {
        let lhs = vec![sample(&[("job", "a")], NAN)];
        let rhs = vec![sample(&[("job", "a")], 5.0)];
        let out = vv(BinOp::Eq, true, &ignoring_default(), &lhs, &rhs).unwrap();
        assert_eq!(out[0].v, 0.0);
    }

    // =====================================================================
    // Issue #70 (M6-07): set operators, atan2, group_left/group_right,
    // fill modifiers.
    // =====================================================================

    fn on(labels: &[&str]) -> Matching {
        Matching {
            on: true,
            labels: labels.iter().map(|l| l.to_string()).collect(),
        }
    }

    fn group_left(include: &[&str]) -> Group {
        Group::Left(include.iter().map(|l| l.to_string()).collect())
    }

    fn group_right(include: &[&str]) -> Group {
        Group::Right(include.iter().map(|l| l.to_string()).collect())
    }

    fn no_fill() -> FillValues {
        FillValues::default()
    }

    // --- set-op signature semantics ---

    /// Default matching drops `__name__` from the set-op signature: two
    /// differently-named series with identical labels are the same
    /// member; the surviving element is copied verbatim (name + value).
    #[test]
    fn set_op_and_default_signature_drops_the_metric_name() {
        let lhs = vec![named_sample("foo", &[("job", "a")], 1.0)];
        let rhs = vec![named_sample("bar", &[("job", "a")], 2.0)];
        let out = set_op(SetOp::And, &ignoring_default(), &lhs, &rhs);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_name.as_deref(), Some("foo"));
        assert_eq!(out[0].v, 1.0);
        assert_eq!(out[0].labels.get("job"), Some("a"));
    }

    /// `on(__name__)` includes the real metric name in the signature:
    /// differently-named series no longer intersect.
    #[test]
    fn set_op_and_on_dunder_name_separates_different_names() {
        let lhs = vec![named_sample("foo", &[("job", "a")], 1.0)];
        let rhs = vec![named_sample("bar", &[("job", "a")], 2.0)];
        let out = set_op(SetOp::And, &on(&["__name__"]), &lhs, &rhs);
        assert!(
            out.is_empty(),
            "different names must not intersect: {out:?}"
        );
        let rhs_same = vec![named_sample("foo", &[("job", "b")], 2.0)];
        let out = set_op(SetOp::And, &on(&["__name__"]), &lhs, &rhs_same);
        assert_eq!(out.len(), 1, "same name intersects regardless of labels");
    }

    /// `or` = every lhs element (lhs precedence), then each rhs element
    /// whose signature is absent from the lhs.
    #[test]
    fn set_op_or_is_the_lhs_precedence_union() {
        let lhs = vec![
            named_sample("foo", &[("job", "a")], 1.0),
            named_sample("foo", &[("job", "b")], 2.0),
        ];
        let rhs = vec![
            named_sample("bar", &[("job", "a")], 10.0), // sig collides with lhs -> dropped
            named_sample("bar", &[("job", "c")], 30.0), // new sig -> kept verbatim
        ];
        let out = set_op(SetOp::Or, &ignoring_default(), &lhs, &rhs);
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].v, 1.0);
        assert_eq!(out[1].v, 2.0);
        assert_eq!(out[2].v, 30.0);
        assert_eq!(out[2].metric_name.as_deref(), Some("bar"));
    }

    #[test]
    fn set_op_unless_is_the_signature_complement() {
        let lhs = vec![sample(&[("job", "a")], 1.0), sample(&[("job", "b")], 2.0)];
        let rhs = vec![sample(&[("job", "a")], 10.0)];
        let out = set_op(SetOp::Unless, &ignoring_default(), &lhs, &rhs);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].labels.get("job"), Some("b"));
        assert_eq!(out[0].v, 2.0);
    }

    #[test]
    fn set_op_with_on_matches_on_the_listed_labels_only() {
        let lhs = vec![sample(&[("job", "a"), ("inst", "1")], 1.0)];
        let rhs = vec![sample(&[("job", "a"), ("inst", "2")], 2.0)];
        assert!(set_op(SetOp::And, &ignoring_default(), &lhs, &rhs).is_empty());
        assert_eq!(set_op(SetOp::And, &on(&["job"]), &lhs, &rhs).len(), 1);
    }

    // --- atan2 ---

    /// atan2 is arithmetic-class: elementwise `l.atan2(r)`, `__name__`
    /// dropped, one-to-one label reduction like every arithmetic op.
    #[test]
    fn atan2_is_elementwise_and_drops_the_metric_name() {
        let lhs = vec![named_sample("foo", &[("job", "a")], 10.0)];
        let rhs = vec![named_sample("bar", &[("job", "a")], 100.0)];
        let out = vv(BinOp::Atan2, false, &ignoring_default(), &lhs, &rhs).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].v, 10.0_f64.atan2(100.0));
        assert_eq!(out[0].metric_name, None);
        let out = vector_scalar(BinOp::Atan2, false, &lhs, 2.0, false);
        assert_eq!(out[0].v, 10.0_f64.atan2(2.0));
        assert_eq!(out[0].metric_name, None);
    }

    // --- group_left/group_right ---

    /// group_left arithmetic: output = the many side's full labels with
    /// `__name__` dropped; include labels copied from the one side.
    #[test]
    fn group_left_keeps_the_many_side_labels_and_copies_include_labels() {
        let many = vec![
            named_sample("requests", &[("method", "GET"), ("status", "200")], 100.0),
            named_sample("requests", &[("method", "POST"), ("status", "200")], 200.0),
        ];
        let one = vec![named_sample(
            "limits",
            &[("status", "200"), ("owner", "team-a")],
            1000.0,
        )];
        let out = vv_group(
            BinOp::Mul,
            false,
            &on(&["status"]),
            &group_left(&["owner"]),
            &many,
            &one,
        )
        .unwrap();
        assert_eq!(out.len(), 2);
        for s in &out {
            assert_eq!(s.metric_name, None, "arithmetic drops the many-side name");
            assert_eq!(s.labels.get("status"), Some("200"));
            assert_eq!(s.labels.get("owner"), Some("team-a"), "include copied");
            assert!(s.labels.get("method").is_some(), "many-side labels kept");
        }
    }

    /// group_right is the operand-swapped mirror: the many side is the
    /// source RHS; the value computation restores source operand order.
    #[test]
    fn group_right_mirrors_group_left_with_swapped_sides() {
        let one = vec![named_sample(
            "limits",
            &[("status", "200"), ("owner", "team-a")],
            1000.0,
        )];
        let many = vec![named_sample(
            "requests",
            &[("method", "GET"), ("status", "200")],
            100.0,
        )];
        // limits / on(status) group_right(owner) requests = 1000 / 100.
        let out = vv_group(
            BinOp::Div,
            false,
            &on(&["status"]),
            &group_right(&["owner"]),
            &one,
            &many,
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].v, 10.0, "value restores source operand order");
        assert_eq!(out[0].labels.get("method"), Some("GET"), "many-side labels");
        assert_eq!(out[0].labels.get("owner"), Some("team-a"), "include copied");
        assert_eq!(out[0].metric_name, None);
    }

    /// An include label absent from the one side is DELETED from the
    /// output even when the many side carries it.
    #[test]
    fn group_left_include_label_absent_on_the_one_side_is_deleted() {
        let many = vec![named_sample(
            "requests",
            &[("owner", "stale"), ("status", "200")],
            100.0,
        )];
        let one = vec![named_sample("limits", &[("status", "200")], 1000.0)];
        let out = vv_group(
            BinOp::Add,
            false,
            &on(&["status"]),
            &group_left(&["owner"]),
            &many,
            &one,
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].labels.get("owner"), None, "deleted, not kept");
    }

    /// Plan v2 D2: a FILTER comparison keeps the many-side name; `bool`
    /// drops it — both directions (`group_left` and the `group_right`
    /// mirror), include applied after the drop.
    #[test]
    fn group_comparison_name_rule_filter_keeps_bool_drops_both_directions() {
        let many = vec![named_sample(
            "requests",
            &[("m", "GET"), ("s", "200")],
            100.0,
        )];
        let one = vec![named_sample("limits", &[("s", "200"), ("o", "t")], 1000.0)];

        // requests < on(s) group_left(o) limits — filter keeps.
        let out = vv_group(
            BinOp::Lt,
            false,
            &on(&["s"]),
            &group_left(&["o"]),
            &many,
            &one,
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_name.as_deref(), Some("requests"));
        assert_eq!(out[0].v, 100.0);
        assert_eq!(out[0].labels.get("o"), Some("t"));

        // requests < bool on(s) group_left(o) limits — bool drops.
        let out = vv_group(
            BinOp::Lt,
            true,
            &on(&["s"]),
            &group_left(&["o"]),
            &many,
            &one,
        )
        .unwrap();
        assert_eq!(out[0].metric_name, None);
        assert_eq!(out[0].v, 1.0);
        assert_eq!(
            out[0].labels.get("o"),
            Some("t"),
            "include applied after drop"
        );

        // limits > on(s) group_right(o) requests — filter keeps the MANY
        // side's name (requests), value = source lhs (limits).
        let out = vv_group(
            BinOp::Gt,
            false,
            &on(&["s"]),
            &group_right(&["o"]),
            &one,
            &many,
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_name.as_deref(), Some("requests"));
        assert_eq!(out[0].v, 1000.0, "filter passes the source-lhs value");

        // limits > bool on(s) group_right(o) requests — bool drops.
        let out = vv_group(
            BinOp::Gt,
            true,
            &on(&["s"]),
            &group_right(&["o"]),
            &one,
            &many,
        )
        .unwrap();
        assert_eq!(out[0].metric_name, None);
        assert_eq!(out[0].v, 1.0);
    }

    /// Plan v2 D2: `group_left(__name__)` copies the one side's name into
    /// the metric-name channel (never a `Labels` entry).
    #[test]
    fn group_left_dunder_name_include_copies_the_one_side_name_via_the_name_channel() {
        let many = vec![named_sample("requests", &[("s", "200")], 100.0)];
        let one = vec![named_sample("limits", &[("s", "200")], 1000.0)];
        let out = vv_group(
            BinOp::Mul,
            false,
            &on(&["s"]),
            &group_left(&["__name__"]),
            &many,
            &one,
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric_name.as_deref(), Some("limits"));
        assert_eq!(out[0].labels.get("__name__"), None, "never a Labels entry");
    }

    // --- the three verbatim duplicate-match errors ---

    /// A duplicate signature on the one side (rhs under group_left) is
    /// the verbatim upstream many-to-many error, "right hand-side".
    #[test]
    fn duplicate_one_side_under_group_left_names_the_right_hand_side() {
        let many = vec![sample(&[("s", "200")], 1.0)];
        let one = vec![
            named_sample("limits", &[("s", "200"), ("z", "a")], 1.0),
            named_sample("limits", &[("s", "200"), ("z", "b")], 2.0),
        ];
        let err = vv_group(
            BinOp::Mul,
            false,
            &on(&["s"]),
            &group_left(&[]),
            &many,
            &one,
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("found duplicate series for the match group {s=\"200\"} on the right hand-side of the operation"),
            "got {text:?}"
        );
        assert!(
            text.contains(
                "many-to-many matching not allowed: matching labels must be unique on one side"
            ),
            "got {text:?}"
        );
    }

    /// Under group_right the one side is the source LHS — "left
    /// hand-side" wording.
    #[test]
    fn duplicate_one_side_under_group_right_names_the_left_hand_side() {
        let one = vec![
            named_sample("limits", &[("s", "200"), ("z", "a")], 1.0),
            named_sample("limits", &[("s", "200"), ("z", "b")], 2.0),
        ];
        let many = vec![sample(&[("s", "200")], 1.0)];
        let err = vv_group(
            BinOp::Mul,
            false,
            &on(&["s"]),
            &group_right(&[]),
            &one,
            &many,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("on the left hand-side of the operation"),
            "got {err}"
        );
    }

    /// One-to-one with multiple lhs matches for one signature is the
    /// verbatim "must be explicit" error.
    #[test]
    fn one_to_one_duplicate_match_is_the_must_be_explicit_error() {
        let lhs = vec![
            sample(&[("s", "200"), ("m", "GET")], 1.0),
            sample(&[("s", "200"), ("m", "POST")], 2.0),
        ];
        let rhs = vec![sample(&[("s", "200")], 10.0)];
        let err = vv(BinOp::Add, false, &on(&["s"]), &lhs, &rhs).unwrap_err();
        assert!(
            err.to_string().contains(
                "multiple matches for labels: many-to-one matching must be explicit \
                 (group_left/group_right)"
            ),
            "got {err}"
        );
    }

    /// Many-to-one output identities must be unique per signature — the
    /// verbatim "grouping labels must ensure unique matches" error (here
    /// the include copy collapses the two many-side rows onto one output
    /// identity).
    #[test]
    fn duplicate_many_to_one_output_identity_is_the_unique_matches_error() {
        let many = vec![
            named_sample("requests", &[("m", "GET"), ("s", "200")], 100.0),
            named_sample("requests", &[("m", "POST"), ("s", "200")], 200.0),
        ];
        let one = vec![named_sample("onemeta", &[("s", "200"), ("m", "ALL")], 7.0)];
        let err = vv_group(
            BinOp::Mul,
            false,
            &on(&["s"]),
            &group_left(&["m"]),
            &many,
            &one,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains(
                "multiple matches for labels: grouping labels must ensure unique matches"
            ),
            "got {err}"
        );
    }

    /// Plan v2 D2: the duplicate-output identity hashes on the full
    /// `(Labels, metric_name)` split — two outputs sharing labels but
    /// differing in kept metric name are NOT duplicates (filter mode);
    /// dropping the names (bool) makes them collide.
    #[test]
    fn duplicate_output_identity_hashes_on_labels_and_metric_name_both() {
        // Two many-side samples, same labels, different names (a legal
        // eval-level vector, e.g. downstream of `or`).
        let many = vec![
            named_sample("foo", &[("job", "a")], 5.0),
            named_sample("foo2", &[("job", "a")], 6.0),
        ];
        let one = vec![named_sample("bar", &[("job", "a")], 1.0)];
        // Filter mode keeps distinct names -> distinct identities -> Ok.
        let out = vv_group(
            BinOp::Gt,
            false,
            &on(&["job"]),
            &group_left(&[]),
            &many,
            &one,
        )
        .unwrap();
        assert_eq!(out.len(), 2);
        // bool mode drops both names -> identical identities -> error.
        let err = vv_group(
            BinOp::Gt,
            true,
            &on(&["job"]),
            &group_left(&[]),
            &many,
            &one,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("grouping labels must ensure unique matches"),
            "got {err}"
        );
    }

    // --- fill (one-to-one, both directions; the group-side swap) ---

    /// One-to-one `fill_right`: an unmatched lhs pairs against the fill
    /// value — output keeps the (reduced) lhs identity. `fill_left`: an
    /// unmatched rhs pairs against a synthetic lhs — output is the rhs
    /// match-group identity.
    #[test]
    fn one_to_one_fill_fills_both_directions() {
        let lhs = vec![sample(&[("l", "a")], 10.0), sample(&[("l", "b")], 20.0)];
        let rhs = vec![sample(&[("l", "a")], 100.0), sample(&[("l", "c")], 300.0)];
        let fill = FillValues {
            lhs: Some(0.0),
            rhs: Some(0.0),
        };
        let mut out = vector_vector(
            BinOp::Add,
            false,
            &ignoring_default(),
            &Group::OneToOne,
            &fill,
            &lhs,
            &rhs,
        )
        .unwrap();
        out.sort_by(|a, b| a.labels.cmp(&b.labels));
        let got: Vec<(Option<&str>, f64)> = out.iter().map(|s| (s.labels.get("l"), s.v)).collect();
        assert_eq!(
            got,
            vec![(Some("a"), 110.0), (Some("b"), 20.0), (Some("c"), 300.0)]
        );
    }

    /// Filter-mode comparison with fill pins the name asymmetry: a real
    /// lhs keeps its name; a synthetic (filled) lhs has none, and the
    /// kept value is the FILL value (the lhs side of the comparison).
    #[test]
    fn one_to_one_fill_filter_comparison_name_and_value_asymmetry() {
        let lhs = vec![named_sample("lv", &[("l", "a")], 10.0)];
        let rhs = vec![
            named_sample("rv", &[("l", "a")], 100.0),
            named_sample("rv", &[("l", "c")], 300.0),
        ];
        let fill = FillValues {
            lhs: Some(30.0),
            rhs: Some(30.0),
        };
        let mut out = vector_vector(
            BinOp::Ne,
            false,
            &ignoring_default(),
            &Group::OneToOne,
            &fill,
            &lhs,
            &rhs,
        )
        .unwrap();
        out.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert_eq!(out.len(), 2);
        // Real lhs (10 != 100): keeps name + value.
        assert_eq!(out[0].labels.get("l"), Some("a"));
        assert_eq!(out[0].metric_name.as_deref(), Some("lv"));
        assert_eq!(out[0].v, 10.0);
        // Filled lhs (30 != 300): no name, the fill value passes through.
        assert_eq!(out[1].labels.get("l"), Some("c"));
        assert_eq!(out[1].metric_name, None);
        assert_eq!(out[1].v, 30.0);
    }

    /// Plan v2 D1, direction 1 (missing ONE side, its match filled):
    /// output keeps the real many-side labels untouched; the include
    /// label CANNOT be copied (the one side is absent) — deleted.
    #[test]
    fn group_fill_missing_one_side_keeps_many_labels_and_deletes_include() {
        let many = vec![
            named_sample("requests", &[("m", "GET"), ("s", "200")], 100.0),
            named_sample(
                "requests",
                &[("m", "GET"), ("s", "500"), ("o", "stale")],
                10.0,
            ),
        ];
        let one = vec![named_sample(
            "limits",
            &[("s", "200"), ("o", "team-a")],
            1000.0,
        )];
        let fill = FillValues {
            lhs: None,
            rhs: Some(0.0),
        };
        let mut out = vector_vector(
            BinOp::Add,
            false,
            &on(&["s"]),
            &group_left(&["o"]),
            &fill,
            &many,
            &one,
        )
        .unwrap();
        out.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert_eq!(out.len(), 2);
        // Matched row: include copied from the real one side.
        assert_eq!(out[0].labels.get("s"), Some("200"));
        assert_eq!(out[0].labels.get("o"), Some("team-a"));
        assert_eq!(out[0].v, 1100.0);
        // Filled row (one side absent): many labels kept, include DELETED
        // (even though the many side carried `o` itself).
        assert_eq!(out[1].labels.get("s"), Some("500"));
        assert_eq!(out[1].labels.get("m"), Some("GET"));
        assert_eq!(out[1].labels.get("o"), None);
        assert_eq!(out[1].v, 10.0);
    }

    /// Plan v2 D1, direction 2 (missing MANY side): output is the
    /// matching-key identity, include copied from the REAL one side.
    #[test]
    fn group_fill_missing_many_side_uses_the_identity_and_copies_include() {
        let many = vec![named_sample(
            "requests",
            &[("m", "GET"), ("s", "200")],
            100.0,
        )];
        let one = vec![
            named_sample("limits", &[("s", "200"), ("o", "team-a")], 1000.0),
            named_sample("limits", &[("s", "404"), ("o", "team-c")], 500.0),
        ];
        let fill = FillValues {
            lhs: Some(0.0),
            rhs: None,
        };
        let mut out = vector_vector(
            BinOp::Add,
            false,
            &on(&["s"]),
            &group_left(&["o"]),
            &fill,
            &many,
            &one,
        )
        .unwrap();
        out.sort_by(|a, b| a.labels.cmp(&b.labels).then(a.v.total_cmp(&b.v)));
        assert_eq!(out.len(), 2);
        // Matched row.
        assert_eq!(out[0].labels.get("s"), Some("200"));
        assert_eq!(out[0].v, 1100.0);
        // Filled row: identity {s="404"} + include copied from the one side.
        assert_eq!(out[1].labels.get("s"), Some("404"));
        assert_eq!(out[1].labels.get("m"), None, "identity, not many labels");
        assert_eq!(out[1].labels.get("o"), Some("team-c"), "include copied");
        assert_eq!(out[1].v, 500.0);
    }

    /// The corpus-pinned `group_right` fill-side swap
    /// (`fill-modifier.test`): `node_meta * on(instance) group_right
    /// fill_left(1) cpu_info` fills the MANY (source-rhs) side —
    /// `{instance="c"} 300` — and `fill_right(0)` fills the ONE
    /// (source-lhs) side — `{instance="b", cpu="0"} 0`.
    #[test]
    fn group_right_swaps_the_fill_sides_with_the_operands() {
        let node_meta = vec![
            named_sample("node_meta", &[("instance", "a")], 100.0),
            named_sample("node_meta", &[("instance", "c")], 300.0),
        ];
        let cpu_info = vec![
            named_sample("cpu_info", &[("instance", "a"), ("cpu", "0")], 1.0),
            named_sample("cpu_info", &[("instance", "a"), ("cpu", "1")], 1.0),
            named_sample("cpu_info", &[("instance", "b"), ("cpu", "0")], 1.0),
        ];

        // fill_left(1): post-swap the source-lhs fill applies to the many
        // side (cpu_info) -> {instance="c"} 300 appears.
        let fill = FillValues {
            lhs: Some(1.0),
            rhs: None,
        };
        let mut out = vector_vector(
            BinOp::Mul,
            false,
            &on(&["instance"]),
            &group_right(&[]),
            &fill,
            &node_meta,
            &cpu_info,
        )
        .unwrap();
        out.sort_by(|a, b| a.labels.cmp(&b.labels));
        let got: Vec<(Option<&str>, Option<&str>, f64)> = out
            .iter()
            .map(|s| (s.labels.get("instance"), s.labels.get("cpu"), s.v))
            .collect();
        assert_eq!(
            got,
            vec![
                (Some("a"), Some("0"), 100.0),
                (Some("a"), Some("1"), 100.0),
                (Some("c"), None, 300.0),
            ]
        );

        // fill_right(0): the source-rhs fill applies to the one side
        // (node_meta) -> {instance="b", cpu="0"} 0 appears, no "c" row.
        let fill = FillValues {
            lhs: None,
            rhs: Some(0.0),
        };
        let mut out = vector_vector(
            BinOp::Mul,
            false,
            &on(&["instance"]),
            &group_right(&[]),
            &fill,
            &node_meta,
            &cpu_info,
        )
        .unwrap();
        out.sort_by(|a, b| a.labels.cmp(&b.labels));
        let got: Vec<(Option<&str>, Option<&str>, f64)> = out
            .iter()
            .map(|s| (s.labels.get("instance"), s.labels.get("cpu"), s.v))
            .collect();
        assert_eq!(
            got,
            vec![
                (Some("a"), Some("0"), 100.0),
                (Some("b"), Some("0"), 0.0),
                (Some("a"), Some("1"), 100.0),
            ]
        );
    }

    /// A many-side match dropped by a filter comparison still registers
    /// its signature (upstream registers BEFORE the keep check), so the
    /// fill-LHS pass must not re-fill it.
    #[test]
    fn a_filtered_out_match_still_blocks_the_fill_pass() {
        let lhs = vec![sample(&[("l", "a")], 10.0)];
        let rhs = vec![sample(&[("l", "a")], 100.0)];
        let fill = FillValues {
            lhs: Some(0.0),
            rhs: None,
        };
        // 10 > 100 is dropped; the rhs sig was matched, so no fill row.
        let out = vector_vector(
            BinOp::Gt,
            false,
            &ignoring_default(),
            &Group::OneToOne,
            &fill,
            &lhs,
            &rhs,
        )
        .unwrap();
        assert!(out.is_empty(), "{out:?}");
    }

    // --- #70 code-review round 1: the upstream empty-input
    // short-circuit and duplicate-error text byte-parity ---

    /// Upstream returns empty BEFORE building the one-side signature map
    /// when either operand is empty and no fill is set — a duplicate
    /// one-side signature that could never pair is NOT an error.
    #[test]
    fn empty_many_side_with_duplicate_one_side_and_no_fill_short_circuits_empty() {
        let one = vec![
            sample(&[("s", "200"), ("z", "a")], 1.0),
            sample(&[("s", "200"), ("z", "b")], 2.0),
        ];
        // group_left: empty many (lhs), duplicate one side (rhs).
        let out = vv_group(BinOp::Mul, false, &on(&["s"]), &group_left(&[]), &[], &one).unwrap();
        assert!(out.is_empty());
        // group_right mirror: the one side is the lhs.
        let out = vv_group(BinOp::Mul, false, &on(&["s"]), &group_right(&[]), &one, &[]).unwrap();
        assert!(out.is_empty());
        // One-to-one too: an empty lhs with a duplicate rhs signature.
        let out = vv(BinOp::Add, false, &on(&["s"]), &[], &one).unwrap();
        assert!(out.is_empty());
        // Both sides empty short-circuit even WITH fill values set.
        let fill = FillValues {
            lhs: Some(0.0),
            rhs: Some(0.0),
        };
        let out = vector_vector(
            BinOp::Add,
            false,
            &ignoring_default(),
            &Group::OneToOne,
            &fill,
            &[],
            &[],
        )
        .unwrap();
        assert!(out.is_empty());
    }

    /// With a fill value set the short-circuit does not apply: an empty
    /// many side still evaluates (the fill pass emits from the one side)
    /// — and a duplicate one-side signature IS the error again, exactly
    /// like upstream.
    #[test]
    fn empty_many_side_with_fill_still_evaluates_and_still_detects_duplicates() {
        let one = vec![sample(&[("s", "200")], 5.0)];
        let fill = FillValues {
            lhs: Some(1.0),
            rhs: None,
        };
        let out = vector_vector(
            BinOp::Add,
            false,
            &on(&["s"]),
            &group_left(&[]),
            &fill,
            &[],
            &one,
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].v, 6.0, "fill(1) + 5 from the fill pass");

        let one_dup = vec![
            sample(&[("s", "200"), ("z", "a")], 1.0),
            sample(&[("s", "200"), ("z", "b")], 2.0),
        ];
        let err = vector_vector(
            BinOp::Add,
            false,
            &on(&["s"]),
            &group_left(&[]),
            &fill,
            &[],
            &one_dup,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("found duplicate series for the match group"),
            "got {err}"
        );
    }

    /// Byte-parity golden: a nameless `on(__name__)` match group renders
    /// `{}` — never `{__name__=""}` (upstream's MatchLabels simply has no
    /// `__name__` entry to keep).
    #[test]
    fn duplicate_error_renders_a_nameless_on_dunder_name_group_as_empty_braces() {
        let many = vec![InstantSample {
            labels: Labels::new(vec![("job".to_string(), "a".to_string())]),
            metric_name: None,
            t_ms: 0,
            v: 1.0,
        }];
        let one = vec![
            InstantSample {
                labels: Labels::new(vec![("z".to_string(), "a".to_string())]),
                metric_name: None,
                t_ms: 0,
                v: 1.0,
            },
            InstantSample {
                labels: Labels::new(vec![("z".to_string(), "b".to_string())]),
                metric_name: None,
                t_ms: 0,
                v: 2.0,
            },
        ];
        let err = vv_group(
            BinOp::Mul,
            false,
            &on(&["__name__"]),
            &group_left(&[]),
            &many,
            &one,
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("found duplicate series for the match group {} on the right hand-side"),
            "got {text:?}"
        );
        assert!(!text.contains("__name__=\"\""), "got {text:?}");
    }

    /// Byte-parity golden: label values are Go-quoted — a control byte
    /// renders `\x01` (Go strconv.Quote), not Rust's `\u{1}`.
    #[test]
    fn duplicate_error_go_quotes_a_control_character_label_value() {
        let many = vec![sample(&[("z", "a\u{1}b")], 1.0)];
        let one = vec![
            sample(&[("z", "a\u{1}b"), ("extra", "1")], 1.0),
            sample(&[("z", "a\u{1}b"), ("extra", "2")], 2.0),
        ];
        let err = vv_group(
            BinOp::Mul,
            false,
            &on(&["z"]),
            &group_left(&[]),
            &many,
            &one,
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("match group {z=\"a\\x01b\"} on the right hand-side"),
            "got {text:?}"
        );
        assert!(!text.contains("u{1}"), "Rust-style escape leaked: {text:?}");
    }

    // The go_quote/is_print escape goldens (incl. the round-2 NBSP/soft-
    // hyphen/zero-width-space pins and the full-codepoint-space checksum
    // against real Go) live with the port itself — `eval::quote::tests`.

    /// Byte-parity golden (round 2): an NBSP label value takes Go's
    /// `\u00a0` through the real error path, not Rust's `\u{a0}` and not
    /// verbatim (Go's Latin-1 IsPrint excludes 0xA0).
    #[test]
    fn duplicate_error_go_quotes_a_nbsp_label_value() {
        let many = vec![sample(&[("z", "a\u{a0}b")], 1.0)];
        let one = vec![
            sample(&[("z", "a\u{a0}b"), ("extra", "1")], 1.0),
            sample(&[("z", "a\u{a0}b"), ("extra", "2")], 2.0),
        ];
        let err = vv_group(
            BinOp::Mul,
            false,
            &on(&["z"]),
            &group_left(&[]),
            &many,
            &one,
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("match group {z=\"a\\u00a0b\"} on the right hand-side"),
            "got {text:?}"
        );
    }

    /// Convenience for the group tests: full vector_vector with a group.
    fn vv_group(
        op: BinOp,
        bool_modifier: bool,
        matching: &Matching,
        group: &Group,
        lhs: &[InstantSample],
        rhs: &[InstantSample],
    ) -> Result<Vec<InstantSample>, PromqlError> {
        vector_vector(op, bool_modifier, matching, group, &no_fill(), lhs, rhs)
    }
}
