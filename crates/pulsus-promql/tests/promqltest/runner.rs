//! Executes parsed `.test` commands against the real
//! `parse -> plan -> evaluate` pipeline (the store stands in for the fetch
//! layer only) and compares results with upstream promqltest semantics:
//! values are compared with the upstream driver's own tolerance
//! (`almost.Equal`, relative epsilon `1e-6` — `defaultEpsilon` in
//! `promql/promqltest/test.go`; expected values in the vendored files are
//! written to that tolerance, so a stricter bit-exact comparison would
//! misread the format itself), `NaN == NaN` for testing purposes, instant
//! vectors compare as sets (`eval_ordered` as an ordered list), range
//! results compare positionally per step with `_` asserting absence.
//!
//! Each failing case carries the construct set collected from its parsed
//! AST so the corpus test can classify it against the coverage manifest
//! (issue #64 plan: expected-fail iff a used construct is
//! `scheduled`/`deferred`; anything else must be ledger-classified or the
//! test fails).

use std::collections::BTreeSet;
use std::fmt::Write as _;

use pulsus_promql::parser::{Expr, VectorMatchCardinality, token};
use pulsus_promql::{PlanParams, QueryValue, evaluate, plan};

use super::grammar::{Command, EvalCmd, EvalKind, EvalMode, Expected, ExpectedSeries, parse_file};
use super::series::SeqValue;
use super::store::TestStorage;

/// Upstream promqltest's `defaultEpsilon`: relative error allowed for
/// sample values.
const EPSILON: f64 = 1e-6;

/// A full sorted label set (including `__name__` when present) — the
/// comparison key for result series.
type LabelsVec = Vec<(String, String)>;

/// The constructs one query uses, collected from its parsed AST — the
/// coverage manifest's classification key.
#[derive(Debug, Clone, Default)]
pub struct Constructs {
    pub functions: BTreeSet<String>,
    pub operators: BTreeSet<String>,
    pub features: BTreeSet<String>,
}

/// Collects functions / aggregation operators / tracked language features
/// used by `expr`. Feature names match the closed `language_features` set
/// in `coverage/function-coverage.json` exactly.
pub fn collect_constructs(expr: &Expr) -> Constructs {
    let mut c = Constructs::default();
    walk(expr, &mut c);
    c
}

fn walk(expr: &Expr, c: &mut Constructs) {
    match expr {
        Expr::Call(call) => {
            c.functions.insert(call.func.name.to_string());
            for arg in &call.args.args {
                walk(arg, c);
            }
        }
        Expr::Aggregate(agg) => {
            c.operators.insert(agg.op.to_string());
            if let Some(p) = &agg.param {
                walk(p, c);
            }
            walk(&agg.expr, c);
        }
        Expr::Binary(bin) => {
            let id = bin.op.id();
            if id == token::T_LAND {
                c.features.insert("set-op-and".to_string());
            } else if id == token::T_LOR {
                c.features.insert("set-op-or".to_string());
            } else if id == token::T_LUNLESS {
                c.features.insert("set-op-unless".to_string());
            } else if id == token::T_ATAN2 {
                c.features.insert("atan2".to_string());
            }
            if let Some(m) = &bin.modifier {
                match m.card {
                    VectorMatchCardinality::ManyToOne(_) => {
                        c.features.insert("group_left".to_string());
                    }
                    VectorMatchCardinality::OneToMany(_) => {
                        c.features.insert("group_right".to_string());
                    }
                    _ => {}
                }
                if m.matching.is_some() {
                    c.features.insert("vector-matching-on-ignoring".to_string());
                }
                // Issue #81: the experimental fill/fill_left/fill_right
                // binary-operator modifiers — scheduled(M6-07); the
                // planner rejects them by name until then.
                if m.fill_values.lhs.is_some() || m.fill_values.rhs.is_some() {
                    c.features.insert("binop-fill-modifier".to_string());
                }
            }
            walk(&bin.lhs, c);
            walk(&bin.rhs, c);
        }
        Expr::Subquery(sq) => {
            c.features.insert("subquery".to_string());
            if sq.at.is_some() {
                c.features.insert("at-modifier".to_string());
            }
            walk(&sq.expr, c);
        }
        Expr::VectorSelector(vs) => {
            if vs.at.is_some() {
                c.features.insert("at-modifier".to_string());
            }
            if !vs.matchers.or_matchers.is_empty() {
                // The UTF-8-quoted label-name-or selector syntax is the
                // slice of UTF-8 support the planner still rejects (plain
                // quoted label/metric names already flow through).
                c.features.insert("utf8-label-names".to_string());
            }
        }
        Expr::MatrixSelector(ms) => {
            if ms.vs.at.is_some() {
                c.features.insert("at-modifier".to_string());
            }
            if !ms.vs.matchers.or_matchers.is_empty() {
                c.features.insert("utf8-label-names".to_string());
            }
        }
        Expr::Paren(p) => walk(&p.expr, c),
        Expr::Unary(u) => walk(&u.expr, c),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {}
        Expr::Extension(_) => {}
    }
}

/// One executed case's outcome.
#[derive(Debug, Clone)]
pub struct CaseReport {
    pub line: usize,
    pub query: String,
    pub mode: EvalMode,
    pub passed: bool,
    /// Failure detail (empty when passed).
    pub detail: String,
    /// `None` when our own `parse()` rejected the query (no AST to walk) —
    /// such a failure can only be ledger-classified.
    pub constructs: Option<Constructs>,
}

/// How many of each executed directive a file used — the proof corpus's
/// grammar-coverage assertion reads these.
#[derive(Debug, Clone, Copy, Default)]
pub struct DirectiveCounts {
    pub clear: usize,
    pub load: usize,
    pub eval_instant: usize,
    /// Bare `eval instant <expr>` (no `at` clause, eval time = `T0`).
    pub eval_instant_bare: usize,
    pub eval_range: usize,
    pub eval_ordered: usize,
    pub eval_fail: usize,
    pub fail_message: usize,
    pub fail_regexp: usize,
}

#[derive(Debug, Clone)]
pub struct FileRun {
    pub file: String,
    pub cases: Vec<CaseReport>,
    pub counts: DirectiveCounts,
}

/// Parses and executes one fully-executable `.test` file. `Err` = grammar
/// error (hard, loud); individual case failures are reported per case.
pub fn run_file(file: &str, text: &str) -> Result<FileRun, String> {
    let commands = parse_file(file, text)?;
    let mut storage = TestStorage::new();
    let mut cases = Vec::new();
    let mut counts = DirectiveCounts::default();

    for command in &commands {
        match command {
            Command::Clear => {
                counts.clear += 1;
                storage.clear();
            }
            Command::Load { step_ms, series } => {
                counts.load += 1;
                storage
                    .load(*step_ms, series)
                    .map_err(|e| format!("{file}: load failed: {e}"))?;
            }
            Command::Eval(cmd) => {
                match cmd.kind {
                    EvalKind::Instant { .. } => {
                        counts.eval_instant += 1;
                        if cmd.bare_instant {
                            counts.eval_instant_bare += 1;
                        }
                    }
                    EvalKind::Range { .. } => counts.eval_range += 1,
                }
                match cmd.mode {
                    EvalMode::Ordered => counts.eval_ordered += 1,
                    EvalMode::Fail => {
                        counts.eval_fail += 1;
                        if let Expected::Fail { message, regexp } = &cmd.expected {
                            if message.is_some() {
                                counts.fail_message += 1;
                            }
                            if regexp.is_some() {
                                counts.fail_regexp += 1;
                            }
                        }
                    }
                    EvalMode::Pass => {}
                }
                cases.push(run_eval(&storage, cmd)?);
            }
        }
    }

    Ok(FileRun {
        file: file.to_string(),
        cases,
        counts,
    })
}

fn params_for(kind: &EvalKind) -> PlanParams {
    match *kind {
        EvalKind::Instant { at_ms } => PlanParams {
            start_ms: at_ms,
            end_ms: at_ms,
            step_ms: 0,
            lookback_ms: pulsus_promql::DEFAULT_LOOKBACK_MS,
        },
        EvalKind::Range {
            from_ms,
            to_ms,
            step_ms,
        } => PlanParams {
            start_ms: from_ms,
            end_ms: to_ms,
            step_ms,
            lookback_ms: pulsus_promql::DEFAULT_LOOKBACK_MS,
        },
    }
}

fn run_eval(storage: &TestStorage, cmd: &EvalCmd) -> Result<CaseReport, String> {
    let params = params_for(&cmd.kind);

    let mut constructs = None;
    let engine_result: Result<QueryValue, String> = match pulsus_promql::parse(&cmd.query) {
        Err(e) => Err(e.to_string()),
        Ok(expr) => {
            constructs = Some(collect_constructs(&expr));
            match plan(&expr, params) {
                Err(e) => Err(e.to_string()),
                Ok(query_plan) => {
                    // A store/regex failure is a driver defect, not an
                    // engine error — it must never satisfy `eval_fail`.
                    let data = storage.fetch(&query_plan).map_err(|e| {
                        format!("{}:{}: driver fetch error: {e}", cmd.query, cmd.line)
                    })?;
                    evaluate(&query_plan, &data).map_err(|e| e.to_string())
                }
            }
        }
    };

    let (passed, detail) = judge(cmd, engine_result)?;
    Ok(CaseReport {
        line: cmd.line,
        query: cmd.query.clone(),
        mode: cmd.mode,
        passed,
        detail,
        constructs,
    })
}

/// Applies the case's expectation to the engine outcome. `Err` = a driver
/// defect (bad expected-regexp etc.), everything else is a pass/fail.
fn judge(cmd: &EvalCmd, outcome: Result<QueryValue, String>) -> Result<(bool, String), String> {
    match (&cmd.expected, outcome) {
        (Expected::Fail { message, regexp }, Err(err_text)) => {
            if let Some(m) = message
                && !err_text.contains(m.as_str())
            {
                return Ok((
                    false,
                    format!(
                        "eval_fail matched (query errored) but the error text mismatched: \
                         got {err_text:?}, want substring {m:?}"
                    ),
                ));
            }
            if let Some(p) = regexp {
                let re = regex::Regex::new(p)
                    .map_err(|e| format!("invalid expected_fail_regexp {p:?}: {e}"))?;
                if !re.is_match(&err_text) {
                    return Ok((
                        false,
                        format!(
                            "eval_fail matched (query errored) but the error text mismatched: \
                             got {err_text:?}, want regexp {p:?}"
                        ),
                    ));
                }
            }
            Ok((true, String::new()))
        }
        (Expected::Fail { .. }, Ok(v)) => Ok((
            false,
            format!("eval_fail expected an error but the query succeeded with {v:?}"),
        )),
        (_, Err(err_text)) => Ok((false, format!("query errored: {err_text}"))),
        (Expected::Scalar(want), Ok(QueryValue::Scalar(got))) => {
            if almost_equal(*want, got) {
                Ok((true, String::new()))
            } else {
                Ok((false, format!("scalar mismatch: got {got}, want {want}")))
            }
        }
        (Expected::Scalar(want), Ok(other)) => Ok((
            false,
            format!("expected scalar {want}, got non-scalar {other:?}"),
        )),
        (Expected::Vector(expected), Ok(QueryValue::Vector(actual))) => {
            let actual: Vec<(LabelsVec, f64)> = actual
                .iter()
                .map(|s| {
                    let mut labels: LabelsVec = s.labels.0.clone();
                    if let Some(name) = &s.metric_name {
                        labels.push(("__name__".to_string(), name.clone()));
                    }
                    labels.sort();
                    (labels, s.v)
                })
                .collect();
            match cmd.mode {
                EvalMode::Ordered => Ok(compare_vector_ordered(expected, &actual)),
                _ => Ok(compare_vector_unordered(expected, &actual)),
            }
        }
        (Expected::Vector(expected), Ok(other)) => {
            if expected.is_empty() {
                // An empty expectation asserts an empty instant vector — a
                // scalar/matrix result is still a type mismatch.
                Ok((
                    false,
                    format!("expected an empty instant vector, got {other:?}"),
                ))
            } else {
                Ok((false, format!("expected an instant vector, got {other:?}")))
            }
        }
        (Expected::Matrix(expected), Ok(QueryValue::Matrix(actual))) => {
            let EvalKind::Range {
                from_ms, step_ms, ..
            } = cmd.kind
            else {
                return Err("matrix expectation on a non-range eval".to_string());
            };
            Ok(compare_matrix(expected, &actual, from_ms, step_ms))
        }
        (Expected::Matrix(_), Ok(other)) => Ok((
            false,
            format!("expected a range-query matrix, got {other:?}"),
        )),
    }
}

fn expected_labels_vec(s: &ExpectedSeries) -> LabelsVec {
    // BTreeMap iterates sorted by key already.
    s.labels
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn expected_single_value(s: &ExpectedSeries) -> f64 {
    match s.values.as_slice() {
        [SeqValue::Value(v)] => *v,
        // The grammar rejects gaps/stale/multi-value in instant
        // expectations before this is reachable.
        other => unreachable!("instant expectation with non-single value {other:?}"),
    }
}

fn fmt_labels(labels: &[(String, String)]) -> String {
    let mut out = String::from("{");
    for (i, (k, v)) in labels.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        let _ = write!(out, "{k}={v:?}");
    }
    out.push('}');
    out
}

fn compare_vector_unordered(
    expected: &[ExpectedSeries],
    actual: &[(LabelsVec, f64)],
) -> (bool, String) {
    if expected.len() != actual.len() {
        return (
            false,
            format!(
                "series count mismatch: got {} ({}), want {} ({})",
                actual.len(),
                actual
                    .iter()
                    .map(|(l, v)| format!("{} {v}", fmt_labels(l)))
                    .collect::<Vec<_>>()
                    .join("; "),
                expected.len(),
                expected
                    .iter()
                    .map(|e| fmt_labels(&expected_labels_vec(e)))
                    .collect::<Vec<_>>()
                    .join("; "),
            ),
        );
    }
    for exp in expected {
        let labels = expected_labels_vec(exp);
        let want = expected_single_value(exp);
        let matches: Vec<&(LabelsVec, f64)> = actual.iter().filter(|(l, _)| *l == labels).collect();
        match matches.as_slice() {
            [] => {
                return (
                    false,
                    format!("expected series {} is absent", fmt_labels(&labels)),
                );
            }
            [(_, got)] => {
                if !almost_equal(want, *got) {
                    return (
                        false,
                        format!(
                            "value mismatch for {}: got {got}, want {want}",
                            fmt_labels(&labels)
                        ),
                    );
                }
            }
            _ => {
                return (
                    false,
                    format!("duplicate result series {}", fmt_labels(&labels)),
                );
            }
        }
    }
    (true, String::new())
}

fn compare_vector_ordered(
    expected: &[ExpectedSeries],
    actual: &[(LabelsVec, f64)],
) -> (bool, String) {
    if expected.len() != actual.len() {
        return (
            false,
            format!(
                "series count mismatch: got {}, want {}",
                actual.len(),
                expected.len()
            ),
        );
    }
    for (i, exp) in expected.iter().enumerate() {
        let labels = expected_labels_vec(exp);
        let want = expected_single_value(exp);
        let (got_labels, got) = &actual[i];
        if *got_labels != labels {
            return (
                false,
                format!(
                    "order mismatch at position {i}: got {}, want {}",
                    fmt_labels(got_labels),
                    fmt_labels(&labels)
                ),
            );
        }
        if !almost_equal(want, *got) {
            return (
                false,
                format!(
                    "value mismatch at position {i} for {}: got {got}, want {want}",
                    fmt_labels(&labels)
                ),
            );
        }
    }
    (true, String::new())
}

fn compare_matrix(
    expected: &[ExpectedSeries],
    actual: &[pulsus_promql::RangeSeries],
    from_ms: i64,
    step_ms: i64,
) -> (bool, String) {
    let actual_by_labels: Vec<(LabelsVec, &[(i64, f64)])> = actual
        .iter()
        .map(|s| {
            let mut labels: LabelsVec = s.labels.0.clone();
            if let Some(name) = &s.metric_name {
                labels.push(("__name__".to_string(), name.clone()));
            }
            labels.sort();
            (labels, s.points.as_slice())
        })
        .collect();

    let mut matched_actual = vec![false; actual_by_labels.len()];
    for exp in expected {
        let labels = expected_labels_vec(exp);
        let want_points: Vec<(i64, f64)> = exp
            .values
            .iter()
            .enumerate()
            .filter_map(|(k, v)| match v {
                SeqValue::Value(v) => Some((from_ms + k as i64 * step_ms, *v)),
                _ => None,
            })
            .collect();

        let found = actual_by_labels.iter().position(|(l, _)| *l == labels);
        let Some(idx) = found else {
            if want_points.is_empty() {
                continue; // all-gap expectation: an absent series is fine
            }
            return (
                false,
                format!("expected series {} is absent", fmt_labels(&labels)),
            );
        };
        if matched_actual[idx] {
            return (
                false,
                format!("duplicate expected series {}", fmt_labels(&labels)),
            );
        }
        matched_actual[idx] = true;
        let got_points = actual_by_labels[idx].1;
        if got_points.len() != want_points.len() {
            return (
                false,
                format!(
                    "point count mismatch for {}: got {:?}, want {:?}",
                    fmt_labels(&labels),
                    got_points,
                    want_points
                ),
            );
        }
        for ((gt, gv), (wt, wv)) in got_points.iter().zip(&want_points) {
            if gt != wt || !almost_equal(*wv, *gv) {
                return (
                    false,
                    format!(
                        "point mismatch for {}: got ({gt}, {gv}), want ({wt}, {wv})",
                        fmt_labels(&labels)
                    ),
                );
            }
        }
    }
    if let Some(idx) = matched_actual.iter().position(|m| !m) {
        let (labels, points) = &actual_by_labels[idx];
        return (
            false,
            format!("unexpected result series {} {points:?}", fmt_labels(labels)),
        );
    }
    (true, String::new())
}

/// Upstream `util/almost.Equal` with `defaultEpsilon` (1e-6): `NaN`
/// equals `NaN` (testing convention), exact equality short-circuits, tiny
/// sums compare against `epsilon * MIN_NORMAL`, everything else compares
/// relative error.
pub fn almost_equal(a: f64, b: f64) -> bool {
    if a.is_nan() && b.is_nan() {
        return true;
    }
    if a == b {
        return true;
    }
    let abs_sum = a.abs() + b.abs();
    let diff = (a - b).abs();
    if a == 0.0 || b == 0.0 || abs_sum < f64::MIN_POSITIVE {
        return diff < EPSILON * f64::MIN_POSITIVE;
    }
    diff / abs_sum.min(f64::MAX) < EPSILON
}
