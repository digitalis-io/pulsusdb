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

use pulsus_model::FloatHistogram;
use pulsus_promql::parser::{Expr, PLabelMatchOp, VectorMatchCardinality, VectorSelector, token};
use pulsus_promql::{Annotations, PlanParams, QueryValue, evaluate, plan};

use super::grammar::{
    AnnotationMatch, Command, EvalCmd, EvalKind, EvalMode, Expected, ExpectedSeries, parse_file,
};
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
            // Issue #84 (M6-08b): any non-literal duration expression in
            // the subquery range/step/offset positions.
            if sq.range_expr.is_some() || sq.step_expr.is_some() || sq.offset_expr.is_some() {
                c.features.insert("duration-expression".to_string());
            }
            walk(&sq.expr, c);
        }
        Expr::VectorSelector(vs) => {
            if vs.at.is_some() {
                c.features.insert("at-modifier".to_string());
            }
            if vs.offset_expr.is_some() {
                c.features.insert("duration-expression".to_string());
            }
            collect_selector_name_features(vs, c);
        }
        Expr::MatrixSelector(ms) => {
            if ms.vs.at.is_some() {
                c.features.insert("at-modifier".to_string());
            }
            if ms.range_expr.is_some() || ms.vs.offset_expr.is_some() {
                c.features.insert("duration-expression".to_string());
            }
            collect_selector_name_features(&ms.vs, c);
        }
        Expr::Paren(p) => walk(&p.expr, c),
        Expr::Unary(u) => walk(&u.expr, c),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {}
        Expr::Extension(_) => {}
    }
}

/// Issue #85 (M6-08c): `utf8-label-names` is emitted when a selector uses
/// a metric or label name outside the legacy (pre-UTF-8) charset — i.e. a
/// name only reachable through the quoted-name syntax (`{"utf8.metric"}`,
/// `m{"label.dot"="x"}`). The previous mapping keyed on the parser's
/// brace-level `or_matchers` extension is gone: pinned upstream v3.13.0
/// has no such syntax (`label_match_list` is comma-only), so it is a
/// permanent plan-time rejection, not the UTF-8 feature.
fn collect_selector_name_features(vs: &VectorSelector, c: &mut Constructs) {
    let mut utf8 = vs
        .name
        .as_deref()
        .is_some_and(|n| !is_legacy_metric_name(n));
    for m in vs
        .matchers
        .matchers
        .iter()
        .chain(vs.matchers.or_matchers.iter().flatten())
    {
        if m.name == "__name__" {
            if matches!(m.op, PLabelMatchOp::Equal) && !is_legacy_metric_name(&m.value) {
                utf8 = true;
            }
        } else if !is_legacy_label_name(&m.name) {
            utf8 = true;
        }
    }
    if utf8 {
        c.features.insert("utf8-label-names".to_string());
    }
}

/// The legacy metric-name charset, `[a-zA-Z_:][a-zA-Z0-9_:]*` (upstream
/// `model.IsValidLegacyMetricName`).
fn is_legacy_metric_name(name: &str) -> bool {
    !name.is_empty()
        && name.chars().enumerate().all(|(i, ch)| {
            ch.is_ascii_alphabetic() || ch == '_' || ch == ':' || (i > 0 && ch.is_ascii_digit())
        })
}

/// The legacy label-name charset, `[a-zA-Z_][a-zA-Z0-9_]*` (upstream
/// `model.LabelName.IsValidLegacy`).
fn is_legacy_label_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .enumerate()
            .all(|(i, ch)| ch.is_ascii_alphabetic() || ch == '_' || (i > 0 && ch.is_ascii_digit()))
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
    /// Block-form `expect fail` directives (issue #86) — counted apart
    /// from the `eval_fail` prefix form.
    pub expect_fail: usize,
    /// `expect fail` lines carrying an inline `msg:`/`regex:` tail.
    pub expect_fail_tagged: usize,
    /// `expect string` directives (issue #86).
    pub expect_string: usize,
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
                    EvalMode::Fail if cmd.expect_fail => {
                        counts.expect_fail += 1;
                        if let Expected::Fail { message, regexp } = &cmd.expected
                            && (message.is_some() || regexp.is_some())
                        {
                            counts.expect_fail_tagged += 1;
                        }
                    }
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
                if cmd.expect_string {
                    counts.expect_string += 1;
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
    // `experimental_functions: true` in both arms: upstream promqltest
    // runs with `EnableExperimentalFunctions` on, so the vendored corpus
    // (and the proof files' `max_of`/`min_of` cases) plan under the same
    // gate state (issue #65).
    match *kind {
        EvalKind::Instant { at_ms } => PlanParams {
            start_ms: at_ms,
            end_ms: at_ms,
            step_ms: 0,
            lookback_ms: pulsus_promql::DEFAULT_LOOKBACK_MS,
            experimental_functions: true,
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
            experimental_functions: true,
        },
    }
}

fn run_eval(storage: &TestStorage, cmd: &EvalCmd) -> Result<CaseReport, String> {
    let params = params_for(&cmd.kind);

    let mut constructs = None;
    let engine_result: Result<(QueryValue, Annotations), String> =
        match pulsus_promql::parse(&cmd.query) {
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
/// Issue #124 (M7-A6): a successful outcome now also carries the
/// [`Annotations`] channel — checked via [`check_annotations`] AFTER the
/// value comparison passes, mirroring upstream's `compareResult` then
/// `checkAnnotations` ordering (`promqltest/test.go:1720-1742`); a failing
/// `eval_fail`/`expect fail` case never reaches annotation checking
/// (upstream's error branch doesn't call `checkAnnotations` either).
fn judge(
    cmd: &EvalCmd,
    outcome: Result<(QueryValue, Annotations), String>,
) -> Result<(bool, String), String> {
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
        (Expected::Fail { .. }, Ok((v, _))) => Ok((
            false,
            format!("eval_fail expected an error but the query succeeded with {v:?}"),
        )),
        (_, Err(err_text)) => Ok((false, format!("query errored: {err_text}"))),
        (expected, Ok((value, annotations))) => {
            let (ok, detail) = judge_value(cmd, expected, value)?;
            if !ok {
                return Ok((false, detail));
            }
            match check_annotations(cmd, &annotations) {
                Ok(()) => Ok((true, String::new())),
                Err(detail) => Ok((false, detail)),
            }
        }
    }
}

/// Renders an `expect string` want-value losslessly for a diagnostic:
/// valid UTF-8 keeps today's `{:?}` form, non-UTF-8 falls back to a
/// `b"…"` `escape_ascii` form (never `from_utf8_lossy` — that could
/// false-read as matching engine output containing U+FFFD).
fn format_expected_bytes(want: &[u8]) -> String {
    match std::str::from_utf8(want) {
        Ok(s) => format!("{s:?}"),
        Err(_) => format!("b\"{}\"", want.escape_ascii()),
    }
}

/// The value-shaped half of [`judge`] (everything but `Expected::Fail`,
/// handled by the caller before this is reached).
fn judge_value(
    cmd: &EvalCmd,
    expected: &Expected,
    value: QueryValue,
) -> Result<(bool, String), String> {
    match (expected, value) {
        (Expected::Fail { .. }, _) => {
            unreachable!("judge() handles Expected::Fail before calling judge_value")
        }
        // `expect string` (issue #86): exact BYTE comparison — no
        // epsilon, no normalization (upstream compares the unquoted Go
        // string, itself a byte slice, against the `promql.String` value
        // verbatim). `want` is `Vec<u8>` so a non-UTF-8 Go literal is
        // representable; the engine's `QueryValue::String` is always
        // valid UTF-8 (Prometheus's data model), so such a case can
        // never pass — it fails honestly with a lossless diagnostic
        // instead of a file-fatal grammar error.
        (Expected::String(want), QueryValue::String(got)) => {
            if want.as_slice() == got.as_bytes() {
                Ok((true, String::new()))
            } else {
                Ok((
                    false,
                    format!(
                        "string mismatch: got {got:?}, want {}",
                        format_expected_bytes(want)
                    ),
                ))
            }
        }
        (Expected::String(want), other) => Ok((
            false,
            format!(
                "expected string {}, got non-string {other:?}",
                format_expected_bytes(want)
            ),
        )),
        (Expected::Scalar(want), QueryValue::Scalar(got)) => {
            if almost_equal(*want, got) {
                Ok((true, String::new()))
            } else {
                Ok((false, format!("scalar mismatch: got {got}, want {want}")))
            }
        }
        (Expected::Scalar(want), other) => Ok((
            false,
            format!("expected scalar {want}, got non-scalar {other:?}"),
        )),
        (Expected::Vector(expected), QueryValue::Vector(actual)) => {
            let actual: Vec<(LabelsVec, Val)> = actual
                .iter()
                .map(|s| {
                    let mut labels: LabelsVec = s.labels.0.clone();
                    if let Some(name) = &s.metric_name {
                        labels.push(("__name__".to_string(), name.clone()));
                    }
                    labels.sort();
                    (labels, Val::from_sample(s.v, &s.h))
                })
                .collect();
            match cmd.mode {
                EvalMode::Ordered => Ok(compare_vector_ordered(expected, &actual)),
                _ => Ok(compare_vector_unordered(expected, &actual)),
            }
        }
        (Expected::Vector(expected), other) => {
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
        (Expected::Matrix(expected), QueryValue::Matrix(actual)) => {
            let EvalKind::Range {
                from_ms, step_ms, ..
            } = cmd.kind
            else {
                return Err("matrix expectation on a non-range eval".to_string());
            };
            Ok(compare_matrix(expected, &actual, from_ms, step_ms))
        }
        (Expected::Matrix(_), other) => Ok((
            false,
            format!("expected a range-query matrix, got {other:?}"),
        )),
    }
}

/// Checks the case's `expect warn|no_warn|info|no_info` directives
/// (issue #124, M7-A6) against `evaluate()`'s [`Annotations`] channel —
/// mirrors upstream `checkAnnotations` (`promqltest/test.go:1211-1235`).
/// [`Annotations::base_messages`] (not `as_strings`) matches upstream's
/// query-unset comparison exactly: no cap/omission line, no
/// forced-monotonicity detail suffix.
fn check_annotations(cmd: &EvalCmd, annotations: &Annotations) -> Result<(), String> {
    let (warnings, infos) = annotations.base_messages();
    check_annotation_kind(&cmd.expect_warn, cmd.expect_no_warn, &warnings, "warn")?;
    check_annotation_kind(&cmd.expect_info, cmd.expect_no_info, &infos, "info")?;
    Ok(())
}

fn check_annotation_kind(
    expected: &[AnnotationMatch],
    expect_none: bool,
    actual: &[String],
    kind: &str,
) -> Result<(), String> {
    if !expected.is_empty() {
        if actual.is_empty() {
            return Err(format!("expected {kind} annotations but none were found"));
        }
        for e in expected {
            if !actual.iter().any(|a| annotation_matches(e, a)) {
                return Err(format!(
                    "expected {kind} annotation matching {e:?} but no matching annotation was \
                     found, found: {actual:?}"
                ));
            }
        }
        for a in actual {
            if !expected.iter().any(|e| annotation_matches(e, a)) {
                return Err(format!("unexpected {kind} annotation {a:?} found"));
            }
        }
    }
    if expect_none && !actual.is_empty() {
        return Err(format!("unexpected {kind} annotations: {actual:?}"));
    }
    Ok(())
}

fn annotation_matches(m: &AnnotationMatch, actual: &str) -> bool {
    match m {
        AnnotationMatch::Any => true,
        AnnotationMatch::Message(want) => want == actual,
        AnnotationMatch::Regex(pat) => regex::Regex::new(pat)
            .map(|re| re.is_match(actual))
            .unwrap_or(false),
    }
}

/// One value channel — float or native-histogram (issue #124, M7-A6) —
/// unifying an expected `{{...}}`/plain-number literal with an actual
/// [`pulsus_promql::InstantSample`]/[`pulsus_promql::Point`]. The `Hist`
/// `bool` is `hint_set` (issue #125): meaningful on the EXPECTED side
/// only (whether the literal spelled `counter_reset_hint:` — the pin's
/// `expectedHPoint.CounterResetHintSet`); actuals always carry `false`
/// and the comparator never reads the actual's flag.
#[derive(Debug, Clone)]
enum Val {
    Float(f64),
    Hist(FloatHistogram, bool),
}

impl Val {
    fn from_sample(v: f64, h: &Option<Box<FloatHistogram>>) -> Val {
        match h {
            Some(h) => Val::Hist((**h).clone(), false),
            None => Val::Float(v),
        }
    }

    fn from_seq(sv: &SeqValue) -> Val {
        match sv {
            SeqValue::Value(v) => Val::Float(*v),
            SeqValue::Histogram(h, hint_set) => Val::Hist(h.clone(), *hint_set),
            // The grammar rejects gaps/stale in an instant expectation,
            // and range-matrix callers filter `Gap` before this is
            // reached — see `compare_matrix`.
            other => unreachable!("gap/stale not valid as a single expected value: {other:?}"),
        }
    }

    fn matches(&self, got: &Val) -> bool {
        match (self, got) {
            (Val::Float(want), Val::Float(got)) => almost_equal(*want, *got),
            (Val::Hist(want, hint_set), Val::Hist(got, _)) => {
                histogram_almost_equal(want, got, *hint_set)
            }
            _ => false,
        }
    }
}

/// Upstream `compareNativeHistogram` (`promqltest/test.go:1401-1447`):
/// schema exact, count/sum/zero_count within `almost_equal` tolerance,
/// NHCB custom bounds exact, zero_threshold exact, bucket layout (spans +
/// per-bucket value) within tolerance — after `Compact(0)` on both sides,
/// exactly like the pin — and (issue #125) the `CounterResetHint` compared
/// EXACTLY iff `hint_set` (the expected literal spelled a
/// `counter_reset_hint:` key; otherwise "don't care", `test.go:1438-1445`).
fn histogram_almost_equal(
    expected: &FloatHistogram,
    actual: &FloatHistogram,
    hint_set: bool,
) -> bool {
    let mut want = expected.clone();
    let mut got = actual.clone();
    want.compact();
    got.compact();
    if want.schema != got.schema {
        return false;
    }
    if !almost_equal(want.count, got.count) || !almost_equal(want.sum, got.sum) {
        return false;
    }
    if want.uses_custom_buckets() && want.custom_values != got.custom_values {
        return false;
    }
    if want.zero_threshold != got.zero_threshold || !almost_equal(want.zero_count, got.zero_count) {
        return false;
    }
    if hint_set && want.counter_reset_hint != got.counter_reset_hint {
        return false;
    }
    want.negative_spans == got.negative_spans
        && buckets_almost_equal(&want.negative_buckets, &got.negative_buckets)
        && want.positive_spans == got.positive_spans
        && buckets_almost_equal(&want.positive_buckets, &got.positive_buckets)
}

fn buckets_almost_equal(want: &[f64], got: &[f64]) -> bool {
    want.len() == got.len() && want.iter().zip(got).all(|(w, g)| almost_equal(*w, *g))
}

fn expected_labels_vec(s: &ExpectedSeries) -> LabelsVec {
    // BTreeMap iterates sorted by key already.
    s.labels
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn expected_single_value(s: &ExpectedSeries) -> Val {
    match s.values.as_slice() {
        [v @ (SeqValue::Value(_) | SeqValue::Histogram(..))] => Val::from_seq(v),
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
    actual: &[(LabelsVec, Val)],
) -> (bool, String) {
    if expected.len() != actual.len() {
        return (
            false,
            format!(
                "series count mismatch: got {} ({}), want {} ({})",
                actual.len(),
                actual
                    .iter()
                    .map(|(l, v)| format!("{} {v:?}", fmt_labels(l)))
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
        let matches: Vec<&(LabelsVec, Val)> = actual.iter().filter(|(l, _)| *l == labels).collect();
        match matches.as_slice() {
            [] => {
                return (
                    false,
                    format!("expected series {} is absent", fmt_labels(&labels)),
                );
            }
            [(_, got)] => {
                if !want.matches(got) {
                    return (
                        false,
                        format!(
                            "value mismatch for {}: got {got:?}, want {want:?}",
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
    actual: &[(LabelsVec, Val)],
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
        if !want.matches(got) {
            return (
                false,
                format!(
                    "value mismatch at position {i} for {}: got {got:?}, want {want:?}",
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
    // Issue #124 (M7-A6): `RangeSeries.points: Vec<Point>` carries a
    // histogram channel too — projects each point to `(t_ms, Val)`
    // instead of the pre-A6 `(t_ms, f64)` (float-only queries stay
    // byte-identical: `Val::from_sample` reads `h: None` as `Val::Float`).
    let actual_by_labels: Vec<(LabelsVec, Vec<(i64, Val)>)> = actual
        .iter()
        .map(|s| {
            let mut labels: LabelsVec = s.labels.0.clone();
            if let Some(name) = &s.metric_name {
                labels.push(("__name__".to_string(), name.clone()));
            }
            labels.sort();
            (
                labels,
                s.points
                    .iter()
                    .map(|p| (p.t_ms, Val::from_sample(p.v, &p.h)))
                    .collect(),
            )
        })
        .collect();

    let mut matched_actual = vec![false; actual_by_labels.len()];
    for exp in expected {
        let labels = expected_labels_vec(exp);
        let want_points: Vec<(i64, Val)> = exp
            .values
            .iter()
            .enumerate()
            .filter_map(|(k, v)| match v {
                SeqValue::Value(_) | SeqValue::Histogram(..) => {
                    Some((from_ms + k as i64 * step_ms, Val::from_seq(v)))
                }
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
        let got_points = &actual_by_labels[idx].1;
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
            if gt != wt || !wv.matches(gv) {
                return (
                    false,
                    format!(
                        "point mismatch for {}: got ({gt}, {gv:?}), want ({wt}, {wv:?})",
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

#[cfg(test)]
mod tests {
    use super::format_expected_bytes;
    use super::histogram_almost_equal;
    use pulsus_model::{CounterResetHint, FloatHistogram};

    /// Issue #125 (AC2): the comparator asserts the counter-reset hint iff
    /// the expected literal SET one (the pin's `compareNativeHistogram(…,
    /// counterResetHintSet)`, `test.go:1438-1445`): a hint-set expectation
    /// with a mismatched actual fails; the identical values WITHOUT the
    /// flag pass (don't-care); a hint-set expectation with a MATCHING
    /// actual passes. Dropping the flag plumbing turns the middle
    /// assertion vacuous and fails the first.
    #[test]
    fn histogram_comparator_asserts_the_hint_only_when_the_literal_set_one() {
        let hist = |hint| FloatHistogram {
            counter_reset_hint: hint,
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 4.0,
            sum: 5.0,
            positive_spans: vec![],
            negative_spans: vec![],
            positive_buckets: vec![],
            negative_buckets: vec![],
            custom_values: vec![],
        };
        let want_gauge = hist(CounterResetHint::Gauge);
        let got_unknown = hist(CounterResetHint::Unknown);
        assert!(
            !histogram_almost_equal(&want_gauge, &got_unknown, true),
            "hint_set + mismatched hint must FAIL"
        );
        assert!(
            histogram_almost_equal(&want_gauge, &got_unknown, false),
            "no hint in the literal ⇒ don't-care, values equal ⇒ pass"
        );
        assert!(
            histogram_almost_equal(&want_gauge, &hist(CounterResetHint::Gauge), true),
            "hint_set + matching hint must pass"
        );
    }

    /// AC4 (issue #86): a UTF-8 `want` renders exactly today's `{:?}`
    /// form — unchanged by the byte-comparison fix (the non-UTF-8 branch
    /// is covered end-to-end by
    /// `promqltest_corpus::expect_string_with_non_utf8_literal_executes_as_a_classifiable_mismatch`).
    #[test]
    fn format_expected_bytes_keeps_the_debug_form_for_utf8_input() {
        assert_eq!(format_expected_bytes(b"Foo"), "\"Foo\"");
        assert_eq!(format_expected_bytes(b""), "\"\"");
    }

    /// A non-UTF-8 `want` falls back to a lossless `b"…"` escape_ascii
    /// form.
    #[test]
    fn format_expected_bytes_uses_escaped_byte_form_for_non_utf8_input() {
        assert_eq!(format_expected_bytes(&[0xff]), "b\"\\xff\"");
    }
}
