//! **A budget rejection must not preempt a decidable semantic error**
//! (issue #290) — the rows that check it at the seam a client reaches.
//!
//! Before this change `combine_binary_capped` reserved
//! `binary_peak_bytes` before `instant_join` ran, so an ambiguous join
//! under a saturated budget was answered `422 query_too_broad` /
//! `MetricPostAggBytes` where the reference answers `400 bad_data` /
//! "many-to-one matching must be explicit". Same status family, wrong
//! reason payload, wrong remediation: the client shortens their time
//! range and never touches the line that is wrong.
//!
//! Every row here drives `combine_binary_capped` with a cap the stage
//! charge cannot possibly clear — `cap = 0`, and for the shape rows a
//! caller counter already at `u64::MAX - 1` — and asserts the SEMANTIC
//! error comes back. The mechanism that makes that true is structural
//! and lives in the source: `decide_binary` moves both operands in, and
//! `Ledger::acquire_binary` is the only way to get them back. These rows
//! are what proves the structure produces the behaviour, at the layer
//! the user experiences (#335's standing lesson: a gate below the user
//! reports agreement exactly where divergence lives).
//!
//! The in-crate side of this issue — the 491 520-case differential
//! against the join itself, the point-read counter, the scratch model
//! and the skip path — is in `src/logql/post_agg.rs`'s `mod tests`,
//! because its oracle needs `Ledger::for_test`.

use pulsus_logql::{BinOp, MatchGroup, VectorMatching};
use pulsus_read::logql::{
    MAX_BINARY_PREFLIGHT_BYTES, MAX_POST_AGG_BYTES, MatrixSeries, PREFLIGHT_BYTES_PER_SERIES,
    PREFLIGHT_FLAT_BYTES, QueryResult, ReadError, TooBroadReason, VectorSample, binary_peak_bytes,
    combine_binary, combine_binary_capped, measure_vector, preflight_scratch_bytes,
};

fn labels(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

fn sample(pairs: &[(&str, &str)], value: f64) -> VectorSample {
    VectorSample {
        labels: labels(pairs),
        value,
    }
}

fn series(pairs: &[(&str, &str)], points: &[i64]) -> MatrixSeries {
    MatrixSeries {
        labels: labels(pairs),
        points: points.iter().map(|&t| (t, 1.0)).collect(),
    }
}

fn on(names: &[&str], group: Option<MatchGroup>) -> VectorMatching {
    VectorMatching {
        on: true,
        labels: names.iter().map(|n| (*n).to_string()).collect(),
        group,
    }
}

fn ignoring(names: &[&str], group: Option<MatchGroup>) -> VectorMatching {
    VectorMatching {
        on: false,
        labels: names.iter().map(|n| (*n).to_string()).collect(),
        group,
    }
}

fn reason(err: ReadError) -> String {
    match err {
        ReadError::PipelineInvalid { reason } => reason,
        other => panic!(
            "expected the semantic 400 (`PipelineInvalid`), got {other:?} — a budget refusal \
             preempted a decidable semantic error, which is exactly issue #290"
        ),
    }
}

const DUPLICATE_ONE_SIDE: &str = "found duplicate series on the right hand-side;many-to-many \
                                  matching not allowed: matching labels must be unique on one \
                                  side";
const MULTIPLE_MATCHES: &str = "multiple matches for labels: many-to-one matching must be explicit \
     (group_left/group_right)";
const GROUPING_UNIQUE: &str = "multiple matches for labels: grouping labels must ensure unique \
                               matches";
const SET_OP_SCALAR_DIV: &str = "unexpected literal for a leg of logical/set binary operation \
                                 (and): set operations are defined between vectors only";
const INCOMPATIBLE_TYPES: &str = "binary operation over incompatible result types";

/// The six pre-committed join-refusal fixtures — AC 3 (a)–(f).
/// `(name, op, matching, lhs, rhs, expected message)`.
#[allow(clippy::type_complexity)]
fn join_refusal_rows() -> Vec<(
    &'static str,
    BinOp,
    Option<VectorMatching>,
    QueryResult,
    QueryResult,
    &'static str,
)> {
    vec![
        (
            // (a) a LATER matrix step carries the duplicate: the first
            // non-empty step is a valid one-to-one join.
            "matrix, duplicate at a later step",
            BinOp::Div,
            Some(on(&["x"], Some(MatchGroup::Left(Vec::new())))),
            QueryResult::Matrix(vec![series(&[("m", "a"), ("x", "1")], &[1, 2])]),
            QueryResult::Matrix(vec![
                series(&[("x", "1"), ("z", "p")], &[1, 2]),
                series(&[("x", "1"), ("z", "q")], &[2]),
            ]),
            DUPLICATE_ONE_SIDE,
        ),
        (
            // (b) the earliest steps are EMPTY on one side, so
            // `instant_join`'s `:372` short-circuit returns before the
            // one-side index is built — the case that defeated the
            // first design.
            "matrix, empty leading steps on one side",
            BinOp::Div,
            Some(on(&["x"], Some(MatchGroup::Left(Vec::new())))),
            QueryResult::Matrix(vec![series(&[("m", "a"), ("x", "1")], &[1, 2])]),
            QueryResult::Matrix(vec![
                series(&[("x", "1"), ("z", "p")], &[2]),
                series(&[("x", "1"), ("z", "q")], &[2]),
            ]),
            DUPLICATE_ONE_SIDE,
        ),
        (
            // (c) the vector one-virtual-step form of the same shape.
            "vector, duplicate one side",
            BinOp::Div,
            Some(on(&["x"], Some(MatchGroup::Left(Vec::new())))),
            QueryResult::Vector(vec![sample(&[("m", "a"), ("x", "1")], 10.0)]),
            QueryResult::Vector(vec![
                sample(&[("x", "1"), ("z", "p")], 2.0),
                sample(&[("x", "1"), ("z", "q")], 3.0),
            ]),
            DUPLICATE_ONE_SIDE,
        ),
        (
            // (d) an EMPTY include list — `group_left([])`, so the
            // include amplifier contributes nothing at all. Two
            // identical many-side series collapse onto one grouped
            // output identity.
            "grouped join with an empty include list",
            BinOp::Div,
            Some(on(&["x"], Some(MatchGroup::Left(Vec::new())))),
            QueryResult::Vector(vec![
                sample(&[("x", "1"), ("y", "p")], 10.0),
                sample(&[("x", "1"), ("y", "p")], 20.0),
            ]),
            QueryResult::Vector(vec![sample(&[("x", "1")], 2.0)]),
            GROUPING_UNIQUE,
        ),
        (
            // (e) one-to-one: two many-side series share the `on(x)`
            // signature.
            "one-to-one, multiple matches",
            BinOp::Div,
            Some(on(&["x"], None)),
            QueryResult::Vector(vec![
                sample(&[("a", "1"), ("x", "1")], 10.0),
                sample(&[("a", "2"), ("x", "1")], 20.0),
            ]),
            QueryResult::Vector(vec![sample(&[("x", "1")], 2.0)]),
            MULTIPLE_MATCHES,
        ),
        (
            // (f) the include-copy collapse of
            // `group_left_include_collapsing_distinct_many_labels_is_grouping_unique_error`
            // (tests/logql_metric_agg_golden.rs:2549), oracle-pinned
            // byte-identical against `grafana/loki:3.4.2`. The one side
            // has no `y`, so copying the `y` include DROPS it from both
            // distinct many-side series.
            "grouped join, include copy collapses two distinct many series",
            BinOp::Div,
            Some(ignoring(
                &["y"],
                Some(MatchGroup::Left(vec!["y".to_string()])),
            )),
            QueryResult::Vector(vec![
                sample(&[("x", "1"), ("y", "p")], 10.0),
                sample(&[("x", "1"), ("y", "q")], 20.0),
            ]),
            QueryResult::Vector(vec![sample(&[("x", "1")], 2.0)]),
            GROUPING_UNIQUE,
        ),
    ]
}

/// **AC 3 — the join's own refusal survives a budget that cannot admit
/// one byte.** `cap = 0` is the tightest possible saturation: before this
/// change every one of these six returned
/// `QueryTooBroad(MetricPostAggBytes)` and a 422.
#[test]
fn a_saturated_budget_does_not_preempt_the_join_refusals() {
    for (name, op, matching, lhs, rhs, want) in join_refusal_rows() {
        let mut charged = 0u64;
        let err = combine_binary_capped(&mut charged, op, false, matching.as_ref(), lhs, rhs, 0)
            .expect_err(name);
        assert_eq!(reason(err), want, "{name}");
        assert_eq!(charged, 0, "{name}: the error path must charge nothing");
    }
}

/// **AC 3c — a caller's spending cannot make a semantic error
/// unreachable.** The same six rows with the counter already at
/// `u64::MAX - 1` return the identical 400 — the composition hazard a
/// shared query-scoped counter (#260) would otherwise reintroduce.
///
/// The mechanism has two halves, and only one of them is "reads neither
/// the counter nor the cap":
///
/// * `PreflightCharge::acquire` takes no `charged` and no `cap`. The
///   preflight's own SCRATCH admission is a function of the operands'
///   counts and its own ceiling alone, so an unrelated caller's spending
///   can never make the preflight unaffordable (plan v5 review's
///   `[high]`).
/// * `decide_binary`'s guard DOES read them, in the one direction that
///   cannot hide an error: it skips (P1) only when the charge about to be
///   levied would admit, and an admitted charge refuses nothing for the
///   join's refusal to be preempted by. More spending moves the guard
///   towards running the preflight, never away from it — which is why an
///   exhausted counter is the case that runs it, as these rows do.
#[test]
fn the_join_refusals_survive_a_counter_that_is_already_exhausted() {
    for (name, op, matching, lhs, rhs, want) in join_refusal_rows() {
        let mut charged = u64::MAX - 1;
        let err = combine_binary_capped(&mut charged, op, false, matching.as_ref(), lhs, rhs, 0)
            .expect_err(name);
        assert_eq!(reason(err), want, "{name}");
        assert_eq!(
            charged,
            u64::MAX - 1,
            "{name}: the error path must not mutate the caller's counter"
        );
    }
}

/// **AC 1 — the three scalar set-operation arms.** `Scalar ⊗ Scalar`,
/// `Scalar ⊗ vector` and `vector ⊗ Scalar` under a set operator are
/// class (P0): deciding them needs no input-scaled scratch (the refusal
/// message itself is built, as every semantic refusal in this crate is,
/// under no charge at all), so they run above EVERY charge — the
/// preflight's own included, and its guard's too. The message is
/// asserted equal to the one `combine_binary` returns at the production
/// cap, so a saturated budget cannot change the answer, only the timing.
#[test]
fn the_scalar_set_operation_refusals_survive_a_saturated_budget() {
    let cases: Vec<(&str, QueryResult, QueryResult)> = vec![
        (
            "scalar ⊗ scalar",
            QueryResult::Scalar(1.0),
            QueryResult::Scalar(2.0),
        ),
        (
            "scalar ⊗ vector",
            QueryResult::Scalar(1.0),
            QueryResult::Vector(vec![sample(&[("x", "1")], 2.0)]),
        ),
        (
            "vector ⊗ scalar",
            QueryResult::Vector(vec![sample(&[("x", "1")], 2.0)]),
            QueryResult::Scalar(1.0),
        ),
        (
            "matrix ⊗ scalar",
            QueryResult::Matrix(vec![series(&[("x", "1")], &[1])]),
            QueryResult::Scalar(1.0),
        ),
    ];
    for (name, lhs, rhs) in cases {
        let unbudgeted = reason(
            combine_binary(BinOp::And, false, None, lhs.clone(), rhs.clone()).expect_err(name),
        );
        assert_eq!(unbudgeted, SET_OP_SCALAR_DIV, "{name}");

        for (cap, mut charged) in [(1u64, 0u64), (0, 0), (0, u64::MAX - 1)] {
            let before = charged;
            let err = combine_binary_capped(
                &mut charged,
                BinOp::And,
                false,
                None,
                lhs.clone(),
                rhs.clone(),
                cap,
            )
            .expect_err(name);
            assert_eq!(reason(err), unbudgeted, "{name} at cap {cap}");
            assert_eq!(charged, before, "{name}: the error path charged");
        }
    }
}

/// **AC 2 — the incompatible-types arm**, same shape. `Vector ⊗ Matrix`
/// and a `String` operand are structurally impossible under one
/// `QuerySpec`, so this is defence in depth — but it is a class-(P0)
/// refusal all the same, and a budget must not answer for it.
#[test]
fn the_incompatible_types_refusal_survives_a_saturated_budget() {
    let cases: Vec<(&str, QueryResult, QueryResult)> = vec![
        (
            "vector ⊗ matrix",
            QueryResult::Vector(vec![sample(&[("x", "1")], 2.0)]),
            QueryResult::Matrix(vec![series(&[("x", "1")], &[1])]),
        ),
        (
            "string ⊗ vector",
            QueryResult::String("s".to_string()),
            QueryResult::Vector(vec![sample(&[("x", "1")], 2.0)]),
        ),
        (
            "streams ⊗ scalar",
            QueryResult::Streams {
                items: Vec::new(),
                partial: false,
            },
            QueryResult::Scalar(1.0),
        ),
    ];
    for (name, lhs, rhs) in cases {
        let unbudgeted = reason(
            combine_binary(BinOp::Div, false, None, lhs.clone(), rhs.clone()).expect_err(name),
        );
        assert_eq!(unbudgeted, INCOMPATIBLE_TYPES, "{name}");
        for (cap, mut charged) in [(1u64, 0u64), (0, 0), (0, u64::MAX - 1)] {
            let before = charged;
            let err = combine_binary_capped(
                &mut charged,
                BinOp::Div,
                false,
                None,
                lhs.clone(),
                rhs.clone(),
                cap,
            )
            .expect_err(name);
            assert_eq!(reason(err), unbudgeted, "{name} at cap {cap}");
            assert_eq!(charged, before, "{name}: the error path charged");
        }
    }
}

/// **AC 4 — the total charged is unchanged, pinned from BOTH sides.**
///
/// `Ledger::Drop` discharges on return, so the caller-visible counter is
/// always back to zero and cannot be read as the total. The observable
/// identity that can: for a join that COMPLETES while carrying an
/// include list, `cap = T` admits and `cap = T - 1` refuses reporting
/// `bytes = T`, with `T = binary_peak_bytes(...)` computed from the
/// public helpers. Issue #290 changes only WHEN the semantic decision
/// happens, never how much the stage is charged, and this is the pair
/// that says so.
#[test]
fn the_stage_charge_is_still_exactly_the_modelled_peak() {
    let matching = on(&["x"], Some(MatchGroup::Left(vec!["y".to_string()])));
    let many = vec![sample(&[("x", "1"), ("m", "a")], 10.0)];
    let one = vec![sample(&[("x", "1"), ("y", "value-of-y")], 2.0)];
    let (lm, rm) = (measure_vector(&many), measure_vector(&one));
    let t = binary_peak_bytes(BinOp::Div, Some(&matching), &lm, &rm);
    assert!(t > 0, "the fixture must model a nonzero envelope");

    let mut charged = 0u64;
    let out = combine_binary_capped(
        &mut charged,
        BinOp::Div,
        false,
        Some(&matching),
        QueryResult::Vector(many.clone()),
        QueryResult::Vector(one.clone()),
        t,
    )
    .expect("cap = T must admit");
    match out {
        QueryResult::Vector(items) => assert_eq!(items.len(), 1),
        other => panic!("expected a vector, got {other:?}"),
    }
    assert_eq!(charged, 0, "the ledger must discharge on success");

    let mut charged = 0u64;
    let err = combine_binary_capped(
        &mut charged,
        BinOp::Div,
        false,
        Some(&matching),
        QueryResult::Vector(many),
        QueryResult::Vector(one),
        t - 1,
    )
    .expect_err("cap = T - 1 must refuse");
    match err {
        ReadError::QueryTooBroad(TooBroadReason::MetricPostAggBytes { bytes, cap }) => {
            assert_eq!(bytes, t, "the refusal must report the modelled peak");
            assert_eq!(cap, t - 1);
        }
        other => panic!("expected MetricPostAggBytes, got {other:?}"),
    }
    assert_eq!(charged, 0, "a refused acquire must not mutate the counter");
}

/// **AC 9 — the rule is where a designer of the next budgeted seam meets
/// it.** `docs/architecture.md` §5.6 exists and states both disciplines,
/// and the two module docs a LogQL or traces cap author already reads
/// point at it. A pointer to a section that does not exist is worse than
/// no pointer, so both halves are asserted together.
#[test]
fn the_two_budget_disciplines_are_stated_where_a_designer_meets_them() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let arch = std::fs::read_to_string(root.join("docs/architecture.md"))
        .expect("read docs/architecture.md");
    assert!(
        arch.contains("### 5.6 Budgeted seams: two disciplines"),
        "docs/architecture.md has no §5.6 heading — the two pointers below resolve to nothing"
    );
    for needle in [
        "Charge before you allocate",
        "Rule out meaning before you refuse for resources",
        "**(P) — decidable from the inputs.**",
        "**(P0)**",
        "**(A) — reachable only during allocation.**",
        "the preflight is SKIPPED, not refused",
        "metrics/exec.rs",
        "post_agg",
    ] {
        assert!(
            arch.contains(needle),
            "docs/architecture.md §5.6 does not state `{needle}`"
        );
    }

    for (path, what) in [
        (
            "crates/pulsus-read/src/logql/charge.rs",
            "the LogQL cap hub",
        ),
        (
            "crates/pulsus-read/src/traces/exec.rs",
            "the traces byte budget",
        ),
    ] {
        let src =
            std::fs::read_to_string(root.join(path)).unwrap_or_else(|e| panic!("{path}: {e}"));
        assert!(
            src.contains("docs/architecture.md §5.6"),
            "{path} ({what}) does not point at the two disciplines"
        );
    }
}

/// **AC 25 — the frozen O7 amplifier witness is PREFLIGHTED, not
/// skipped.** `both_amplifiers_are_refused_end_to_end_from_query_text`
/// must pass unmodified after issue #290, and it does; but "it still
/// refuses" would be equally true if the preflight had silently skipped
/// on it, which would make it evidence of nothing. Its O7 leg drives 256
/// many-side series against 256 one-side ones — this is the arithmetic
/// that says the preflight ran on that shape and was transparent.
///
/// Two conditions have to hold for "the preflight ran", and both are
/// asserted here:
///
/// * the scratch fits under `MAX_BINARY_PREFLIGHT_BYTES`, so
///   `PreflightCharge::acquire` does not skip it, and
/// * the stage charge would REFUSE, so `decide_binary`'s guard does not
///   skip it either. That is the same condition the frozen witness
///   itself asserts (`bytes > cap` on its O7 leg), restated here over the
///   modelled envelope so this test does not depend on reading it.
///
/// It is also the only place a passing preflight's transparency is
/// asserted against a FROZEN artifact rather than against the
/// differential's own fixtures.
#[test]
fn the_frozen_o7_amplifier_witness_is_inside_the_preflight_ceiling() {
    let many: Vec<VectorSample> = (0..256u64)
        .map(|i| sample(&[("id00", &format!("{i:08}"))], 1.0))
        .collect();
    let one: Vec<VectorSample> = (0..256u64)
        .map(|i| VectorSample {
            labels: vec![
                ("id00".to_string(), format!("{i:08}")),
                ("wide".to_string(), "w".repeat(1_000)),
            ],
            value: 1.0,
        })
        .collect();
    let (lm, rm) = (measure_vector(&many), measure_vector(&one));
    assert_eq!(lm.series() + rm.series(), 512);
    let charge = preflight_scratch_bytes(&lm, &rm);
    assert_eq!(
        charge,
        PREFLIGHT_BYTES_PER_SERIES * 512 + PREFLIGHT_FLAT_BYTES
    );
    assert!(
        charge <= MAX_BINARY_PREFLIGHT_BYTES,
        "the O7 witness would SKIP the preflight ({charge} B of scratch against a \
         {MAX_BINARY_PREFLIGHT_BYTES} B ceiling) — it would then prove nothing about a passing \
         preflight being transparent"
    );
    // And the stage charge the witness levies REFUSES, so the guard runs
    // the preflight rather than short-circuiting it. The include list is
    // the witness's own: 6 000 names, priced against a one side whose
    // widest value is 1 000 bytes.
    let matching = on(
        &["id00"],
        Some(MatchGroup::Left(
            (0..6_000u32).map(|i| format!("i{i:05}")).collect(),
        )),
    );
    let modelled = binary_peak_bytes(BinOp::Mul, Some(&matching), &lm, &rm);
    assert!(
        modelled > MAX_POST_AGG_BYTES,
        "the O7 witness would SKIP the preflight through the guard: its modelled envelope \
         {modelled} B is inside the {MAX_POST_AGG_BYTES} B cap, so the charge would admit and \
         there would be nothing for the preflight to get in front of"
    );

    // And its one side is unique on the matching labels, so the
    // preflight passes rather than refusing: 256 distinct `id00` values
    // on each side.
    let mut ids: Vec<&str> = one.iter().map(|s| s.labels[0].1.as_str()).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(
        ids.len(),
        256,
        "the O7 witness's one side must be unique on `id00`"
    );
}
