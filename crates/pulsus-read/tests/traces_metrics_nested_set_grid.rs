//! Issue #458 AC 3a: the accept surface of the `nestedSetParent` metrics
//! lowering, pinned as a REGION rather than as the handful of points a
//! plan happened to trip over.
//!
//! `nested_set_metrics_sql` is a total function of
//! `(NestedSetField, ComparisonOp, f64)`. This suite enumerates the whole
//! `ComparisonOp × value` grid — all **8** operator variants × **9**
//! boundary-straddling values = **72** cells — for each of the three
//! nested-set fields, and asserts the exact rendered SQL fragment (for
//! `nestedSetParent`) or the exact refusal body (for every other cell).
//!
//! **Why the value list is what it is.** `nonroot_constant` has two
//! boundaries, `-1` (the root sentinel, `nested_set_model.go:11-12` @
//! v3.0.2) and `1` (the smallest Euler `left` a parent can carry). The
//! list is those two boundaries, one point on each side of each (`-2`,
//! `-0.5`, `0.5`, `2`), zero, one value well inside the refusing region
//! (`5`), and `NaN`.
//!
//! # The compile-time completeness device, and the two conditions it
//! # needs to be true
//!
//! The expected outcome of every cell comes from [`apply`], a `match` on
//! `ComparisonOp` with **one arm per variant and no `_` wildcard arm**.
//! Adding a variant to `ComparisonOp` therefore fails to COMPILE here
//! rather than silently shrinking the region this suite claims to cover.
//! That claim holds only while two things stay true, and neither is
//! assertable at runtime, so they are written down instead:
//!
//! 1. **No `_` wildcard arm may be added to [`apply`] or [`op_ordinal`].**
//!    A wildcard compiles forever and makes the check decorative.
//! 2. **`ComparisonOp` must not carry `#[non_exhaustive]`.** That
//!    attribute moves the match out of the compiler's reach from outside
//!    the defining crate, and an exhaustive external match stops being
//!    possible at all.
//!
//! [`all_ops_is_exactly_the_comparison_op_variant_set`] closes the third
//! gap the compiler cannot: that the const array actually FED to the grid
//! lists every variant the exhaustive match knows about.
//!
//! # This is an oracle, not a mirror
//!
//! The expected non-root truth is **not** a copy of `nonroot_constant`'s
//! arm list. It is derived from what "decidable per span" MEANS: the
//! comparison is lowered only if its answer is the same for every value
//! the non-root domain can hold, so the expectation samples that domain
//! (`NONROOT_SAMPLES`) and reports `Some(v)` iff every sample agrees.
//! Re-spelling the implementation's arms here would make the suite agree
//! with any future mis-spelling of them.
//!
//! Hermetic: pure functions, no container, no corpus. Runs in the `ci`
//! job on every PR.

use std::collections::BTreeMap;

use pulsus_read::traces::PlanError;
use pulsus_read::traces::metrics_sql::{SnappedWindow, compile_filter_bool};
use pulsus_traceql::{ComparisonOp, Field, FieldExpr, FieldOp, Intrinsic, Value};

/// The window is irrelevant to a nested-set leaf (no date/time-pruned
/// subquery is emitted) but `compile_filter_bool` needs one.
const WINDOW: SnappedWindow = SnappedWindow {
    start_ns: 1_699_999_980_000_000_000,
    end_ns: 1_700_010_840_000_000_000,
};

const ATTRS_TABLE: &str = "trace_attrs_idx";

/// The root sentinel's SQL identity — `metrics_sql::is_root_sql`'s exact
/// spelling. Written out rather than imported so a change to it moves a
/// pinned byte here (issue #458 AC 3a / AC 5).
const ROOT_SQL: &str = "parent_id = toFixedString(unhex('0000000000000000'), 8)";

const REGEX_ERR: &str = "nested-set intrinsics do not support regex operators";
const NONFINITE_ERR: &str = "not a finite number: \"NaN\"";
const NUMBERING_ERR: &str = "nestedSetParent comparisons inside the numbering range are not \
                             supported in metrics filters";
const GUARD_ERR: &str = "nestedSetLeft and nestedSetRight are not supported in metrics filters";

/// Every `ComparisonOp` variant, in declaration order. Kept honest by
/// [`all_ops_is_exactly_the_comparison_op_variant_set`], which maps this
/// array through an exhaustive `match` and fails if the two disagree.
const ALL_OPS: [ComparisonOp; 8] = [
    ComparisonOp::Eq,
    ComparisonOp::Neq,
    ComparisonOp::Gt,
    ComparisonOp::Gte,
    ComparisonOp::Lt,
    ComparisonOp::Lte,
    ComparisonOp::Re,
    ComparisonOp::Nre,
];

/// The nine boundary-straddling values, with the literal spelling each is
/// fed to the AST as.
const VALUES: [(&str, f64); 9] = [
    ("-2", -2.0),
    ("-1", -1.0),
    ("-0.5", -0.5),
    ("0", 0.0),
    ("0.5", 0.5),
    ("1", 1.0),
    ("2", 2.0),
    ("5", 5.0),
    ("NaN", f64::NAN),
];

/// A dense sample of the non-root domain `[1, ∞)`. Every value in
/// [`VALUES`] that a span could equal is present, so an `=`/`!=` cell can
/// never be mistaken for constant.
const NONROOT_SAMPLES: [f64; 12] = [
    1.0,
    2.0,
    3.0,
    4.0,
    5.0,
    6.0,
    7.0,
    8.0,
    9.0,
    10.0,
    1_000_000.0,
    1e18,
];

/// One numeric comparison, or `None` when the operator is not a numeric
/// comparison at all.
///
/// **Exhaustive by construction — one arm per `ComparisonOp` variant, no
/// wildcard.** This is the compile-time device the module doc describes.
fn apply(op: ComparisonOp, lhs: f64, rhs: f64) -> Option<bool> {
    match op {
        ComparisonOp::Eq => Some(lhs == rhs),
        ComparisonOp::Neq => Some(lhs != rhs),
        ComparisonOp::Gt => Some(lhs > rhs),
        ComparisonOp::Gte => Some(lhs >= rhs),
        ComparisonOp::Lt => Some(lhs < rhs),
        ComparisonOp::Lte => Some(lhs <= rhs),
        ComparisonOp::Re => None,
        ComparisonOp::Nre => None,
    }
}

/// A distinct ordinal per variant. **Exhaustive, no wildcard** — the
/// second half of the completeness device: `apply` proves every variant
/// has an expected outcome, this proves [`ALL_OPS`] actually feeds every
/// variant into the grid.
fn op_ordinal(op: ComparisonOp) -> usize {
    match op {
        ComparisonOp::Eq => 0,
        ComparisonOp::Neq => 1,
        ComparisonOp::Gt => 2,
        ComparisonOp::Gte => 3,
        ComparisonOp::Lt => 4,
        ComparisonOp::Lte => 5,
        ComparisonOp::Re => 6,
        ComparisonOp::Nre => 7,
    }
}

/// The non-root domain's constant truth for `x OP v`, derived from the
/// MEANING of decidability rather than from the implementation's arms:
/// `Some(b)` iff every sampled non-root `x` gives `b`.
fn expected_nonroot_constant(op: ComparisonOp, v: f64) -> Option<bool> {
    let mut seen: Option<bool> = None;
    for x in NONROOT_SAMPLES {
        let got = apply(op, x, v)?;
        match seen {
            None => seen = Some(got),
            Some(prev) if prev == got => {}
            Some(_) => return None,
        }
    }
    seen
}

/// What the compiler must produce for one cell.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Cell {
    Sql(String),
    Err(String),
}

impl Cell {
    fn err(msg: &str) -> Self {
        Cell::Err(msg.to_string())
    }
}

/// Which nested-set intrinsic a column of the grid is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NsField {
    Parent,
    Left,
    Right,
}

impl NsField {
    fn intrinsic(self) -> Intrinsic {
        match self {
            NsField::Parent => Intrinsic::NestedSetParent,
            NsField::Left => Intrinsic::NestedSetLeft,
            NsField::Right => Intrinsic::NestedSetRight,
        }
    }

    fn name(self) -> &'static str {
        match self {
            NsField::Parent => "nestedSetParent",
            NsField::Left => "nestedSetLeft",
            NsField::Right => "nestedSetRight",
        }
    }
}

/// The expected outcome of one cell, in the order the three rejection
/// sites actually fire on the real path:
///
/// 1. `filter::compile_nested_set_leaf` rejects a regex operator before it
///    looks at the value at all;
/// 2. `filter::parse_num` rejects a non-finite literal;
/// 3. `metrics_sql::nested_set_metrics_sql` applies the field guard, then
///    the decidability test.
fn expected(field: NsField, op: ComparisonOp, v: f64) -> Cell {
    let Some(root_true) = apply(op, -1.0, v) else {
        return Cell::err(REGEX_ERR);
    };
    if !v.is_finite() {
        return Cell::err(NONFINITE_ERR);
    }
    if field != NsField::Parent {
        return Cell::err(GUARD_ERR);
    }
    let Some(nonroot_true) = expected_nonroot_constant(op, v) else {
        return Cell::err(NUMBERING_ERR);
    };
    Cell::Sql(match (root_true, nonroot_true) {
        (true, true) => "1".to_string(),
        (false, false) => "0".to_string(),
        (true, false) => ROOT_SQL.to_string(),
        (false, true) => format!("NOT ({ROOT_SQL})"),
    })
}

/// What the tree under test produces for one cell.
fn actual(field: NsField, op: ComparisonOp, raw: &str) -> Cell {
    let expr = FieldExpr::Binary {
        op: FieldOp::Cmp(op),
        lhs: Box::new(FieldExpr::Field(Field::Intrinsic(field.intrinsic()))),
        rhs: Box::new(FieldExpr::Literal(Value::Number(raw.to_string()))),
    };
    match compile_filter_bool(Some(&expr), ATTRS_TABLE, WINDOW) {
        Ok(sql) => Cell::Sql(sql),
        Err(PlanError::TypeMismatch(msg)) => Cell::Err(msg),
        Err(other) => panic!("{} {op} {raw}: unexpected error {other:?}", field.name()),
    }
}

#[test]
fn all_ops_is_exactly_the_comparison_op_variant_set() {
    let mut ordinals: Vec<usize> = ALL_OPS.iter().copied().map(op_ordinal).collect();
    ordinals.sort_unstable();
    ordinals.dedup();
    assert_eq!(
        ordinals,
        (0..ALL_OPS.len()).collect::<Vec<_>>(),
        "ALL_OPS must list every ComparisonOp variant exactly once — op_ordinal is exhaustive, \
         so a variant added to the enum without being added here shows up as a missing ordinal"
    );
}

/// AC 3a. All 72 cells, for all three nested-set fields.
#[test]
fn the_nested_set_metrics_grid_is_pinned_cell_for_cell() {
    let mut mismatches = Vec::new();
    let mut per_field: BTreeMap<&str, BTreeMap<String, usize>> = BTreeMap::new();

    for field in [NsField::Parent, NsField::Left, NsField::Right] {
        let histogram = per_field.entry(field.name()).or_default();
        for op in ALL_OPS {
            for (raw, v) in VALUES {
                let want = expected(field, op, v);
                let got = actual(field, op, raw);
                let bucket = match &want {
                    Cell::Sql(sql) => format!("sql:{sql}"),
                    Cell::Err(msg) => format!("err:{msg}"),
                };
                *histogram.entry(bucket).or_default() += 1;
                if want != got {
                    mismatches.push(format!(
                        "{{ {} {op} {raw} }}: want {want:?}, got {got:?}",
                        field.name()
                    ));
                }
            }
        }
    }

    assert!(
        mismatches.is_empty(),
        "{} of {} cells drifted:\n{}",
        mismatches.len(),
        3 * ALL_OPS.len() * VALUES.len(),
        mismatches.join("\n")
    );

    // The region's SHAPE, so a change that keeps every cell internally
    // consistent but moves the boundary still reddens.
    let parent = &per_field["nestedSetParent"];
    let accepted: usize = parent
        .iter()
        .filter(|(k, _)| k.starts_with("sql:"))
        .map(|(_, n)| *n)
        .sum();
    assert_eq!(
        accepted, 32,
        "accepting cells for nestedSetParent: {parent:?}"
    );
    assert_eq!(
        parent.get(&format!("sql:{ROOT_SQL}")).copied(),
        Some(9),
        "`is_root` cells: {parent:?}"
    );
    assert_eq!(
        parent.get(&format!("sql:NOT ({ROOT_SQL})")).copied(),
        Some(9),
        "`NOT is_root` cells: {parent:?}"
    );
    assert_eq!(
        parent.get("sql:1").copied(),
        Some(7),
        "constant-true cells: {parent:?}"
    );
    assert_eq!(
        parent.get("sql:0").copied(),
        Some(7),
        "constant-false cells: {parent:?}"
    );
    assert_eq!(
        parent.get(&format!("err:{REGEX_ERR}")).copied(),
        Some(18),
        "regex cells (rejected at compile_nested_set_leaf, a different site with a different \
         body): {parent:?}"
    );
    assert_eq!(
        parent.get(&format!("err:{NONFINITE_ERR}")).copied(),
        Some(6),
        "non-finite cells (rejected at parse_num, earlier still): {parent:?}"
    );
    assert_eq!(
        parent.get(&format!("err:{NUMBERING_ERR}")).copied(),
        Some(16),
        "undecidable-region cells: {parent:?}"
    );

    // Every non-parent field refuses all 72, and the 32 that would have
    // been accepted are exactly the ones the field guard is there to stop.
    for name in ["nestedSetLeft", "nestedSetRight"] {
        let other = &per_field[name];
        assert_eq!(
            other.get(&format!("err:{GUARD_ERR}")).copied(),
            Some(48),
            "{name}: the guard body covers everything the two earlier sites do not: {other:?}"
        );
        assert_eq!(
            other.get(&format!("err:{REGEX_ERR}")).copied(),
            Some(18),
            "{name}: regex cells keep the regex body: {other:?}"
        );
        assert_eq!(
            other.get(&format!("err:{NONFINITE_ERR}")).copied(),
            Some(6),
            "{name}: non-finite cells keep the parse_num body: {other:?}"
        );
        assert!(
            other.keys().all(|k| k.starts_with("err:")),
            "{name}: no cell may be accepted: {other:?}"
        );
    }
}

/// Prints the grid, for the record. `#[ignore]`d — it asserts nothing.
#[test]
#[ignore = "prints the 72-cell grid; run explicitly"]
fn print_the_grid() {
    for field in [NsField::Parent, NsField::Left, NsField::Right] {
        println!("== {} ==", field.name());
        print!("{:<4}", "op");
        for (raw, _) in VALUES {
            print!("{raw:>12}");
        }
        println!();
        for op in ALL_OPS {
            print!("{op:<4}");
            for (raw, _) in VALUES {
                let label = match actual(field, op, raw) {
                    Cell::Sql(sql) if sql == ROOT_SQL => "is_root".to_string(),
                    Cell::Sql(sql) if sql == format!("NOT ({ROOT_SQL})") => "!is_root".to_string(),
                    Cell::Sql(sql) => sql,
                    Cell::Err(msg) if msg == REGEX_ERR => "Err/regex".to_string(),
                    Cell::Err(msg) if msg == NONFINITE_ERR => "Err/nonfin".to_string(),
                    Cell::Err(msg) if msg == NUMBERING_ERR => "Err/numb".to_string(),
                    Cell::Err(_) => "Err/guard".to_string(),
                };
                print!("{label:>12}");
            }
            println!();
        }
    }
}
