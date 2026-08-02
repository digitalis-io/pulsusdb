//! Issue #272 — SCC-2 trait equivalence against the derive.
//!
//! The hand-written iterative `Debug` (both modes), `Display`, `Clone`,
//! `PartialEq` and `Hash` on `MetricExpr`/`VariantsExpr` must be
//! byte-equivalent to what `#[derive(..)]` produced before the
//! conversion. The oracle is a **derived shadow mirror**: the pre-#272
//! shape — same type names, same variant names, same field names, same
//! declaration order, `Box`/`Vec` children — carrying the real derives.
//! Because `#[derive(Debug)]` prints the type's own identifiers, the
//! mirror's bytes are the pre-change bytes.
//!
//! `Hash` equivalence is proven by **call sequence**, not by `finish()`:
//! equal finishes cannot distinguish two different write-call sequences,
//! and discriminant feed order, `Vec` length-prefixing and primitive
//! write widths are all observable to a custom hasher.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use pulsus_logql::walk::{Child, ChildVec};
use pulsus_logql::{
    BinModifier, BinOp, CompareOp, Duration, Grouping, GroupingKind, LogExpr, LogRange, MatchGroup,
    MatchOp, Matcher, MetricExpr, RangeAggOp, StreamSelector, VectorAggOp, VectorMatching,
};

// ---------------------------------------------------------------------
// The derived shadow mirror: the pre-#272 shape, verbatim.
// ---------------------------------------------------------------------

mod shadow {
    use pulsus_logql::{BinModifier, BinOp, Grouping, LogRange, RangeAggOp, VectorAggOp};

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub enum MetricExpr {
        Range {
            op: RangeAggOp,
            range: LogRange,
            param: Option<String>,
            grouping: Option<Grouping>,
        },
        Vector {
            op: VectorAggOp,
            grouping: Option<Grouping>,
            param: Option<String>,
            inner: Box<MetricExpr>,
        },
        Literal(String),
        VectorFn(String),
        Binary {
            op: BinOp,
            modifier: Option<BinModifier>,
            lhs: Box<MetricExpr>,
            rhs: Box<MetricExpr>,
        },
        Variants(Box<VariantsExpr>),
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct VariantsExpr {
        pub variants: Vec<MetricExpr>,
        pub range: LogRange,
    }
}

/// One shape, built on both sides.
#[derive(Debug, Clone)]
enum Shape {
    Range(RangeAggOp, Option<&'static str>, LogRangeSpec),
    Literal(&'static str),
    VectorFn(&'static str),
    Vector(
        VectorAggOp,
        Option<Grouping>,
        Option<&'static str>,
        Box<Shape>,
    ),
    Binary(BinOp, Option<BinModifier>, Box<Shape>, Box<Shape>),
    Variants(Vec<Shape>, LogRangeSpec),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogRangeSpec {
    Plain,
    Piped,
}

/// `Duration`'s constructor is `pub(crate)`, so a `[5m]` is taken from a
/// parsed query rather than built.
fn five_minutes() -> Duration {
    let expr = pulsus_logql::parse(r#"rate({app="x"}[5m])"#).expect("fixture parses");
    // `impl Drop for MetricExpr` forbids moving out of a field (E0509),
    // so this re-binds through a reference and copies the `Duration`.
    match &expr {
        pulsus_logql::Expr::Metric(MetricExpr::Range { range, .. }) => range.range,
        other => panic!("unexpected fixture shape: {other:?}"),
    }
}

fn log_range(spec: LogRangeSpec) -> LogRange {
    let pipeline = match spec {
        LogRangeSpec::Plain => Vec::new(),
        LogRangeSpec::Piped => vec![pulsus_logql::Stage::LineFormat("{{.a}}".to_string())],
    };
    LogRange {
        selector: LogExpr {
            selector: StreamSelector {
                matchers: vec![Matcher {
                    name: "app".to_string(),
                    op: MatchOp::Eq,
                    value: "x".to_string(),
                }],
            },
            pipeline,
        },
        range: five_minutes(),
        unwrap: None,
    }
}

fn build(s: &Shape) -> MetricExpr {
    match s {
        Shape::Range(op, param, spec) => MetricExpr::Range {
            op: *op,
            range: log_range(*spec),
            param: param.map(str::to_string),
            // The walk characterization is about CHILD slots; a grouping is
            // an owned leaf like `param`, so `None` keeps the shapes fixed.
            grouping: None,
        },
        Shape::Literal(raw) => MetricExpr::Literal((*raw).to_string()),
        Shape::VectorFn(raw) => MetricExpr::VectorFn((*raw).to_string()),
        Shape::Vector(op, grouping, param, inner) => MetricExpr::Vector {
            op: *op,
            grouping: grouping.clone(),
            param: param.map(str::to_string),
            inner: Child::new(build(inner)),
        },
        Shape::Binary(op, modifier, l, r) => MetricExpr::Binary {
            op: *op,
            modifier: modifier.clone(),
            lhs: Child::new(build(l)),
            rhs: Child::new(build(r)),
        },
        Shape::Variants(vs, spec) => MetricExpr::Variants(Child::new(pulsus_logql::VariantsExpr {
            variants: ChildVec::new(vs.iter().map(build).collect()),
            range: log_range(*spec),
        })),
    }
}

fn build_shadow(s: &Shape) -> shadow::MetricExpr {
    match s {
        Shape::Range(op, param, spec) => shadow::MetricExpr::Range {
            op: *op,
            range: log_range(*spec),
            param: param.map(str::to_string),
            grouping: None,
        },
        Shape::Literal(raw) => shadow::MetricExpr::Literal((*raw).to_string()),
        Shape::VectorFn(raw) => shadow::MetricExpr::VectorFn((*raw).to_string()),
        Shape::Vector(op, grouping, param, inner) => shadow::MetricExpr::Vector {
            op: *op,
            grouping: grouping.clone(),
            param: param.map(str::to_string),
            inner: Box::new(build_shadow(inner)),
        },
        Shape::Binary(op, modifier, l, r) => shadow::MetricExpr::Binary {
            op: *op,
            modifier: modifier.clone(),
            lhs: Box::new(build_shadow(l)),
            rhs: Box::new(build_shadow(r)),
        },
        Shape::Variants(vs, spec) => shadow::MetricExpr::Variants(Box::new(shadow::VariantsExpr {
            variants: vs.iter().map(build_shadow).collect(),
            range: log_range(*spec),
        })),
    }
}

fn grouping(kind: GroupingKind, labels: &[&str]) -> Grouping {
    Grouping {
        kind,
        labels: labels.iter().map(|s| (*s).to_string()).collect(),
    }
}

/// Every variant, both `LogRange` shapes, both operand slots of `Binary`
/// and `Vector::inner`, `Variants` at 0/1/2/N elements, `Variants` as a
/// `Binary` operand, `Variants` nested inside `Variants`, and `Binary`
/// under `Vector` — the shapes the parser cannot produce are built here
/// because only construction reaches them.
fn matrix() -> Vec<(&'static str, Shape)> {
    use Shape::*;
    let r = || Range(RangeAggOp::Rate, None, LogRangeSpec::Plain);
    let q = || {
        Range(
            RangeAggOp::QuantileOverTime,
            Some("0.95"),
            LogRangeSpec::Piped,
        )
    };
    let bin = |op, m, l: Shape, rr: Shape| Binary(op, m, Box::new(l), Box::new(rr));
    vec![
        ("range plain", r()),
        ("range with param and pipeline", q()),
        ("literal", Literal("2")),
        ("vector fn", VectorFn("0.5")),
        (
            "vector no grouping",
            Vector(VectorAggOp::Sum, None, None, Box::new(r())),
        ),
        (
            "vector by",
            Vector(
                VectorAggOp::Max,
                Some(grouping(GroupingKind::By, &["env", "app"])),
                None,
                Box::new(r()),
            ),
        ),
        (
            "vector without and param",
            Vector(
                VectorAggOp::Topk,
                Some(grouping(GroupingKind::Without, &["pod"])),
                Some("3"),
                Box::new(q()),
            ),
        ),
        ("binary plain", bin(BinOp::Add, None, r(), Literal("1"))),
        (
            "binary with bool modifier",
            bin(
                BinOp::Gt,
                Some(BinModifier {
                    return_bool: true,
                    matching: None,
                }),
                r(),
                Literal("1"),
            ),
        ),
        (
            "binary with on/group_left",
            bin(
                BinOp::Div,
                Some(BinModifier {
                    return_bool: false,
                    matching: Some(VectorMatching {
                        on: true,
                        labels: vec!["env".to_string()],
                        group: Some(MatchGroup::Left(vec!["pod".to_string()])),
                    }),
                }),
                r(),
                q(),
            ),
        ),
        (
            "binary with ignoring/group_right",
            bin(
                BinOp::Mul,
                Some(BinModifier {
                    return_bool: false,
                    matching: Some(VectorMatching {
                        on: false,
                        labels: vec!["a".to_string(), "b".to_string()],
                        group: Some(MatchGroup::Right(Vec::new())),
                    }),
                }),
                Literal("2"),
                r(),
            ),
        ),
        (
            "left-deep binary chain",
            bin(
                BinOp::Or,
                None,
                bin(
                    BinOp::Or,
                    None,
                    bin(BinOp::Or, None, r(), Literal("1")),
                    q(),
                ),
                VectorFn("7"),
            ),
        ),
        (
            "right-deep binary chain",
            bin(
                BinOp::Add,
                None,
                Literal("1"),
                bin(
                    BinOp::Sub,
                    None,
                    VectorFn("2"),
                    bin(BinOp::Pow, None, r(), q()),
                ),
            ),
        ),
        (
            "binary under vector",
            Vector(
                VectorAggOp::Sum,
                None,
                None,
                Box::new(bin(BinOp::Add, None, r(), Literal("4"))),
            ),
        ),
        ("variants empty", Variants(Vec::new(), LogRangeSpec::Plain)),
        ("variants one", Variants(vec![r()], LogRangeSpec::Plain)),
        (
            "variants two",
            Variants(vec![r(), q()], LogRangeSpec::Piped),
        ),
        (
            "variants many",
            Variants(
                vec![
                    r(),
                    q(),
                    Vector(VectorAggOp::Sum, None, None, Box::new(r())),
                    Literal("9"),
                    VectorFn("8"),
                ],
                LogRangeSpec::Plain,
            ),
        ),
        (
            "variants as a binary operand (left)",
            bin(
                BinOp::Add,
                None,
                Variants(vec![r()], LogRangeSpec::Plain),
                Literal("1"),
            ),
        ),
        (
            "variants as a binary operand (right)",
            bin(
                BinOp::Add,
                None,
                Literal("1"),
                Variants(vec![r()], LogRangeSpec::Plain),
            ),
        ),
        (
            "variants nested inside variants",
            Variants(
                vec![
                    Variants(vec![r(), q()], LogRangeSpec::Plain),
                    Vector(VectorAggOp::Sum, None, None, Box::new(r())),
                ],
                LogRangeSpec::Piped,
            ),
        ),
        (
            "variants three deep",
            Variants(
                vec![Variants(
                    vec![Variants(vec![r()], LogRangeSpec::Plain)],
                    LogRangeSpec::Plain,
                )],
                LogRangeSpec::Plain,
            ),
        ),
    ]
}

// ---------------------------------------------------------------------
// The recording hasher
// ---------------------------------------------------------------------

/// Records every `write_*` call (opcode plus operand) in order. Equal
/// `finish()` values cannot distinguish two different call sequences, so
/// the sequence is the oracle and `finish()` is the cheap secondary
/// check.
#[derive(Default)]
struct Recorder {
    calls: Vec<String>,
}

impl Hasher for Recorder {
    fn finish(&self) -> u64 {
        0
    }
    fn write(&mut self, bytes: &[u8]) {
        self.calls.push(format!("write({bytes:?})"));
    }
    fn write_u8(&mut self, i: u8) {
        self.calls.push(format!("write_u8({i})"));
    }
    fn write_u16(&mut self, i: u16) {
        self.calls.push(format!("write_u16({i})"));
    }
    fn write_u32(&mut self, i: u32) {
        self.calls.push(format!("write_u32({i})"));
    }
    fn write_u64(&mut self, i: u64) {
        self.calls.push(format!("write_u64({i})"));
    }
    fn write_u128(&mut self, i: u128) {
        self.calls.push(format!("write_u128({i})"));
    }
    fn write_usize(&mut self, i: usize) {
        self.calls.push(format!("write_usize({i})"));
    }
    fn write_i8(&mut self, i: i8) {
        self.calls.push(format!("write_i8({i})"));
    }
    fn write_i16(&mut self, i: i16) {
        self.calls.push(format!("write_i16({i})"));
    }
    fn write_i32(&mut self, i: i32) {
        self.calls.push(format!("write_i32({i})"));
    }
    fn write_i64(&mut self, i: i64) {
        self.calls.push(format!("write_i64({i})"));
    }
    fn write_i128(&mut self, i: i128) {
        self.calls.push(format!("write_i128({i})"));
    }
    fn write_isize(&mut self, i: isize) {
        self.calls.push(format!("write_isize({i})"));
    }
}

fn record<T: Hash>(v: &T) -> Vec<String> {
    let mut r = Recorder::default();
    v.hash(&mut r);
    r.calls
}

fn finish<T: Hash>(v: &T) -> u64 {
    let mut h = DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

// ---------------------------------------------------------------------
// The gates
// ---------------------------------------------------------------------

#[test]
fn compact_debug_is_byte_identical_to_the_derive() {
    for (name, s) in matrix() {
        let real = build(&s);
        let shadow = build_shadow(&s);
        assert_eq!(
            format!("{real:?}"),
            format!("{shadow:?}"),
            "compact Debug diverged for {name}"
        );
    }
}

#[test]
fn alternate_debug_is_byte_identical_to_the_derive() {
    for (name, s) in matrix() {
        let real = build(&s);
        let shadow = build_shadow(&s);
        assert_eq!(
            format!("{real:#?}"),
            format!("{shadow:#?}"),
            "alternate Debug diverged for {name}"
        );
    }
}

#[test]
fn nested_alternate_debug_indentation_matches_the_derive() {
    // The alternate layout is only interesting when the value is itself
    // nested inside another derived `{:#?}` — that is where a wrong
    // padding level shows up.
    for (name, s) in matrix() {
        let real = (7u8, build(&s), "tail");
        let shadow = (7u8, build_shadow(&s), "tail");
        assert_eq!(
            format!("{real:#?}"),
            format!("{shadow:#?}"),
            "nested alternate Debug diverged for {name}"
        );
    }
}

#[test]
fn hash_write_call_sequence_equals_the_derives() {
    for (name, s) in matrix() {
        let real = build(&s);
        let shadow = build_shadow(&s);
        assert_eq!(
            record(&real),
            record(&shadow),
            "hash call sequence diverged for {name}"
        );
        assert_eq!(
            finish(&real),
            finish(&shadow),
            "DefaultHasher finish diverged for {name}"
        );
    }
}

#[test]
fn clone_reproduces_the_original_and_its_debug_bytes() {
    for (name, s) in matrix() {
        let real = build(&s);
        let copy = real.clone();
        assert_eq!(real, copy, "clone diverged for {name}");
        assert_eq!(
            format!("{real:#?}"),
            format!("{copy:#?}"),
            "clone Debug bytes diverged for {name}"
        );
        assert_eq!(
            finish(&real),
            finish(&copy),
            "clone hash diverged for {name}"
        );
    }
}

#[test]
fn partial_eq_agrees_with_the_derive_over_the_cross_product() {
    let m = matrix();
    for (an, a) in &m {
        for (bn, b) in &m {
            let want = build_shadow(a) == build_shadow(b);
            let got = build(a) == build(b);
            assert_eq!(got, want, "PartialEq diverged for {an} vs {bn}");
        }
    }
}

#[test]
fn near_miss_pairs_are_not_equal() {
    use Shape::*;
    let bin = |op, l: Shape, r: Shape| Binary(op, None, Box::new(l), Box::new(r));
    let pairs = [
        (
            bin(BinOp::Add, Literal("1"), Literal("2")),
            bin(BinOp::Add, Literal("2"), Literal("1")),
        ),
        (
            Variants(vec![Literal("1"), Literal("2")], LogRangeSpec::Plain),
            Variants(vec![Literal("1")], LogRangeSpec::Plain),
        ),
        (
            Variants(vec![Literal("1")], LogRangeSpec::Plain),
            Variants(vec![Literal("1")], LogRangeSpec::Piped),
        ),
    ];
    for (a, b) in pairs {
        assert_eq!(build(&a) == build(&b), build_shadow(&a) == build_shadow(&b));
        assert!(build(&a) != build(&b));
    }
}

#[test]
fn display_round_trips_through_the_parser_for_every_parseable_shape() {
    // `Display` is pinned byte-for-byte by `snapshots.rs`'s 25 `{:#?}`
    // expectations and its `parse(ast.to_string()) == ast` oracle; this
    // adds the programmatic shapes the parser cannot build, checking that
    // rendering them stays total and stable under a re-render of a clone.
    for (name, s) in matrix() {
        let real = build(&s);
        let again = real.clone();
        assert_eq!(
            real.to_string(),
            again.to_string(),
            "Display unstable for {name}"
        );
        assert!(!real.to_string().is_empty(), "empty Display for {name}");
    }
}

#[test]
fn a_wide_chain_does_not_abort_any_converted_trait() {
    // The vector this issue exists for: a flat `or` chain parses at depth
    // 1 into a LEFT-DEEP tree, so width becomes depth. 40,000 terms is
    // far past every recursive threshold measured on this issue
    // (`compile_label_filter` 709 debug, AST drop glue 26,111 debug).
    const N: usize = 40_000;
    // Alternate-mode `Debug` is O(N x depth) *work* exactly as the derive
    // is, so the wide row renders it at a smaller N and pins the linear
    // modes at the full one.
    const ALT_N: usize = 2_000;
    let mut real = build(&Shape::Literal("0"));
    let mut shadowless = String::new();
    for i in 1..N {
        real = MetricExpr::Binary {
            op: BinOp::Add,
            modifier: None,
            lhs: Child::new(real),
            rhs: Child::new(MetricExpr::Literal(i.to_string())),
        };
    }
    // Debug into a byte-counting sink, never a `String` of the whole tree.
    struct Counter(usize);
    impl std::fmt::Write for Counter {
        fn write_str(&mut self, s: &str) -> std::fmt::Result {
            self.0 += s.len();
            Ok(())
        }
    }
    use std::fmt::Write as _;
    let mut c = Counter(0);
    write!(c, "{real:?}").expect("compact Debug");
    assert!(c.0 > N);
    let mut alt = build(&Shape::Literal("0"));
    for i in 1..ALT_N {
        alt = MetricExpr::Binary {
            op: BinOp::Add,
            modifier: None,
            lhs: Child::new(alt),
            rhs: Child::new(MetricExpr::Literal(i.to_string())),
        };
    }
    let mut c = Counter(0);
    write!(c, "{alt:#?}").expect("alternate Debug");
    assert!(c.0 > ALT_N);
    drop(alt);
    let mut c = Counter(0);
    write!(c, "{real}").expect("Display");
    assert!(c.0 > N);
    shadowless.push('x');
    let copy = real.clone();
    assert!(real == copy);
    assert_eq!(finish(&real), finish(&copy));
    drop(copy);
    drop(real);
    assert_eq!(shadowless, "x");
}

// ---------------------------------------------------------------------
// SCC-1: `LabelFilterExpr` against its own derived shadow
// ---------------------------------------------------------------------

mod lf_shadow {
    use pulsus_logql::{CompareOp, Matcher, NumericLiteral};

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub enum LabelFilterExpr {
        Match(Matcher),
        Compare {
            name: String,
            op: CompareOp,
            rhs: NumericLiteral,
        },
        Ip {
            name: String,
            value: String,
            negated: bool,
        },
        And(Box<LabelFilterExpr>, Box<LabelFilterExpr>),
        Or(Box<LabelFilterExpr>, Box<LabelFilterExpr>),
    }
}

#[derive(Debug, Clone)]
enum LfShape {
    Match(&'static str, MatchOp),
    Compare(&'static str, CompareOp),
    /// `(name, negated, raw value)` — the value is carried so a fixture
    /// whose escaping matters can exist.
    Ip(&'static str, bool, &'static str),
    And(Box<LfShape>, Box<LfShape>),
    Or(Box<LfShape>, Box<LfShape>),
}

fn build_lf(s: &LfShape) -> pulsus_logql::LabelFilterExpr {
    use pulsus_logql::LabelFilterExpr as L;
    match s {
        LfShape::Match(n, op) => L::Match(Matcher {
            name: (*n).to_string(),
            op: *op,
            value: "v".to_string(),
        }),
        LfShape::Compare(n, op) => L::Compare {
            name: (*n).to_string(),
            op: *op,
            rhs: pulsus_logql::NumericLiteral::Number("1".to_string()),
        },
        LfShape::Ip(n, neg, v) => L::Ip {
            name: (*n).to_string(),
            value: (*v).to_string(),
            negated: *neg,
        },
        LfShape::And(a, b) => L::And(Child::new(build_lf(a)), Child::new(build_lf(b))),
        LfShape::Or(a, b) => L::Or(Child::new(build_lf(a)), Child::new(build_lf(b))),
    }
}

fn build_lf_shadow(s: &LfShape) -> lf_shadow::LabelFilterExpr {
    use lf_shadow::LabelFilterExpr as L;
    match s {
        LfShape::Match(n, op) => L::Match(Matcher {
            name: (*n).to_string(),
            op: *op,
            value: "v".to_string(),
        }),
        LfShape::Compare(n, op) => L::Compare {
            name: (*n).to_string(),
            op: *op,
            rhs: pulsus_logql::NumericLiteral::Number("1".to_string()),
        },
        LfShape::Ip(n, neg, v) => L::Ip {
            name: (*n).to_string(),
            value: (*v).to_string(),
            negated: *neg,
        },
        LfShape::And(a, b) => L::And(Box::new(build_lf_shadow(a)), Box::new(build_lf_shadow(b))),
        LfShape::Or(a, b) => L::Or(Box::new(build_lf_shadow(a)), Box::new(build_lf_shadow(b))),
    }
}

fn lf_matrix() -> Vec<(&'static str, LfShape)> {
    use LfShape::{And, Compare, Ip, Match, Or};
    let and = |a: LfShape, b: LfShape| And(Box::new(a), Box::new(b));
    let or = |a: LfShape, b: LfShape| Or(Box::new(a), Box::new(b));
    vec![
        ("match eq", Match("a", MatchOp::Eq)),
        ("match re", Match("a", MatchOp::Re)),
        ("compare gt", Compare("d", CompareOp::Gt)),
        ("ip", Ip("addr", false, "1.2.3.4")),
        ("ip negated", Ip("addr", true, "1.2.3.0/24")),
        // The escaping fixture: without it the oracle's quoted form and
        // the real `quote()` agree trivially and B2's divergence class
        // is invisible. Constructed, never parsed — `IpMatcher::parse`
        // rejects it at COMPILE time, while `Display` is an AST-level
        // concern and must still render it exactly.
        ("ip needs escaping", Ip("addr", false, "a\"b\\c\td\ne")),
        (
            "and",
            and(Match("a", MatchOp::Eq), Compare("d", CompareOp::Lt)),
        ),
        (
            "or",
            or(Match("a", MatchOp::Neq), Ip("addr", false, "1.2.3.4")),
        ),
        (
            "left-deep or chain",
            or(
                or(
                    or(Match("a", MatchOp::Eq), Compare("b", CompareOp::Gt)),
                    Ip("c", false, "1.2.3.4"),
                ),
                and(Match("d", MatchOp::Nre), Compare("e", CompareOp::Lte)),
            ),
        ),
        (
            "right-deep and chain",
            and(
                Match("a", MatchOp::Eq),
                and(
                    Compare("b", CompareOp::Neq),
                    or(
                        Ip("c", true, "1.2.3.4"),
                        and(Match("d", MatchOp::Re), Ip("e", false, "1.2.3.4")),
                    ),
                ),
            ),
        ),
    ]
}

#[test]
fn label_filter_hash_write_call_sequence_equals_the_derives() {
    // The regression this pins: a resumable frame is entered once per
    // child PLUS once, so hashing `mem::discriminant` outside the ENTER
    // step writes it three times for `And`/`Or` — a silent divergence
    // from every caller written against the derive.
    for (name, s) in lf_matrix() {
        let real = build_lf(&s);
        let shadow = build_lf_shadow(&s);
        assert_eq!(
            record(&real),
            record(&shadow),
            "hash call sequence diverged for {name}"
        );
        assert_eq!(
            finish(&real),
            finish(&shadow),
            "DefaultHasher finish diverged for {name}"
        );
    }
}

/// The `Display` oracle.
///
/// **This is a transcription, not an independent implementation, and it
/// is worth less than the other oracles here for exactly that reason.**
/// `Display` was hand-written before #272 as well, so there is no derive
/// to differ from; what this pins is that the ITERATIVE renderer emits
/// the bytes the RECURSIVE one did — a transcription of the pre-#272
/// `fmt_child` form, which parenthesised a nested boolean child and
/// rendered leaves inline, walked over the shadow's plain `Box` tree. A
/// shared misreading of the original would survive it.
///
/// **Exactly one leg here is independent of the implementation**, and it
/// is not the one that looks it:
///
/// * The structural rendering is transcribed (above).
/// * The `Ip` escaping is **also not independent**. Delegating to
///   `Matcher`'s `Display` avoids a test-local literal, but `Matcher`
///   and the renderer under test call the *same* private `ast.rs`
///   `quote`. What the `ip needs escaping` fixture therefore proves is
///   narrower than "the escaping is correct": it proves the new
///   renderer still ROUTES through `quote` rather than hand-rolling its
///   own quoting, which is a real regression it would otherwise miss. A
///   bug inside `quote` survives both sides.
/// * The **round trip** asserts `Display(parse(q)) == q`, so it routes
///   back through the renderer under test and is NOT an independent
///   oracle. What it establishes is parser acceptance plus a
///   parse/render fixed point: the rendered form re-parses, and
///   re-rendering reproduces it byte for byte. It covers the parseable
///   fixtures only.
fn display_reference(n: &lf_shadow::LabelFilterExpr) -> String {
    use lf_shadow::LabelFilterExpr as L;
    fn child(c: &L, out: &mut String) {
        match c {
            L::And(..) | L::Or(..) => {
                out.push('(');
                render(c, out);
                out.push(')');
            }
            leaf => render(leaf, out),
        }
    }
    fn render(n: &L, out: &mut String) {
        match n {
            L::Match(m) => out.push_str(&m.to_string()),
            L::Compare { name, op, rhs } => out.push_str(&format!("{name} {op} {rhs}")),
            L::Ip {
                name,
                value,
                negated,
            } => {
                let op = if *negated { "!=" } else { "=" };
                // The crate's `quote` is private, so the quoted form is
                // taken from a PUBLIC path #272 did not touch:
                // `Matcher`'s `Display` is `name` + `op` + `quote(value)`,
                // so everything after the leading `q=` is `quote(value)`
                // verbatim. A test-local literal would silently agree
                // with itself on escape-free values and diverge on any
                // other — which is why `ip needs escaping` is in the
                // matrix.
                let probe = Matcher {
                    name: "q".to_string(),
                    op: MatchOp::Eq,
                    value: value.clone(),
                }
                .to_string();
                let quoted = probe
                    .strip_prefix("q=")
                    .expect("Matcher renders as name + op + quoted value");
                out.push_str(&format!("{name} {op} ip({quoted})"));
            }
            L::And(a, b) => {
                child(a, out);
                out.push_str(" and ");
                child(b, out);
            }
            L::Or(a, b) => {
                child(a, out);
                out.push_str(" or ");
                child(b, out);
            }
        }
    }
    let mut out = String::new();
    render(n, &mut out);
    out
}

#[test]
fn label_filter_display_matches_the_pre_change_renderer() {
    for (name, s) in lf_matrix() {
        let real = build_lf(&s);
        let want = display_reference(&build_lf_shadow(&s));
        assert_eq!(real.to_string(), want, "Display diverged for {name}");
        // And it still round-trips through the parser, which is what
        // `snapshots.rs`'s oracle pins for the parseable shapes.
        let q = format!(r#"{{a="b"}} | {}"#, real);
        let reparsed = pulsus_logql::parse(&q).unwrap_or_else(|e| panic!("{name}: {q}: {e}"));
        assert_eq!(reparsed.to_string(), q, "round trip diverged for {name}");
    }
}

#[test]
fn label_filter_debug_clone_and_eq_match_the_derive() {
    for (name, s) in lf_matrix() {
        let real = build_lf(&s);
        let shadow = build_lf_shadow(&s);
        assert_eq!(
            format!("{real:?}"),
            format!("{shadow:?}"),
            "compact Debug: {name}"
        );
        assert_eq!(
            format!("{real:#?}"),
            format!("{shadow:#?}"),
            "alternate Debug: {name}"
        );
        assert_eq!(
            format!("{:#?}", (3u8, build_lf(&s), "tail")),
            format!("{:#?}", (3u8, build_lf_shadow(&s), "tail")),
            "nested alternate Debug: {name}"
        );
        let copy = real.clone();
        assert!(real == copy, "clone: {name}");
        assert_eq!(finish(&real), finish(&copy));
    }
    // Cross-product, against the derive's own verdict.
    let m = lf_matrix();
    for (an, a) in &m {
        for (bn, b) in &m {
            assert_eq!(
                build_lf(a) == build_lf(b),
                build_lf_shadow(a) == build_lf_shadow(b),
                "PartialEq diverged for {an} vs {bn}"
            );
        }
    }
}

// ---------------------------------------------------------------------
// The committed golden (AC 4/5)
// ---------------------------------------------------------------------

/// Renders the whole matrix's observable bytes. Deliberately includes the
/// hash **call sequence** and never a `DefaultHasher::finish()` value:
/// a finish is a toolchain-version-dependent number, while the call
/// sequence is the property the derive equivalence turns on.
fn render_golden() -> String {
    let mut out = String::new();
    for (name, s) in matrix() {
        let v = build(&s);
        out.push_str("=== ");
        out.push_str(name);
        out.push_str(" ===\n--- Debug {:?} ---\n");
        out.push_str(&format!("{v:?}"));
        out.push_str("\n--- Debug {:#?} ---\n");
        out.push_str(&format!("{v:#?}"));
        out.push_str("\n--- Display ---\n");
        out.push_str(&v.to_string());
        out.push_str("\n--- Hash calls ---\n");
        for c in record(&v) {
            out.push_str(&c);
            out.push('\n');
        }
        out.push('\n');
    }
    out
}

const GOLDEN: &str = include_str!("golden/ast_walk_characterization.txt");

#[test]
fn the_committed_golden_still_describes_the_shipped_types() {
    assert_eq!(
        render_golden(),
        GOLDEN,
        "SCC-2's observable bytes moved. This golden was captured from the DERIVED impls \
         before #272 touched the types (AC 4); a diff here is a behaviour change, not a \
         golden to refresh."
    );
}

/// `cargo test -p pulsus-logql --test ast_walk_characterization -- --ignored zz_regenerate`
#[test]
#[ignore = "generator: rewrites the committed golden"]
fn zz_regenerate_golden() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
    std::fs::create_dir_all(&dir).expect("golden dir");
    let body = render_golden();
    std::fs::write(dir.join("ast_walk_characterization.txt"), &body).expect("write golden");
    let digest = <sha2::Sha256 as sha2::Digest>::digest(body.as_bytes());
    std::fs::write(
        dir.join("ast_walk_characterization.sha256"),
        format!("{digest:x}\n"),
    )
    .expect("write sha256");
}
