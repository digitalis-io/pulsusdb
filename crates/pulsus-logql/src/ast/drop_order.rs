//! The SCC-2 drop oracle (issue #272).
//!
//! The hook lives in the **shipped** `impl Drop for MetricExpr`, one line
//! above its `walk::dismantle` call, so the sequence under test is the
//! one production runs — not a copy of the algorithm. It compiles to
//! nothing outside `cfg(test)`, and because `Drop::drop`'s body *is* the
//! `dismantle` call it fires at exactly the same point on the flattened
//! path and on the emptied-node glue path.
//!
//! **The alphabet is the node's variant kind, on both sides.** Node
//! addresses cannot be used: `steal_children` moves each child value out
//! of its box, so its `&mut self` address is not the address any pre-pass
//! could have recorded. Kinds alias where addresses would not, so
//! discrimination is a pre-committed fixture property —
//! [`assert_sibling_kind_asymmetry`] runs before any drop leg and fails
//! the suite if a fixture cannot tell a sibling reordering apart.
//!
//! `VariantsExpr` carries no `impl Drop`, hence no hook site, so it
//! contributes no entry — and the shadow's `VeShadow` carries no `Tag`,
//! so neither does it. The exclusion is a property of the code rather
//! than a predicate that can lie.

use std::cell::RefCell;

use super::{
    Duration, LabelFilterExpr, LogExpr, LogRange, MetricExpr, RangeAggOp, StreamSelector,
    VariantsExpr,
};
use crate::walk::{Child, ChildVec};

thread_local! {
    static TRACE: RefCell<Option<Vec<&'static str>>> = const { RefCell::new(None) };
}

/// Records `kind_of(n)` when a trace is installed; returns early on a
/// placeholder.
///
/// The placeholders written by `mem::replace` are themselves dropped when
/// the emptied parent's glue runs, so without this filter every stolen
/// slot would contribute a spurious entry. Control (c) deletes this early
/// return and the oracle fails on length.
pub(super) fn note_visited_expr(n: &MetricExpr) {
    if is_placeholder(n) {
        return;
    }
    let kind = kind_of(n);
    // `try_with` rather than `with`: a `MetricExpr` dropped during
    // thread-local teardown must not panic.
    let _ = TRACE.try_with(|t| {
        if let Ok(mut slot) = t.try_borrow_mut()
            && let Some(v) = slot.as_mut()
        {
            v.push(kind);
        }
    });
}

/// The exact shape `steal_children` writes over a stolen `MetricExpr`
/// slot: `Literal("")`, which the parser can never produce.
pub(super) fn is_placeholder(n: &MetricExpr) -> bool {
    matches!(n, MetricExpr::Literal(raw) if raw.is_empty())
}

pub(super) fn kind_of(n: &MetricExpr) -> &'static str {
    match n {
        MetricExpr::Range { .. } => "Range",
        MetricExpr::Vector { .. } => "Vector",
        MetricExpr::Literal(_) => "Literal",
        MetricExpr::VectorFn(_) => "VectorFn",
        MetricExpr::Binary { .. } => "Binary",
        MetricExpr::Variants(_) => "Variants",
        MetricExpr::LabelReplace { .. } => "LabelReplace",
    }
}

/// RAII: installs the thread-local trace and takes it on drop. The scope
/// must contain the fixture drop **and nothing else** — a stray
/// `MetricExpr` temporary inside the window pollutes the trace, which
/// [`assert_sibling_kind_asymmetry`] turns into a loud failure rather
/// than a weak pass.
pub(super) struct TraceGuard;

impl TraceGuard {
    pub(super) fn install() -> Self {
        TRACE.with(|t| *t.borrow_mut() = Some(Vec::new()));
        TraceGuard
    }

    pub(super) fn take(self) -> Vec<&'static str> {
        TRACE.with(|t| t.borrow_mut().take().unwrap_or_default())
    }
}

// ---------------------------------------------------------------------
// The derived shadow mirror
// ---------------------------------------------------------------------

/// Records its kind when the compiler's own glue drops it.
struct Tag(&'static str);

impl Drop for Tag {
    fn drop(&mut self) {
        let kind = self.0;
        let _ = TRACE.try_with(|t| {
            if let Ok(mut slot) = t.try_borrow_mut()
                && let Some(v) = slot.as_mut()
            {
                v.push(kind);
            }
        });
    }
}

/// A plain `Box`-child mirror of `MetricExpr` with the same variant
/// arities and a **leading** [`Tag`], and **no** `impl Drop` of its own —
/// so its order is pure compiler glue.
enum MeShadow {
    Range(Tag),
    Literal(Tag),
    VectorFn(Tag),
    Vector(Tag, Box<MeShadow>),
    Binary(Tag, Box<MeShadow>, Box<MeShadow>),
    Variants(Tag, Box<VeShadow>),
}

/// `VariantsExpr`'s mirror. No `Tag`, because `VariantsExpr` has no
/// `impl Drop` and therefore no hook site.
struct VeShadow {
    variants: Vec<MeShadow>,
}

impl MeShadow {
    /// Reads the mirror's tags in pre-order. Asserted against the shape
    /// before the drop leg runs, so a mistagged mirror fails as a build
    /// error rather than as a weaker control.
    fn kinds(&self, out: &mut Vec<&'static str>) {
        match self {
            MeShadow::Range(t) | MeShadow::Literal(t) | MeShadow::VectorFn(t) => out.push(t.0),
            MeShadow::Vector(t, inner) => {
                out.push(t.0);
                inner.kinds(out);
            }
            MeShadow::Binary(t, l, r) => {
                out.push(t.0);
                l.kinds(out);
                r.kinds(out);
            }
            MeShadow::Variants(t, v) => {
                out.push(t.0);
                for k in &v.variants {
                    k.kinds(out);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------
// Fixtures: one shape spec builds both sides
// ---------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(super) enum Shape {
    Range,
    Literal,
    VectorFn,
    Vector(Box<Shape>),
    Binary(Box<Shape>, Box<Shape>),
    /// `MetricExpr::Variants(VariantsExpr { variants })`.
    Variants(Vec<Shape>),
}

fn leaf_range() -> LogRange {
    LogRange {
        selector: LogExpr {
            selector: StreamSelector {
                matchers: Vec::new(),
            },
            pipeline: Vec::new(),
        },
        range: Duration::from_nanos(1),
        unwrap: None,
    }
}

pub(super) fn build_expr(s: &Shape) -> MetricExpr {
    match s {
        Shape::Range => MetricExpr::Range {
            op: RangeAggOp::Rate,
            range: leaf_range(),
            param: None,
            // Drop order is about CHILD slots; a grouping is an owned leaf
            // like `param`, dropped in field order with the rest.
            grouping: None,
        },
        // Never empty: an empty `Literal` is the placeholder shape and
        // would be filtered out of the trace.
        Shape::Literal => MetricExpr::Literal("1".to_string()),
        Shape::VectorFn => MetricExpr::VectorFn("2".to_string()),
        Shape::Vector(inner) => MetricExpr::Vector {
            op: super::VectorAggOp::Sum,
            grouping: None,
            param: None,
            inner: Child::new(build_expr(inner)),
        },
        Shape::Binary(l, r) => MetricExpr::Binary {
            op: super::BinOp::Add,
            modifier: None,
            lhs: Child::new(build_expr(l)),
            rhs: Child::new(build_expr(r)),
        },
        Shape::Variants(vs) => MetricExpr::Variants(Child::new(VariantsExpr {
            variants: ChildVec::new(vs.iter().map(build_expr).collect()),
            range: leaf_range(),
        })),
    }
}

fn build_shadow(s: &Shape) -> MeShadow {
    match s {
        Shape::Range => MeShadow::Range(Tag("Range")),
        Shape::Literal => MeShadow::Literal(Tag("Literal")),
        Shape::VectorFn => MeShadow::VectorFn(Tag("VectorFn")),
        Shape::Vector(inner) => MeShadow::Vector(Tag("Vector"), Box::new(build_shadow(inner))),
        Shape::Binary(l, r) => MeShadow::Binary(
            Tag("Binary"),
            Box::new(build_shadow(l)),
            Box::new(build_shadow(r)),
        ),
        Shape::Variants(vs) => MeShadow::Variants(
            Tag("Variants"),
            Box::new(VeShadow {
                variants: vs.iter().map(build_shadow).collect(),
            }),
        ),
    }
}

/// The pre-order kind sequence of a shape, `VariantsExpr` excluded (it
/// has no `impl Drop`, so it appears on neither side).
fn shape_kinds(s: &Shape, out: &mut Vec<&'static str>) {
    match s {
        Shape::Range => out.push("Range"),
        Shape::Literal => out.push("Literal"),
        Shape::VectorFn => out.push("VectorFn"),
        Shape::Vector(inner) => {
            out.push("Vector");
            shape_kinds(inner, out);
        }
        Shape::Binary(l, r) => {
            out.push("Binary");
            shape_kinds(l, out);
            shape_kinds(r, out);
        }
        Shape::Variants(vs) => {
            out.push("Variants");
            for v in vs {
                shape_kinds(v, out);
            }
        }
    }
}

/// Every node of arity >= 2 must have pairwise-distinct child kind
/// sequences, so a sibling reordering or a moved subtree necessarily
/// changes the trace. Asserted before any drop leg runs.
pub(super) fn assert_sibling_kind_asymmetry(s: &Shape) {
    let kids: Vec<&Shape> = match s {
        Shape::Range | Shape::Literal | Shape::VectorFn => Vec::new(),
        Shape::Vector(inner) => vec![inner.as_ref()],
        Shape::Binary(l, r) => vec![l.as_ref(), r.as_ref()],
        Shape::Variants(vs) => vs.iter().collect(),
    };
    if kids.len() >= 2 {
        let seqs: Vec<Vec<&'static str>> = kids
            .iter()
            .map(|k| {
                let mut v = Vec::new();
                shape_kinds(k, &mut v);
                v
            })
            .collect();
        for i in 0..seqs.len() {
            for j in (i + 1)..seqs.len() {
                assert_ne!(
                    seqs[i], seqs[j],
                    "fixture cannot discriminate a sibling swap at children {i}/{j}"
                );
            }
        }
    }
    for k in kids {
        assert_sibling_kind_asymmetry(k);
    }
}

// ---------------------------------------------------------------------
// SCC-1's hook and mirror (issue #272 finding 1)
// ---------------------------------------------------------------------

/// Records `lf_kind_of(n)` when a trace is installed; returns early on
/// the placeholder `steal_children` writes over a stolen slot.
pub(super) fn note_visited_lf(n: &LabelFilterExpr) {
    if is_lf_placeholder(n) {
        return;
    }
    let kind = lf_kind_of(n);
    let _ = TRACE.try_with(|t| {
        if let Ok(mut slot) = t.try_borrow_mut()
            && let Some(v) = slot.as_mut()
        {
            v.push(kind);
        }
    });
}

/// The exact shape `steal_children` writes: an `Ip` with two empty
/// strings, which the parser can never produce (a bare `name != ip("")`
/// fails `IpMatcher::parse`, and the name is never empty).
fn is_lf_placeholder(n: &LabelFilterExpr) -> bool {
    matches!(n, LabelFilterExpr::Ip { name, value, .. } if name.is_empty() && value.is_empty())
}

fn lf_kind_of(n: &LabelFilterExpr) -> &'static str {
    match n {
        LabelFilterExpr::Match(_) => "Match",
        LabelFilterExpr::Compare { .. } => "Compare",
        LabelFilterExpr::Ip { .. } => "Ip",
        LabelFilterExpr::And(..) => "And",
        LabelFilterExpr::Or(..) => "Or",
    }
}

/// SCC-1's mirror: plain `Box` children, a leading [`Tag`], no
/// `impl Drop`, so its order is pure compiler glue.
enum LfShadow {
    Match(Tag),
    Compare(Tag),
    Ip(Tag),
    And(Tag, Box<LfShadow>, Box<LfShadow>),
    Or(Tag, Box<LfShadow>, Box<LfShadow>),
}

impl LfShadow {
    fn kinds(&self, out: &mut Vec<&'static str>) {
        match self {
            LfShadow::Match(t) | LfShadow::Compare(t) | LfShadow::Ip(t) => out.push(t.0),
            LfShadow::And(t, a, b) | LfShadow::Or(t, a, b) => {
                out.push(t.0);
                a.kinds(out);
                b.kinds(out);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(super) enum LfShape {
    Match(&'static str),
    Compare(&'static str),
    Ip(&'static str),
    And(Box<LfShape>, Box<LfShape>),
    Or(Box<LfShape>, Box<LfShape>),
}

pub(super) fn build_lf(s: &LfShape) -> LabelFilterExpr {
    match s {
        LfShape::Match(name) => LabelFilterExpr::Match(super::Matcher {
            name: (*name).to_string(),
            op: super::MatchOp::Eq,
            value: "v".to_string(),
        }),
        LfShape::Compare(name) => LabelFilterExpr::Compare {
            name: (*name).to_string(),
            op: super::CompareOp::Gt,
            rhs: super::NumericLiteral::Number("1".to_string()),
        },
        // Never both-empty: that is the placeholder shape.
        LfShape::Ip(name) => LabelFilterExpr::Ip {
            name: (*name).to_string(),
            value: "1.2.3.4".to_string(),
            negated: false,
        },
        LfShape::And(a, b) => {
            LabelFilterExpr::And(Child::new(build_lf(a)), Child::new(build_lf(b)))
        }
        LfShape::Or(a, b) => LabelFilterExpr::Or(Child::new(build_lf(a)), Child::new(build_lf(b))),
    }
}

fn build_lf_shadow(s: &LfShape) -> LfShadow {
    match s {
        LfShape::Match(_) => LfShadow::Match(Tag("Match")),
        LfShape::Compare(_) => LfShadow::Compare(Tag("Compare")),
        LfShape::Ip(_) => LfShadow::Ip(Tag("Ip")),
        LfShape::And(a, b) => LfShadow::And(
            Tag("And"),
            Box::new(build_lf_shadow(a)),
            Box::new(build_lf_shadow(b)),
        ),
        LfShape::Or(a, b) => LfShadow::Or(
            Tag("Or"),
            Box::new(build_lf_shadow(a)),
            Box::new(build_lf_shadow(b)),
        ),
    }
}

fn lf_shape_kinds(s: &LfShape, out: &mut Vec<&'static str>) {
    match s {
        LfShape::Match(_) => out.push("Match"),
        LfShape::Compare(_) => out.push("Compare"),
        LfShape::Ip(_) => out.push("Ip"),
        LfShape::And(a, b) => {
            out.push("And");
            lf_shape_kinds(a, out);
            lf_shape_kinds(b, out);
        }
        LfShape::Or(a, b) => {
            out.push("Or");
            lf_shape_kinds(a, out);
            lf_shape_kinds(b, out);
        }
    }
}

pub(super) fn assert_lf_sibling_kind_asymmetry(s: &LfShape) {
    if let LfShape::And(a, b) | LfShape::Or(a, b) = s {
        let (mut x, mut y) = (Vec::new(), Vec::new());
        lf_shape_kinds(a, &mut x);
        lf_shape_kinds(b, &mut y);
        assert_ne!(x, y, "fixture cannot discriminate a sibling swap");
        assert_lf_sibling_kind_asymmetry(a);
        assert_lf_sibling_kind_asymmetry(b);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shapes() -> Vec<(&'static str, Shape)> {
        use Shape::*;
        let b = |l: Shape, r: Shape| Binary(Box::new(l), Box::new(r));
        vec![
            (
                "left-deep",
                b(b(b(Range, Literal), VectorFn), Vector(Box::new(Range))),
            ),
            (
                "right-deep",
                b(Literal, b(VectorFn, b(Range, Vector(Box::new(Literal))))),
            ),
            (
                "mixed with variants",
                b(
                    Variants(vec![Range, Vector(Box::new(Literal)), b(Range, Literal)]),
                    VectorFn,
                ),
            ),
            (
                "nested variants",
                Variants(vec![
                    Variants(vec![Range, Vector(Box::new(Literal))]),
                    Vector(Box::new(VectorFn)),
                    b(Literal, VectorFn),
                ]),
            ),
        ]
    }

    fn production_trace(s: &Shape) -> Vec<&'static str> {
        let fixture = build_expr(s);
        let g = TraceGuard::install();
        drop(fixture);
        g.take()
    }

    fn shadow_trace(s: &Shape) -> Vec<&'static str> {
        let fixture = build_shadow(s);
        let mut declared = Vec::new();
        fixture.kinds(&mut declared);
        let mut expected = Vec::new();
        shape_kinds(s, &mut expected);
        assert_eq!(declared, expected, "the mirror carries the shape's tags");
        let g = TraceGuard::install();
        drop(fixture);
        g.take()
    }

    #[test]
    fn every_fixture_discriminates_a_sibling_swap() {
        for (name, s) in shapes() {
            assert_sibling_kind_asymmetry(&s);
            let mut expected = Vec::new();
            shape_kinds(&s, &mut expected);
            assert!(!expected.is_empty(), "{name} is empty");
        }
    }

    #[test]
    fn no_fixture_node_is_placeholder_shaped() {
        for (name, s) in shapes() {
            let fixture = build_expr(&s);
            let mut bad = 0usize;
            crate::ast::for_each_metric_expr(&fixture, |n| {
                if let crate::ast::MeNode::Expr(e) = n
                    && is_placeholder(e)
                {
                    bad += 1;
                }
            });
            assert_eq!(bad, 0, "{name} contains a placeholder-shaped node");
        }
    }

    #[test]
    fn shipped_drop_visits_nodes_in_compiler_glue_order() {
        for (name, s) in shapes() {
            assert_sibling_kind_asymmetry(&s);
            let mut expected = Vec::new();
            shape_kinds(&s, &mut expected);
            let shadow = shadow_trace(&s);
            assert_eq!(shadow, expected, "{name}: shadow glue trace");
            let production = production_trace(&s);
            assert_eq!(production, shadow, "{name}: shipped `dismantle` trace");
        }
    }

    #[test]
    fn no_variants_expr_tag_appears_on_either_side() {
        for (_, s) in shapes() {
            assert!(!production_trace(&s).contains(&"VariantsExpr"));
            assert!(!shadow_trace(&s).contains(&"VariantsExpr"));
        }
    }

    fn lf_shapes() -> Vec<(&'static str, LfShape)> {
        use LfShape::{And, Compare, Ip, Match, Or};
        let and = |a: LfShape, b: LfShape| And(Box::new(a), Box::new(b));
        let or = |a: LfShape, b: LfShape| Or(Box::new(a), Box::new(b));
        vec![
            (
                "left-deep or chain",
                or(
                    or(or(Match("a"), Compare("b")), Ip("c")),
                    and(Match("d"), Compare("e")),
                ),
            ),
            (
                "right-deep and chain",
                and(
                    Match("a"),
                    and(Compare("b"), or(Ip("c"), and(Match("d"), Ip("e")))),
                ),
            ),
            (
                "mixed",
                or(
                    and(Match("a"), or(Compare("b"), Ip("c"))),
                    and(and(Ip("d"), Match("e")), Compare("f")),
                ),
            ),
        ]
    }

    #[test]
    fn label_filter_drop_visits_nodes_in_compiler_glue_order() {
        for (name, s) in lf_shapes() {
            assert_lf_sibling_kind_asymmetry(&s);
            let mut expected = Vec::new();
            lf_shape_kinds(&s, &mut expected);

            let mirror = build_lf_shadow(&s);
            let mut declared = Vec::new();
            mirror.kinds(&mut declared);
            assert_eq!(
                declared, expected,
                "{name}: the mirror carries the shape's tags"
            );
            let g = TraceGuard::install();
            drop(mirror);
            let shadow = g.take();
            assert_eq!(shadow, expected, "{name}: shadow glue trace");

            let fixture = build_lf(&s);
            let g = TraceGuard::install();
            drop(fixture);
            assert_eq!(g.take(), shadow, "{name}: shipped `dismantle` trace");
        }
    }

    #[test]
    fn no_label_filter_fixture_node_is_placeholder_shaped() {
        for (name, s) in lf_shapes() {
            let fixture = build_lf(&s);
            let mut bad = 0usize;
            crate::ast::for_each_label_filter(&fixture, |n| {
                if is_lf_placeholder(n) {
                    bad += 1;
                }
            });
            assert_eq!(bad, 0, "{name} contains a placeholder-shaped node");
        }
    }
}
