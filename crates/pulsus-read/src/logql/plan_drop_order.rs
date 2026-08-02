//! The SCC-3 drop oracle (issue #272).
//!
//! Same mechanism as `pulsus-logql`'s SCC-2 oracle: the hook sits in the
//! **shipped** `impl Drop for MetricNode`, one line above its
//! `walk::dismantle` call, so the sequence under test is production's. It
//! has to live in this crate — `#[cfg(test)]` items in a dependency do
//! not exist when a dependent compiles, which is exactly why the hook is
//! per `impl Drop` rather than inside `dismantle`.
//!
//! The alphabet is the node's variant kind on both sides, and
//! [`assert_sibling_kind_asymmetry`] pre-commits every fixture to being
//! able to tell a sibling swap apart.

use std::cell::RefCell;

use pulsus_logql::walk::{self, Child};

use super::{MetricNode, MetricNodeScc};

thread_local! {
    static TRACE: RefCell<Option<Vec<&'static str>>> = const { RefCell::new(None) };
}

/// Records `kind_of(n)` when a trace is installed; returns early on the
/// placeholder `steal_children` writes over a stolen slot.
pub(super) fn note_visited(n: &MetricNode) {
    if is_placeholder(n) {
        return;
    }
    let kind = kind_of(n);
    let _ = TRACE.try_with(|t| {
        if let Ok(mut slot) = t.try_borrow_mut()
            && let Some(v) = slot.as_mut()
        {
            v.push(kind);
        }
    });
}

/// The exact shape `steal_children` writes: `Scalar(f64::NAN)`, which the
/// planner can never produce.
fn is_placeholder(n: &MetricNode) -> bool {
    matches!(n, MetricNode::Scalar(v) if v.is_nan())
}

fn kind_of(n: &MetricNode) -> &'static str {
    match n {
        MetricNode::Leaf(_) => "Leaf",
        MetricNode::Scalar(_) => "Scalar",
        MetricNode::VectorLit { .. } => "VectorLit",
        MetricNode::Binary { .. } => "Binary",
        MetricNode::VectorAgg { .. } => "VectorAgg",
        MetricNode::Variants { .. } => "Variants",
        MetricNode::LabelReplace { .. } => "LabelReplace",
    }
}

/// RAII: installs the thread-local trace and takes it on drop.
struct TraceGuard;

impl TraceGuard {
    fn install() -> Self {
        TRACE.with(|t| *t.borrow_mut() = Some(Vec::new()));
        TraceGuard
    }

    fn take(self) -> Vec<&'static str> {
        TRACE.with(|t| t.borrow_mut().take().unwrap_or_default())
    }
}

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

/// A plain `Box`-child mirror of `MetricNode` with the same arities, a
/// leading [`Tag`] and **no** `impl Drop` — so its order is pure compiler
/// glue.
enum MnShadow {
    Scalar(Tag),
    VectorLit(Tag),
    VectorAgg(Tag, Box<MnShadow>),
    Binary(Tag, Box<MnShadow>, Box<MnShadow>),
}

impl MnShadow {
    /// Reads the mirror's tags in pre-order, so a mistagged mirror fails
    /// before the drop leg rather than becoming a weaker control.
    fn kinds(&self, out: &mut Vec<&'static str>) {
        match self {
            MnShadow::Scalar(t) | MnShadow::VectorLit(t) => out.push(t.0),
            MnShadow::VectorAgg(t, inner) => {
                out.push(t.0);
                inner.kinds(out);
            }
            MnShadow::Binary(t, l, r) => {
                out.push(t.0);
                l.kinds(out);
                r.kinds(out);
            }
        }
    }
}

/// One shape, built on both sides. `Leaf`/`Variants` are omitted: their
/// boxed `MetricPlan` leaves the SCC and needs a planner to build, and
/// both are arity-0 for this walk exactly as `Scalar` is.
#[derive(Debug, Clone)]
pub(super) enum Shape {
    Scalar(u32),
    VectorLit,
    VectorAgg(Box<Shape>),
    Binary(Box<Shape>, Box<Shape>),
}

pub(super) fn build(s: &Shape) -> MetricNode {
    match s {
        // Never NaN: a NaN `Scalar` is the placeholder shape.
        Shape::Scalar(i) => MetricNode::Scalar(f64::from(*i)),
        Shape::VectorLit => MetricNode::VectorLit {
            value: 1.0,
            window: super::super::window::GridWindow {
                start_ns: 0,
                end_ns: 0,
                step_ns: None,
            },
        },
        Shape::VectorAgg(inner) => MetricNode::VectorAgg {
            aggs: Vec::new(),
            inner: Child::new(build(inner)),
        },
        Shape::Binary(l, r) => MetricNode::Binary {
            op: pulsus_logql::BinOp::Add,
            return_bool: false,
            matching: None,
            lhs: Child::new(build(l)),
            rhs: Child::new(build(r)),
        },
    }
}

fn build_shadow(s: &Shape) -> MnShadow {
    match s {
        Shape::Scalar(_) => MnShadow::Scalar(Tag("Scalar")),
        Shape::VectorLit => MnShadow::VectorLit(Tag("VectorLit")),
        Shape::VectorAgg(inner) => {
            MnShadow::VectorAgg(Tag("VectorAgg"), Box::new(build_shadow(inner)))
        }
        Shape::Binary(l, r) => MnShadow::Binary(
            Tag("Binary"),
            Box::new(build_shadow(l)),
            Box::new(build_shadow(r)),
        ),
    }
}

fn shape_kinds(s: &Shape, out: &mut Vec<&'static str>) {
    match s {
        Shape::Scalar(_) => out.push("Scalar"),
        Shape::VectorLit => out.push("VectorLit"),
        Shape::VectorAgg(inner) => {
            out.push("VectorAgg");
            shape_kinds(inner, out);
        }
        Shape::Binary(l, r) => {
            out.push("Binary");
            shape_kinds(l, out);
            shape_kinds(r, out);
        }
    }
}

fn shape_post_order(s: &Shape, out: &mut Vec<&'static str>) {
    match s {
        Shape::Scalar(_) => out.push("Scalar"),
        Shape::VectorLit => out.push("VectorLit"),
        Shape::VectorAgg(inner) => {
            shape_post_order(inner, out);
            out.push("VectorAgg");
        }
        Shape::Binary(l, r) => {
            shape_post_order(l, out);
            shape_post_order(r, out);
            out.push("Binary");
        }
    }
}

/// Every node of arity >= 2 must have pairwise-distinct child kind
/// sequences, so a sibling reordering necessarily changes the trace.
pub(super) fn assert_sibling_kind_asymmetry(s: &Shape) {
    match s {
        Shape::Scalar(_) | Shape::VectorLit => {}
        Shape::VectorAgg(inner) => assert_sibling_kind_asymmetry(inner),
        Shape::Binary(l, r) => {
            let (mut a, mut b) = (Vec::new(), Vec::new());
            shape_kinds(l, &mut a);
            shape_kinds(r, &mut b);
            assert_ne!(a, b, "fixture cannot discriminate a sibling swap");
            assert_sibling_kind_asymmetry(l);
            assert_sibling_kind_asymmetry(r);
        }
    }
}

pub(super) fn shapes() -> Vec<(&'static str, Shape)> {
    use Shape::{Binary, Scalar, VectorAgg, VectorLit};
    let b = |l: Shape, r: Shape| Binary(Box::new(l), Box::new(r));
    vec![
        (
            "left-deep",
            b(
                b(b(Scalar(1), VectorLit), VectorAgg(Box::new(Scalar(2)))),
                Scalar(3),
            ),
        ),
        (
            "right-deep",
            b(
                Scalar(1),
                b(VectorLit, b(Scalar(2), VectorAgg(Box::new(VectorLit)))),
            ),
        ),
        (
            "mixed",
            VectorAgg(Box::new(b(
                VectorAgg(Box::new(Scalar(1))),
                b(VectorLit, Scalar(2)),
            ))),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn production_trace(s: &Shape) -> Vec<&'static str> {
        let fixture = build(s);
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
    fn no_fixture_node_is_placeholder_shaped() {
        for (name, s) in shapes() {
            let fixture = build(&s);
            let mut bad = 0usize;
            walk::preorder::<MetricNodeScc>(&fixture, |n| {
                if is_placeholder(n) {
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
    fn post_order_matches_the_shape_and_pins_the_value_stack() {
        for (name, s) in shapes() {
            let fixture = build(&s);
            let mut collected: Vec<&MetricNode> = Vec::new();
            walk::postorder_into::<MetricNodeScc>(&fixture, &mut collected);
            let (nodes, peak) = crate::logql::plan::postorder_peak(collected);
            let mut want = Vec::new();
            shape_post_order(&s, &mut want);
            let got: Vec<&'static str> = nodes.iter().map(|n| kind_of(n)).collect();
            assert_eq!(got, want, "{name}: post-order sequence");

            // The high-water mark the exec value stack actually reaches.
            let mut live = 0usize;
            let mut hi = 0usize;
            for n in &nodes {
                live -= walk::arity::<MetricNodeScc>(n);
                live += 1;
                hi = hi.max(live);
            }
            assert_eq!(peak, hi, "{name}: value-stack high-water mark");
        }
    }

    #[test]
    fn a_wide_chain_does_not_abort_any_converted_metric_node_trait() {
        // A flat `a or b or c …` chain plans into a LEFT-DEEP
        // `MetricNode::Binary` spine: width becomes depth. 40,000 terms is
        // far past every recursive threshold measured on this issue.
        const N: usize = 40_000;
        let mut node = MetricNode::Scalar(0.0);
        for i in 1..N {
            node = MetricNode::Binary {
                op: pulsus_logql::BinOp::Add,
                return_bool: false,
                matching: None,
                lhs: Child::new(node),
                rhs: Child::new(MetricNode::Scalar(f64::from(
                    u32::try_from(i % 1000).unwrap_or(0),
                ))),
            };
        }
        assert!(!node.produces_series());
        assert!(node.leaves().is_empty());
        let mut collected: Vec<&MetricNode> = Vec::new();
        walk::postorder_into::<MetricNodeScc>(&node, &mut collected);
        let (nodes, peak) = crate::logql::plan::postorder_peak(collected);
        assert_eq!(nodes.len(), 2 * N - 1);
        assert_eq!(peak, 2);

        struct Counter(usize);
        impl std::fmt::Write for Counter {
            fn write_str(&mut self, s: &str) -> std::fmt::Result {
                self.0 += s.len();
                Ok(())
            }
        }
        use std::fmt::Write as _;
        let mut c = Counter(0);
        write!(c, "{node:?}").expect("compact Debug");
        assert!(c.0 > N);

        let copy = node.clone();
        assert!(node == copy);
        drop(copy);
        drop(node);
    }
}
