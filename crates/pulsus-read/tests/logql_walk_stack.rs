//! Issue #272 — SCC-3's paired pinned-stack gates (AC 14/15).
//!
//! `MetricNode`'s converted edges (`Debug`, `Clone`, `PartialEq`, drop)
//! and its converted walks (`leaves`, `produces_series`,
//! `metric_node_postorder`) complete over an `N`-node tree on a pinned
//! `S`-byte stack; a per-node-recursive walk of the same shape over the
//! same tree overflows at `N/4` on the same `S`.
//!
//! The PLANNER's own walk (#293, #285) is gated the same way but not
//! here: its control is the BODY of the recursive `build_metric_node`
//! that issue deleted, with two substituted child accessors — not the
//! historical function itself; see `plan_recursive_control.rs`. That
//! body calls six
//! `plan.rs`-private items — so the pairing lives in
//! `crates/pulsus-read/src/logql/plan_recursive_control.rs`, which is
//! compiled inside the module that can see them.

#[path = "stackgate/mod.rs"]
mod stackgate;

use std::borrow::Cow;
use std::fmt::Write as _;

use pulsus_logql::BinOp;
use pulsus_logql::walk::Child;
use pulsus_read::logql::pipeline::CompiledPipeline;
use pulsus_read::logql::{MetricNode, MetricNodeScc};

/// The pinned stack every row runs on.
const S: usize = 256 * 1024;

/// Nodes in the positive leg's tree. The control runs at `N / 4`.
const N: usize = 20_000;

struct Counter(usize);

impl std::fmt::Write for Counter {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.0 += s.len();
        Ok(())
    }
}

/// A left-deep `Binary` spine of `k` branching nodes over `Scalar`
/// leaves — the shape a flat `a or b or c …` chain PLANS into.
fn build(k: usize) -> MetricNode {
    let mut e = MetricNode::Scalar(0.0);
    for i in 0..k {
        e = MetricNode::Binary {
            op: BinOp::Add,
            return_bool: false,
            matching: None,
            lhs: Child::new(e),
            rhs: Child::new(MetricNode::Scalar(f64::from(
                u32::try_from(i % 997).unwrap_or(0),
            ))),
        };
    }
    e
}

/// The candidate flat label-filter terms, each validated by the real
/// parser below. The chain is built from whichever ENUMERATED candidate
/// is cheapest per term — a global minimum over the grammar is not
/// claimed; see the note above,
/// so a worst-case row cannot quietly rest on a convenient shape.
///
/// **The claim is a minimum over THIS LIST, not a derivation from the
/// grammar.** `a>1` is the cheapest production enumerated here at three
/// bytes; nothing in this test rules out a shorter one the grammar
/// admits and this list omits. If the grammar ever grows one — or if one
/// was missed — it must be added here, and the row's term count will
/// rise accordingly. Stated rather than dressed up as exhaustive,
/// because a strained derivation would be the wider-claim defect this
/// row already carried once.
///
/// `,` is the one-byte AND separator; ` or `/` and ` are four and five,
/// so comma always wins. A term whose text grows with its index — the
/// `x="<n>" or` form this row was first written with — reaches under
/// half the term count of the cheapest candidate here, and is
/// deliberately absent.
const FLAT_TERM_CANDIDATES: &[&str] = &[r#"a="""#, "a>1", "a=1", "a>0"];

/// The widest flat label-filter chain buildable from
/// [`FLAT_TERM_CANDIDATES`] under `pulsus_logql::MAX_QUERY_BYTES`, as
/// `(text, term count)`. Widest over that list — not over the grammar.
///
/// Derived from the cap AND from the cheapest ENUMERATED term (see
/// [`FLAT_TERM_CANDIDATES`] — it is a minimum over that list, not over
/// the grammar), so it
/// tracks #279 and cannot be built on a non-minimal shape by accident.
fn widest_enumerated_label_filter_chain() -> (String, usize) {
    const HEAD: &str = r#"{a="b"} | "#;
    const SEP: char = ',';

    // Every candidate must actually parse, repeated — a candidate that
    // is shorter but invalid would silently shrink the probe.
    let mut best: Option<&str> = None;
    for cand in FLAT_TERM_CANDIDATES {
        let probe = format!("{HEAD}{cand}{SEP}{cand}{SEP}{cand}");
        assert!(
            pulsus_logql::parse(&probe).is_ok(),
            "candidate term {cand:?} does not parse as a flat chain"
        );
        if best.is_none_or(|b| cand.len() < b.len()) {
            best = Some(cand);
        }
    }
    let term = best.expect("at least one candidate term");

    let mut text = String::with_capacity(pulsus_logql::MAX_QUERY_BYTES);
    text.push_str(HEAD);
    let mut terms = 0usize;
    loop {
        let cost = term.len() + usize::from(terms > 0);
        if text.len() + cost >= pulsus_logql::MAX_QUERY_BYTES {
            break;
        }
        if terms > 0 {
            text.push(SEP);
        }
        text.push_str(term);
        terms += 1;
    }
    (text, terms)
}

#[test]
fn walk_stack_child() {
    let Some(mode) = stackgate::child_mode() else {
        return;
    };
    match mode.as_str() {
        "debug_compact" | "clone" | "eq" | "drop" | "leaves" | "produces_series" | "postorder" => {
            let k = (N - 1) / 2;
            stackgate::on_stack(S, move || {
                let tree = build(k);
                match mode.as_str() {
                    "debug_compact" => {
                        let mut c = Counter(0);
                        write!(c, "{tree:?}").expect("compact Debug");
                        assert!(c.0 > k);
                    }
                    "clone" => {
                        let copy = tree.clone();
                        assert!(copy == tree);
                        drop(copy);
                    }
                    "eq" => {
                        let other = build(k);
                        assert!(tree == other);
                        drop(other);
                    }
                    "leaves" => assert!(tree.leaves().is_empty()),
                    "produces_series" => assert!(!tree.produces_series()),
                    "postorder" => {
                        let mut nodes = Vec::new();
                        pulsus_logql::walk::postorder_into::<MetricNodeScc>(&tree, &mut nodes);
                        assert_eq!(nodes.len(), 2 * k + 1);
                    }
                    "drop" => {}
                    other => panic!("unknown positive mode {other}"),
                }
                drop(tree);
            });
        }
        // Issue #272 finding 4: the headline result, GATED. The longest
        // flat `and`/`or` label-filter chain the query-text cap admits
        // over `FLAT_TERM_CANDIDATES`,
        // driven all the way through parse -> compile -> per-row eval ->
        // Debug -> Clone -> drop on the pinned stack. Before the
        // conversion this shape aborted at 3,000 terms / 33,696 bytes;
        // this row fails if that vector reopens.
        "widest_enumerated_label_filter" => {
            stackgate::on_stack(S, || {
                let (text, terms) = widest_enumerated_label_filter_chain();
                println!("widest enumerated: terms={terms} bytes={}", text.len());
                assert!(
                    text.len() < pulsus_logql::MAX_QUERY_BYTES,
                    "the probe must be ADMISSIBLE: {} vs {}",
                    text.len(),
                    pulsus_logql::MAX_QUERY_BYTES
                );
                assert!(
                    text.len() + 16 >= pulsus_logql::MAX_QUERY_BYTES,
                    "the probe must fill the query-text cap, not sit comfortably inside it: \
                     {} vs {}",
                    text.len(),
                    pulsus_logql::MAX_QUERY_BYTES
                );
                // The cheapest enumerated term is three bytes plus a
                // one-byte comma, so the cap admits ~32.7k of them. A
                // probe built on a longer term shape exercises a
                // fraction of that.
                assert!(
                    terms > 32_000,
                    "the probe collapsed to {terms} terms — the chain is not being maximised \
                     over the cheapest ENUMERATED term"
                );
                let expr = pulsus_logql::parse(&text).expect("the widest enumerated chain parses");
                let stages = match &expr {
                    pulsus_logql::Expr::Log(l) => l.pipeline.clone(),
                    other => panic!("unexpected fixture shape: {other:?}"),
                };
                let compiled = CompiledPipeline::compile(&stages).expect("compile");

                // Per-row evaluation over the whole chain.
                let base: Vec<(String, String)> = vec![("a".to_string(), "b".to_string())];
                let mut labels: Vec<(Cow<'_, str>, Cow<'_, str>)> = Vec::new();
                compiled
                    .run_into("x=0", &base, 0, &mut labels)
                    .expect("no budget breach");

                // Debug into a byte-counting sink, never a `String` of
                // the whole tree.
                let mut c = Counter(0);
                write!(c, "{compiled:?}").expect("compact Debug");
                let rendered = c.0;
                assert!(rendered > terms);

                // The clone leg asserts STRUCTURAL EQUALITY, not a
                // proxy for it. `let copy = x.clone(); drop(copy);`
                // proved only that cloning does not abort; comparing
                // RENDERED LENGTH caught omission but not same-width
                // corruption, and `Debug` renders neither `max_stack`
                // nor `has_compare` at all — so a clone that kept
                // `ops.len()` while changing an op, or that dropped
                // either field, passed both. Equality is the property.
                let copy = compiled.clone();
                assert!(
                    compiled.label_filter_programs_eq(&copy),
                    "the clone's label-filter program differs from the original's over the \
                     {terms}-term chain — op content, `max_stack` or `has_compare`"
                );
                // Retained because it is cheap and it is the leg that
                // notices a partial chain even if the comparison above
                // is ever weakened: rendered length depends on every
                // node.
                let mut c = Counter(0);
                write!(c, "{copy:?}").expect("compact Debug of the copy");
                assert_eq!(
                    c.0, rendered,
                    "the clone rendered {} bytes against the original's {rendered} — it did \
                     not carry the whole {terms}-term chain",
                    c.0
                );
                drop(copy);
                drop(compiled);
                drop(stages);
                drop(expr);
            });
        }
        "control" => {
            let k = ((N / 4) - 1) / 2;
            stackgate::on_stack(S, move || {
                let shadow = stackgate::build_mn_shadow(k);
                let real = build(k);
                assert_eq!(
                    pulsus_logql::walk::arity::<MetricNodeScc>(&real),
                    stackgate::shadow_arity(&shadow),
                    "the mirror drifted from SCC-3's arities"
                );
                drop(real);
                let depth = stackgate::walk_mn_shadow_recursive(&shadow, 0);
                println!("control reached depth {depth}");
                stackgate::dismantle_mn_shadow(shadow);
            });
        }
        other => panic!("unknown child mode {other}"),
    }
}

macro_rules! positive_row {
    ($name:ident, $mode:literal) => {
        #[test]
        fn $name() {
            if stackgate::child_mode().is_some() {
                return;
            }
            stackgate::assert_child_ok("walk_stack_child", $mode);
        }
    };
}

positive_row!(
    metric_node_compact_debug_survives_a_wide_tree,
    "debug_compact"
);
positive_row!(metric_node_clone_survives_a_wide_tree, "clone");
positive_row!(metric_node_partial_eq_survives_a_wide_tree, "eq");
positive_row!(metric_node_drop_survives_a_wide_tree, "drop");
positive_row!(metric_node_leaves_survives_a_wide_tree, "leaves");
positive_row!(
    metric_node_produces_series_survives_a_wide_tree,
    "produces_series"
);
positive_row!(metric_node_postorder_survives_a_wide_tree, "postorder");
// Issue #272 finding 4: the PR's headline result, as a gate.
positive_row!(
    the_widest_enumerated_label_filter_chain_survives_end_to_end,
    "widest_enumerated_label_filter"
);

#[test]
fn a_per_node_recursion_over_the_same_shape_overflows_at_a_quarter_the_size() {
    if stackgate::child_mode().is_some() {
        return;
    }
    stackgate::assert_child_overflowed("walk_stack_child", "control");
}
