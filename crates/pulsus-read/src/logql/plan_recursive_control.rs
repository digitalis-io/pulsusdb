//! Issue #293 / #285 — the plan walk's paired pinned-stack gate.
//!
//! # Two claims, deliberately separated
//!
//! **The historical claim is MEASURED, not gated here.** The recursive
//! `build_metric_node` this issue removed aborted the process inside
//! `plan()` — observed directly, by running the real pre-change code:
//!
//! * Tree: `origin/main` **`ae66648`**, unmodified, RELEASE profile, body
//!   on a `std::thread::Builder::new().stack_size(2 * 1024 * 1024)`
//!   thread (the `std` and tokio-worker default). Query:
//!   `count_over_time({app="a"}[5m]) or …`, one PROCESS per term count,
//!   because an abort takes the process down.
//! * **T = 1,200 (40,796 bytes) planned; T = 1,250 (42,496 bytes) exited
//!   134 with `fatal runtime error: stack overflow, aborting`.** T =
//!   1,300, 2,000 and 3,855 aborted likewise.
//! * The same probe against the CONVERTED planner: 1,250, 1,300, 2,000
//!   and 3,855 (131,066 bytes, the widest chain of that shape the
//!   query-text cap admits) all plan.
//!
//! Machine- and profile-dependent, recorded as what was true on the
//! machine that ran it — never pinned as a threshold. The probe was a
//! throwaway and is deleted; nothing below re-establishes it, and nothing
//! below needs to.
//!
//! **The claim THIS FILE gates is narrower, and is a regression guard.**
//! In full and nothing wider: the CONVERTED `build_metric_node` plans the
//! widest flat chain [`pulsus_logql::MAX_QUERY_BYTES`] admits on a 256
//! KiB stack, and *a recursive planner over the same parsed shape* —
//! [`legacy::build_metric_node`], which is `ae66648`'s body byte for byte
//! with two SUBSTITUTED child accessors — overflows on that same stack at
//! a quarter that width.
//!
//! It does **not** assert that the deleted function's own generated
//! frames overflow at that width. They might not: substituting the
//! accessors changes the live call path, and that can change inlining and
//! frame layout in either direction. The historical fact does not rest on
//! this gate — it was measured above — and this gate does not need the
//! historical fact to be a useful regression guard.
//!
//! # The two substituted accessors, and why they are substituted
//!
//! `ae66648`'s accessors reached a child through
//! `pulsus_logql::walk::child_of`. **This issue deletes `child_of`** —
//! restoring it, even under `#[cfg(test)]`, would reinstate the escape
//! hatch the change exists to remove, and a `#[cfg(test)]` item in a
//! dependency does not exist when a dependent compiles. So
//! `walk::child_of::<MetricScc>(MeNode::Expr(n), i)` becomes
//! [`legacy::nth_child`]/[`legacy::nth_node`], which obtain the same
//! reference from `walk::find_preorder` pruned at depth 1.
//!
//! What that substitution preserves is the VALUES: `the_restored_control_
//! plans_identically_to_the_converted_planner` compares both planners'
//! rendered trees and rejections over twelve shapes. What it does not
//! preserve — and is not claimed to — is the generated code. That is
//! exactly why the historical claim is carried by the measurement above
//! rather than by this control.
//!
//! Round 2 shipped a hand-written mirror of the recursion over a `Box`
//! shadow tree instead, claiming its frame was a lower bound on the
//! deleted function's. That claim was unsound — `#[inline(never)]` blocks
//! inlining but says nothing about frame SIZE, and source-level locals
//! induce no frame-size ordering — and the narrowing above is what
//! replaces it, rather than a third version of the same reasoning.
//!
//! # Where this lives, and why not in `tests/`
//!
//! The restored function calls six `plan.rs`-private items, so it has to
//! be compiled inside this module. It is a `#[cfg(test)]`-gated
//! `#[path]` sibling, the arrangement `plan_drop_order.rs` already uses
//! for the SCC-3 drop oracle. Everything lives under a single trailing
//! `mod tests`, so the two source censuses scoped to `src/logql/`
//! (`charge.rs`'s `SOURCES`, `logqltest_provenance`'s `PipelineInvalid`
//! count) see an EMPTY production region here — which is the truth: this
//! file ships in no binary.

#[cfg(test)]
mod tests {
    use std::process::Command;

    use pulsus_logql::walk;
    use pulsus_logql::{Expr, MetricExpr};

    use crate::logql::plan::{Plan, plan};
    use crate::logql::{Direction, PlanCtx, QueryParams, QuerySpec};

    // -----------------------------------------------------------------
    // The pinned-stack child-process harness
    // -----------------------------------------------------------------

    /// The stack every row runs on — a QUARTER of the 2 MiB a `std`
    /// thread and a tokio worker get by default, so the positive rows
    /// clear the real environment with room to spare.
    const S: usize = 256 * 1024;

    const CHILD_ENV: &str = "PULSUS_PLAN_STACK_CHILD";

    /// The child test's full path. Asserted-by-use: a rename makes the
    /// child run zero tests and `assert_child` fails on the "1 passed"
    /// check rather than passing vacuously.
    const CHILD_TEST: &str = "logql::plan::recursive_control::tests::plan_stack_child";

    fn child_mode() -> Option<String> {
        std::env::var(CHILD_ENV).ok()
    }

    fn on_stack(f: impl FnOnce() + Send + 'static) {
        std::thread::Builder::new()
            .stack_size(S)
            .spawn(f)
            .expect("spawn pinned-stack thread")
            .join()
            .expect("pinned-stack thread panicked");
    }

    fn spawn_child(mode: &str) -> std::process::Output {
        let exe = std::env::current_exe().expect("current_exe");
        Command::new(exe)
            .args([CHILD_TEST, "--exact", "--nocapture", "--test-threads=1"])
            .env(CHILD_ENV, mode)
            .output()
            .expect("spawn child test process")
    }

    fn assert_child_ok(mode: &str) {
        let out = spawn_child(mode);
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        assert!(
            out.status.success(),
            "child mode {mode:?} failed with {}\n--- stdout ---\n{stdout}\n--- stderr ---\n\
             {stderr}",
            out.status
        );
        assert!(
            stdout.contains("1 passed"),
            "child mode {mode:?} did not run exactly one test — has {CHILD_TEST} been \
             renamed?\n--- stdout ---\n{stdout}"
        );
    }

    /// Two assertions, not one: the child must FAIL, and it must fail
    /// with a stack overflow specifically. A panic, an `abort()` or any
    /// other non-zero exit reddens this rather than being read as the
    /// overflow the pairing claims.
    fn assert_child_overflowed(mode: &str) {
        let out = spawn_child(mode);
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        assert!(
            !out.status.success(),
            "control mode {mode:?} was expected to overflow but exited successfully\n{stderr}"
        );
        assert!(
            stderr.contains("stack overflow"),
            "control mode {mode:?} failed without a stack overflow: {}\n{stderr}",
            out.status
        );
    }

    // -----------------------------------------------------------------
    // The widest admissible input, DERIVED
    // -----------------------------------------------------------------

    /// The candidate flat metric-chain terms, each validated by the real
    /// parser below; the chain is built from whichever ENUMERATED
    /// candidate is cheapest per term.
    ///
    /// **The claim is a minimum over THIS LIST, not a derivation from
    /// the grammar.** A one-byte scalar joined by a one-byte arithmetic
    /// operator is the cheapest production enumerated here, at two bytes
    /// a term; nothing rules out a shorter one the grammar admits and
    /// this list omits, and if one appears it must be added here and the
    /// term count will rise. Stated rather than dressed up as
    /// exhaustive.
    ///
    /// A probe built on a convenient term understates the reachable
    /// depth by a large factor: #285's own reproducer
    /// (`count_over_time(…) or …`, 34 bytes a term) reaches ~3.8k terms
    /// where a two-byte term reaches ~65.5k.
    const FLAT_TERM_CANDIDATES: &[(&str, &str)] = &[
        ("1", "+"),
        ("1", "*"),
        ("0", "-"),
        ("1", " or "),
        (r#"count_over_time({app="a"}[5m])"#, " or "),
    ];

    /// #285's filed reproducer in shape: a flat `or` chain of range
    /// aggregations. Its own row, because that issue is a separate
    /// report and because it exercises the `Range` leaf arm (one
    /// `metric_plan` a term) where the cheapest chain exercises
    /// `Literal`.
    const OR_CHAIN_285: (&str, &str) = (r#"count_over_time({app="a"}[5m])"#, " or ");

    /// The widest chain of `(term, separator)` under
    /// [`pulsus_logql::MAX_QUERY_BYTES`], as `(text, term count)` —
    /// derived from the cap so it tracks #279 rather than pinning a
    /// literal.
    fn widest_flat_chain(term: &str, sep: &str, cap: usize) -> (String, usize) {
        let mut text = String::with_capacity(cap);
        let mut terms = 0usize;
        loop {
            let cost = term.len() + if terms > 0 { sep.len() } else { 0 };
            if text.len() + cost >= cap {
                break;
            }
            if terms > 0 {
                text.push_str(sep);
            }
            text.push_str(term);
            terms += 1;
        }
        (text, terms)
    }

    /// The cheapest ENUMERATED candidate, each proven to parse as a flat
    /// chain first — a shorter but invalid candidate would silently
    /// shrink the probe.
    fn cheapest_term() -> (&'static str, &'static str) {
        let mut best: Option<(&str, &str)> = None;
        for (term, sep) in FLAT_TERM_CANDIDATES {
            let probe = format!("{term}{sep}{term}{sep}{term}");
            assert!(
                pulsus_logql::parse(&probe).is_ok(),
                "candidate term {term:?}/{sep:?} does not parse as a flat chain"
            );
            let cost = term.len() + sep.len();
            if best.is_none_or(|(t, s)| cost < t.len() + s.len()) {
                best = Some((term, sep));
            }
        }
        best.expect("at least one candidate term")
    }

    // -----------------------------------------------------------------
    // The shape the defect needs
    // -----------------------------------------------------------------

    /// The length of `root`'s LEFT spine in edges — how many frames a
    /// walk that recurses `lhs` first holds at once.
    fn left_spine_depth(root: &MetricExpr) -> usize {
        let mut depth = 0usize;
        let mut cur = root;
        while let Some(child) = legacy::nth_child(cur, 0) {
            cur = child;
            depth += 1;
        }
        depth
    }

    /// Asserts the parsed chain IS the left-deep shape the defect needs:
    /// `terms - 1` binary nodes stacked on the `lhs` edge, every `rhs` a
    /// leaf.
    ///
    /// Without this the legs would show only that SOME deep structure
    /// exhausts the stack, not that query WIDTH became tree DEPTH
    /// through the `lhs` edge — which is the defect's mechanism.
    fn assert_left_deep_chain(root: &MetricExpr, terms: usize) {
        assert_eq!(
            left_spine_depth(root),
            terms - 1,
            "a flat {terms}-term chain must parse into a left spine of {} binary nodes",
            terms - 1
        );
        let mut cur = root;
        let mut seen = 0usize;
        while let Some(lhs) = legacy::nth_child(cur, 0) {
            let rhs = legacy::nth_child(cur, 1).expect("a binary node has a second child");
            assert!(
                legacy::nth_child(rhs, 0).is_none(),
                "the chain is not left-deep: the rhs at spine position {seen} has children"
            );
            cur = lhs;
            seen += 1;
        }
        assert_eq!(seen, terms - 1, "the left spine is not the whole chain");
    }

    fn ctx() -> PlanCtx<'static> {
        PlanCtx {
            db: "pulsus",
            streams_idx: "log_streams_idx",
            streams: "log_streams",
            samples: "log_samples",
            rollup_table: "log_metrics_5s",
            rollup_res_ns: 5_000_000_000,
            scan_budget_bytes: 50 * 1024 * 1024 * 1024,
            max_streams: 100_000,
            pipeline_scan_factor: 10,
        }
    }

    fn params() -> QueryParams {
        QueryParams {
            spec: QuerySpec::Instant {
                at_ns: 1_782_907_200_000_000_000,
            },
            limit: 100,
            direction: Direction::Backward,
        }
    }

    // -----------------------------------------------------------------
    // The control: `ae66648`'s body, two accessors substituted
    // -----------------------------------------------------------------

    /// `build_metric_node`'s body exactly as it stood at `ae66648`, with
    /// its two child accessors SUBSTITUTED. See this file's header for
    /// the substitution, its reason, and the claim split it forces.
    mod legacy {
        use std::ops::ControlFlow;

        use pulsus_logql::walk::{self, Step};
        use pulsus_logql::{BinModifier, MeNode, MetricExpr, MetricScc, VariantsExpr};

        use crate::logql::error::ReadError;
        use crate::logql::params::{PlanCtx, QueryParams, QuerySpec};
        use crate::logql::plan::{
            MetricNode, build_variants_node, metric_plan, parse_plan_number,
            parse_vector_agg_params, unwrap_vector_aggs, window_from,
        };

        /// The i-th child of `n`, or `None`. **The SUBSTITUTION**:
        /// `ae66648` called `walk::child_of`, which #293 deletes, so the
        /// same reference comes from `find_preorder` pruned at depth 1 —
        /// the driver every consumer already has. It is a loop, not a
        /// recursion, so it adds no per-node frames; it does change the
        /// live call path, which is why this file's gate claims a
        /// regression guard rather than the deleted function's own
        /// overflow width.
        pub(super) fn nth_child(n: &MetricExpr, i: usize) -> Option<&MetricExpr> {
            match nth_node(MeNode::Expr(n), i) {
                Some(MeNode::Expr(e)) => Some(e),
                _ => None,
            }
        }

        pub(super) fn nth_node<'a>(n: MeNode<'a>, i: usize) -> Option<MeNode<'a>> {
            let mut root = true;
            let mut seen = 0usize;
            walk::find_preorder::<MetricScc, MeNode<'a>>(n, |x| {
                if root {
                    root = false;
                    return ControlFlow::Continue(Step::Descend);
                }
                if seen == i {
                    return ControlFlow::Break(x);
                }
                seen += 1;
                ControlFlow::Continue(Step::Prune)
            })
        }

        fn scc2_child(n: &MetricExpr, i: usize) -> &MetricExpr {
            match nth_child(n, i) {
                Some(e) => e,
                None => unreachable!("`build_metric_node` indexes a child its arm declared"),
            }
        }

        fn scc2_variants(n: &MetricExpr) -> &VariantsExpr {
            match nth_node(MeNode::Expr(n), 0) {
                Some(MeNode::Var(v)) => v,
                _ => unreachable!("`MetricExpr::Variants` has exactly one `VariantsExpr` child"),
            }
        }

        /// Recursively plans a binary/literal metric expression into a
        /// [`MetricNode`] tree. Every (vector-agg-wrapped)
        /// range-aggregation operand becomes a [`MetricNode::Leaf`] via
        /// the ordinary [`metric_plan`] path, so per-leaf routing/rollup
        /// decisions are exactly what the same expression would get
        /// standalone.
        pub(super) fn build_metric_node(
            metric_expr: &MetricExpr,
            p: &QueryParams,
            ctx: &PlanCtx<'_>,
        ) -> Result<MetricNode, ReadError> {
            match metric_expr {
                MetricExpr::Literal(raw) => Ok(MetricNode::Scalar(parse_plan_number(
                    raw,
                    format_args!("scalar literal"),
                )?)),
                MetricExpr::VectorFn(raw) => Ok(MetricNode::VectorLit {
                    value: parse_plan_number(raw, format_args!("vector() value"))?,
                    window: window_from(p)?,
                }),
                MetricExpr::Variants(_) => build_variants_node(scc2_variants(metric_expr), p, ctx),
                MetricExpr::Binary { op, modifier, .. } => Ok(MetricNode::Binary {
                    op: *op,
                    return_bool: matches!(
                        modifier,
                        Some(BinModifier {
                            return_bool: true,
                            ..
                        })
                    ),
                    matching: modifier.as_ref().and_then(|m| m.matching.clone()),
                    lhs: walk::Child::new(build_metric_node(scc2_child(metric_expr, 0), p, ctx)?),
                    rhs: walk::Child::new(build_metric_node(scc2_child(metric_expr, 1), p, ctx)?),
                }),
                MetricExpr::Range { .. } => Ok(MetricNode::Leaf(Box::new(metric_plan(
                    metric_expr,
                    p,
                    ctx,
                    false,
                )?))),
                MetricExpr::Vector { .. } => {
                    let (base, raw_aggs) = unwrap_vector_aggs(metric_expr);
                    match base {
                        MetricExpr::Range { .. } => Ok(MetricNode::Leaf(Box::new(metric_plan(
                            metric_expr,
                            p,
                            ctx,
                            false,
                        )?))),
                        MetricExpr::Literal(_) => Err(ReadError::PipelineInvalid {
                            reason: "a vector aggregation cannot aggregate a bare scalar literal"
                                .to_string(),
                        }),
                        inner => Ok(MetricNode::VectorAgg {
                            aggs: parse_vector_agg_params(
                                &raw_aggs,
                                matches!(p.spec, QuerySpec::Range { .. }),
                            )?,
                            inner: walk::Child::new(build_metric_node(inner, p, ctx)?),
                        }),
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------
    // The legs
    // -----------------------------------------------------------------

    /// parse -> plan -> `Debug` -> `Clone` -> `PartialEq` -> drop over a
    /// flat chain, on the pinned stack, through the CONVERTED planner.
    fn drive_converted(text: &str, terms: usize) {
        assert!(
            text.len() < pulsus_logql::MAX_QUERY_BYTES,
            "the probe must be ADMISSIBLE: {} vs {}",
            text.len(),
            pulsus_logql::MAX_QUERY_BYTES
        );
        let expr = pulsus_logql::parse(text).expect("the widest chain parses");
        let Expr::Metric(root) = &expr else {
            panic!("expected a metric expression");
        };
        assert_left_deep_chain(root, terms);

        let planned = plan(&expr, &params(), &ctx()).expect("plan");
        let Plan::MetricBinary(node) = planned else {
            panic!("a flat chain must plan to the MetricBinary route");
        };

        let mut nodes = Vec::new();
        walk::postorder_into::<crate::logql::MetricNodeScc>(&node, &mut nodes);
        assert_eq!(
            nodes.len(),
            2 * terms - 1,
            "a left-deep chain of {terms} terms plans to {terms} leaves and {} combiners",
            terms - 1
        );
        drop(nodes);

        let mut sink = Counter(0);
        {
            use std::fmt::Write as _;
            write!(sink, "{node:?}").expect("compact Debug");
        }
        assert!(sink.0 > terms);

        let copy = node.clone();
        assert!(copy == node, "the clone of a {terms}-term plan differs");
        drop(copy);
        drop(node);
        drop(expr);
    }

    struct Counter(usize);

    impl std::fmt::Write for Counter {
        fn write_str(&mut self, s: &str) -> std::fmt::Result {
            self.0 += s.len();
            Ok(())
        }
    }

    #[test]
    fn plan_stack_child() {
        let Some(mode) = child_mode() else {
            return;
        };
        let cap = pulsus_logql::MAX_QUERY_BYTES;
        match mode.as_str() {
            "widest" => {
                let (term, sep) = cheapest_term();
                let (text, terms) = widest_flat_chain(term, sep, cap);
                println!(
                    "widest metric chain: term={term:?} sep={sep:?} terms={terms} bytes={}",
                    text.len()
                );
                assert!(
                    text.len() + 16 >= cap,
                    "the probe must fill the query-text cap, not sit comfortably inside it: \
                     {} vs {cap}",
                    text.len()
                );
                assert!(
                    terms > 65_000,
                    "the probe collapsed to {terms} terms — the chain is not being maximised \
                     over the cheapest ENUMERATED term"
                );
                on_stack(move || drive_converted(&text, terms));
            }
            "or_chain_285" => {
                let (term, sep) = OR_CHAIN_285;
                let (text, terms) = widest_flat_chain(term, sep, cap);
                println!("widest #285 or-chain: terms={terms} bytes={}", text.len());
                assert!(
                    text.len() + term.len() + sep.len() >= cap,
                    "the #285 probe must fill the query-text cap"
                );
                assert!(
                    terms > 3_000,
                    "the #285 probe collapsed to {terms} terms, below the measured abort point"
                );
                on_stack(move || drive_converted(&text, terms));
            }
            // The control: `ae66648`'s body with two substituted child
            // accessors, over the same parsed chain at a quarter the
            // width the positive rows plan.
            "legacy_quarter" => {
                let (term, sep) = cheapest_term();
                let (_, full_terms) = widest_flat_chain(term, sep, cap);
                let quarter = full_terms / 4;
                let (text, terms) =
                    widest_flat_chain(term, sep, quarter * (term.len() + sep.len()));
                assert_eq!(terms, quarter, "the quarter chain lost a term");
                on_stack(move || {
                    let expr = pulsus_logql::parse(&text).expect("parse");
                    let Expr::Metric(root) = &expr else {
                        panic!("expected a metric expression");
                    };
                    assert_left_deep_chain(root, terms);
                    println!("control: {terms} terms, left spine {}", terms - 1);
                    let planned = legacy::build_metric_node(root, &params(), &ctx())
                        .expect("the control planner");
                    println!("control planned a tree (it should not have got here)");
                    drop(planned);
                    drop(expr);
                });
            }
            other => panic!("unknown child mode {other}"),
        }
    }

    #[test]
    fn the_converted_planner_handles_the_widest_admissible_chain() {
        if child_mode().is_some() {
            return;
        }
        assert_child_ok("widest");
    }

    #[test]
    fn the_converted_planner_handles_the_widest_flat_or_chain_from_285() {
        if child_mode().is_some() {
            return;
        }
        assert_child_ok("or_chain_285");
    }

    /// The pairing, at the width this gate claims and no wider: on the
    /// SAME pinned `S`, a recursive planner over the same parsed chain
    /// aborts over a quarter of the terms the rows above plan.
    ///
    /// The control is `ae66648`'s `build_metric_node` byte for byte with
    /// two substituted child accessors (see the module header). It is
    /// therefore a REGRESSION GUARD — reintroduce a per-node recursion
    /// here and this reddens — not the evidence for what the deleted
    /// function did, which was measured directly and is recorded in that
    /// header.
    #[test]
    fn a_recursive_planner_over_the_same_chain_overflows_at_a_quarter_the_width() {
        if child_mode().is_some() {
            return;
        }
        assert_child_overflowed("legacy_quarter");
    }

    /// The left-deep assertion is not vacuous: the same operator and the
    /// same node count, parenthesised right-nested, has a left spine of
    /// depth one. If it held of any parse it would license the control
    /// to overflow for a reason the defect never had.
    #[test]
    fn the_left_deep_assertion_distinguishes_a_right_nested_chain() {
        if child_mode().is_some() {
            return;
        }
        let parsed = |q: &str| pulsus_logql::parse(q).expect("parse");
        let flat = parsed("1+1+1+1");
        let Expr::Metric(flat_root) = &flat else {
            panic!("metric expression")
        };
        assert_left_deep_chain(flat_root, 4);
        assert_eq!(left_spine_depth(flat_root), 3);

        let nested = parsed("1+(1+(1+1))");
        let Expr::Metric(nested_root) = &nested else {
            panic!("metric expression")
        };
        assert_eq!(
            left_spine_depth(nested_root),
            1,
            "a right-nested chain must NOT satisfy the left-deep shape the gates assert"
        );
    }

    /// The control and the shipped planner agree on every shape this
    /// compares — so the control is the deleted function's BEHAVIOUR, not
    /// merely some function that recurses.
    ///
    /// Stated as narrowly as it holds: this is plan/rejection equality,
    /// which the accessor substitution preserves. It says nothing about
    /// generated code or frame size, and the gate above does not ask it
    /// to.
    #[test]
    fn the_restored_control_plans_identically_to_the_converted_planner() {
        if child_mode().is_some() {
            return;
        }
        let queries = [
            "1",
            "1 + 2",
            "1 + 1 + 1 + 1",
            "1 + (1 + (1 + 1))",
            "vector(1)",
            r#"rate({a="b"}[5m]) + 1"#,
            r#"sum(rate({a="b"}[5m])) + 1"#,
            r#"sum(max(rate({a="b"}[5m]) + rate({c="d"}[5m])))"#,
            "sum(vector(1))",
            r#"variants(count_over_time({a="b"}[5m])) of ({a="b"}[5m]) + 1"#,
            "sum(1)",
            r#"rate({a="b"} |~ "(" [5m]) + 1"#,
        ];
        let c = ctx();
        let p = params();
        let mut compared = 0usize;
        for q in queries {
            let expr = pulsus_logql::parse(q).unwrap_or_else(|e| panic!("{q}: {e}"));
            let Expr::Metric(root) = &expr else {
                panic!("{q}: expected a metric expression")
            };
            let legacy = legacy::build_metric_node(root, &p, &c);
            let shipped = plan(&expr, &p, &c);
            let shipped = match shipped {
                Ok(Plan::MetricBinary(node)) => Ok(node),
                Ok(other) => panic!("{q}: expected MetricBinary, got {other:?}"),
                Err(e) => Err(e),
            };
            match (legacy, shipped) {
                (Ok(a), Ok(b)) => assert_eq!(
                    format!("{a:#?}"),
                    format!("{b:#?}"),
                    "{q}: the restored control and the shipped planner disagree"
                ),
                (Err(a), Err(b)) => assert_eq!(
                    format!("{a:?}"),
                    format!("{b:?}"),
                    "{q}: the two planners reject differently"
                ),
                (a, b) => panic!("{q}: one planner succeeded and the other did not: {a:?} / {b:?}"),
            }
            compared += 1;
        }
        assert!(compared >= 12, "only {compared} shapes compared");
    }
}
