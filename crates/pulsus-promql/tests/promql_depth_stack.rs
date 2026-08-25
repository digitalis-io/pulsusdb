//! Issue #262 (AC 7) — the production margin, measured on the shipped
//! configuration.
//!
//! Every row is a **pair**, on a `Builder::stack_size(2 MiB)` thread —
//! tokio's worker default, and the stack `pulsus-server`'s bare
//! `#[tokio::main]` gives every request handler — inside a name-filtered
//! re-exec'd child:
//!
//! * the **positive** leg takes the shape at depth `MAX_EXPR_DEPTH`
//!   through the production pipeline (`pulsus_promql::parse` -> `plan` ->
//!   `evaluate`) and exits 0;
//! * the **control** leg takes the SAME shape at depth 4,000 through the
//!   same pipeline, bypassing the cap by calling the vendored parser
//!   directly, and must exit non-zero **with `stack overflow` in
//!   stderr**.
//!
//! Pairing is what makes the positive leg mean anything. The toolchain is
//! pinned but not frozen: a bump, target or profile change can shrink
//! frames until a depth chosen today overflows nothing, and a bare "no
//! overflow" assertion would then be vacuously true. Measured at
//! `f916d7f`, the fifteen shapes below abort between **905** and
//! **1,221** levels in release on this stack, so 4,000 is inside the
//! overflow regime for every one of them and 250 has 3.6x of room.
//!
//! **This is the only gate that measures the shipped configuration, and
//! it does not run on a PR.** `.github/workflows/ci.yml` has no
//! `--release` leg at all, so this suite rides a `workflow_dispatch ||
//! schedule` job. A PR can go green without it ever having run; that gap
//! is stated rather than papered over with a debug-profile gate that
//! would pass while telling you nothing about a 2 MiB release binary.
//!
//! **Release-only, and not by preference.** Run in **debug** the very
//! first positive leg aborts: `lr:boundary` — `label_replace` nesting at
//! depth 250 — overflows a 2 MiB stack with `signal: 6 (SIGABRT)`,
//! because debug's worst-shape abort floor is **70**, not 905. A debug
//! build is therefore unprotected in the depth window 71-250; nothing
//! reaches that window unless we write it, and
//! `promqltest_corpus.rs::corpus_expression_depths_match_their_pinned_artifact`
//! is the tripwire that keeps it that way.
//!
//! ```text
//! PULSUS_PROMQL_DEPTH_STACK=1 cargo test --release -p pulsus-promql --test promql_depth_stack
//! ```

#[path = "stackgate/mod.rs"]
mod stackgate;

use pulsus_promql::{
    DEFAULT_LOOKBACK_MS, MAX_EXPR_DEPTH, PlanParams, PromqlError, SeriesData, evaluate, parse, plan,
};

/// The pinned stack every leg runs on.
const S: usize = 2 * 1024 * 1024;

/// The control leg's depth. Above every measured abort floor (905, the
/// `label_replace` shape) by 4.4x, so the control is inside the overflow
/// regime with margin of its own.
const CONTROL_DEPTH: usize = 4_000;

/// The fifteen shapes whose abort floors were measured for this issue.
/// `paren` and `unary` are deliberately absent: neither aborts at 4,000
/// (a unary chain folds to depth 1), so neither can serve as a control.
///
/// Each builder takes the TREE DEPTH it must produce, not a term or
/// nesting count — the two differ per shape, and getting that wrong
/// silently is exactly what the boundary oracle below catches.
const SHAPES: &[(&str, fn(usize) -> String)] = &[
    ("lr", lr),
    ("call", call),
    ("clamp", clamp),
    ("lj", lj),
    ("agg", agg),
    ("aggby", aggby),
    ("sumchain", sumchain),
    ("topk", topk),
    ("hq", hq),
    ("bin", bin),
    ("named", named),
    ("or", or),
    ("cmp", cmp),
    ("onchain", onchain),
    ("gl", gl),
];

fn nest(depth: usize, open: &str, close: &str, leaf: &str) -> String {
    let levels = depth - 1;
    format!("{}{leaf}{}", open.repeat(levels), close.repeat(levels))
}

fn lr(depth: usize) -> String {
    nest(depth, "label_replace(", r#", "d", "$1", "s", "(.*)")"#, "up")
}
fn call(depth: usize) -> String {
    nest(depth, "abs(", ")", "up")
}
fn clamp(depth: usize) -> String {
    nest(depth, "clamp(", ", 0, 1)", "up")
}
fn lj(depth: usize) -> String {
    nest(depth, "label_join(", r#", "d", "-", "s")"#, "up")
}
fn agg(depth: usize) -> String {
    nest(depth, "sum(", ")", "up")
}
fn aggby(depth: usize) -> String {
    nest(depth, "sum by (a) (", ")", "up")
}
fn topk(depth: usize) -> String {
    nest(depth, "topk(1, ", ")", "up")
}
fn hq(depth: usize) -> String {
    nest(depth, "histogram_quantile(0.9, ", ")", "up")
}

/// `sum(m0) + sum(m1) + …` — depth is `terms + 1`, because each term is
/// itself an `Aggregate` over a selector.
fn sumchain(depth: usize) -> String {
    let mut q = "sum(m0)".to_string();
    for i in 1..(depth - 1) {
        q.push_str(&format!(" + sum(m{i})"));
    }
    q
}
fn bin(depth: usize) -> String {
    format!("1{}", " + 1".repeat(depth - 1))
}
fn named(depth: usize) -> String {
    let mut q = "m0".to_string();
    for i in 1..depth {
        q.push_str(&format!(" + m{i}"));
    }
    q
}
fn or(depth: usize) -> String {
    let mut q = r#"up{i="0"}"#.to_string();
    for i in 1..depth {
        q.push_str(&format!(r#" or up{{i="{i}"}}"#));
    }
    q
}
fn cmp(depth: usize) -> String {
    format!("1{}", " > bool 1".repeat(depth - 1))
}
fn onchain(depth: usize) -> String {
    format!("up{}", " + on(a) up".repeat(depth - 1))
}
fn gl(depth: usize) -> String {
    format!("up{}", " + on(a) group_left(b, c) up".repeat(depth - 1))
}

fn instant_params() -> PlanParams {
    PlanParams {
        start_ms: 1_700_000_000_000,
        end_ms: 1_700_000_000_000,
        step_ms: 0,
        lookback_ms: DEFAULT_LOOKBACK_MS,
        experimental_functions: false,
    }
}

/// Plans and evaluates over an empty `SeriesData`. A planning or
/// evaluation ERROR is not a failure here — this leg measures stack
/// depth, not semantics, and the control leg is what proves the pipeline
/// really does recurse over these trees.
fn plan_and_evaluate(expr: &pulsus_promql::parser::Expr) {
    if let Ok(query_plan) = plan(expr, instant_params()) {
        let _ = evaluate(&query_plan, &SeriesData::new());
    }
}

/// The child entry point. One `#[test]`, dispatched by `CHILD_ENV`, so
/// the parent can name-filter it exactly.
#[test]
fn depth_stack_child() {
    let Some(mode) = stackgate::child_mode() else {
        return;
    };
    let (raw_shape, leg) = mode.split_once(':').expect("mode is `<shape>:<leg>`");
    let (shape, build) = SHAPES
        .iter()
        .find(|(name, _)| *name == raw_shape)
        .map(|(name, build)| (*name, *build))
        .unwrap_or_else(|| panic!("unknown shape {raw_shape}"));
    let leg = leg.to_string();

    match leg.as_str() {
        // The positive leg: the production pipeline, at the cap's own
        // boundary, through the guard.
        "boundary" => {
            stackgate::on_stack(S, move || {
                // The boundary IS the depth oracle: one level more must
                // be `ExprTooDeep { depth: MAX_EXPR_DEPTH + 1 }`, which
                // pins that `build(MAX_EXPR_DEPTH)` really is depth 250
                // and not something the builder got wrong by one.
                match parse(&build(MAX_EXPR_DEPTH + 1)) {
                    Err(PromqlError::ExprTooDeep { depth, limit }) => {
                        assert_eq!(depth, MAX_EXPR_DEPTH + 1, "{shape}");
                        assert_eq!(limit, MAX_EXPR_DEPTH, "{shape}");
                    }
                    other => panic!("{shape}: expected ExprTooDeep, got {other:?}"),
                }
                let expr = parse(&build(MAX_EXPR_DEPTH))
                    .unwrap_or_else(|e| panic!("{shape} at the boundary must parse: {e}"));
                plan_and_evaluate(&expr);
                println!("{shape}: parse -> plan -> evaluate at depth {MAX_EXPR_DEPTH} on {S}");
            });
        }
        // The control leg: the SAME shape well past the cap, reaching
        // the pipeline through the VENDORED parser so the guard cannot
        // reject it first. This is what proves `(2 MiB, 4000)` is inside
        // the overflow regime.
        "control" => {
            stackgate::on_stack(S, move || {
                let expr = promql_parser::parser::parse(&build(CONTROL_DEPTH))
                    .unwrap_or_else(|e| panic!("{shape} control must parse: {e}"));
                plan_and_evaluate(&expr);
                println!("{shape}: control at depth {CONTROL_DEPTH} did NOT overflow");
            });
        }
        other => panic!("unknown leg {other}"),
    }
}

/// Drives every shape's pair. One test rather than thirty so the suite's
/// shape list cannot drift from `SHAPES`.
#[test]
fn every_measured_shape_survives_the_cap_and_overflows_well_past_it() {
    if stackgate::child_mode().is_some() {
        return;
    }
    if !stackgate::gate_is_open() {
        eprintln!(
            "skipped: set {}=1 to run the pinned-stack legs (they abort child processes \
             by design, and only the RELEASE profile measures the shipped configuration)",
            stackgate::GATE_ENV
        );
        return;
    }
    assert_eq!(SHAPES.len(), 15, "the measured shape set");
    for (shape, _) in SHAPES {
        stackgate::assert_child_ok("depth_stack_child", &format!("{shape}:boundary"));
        stackgate::assert_child_overflowed("depth_stack_child", &format!("{shape}:control"));
    }
}
