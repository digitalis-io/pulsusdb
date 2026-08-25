//! Thin re-export of the vendored, patched `promql-parser` crate (issue
//! #31, docs/decisions/0003) — the single import surface #32/#13 and this
//! crate's own `plan` module use, so no other module in this workspace
//! ever imports `promql_parser` directly.

use crate::error::PromqlError;

pub use promql_parser::label::{
    MatchOp as PLabelMatchOp, Matcher as PMatcher, Matchers as PMatchers,
};
pub use promql_parser::parser::value::ValueType;
pub use promql_parser::parser::{
    AggregateExpr, AtModifier, BinModifier, BinaryExpr, Call, DurationExpr, Expr, LabelModifier,
    MatrixSelector, NumberLiteral, Offset, ParenExpr, StringLiteral, SubqueryExpr, UnaryExpr,
    VectorMatchCardinality, VectorSelector, token,
};

/// Parses `q` into the vendored parser's AST. The `Err` case's `Display`
/// carries the parser's own error text verbatim — see [`PromqlError::Parse`].
///
/// **The depth cap runs here (issue #262)** — after the vendored parser
/// returns and before any caller can plan or evaluate. The placement is
/// not a convention but a property of the crate graph: `promql-parser`
/// appears in exactly one manifest in this workspace
/// (`crates/pulsus-promql/Cargo.toml`), and this line is the only
/// non-test call of `promql_parser::parser::parse` inside it, so no
/// production code in this workspace can obtain an `Expr` without
/// passing the guard — including entry points added later. See
/// [`crate::limits`] for why the check and the teardown are both
/// iterative.
pub fn parse(q: &str) -> Result<Expr, PromqlError> {
    let expr = promql_parser::parser::parse(q).map_err(PromqlError::Parse)?;
    let (depth, _capacity) = crate::limits::depth_and_capacity(&expr);
    if depth > crate::limits::MAX_EXPR_DEPTH {
        // The tree may be far deeper than MAX_EXPR_DEPTH, and letting it
        // fall out of scope would recurse once per level: measured
        // drop-glue abort at depth 43,564 release / 21,718 debug on a
        // 2 MiB stack.
        crate::limits::dismantle(expr);
        return Err(PromqlError::ExprTooDeep {
            depth,
            limit: crate::limits::MAX_EXPR_DEPTH,
        });
    }
    Ok(expr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_a_simple_selector() {
        assert!(parse("up").is_ok());
    }

    /// Issue #82 code review round 1 (finding 1): a deeply nested input
    /// whose only defect is an all-empty-matching selector must reject
    /// AND unwind without overflowing the stack. **Issue #82 v5**
    /// relocated the rejection from the post-parse deferred walk onto
    /// the innermost `unary_expr` reduction wrapping this shape
    /// (`reject_empty_operand`, vendored PATCHES.md #6) — strictly
    /// faster (eager short-circuit, no full-tree build) and still
    /// stack-safe: on a 2 MiB test-thread stack the GRAMMAR itself
    /// (instrumented, independent of this check) overflows at 9_000
    /// repetitions of this unit for fully VALID input (`-m-1`), and
    /// survives 8_000. This test pins that the whole path (parse →
    /// eager reject) completes at 4_000 repetitions (~8k nested `Expr`
    /// nodes — half the measured grammar bound). `!=` rather than `=~`
    /// keeps the case fast (no per-matcher regex compilation) while
    /// still matching the empty string.
    #[test]
    fn deep_all_empty_matcher_input_is_rejected_and_destroyed_without_overflow() {
        let input = "(".to_string() + &r#"-{x!="a"}-1"#.repeat(4_000) + ")";
        let err = parse(&input).unwrap_err();
        assert!(
            err.to_string()
                .contains("vector selector must contain at least one non-empty matcher"),
            "{err}"
        );
    }

    /// Issue #82 v5/v6 (retroactive re-review finding 2): the CI-visible
    /// twin of the vendored `test_issue_82_info_empty_matcher_parity`
    /// suite (this crate rides `cargo test --workspace`; the vendored
    /// crate is its own cargo workspace and does not). `info(m, {})` —
    /// an empty label-selector as `info()`'s SECOND argument — parses;
    /// `info({})` (empty in the FIRST argument) and `info(m, ({}))`
    /// (paren-wrapped, upstream-parity per plan v6) still reject.
    #[test]
    fn info_second_argument_empty_matcher_parses_but_first_argument_and_paren_wrapped_do_not() {
        assert!(parse("info(m, {})").is_ok());
        assert!(parse(r#"info(m, {x=~".*"})"#).is_ok());
        assert!(parse("info(m, {}@5)").is_ok());
        assert!(parse("info(m, {} offset 5m)").is_ok());

        for q in [
            "info({})",
            r#"info({}, {foo="bar"})"#,
            "info(m, ({}))",
            "info(m, (({})))",
        ] {
            let err = parse(q).unwrap_err();
            assert!(
                err.to_string()
                    .contains("vector selector must contain at least one non-empty matcher"),
                "{q}: {err}"
            );
        }
    }

    /// Issue #132: the CI-visible twin of the vendored
    /// `test_issue_132_info_direct_selector_parity` suite (vendored
    /// PATCHES.md #7 — the vendored crate is its own cargo workspace and
    /// does not ride `cargo test --workspace`). `info()`'s second
    /// argument, when vector-typed, must be a DIRECT name-less
    /// `VectorSelector` (Prometheus v3.13.0 `parse.go:846-859`): a named
    /// selector rejects `expected label selectors only, got vector
    /// selector instead`; wrapper/other vector-typed nodes reject
    /// `expected label selectors only`; in-place `@`/`offset` selector
    /// fields stay accepted; a non-vector arg keeps its type error
    /// first; `info(m, ({}))` keeps #82's empty-matcher message.
    #[test]
    fn info_second_argument_must_be_a_direct_nameless_selector() {
        for q in [
            r#"info(m, {job="x"})"#,
            r#"info(m, {job="x"} offset 5m)"#,
            r#"info(m, {job="x"} @ 5)"#,
            "info(m)",
            r#"(({job="x"}))"#,
        ] {
            assert!(parse(q).is_ok(), "{q}: {:?}", parse(q));
        }

        for (q, msg) in [
            (r#"info(m, ({job="x"}))"#, "expected label selectors only"),
            (r#"info(m, (({job="x"})))"#, "expected label selectors only"),
            ("info(m, sum(x))", "expected label selectors only"),
            ("info(m, m2 + m3)", "expected label selectors only"),
            ("info(m, rate(x[5m]))", "expected label selectors only"),
            (
                r#"info(m, target_info{job="x"})"#,
                "expected label selectors only, got vector selector instead",
            ),
            (
                "info(m, 1)",
                "expected type vector in call to function 'info', got scalar",
            ),
            (
                "info(m, ({}))",
                "vector selector must contain at least one non-empty matcher",
            ),
        ] {
            let err = parse(q).unwrap_err();
            assert_eq!(err.to_string(), msg, "{q}");
        }
    }

    /// Issue #82 v5 (retroactive re-review finding 1): a range-wrapped
    /// empty selector rejects without ever reaching the deferred
    /// post-parse walk, even nested behind ordinary function calls —
    /// the `check_ast_for_matrix_selector` eager check (vendored
    /// PATCHES.md #6).
    #[test]
    fn matrix_wrapped_empty_matcher_rejects_without_overflow() {
        let err = parse("rate({}[5m])").unwrap_err();
        assert!(err.to_string().contains("non-empty matcher"), "{err}");

        let err = parse("abs(rate({}[5m]))").unwrap_err();
        assert!(err.to_string().contains("non-empty matcher"), "{err}");

        let deep = "abs(".repeat(10_000) + "rate({}[5m])" + &")".repeat(10_000);
        let err = parse(&deep).unwrap_err();
        assert!(err.to_string().contains("non-empty matcher"), "{err}");
    }

    /// Issue #128: the CI-visible twin of the vendored
    /// `test_issue_128_ast_start_offsets` suite (vendored PATCHES.md #8,
    /// AST-metadata class — the vendored crate is its own cargo workspace
    /// and does not ride `cargo test --workspace`). Every parse-produced
    /// node carries the start byte offset of its first token
    /// (`Expr::pos_start()`), matching upstream Prometheus's
    /// `Expr.PositionRange().Start` (v3.13.0, pin `40af9c2`) — the input
    /// to `annotations::start_pos_input`'s `(<line>:<col>)` rendering.
    /// Pinned over a multi-line query so byte offsets are proven to run
    /// across `\n`s, and over every Expr kind (paren-wrapped call
    /// argument included — upstream keeps the `ParenExpr` in `e.Args`, so
    /// `(0.9)`'s position is the `(`).
    #[test]
    fn parsed_nodes_carry_exact_start_byte_offsets() {
        // Offsets (bytes):     0123456789...
        let input = "sum by (a) (\n  rate(foo{x=\"y\"}[5m])\n)\n+ histogram_quantile((0.9), up)";
        // Line 1: `sum by (a) (` = bytes 0..12, `\n` at 12.
        // Line 2: `  rate(foo{x="y"}[5m])` = bytes 13..35 (`rate` at 15,
        //         `foo` at 20), `\n` at 35.
        // Line 3: `)` at 36, `\n` at 37.
        // Line 4: `+ histogram_quantile((0.9), up)` — `+` at 38,
        //         `histogram_quantile` at 40, its `(` at 58, `(0.9)` at
        //         59, `0.9` at 60, `up` at 66.
        let expr = parse(input).unwrap();
        assert_eq!(expr.pos_start(), Some(0), "binary = its lhs");
        let (lhs, rhs) = match &expr {
            Expr::Binary(bin) => (bin.lhs.as_ref(), bin.rhs.as_ref()),
            other => panic!("expected Binary, got {other:?}"),
        };
        let agg = match lhs {
            Expr::Aggregate(agg) => agg,
            other => panic!("expected Aggregate, got {other:?}"),
        };
        assert_eq!(agg.pos.start(), Some(0), "sum");
        assert_eq!(agg.expr.pos_start(), Some(15), "rate, across a newline");
        let rate_args = match agg.expr.as_ref() {
            Expr::Call(call) => &call.args.args,
            other => panic!("expected Call, got {other:?}"),
        };
        assert_eq!(
            rate_args[0].pos_start(),
            Some(20),
            "matrix selector starts at its vector selector"
        );
        assert_eq!(rhs.pos_start(), Some(40), "histogram_quantile");
        let hq_args = match rhs {
            Expr::Call(call) => &call.args.args,
            other => panic!("expected Call, got {other:?}"),
        };
        assert_eq!(hq_args[0].pos_start(), Some(59), "(0.9) starts at the `(`");
        match hq_args[0].as_ref() {
            Expr::Paren(p) => assert_eq!(p.expr.pos_start(), Some(60), "0.9"),
            other => panic!("expected Paren, got {other:?}"),
        }
        assert_eq!(hq_args[1].pos_start(), Some(66), "up");

        // The remaining Expr kinds, single-line.
        for (q, expected) in [
            ("-foo", 0usize),     // Unary
            ("-1", 0),            // sign-collapsed NumberLiteral
            ("\"abc\"", 0),       // StringLiteral
            ("foo[5m:1m]", 0),    // Subquery = its inner expr
            ("foo offset 5m", 0), // offset never moves the start
        ] {
            match parse(q) {
                Ok(e) => assert_eq!(e.pos_start(), Some(expected), "{q}"),
                Err(e) => panic!("expected {q:?} to parse, got {e}"),
            }
        }

        // Hand-built nodes carry no position (the planner's
        // `unwrap_or(0)` fallback input).
        assert_eq!(Expr::from(1.0).pos_start(), None);
    }

    /// Issue #262 (AC 1, parse-level twin). The depth cap's boundary
    /// over real query text rather than a constructed tree: the deepest
    /// ACCEPTED expression has depth exactly `MAX_EXPR_DEPTH`, and one
    /// level more is `ExprTooDeep` naming both the depth measured and
    /// the limit.
    ///
    /// Term count is NOT depth, which is the arithmetic a user will get
    /// wrong: `m0 + … + m249` is 250 terms at depth 250, while
    /// `sum(m0) + … + sum(m248)` is 249 terms at depth 250 — each term
    /// is itself an `Aggregate` over a selector.
    ///
    /// These parse only; they never plan or evaluate, so they are safe
    /// on a default 2 MiB stack in both profiles.
    #[test]
    fn the_depth_cap_accepts_the_boundary_and_rejects_one_level_deeper() {
        let named = |terms: usize| {
            let mut q = "m0".to_string();
            for i in 1..terms {
                q.push_str(&format!(" + m{i}"));
            }
            q
        };
        let summed = |terms: usize| {
            let mut q = "sum(m0)".to_string();
            for i in 1..terms {
                q.push_str(&format!(" + sum(m{i})"));
            }
            q
        };

        // (query, depth of the tree it builds)
        for (query, depth) in [(named(250), 250), (summed(249), 250)] {
            assert!(
                parse(&query).is_ok(),
                "depth {depth} is the deepest ACCEPTED expression"
            );
        }
        for (query, depth) in [(named(251), 251), (summed(250), 251)] {
            match parse(&query) {
                Err(PromqlError::ExprTooDeep { depth: got, limit }) => {
                    assert_eq!(got, depth);
                    assert_eq!(limit, crate::limits::MAX_EXPR_DEPTH);
                }
                other => panic!("expected ExprTooDeep at depth {depth}, got {other:?}"),
            }
        }
    }

    /// Issue #262 non-vacuity: a 2,001-byte query that must NOT be
    /// refused. The parser folds a unary chain into a single
    /// `NumberLiteral` (depth 1), so a cap implemented on query bytes,
    /// token count or a parse-time counter fails this case and a cap on
    /// the BUILT TREE's depth passes it.
    #[test]
    fn a_two_kilobyte_unary_chain_folds_to_depth_one_and_is_accepted() {
        let query = format!("{}1", "- ".repeat(1_000));
        assert_eq!(query.len(), 2_001);
        let expr = parse(&query).expect("a folded unary chain is depth 1");
        assert!(matches!(expr, Expr::NumberLiteral(_)));
    }

    /// Issue #262: the reject path tears the tree down iteratively, so a
    /// rejection far past the cap returns rather than aborting. 60,000
    /// terms is above BOTH measured drop-glue floors (43,564 release /
    /// 21,718 debug on 2 MiB) — but parsing that chain costs minutes
    /// (the vendored parser is superlinear in chain length), so this
    /// standing test runs a cheap depth-1,000 rejection and the
    /// out-of-process 100,000-level teardown gate lives in
    /// `limits.rs`'s own tests.
    #[test]
    fn an_over_deep_query_is_rejected_rather_than_planned() {
        let mut query = "1".to_string();
        query.push_str(&" + 1".repeat(999));
        match parse(&query) {
            Err(PromqlError::ExprTooDeep { depth, limit }) => {
                assert_eq!(depth, 1_000);
                assert_eq!(limit, crate::limits::MAX_EXPR_DEPTH);
            }
            other => panic!("expected ExprTooDeep, got {other:?}"),
        }
    }

    #[test]
    fn parse_rejects_invalid_syntax_with_the_parser_own_message() {
        let err = parse("up{").unwrap_err();
        match err {
            PromqlError::Parse(msg) => assert!(!msg.is_empty()),
            other => panic!("expected Parse, got {other:?}"),
        }
    }
}
