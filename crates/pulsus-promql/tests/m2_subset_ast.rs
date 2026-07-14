//! Issue #29 (M2 `promql-parser` validation spike), plan amendment F2's
//! "systematic golden matrix": one committed `{:#?}` AST snapshot per M2
//! construct × modifier cell — not example-of-choice picks. Cells are
//! enumerated exactly per the architect plan amendment: every matcher op
//! incl. `__name__`; `offset` on both vector and matrix selectors; each
//! range fn / `*_over_time` fn over `[5m]`; each aggregation × `by`/
//! `without` (`topk`/`bottomk` incl. the scalar param); binary ops ×
//! {vector-scalar, vector-vector} × {arithmetic, comparison} × {plain,
//! bool, `on(...)`, `ignoring(...)`} (only the combinations that are valid
//! PromQL syntax — `bool` never applies to arithmetic ops, `on`/`ignoring`
//! never apply to a scalar operand); `histogram_quantile`; and 3 explicit
//! precedence goldens. Every case exercises the upstream-shaped AST
//! (`promql_parser::parser::Expr`) the #31 planner will consume directly —
//! this file is what proves the *shape* fidelity that
//! `upstream_parser_corpus.rs`'s accept/reject + round-trip gate cannot
//! (the upstream Go `expected` AST literal is not portable to Rust — see
//! docs/decisions/0002-promql-parser-selection.md).
//!
//! No `insta`/snapshot crate: `assert_eq!` against a committed
//! `golden/<cell>.txt` file, matching this workspace's established
//! golden-file convention (`pulsus-read/tests/explain_indexes.rs`,
//! `pulsus-logql/tests/snapshots.rs`).
//!
//! `TokenType`'s `Debug` renders the lexer's internal numeric token ID
//! (e.g. `TokenType(24)`), not an operator name — that is genuinely what
//! `{:#?}` produces (`promql_parser::parser::token::TokenType` wraps a
//! `u16` with only a derived `Debug`); the #31 planner compares `op`
//! against named constants (`token::T_ADD`, ...), not the numeral, so this
//! is a golden-readability wrinkle, not a shape problem.

macro_rules! golden_test {
    ($test_name:ident, $golden:literal, $input:expr) => {
        #[test]
        fn $test_name() {
            let input: &str = $input;
            let expr = promql_parser::parser::parse(input)
                .unwrap_or_else(|e| panic!("expected {input:?} to parse, got {e}"));
            let actual = format!("{expr:#?}\n");
            assert_eq!(
                actual,
                include_str!(concat!("golden/", $golden, ".txt")),
                "input: {input}"
            );
        }
    };
}

// ---------------------------------------------------------------------
// Selectors: every matcher op, incl. `__name__`. The bare metric-name
// prefix cases below carry an implicit `__name__` (via `VectorSelector.name`,
// not a `Matcher`); `selector_name_matcher_{re,neq}` additionally cover
// `__name__` written as an *explicit* brace matcher (code review finding
// 3, issue #29) — the AST shape differs (`name: None`, an explicit
// `Matcher { name: "__name__", .. }` entry instead). A bare `!=`/`!~`
// matcher alone is not valid PromQL (Prometheus/`promql-parser` both
// reject a selector with no matcher that excludes the empty string), so
// `selector_name_matcher_neq` pairs it with a second, ordinary matcher.
// ---------------------------------------------------------------------

golden_test!(
    selector_matcher_eq,
    "selector_matcher_eq",
    r#"foo{env="prod"}"#
);
golden_test!(
    selector_matcher_neq,
    "selector_matcher_neq",
    r#"foo{env!="prod"}"#
);
golden_test!(
    selector_matcher_re,
    "selector_matcher_re",
    r#"foo{env=~"prod|staging"}"#
);
golden_test!(
    selector_matcher_nre,
    "selector_matcher_nre",
    r#"foo{env!~"prod|staging"}"#
);
golden_test!(
    selector_name_matcher_re,
    "selector_name_matcher_re",
    r#"{__name__=~"foo|bar"}"#
);
golden_test!(
    selector_name_matcher_neq,
    "selector_name_matcher_neq",
    r#"{__name__!="foo", job="test"}"#
);
golden_test!(
    selector_bare_metric_name,
    "selector_bare_metric_name",
    "foo"
);

// ---------------------------------------------------------------------
// `offset` on both vector and matrix selectors.
// ---------------------------------------------------------------------

golden_test!(
    offset_vector_selector,
    "offset_vector_selector",
    "foo offset 5m"
);
golden_test!(
    offset_matrix_selector,
    "offset_matrix_selector",
    "foo[5m] offset 5m"
);

// ---------------------------------------------------------------------
// Matrix selector on its own, each range fn, each `*_over_time` fn.
// ---------------------------------------------------------------------

golden_test!(matrix_selector_range, "matrix_selector_range", "foo[5m]");
golden_test!(range_fn_rate, "range_fn_rate", "rate(foo[5m])");
golden_test!(range_fn_irate, "range_fn_irate", "irate(foo[5m])");
golden_test!(range_fn_increase, "range_fn_increase", "increase(foo[5m])");
golden_test!(range_fn_delta, "range_fn_delta", "delta(foo[5m])");
golden_test!(over_time_avg, "over_time_avg", "avg_over_time(foo[5m])");
golden_test!(over_time_min, "over_time_min", "min_over_time(foo[5m])");
golden_test!(over_time_max, "over_time_max", "max_over_time(foo[5m])");
golden_test!(over_time_sum, "over_time_sum", "sum_over_time(foo[5m])");
golden_test!(
    over_time_count,
    "over_time_count",
    "count_over_time(foo[5m])"
);

// ---------------------------------------------------------------------
// Aggregations × {by, without} (topk/bottomk incl. the scalar param).
// ---------------------------------------------------------------------

golden_test!(agg_sum_by, "agg_sum_by", "sum by (env) (foo)");
golden_test!(
    agg_sum_without,
    "agg_sum_without",
    "sum without (env) (foo)"
);
golden_test!(agg_avg_by, "agg_avg_by", "avg by (env) (foo)");
golden_test!(
    agg_avg_without,
    "agg_avg_without",
    "avg without (env) (foo)"
);
golden_test!(agg_min_by, "agg_min_by", "min by (env) (foo)");
golden_test!(
    agg_min_without,
    "agg_min_without",
    "min without (env) (foo)"
);
golden_test!(agg_max_by, "agg_max_by", "max by (env) (foo)");
golden_test!(
    agg_max_without,
    "agg_max_without",
    "max without (env) (foo)"
);
golden_test!(agg_count_by, "agg_count_by", "count by (env) (foo)");
golden_test!(
    agg_count_without,
    "agg_count_without",
    "count without (env) (foo)"
);
golden_test!(agg_topk_by, "agg_topk_by", "topk by (env) (5, foo)");
golden_test!(
    agg_topk_without,
    "agg_topk_without",
    "topk without (env) (5, foo)"
);
golden_test!(
    agg_bottomk_by,
    "agg_bottomk_by",
    "bottomk by (env) (5, foo)"
);
golden_test!(
    agg_bottomk_without,
    "agg_bottomk_without",
    "bottomk without (env) (5, foo)"
);

// ---------------------------------------------------------------------
// Binary ops × {vector-scalar, vector-vector} × {arithmetic, comparison} ×
// {plain, bool, on(...), ignoring(...)} — only syntactically-valid
// combinations: `bool` never applies to arithmetic ops; `on`/`ignoring`
// never apply when one operand is a scalar (no vector matching to do).
// ---------------------------------------------------------------------

golden_test!(
    binop_vector_scalar_arith_plain,
    "binop_vector_scalar_arith_plain",
    "foo * 2"
);
golden_test!(
    binop_vector_scalar_cmp_plain,
    "binop_vector_scalar_cmp_plain",
    "foo > 2"
);
golden_test!(
    binop_vector_scalar_cmp_bool,
    "binop_vector_scalar_cmp_bool",
    "foo > bool 2"
);
golden_test!(
    binop_vector_vector_arith_plain,
    "binop_vector_vector_arith_plain",
    "foo + bar"
);
golden_test!(
    binop_vector_vector_arith_on,
    "binop_vector_vector_arith_on",
    "foo + on(env) bar"
);
golden_test!(
    binop_vector_vector_arith_ignoring,
    "binop_vector_vector_arith_ignoring",
    "foo + ignoring(env) bar"
);
golden_test!(
    binop_vector_vector_cmp_plain,
    "binop_vector_vector_cmp_plain",
    "foo > bar"
);
golden_test!(
    binop_vector_vector_cmp_bool,
    "binop_vector_vector_cmp_bool",
    "foo > bool bar"
);
golden_test!(
    binop_vector_vector_cmp_on,
    "binop_vector_vector_cmp_on",
    "foo > on(env) bar"
);
golden_test!(
    binop_vector_vector_cmp_ignoring,
    "binop_vector_vector_cmp_ignoring",
    "foo > ignoring(env) bar"
);

// ---------------------------------------------------------------------
// `histogram_quantile`.
// ---------------------------------------------------------------------

golden_test!(
    histogram_quantile_basic,
    "histogram_quantile_basic",
    "histogram_quantile(0.9, rate(foo_bucket[5m]))"
);

// ---------------------------------------------------------------------
// Precedence — the highest-risk AST shape (plan amendment F2).
// ---------------------------------------------------------------------

golden_test!(
    precedence_add_then_mul,
    "precedence_add_then_mul",
    "a + b * c"
);
golden_test!(
    precedence_mul_then_add,
    "precedence_mul_then_add",
    "a * b + c"
);
golden_test!(precedence_neg_pow, "precedence_neg_pow", "-a ^ b");
