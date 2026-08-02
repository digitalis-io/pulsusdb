//! One test per out-of-subset construct (docs/features.md §2 "LogQL —
//! parity (M6)") asserting `NotYetSupported` names it, plus malformed-
//! syntax tests asserting byte offset and message content, and the
//! `!=`/`!~` disambiguation tests mandated by the review cycles
//! (architect plan amendments 1-3).

use pulsus_logql::{LogQlError, parse};

fn assert_not_yet_supported(query: &str, construct: &str) {
    match parse(query) {
        Err(LogQlError::NotYetSupported { construct: got, .. }) => {
            assert_eq!(got, construct, "query: {query}")
        }
        other => panic!("expected {query:?} to be NotYetSupported({construct:?}), got {other:?}"),
    }
}

// --- Vector-matching modifiers (issue #91): now fully parsed, no longer
// --- `NotYetSupported`. A grammar violation is a positional
// --- `UnexpectedToken`, never `NotYetSupported`. ---

#[test]
fn every_vector_matching_modifier_now_parses() {
    for query in [
        r#"rate({a="b"}[5m]) + on(x) rate({a="c"}[5m])"#,
        r#"rate({a="b"}[5m]) + ignoring(x) rate({a="c"}[5m])"#,
        r#"rate({a="b"}[5m]) + on(x) group_left rate({a="c"}[5m])"#,
        r#"rate({a="b"}[5m]) + on(x) group_right(y) rate({a="c"}[5m])"#,
    ] {
        parse(query).unwrap_or_else(|e| panic!("query {query:?} should parse now: {e}"));
    }
}

#[test]
fn a_matching_modifier_after_bool_parses() {
    parse(r#"rate({a="b"}[5m]) > bool on(x) rate({a="c"}[5m])"#)
        .expect("bool + matching should parse");
}

#[test]
fn an_incomplete_matching_clause_is_an_unexpected_token_not_not_yet_supported() {
    // A missing paren after `on` is a positional grammar error, not a
    // `NotYetSupported` for the keyword (issue #91 AC2).
    match parse(r#"rate({a="b"}[5m]) + on rate({a="c"}[5m])"#) {
        Err(LogQlError::UnexpectedToken { .. }) => {}
        other => panic!("expected UnexpectedToken for a bare `on`, got {other:?}"),
    }
}

// --- Aggregation parameter arity (issue M6-10) ---

#[test]
fn quantile_over_time_without_a_parameter_is_rejected() {
    match parse(r#"quantile_over_time({a="b"}[5m])"#) {
        Err(LogQlError::UnexpectedToken { expected, .. }) => {
            assert!(expected.contains("quantile parameter"), "{expected}");
        }
        other => panic!("expected the missing quantile parameter to be rejected, got {other:?}"),
    }
}

#[test]
fn a_parameter_on_a_parameterless_range_aggregation_is_rejected() {
    match parse(r#"count_over_time(0.5, {a="b"}[5m])"#) {
        Err(LogQlError::UnexpectedToken { expected, .. }) => {
            assert!(expected.contains("'{'"), "{expected}");
        }
        other => panic!("expected a stray count_over_time parameter to be rejected, got {other:?}"),
    }
}

#[test]
fn topk_without_a_parameter_is_rejected() {
    match parse(r#"topk(rate({a="b"}[5m]))"#) {
        Err(LogQlError::UnexpectedToken { expected, .. }) => {
            assert!(expected.contains("k parameter"), "{expected}");
        }
        other => panic!("expected the missing topk k to be rejected, got {other:?}"),
    }
}

// --- Loki-exact `k` validation, shared by topk/bottomk/approx_topk
// --- (issue #221, root-cause fix: `topk(0, ...)` is now a 400, no longer
// --- an empty 200 — adjudicated).

#[test]
fn a_zero_k_is_rejected_as_not_positive_for_every_k_selection() {
    for op in ["topk", "bottomk", "approx_topk"] {
        match parse(&format!(r#"{op}(0, rate({{a="b"}}[5m]))"#)) {
            Err(err @ LogQlError::AggregationParamNotPositive { .. }) => {
                let msg = err.to_string();
                assert!(
                    msg.contains(&format!(
                        "invalid parameter (must be greater than 0) {op}(0"
                    )),
                    "{msg}"
                );
            }
            other => panic!("expected {op}(0, ...) to be rejected, got {other:?}"),
        }
    }
}

#[test]
fn a_non_integer_k_is_rejected_for_every_k_selection() {
    for op in ["topk", "bottomk", "approx_topk"] {
        match parse(&format!(r#"{op}(2.5, rate({{a="b"}}[5m]))"#)) {
            Err(err @ LogQlError::InvalidAggregationParam { .. }) => {
                let msg = err.to_string();
                assert!(
                    msg.contains(&format!("invalid parameter {op}(2.5,")),
                    "{msg}"
                );
            }
            other => panic!("expected {op}(2.5, ...) to be rejected, got {other:?}"),
        }
    }
}

#[test]
fn a_negative_k_is_rejected_when_parsed_as_not_positive() {
    // The lexer has no negative NUMBER token (`topk(-1, ...)` fails on the
    // `-` exactly as the reference's `syntax error: unexpected ,` class);
    // a programmatic negative that DOES tokenize (none today) would take
    // the not-positive arm — pin the boundary with k=0 above and the
    // syntax-shape here.
    match parse(r#"topk(-1, rate({a="b"}[5m]))"#) {
        Err(LogQlError::UnexpectedToken { .. }) => {}
        other => panic!("expected topk(-1, ...) to be rejected, got {other:?}"),
    }
}

/// The `k` checks fire at reduce time in the reference
/// (`mustNewVectorAggregationExpr` runs only once the whole call parsed),
/// so a syntax error inside the call still wins over a bad `k`.
#[test]
fn a_syntax_error_inside_the_call_wins_over_a_bad_k() {
    match parse(r#"topk(0, rate({a="b"}[5m])"#) {
        Err(LogQlError::UnexpectedEof { .. }) => {}
        other => panic!("expected the unclosed call to fail on EOF, got {other:?}"),
    }
}

// --- `approx_topk` grouping rejection (issue #221; reference:
// --- `grouping not allowed for approx_topk aggregation`).

#[test]
fn approx_topk_rejects_a_prefix_grouping() {
    match parse(r#"approx_topk by (lvl) (2, rate({a="b"}[5m]))"#) {
        Err(err @ LogQlError::GroupingNotAllowed { .. }) => {
            assert!(
                err.to_string()
                    .contains("grouping not allowed for approx_topk aggregation"),
                "{err}"
            );
        }
        other => panic!("expected the prefix grouping to be rejected, got {other:?}"),
    }
}

#[test]
fn approx_topk_rejects_a_postfix_grouping() {
    match parse(r#"approx_topk(2, rate({a="b"}[5m])) by (lvl)"#) {
        Err(err @ LogQlError::GroupingNotAllowed { .. }) => {
            assert!(
                err.to_string()
                    .contains("grouping not allowed for approx_topk aggregation"),
                "{err}"
            );
        }
        other => panic!("expected the postfix grouping to be rejected, got {other:?}"),
    }
}

/// The reference validates `k` BEFORE the grouping check — the
/// `mustNewVectorAggregationExpr` order (pkg/logql/syntax/ast.go: the
/// `Atoi`/`> 0` checks precede the `OpTypeApproxTopK && gr != nil` arm).
#[test]
fn a_bad_k_wins_over_the_approx_topk_grouping_rejection() {
    match parse(r#"approx_topk by (lvl) (0, rate({a="b"}[5m]))"#) {
        Err(LogQlError::AggregationParamNotPositive { .. }) => {}
        other => panic!("expected the k=0 rejection first, got {other:?}"),
    }
}

#[test]
fn a_parameter_on_a_parameterless_vector_aggregation_is_rejected() {
    // `sum(0.5, ...)`: the `0.5` parses as a scalar-literal operand, so
    // the stray `,` is the offending token (expected `)`).
    match parse(r#"sum(0.5, rate({a="b"}[5m]))"#) {
        Err(LogQlError::UnexpectedToken {
            found, expected, ..
        }) => {
            assert!(found.contains(','), "{found}");
            assert!(expected.contains(')'), "{expected}");
        }
        other => panic!("expected a stray sum parameter to be rejected, got {other:?}"),
    }
}

// --- Remaining unsupported pipeline stage keywords (issue M6-09: the
// --- parsers/label filters/formats/unwrap now parse; these still don't).

#[test]
fn every_remaining_unsupported_stage_keyword_is_named() {
    // Issue #200 flipped `unpack`/`drop`/`keep`/`decolorize` to first-class
    // stages; issue #221 removed `distinct` (not a Loki v3.7.4 construct —
    // reject-parity means a plain bad-stage rejection, not a placeholder
    // `NotYetSupported`), so only `ip` remains out-of-subset.
    assert_eq!(pulsus_logql::REMAINING_UNSUPPORTED_STAGES, &["ip"]);
    for &keyword in pulsus_logql::REMAINING_UNSUPPORTED_STAGES {
        let query = format!(r#"{{a="b"}} | {keyword}"#);
        assert_not_yet_supported(&query, keyword);
    }
}

#[test]
fn distinct_stage_is_a_generic_rejection_not_not_yet_supported() {
    // `distinct` is not a Loki v3.7.4 pipeline stage (issue #221); parity
    // means it rejects as an ordinary bad stage. `| distinct foo` falls
    // through to the label-filter parser, which fails on the missing
    // operator — a generic `UnexpectedToken`, never `NotYetSupported`.
    match parse(r#"{a="b"} | distinct foo"#) {
        Err(LogQlError::UnexpectedToken { expected, .. }) => {
            assert!(expected.contains("label-filter operator"), "{expected}");
        }
        other => panic!("expected a generic bad-stage rejection for distinct, got {other:?}"),
    }
}

#[test]
fn scalar_function_is_rejected() {
    // Loki v3.7.4 has no `scalar()` function (issue #221); it rejects as an
    // unknown aggregation function — a generic `UnexpectedToken`.
    match parse(r#"scalar(rate({a="b"}[1m]))"#) {
        Err(LogQlError::UnexpectedToken { expected, .. }) => {
            assert!(expected.contains("aggregation function"), "{expected}");
        }
        other => panic!("expected scalar() to be rejected, got {other:?}"),
    }
}

// --- Post-`unwrap` ordering: only label filters may follow (plan v3
// --- delta 1 — the grammar rule, enforced by the parser).

fn assert_post_unwrap_rejected(query: &str) {
    match parse(query) {
        Err(LogQlError::UnexpectedToken { expected, .. }) => {
            assert!(
                expected.contains("only label filters may follow `unwrap`"),
                "query {query:?}: expected the post-unwrap rule to be named, got {expected:?}"
            );
        }
        other => panic!("expected {query:?} to be rejected post-unwrap, got {other:?}"),
    }
}

#[test]
fn a_parser_stage_after_unwrap_is_rejected_with_the_named_rule() {
    assert_post_unwrap_rejected(r#"count_over_time({a="b"} | json | unwrap x | logfmt [5m])"#);
}

#[test]
fn a_line_filter_after_unwrap_is_rejected_with_the_named_rule() {
    assert_post_unwrap_rejected(r#"count_over_time({a="b"} | json | unwrap x |= "err" [5m])"#);
}

#[test]
fn a_line_format_after_unwrap_is_rejected_with_the_named_rule() {
    assert_post_unwrap_rejected(
        r#"count_over_time({a="b"} | json | unwrap x | line_format "{{.y}}" [5m])"#,
    );
}

#[test]
fn a_second_unwrap_after_unwrap_is_rejected_with_the_named_rule() {
    assert_post_unwrap_rejected(r#"count_over_time({a="b"} | json | unwrap x | unwrap y [5m])"#);
}

// --- Malformed new-stage syntax ---

#[test]
fn an_unknown_unwrap_conversion_names_the_accepted_set() {
    match parse(r#"count_over_time({a="b"} | unwrap seconds(x) [5m])"#) {
        Err(LogQlError::UnexpectedToken { expected, .. }) => {
            assert!(expected.contains("duration_seconds"), "{expected}");
        }
        other => panic!("expected an unknown conversion to be rejected, got {other:?}"),
    }
}

#[test]
fn a_regex_label_filter_with_a_numeric_rhs_is_rejected() {
    match parse(r#"{a="b"} | status =~ 500"#) {
        Err(LogQlError::UnexpectedToken { expected, .. }) => {
            assert!(expected.contains("a string"), "{expected}");
        }
        other => panic!("expected =~ with a numeric RHS to be rejected, got {other:?}"),
    }
}

#[test]
fn a_comparison_label_filter_with_a_string_rhs_is_rejected() {
    match parse(r#"{a="b"} | status > "500""#) {
        Err(LogQlError::UnexpectedToken { expected, .. }) => {
            assert!(expected.contains("number"), "{expected}");
        }
        other => panic!("expected > with a string RHS to be rejected, got {other:?}"),
    }
}

#[test]
fn an_unclosed_label_filter_paren_is_reported() {
    match parse(r#"{a="b"} | (status="500" or level="error""#) {
        Err(LogQlError::UnexpectedEof { expected, .. }) => {
            assert!(expected.contains(')'), "{expected}");
        }
        other => panic!("expected an unclosed paren to be UnexpectedEof, got {other:?}"),
    }
}

#[test]
fn a_label_format_with_a_numeric_rhs_is_rejected() {
    match parse(r#"{a="b"} | label_format x=5"#) {
        Err(LogQlError::UnexpectedToken { expected, .. }) => {
            assert!(expected.contains("template"), "{expected}");
        }
        other => panic!("expected label_format x=5 to be rejected, got {other:?}"),
    }
}

// --- IP line/label filters (M8-LQ2): rejected shapes ---

#[test]
fn an_empty_ip_line_filter_spec_is_rejected() {
    // `ip()` with no string argument: the `(` must be followed by a string.
    match parse(r#"{a="b"} |= ip()"#) {
        Err(LogQlError::UnexpectedToken { expected, .. }) => {
            assert!(expected.contains("a string"), "{expected}");
        }
        other => panic!("expected `ip()` with no argument to be rejected, got {other:?}"),
    }
}

#[test]
fn an_ip_line_filter_with_a_regex_operator_is_rejected() {
    // `ip()` is accepted only with `|=`/`!=`; `|~ ip(...)` names the rule.
    match parse(r#"{a="b"} |~ ip("10.0.0.0/8")"#) {
        Err(LogQlError::UnexpectedToken { expected, .. }) => {
            assert!(
                expected.contains("|=") && expected.contains("!="),
                "{expected}"
            );
        }
        other => panic!("expected `|~ ip(...)` to be rejected, got {other:?}"),
    }
}

#[test]
fn an_ip_label_filter_with_a_regex_operator_is_rejected() {
    // `=~ ip(...)` is not an IP label filter — `=~` takes a string RHS, so
    // the `ip` identifier is an unexpected non-string.
    match parse(r#"{a="b"} | addr =~ ip("10.0.0.0/8")"#) {
        Err(LogQlError::UnexpectedToken { expected, .. }) => {
            assert!(expected.contains("a string"), "{expected}");
        }
        other => panic!("expected `=~ ip(...)` to be rejected, got {other:?}"),
    }
}

#[test]
fn an_ip_label_filter_without_parens_is_rejected() {
    // `addr = ip` (no `(...)`) is a bare identifier RHS — neither a string
    // matcher nor a numeric comparison.
    match parse(r#"{a="b"} | addr = ip"#) {
        Err(LogQlError::UnexpectedToken { expected, .. }) => {
            assert!(expected.contains("number"), "{expected}");
        }
        other => panic!("expected `addr = ip` without parens to be rejected, got {other:?}"),
    }
}

// --- `!=`/`!~` disambiguation, both directions (amendments 1-3) ---

#[test]
fn neq_after_a_log_expr_is_a_line_filter_not_a_binary_operation() {
    let expr = parse(r#"{a="b"} != "err""#).unwrap();
    let pulsus_logql::Expr::Log(log) = &expr else {
        panic!("expected a log expr");
    };
    assert_eq!(log.pipeline.len(), 1);
    let pulsus_logql::Stage::LineFilter(lf) = &log.pipeline[0] else {
        panic!("expected a line filter stage");
    };
    assert_eq!(lf.op, pulsus_logql::LineFilterOp::NotContains);
}

#[test]
fn neq_between_two_metric_exprs_is_a_binary_comparison() {
    // Issue M6-10: `!=` at binary position now PARSES as a comparison —
    // the other half of the `!=` disambiguation contract.
    let expr = parse(r#"rate({a="b"}[5m]) != rate({a="c"}[5m])"#).unwrap();
    let pulsus_logql::Expr::Metric(pulsus_logql::MetricExpr::Binary { op, .. }) = &expr else {
        panic!("expected a binary metric expr, got {expr:?}");
    };
    assert_eq!(*op, pulsus_logql::BinOp::Neq);
}

#[test]
fn nre_after_a_log_expr_is_a_line_filter() {
    let expr = parse(r#"{a="b"} !~ "e.*r""#).unwrap();
    let pulsus_logql::Expr::Log(log) = &expr else {
        panic!("expected a log expr");
    };
    assert_eq!(log.pipeline.len(), 1);
    let pulsus_logql::Stage::LineFilter(lf) = &log.pipeline[0] else {
        panic!("expected a line filter stage");
    };
    assert_eq!(lf.op, pulsus_logql::LineFilterOp::NotRegex);
}

#[test]
fn nre_between_two_metric_exprs_is_unexpected_token_not_not_yet_supported() {
    // Amendment 3: `!~` is not a LogQL binary/comparison operator in any
    // milestone, so this must NOT be reclassified as a future binary op.
    match parse(r#"rate({a="b"}[5m]) !~ rate({a="c"}[5m])"#) {
        Err(LogQlError::UnexpectedToken { span, .. }) => {
            assert_eq!(span.start, 18);
        }
        other => panic!("expected UnexpectedToken, got {other:?}"),
    }
}

#[test]
fn pipe_exact_between_two_metric_exprs_is_unexpected_token() {
    match parse(r#"rate({a="b"}[5m]) |= "x""#) {
        Err(LogQlError::UnexpectedToken { .. }) => {}
        other => panic!("expected UnexpectedToken, got {other:?}"),
    }
}

// --- Malformed-syntax tests: offset + message content ---

#[test]
fn empty_selector_is_rejected_with_its_own_variant() {
    match parse("{}") {
        Err(LogQlError::EmptySelector { span }) => {
            assert_eq!(span.start, 0);
            assert_eq!(span.end, 1);
        }
        other => panic!("expected EmptySelector, got {other:?}"),
    }
}

#[test]
fn unterminated_double_quoted_string_names_its_start_offset() {
    match parse(r#"{a="b"} |= "unterminated"#) {
        Err(LogQlError::UnterminatedString { span }) => assert_eq!(span.start, 11),
        other => panic!("expected UnterminatedString, got {other:?}"),
    }
}

#[test]
fn a_missing_closing_brace_is_unexpected_eof() {
    let query = r#"{a="b""#;
    match parse(query) {
        Err(LogQlError::UnexpectedEof { expected, span }) => {
            assert!(expected.contains('}'));
            assert_eq!(span.start, query.len());
            assert_eq!(span.end, query.len());
        }
        other => panic!("expected UnexpectedEof, got {other:?}"),
    }
}

#[test]
fn unexpected_eof_carries_a_zero_width_span_at_end_of_input_after_a_missing_value() {
    // Truncation point 1: a matcher with `=` but no value — the query
    // ends where a string was required.
    let query = "{app=";
    match parse(query) {
        Err(LogQlError::UnexpectedEof { span, .. }) => {
            assert_eq!(span.start, query.len());
            assert_eq!(span.end, query.len());
        }
        other => panic!("expected UnexpectedEof, got {other:?}"),
    }
}

#[test]
fn unexpected_eof_carries_a_zero_width_span_at_end_of_input_after_a_missing_close_paren() {
    // Truncation point 2: a range-agg call missing its closing `)`.
    let query = r#"rate({a="b"}[5m]"#;
    match parse(query) {
        Err(LogQlError::UnexpectedEof { expected, span }) => {
            assert!(expected.contains(')'));
            assert_eq!(span.start, query.len());
            assert_eq!(span.end, query.len());
        }
        other => panic!("expected UnexpectedEof, got {other:?}"),
    }
}

#[test]
fn a_bad_matcher_value_reports_the_offending_token_and_offset() {
    match parse(r#"{a=b}"#) {
        Err(LogQlError::UnexpectedToken { found, span, .. }) => {
            assert_eq!(span.start, 3);
            assert!(found.contains('b'));
        }
        other => panic!("expected UnexpectedToken, got {other:?}"),
    }
}

#[test]
fn an_unknown_function_name_is_a_plain_unexpected_token_error() {
    // `offset` is a PromQL-ism with no LogQL grammar (amendment 1 §3): it
    // is just an unrecognized function-position identifier, not named.
    match parse("offset") {
        Err(LogQlError::UnexpectedToken { found, .. }) => assert!(found.contains("offset")),
        other => panic!("expected UnexpectedToken, got {other:?}"),
    }
}

#[test]
fn an_empty_query_is_unexpected_eof() {
    match parse("") {
        Err(LogQlError::UnexpectedEof { span, .. }) => {
            assert_eq!(span.start, 0);
            assert_eq!(span.end, 0);
        }
        other => panic!("expected UnexpectedEof, got {other:?}"),
    }
}

#[test]
fn trailing_input_after_a_complete_expression_is_reported() {
    match parse(r#"{a="b"} ,"#) {
        Err(LogQlError::TrailingInput { span }) => assert_eq!(span.start, 8),
        other => panic!("expected TrailingInput, got {other:?}"),
    }
}

#[test]
fn an_invalid_duration_unit_names_the_raw_literal() {
    match parse(r#"rate({a="b"}[5x])"#) {
        Err(LogQlError::InvalidDuration { raw, .. }) => assert_eq!(raw, "5x"),
        other => panic!("expected InvalidDuration, got {other:?}"),
    }
}

#[test]
fn an_overflowing_duration_is_a_parse_error_not_a_panic() {
    match parse(r#"rate({a="b"}[99999999999999999999y])"#) {
        Err(LogQlError::InvalidDuration { .. }) => {}
        other => panic!("expected InvalidDuration, got {other:?}"),
    }
}

// --- Recursion guard ---

#[test]
fn deeply_nested_vector_aggregations_hit_the_recursion_limit_not_a_stack_overflow() {
    let mut query = String::new();
    for _ in 0..100 {
        query.push_str("sum(");
    }
    query.push_str(r#"count_over_time({a="b"}[5m])"#);
    for _ in 0..100 {
        query.push(')');
    }
    match parse(&query) {
        Err(LogQlError::RecursionLimitExceeded { .. }) => {}
        other => panic!("expected RecursionLimitExceeded, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// Issue #221: `variants(...) of (...)` positional + grammar rejections.
// Every rejection is a plain positional `UnexpectedToken`/`UnexpectedEof`
// — never `NotYetSupported` — matching the reference's 400s
// (`variantsExpr` is an `expr` alternative, not a `metricExpr`).
// ---------------------------------------------------------------------

/// A4 — the four positions the reference grammar rejects: inside a vector
/// aggregation, inside `topk`'s operand, inside parentheses, and inside
/// another `variants` argument list.
#[test]
fn variants_in_a_nested_position_is_a_positional_parse_error() {
    for q in [
        r#"sum(variants(count_over_time({app="x"}[5m])) of ({app="x"}[5m]))"#,
        r#"topk(1, variants(count_over_time({app="x"}[5m])) of ({app="x"}[5m]))"#,
        r#"(variants(count_over_time({app="x"}[5m])) of ({app="x"}[5m]))"#,
        r#"variants(variants(count_over_time({app="x"}[5m])) of ({app="x"}[5m])) of ({app="x"}[5m])"#,
        r#"label_replace(variants(count_over_time({app="x"}[5m])) of ({app="x"}[5m]), "a", "b", "c", "d")"#,
    ] {
        match parse(q) {
            Err(LogQlError::UnexpectedToken { .. } | LogQlError::UnexpectedEof { .. }) => {}
            other => panic!("expected a positional parse error for {q:?}, got {other:?}"),
        }
    }
}

/// A5 — the grammar-shape rejections: empty argument list, trailing
/// comma, missing `of`, unparenthesised range.
///
/// **`VARIANTS(...) OF (...)` used to be pinned here as a rejection**
/// (issue #221 plan §risk 6 recorded the reference's case-insensitive
/// lexer as a known gap). Issue #339 closed that gap across the whole
/// keyword surface, so the uppercase spelling now parses and has moved
/// to `tests/case_folding.rs`; keeping it here would have left a stale
/// expectation contradicting the fix.
#[test]
fn variants_grammar_shape_rejections() {
    for q in [
        r#"variants() of ({app="x"}[5m])"#,
        r#"variants(count_over_time({app="x"}[5m]),) of ({app="x"}[5m])"#,
        r#"variants(count_over_time({app="x"}[5m])) of {app="x"}[5m]"#,
    ] {
        match parse(q) {
            Err(LogQlError::UnexpectedToken { .. } | LogQlError::UnexpectedEof { .. }) => {}
            other => panic!("expected a parse error for {q:?}, got {other:?}"),
        }
    }
    // Missing `of` names the expected token (the reference: `unexpected
    // $end, expecting of`).
    match parse(r#"variants(count_over_time({app="x"}[5m]))"#) {
        Err(LogQlError::UnexpectedToken { expected, .. }) => {
            assert!(expected.contains("of"), "expected 'of' in {expected:?}");
        }
        Err(LogQlError::UnexpectedEof { expected, .. }) => {
            assert!(expected.contains("of"), "expected 'of' in {expected:?}");
        }
        other => panic!("expected a missing-`of` parse error, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// Issue #276: `label_replace(...)` grammar rejections. Every rejection
// is a plain positional `UnexpectedToken`/`UnexpectedEof` — never
// `NotYetSupported` — matching the reference's 400 class (probed,
// v3.7.4: 4 args → `unexpected ), expecting ,`; 6 args → `unexpected ,,
// expecting )`; a bare-identifier argument → `unexpected IDENTIFIER,
// expecting STRING`).
// ---------------------------------------------------------------------

/// Arity/argument-type shapes the reference grammar rejects.
#[test]
fn label_replace_grammar_shape_rejections() {
    for q in [
        // 4 arguments.
        r#"label_replace(rate({app="x"}[5m]), "dst", "v", "src")"#,
        // 6 arguments.
        r#"label_replace(rate({app="x"}[5m]), "dst", "v", "src", ".*", "extra")"#,
        // A bare identifier where a STRING is required.
        r#"label_replace(rate({app="x"}[5m]), dst, "v", "src", ".*")"#,
        // Trailing comma.
        r#"label_replace(rate({app="x"}[5m]), "dst", "v", "src", ".*",)"#,
        // Empty argument list / missing operand.
        r#"label_replace()"#,
        r#"label_replace"#,
        // `LABEL_REPLACE(...)` used to be pinned here as a rejection.
        // It is the case that SURFACED issue #339 — the reference
        // accepts it (probed 200) — so it now parses and its assertion
        // lives in `tests/case_folding.rs`.
    ] {
        match parse(q) {
            Err(LogQlError::UnexpectedToken { .. } | LogQlError::UnexpectedEof { .. }) => {}
            other => panic!("expected a positional parse error for {q:?}, got {other:?}"),
        }
    }
}

/// A syntax error elsewhere in the query wins over anything about the
/// `label_replace` arguments themselves (the reference's ordering: its
/// embedded regex error surfaces only after a clean parse — probed:
/// `label_replace(…, "(") + (` → `syntax error: unexpected $end`).
#[test]
fn label_replace_trailing_syntax_error_wins_over_argument_content() {
    match parse(r#"label_replace(rate({app="x"}[5m]), "d", "r", "s", "(") + ("#) {
        Err(LogQlError::UnexpectedToken { .. } | LogQlError::UnexpectedEof { .. }) => {}
        other => panic!("expected a positional parse error, got {other:?}"),
    }
}
