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

// --- Vector-matching modifiers: the M6-10 adjudicated deferral —
// --- individually enumerated by name, no catch-all. ---

#[test]
fn every_vector_matching_modifier_is_named_not_yet_supported() {
    for modifier in ["on", "ignoring", "group_left", "group_right"] {
        let query = format!(r#"rate({{a="b"}}[5m]) + {modifier}(x) rate({{a="c"}}[5m])"#);
        assert_not_yet_supported(&query, modifier);
    }
}

#[test]
fn a_matching_modifier_after_bool_is_still_named_not_yet_supported() {
    assert_not_yet_supported(r#"rate({a="b"}[5m]) > bool on(x) rate({a="c"}[5m])"#, "on");
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
    for keyword in ["unpack", "drop", "keep", "decolorize", "distinct", "ip"] {
        // `drop`/`keep` take label arguments upstream, but the keyword is
        // rejected before any argument is consumed, so the bare form is
        // representative for all six.
        let query = format!(r#"{{a="b"}} | {keyword}"#);
        assert_not_yet_supported(&query, keyword);
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
