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

// --- All ten `*_over_time` range aggregations (amendment 1 §3) ---

#[test]
fn sum_over_time_is_not_yet_supported() {
    assert_not_yet_supported(r#"sum_over_time({a="b"}[5m])"#, "sum_over_time");
}

#[test]
fn avg_over_time_is_not_yet_supported() {
    assert_not_yet_supported(r#"avg_over_time({a="b"}[5m])"#, "avg_over_time");
}

#[test]
fn min_over_time_is_not_yet_supported() {
    assert_not_yet_supported(r#"min_over_time({a="b"}[5m])"#, "min_over_time");
}

#[test]
fn max_over_time_is_not_yet_supported() {
    assert_not_yet_supported(r#"max_over_time({a="b"}[5m])"#, "max_over_time");
}

#[test]
fn stddev_over_time_is_not_yet_supported() {
    assert_not_yet_supported(r#"stddev_over_time({a="b"}[5m])"#, "stddev_over_time");
}

#[test]
fn stdvar_over_time_is_not_yet_supported() {
    assert_not_yet_supported(r#"stdvar_over_time({a="b"}[5m])"#, "stdvar_over_time");
}

#[test]
fn quantile_over_time_is_not_yet_supported() {
    assert_not_yet_supported(
        r#"quantile_over_time(0.95, {a="b"}[5m])"#,
        "quantile_over_time",
    );
}

#[test]
fn first_over_time_is_not_yet_supported() {
    assert_not_yet_supported(r#"first_over_time({a="b"}[5m])"#, "first_over_time");
}

#[test]
fn last_over_time_is_not_yet_supported() {
    assert_not_yet_supported(r#"last_over_time({a="b"}[5m])"#, "last_over_time");
}

#[test]
fn absent_over_time_is_not_yet_supported() {
    assert_not_yet_supported(r#"absent_over_time({a="b"}[5m])"#, "absent_over_time");
}

// --- Vector aggregations: stddev, stdvar, topk, bottomk ---

#[test]
fn stddev_is_not_yet_supported() {
    assert_not_yet_supported(r#"stddev(rate({a="b"}[5m]))"#, "stddev");
}

#[test]
fn stdvar_is_not_yet_supported() {
    assert_not_yet_supported(r#"stdvar(rate({a="b"}[5m]))"#, "stdvar");
}

#[test]
fn topk_is_not_yet_supported() {
    assert_not_yet_supported(r#"topk(5, rate({a="b"}[5m]))"#, "topk");
}

#[test]
fn bottomk_is_not_yet_supported() {
    assert_not_yet_supported(r#"bottomk(5, rate({a="b"}[5m]))"#, "bottomk");
}

// --- Parsers: json, logfmt, regexp, pattern ---

#[test]
fn json_parser_stage_is_not_yet_supported() {
    assert_not_yet_supported(r#"{a="b"} | json"#, "json");
}

#[test]
fn logfmt_parser_stage_is_not_yet_supported() {
    assert_not_yet_supported(r#"{a="b"} | logfmt"#, "logfmt");
}

#[test]
fn regexp_parser_stage_is_not_yet_supported() {
    assert_not_yet_supported(r#"{a="b"} | regexp "(?P<x>.*)""#, "regexp");
}

#[test]
fn pattern_parser_stage_is_not_yet_supported() {
    assert_not_yet_supported(r#"{a="b"} | pattern "<x>""#, "pattern");
}

// --- line_format, label_format, unwrap ---

#[test]
fn line_format_stage_is_not_yet_supported() {
    assert_not_yet_supported(r#"{a="b"} | line_format "{{.x}}""#, "line_format");
}

#[test]
fn label_format_stage_is_not_yet_supported() {
    assert_not_yet_supported(r#"{a="b"} | label_format x="y""#, "label_format");
}

#[test]
fn unwrap_stage_is_not_yet_supported() {
    assert_not_yet_supported(r#"count_over_time({a="b"} | unwrap x [5m])"#, "unwrap");
}

// --- Label filter (a label matcher at stage position) ---

#[test]
fn a_label_filter_is_not_yet_supported() {
    assert_not_yet_supported(r#"{a="b"} | status="500""#, "label filter");
}

#[test]
fn a_numeric_label_filter_is_not_yet_supported() {
    assert_not_yet_supported(r#"{a="b"} | status >= 500"#, "label filter");
}

// --- Binary operations: the exact recognition table ---

#[test]
fn arithmetic_binary_operators_are_not_yet_supported() {
    for op in ["+", "-", "*", "/", "%", "^"] {
        let query = format!(r#"rate({{a="b"}}[5m]) {op} rate({{a="c"}}[5m])"#);
        assert_not_yet_supported(&query, "binary operation");
    }
}

#[test]
fn comparison_binary_operators_are_not_yet_supported() {
    for op in ["==", "!=", ">", "<", ">=", "<="] {
        let query = format!(r#"rate({{a="b"}}[5m]) {op} rate({{a="c"}}[5m])"#);
        assert_not_yet_supported(&query, "binary operation");
    }
}

#[test]
fn set_binary_operators_are_not_yet_supported() {
    for op in ["and", "or", "unless"] {
        let query = format!(r#"rate({{a="b"}}[5m]) {op} rate({{a="c"}}[5m])"#);
        assert_not_yet_supported(&query, "binary operation");
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
    let pulsus_logql::Stage::LineFilter(lf) = &log.pipeline[0];
    assert_eq!(lf.op, pulsus_logql::LineFilterOp::NotContains);
}

#[test]
fn neq_between_two_metric_exprs_is_a_named_binary_operation() {
    assert_not_yet_supported(
        r#"rate({a="b"}[5m]) != rate({a="c"}[5m])"#,
        "binary operation",
    );
}

#[test]
fn nre_after_a_log_expr_is_a_line_filter() {
    let expr = parse(r#"{a="b"} !~ "e.*r""#).unwrap();
    let pulsus_logql::Expr::Log(log) = &expr else {
        panic!("expected a log expr");
    };
    assert_eq!(log.pipeline.len(), 1);
    let pulsus_logql::Stage::LineFilter(lf) = &log.pipeline[0];
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
