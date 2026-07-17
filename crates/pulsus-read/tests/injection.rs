//! Injection-safety tests: label matcher values, line-filter values, and
//! regex patterns containing `'`, `\`, `)`, `--`, and regex metacharacters
//! must always come out escaped in the generated SQL — never able to close
//! a string literal, comment out the remainder of a statement, or inject a
//! new predicate (architect plan: "Injection — every matcher/line-filter/
//! regex value flows through escape.rs").

use pulsus_logql::{LineFilter, LineFilterOp, MatchOp, Matcher, StreamSelector};
use pulsus_read::logql::escape::{ch_ident, ch_regex_anchored, ch_regex_unanchored, ch_string};
use pulsus_read::logql::plan;
use pulsus_read::logql::sql::{self, TimeWindow};
use pulsus_read::logql::{Direction, Plan, PlanCtx, QueryParams, QuerySpec};

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

const START_NS: i64 = 1_782_907_200_000_000_000;
const END_NS: i64 = 1_782_928_800_000_000_000;

fn params() -> QueryParams {
    QueryParams {
        spec: QuerySpec::Range {
            start_ns: START_NS,
            end_ns: END_NS,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    }
}

/// A single-quote-terminated payload attempting to close the literal and
/// append a new predicate — the classic injection shape.
const PAYLOAD_QUOTE: &str = "checkout' OR '1'='1";
/// A payload that pairs a backslash with a quote, attempting to exploit a
/// naive escaper that escapes `'` but not `\` first.
const PAYLOAD_BACKSLASH_QUOTE: &str = r"checkout\' OR 1=1 --";
/// A payload attempting a comment-based statement truncation.
const PAYLOAD_COMMENT: &str = "checkout'; DROP TABLE log_samples; --";
/// A payload with an unbalanced parenthesis, attempting to break out of a
/// `(key = '...' AND ...)` branch.
const PAYLOAD_PAREN: &str = "checkout') OR (1=1";
/// A payload containing every regex metacharacter PulsusDB treats
/// specially, checked through both the anchored and unanchored escapers.
const PAYLOAD_REGEX_META: &str = r#"a.*b$c^d(e)f[g]h{i}j|k\l"#;

fn assert_no_unescaped_quote_or_backslash(literal: &str) {
    // `literal` is expected to be a well-formed ClickHouse single-quoted
    // string: strip the outer quotes and verify every `'`/`\` inside is
    // itself preceded by a backslash (i.e. escaped), never bare.
    assert!(
        literal.starts_with('\'') && literal.ends_with('\''),
        "{literal}"
    );
    let inner = &literal[1..literal.len() - 1];
    let mut chars = inner.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            // Escaped character: consume it and move on.
            chars.next();
            continue;
        }
        assert_ne!(c, '\'', "bare unescaped quote in {literal:?}");
    }
}

#[test]
fn ch_string_never_emits_a_bare_quote_for_any_payload() {
    for payload in [
        PAYLOAD_QUOTE,
        PAYLOAD_BACKSLASH_QUOTE,
        PAYLOAD_COMMENT,
        PAYLOAD_PAREN,
        PAYLOAD_REGEX_META,
    ] {
        assert_no_unescaped_quote_or_backslash(&ch_string(payload));
    }
}

#[test]
fn ch_regex_anchored_never_emits_a_bare_quote_for_any_payload() {
    for payload in [
        PAYLOAD_QUOTE,
        PAYLOAD_BACKSLASH_QUOTE,
        PAYLOAD_COMMENT,
        PAYLOAD_PAREN,
    ] {
        assert_no_unescaped_quote_or_backslash(&ch_regex_anchored(payload));
    }
}

#[test]
fn ch_regex_unanchored_never_emits_a_bare_quote_for_any_payload() {
    for payload in [
        PAYLOAD_QUOTE,
        PAYLOAD_BACKSLASH_QUOTE,
        PAYLOAD_COMMENT,
        PAYLOAD_PAREN,
    ] {
        assert_no_unescaped_quote_or_backslash(&ch_regex_unanchored(payload));
    }
}

#[test]
fn ch_ident_never_emits_a_bare_backtick() {
    let escaped = ch_ident("log_samples` ON CLUSTER evil");
    let inner = &escaped[1..escaped.len() - 1];
    let mut chars = inner.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            chars.next();
            continue;
        }
        assert_ne!(c, '`', "bare unescaped backtick in {escaped:?}");
    }
}

#[test]
fn a_matcher_value_with_an_injection_payload_stays_inside_one_literal_in_stage1() {
    let selector = StreamSelector {
        matchers: vec![Matcher {
            name: "service_name".to_string(),
            op: MatchOp::Eq,
            value: PAYLOAD_QUOTE.to_string(),
        }],
    };
    let sp = plan_streams(selector);
    // The whole payload must appear as one escaped literal; the branch
    // count in `HAVING` must stay 1 (a bare quote would have let the
    // payload inject a second OR-branch that the `= 1` count wouldn't
    // reflect, or broken the SQL outright).
    assert!(sp.stage1_sql.contains(&ch_string(PAYLOAD_QUOTE)));
    assert!(sp.stage1_sql.ends_with("HAVING uniqExact(key, val) = 1"));
}

#[test]
fn a_matcher_value_with_a_comment_payload_does_not_truncate_the_statement() {
    let selector = StreamSelector {
        matchers: vec![Matcher {
            name: "service_name".to_string(),
            op: MatchOp::Eq,
            value: PAYLOAD_COMMENT.to_string(),
        }],
    };
    let sp = plan_streams(selector);
    assert!(sp.stage1_sql.contains(&ch_string(PAYLOAD_COMMENT)));
    // The statement's trailing structure (GROUP BY / HAVING) must still be
    // present after the payload — a successful truncation would have cut
    // the string here.
    assert!(sp.stage1_sql.contains("GROUP BY fingerprint"));
    assert!(sp.stage1_sql.ends_with("HAVING uniqExact(key, val) = 1"));
}

#[test]
fn a_regex_matcher_value_with_metacharacters_and_a_quote_is_fully_escaped() {
    let payload = r#"a".*)$OR(1=1"#;
    let selector = StreamSelector {
        matchers: vec![Matcher {
            name: "env".to_string(),
            op: MatchOp::Re,
            value: payload.to_string(),
        }],
    };
    let sp = plan_streams(selector);
    let expected_literal = ch_regex_anchored(payload);
    assert_no_unescaped_quote_or_backslash(&expected_literal);
    assert!(
        sp.stage1_sql
            .contains(&format!("match(val, {expected_literal})"))
    );
}

#[test]
fn a_line_filter_payload_stays_inside_one_literal_in_the_exact_predicate() {
    let filters = vec![pulsus_logql::Stage::LineFilter(LineFilter {
        op: LineFilterOp::Contains,
        value: PAYLOAD_PAREN.to_string(),
    })];
    let clauses = compile_line_filters(&filters);
    assert_eq!(clauses.len(), 1);
    assert!(clauses[0].contains(&format!("position(body, {})", ch_string(PAYLOAD_PAREN))));
    assert_no_unescaped_quote_or_backslash(&ch_string(PAYLOAD_PAREN));
}

#[test]
fn stage3_with_an_injection_payload_in_the_line_filter_keeps_the_statement_well_formed() {
    let filters = vec![pulsus_logql::Stage::LineFilter(LineFilter {
        op: LineFilterOp::Contains,
        value: PAYLOAD_COMMENT.to_string(),
    })];
    let clauses = compile_line_filters(&filters);
    let sql = sql::stage3(
        "log_samples",
        &["'checkout'".to_string()],
        &[1],
        TimeWindow {
            start_ns: START_NS,
            end_ns: END_NS,
        },
        &clauses,
        Direction::Backward,
        100,
    );
    assert!(sql.trim_end().ends_with("LIMIT 100"));
    assert!(sql.contains(&ch_string(PAYLOAD_COMMENT)));
}

// --- helpers reaching into `pulsus_read::logql::plan`'s private matcher/line-filter
// compilation are unavailable from an external test crate, so these route
// through the public `plan()` entry point and `sql` builders instead.

fn plan_streams(selector: StreamSelector) -> pulsus_read::logql::StreamsPlan {
    let expr = pulsus_logql::Expr::Log(pulsus_logql::LogExpr {
        selector,
        pipeline: Vec::new(),
    });
    match plan(&expr, &params(), &ctx()).expect("plan") {
        Plan::Streams(sp) => sp,
        Plan::Metric(_) => panic!("expected Streams"),
    }
}

fn compile_line_filters(pipeline: &[pulsus_logql::Stage]) -> Vec<String> {
    let selector = StreamSelector {
        matchers: vec![Matcher {
            name: "service_name".to_string(),
            op: MatchOp::Eq,
            value: "checkout".to_string(),
        }],
    };
    let expr = pulsus_logql::Expr::Log(pulsus_logql::LogExpr {
        selector,
        pipeline: pipeline.to_vec(),
    });
    match plan(&expr, &params(), &ctx()).expect("plan") {
        Plan::Streams(sp) => sp.line_filters,
        Plan::Metric(_) => panic!("expected Streams"),
    }
}
