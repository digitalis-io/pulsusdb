//! `assert_eq!` snapshots of `{:#?}` Debug output over every M1 query
//! shape (docs/features.md §2), plus the `Display`-round-trip oracle:
//! `parse(ast.to_string()) == ast` (task-manager resolution #1). Derived
//! `Debug` is the snapshot mechanism — no `insta` (lean-deps ethos).

use pulsus_logql::parse;

/// Asserts `parse(query)` succeeds and its pretty Debug form matches
/// `expected`, then re-parses `Display`'d output and checks it produces
/// an equal AST — the round-trip fuzz oracle.
fn assert_snapshot(query: &str, expected: &str) {
    let expr = parse(query).unwrap_or_else(|e| panic!("expected {query:?} to parse, got {e}"));
    let actual = format!("{expr:#?}");
    assert_eq!(actual.trim(), expected.trim(), "query: {query}");

    let rendered = expr.to_string();
    let reparsed = parse(&rendered)
        .unwrap_or_else(|e| panic!("expected the rendered form {rendered:?} to reparse, got {e}"));
    assert_eq!(
        reparsed, expr,
        "round-trip mismatch: {query:?} -> {rendered:?}"
    );
}

#[test]
fn a_selector_with_a_single_equality_matcher() {
    assert_snapshot(
        r#"{app="x"}"#,
        r#"
Log(
    LogExpr {
        selector: StreamSelector {
            matchers: [
                Matcher {
                    name: "app",
                    op: Eq,
                    value: "x",
                },
            ],
        },
        pipeline: [],
    },
)
"#,
    );
}

#[test]
fn a_selector_with_every_matcher_operator() {
    assert_snapshot(
        r#"{app="x", env!="prod", host=~"web.*", host!~"db.*"}"#,
        r#"
Log(
    LogExpr {
        selector: StreamSelector {
            matchers: [
                Matcher {
                    name: "app",
                    op: Eq,
                    value: "x",
                },
                Matcher {
                    name: "env",
                    op: Neq,
                    value: "prod",
                },
                Matcher {
                    name: "host",
                    op: Re,
                    value: "web.*",
                },
                Matcher {
                    name: "host",
                    op: Nre,
                    value: "db.*",
                },
            ],
        },
        pipeline: [],
    },
)
"#,
    );
}

#[test]
fn a_positive_contains_line_filter() {
    assert_snapshot(
        r#"{app="x"} |= "err""#,
        r#"
Log(
    LogExpr {
        selector: StreamSelector {
            matchers: [
                Matcher {
                    name: "app",
                    op: Eq,
                    value: "x",
                },
            ],
        },
        pipeline: [
            LineFilter(
                LineFilter {
                    op: Contains,
                    value: "err",
                },
            ),
        ],
    },
)
"#,
    );
}

#[test]
fn a_negative_line_filter_with_no_leading_pipe() {
    // Review-cycle-mandated case: `!=` at stage position, no leading `|`.
    assert_snapshot(
        r#"{app="x"} != "err""#,
        r#"
Log(
    LogExpr {
        selector: StreamSelector {
            matchers: [
                Matcher {
                    name: "app",
                    op: Eq,
                    value: "x",
                },
            ],
        },
        pipeline: [
            LineFilter(
                LineFilter {
                    op: NotContains,
                    value: "err",
                },
            ),
        ],
    },
)
"#,
    );
}

#[test]
fn a_negative_regex_line_filter_with_no_leading_pipe() {
    assert_snapshot(
        r#"{app="x"} !~ "e.*r""#,
        r#"
Log(
    LogExpr {
        selector: StreamSelector {
            matchers: [
                Matcher {
                    name: "app",
                    op: Eq,
                    value: "x",
                },
            ],
        },
        pipeline: [
            LineFilter(
                LineFilter {
                    op: NotRegex,
                    value: "e.*r",
                },
            ),
        ],
    },
)
"#,
    );
}

#[test]
fn a_chained_mix_of_all_four_line_filter_operators() {
    assert_snapshot(
        r#"{app="x"} |= "a" != "b" |~ "c" !~ "d""#,
        r#"
Log(
    LogExpr {
        selector: StreamSelector {
            matchers: [
                Matcher {
                    name: "app",
                    op: Eq,
                    value: "x",
                },
            ],
        },
        pipeline: [
            LineFilter(
                LineFilter {
                    op: Contains,
                    value: "a",
                },
            ),
            LineFilter(
                LineFilter {
                    op: NotContains,
                    value: "b",
                },
            ),
            LineFilter(
                LineFilter {
                    op: Regex,
                    value: "c",
                },
            ),
            LineFilter(
                LineFilter {
                    op: NotRegex,
                    value: "d",
                },
            ),
        ],
    },
)
"#,
    );
}

#[test]
fn matcher_context_neq_and_filter_context_neq_in_one_query() {
    // Disambiguation, both directions, in a single parse: `!=` inside
    // `{...}` is a MatchOp; `!=` after the selector is a LineFilterOp.
    let expr = parse(r#"{app="x", env!="prod"} != "err""#).unwrap();
    let pulsus_logql::Expr::Log(log) = &expr else {
        panic!("expected a log expr");
    };
    assert_eq!(log.selector.matchers[1].op, pulsus_logql::MatchOp::Neq);
    assert_eq!(log.pipeline.len(), 1);
    let pulsus_logql::Stage::LineFilter(lf) = &log.pipeline[0];
    assert_eq!(lf.op, pulsus_logql::LineFilterOp::NotContains);
}

#[test]
fn rate_over_a_selector() {
    assert_snapshot(
        r#"rate({app="x"}[5m])"#,
        r#"
Metric(
    Range {
        op: Rate,
        range: LogRange {
            selector: LogExpr {
                selector: StreamSelector {
                    matchers: [
                        Matcher {
                            name: "app",
                            op: Eq,
                            value: "x",
                        },
                    ],
                },
                pipeline: [],
            },
            range: Duration(
                300000000000,
            ),
            unwrap: None,
        },
        param: None,
    },
)
"#,
    );
}

#[test]
fn count_over_time_over_a_selector() {
    assert_snapshot(
        r#"count_over_time({app="x"}[5m])"#,
        r#"
Metric(
    Range {
        op: CountOverTime,
        range: LogRange {
            selector: LogExpr {
                selector: StreamSelector {
                    matchers: [
                        Matcher {
                            name: "app",
                            op: Eq,
                            value: "x",
                        },
                    ],
                },
                pipeline: [],
            },
            range: Duration(
                300000000000,
            ),
            unwrap: None,
        },
        param: None,
    },
)
"#,
    );
}

#[test]
fn bytes_rate_over_a_selector() {
    assert_snapshot(
        r#"bytes_rate({app="x"}[5m])"#,
        r#"
Metric(
    Range {
        op: BytesRate,
        range: LogRange {
            selector: LogExpr {
                selector: StreamSelector {
                    matchers: [
                        Matcher {
                            name: "app",
                            op: Eq,
                            value: "x",
                        },
                    ],
                },
                pipeline: [],
            },
            range: Duration(
                300000000000,
            ),
            unwrap: None,
        },
        param: None,
    },
)
"#,
    );
}

#[test]
fn bytes_over_time_over_a_selector() {
    assert_snapshot(
        r#"bytes_over_time({app="x"}[5m])"#,
        r#"
Metric(
    Range {
        op: BytesOverTime,
        range: LogRange {
            selector: LogExpr {
                selector: StreamSelector {
                    matchers: [
                        Matcher {
                            name: "app",
                            op: Eq,
                            value: "x",
                        },
                    ],
                },
                pipeline: [],
            },
            range: Duration(
                300000000000,
            ),
            unwrap: None,
        },
        param: None,
    },
)
"#,
    );
}

#[test]
fn a_range_agg_over_a_selector_with_line_filters() {
    assert_snapshot(
        r#"count_over_time({app="x"} |= "a" != "b" [5m])"#,
        r#"
Metric(
    Range {
        op: CountOverTime,
        range: LogRange {
            selector: LogExpr {
                selector: StreamSelector {
                    matchers: [
                        Matcher {
                            name: "app",
                            op: Eq,
                            value: "x",
                        },
                    ],
                },
                pipeline: [
                    LineFilter(
                        LineFilter {
                            op: Contains,
                            value: "a",
                        },
                    ),
                    LineFilter(
                        LineFilter {
                            op: NotContains,
                            value: "b",
                        },
                    ),
                ],
            },
            range: Duration(
                300000000000,
            ),
            unwrap: None,
        },
        param: None,
    },
)
"#,
    );
}

#[test]
fn a_compound_duration_literal() {
    let expr = parse(r#"rate({app="x"}[1h30m])"#).unwrap();
    let pulsus_logql::Expr::Metric(pulsus_logql::MetricExpr::Range { range, .. }) = &expr else {
        panic!("expected a range agg");
    };
    assert_eq!(
        range.range.as_nanos(),
        3_600_000_000_000 + 30 * 60_000_000_000
    );
}

#[test]
fn sum_with_no_grouping() {
    assert_snapshot(
        r#"sum(rate({app="x"}[5m]))"#,
        r#"
Metric(
    Vector {
        op: Sum,
        grouping: None,
        inner: Range {
            op: Rate,
            range: LogRange {
                selector: LogExpr {
                    selector: StreamSelector {
                        matchers: [
                            Matcher {
                                name: "app",
                                op: Eq,
                                value: "x",
                            },
                        ],
                    },
                    pipeline: [],
                },
                range: Duration(
                    300000000000,
                ),
                unwrap: None,
            },
            param: None,
        },
    },
)
"#,
    );
}

#[test]
fn sum_with_prefix_by_grouping() {
    assert_snapshot(
        r#"sum by(app)(rate({app="x"}[5m]))"#,
        r#"
Metric(
    Vector {
        op: Sum,
        grouping: Some(
            Grouping {
                kind: By,
                labels: [
                    "app",
                ],
            },
        ),
        inner: Range {
            op: Rate,
            range: LogRange {
                selector: LogExpr {
                    selector: StreamSelector {
                        matchers: [
                            Matcher {
                                name: "app",
                                op: Eq,
                                value: "x",
                            },
                        ],
                    },
                    pipeline: [],
                },
                range: Duration(
                    300000000000,
                ),
                unwrap: None,
            },
            param: None,
        },
    },
)
"#,
    );
}

#[test]
fn sum_with_postfix_by_grouping_normalizes_to_the_same_ast_as_prefix() {
    let prefix = parse(r#"sum by(app)(rate({app="x"}[5m]))"#).unwrap();
    let postfix = parse(r#"sum(rate({app="x"}[5m])) by(app)"#).unwrap();
    assert_eq!(prefix, postfix);
}

#[test]
fn avg_without_grouping_with_multiple_labels() {
    assert_snapshot(
        r#"avg without(app, env)(count_over_time({app="x"}[5m]))"#,
        r#"
Metric(
    Vector {
        op: Avg,
        grouping: Some(
            Grouping {
                kind: Without,
                labels: [
                    "app",
                    "env",
                ],
            },
        ),
        inner: Range {
            op: CountOverTime,
            range: LogRange {
                selector: LogExpr {
                    selector: StreamSelector {
                        matchers: [
                            Matcher {
                                name: "app",
                                op: Eq,
                                value: "x",
                            },
                        ],
                    },
                    pipeline: [],
                },
                range: Duration(
                    300000000000,
                ),
                unwrap: None,
            },
            param: None,
        },
    },
)
"#,
    );
}

#[test]
fn min_and_max_and_count_vector_aggs_parse() {
    for op_name in ["min", "max", "count"] {
        let query = format!(r#"{op_name}(count_over_time({{app="x"}}[5m]))"#);
        assert!(parse(&query).is_ok(), "expected {query:?} to parse");
    }
}

#[test]
fn every_m1_subset_query_shape_from_features_md_section_2_parses() {
    // docs/features.md §2: "stream selectors with =, !=, =~, !~; line
    // filters |=, !=, |~, !~; range aggregations rate, count_over_time,
    // bytes_rate, bytes_over_time; vector aggregations sum, avg, min,
    // max, count with by/without."
    let queries = [
        r#"{app="x"}"#,
        r#"{app!="x"}"#,
        r#"{app=~"x.*"}"#,
        r#"{app!~"x.*"}"#,
        r#"{app="x"} |= "a""#,
        r#"{app="x"} != "a""#,
        r#"{app="x"} |~ "a""#,
        r#"{app="x"} !~ "a""#,
        r#"rate({app="x"}[5m])"#,
        r#"count_over_time({app="x"}[5m])"#,
        r#"bytes_rate({app="x"}[5m])"#,
        r#"bytes_over_time({app="x"}[5m])"#,
        r#"sum(rate({app="x"}[5m]))"#,
        r#"avg(rate({app="x"}[5m]))"#,
        r#"min(rate({app="x"}[5m]))"#,
        r#"max(rate({app="x"}[5m]))"#,
        r#"count(rate({app="x"}[5m]))"#,
        r#"sum by(app)(rate({app="x"}[5m]))"#,
        r#"sum without(app)(rate({app="x"}[5m]))"#,
        r#"sum(rate({app="x"}[5m])) by(app)"#,
        r#"sum(rate({app="x"}[5m])) without(app)"#,
    ];
    for q in queries {
        parse(q).unwrap_or_else(|e| panic!("expected {q:?} to parse, got {e}"));
    }
}
