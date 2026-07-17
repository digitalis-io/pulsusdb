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
    let pulsus_logql::Stage::LineFilter(lf) = &log.pipeline[0] else {
        panic!("expected a line filter stage");
    };
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
        param: None,
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
        param: None,
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
        param: None,
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

// ---------------------------------------------------------------------
// M6-09 pipeline stages: parsers, label filters, formats, unwrap. Every
// construct that moved out of `errors.rs`'s NotYetSupported set gets a
// round-trip snapshot (`parse(ast.to_string()) == ast` — AC1); key
// shapes also pin their full Debug form.
// ---------------------------------------------------------------------

/// The round-trip half of [`assert_snapshot`] alone, for the wide M6-09
/// construct matrix where the full Debug form would add bulk without
/// pinning anything the round-trip identity doesn't.
fn assert_round_trip(query: &str) {
    let expr = parse(query).unwrap_or_else(|e| panic!("expected {query:?} to parse, got {e}"));
    let rendered = expr.to_string();
    let reparsed = parse(&rendered)
        .unwrap_or_else(|e| panic!("expected the rendered form {rendered:?} to reparse, got {e}"));
    assert_eq!(
        reparsed, expr,
        "round-trip mismatch: {query:?} -> {rendered:?}"
    );
}

#[test]
fn a_json_parser_stage_with_a_label_filter() {
    assert_snapshot(
        r#"{app="x"} | json | status = "500""#,
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
            Parser(
                Json {
                    extractions: [],
                },
            ),
            LabelFilter(
                Match(
                    Matcher {
                        name: "status",
                        op: Eq,
                        value: "500",
                    },
                ),
            ),
        ],
    },
)
"#,
    );
}

#[test]
fn a_numeric_label_filter_carries_the_raw_literal() {
    assert_snapshot(
        r#"{app="x"} | logfmt | took > 250ms"#,
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
            Parser(
                Logfmt {
                    extractions: [],
                },
            ),
            LabelFilter(
                Compare {
                    name: "took",
                    op: Gt,
                    rhs: DurationOrBytes(
                        "250ms",
                    ),
                },
            ),
        ],
    },
)
"#,
    );
}

/// The plan v3 delta 1 combined form: unwrap as an ordered stage with
/// post-unwrap label filters preserved in position, inside a range
/// aggregation.
#[test]
fn unwrap_with_post_unwrap_label_filters_round_trips_in_position() {
    let query = r#"count_over_time({a="b"} | json | unwrap duration(latency) | __error__ = "" | latency > 1s [5m])"#;
    assert_round_trip(query);

    let expr = parse(query).unwrap();
    let pulsus_logql::Expr::Metric(pulsus_logql::MetricExpr::Range { range, .. }) = &expr else {
        panic!("expected a range aggregation");
    };
    // `LogRange.unwrap` stays retained-but-unused: the stage lives in the
    // ordered pipeline instead.
    assert_eq!(range.unwrap, None);
    let pipeline = &range.selector.pipeline;
    assert_eq!(pipeline.len(), 4);
    assert!(matches!(
        &pipeline[1],
        pulsus_logql::Stage::Unwrap(u)
            if u.label == "latency" && u.conversion.as_deref() == Some("duration")
    ));
    assert!(matches!(&pipeline[2], pulsus_logql::Stage::LabelFilter(_)));
    assert!(matches!(&pipeline[3], pulsus_logql::Stage::LabelFilter(_)));
}

#[test]
fn every_new_pipeline_construct_round_trips() {
    let queries = [
        // Parsers, bare and with extractions (incl. the bare-identifier
        // shorthand, which canonicalizes to `label="label"`).
        r#"{a="b"} | json"#,
        r#"{a="b"} | json first_server="servers[0]", ua="request.headers.User-Agent""#,
        r#"{a="b"} | json host"#,
        r#"{a="b"} | logfmt"#,
        r#"{a="b"} | logfmt host="hostname""#,
        r#"{a="b"} | regexp "(?P<method>\\w+) (?P<path>\\S+)""#,
        r#"{a="b"} | regexp `(?P<method>\w+) (?P<path>\S+)`"#,
        r#"{a="b"} | pattern "<method> <path> <_> <status>""#,
        // String label filters, all four operators.
        r#"{a="b"} | status = "500""#,
        r#"{a="b"} | status != "500""#,
        r#"{a="b"} | status =~ "5..""#,
        r#"{a="b"} | status !~ "2..""#,
        // Numeric label filters over number / duration / bytes literals.
        r#"{a="b"} | status == 500"#,
        r#"{a="b"} | status != 500"#,
        r#"{a="b"} | ratio > 0.5"#,
        r#"{a="b"} | took >= 250ms"#,
        r#"{a="b"} | took < 1h30m"#,
        r#"{a="b"} | size <= 5KB"#,
        r#"{a="b"} | size > 1MiB"#,
        // Boolean mini-grammar: `and`, `or`, `,`, parens, nesting.
        r#"{a="b"} | json | status = "500" and level = "error""#,
        r#"{a="b"} | json | status = "500", level = "error""#,
        r#"{a="b"} | json | status = "500" or level = "error""#,
        r#"{a="b"} | json | (status = "500" or status = "503") and level = "error""#,
        r#"{a="b"} | json | level = "error" or (took > 250ms and size > 5KB)"#,
        // Formats.
        r#"{a="b"} | json | line_format "{{.method}} {{.path}}""#,
        r#"{a="b"} | logfmt | label_format lvl=level"#,
        r#"{a="b"} | logfmt | label_format lvl=level, summary="{{.method}} {{.path}}""#,
        // Unwrap, bare and converted, log-range position.
        r#"count_over_time({a="b"} | logfmt | unwrap bytes_processed [5m])"#,
        r#"rate({a="b"} | json | unwrap duration_seconds(latency) [1m])"#,
        r#"rate({a="b"} | json | unwrap bytes(sz) [1m])"#,
        // Line filters interleaved with the new stages (pre-unwrap).
        r#"{a="b"} |= "err" | json | status = "500""#,
        r#"{a="b"} | line_format "{{.msg}}" |= "err""#,
        // A bare log query with unwrap parses (the planner, not the
        // parser, owns the "unwrap needs a range aggregation" rejection).
        r#"{a="b"} | json | unwrap latency"#,
    ];
    for q in queries {
        assert_round_trip(q);
    }
}

#[test]
fn boolean_label_filters_default_to_and_binding_tighter_than_or() {
    let expr = parse(r#"{a="b"} | x = "1" or y = "2" and z = "3""#).unwrap();
    let pulsus_logql::Expr::Log(log) = &expr else {
        panic!("expected a log expr");
    };
    let pulsus_logql::Stage::LabelFilter(filter) = &log.pipeline[0] else {
        panic!("expected a label filter stage");
    };
    // `or` is loosest: x or (y and z).
    let pulsus_logql::LabelFilterExpr::Or(left, right) = filter else {
        panic!("expected Or at the root, got {filter:?}");
    };
    assert!(matches!(**left, pulsus_logql::LabelFilterExpr::Match(_)));
    assert!(matches!(**right, pulsus_logql::LabelFilterExpr::And(..)));
}

#[test]
fn consecutive_label_filter_stages_stay_separate_stages() {
    let expr = parse(r#"{a="b"} | json | status = "500" | level = "error""#).unwrap();
    let pulsus_logql::Expr::Log(log) = &expr else {
        panic!("expected a log expr");
    };
    assert_eq!(log.pipeline.len(), 3);
    assert!(matches!(
        &log.pipeline[1],
        pulsus_logql::Stage::LabelFilter(_)
    ));
    assert!(matches!(
        &log.pipeline[2],
        pulsus_logql::Stage::LabelFilter(_)
    ));
}

#[test]
fn an_eq_label_filter_with_a_numeric_rhs_canonicalizes_to_double_eq() {
    // RHS-typed dispatch: `= 500` is the numeric comparison (upstream
    // accepts both spellings); canonical Display renders `== 500`.
    let expr = parse(r#"{a="b"} | status = 500"#).unwrap();
    assert_eq!(expr.to_string(), r#"{a="b"} | status == 500"#);
    assert_round_trip(r#"{a="b"} | status = 500"#);
}

// ---------------------------------------------------------------------
// M6-10: the full over-time set, parameterized aggregations, and binary
// operations. Round-trip oracle (`parse(ast.to_string()) == ast`) for
// every construct that moved out of `errors.rs`'s NotYetSupported set
// (AC1), plus tree-shape pins for precedence/associativity (AC4c's
// parser half).
// ---------------------------------------------------------------------

#[test]
fn every_over_time_aggregation_round_trips() {
    let queries = [
        r#"sum_over_time({a="b"} | logfmt | unwrap took [5m])"#,
        r#"avg_over_time({a="b"} | logfmt | unwrap duration(took) [5m])"#,
        r#"min_over_time({a="b"} | json | unwrap latency [5m])"#,
        r#"max_over_time({a="b"} | json | unwrap latency [5m])"#,
        r#"stddev_over_time({a="b"} | logfmt | unwrap v [5m])"#,
        r#"stdvar_over_time({a="b"} | logfmt | unwrap v [5m])"#,
        r#"quantile_over_time(0.95, {a="b"} | logfmt | unwrap v [5m])"#,
        r#"first_over_time({a="b"} | logfmt | unwrap v [5m])"#,
        r#"last_over_time({a="b"} | logfmt | unwrap v [5m])"#,
        r#"absent_over_time({a="b"}[5m])"#,
        r#"absent_over_time({a="b"} | logfmt | unwrap v [5m])"#,
        r#"rate({a="b"} | logfmt | unwrap bytes(sz) [1m])"#,
    ];
    for q in queries {
        assert_round_trip(q);
    }
}

#[test]
fn quantile_over_time_carries_its_raw_parameter() {
    let expr = parse(r#"quantile_over_time(0.95, {a="b"} | logfmt | unwrap v [5m])"#).unwrap();
    let pulsus_logql::Expr::Metric(pulsus_logql::MetricExpr::Range { op, param, .. }) = &expr
    else {
        panic!("expected a range aggregation");
    };
    assert_eq!(*op, pulsus_logql::RangeAggOp::QuantileOverTime);
    assert_eq!(param.as_deref(), Some("0.95"));
}

#[test]
fn new_vector_aggregations_round_trip_with_grouping_and_params() {
    let queries = [
        r#"stddev(rate({a="b"}[5m]))"#,
        r#"stdvar by(app)(rate({a="b"}[5m]))"#,
        r#"topk(5, rate({a="b"}[5m]))"#,
        r#"bottomk(3, rate({a="b"}[5m]))"#,
        r#"topk by(app)(5, rate({a="b"}[5m]))"#,
        r#"sum by(app)(topk(2, count_over_time({a="b"}[5m])))"#,
    ];
    for q in queries {
        assert_round_trip(q);
    }
}

#[test]
fn topk_carries_its_raw_k_parameter_and_grouping() {
    let expr = parse(r#"topk by(app)(5, rate({a="b"}[5m]))"#).unwrap();
    let pulsus_logql::Expr::Metric(pulsus_logql::MetricExpr::Vector {
        op,
        grouping,
        param,
        ..
    }) = &expr
    else {
        panic!("expected a vector aggregation");
    };
    assert_eq!(*op, pulsus_logql::VectorAggOp::Topk);
    assert_eq!(param.as_deref(), Some("5"));
    assert_eq!(grouping.as_ref().unwrap().labels, vec!["app".to_string()]);
}

#[test]
fn every_binary_operator_round_trips() {
    for op in [
        "+", "-", "*", "/", "%", "^", "==", "!=", ">", "<", ">=", "<=", "and", "or", "unless",
    ] {
        let query = format!(r#"rate({{a="b"}}[5m]) {op} rate({{a="c"}}[5m])"#);
        assert_round_trip(&query);
    }
}

#[test]
fn scalar_literals_and_both_orientations_round_trip() {
    let queries = [
        "2",
        "0.95",
        r#"rate({a="b"}[5m]) * 2"#,
        r#"2 - rate({a="b"}[5m])"#,
        r#"2 ^ 2 ^ 3"#,
        r#"(rate({a="b"}[5m]) + rate({a="c"}[5m])) * 2"#,
        r#"rate({a="b"}[5m]) > bool 0.5"#,
        r#"sum(rate({a="b"}[5m]) + rate({a="c"}[5m]))"#,
    ];
    for q in queries {
        assert_round_trip(q);
    }
}

/// AC4c (parser half): `^` is right-associative — `2 ^ 2 ^ 3` must parse
/// as `2 ^ (2 ^ 3)`, never `(2 ^ 2) ^ 3`.
#[test]
fn caret_is_right_associative() {
    use pulsus_logql::{BinOp, MetricExpr};
    let expr = parse("2 ^ 2 ^ 3").unwrap();
    let pulsus_logql::Expr::Metric(MetricExpr::Binary { op, lhs, rhs, .. }) = &expr else {
        panic!("expected a binary expr");
    };
    assert_eq!(*op, BinOp::Pow);
    assert_eq!(**lhs, MetricExpr::Literal("2".to_string()));
    let MetricExpr::Binary {
        op: inner_op,
        lhs: inner_lhs,
        rhs: inner_rhs,
        ..
    } = &**rhs
    else {
        panic!("expected the RIGHT side to nest: {rhs:?}");
    };
    assert_eq!(*inner_op, BinOp::Pow);
    assert_eq!(**inner_lhs, MetricExpr::Literal("2".to_string()));
    assert_eq!(**inner_rhs, MetricExpr::Literal("3".to_string()));
}

/// AC4c (parser half): `*` binds tighter than `+` — `a + b * c` parses
/// as `a + (b * c)`.
#[test]
fn multiplication_binds_tighter_than_addition() {
    use pulsus_logql::{BinOp, MetricExpr};
    let expr = parse("1 + 2 * 3").unwrap();
    let pulsus_logql::Expr::Metric(MetricExpr::Binary { op, lhs, rhs, .. }) = &expr else {
        panic!("expected a binary expr");
    };
    assert_eq!(*op, BinOp::Add);
    assert_eq!(**lhs, MetricExpr::Literal("1".to_string()));
    assert!(matches!(&**rhs, MetricExpr::Binary { op: BinOp::Mul, .. }));
}

/// Comparisons bind looser than arithmetic and `and` looser still:
/// `a > b + c and d` parses as `(a > (b + c)) and d`.
#[test]
fn comparison_and_set_operator_precedence_nests_correctly() {
    use pulsus_logql::{BinOp, MetricExpr};
    let expr = parse("1 > 2 + 3 and 4").unwrap();
    let pulsus_logql::Expr::Metric(MetricExpr::Binary { op, lhs, .. }) = &expr else {
        panic!("expected a binary expr");
    };
    assert_eq!(*op, BinOp::And);
    let MetricExpr::Binary {
        op: cmp_op,
        rhs: cmp_rhs,
        ..
    } = &**lhs
    else {
        panic!("expected the left side to be the comparison: {lhs:?}");
    };
    assert_eq!(*cmp_op, BinOp::Gt);
    assert!(matches!(
        &**cmp_rhs,
        MetricExpr::Binary { op: BinOp::Add, .. }
    ));
}

#[test]
fn bool_modifier_is_captured_and_rendered() {
    use pulsus_logql::{BinModifier, MetricExpr};
    let expr = parse(r#"rate({a="b"}[5m]) > bool 0.5"#).unwrap();
    let pulsus_logql::Expr::Metric(MetricExpr::Binary { modifier, .. }) = &expr else {
        panic!("expected a binary expr");
    };
    assert_eq!(
        *modifier,
        Some(BinModifier {
            return_bool: true,
            matching: None,
        })
    );
    assert_eq!(expr.to_string(), r#"rate({a="b"}[5m]) > bool 0.5"#);
}

/// Issue #91: every vector-matching modifier form parses into the
/// specified [`VectorMatching`] and round-trips through `Display`.
#[test]
fn vector_matching_modifiers_are_captured_and_render() {
    use pulsus_logql::{BinModifier, MatchGroup, MetricExpr, VectorMatching};

    let cases: &[(&str, VectorMatching)] = &[
        (
            r#"rate({a="b"}[5m]) / on(app) rate({a="c"}[5m])"#,
            VectorMatching {
                on: true,
                labels: vec!["app".to_string()],
                group: None,
            },
        ),
        (
            r#"rate({a="b"}[5m]) / ignoring(inst) rate({a="c"}[5m])"#,
            VectorMatching {
                on: false,
                labels: vec!["inst".to_string()],
                group: None,
            },
        ),
        (
            r#"rate({a="b"}[5m]) / on(app) group_left rate({a="c"}[5m])"#,
            VectorMatching {
                on: true,
                labels: vec!["app".to_string()],
                group: Some(MatchGroup::Left(vec![])),
            },
        ),
        (
            r#"rate({a="b"}[5m]) * on(app) group_left(extra) rate({a="c"}[5m])"#,
            VectorMatching {
                on: true,
                labels: vec!["app".to_string()],
                group: Some(MatchGroup::Left(vec!["extra".to_string()])),
            },
        ),
        (
            r#"rate({a="b"}[5m]) * on(app) group_right(a, b) rate({a="c"}[5m])"#,
            VectorMatching {
                on: true,
                labels: vec!["app".to_string()],
                group: Some(MatchGroup::Right(vec!["a".to_string(), "b".to_string()])),
            },
        ),
        (
            r#"rate({a="b"}[5m]) == bool on() rate({a="c"}[5m])"#,
            VectorMatching {
                on: true,
                labels: vec![],
                group: None,
            },
        ),
    ];

    for (query, want) in cases {
        let expr = parse(query).unwrap_or_else(|e| panic!("parse {query:?}: {e}"));
        let pulsus_logql::Expr::Metric(MetricExpr::Binary { modifier, .. }) = &expr else {
            panic!("expected a binary expr for {query:?}");
        };
        let modifier = modifier.as_ref().expect("a modifier");
        assert_eq!(
            modifier.matching.as_ref(),
            Some(want),
            "matching clause for {query:?}"
        );
        // Round-trip: reparse of the rendered form equals the original.
        assert_round_trip(query);
    }

    // `bool` + matching coexist on a comparison.
    let expr = parse(r#"rate({a="b"}[5m]) == bool on() rate({a="c"}[5m])"#).unwrap();
    let pulsus_logql::Expr::Metric(MetricExpr::Binary { modifier, .. }) = &expr else {
        panic!("expected a binary expr");
    };
    assert_eq!(
        *modifier,
        Some(BinModifier {
            return_bool: true,
            matching: Some(VectorMatching {
                on: true,
                labels: vec![],
                group: None,
            }),
        })
    );
}

/// Issue #91: a bare `group_left`/`group_right` with no preceding
/// `on`/`ignoring` is a positional parse error (an `UnexpectedToken`,
/// NOT `NotYetSupported`) — oracle-pinned: Loki rejects it HTTP 400.
#[test]
fn group_modifier_without_on_or_ignoring_is_a_parse_error() {
    use pulsus_logql::LogQlError;
    for q in [
        r#"rate({a="b"}[5m]) / group_left rate({a="c"}[5m])"#,
        r#"rate({a="b"}[5m]) / group_right(x) rate({a="c"}[5m])"#,
    ] {
        let err = parse(q).expect_err("must reject a bare grouping modifier");
        assert!(
            matches!(err, LogQlError::UnexpectedToken { .. }),
            "expected UnexpectedToken for {q:?}, got {err:?}"
        );
    }
}

/// Issue #91 (review round 2, finding 1): a label appearing in BOTH the
/// `on`/`ignoring` signature AND the `group_left`/`group_right` include
/// list. Prometheus rejects this at parse time ("label \"x\" must not
/// occur in ON and GROUP clause at once"), and the review expected Loki
/// to inherit that rule — but the `grafana/loki:3.4.2` oracle does NOT:
/// it PARSES and EVALUATES `... on(x) group_left(x) ...` (probed live —
/// HTTP 200 with a correctly joined result, both `query` and
/// `query_range`). Matching the oracle (the standing "don't invent rules,
/// match Loki" mandate), we therefore ACCEPT the overlap rather than
/// synthesize a Prometheus-only rejection Loki lacks. This test locks the
/// oracle-confirmed acceptance so a future "add the Prometheus check"
/// change cannot silently regress LogQL/Loki parity.
#[test]
fn a_label_shared_by_on_and_group_clauses_parses_matching_the_loki_oracle() {
    use pulsus_logql::{MatchGroup, MetricExpr, VectorMatching};
    for (query, want) in [
        (
            r#"rate({a="b"}[5m]) / on(x) group_left(x) rate({a="c"}[5m])"#,
            VectorMatching {
                on: true,
                labels: vec!["x".to_string()],
                group: Some(MatchGroup::Left(vec!["x".to_string()])),
            },
        ),
        (
            r#"rate({a="b"}[5m]) / ignoring(y) group_left(y) rate({a="c"}[5m])"#,
            VectorMatching {
                on: false,
                labels: vec!["y".to_string()],
                group: Some(MatchGroup::Left(vec!["y".to_string()])),
            },
        ),
    ] {
        let expr = parse(query).unwrap_or_else(|e| panic!("must parse {query:?}: {e}"));
        let pulsus_logql::Expr::Metric(MetricExpr::Binary { modifier, .. }) = &expr else {
            panic!("expected a binary expr for {query:?}");
        };
        assert_eq!(
            modifier.as_ref().and_then(|m| m.matching.as_ref()),
            Some(&want),
            "matching clause for {query:?}"
        );
        assert_round_trip(query);
    }
}

/// Parenthesized grouping overrides precedence and the tree shape
/// round-trips through the paren-preserving `Display`.
#[test]
fn explicit_parens_override_precedence() {
    use pulsus_logql::{BinOp, MetricExpr};
    let expr = parse("(1 + 2) * 3").unwrap();
    let pulsus_logql::Expr::Metric(MetricExpr::Binary { op, lhs, .. }) = &expr else {
        panic!("expected a binary expr");
    };
    assert_eq!(*op, BinOp::Mul);
    assert!(matches!(&**lhs, MetricExpr::Binary { op: BinOp::Add, .. }));
    assert_eq!(expr.to_string(), "(1 + 2) * 3");
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
