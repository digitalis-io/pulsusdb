//! Tier-1 hermetic goldens for the LogQL pipeline evaluator (issue
//! M6-09, AC2): hand-derived exact `(final label set, final line)`
//! expectations per stage — parsers (incl. nested `json`, `logfmt`
//! empties, non-matching `regexp`, `pattern` `<_>`), string + numeric
//! label filters over number/duration/bytes, `line_format`,
//! `label_format`, parser failure/collision semantics, and the shared
//! unit parser. The runtime differential against the pinned oracle
//! container is the separate e2e parity lane; these goldens pin OUR
//! semantics byte-exactly with no infrastructure.

use pulsus_read::logql::pipeline::{CompiledPipeline, PipelineError};

/// Compiles the pipeline of a parsed log query.
fn compiled(query: &str) -> CompiledPipeline {
    let expr = pulsus_logql::parse(query).expect("parse");
    let pulsus_logql::Expr::Log(log) = expr else {
        panic!("expected a log query: {query}");
    };
    CompiledPipeline::compile(&log.pipeline).expect("compile")
}

fn compile_err(query: &str) -> PipelineError {
    let expr = pulsus_logql::parse(query).expect("parse");
    let pulsus_logql::Expr::Log(log) = expr else {
        panic!("expected a log query: {query}");
    };
    CompiledPipeline::compile(&log.pipeline).expect_err("expected a compile error")
}

fn base() -> Vec<(String, String)> {
    vec![
        ("app".to_string(), "checkout".to_string()),
        ("env".to_string(), "prod".to_string()),
    ]
}

/// Runs one line and returns the exact sorted final label set plus the
/// final line; `None` = dropped.
fn run(query: &str, body: &str) -> Option<(Vec<(String, String)>, String)> {
    let pipeline = compiled(query);
    let base = base();
    let out = pipeline.run(body, &base)?;
    let mut labels: Vec<(String, String)> = out
        .labels
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    labels.sort();
    Some((labels, out.line.into_owned()))
}

fn labels(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    out.sort();
    out
}

// ---------------------------------------------------------------------
// json
// ---------------------------------------------------------------------

#[test]
fn json_flattens_nested_objects_and_stringifies_scalars() {
    let (got, line) = run(
        r#"{a="b"} | json"#,
        r#"{"status":500,"ok":false,"req":{"path":"/x","hdr":{"ua":"curl"}},"tags":["a"],"nil":null}"#,
    )
    .unwrap();
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("env", "prod"),
            ("status", "500"),
            ("ok", "false"),
            ("req_path", "/x"),
            ("req_hdr_ua", "curl"),
            // arrays and nulls are skipped
        ])
    );
    assert_eq!(
        line,
        r#"{"status":500,"ok":false,"req":{"path":"/x","hdr":{"ua":"curl"}},"tags":["a"],"nil":null}"#,
        "parsers never rewrite the line"
    );
}

#[test]
fn json_malformed_line_is_kept_with_the_exact_error_label() {
    let (got, line) = run(r#"{a="b"} | json"#, "not json at all").unwrap();
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("env", "prod"),
            ("__error__", "JSONParserErr"),
        ])
    );
    assert_eq!(line, "not json at all");
}

#[test]
fn json_collision_with_a_stream_label_lands_under_the_extracted_suffix() {
    let (got, _) = run(r#"{a="b"} | json"#, r#"{"app":"other","x":"1"}"#).unwrap();
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("app_extracted", "other"),
            ("env", "prod"),
            ("x", "1"),
        ])
    );
}

#[test]
fn json_targeted_extraction_follows_paths_and_missing_paths_render_empty() {
    let (got, _) = run(
        r#"{a="b"} | json first="servers[0]", ua="req.hdr[\"User-Agent\"]", missing="nope.deep""#,
        r#"{"servers":["s1","s2"],"req":{"hdr":{"User-Agent":"curl"}}}"#,
    )
    .unwrap();
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("env", "prod"),
            ("first", "s1"),
            ("ua", "curl"),
            ("missing", ""),
        ])
    );
}

#[test]
fn json_extraction_landing_on_an_object_renders_compact_json() {
    let (got, _) = run(
        r#"{a="b"} | json hdr="req.hdr""#,
        r#"{"req":{"hdr":{"a":"1"}}}"#,
    )
    .unwrap();
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("env", "prod"),
            ("hdr", r#"{"a":"1"}"#),
        ])
    );
}

/// Issue #72 review round 1, finding 2: malformed extraction paths are
/// compile-time `PipelineInvalid` material, never silently normalized
/// into reading a different field.
#[test]
fn malformed_json_extraction_paths_are_named_compile_errors() {
    for expr in ["a..b", "a.", "a.[0]", ".a", "a[", "a[b]", ""] {
        let query = format!(r#"{{a="b"}} | json x="{expr}""#);
        let err = compile_err(&query);
        assert!(
            matches!(err, PipelineError::BadParserExpr(_)),
            "expr {expr:?}: {err}"
        );
    }
    // The valid shapes still compile.
    for expr in ["a.b.c", "servers[0]", r#"req.hdr[\"User-Agent\"]"#] {
        let query = format!(r#"{{a="b"}} | json x="{expr}""#);
        compiled(&query);
    }
}

#[test]
fn json_sanitizes_extracted_keys_and_prefixes_leading_digits() {
    let (got, _) = run(r#"{a="b"} | json"#, r#"{"http.status":"200","2fa":"on"}"#).unwrap();
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("env", "prod"),
            ("http_status", "200"),
            ("_2fa", "on"),
        ])
    );
}

// ---------------------------------------------------------------------
// logfmt
// ---------------------------------------------------------------------

#[test]
fn logfmt_splits_pairs_with_quoted_values_and_bare_keys() {
    let (got, _) = run(
        r#"{a="b"} | logfmt"#,
        r#"level=error msg="conn \"lost\"" retry took=250ms"#,
    )
    .unwrap();
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("env", "prod"),
            ("level", "error"),
            ("msg", r#"conn "lost""#),
            ("retry", ""), // bare key => empty value
            ("took", "250ms"),
        ])
    );
}

#[test]
fn logfmt_unterminated_quote_is_kept_with_the_exact_error_label() {
    let (got, line) = run(r#"{a="b"} | logfmt"#, r#"level="unterminated"#).unwrap();
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("env", "prod"),
            ("__error__", "LogfmtParserErr"),
        ])
    );
    assert_eq!(line, r#"level="unterminated"#);
}

#[test]
fn logfmt_targeted_extraction_renames_the_source_key() {
    let (got, _) = run(
        r#"{a="b"} | logfmt lvl="level", missing="nope""#,
        "level=warn other=x",
    )
    .unwrap();
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("env", "prod"),
            ("lvl", "warn"),
            ("missing", ""),
        ])
    );
}

// ---------------------------------------------------------------------
// regexp
// ---------------------------------------------------------------------

#[test]
fn regexp_named_groups_become_labels() {
    let (got, _) = run(
        r#"{a="b"} | regexp `^(?P<method>\w+) (?P<path>/\S*) (?P<status>\d+)`"#,
        "GET /api/x 500",
    )
    .unwrap();
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("env", "prod"),
            ("method", "GET"),
            ("path", "/api/x"),
            ("status", "500"),
        ])
    );
}

#[test]
fn regexp_non_matching_line_adds_no_labels_and_is_kept() {
    let (got, line) = run(
        r#"{a="b"} | regexp `^(?P<method>\w+) (?P<path>/\S*)`"#,
        "completely different",
    )
    .unwrap();
    assert_eq!(got, labels(&[("app", "checkout"), ("env", "prod")]));
    assert_eq!(line, "completely different");
}

#[test]
fn regexp_without_a_named_capture_is_a_named_compile_error() {
    let err = compile_err(r#"{a="b"} | regexp "no captures""#);
    assert!(matches!(err, PipelineError::BadParserExpr(_)), "{err}");
    assert!(err.to_string().contains("named capture"), "{err}");
}

#[test]
fn regexp_with_a_bad_pattern_is_a_named_bad_regex_error() {
    let err = compile_err(r#"{a="b"} | regexp "(?P<x>[unclosed""#);
    assert!(matches!(err, PipelineError::BadRegex(_)), "{err}");
}

// ---------------------------------------------------------------------
// pattern
// ---------------------------------------------------------------------

#[test]
fn pattern_captures_between_literals_and_discards_underscore() {
    let (got, _) = run(
        r#"{a="b"} | pattern "<method> <_> <status> took <took>""#,
        "GET /api/x 500 took 250ms",
    )
    .unwrap();
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("env", "prod"),
            ("method", "GET"),
            ("status", "500"),
            ("took", "250ms"),
        ])
    );
}

#[test]
fn pattern_non_matching_line_adds_no_labels_and_is_kept() {
    let (got, line) = run(
        r#"{a="b"} | pattern "level=<level> msg""#,
        "nothing like the pattern",
    )
    .unwrap();
    assert_eq!(got, labels(&[("app", "checkout"), ("env", "prod")]));
    assert_eq!(line, "nothing like the pattern");
}

// ---------------------------------------------------------------------
// String label filters (missing label matches as the empty string).
// ---------------------------------------------------------------------

#[test]
fn string_label_filter_operators_pin_the_prometheus_matcher_semantics() {
    let body = r#"{"status":"500","level":"error"}"#;
    // = / != / =~ / !~ over an extracted label.
    assert!(run(r#"{a="b"} | json | status = "500""#, body).is_some());
    assert!(run(r#"{a="b"} | json | status = "200""#, body).is_none());
    assert!(run(r#"{a="b"} | json | status != "200""#, body).is_some());
    assert!(run(r#"{a="b"} | json | status =~ "5..""#, body).is_some());
    assert!(run(r#"{a="b"} | json | status =~ "2..""#, body).is_none());
    assert!(run(r#"{a="b"} | json | status !~ "2..""#, body).is_some());
    // Anchoring: a partial match is not a match.
    assert!(run(r#"{a="b"} | json | status =~ "5""#, body).is_none());
    // Missing label behaves as "": `= ""` keeps, `!= ""` drops.
    assert!(run(r#"{a="b"} | json | missing = """#, body).is_some());
    assert!(run(r#"{a="b"} | json | missing != """#, body).is_none());
    // `__error__ = ""` drops a malformed-parse survivor.
    assert!(run(r#"{a="b"} | json | __error__ = """#, "not json").is_none());
}

#[test]
fn boolean_label_filters_combine_with_and_or_comma_and_parens() {
    let body = r#"{"status":"500","level":"error"}"#;
    assert!(
        run(
            r#"{a="b"} | json | status = "500" and level = "error""#,
            body
        )
        .is_some()
    );
    assert!(run(r#"{a="b"} | json | status = "500", level = "warn""#, body).is_none());
    assert!(
        run(
            r#"{a="b"} | json | status = "200" or level = "error""#,
            body
        )
        .is_some()
    );
    assert!(
        run(
            r#"{a="b"} | json | (status = "200" or status = "500") and level = "error""#,
            body
        )
        .is_some()
    );
}

// ---------------------------------------------------------------------
// Numeric label filters: every operator over number, duration, bytes.
// ---------------------------------------------------------------------

#[test]
fn numeric_label_filters_compare_plain_numbers_with_every_operator() {
    let body = r#"{"status":"500"}"#;
    for (q, keep) in [
        (r#"{a="b"} | json | status == 500"#, true),
        (r#"{a="b"} | json | status == 200"#, false),
        (r#"{a="b"} | json | status != 200"#, true),
        (r#"{a="b"} | json | status > 499"#, true),
        (r#"{a="b"} | json | status > 500"#, false),
        (r#"{a="b"} | json | status >= 500"#, true),
        (r#"{a="b"} | json | status < 501"#, true),
        (r#"{a="b"} | json | status < 500"#, false),
        (r#"{a="b"} | json | status <= 500"#, true),
    ] {
        assert_eq!(run(q, body).is_some(), keep, "query: {q}");
    }
}

#[test]
fn numeric_label_filters_compare_durations_in_seconds() {
    let body = "took=300ms other=1";
    assert!(run(r#"{a="b"} | logfmt | took > 250ms"#, body).is_some());
    assert!(run(r#"{a="b"} | logfmt | took > 300ms"#, body).is_none());
    assert!(run(r#"{a="b"} | logfmt | took >= 300ms"#, body).is_some());
    assert!(run(r#"{a="b"} | logfmt | took < 1s"#, body).is_some());
    // (No fractional duration literals: the lexer scans `0.3s` as a
    // number plus a trailing ident — fractional *label values* like
    // "1.5s" still convert via the unit parser.)
    assert!(run(r#"{a="b"} | logfmt | took <= 300ms"#, body).is_some());
    // Compound label value against a compound literal.
    assert!(run(r#"{a="b"} | logfmt | took == 300ms"#, body).is_some());
    let compound = "took=1h30m";
    assert!(run(r#"{a="b"} | logfmt | took > 1h"#, compound).is_some());
    assert!(run(r#"{a="b"} | logfmt | took < 2h"#, compound).is_some());
}

#[test]
fn numeric_label_filters_compare_bytes_with_binary_and_decimal_units() {
    let body = "size=6000b";
    assert!(run(r#"{a="b"} | logfmt | size > 5KB"#, body).is_some());
    assert!(run(r#"{a="b"} | logfmt | size < 1MiB"#, body).is_some());
    assert!(run(r#"{a="b"} | logfmt | size > 6KB"#, body).is_none());
    // A KiB-valued label against a KB literal (1024 > 1000).
    let kib = "size=1KiB";
    assert!(run(r#"{a="b"} | logfmt | size > 1KB"#, kib).is_some());
}

#[test]
fn numeric_filter_on_a_missing_label_drops_the_line_without_an_error() {
    assert!(run(r#"{a="b"} | logfmt | took > 250ms"#, "level=info").is_none());
}

#[test]
fn numeric_filter_conversion_failure_keeps_the_line_with_the_exact_error_label() {
    let (got, line) = run(r#"{a="b"} | logfmt | took > 250ms"#, "took=banana").unwrap();
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("env", "prod"),
            ("took", "banana"),
            ("__error__", "LabelFilterErr"),
        ])
    );
    assert_eq!(line, "took=banana");
}

/// Issue #72 review round 1, finding 1 — unit-family strictness, each
/// outcome verified against the pinned oracle container:
/// - a UNITLESS label value against a DURATION filter is a conversion
///   error (kept + `LabelFilterErr`), never coerced to seconds;
/// - a unitless value against a BYTES filter is a byte count (the
///   upstream bytes parser accepts bare numbers);
/// - a unit-suffixed value against a plain NUMBER filter is a
///   conversion error.
#[test]
fn unit_family_mismatches_match_the_oracle_semantics() {
    // Duration filter, unitless value: error, line kept regardless of
    // which way the comparison would have gone.
    for q in [
        r#"{a="b"} | logfmt | took > 250ms"#,
        r#"{a="b"} | logfmt | took > 350ms"#,
    ] {
        let (got, _) = run(q, "took=300").unwrap();
        assert_eq!(
            got,
            labels(&[
                ("app", "checkout"),
                ("env", "prod"),
                ("took", "300"),
                ("__error__", "LabelFilterErr"),
            ]),
            "query: {q}"
        );
    }
    // Bytes filter, unitless value: 300 bytes — no error.
    assert!(run(r#"{a="b"} | logfmt | size > 200B"#, "size=300").is_some());
    let (got, _) = run(r#"{a="b"} | logfmt | size > 200B"#, "size=300").unwrap();
    assert!(!got.iter().any(|(k, _)| k == "__error__"), "{got:?}");
    assert!(run(r#"{a="b"} | logfmt | size > 400B"#, "size=300").is_none());
    // Number filter, unit-suffixed value: error, line kept.
    let (got, _) = run(r#"{a="b"} | logfmt | status > 100"#, "status=200ms").unwrap();
    assert!(
        got.contains(&("__error__".to_string(), "LabelFilterErr".to_string())),
        "{got:?}"
    );
}

#[test]
fn a_rejected_unit_literal_is_a_named_compile_error() {
    let err = compile_err(r#"{a="b"} | logfmt | took > 5xz"#);
    assert!(matches!(err, PipelineError::BadParserExpr(_)), "{err}");
    assert!(err.to_string().contains("5xz"), "{err}");
}

// ---------------------------------------------------------------------
// line_format / label_format
// ---------------------------------------------------------------------

#[test]
fn line_format_substitutes_fields_and_missing_fields_render_empty() {
    let (got, line) = run(
        r#"{a="b"} | json | line_format "{{.method}} {{.missing}}->{{.status}}""#,
        r#"{"method":"GET","status":"500"}"#,
    )
    .unwrap();
    assert_eq!(line, "GET ->500");
    // line_format never changes labels.
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("env", "prod"),
            ("method", "GET"),
            ("status", "500"),
        ])
    );
}

#[test]
fn label_format_rename_moves_the_value_and_removes_the_source() {
    let (got, _) = run(
        r#"{a="b"} | logfmt | label_format lvl=level"#,
        "level=error",
    )
    .unwrap();
    assert_eq!(
        got,
        labels(&[("app", "checkout"), ("env", "prod"), ("lvl", "error"),])
    );
}

#[test]
fn label_format_template_computes_a_new_label_from_existing_ones() {
    let (got, _) = run(
        r#"{a="b"} | json | label_format summary="{{.method}} {{.status}}""#,
        r#"{"method":"GET","status":"500"}"#,
    )
    .unwrap();
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("env", "prod"),
            ("method", "GET"),
            ("status", "500"),
            ("summary", "GET 500"),
        ])
    );
}

#[test]
fn label_format_assigning_the_same_label_twice_is_a_named_compile_error() {
    let err = compile_err(r#"{a="b"} | label_format x=a, x=b"#);
    assert!(matches!(err, PipelineError::BadParserExpr(_)), "{err}");
    assert!(err.to_string().contains("twice"), "{err}");
}

#[test]
fn an_excluded_template_function_is_rejected_by_its_own_name() {
    for func in ["printf", "regexReplaceAll", "__line__", "ToLower"] {
        let query = format!(r#"{{a="b"}} | line_format "{{{{ {func} .x }}}}""#);
        let err = compile_err(&query);
        match err {
            PipelineError::UnsupportedTemplate(name) => assert_eq!(name, func),
            other => panic!("expected UnsupportedTemplate({func}), got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------
// Path classification (fast/transform/fan-out gates).
// ---------------------------------------------------------------------

#[test]
fn pipeline_classification_gates_match_the_exec_paths() {
    assert!(compiled(r#"{a="b"} |= "err" != "x""#).is_line_filter_only());
    let transform = compiled(r#"{a="b"} | line_format "{{.env}}" |= "prod""#);
    assert!(!transform.is_line_filter_only());
    assert!(!transform.mutates_labels());
    assert!(transform.rewrites_line());
    let fan_out = compiled(r#"{a="b"} | json"#);
    assert!(fan_out.mutates_labels());
    assert!(!fan_out.rewrites_line());
    // A numeric label filter can add `__error__` -> label-set-changing.
    assert!(compiled(r#"{a="b"} | took > 1s"#).mutates_labels());
    // A string-only label filter never changes the label set.
    assert!(!compiled(r#"{a="b"} | level = "error""#).mutates_labels());
}

#[test]
fn pushed_down_line_filters_are_not_re_evaluated_in_engine() {
    // The filter precedes any line_format, so it pushed down to SQL; the
    // compiled pipeline must treat the line as already-filtered (running
    // a line that would NOT match the filter still passes — SQL owns it).
    let pipeline = compiled(r#"{a="b"} |= "err" | json"#);
    let base = base();
    assert!(
        pipeline.run(r#"{"clean":"1"}"#, &base).is_some(),
        "pre-line_format line filters are SQL's job, not the evaluator's"
    );
}
