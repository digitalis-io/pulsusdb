//! Tier-1 hermetic goldens for the LogQL pipeline evaluator (issue
//! M6-09, AC2): hand-derived exact `(final label set, final line)`
//! expectations per stage — parsers (incl. nested `json`, `logfmt`
//! empties, non-matching `regexp`, `pattern` `<_>`), string + numeric
//! label filters over number/duration/bytes, `line_format`,
//! `label_format`, parser failure/collision semantics, and the shared
//! unit parser. The runtime differential against the pinned oracle
//! container is the separate e2e parity lane; these goldens pin OUR
//! semantics byte-exactly with no infrastructure.

use std::borrow::Cow;

use pulsus_read::logql::pipeline::{CompiledPipeline, MetricRun, PipelineError};

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
            // issue #99: the streams-path detail label, byte-exact vs
            // grafana/loki:3.4.2 for a top-level non-object line.
            (
                "__error_details__",
                "Value looks like object, but can't find closing '}' symbol",
            ),
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
            // issue #99: `level="unterminated` is 19 runes; Loki's 1-based
            // position for the unterminated quote at EOF is one past the
            // final rune (oracle_probe.txt [2]).
            (
                "__error_details__",
                "logfmt syntax error at pos 20 : unterminated quoted value",
            ),
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
            // issue #99: Go time.ParseDuration's `invalid duration` branch
            // (no leading numeric char), value verbatim.
            ("__error_details__", r#"time: invalid duration "banana""#),
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
                // issue #99: Go time.ParseDuration's `missing unit` branch
                // (all-numeric value, no unit).
                (
                    "__error_details__",
                    r#"time: missing unit in duration "300""#
                ),
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

// ---------------------------------------------------------------------
// __error_details__ (issue #99): the streams-path companion label to
// __error__, byte-exact vs grafana/loki:3.4.2 where feasible and
// faithful-format/ledgered for the value-interpolated long tail (see
// tests/golden/logql_error_details/oracle_probe.txt for the probe).
// ---------------------------------------------------------------------

/// The `__error_details__` value a streams run produced (or `None`).
fn detail(query: &str, body: &str) -> Option<String> {
    let (got, _) = run(query, body)?;
    got.iter()
        .find(|(k, _)| k == "__error_details__")
        .map(|(_, v)| v.clone())
}

#[test]
fn json_error_details_is_the_probed_fixed_string_regardless_of_the_body() {
    // Every top-level non-object line takes the one representative
    // buger/jsonparser message (oracle_probe.txt [1]).
    for body in ["not json at all", "[1,2,3]", "12345", "hello world"] {
        assert_eq!(
            detail(r#"{a="b"} | json"#, body).as_deref(),
            Some("Value looks like object, but can't find closing '}' symbol"),
            "body: {body:?}"
        );
    }
}

#[test]
fn label_filter_number_family_detail_is_the_probed_parsefloat_message() {
    // RHS `100` is a plain number -> Number family -> Go strconv.ParseFloat.
    assert_eq!(
        detail(r#"{a="b"} | logfmt | status > 100"#, "status=abc").as_deref(),
        Some(r#"strconv.ParseFloat: parsing "abc": invalid syntax"#),
    );
    assert_eq!(
        detail(r#"{a="b"} | logfmt | status > 100"#, "status=5abc").as_deref(),
        Some(r#"strconv.ParseFloat: parsing "5abc": invalid syntax"#),
    );
}

#[test]
fn label_filter_duration_family_detail_covers_the_three_probed_branches() {
    // invalid duration (no leading numeric), missing unit (bare number) —
    // both pinned byte-exact; unknown unit — faithful-format (matches Loki
    // for a single number+unit, ledgered for compound values).
    assert_eq!(
        detail(r#"{a="b"} | logfmt | took > 5s"#, "took=abc").as_deref(),
        Some(r#"time: invalid duration "abc""#),
    );
    assert_eq!(
        detail(r#"{a="b"} | logfmt | took > 5s"#, "took=5").as_deref(),
        Some(r#"time: missing unit in duration "5""#),
    );
    assert_eq!(
        detail(r#"{a="b"} | logfmt | took > 5s"#, "took=5xyz").as_deref(),
        Some(r#"time: unknown unit "xyz" in duration "5xyz""#),
    );
}

#[test]
fn label_filter_number_family_detail_is_byte_exact_for_nonascii_values() {
    // issue #99 finding 1: the offending value is rendered through Go
    // `strconv.Quote` (not raw between literal quotes), so the message is
    // byte-exact for ALL values, not just plain ASCII. Expected strings
    // captured from go1.25.5 `strconv.ParseFloat(v, 64).Error()` (== the
    // Loki 3.4.2 oracle). Reverting the quoting makes every case fail
    // (embedded `"` stays raw, `\x01` becomes a literal control byte,
    // etc.).
    // (a) embedded double-quote (logfmt `\"` unescapes to `"`).
    assert_eq!(
        detail(r#"{a="b"} | logfmt | status > 100"#, "status=\"ab\\\"cd\"").as_deref(),
        Some(r#"strconv.ParseFloat: parsing "ab\"cd": invalid syntax"#),
    );
    // (b) C0 control byte 0x01 -> `\x01`.
    assert_eq!(
        detail(r#"{a="b"} | logfmt | status > 100"#, "status=ab\u{1}cd").as_deref(),
        Some(r#"strconv.ParseFloat: parsing "ab\x01cd": invalid syntax"#),
    );
    // (c) multi-byte UTF-8 rune (printable -> passes through under
    // strconv.Quote's IsPrint).
    assert_eq!(
        detail(r#"{a="b"} | logfmt | status > 100"#, "status=1中2").as_deref(),
        Some(r#"strconv.ParseFloat: parsing "1中2": invalid syntax"#),
    );
}

#[test]
fn label_filter_duration_family_detail_is_byte_exact_for_nonascii_values() {
    // issue #99 finding 1: the value/unit are rendered through Go
    // `time`'s internal `quote` (per-byte `\xNN` for controls AND every
    // byte of a non-ASCII rune, `\"`/`\\` for quote/backslash). Expected
    // strings captured from go1.25.5 `time.ParseDuration(v).Error()`.
    // (a) embedded double-quote -> invalid-duration branch.
    assert_eq!(
        detail(r#"{a="b"} | logfmt | took > 5s"#, "took=\"ab\\\"cd\"").as_deref(),
        Some(r#"time: invalid duration "ab\"cd""#),
    );
    // (b) C0 control byte 0x01 -> `\x01` (time.quote has NO named escapes).
    assert_eq!(
        detail(r#"{a="b"} | logfmt | took > 5s"#, "took=ab\u{1}cd").as_deref(),
        Some(r#"time: invalid duration "ab\x01cd""#),
    );
    // (c) multi-byte UTF-8 rune -> unknown-unit branch; BOTH the unit and
    // the whole value are per-byte `\xNN` escaped (中 == e4 b8 ad).
    assert_eq!(
        detail(r#"{a="b"} | logfmt | took > 5s"#, "took=1中2").as_deref(),
        Some(r#"time: unknown unit "\xe4\xb8\xad" in duration "1\xe4\xb8\xad2""#),
    );
}

#[test]
fn label_filter_bytes_family_detail_is_faithful_format_ledgered() {
    // Ledgered: Loki's humanize.ParseBytes interpolates an internal
    // numeric split; a fully non-numeric value yields the empty prefix
    // Loki reports byte-exact (oracle_probe.txt [3]).
    assert_eq!(
        detail(r#"{a="b"} | logfmt | size > 5B"#, "size=xyz").as_deref(),
        Some(r#"strconv.ParseFloat: parsing "": invalid syntax"#),
    );
}

#[test]
fn error_detail_label_survives_and_drops_with_its_error_partner() {
    // `__error__ = ""` drops the errored line entirely (both labels gone).
    assert!(run(r#"{a="b"} | json | __error__ = """#, "not json").is_none());
    // `__error__ != ""` keeps it, carrying BOTH the error and its detail.
    let (got, _) = run(r#"{a="b"} | json | __error__ != """#, "not json").unwrap();
    assert!(
        got.contains(&("__error__".to_string(), "JSONParserErr".to_string())),
        "{got:?}"
    );
    assert!(
        got.contains(&(
            "__error_details__".to_string(),
            "Value looks like object, but can't find closing '}' symbol".to_string(),
        )),
        "{got:?}"
    );
}

#[test]
fn metric_path_sets_both_error_and_the_detail_label() {
    // Parity flip (issue #104): the metric path now tags __error__ AND the
    // byte-exact __error_details__ — the same detail the streams path emits
    // (grafana/loki:3.4.2 DOES include it in its metric pipeline-error
    // message; oracle-confirmed). __error_details__ sorts immediately after
    // __error__.
    let pipeline = compiled(r#"{a="b"} | json"#);
    let base = base();
    let mut labels: Vec<(Cow<'_, str>, Cow<'_, str>)> = Vec::new();
    let out = pipeline.run_metric_into("not json", &base, &mut labels);
    assert!(matches!(out, MetricRun::Kept { .. }));
    assert!(
        labels
            .iter()
            .any(|(k, v)| k == "__error__" && v == "JSONParserErr"),
        "metric path still tags __error__: {labels:?}"
    );
    assert!(
        labels.iter().any(|(k, v)| k == "__error_details__"
            && v == "Value looks like object, but can't find closing '}' symbol"),
        "metric path must now carry the byte-exact __error_details__: {labels:?}"
    );
}
