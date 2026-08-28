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
    let out = pipeline
        .run(body, &base, 0)
        .expect("no template budget breach")?;
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

/// Renamed by issue #389 part B: the answer is the document's own bytes,
/// which for THIS line happens to look like compact JSON because the
/// line has no inner whitespace and no duplicate key. The name used to
/// assert the rendering we have now removed.
#[test]
fn json_extraction_landing_on_an_object_hands_back_its_bytes() {
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

// --- issue #389 part A: the scan stops at the end of the first value ---
//
// `serde_json::from_str` demands end-of-input after the value; the
// reference's scanner simply stops (`jsonparser.ObjectEach` returns the
// moment it reaches the closing brace,
// `vendor/github.com/grafana/jsonparser/parser.go:1108-1112,1155-1160 @
// v3.7.4`). ONE call, THREE entrypoints — `| json`, `| json a="…"` and
// `| unpack` — so each gets its own case.

#[test]
fn json_flatten_ignores_bytes_after_the_first_value() {
    for (line, want) in [
        (r#"{"a":1}trailing"#, vec![("a", "1")]),
        // Only the FIRST value is read: `b` is never seen.
        (r#"{"a":1}{"b":2}"#, vec![("a", "1")]),
        (r#"{"a":1,"o":{"x":2}}junk"#, vec![("a", "1"), ("o_x", "2")]),
        // Rules out a "truncate at the last/first `}`" hack: the brace
        // here is INSIDE a string.
        (r#"{"a":"}x"}trailing"#, vec![("a", "}x")]),
    ] {
        let (got, kept) = run(r#"{a="b"} | json"#, line).unwrap();
        let mut want: Vec<(&str, &str)> = want;
        want.extend([("app", "checkout"), ("env", "prod")]);
        assert_eq!(got, labels(&want), "line {line:?}");
        assert_eq!(kept, line, "parsers never rewrite the line");
    }
}

#[test]
fn json_expression_ignores_bytes_after_the_first_value() {
    for line in [
        r#"{"a":1}trailing"#,
        r#"{"a":1}{"b":2}"#,
        // The first occurrence wins, and the second value is not read.
        r#"{"a":1,"a":2}zz"#,
    ] {
        let (got, _) = run(r#"{a="b"} | json a="a""#, line).unwrap();
        assert_eq!(
            got,
            labels(&[("app", "checkout"), ("env", "prod"), ("a", "1")]),
            "line {line:?}"
        );
    }
}

#[test]
fn unpack_ignores_bytes_after_the_first_value() {
    let (got, line) = run(
        r#"{a="b"} | unpack"#,
        r#"{"_entry":"hi","lbl":"v"}trailing"#,
    )
    .unwrap();
    assert_eq!(
        got,
        labels(&[("app", "checkout"), ("env", "prod"), ("lbl", "v")])
    );
    assert_eq!(line, "hi");
}

/// `JSONExpressionParser.Process` gates on `len(line) == 0` and then on
/// `isValidJSONStart(line)` — ONE raw byte, whitespace NOT skipped
/// (`pkg/logql/log/parser.go:664-670,726-732 @ v3.7.4`). Everything past
/// that gate is a non-validating scan whose misses are the missing-path
/// fill, so a line that does not parse still answers `a=""` with no
/// error. We used to route this arm through the flatten arm's
/// "must parse to an object" test, which is wrong in both directions.
#[test]
fn json_expression_gates_on_the_first_byte_only() {
    // `addErrLabel(errJSON, nil, lbs)`: the error, and NO details.
    for line in [r#" {"a":1}"#, "\t{\"a\":1}", r#"x{"a":1}"#] {
        let (got, _) = run(r#"{a="b"} | json a="a""#, line).unwrap();
        assert_eq!(
            got,
            labels(&[
                ("app", "checkout"),
                ("env", "prod"),
                ("__error__", "JSONParserErr"),
            ]),
            "line {line:?}"
        );
    }
    // An empty line writes NOTHING — not the fill, not an error.
    let (got, _) = run(r#"{a="b"} | json a="a""#, "").unwrap();
    assert_eq!(got, labels(&[("app", "checkout"), ("env", "prod")]));
    // Past the gate, a miss is the fill: `a=""` and no error.
    for line in [r#"[1,2]junk"#, r#""hello"trailing"#, "{garbage", r#"{"a"}"#] {
        let (got, _) = run(r#"{a="b"} | json a="a""#, line).unwrap();
        assert_eq!(
            got,
            labels(&[("app", "checkout"), ("env", "prod"), ("a", "")]),
            "line {line:?}"
        );
    }
}

/// `UnpackParser.Process`'s own gate (`parser.go:753-762 @ v3.7.4`): an
/// empty line returns silently, and the object test is `line[0] != '{'`.
#[test]
fn unpack_gates_on_the_first_byte_only() {
    let (got, line) = run(r#"{a="b"} | unpack"#, "").unwrap();
    assert_eq!(got, labels(&[("app", "checkout"), ("env", "prod")]));
    assert_eq!(line, "");

    let (got, line) = run(r#"{a="b"} | unpack"#, r#" {"a":"1"}"#).unwrap();
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("env", "prod"),
            ("__error__", "JSONParserErr"),
            (
                "__error_details__",
                "Value looks like object, but can't find closing '}' symbol",
            ),
        ])
    );
    assert_eq!(line, r#" {"a":"1"}"#);
}

// --- issue #389 part B: a container extraction hands back the bytes ---

/// `case jsonparser.Object: lbs.Set(ParsedLabel, key, string(data))`
/// (`pkg/logql/log/parser.go:700-706 @ v3.7.4`) — the value's own slice
/// out of the line. Key order, inner whitespace, duplicate keys and the
/// spelling of an escape all survive; we used to re-render a parsed
/// value and lose every one of them.
#[test]
fn json_extraction_hands_back_the_document_bytes() {
    for (line, want) in [
        (r#"{"o":{"b":1,  "a":2},"k":3}"#, r#"{"b":1,  "a":2}"#),
        ("{\"o\":{\"a\":\t1,\n\"b\":2}}", "{\"a\":\t1,\n\"b\":2}"),
        (r#"{ "a" : 1 , "o" : { "z" : 1 } }"#, r#"{ "z" : 1 }"#),
        (r#"{"o":{"a":1,"a":2},"k":3}"#, r#"{"a":1,"a":2}"#),
        (r#"{"o":{"k":"a\u0041b"},"k":3}"#, r#"{"k":"a\u0041b"}"#),
        (r#"{"o":{"k":"\u00e9"},"k":3}"#, r#"{"k":"\u00e9"}"#),
        (r#"{"o":{"p":"a\/b"}}"#, r#"{"p":"a\/b"}"#),
    ] {
        let (got, _) = run(r#"{a="b"} | json o="o""#, line).unwrap();
        assert_eq!(
            got,
            labels(&[("app", "checkout"), ("env", "prod"), ("o", want)]),
            "line {line:?}"
        );
    }
}

/// An ARRAY does not take the object arm: it falls to
/// `default: unescapeJSONString(data)` (`parser.go:700-706 @ v3.7.4`),
/// which is `jsonparser.Unescape` over the whole span
/// (`vendor/github.com/grafana/jsonparser/escape.go:130-171`) followed by
/// U+FFFD → space (`parser.go:44-49,278-281`). So the span's whitespace
/// survives while its escapes do not.
#[test]
fn json_extraction_of_an_array_unescapes_the_span() {
    for (line, want) in [
        (r#"{"o":[3,  1,2],"k":3}"#, r#"[3,  1,2]"#),
        ("{\"o\":[1,\n2],\"k\":3}", "[1,\n2]"),
        (r#"{"o":["a\"b",  "c"],"k":3}"#, r#"["a"b",  "c"]"#),
        (r#"{"o":["a\u0041b"],"k":3}"#, r#"["aAb"]"#),
        ("{\"o\":[\"x\u{FFFD}y\"]}", "[\"x y\"]"),
    ] {
        let (got, _) = run(r#"{a="b"} | json o="o""#, line).unwrap();
        assert_eq!(
            got,
            labels(&[("app", "checkout"), ("env", "prod"), ("o", want)]),
            "line {line:?}"
        );
    }
}

/// **The discriminator.** Two rows that a plausible-but-wrong fix passes
/// the rest of this file with.
///
/// Turning on `serde_json`'s `preserve_order` feature (or any other
/// order-preserving RE-SERIALISE) makes the key order right —
/// `{"b":1,"a":2}` — and still collapses the two spaces to one comma and
/// still unescapes `A` to `A`. Only handing back the bytes passes
/// both rows. The third row rules out the other cheap fix, for part A:
/// any "truncate at a brace" or brace-counting-outside-strings hack
/// answers something other than `}x`.
#[test]
fn a_key_order_fix_alone_does_not_pass() {
    let (got, _) = run(r#"{a="b"} | json o="o""#, r#"{"o":{"b":1,  "a":2}}"#).unwrap();
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("env", "prod"),
            ("o", r#"{"b":1,  "a":2}"#),
        ])
    );

    let (got, _) = run(r#"{a="b"} | json o="o""#, r#"{"o":{"k":"a\u0041b"}}"#).unwrap();
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("env", "prod"),
            ("o", r#"{"k":"a\u0041b"}"#),
        ])
    );

    let (got, _) = run(r#"{a="b"} | json a="a""#, r#"{"a":"}x"}trailing"#).unwrap();
    assert_eq!(
        got,
        labels(&[("app", "checkout"), ("env", "prod"), ("a", "}x"),])
    );
}

/// The span search and [the walk] must resolve a duplicated key the same
/// way, and the rule is "the first occurrence whose REMAINING path
/// resolves", not a naive first-occurrence descent. Container-captured
/// at v3.7.4; a naive descent gets the `o[0]` column wrong on both lines.
#[test]
fn json_extraction_backtracks_over_a_duplicated_key_like_the_walk() {
    for (line, expr, want) in [
        (r#"{"o":{"z":1},"o":[[7],  8]}"#, "o", r#"{"z":1}"#),
        (r#"{"o":{"z":1},"o":[[7],  8]}"#, "o.z", "1"),
        (r#"{"o":{"z":1},"o":[[7],  8]}"#, "o[0]", "[7]"),
        (r#"{"o":[1],"o":{"z":{"w":  9}}}"#, "o", "[1]"),
        (r#"{"o":[1],"o":{"z":{"w":  9}}}"#, "o.z", r#"{"w":  9}"#),
        (r#"{"o":[1],"o":{"z":{"w":  9}}}"#, "o[0]", "1"),
    ] {
        let (got, _) = run(&format!(r#"{{a="b"}} | json x="{expr}""#), line).unwrap();
        assert_eq!(
            got,
            labels(&[("app", "checkout"), ("env", "prod"), ("x", want)]),
            "line {line:?} expr {expr:?}"
        );
    }
}

/// Nothing on this path may recurse: a path is bounded only by the
/// query-text cap and a document by nothing at all. The nested line is
/// past `serde_json`'s own recursion limit, so the parse fails and the
/// missing-path fill answers — without touching the stack, which is the
/// point. The flat line resolves a container span across 50 000 sibling
/// fields.
#[test]
fn json_expression_resolves_a_span_without_recursing() {
    let deep = format!("{}1{}", "[".repeat(50_000), "]".repeat(50_000));
    let (got, _) = run(r#"{a="b"} | json a="a""#, &format!(r#"{{"a":{deep}}}"#)).unwrap();
    assert_eq!(
        got,
        labels(&[("app", "checkout"), ("env", "prod"), ("a", "")])
    );

    let mut flat = String::from("{");
    for i in 0..50_000 {
        flat.push_str(&format!("\"f{i}\":{i},"));
    }
    flat.push_str(r#""o":{"z":  1}}"#);
    let (got, _) = run(r#"{a="b"} | json o="o""#, &flat).unwrap();
    assert_eq!(
        got,
        labels(&[("app", "checkout"), ("env", "prod"), ("o", r#"{"z":  1}"#)])
    );
}

/// Issue #72 review round 1, finding 2: malformed extraction paths are
/// compile-time `PipelineInvalid` material, never silently normalized
/// into reading a different field.
///
/// **Issue #394 (folded into #388) widened the refused set and, in the
/// other direction, widened the ACCEPTED one.** The shapes below the
/// first blank group are the ones `parse_json_path` served and the
/// reference's own sub-grammar refuses; nothing was removed from the
/// original list. The accept-side control gains `b[ 0 ]`, which
/// `parse_json_path` REFUSED and the reference serves — whitespace is
/// skipped between every token (`jsonexpr/lexer.go:47-49 @ v3.7.4`).
/// The full matrix is `logql_json_expr_matrix.rs`; this is the
/// golden-file end of it.
#[test]
fn malformed_json_extraction_paths_are_named_compile_errors() {
    for expr in [
        // The original list.
        "a..b", "a.", "a.[0]", ".a", "a[", "a[b]", "",
        // Refused since #394: an identifier ends at any character
        // outside `[A-Za-z0-9_]`, and what follows must still parse.
        "a b", "a-c", "a/c", "a:c", "a,c", "a!", "a]", "a.0", "0a", "é",
    ] {
        let query = format!(r#"{{a="b"}} | json x="{expr}""#);
        let err = compile_err(&query);
        assert!(
            matches!(err, PipelineError::BadParserExpr(_)),
            "expr {expr:?}: {err}"
        );
    }
    // The valid shapes still compile — including the whitespace form
    // this change ADDED to the accepted set.
    for expr in [
        "a.b.c",
        "servers[0]",
        r#"req.hdr[\"User-Agent\"]"#,
        "b[ 0 ]",
    ] {
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
fn logfmt_splits_pairs_with_quoted_values_and_drops_empty_bare_keys() {
    // Default-behaviour correction (issue #200): the default `| logfmt` is
    // reference-lenient and DROPS empty-value extractions — the bare key
    // `retry` no longer appears (previously the pre-existing #72 over-keep).
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
            ("took", "250ms"),
            // `retry` (bare key => empty value) is dropped by default.
        ])
    );
}

#[test]
fn logfmt_default_is_lenient_on_malformed_lines() {
    // Default `| logfmt` NEVER sets `__error__` (issue #200 — reference
    // default is lenient best-effort, resolving the pre-existing #72
    // over-eager trigger). Pairs decoded before the malformed token are
    // kept; the error is swallowed.
    //
    // (a) An unterminated quote after a valid pair: `foo=bar` survives.
    let (got, line) = run(r#"{a="b"} | logfmt"#, r#"foo="bar" baz="qux"#).unwrap();
    assert_eq!(
        got,
        labels(&[("app", "checkout"), ("env", "prod"), ("foo", "bar")])
    );
    assert_eq!(
        line, r#"foo="bar" baz="qux"#,
        "parsers never rewrite the line"
    );

    // (b) An unterminated quote as the first token: no extracted label, no
    // `__error__`.
    let (got, _) = run(r#"{a="b"} | logfmt"#, r#"level="unterminated"#).unwrap();
    assert_eq!(got, labels(&[("app", "checkout"), ("env", "prod")]));
}

#[test]
fn logfmt_targeted_extraction_emits_an_empty_label_for_a_missing_source() {
    // **This assertion CHANGED DIRECTION at issue #393**, and it is the
    // place to look hardest. It used to be
    // `logfmt_targeted_extraction_drops_a_missing_source_by_default` and
    // asserted that `missing` was ABSENT, crediting the empty-drop rule
    // to issue #200 — pinning the defect as if it were correct
    // behaviour. The expression parser has no `keepEmpty` field at all
    // (`LogfmtExpressionParserExpr.Stage()` passes only `l.Strict`,
    // `pkg/logql/syntax/ast.go:1073-1075 @ v3.7.4`); the label comes from
    // the unconditional pre-seed of every identifier
    // (`pkg/logql/log/parser.go:545-550`).
    //
    // Captured on `grafana/loki:3.7.4` (buildinfo from the running
    // process: revision `b318f282`) on 2026-08-10: `| logfmt a="nosuch"`
    // over `b=1 a-b=2 x=3` answers `{a=""}` with AND without
    // `--keep-empty`.
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

#[test]
fn logfmt_keep_empty_retains_empty_value_keys() {
    // `--keep-empty` retains empty-value extractions (issue #200): the bare
    // key `retry` comes back as `""`.
    //
    // The second half is the TARGETED arm, where the flag is now inert
    // (issue #393): `missing` comes back as `""` whether or not it is
    // written, and the flagless spelling is asserted by
    // `logfmt_targeted_extraction_emits_an_empty_label_for_a_missing_source`
    // above. Kept here so a future change that reconnects `keep_empty` to
    // the targeted arm has to move BOTH tests, not one.
    let (got, _) = run(
        r#"{a="b"} | logfmt --keep-empty"#,
        "level=error retry took=250ms",
    )
    .unwrap();
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("env", "prod"),
            ("level", "error"),
            ("retry", ""),
            ("took", "250ms"),
        ])
    );

    let (got, _) = run(
        r#"{a="b"} | logfmt --keep-empty lvl="level", missing="nope""#,
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

#[test]
fn logfmt_strict_errors_per_malformed_class() {
    // `--strict` sets `__error__="LogfmtParserErr"` for every malformed
    // class (issue #200). Detail is byte-exact for the unterminated-quote
    // class (oracle_probe.txt [2]) and faithful-format for the others (the
    // LABEL is always correct; only the detail STRING is ledgered).

    // (1) Unterminated quote — `level="unterminated` is 19 runes, pos 20.
    let (got, _) = run(r#"{a="b"} | logfmt --strict"#, r#"level="unterminated"#).unwrap();
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("env", "prod"),
            ("__error__", "LogfmtParserErr"),
            (
                "__error_details__",
                "logfmt syntax error at pos 20 : unterminated quoted value",
            ),
        ])
    );

    // (2) Unexpected `=` — `a=1=2`: the completed `a="1"` pair is kept, the
    // second `=` (rune pos 4) is unexpected.
    let (got, _) = run(r#"{a="b"} | logfmt --strict"#, "a=1=2").unwrap();
    assert_eq!(
        got,
        labels(&[
            ("a", "1"),
            ("app", "checkout"),
            ("env", "prod"),
            ("__error__", "LogfmtParserErr"),
            (
                "__error_details__",
                "logfmt syntax error at pos 4 : unexpected '='",
            ),
        ])
    );

    // (3) A `"` opening a key at rune pos 1 is unexpected. Expected values
    // captured against the pinned reference (v3.7.3): the reference names
    // the offending byte (`unexpected '"'`) and has no "invalid key" text.
    let (got, _) = run(r#"{a="b"} | logfmt --strict"#, r#""quoted=1"#).unwrap();
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("env", "prod"),
            ("__error__", "LogfmtParserErr"),
            (
                "__error_details__",
                r#"logfmt syntax error at pos 1 : unexpected '"'"#,
            ),
        ])
    );

    // (4) A `"` following an UNQUOTED value is unexpected. Reference
    // (v3.7.3): `a=1"b"` → pos 4 `unexpected '"'`; the completed `a="1"`
    // pair before the fault is kept.
    let (got, _) = run(r#"{a="b"} | logfmt --strict"#, r#"a=1"b""#).unwrap();
    assert_eq!(
        got,
        labels(&[
            ("a", "1"),
            ("app", "checkout"),
            ("env", "prod"),
            ("__error__", "LogfmtParserErr"),
            (
                "__error_details__",
                r#"logfmt syntax error at pos 4 : unexpected '"'"#,
            ),
        ])
    );

    // (5) Parity lock: after a CLOSED quoted value the next token may start
    // with no separating whitespace. Reference (v3.7.3): `a="b"c=1` is
    // ACCEPTED as `{a="b", c="1"}` with NO `__error__` — the
    // whitespace-after-close-quote strictness a code-review proposed would
    // have wrongly diverged here.
    let (got, _) = run(r#"{a="b"} | logfmt --strict"#, r#"a="b"c=1"#).unwrap();
    assert_eq!(
        got,
        labels(&[("a", "b"), ("app", "checkout"), ("c", "1"), ("env", "prod"),])
    );
}

// ---------------------------------------------------------------------
// unpack (issue #200)
// ---------------------------------------------------------------------

#[test]
fn unpack_promotes_entry_to_the_line_and_string_fields_to_labels() {
    let (got, line) = run(
        r#"{a="b"} | unpack"#,
        r#"{"_entry":"the real log line","level":"info","count":5,"nested":{"x":1}}"#,
    )
    .unwrap();
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("env", "prod"),
            ("level", "info"),
            // `count` (number) and `nested` (object) are skipped — only
            // string fields become labels.
        ])
    );
    assert_eq!(line, "the real log line");
}

/// No `_entry` field, no labels AT ALL. The reference resolves each
/// field into `lbsBuffer` and `Set`s the buffer only `if isPacked`
/// (`UnpackParser.unpack` and its caller, `pkg/logql/log/parser.go:789-838
/// @ v3.7.4`), and only a string `_entry` sets `isPacked`. Captured at
/// v3.7.4 for issue #334; before that fix PulsusDB promoted the fields
/// anyway.
#[test]
fn unpack_without_an_entry_field_promotes_nothing() {
    let (got, line) = run(r#"{a="b"} | unpack"#, r#"{"level":"warn","svc":"api"}"#).unwrap();
    assert_eq!(got, labels(&[("app", "checkout"), ("env", "prod")]));
    assert_eq!(line, r#"{"level":"warn","svc":"api"}"#);
}

/// The SAME object with `_entry` promotes both fields — so the test
/// above is about the flush gate, not about those fields being
/// unreachable. Captured at v3.7.4.
#[test]
fn unpack_with_an_entry_field_promotes_the_same_fields() {
    let (got, line) = run(
        r#"{a="b"} | unpack"#,
        r#"{"level":"warn","svc":"api","_entry":"real line"}"#,
    )
    .unwrap();
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("env", "prod"),
            ("level", "warn"),
            ("svc", "api"),
        ])
    );
    assert_eq!(line, "real line");
}

#[test]
fn unpack_malformed_line_is_kept_with_the_json_error_label() {
    let (got, line) = run(r#"{a="b"} | unpack"#, "not a json object").unwrap();
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("env", "prod"),
            ("__error__", "JSONParserErr"),
            (
                "__error_details__",
                "Value looks like object, but can't find closing '}' symbol",
            ),
        ])
    );
    assert_eq!(line, "not a json object");
}

/// `_entry` is present so the buffer flushes (see above); a packed field
/// colliding with a stream label lands under `_extracted`. Captured at
/// v3.7.4.
#[test]
fn unpack_collision_with_a_stream_label_lands_under_the_extracted_suffix() {
    let (got, _) = run(
        r#"{a="b"} | unpack"#,
        r#"{"app":"other","x":"1","_entry":"real line"}"#,
    )
    .unwrap();
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

/// A repeated packed key is LAST-wins, the opposite of every other
/// parser, because `RecordExtracted` only happens at the flush — so no
/// earlier field of the same stage is ever "already extracted"
/// (`parser.go:812-837 @ v3.7.4`). Captured at v3.7.4 (issue #334).
#[test]
fn unpack_repeats_a_key_last_write_wins_unlike_every_other_parser() {
    let (got, _) = run(
        r#"{a="b"} | unpack"#,
        r#"{"x":"1","x":"2","_entry":"real line"}"#,
    )
    .unwrap();
    assert_eq!(
        got,
        labels(&[("app", "checkout"), ("env", "prod"), ("x", "2")])
    );
}

// ---------------------------------------------------------------------
// decolorize (issue #200)
// ---------------------------------------------------------------------

#[test]
fn decolorize_strips_ansi_color_escapes() {
    let (got, line) = run(
        r#"{a="b"} | decolorize"#,
        "\u{1b}[31merror:\u{1b}[0m disk full",
    )
    .unwrap();
    assert_eq!(got, labels(&[("app", "checkout"), ("env", "prod")]));
    assert_eq!(line, "error: disk full");
}

#[test]
fn decolorize_leaves_a_line_without_color_codes_unchanged() {
    let (got, line) = run(r#"{a="b"} | decolorize"#, "plain message").unwrap();
    assert_eq!(got, labels(&[("app", "checkout"), ("env", "prod")]));
    assert_eq!(line, "plain message");
}

// ---------------------------------------------------------------------
// drop / keep (issue #200)
// ---------------------------------------------------------------------

#[test]
fn drop_removes_a_bare_label() {
    let (got, _) = run(r#"{a="b"} | logfmt | drop level"#, "level=info msg=hi").unwrap();
    assert_eq!(
        got,
        labels(&[("app", "checkout"), ("env", "prod"), ("msg", "hi")])
    );
}

#[test]
fn drop_with_a_value_matcher_only_removes_on_a_match() {
    let (got, _) = run(
        r#"{a="b"} | logfmt | drop level="info""#,
        "level=info msg=hi",
    )
    .unwrap();
    assert_eq!(
        got,
        labels(&[("app", "checkout"), ("env", "prod"), ("msg", "hi")])
    );
    // A non-matching value keeps the label.
    let (got, _) = run(
        r#"{a="b"} | logfmt | drop level="info""#,
        "level=warn msg=hi",
    )
    .unwrap();
    assert_eq!(
        got,
        labels(&[
            ("app", "checkout"),
            ("env", "prod"),
            ("level", "warn"),
            ("msg", "hi"),
        ])
    );
}

#[test]
fn drop_error_leaves_a_clean_builders_details_invisible() {
    // Issue #238: `drop __error__` resets ONLY the err slot (the reference's
    // `ResetError`, never a `Del`), leaving the details slot set — but a
    // failed `| json` writes no label, so the builder is CLEAN and the
    // materialisation gate (`!hasDel() && !hasAdd() && !HasErr()`,
    // labels.go:554-563) keeps the lone details slot invisible. Same emitted
    // set as before #238, for the opposite mechanistic reason — reference
    // capture `{v0} | json | drop __error__ => {service_name}` (v3.7.4,
    // discover_log_levels: false).
    let (got, _) = run(r#"{a="b"} | json | drop __error__"#, "not json").unwrap();
    assert_eq!(got, labels(&[("app", "checkout"), ("env", "prod")]));
}

#[test]
fn drop_error_plus_a_label_del_surfaces_the_orphaned_details() {
    // The dirty sibling of the test above (without it the pair cannot
    // distinguish the visibility gate from "drop __error__ clears both"):
    // dropping a PRESENT ordinary label is a `Del`, which dirties the
    // builder and re-opens the gate — the orphaned `__error_details__`
    // surfaces. Reference capture `{v0} | json | drop __error__, env =>
    // {__error_details__=DET, service_name}` (v3.7.4).
    let (got, _) = run(r#"{a="b"} | json | drop __error__, app"#, "not json").unwrap();
    assert_eq!(
        got,
        labels(&[
            ("env", "prod"),
            (
                "__error_details__",
                "Value looks like object, but can't find closing '}' symbol"
            ),
        ])
    );
}

#[test]
fn keep_always_retains_the_error_pair() {
    // `keep` skips `__error__`/`__error_details__`/`__preserve_error__` by
    // name (`keep_labels.go:22`, `:51-57`) — they survive ANY keep list.
    // Reference capture `{…} | json | keep env => {__error__, __error_details__,
    // env}` (v3.7.4).
    let (got, _) = run(r#"{a="b"} | json | keep env"#, "not json").unwrap();
    assert_eq!(
        got,
        labels(&[
            ("env", "prod"),
            ("__error__", "JSONParserErr"),
            (
                "__error_details__",
                "Value looks like object, but can't find closing '}' symbol"
            ),
        ])
    );
}

#[test]
fn keep_retains_only_the_listed_labels() {
    let (got, _) = run(
        r#"{a="b"} | logfmt | keep app, level"#,
        "level=info msg=hi svc=api",
    )
    .unwrap();
    // Only `app` and `level` survive — the `env` stream label, `msg`, and
    // `svc` are all dropped.
    assert_eq!(got, labels(&[("app", "checkout"), ("level", "info")]));
}

#[test]
fn keep_with_a_value_matcher_retains_only_on_a_match() {
    let (got, _) = run(
        r#"{a="b"} | logfmt | keep level="info""#,
        "level=info msg=hi",
    )
    .unwrap();
    assert_eq!(got, labels(&[("level", "info")]));
    // A non-matching value drops it too — nothing is kept.
    let (got, _) = run(
        r#"{a="b"} | logfmt | keep level="info""#,
        "level=warn msg=hi",
    )
    .unwrap();
    assert_eq!(got, labels(&[]));
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
fn an_unregistered_template_function_is_rejected_with_the_reference_text() {
    // Post-#230 every reference-registered function COMPILES; only names
    // outside the 86-name surface reject, with Go's parse-error wording
    // wrapped in the reference's line-template prefix (`fmt.go:216`).
    for func in ["sha256sum", "toJson", "b32enc", "nosuchfn"] {
        let query = format!(r#"{{a="b"}} | line_format "{{{{ {func} .x }}}}""#);
        let err = compile_err(&query);
        assert!(matches!(err, PipelineError::InvalidTemplate(_)), "{err}");
        let text = err.to_string();
        assert!(
            text.contains(&format!("function \"{func}\" not defined")),
            "{text}"
        );
        assert!(
            text.starts_with("invalid line template: template: line:1: "),
            "{text}"
        );
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
        pipeline
            .run(r#"{"clean":"1"}"#, &base, 0)
            .expect("no budget breach")
            .is_some(),
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
    let out = pipeline
        .run_metric_into("not json", &base, 0, None, &mut labels)
        .expect("no budget breach");
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

// ---------------------------------------------------------------------
// ip() line + label filters (M8-LQ2)
//
// The IpMatcher parse/contains boundary matrix (v4/v6 × single/CIDR/range,
// off-by-one edges, malformed specs) is pinned exhaustively in the
// `logql::ip` unit tests; these goldens pin the EXEC wiring through the
// client pipeline — line-filter substring scan and label-filter membership,
// negation inversion, the mixed `or` disjunction, and the missing/invalid
// label error semantics — for at least one line + one label case per family.
// ---------------------------------------------------------------------

/// Runs a `| logfmt | <label filter>` pipeline against a logfmt body so the
/// filtered label is a real extracted label; returns the sorted final label
/// set, or `None` when dropped.
fn run_label(query: &str, body: &str) -> Option<Vec<(String, String)>> {
    run(query, body).map(|(labels, _)| labels)
}

#[test]
fn ip_line_filter_v4_cidr_keeps_in_range_and_drops_out_of_range() {
    // In-range IP embedded in a larger line is kept, label set + line intact.
    let (got, line) = run(
        r#"{a="b"} |= ip("10.0.0.0/8")"#,
        "request from 10.1.2.3 done",
    )
    .unwrap();
    assert_eq!(got, labels(&[("app", "checkout"), ("env", "prod")]));
    assert_eq!(line, "request from 10.1.2.3 done");
    // Out-of-range IP drops the line.
    assert!(
        run(
            r#"{a="b"} |= ip("10.0.0.0/8")"#,
            "request from 8.8.8.8 done"
        )
        .is_none()
    );
}

#[test]
fn ip_line_filter_negation_inverts_v4() {
    assert!(run(r#"{a="b"} != ip("10.0.0.0/8")"#, "from 10.1.2.3").is_none());
    assert!(run(r#"{a="b"} != ip("10.0.0.0/8")"#, "from 8.8.8.8").is_some());
}

#[test]
fn ip_line_filter_v4_range_boundaries() {
    let q = r#"{a="b"} |= ip("10.0.0.1-10.0.0.5")"#;
    assert!(run(q, "x 10.0.0.1 y").is_some()); // first
    assert!(run(q, "x 10.0.0.5 y").is_some()); // last
    assert!(run(q, "x 10.0.0.6 y").is_none()); // just past
    assert!(run(q, "x 10.0.0.0 y").is_none()); // just before
}

#[test]
fn ip_line_filter_v4_cidr_boundaries() {
    let q = r#"{a="b"} |= ip("10.0.0.0/24")"#;
    assert!(run(q, "a 10.0.0.0 b").is_some()); // network
    assert!(run(q, "a 10.0.0.255 b").is_some()); // broadcast/last
    assert!(run(q, "a 10.0.1.0 b").is_none()); // first of next block
    assert!(run(q, "a 9.255.255.255 b").is_none()); // last before block
}

#[test]
fn ip_line_filter_v6_cidr_boundaries_including_predecessor() {
    let q = r#"{a="b"} |= ip("2001:db8::/126")"#;
    assert!(run(q, "peer [2001:db8::] up").is_some()); // network
    assert!(run(q, "peer [2001:db8::3] up").is_some()); // last of block
    assert!(run(q, "peer [2001:db8::4] up").is_none()); // first of next block
    // last address of the immediately-preceding block.
    assert!(run(q, "peer [2001:db7:ffff:ffff:ffff:ffff:ffff:ffff] up").is_none());
}

#[test]
fn ip_line_filter_v6_range_boundaries() {
    let q = r#"{a="b"} |= ip("2001:db8::1-2001:db8::5")"#;
    assert!(run(q, "x 2001:db8::1 y").is_some()); // first
    assert!(run(q, "x 2001:db8::5 y").is_some()); // last
    assert!(run(q, "x 2001:db8::6 y").is_none()); // just past
    assert!(run(q, "x 2001:db8:: y").is_none()); // just before
}

#[test]
fn mixed_or_line_filter_matches_via_either_literal_or_ip() {
    let q = r#"{a="b"} |= "boot" or ip("10.0.0.0/8")"#;
    assert!(run(q, "system boot complete").is_some()); // literal alternative
    assert!(run(q, "packet from 10.1.2.3").is_some()); // ip alternative
    assert!(run(q, "nothing relevant here").is_none()); // neither
}

#[test]
fn or_line_filter_disjunction_over_a_rewritten_line() {
    // `| line_format` rewrites the line; the trailing `or` literal filter is
    // non-pushable (post-`line_format`) and evaluated client-side as a
    // disjunction over the rewritten text.
    let q = r#"{a="b"} | logfmt | line_format "{{.level}}" |= "warn" or "error""#;
    // rewritten line = "error" -> matches the second alternative.
    assert!(run(q, "level=error msg=x").is_some());
    // rewritten line = "info" -> matches neither.
    assert!(run(q, "level=info msg=x").is_none());
}

#[test]
fn ip_label_filter_v4_cidr_membership_and_negation() {
    let q = r#"{a="b"} | logfmt | addr = ip("10.0.0.0/8")"#;
    // in range: kept, addr extracted.
    assert_eq!(
        run_label(q, "addr=10.1.2.3").unwrap(),
        labels(&[("app", "checkout"), ("env", "prod"), ("addr", "10.1.2.3")])
    );
    // out of range: dropped.
    assert!(run_label(q, "addr=8.8.8.8").is_none());
    // negation inverts both verdicts.
    let nq = r#"{a="b"} | logfmt | addr != ip("10.0.0.0/8")"#;
    assert!(run_label(nq, "addr=10.1.2.3").is_none());
    assert!(run_label(nq, "addr=8.8.8.8").is_some());
}

#[test]
fn ip_label_filter_v4_range_boundaries() {
    let q = r#"{a="b"} | logfmt | addr = ip("10.0.0.1-10.0.0.5")"#;
    assert!(run_label(q, "addr=10.0.0.1").is_some()); // first
    assert!(run_label(q, "addr=10.0.0.5").is_some()); // last
    assert!(run_label(q, "addr=10.0.0.6").is_none()); // just past
    assert!(run_label(q, "addr=10.0.0.0").is_none()); // just before
}

#[test]
fn ip_label_filter_v6_cidr_boundaries_including_predecessor() {
    let q = r#"{a="b"} | logfmt | addr = ip("2001:db8::/126")"#;
    assert!(run_label(q, "addr=2001:db8::").is_some()); // network
    assert!(run_label(q, "addr=2001:db8::3").is_some()); // last of block
    assert!(run_label(q, "addr=2001:db8::4").is_none()); // first of next block
    assert!(run_label(q, "addr=2001:db7:ffff:ffff:ffff:ffff:ffff:ffff").is_none()); // predecessor
}

#[test]
fn ip_label_filter_v6_range_boundaries() {
    let q = r#"{a="b"} | logfmt | addr = ip("2001:db8::1-2001:db8::5")"#;
    assert!(run_label(q, "addr=2001:db8::1").is_some()); // first
    assert!(run_label(q, "addr=2001:db8::5").is_some()); // last
    assert!(run_label(q, "addr=2001:db8::6").is_none()); // just past
    assert!(run_label(q, "addr=2001:db8::").is_none()); // just before
}

#[test]
fn ip_label_filter_missing_label_drops_under_eq_keeps_under_neq() {
    // Reference v3.7.3 (differential-authoritative): a missing label is a
    // non-match — dropped under `=`, kept under `!=`, and NEVER tagged with
    // `__error__`/`__error_details__`.
    let eq = r#"{a="b"} | logfmt | addr = ip("10.0.0.0/8")"#;
    assert!(run_label(eq, "other=1 msg=x").is_none());
    let neq = r#"{a="b"} | logfmt | addr != ip("10.0.0.0/8")"#;
    let kept = run_label(neq, "other=1 msg=x").expect("missing label survives `!=`");
    assert!(
        !kept
            .iter()
            .any(|(k, _)| k == "__error__" || k == "__error_details__"),
        "a missing IP label must not set any error label: {kept:?}"
    );
}

#[test]
fn ip_label_filter_invalid_value_is_a_non_match_without_error() {
    // Reference v3.7.3 (differential-authoritative): a present-but-unparseable
    // value is a plain non-match — NO `__error__`/`__error_details__` is ever
    // set (this is the key divergence from the numeric label filter, which does
    // tag `LabelFilterErr`). Dropped under `=`, kept under `!=` carrying the raw
    // label untouched.
    let eq = r#"{a="b"} | logfmt | addr = ip("10.0.0.0/8")"#;
    assert!(
        run_label(eq, "addr=not-an-ip msg=x").is_none(),
        "an invalid IP value must DROP under `=`"
    );

    let neq = r#"{a="b"} | logfmt | addr != ip("10.0.0.0/8")"#;
    let kept = run_label(neq, "addr=not-an-ip msg=x").expect("invalid value survives `!=`");
    assert!(
        !kept
            .iter()
            .any(|(k, _)| k == "__error__" || k == "__error_details__"),
        "an invalid IP value must NOT set any error label: {kept:?}"
    );
    assert!(
        kept.contains(&("addr".to_string(), "not-an-ip".to_string())),
        "the raw non-IP label value is carried unchanged: {kept:?}"
    );
}
