//! Hermetic gates for the issue #230 template engine — the plan's
//! structural ACs that are not corpus values: the registry surface
//! (AC-10/AC-13), the fast-path preservation property (AC-5), the
//! method-dispatch boundary (AC-17), the `[]byte` verb split (AC-19),
//! the pinned-address exclusion class and its shape contract
//! (AC-20/AC-21, plan v7 §D), the injectable clock (AC-9's `now` leg),
//! the depth cap, and the pipeline error-flow contracts (per-line
//! `TemplateFormatErr`, line preservation, destination-unset,
//! last-error-wins, frozen data map).
//!
//! Corpus VALUES live in `tests/logqltest/corpus/t*.test`
//! (container-captured, PROVENANCE §issue-230); nothing here pins a
//! reference-derived number that was not probed through the engine's
//! own differential run.

use std::borrow::Cow;

use pulsus_read::logql::CompiledPipeline;
use pulsus_read::logql::pipeline::PipelineError;
use pulsus_read::logql::template::{self, Template, TemplateEnv, TemplateKind};

fn base() -> Vec<(String, String)> {
    vec![
        ("env".to_string(), "prod".to_string()),
        ("service_name".to_string(), "checkout".to_string()),
    ]
}

fn compiled(query: &str) -> CompiledPipeline {
    let expr = pulsus_logql::parse(query).expect("parse");
    let pulsus_logql::Expr::Log(log) = expr else {
        panic!("expected a log query: {query}");
    };
    CompiledPipeline::compile(&log.pipeline)
        .expect("compile")
        .with_template_env(TemplateEnv::default())
}

fn compile_err(query: &str) -> PipelineError {
    let expr = pulsus_logql::parse(query).expect("parse");
    let pulsus_logql::Expr::Log(log) = expr else {
        panic!("expected a log query: {query}");
    };
    CompiledPipeline::compile(&log.pipeline).expect_err("must reject")
}

/// Renders one line_format body over a fixed label set through the FULL
/// engine surface (compile + render), UTC-degenerate env.
fn render(tmpl: &str, ts_ns: i64) -> Result<String, String> {
    render_env(tmpl, ts_ns, &TemplateEnv::default())
}

fn render_env(tmpl: &str, ts_ns: i64, env: &TemplateEnv) -> Result<String, String> {
    let compiled =
        template::compile(tmpl, TemplateKind::Line).map_err(|e| format!("compile: {e}"))?;
    let labels: Vec<(Cow<'_, str>, Cow<'_, str>)> = vec![
        (Cow::Borrowed("a"), Cow::Borrowed("Hello")),
        (Cow::Borrowed("env"), Cow::Borrowed("prod")),
    ];
    match compiled {
        Template::Full(prog) => {
            let mut out = Vec::new();
            template::render_full(&prog, &labels, None, None, "the line", ts_ns, env, &mut out)
                .map_err(|e| e.msg)?;
            Ok(String::from_utf8_lossy(&out).into_owned())
        }
        other => panic!("expected a Full template for {tmpl:?}, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// Registry surface (AC-10 / AC-13).
// ---------------------------------------------------------------------

/// The 67 non-builtin names the reference registers (23 `functionMap` +
/// 2 injected + 42 sprig — plan v2 Δ2), written out literally.
const NON_BUILTINS: [&str; 67] = [
    // functionMap literal (23).
    "ToLower",
    "ToUpper",
    "Replace",
    "Trim",
    "TrimLeft",
    "TrimRight",
    "TrimPrefix",
    "TrimSuffix",
    "TrimSpace",
    "regexReplaceAll",
    "regexReplaceAllLiteral",
    "count",
    "urldecode",
    "urlencode",
    "bytes",
    "duration",
    "duration_seconds",
    "unixEpochMillis",
    "unixEpochNanos",
    "toDateInZone",
    "unixToTime",
    "alignLeft",
    "alignRight",
    // injected per line (2).
    "__line__",
    "__timestamp__",
    // sprig subset (42).
    "b64enc",
    "b64dec",
    "lower",
    "upper",
    "title",
    "trunc",
    "substr",
    "contains",
    "hasPrefix",
    "hasSuffix",
    "indent",
    "nindent",
    "replace",
    "repeat",
    "trim",
    "trimAll",
    "trimSuffix",
    "trimPrefix",
    "int",
    "float64",
    "add",
    "sub",
    "mul",
    "div",
    "mod",
    "addf",
    "subf",
    "mulf",
    "divf",
    "max",
    "min",
    "maxf",
    "minf",
    "ceil",
    "floor",
    "round",
    "fromJson",
    "date",
    "toDate",
    "now",
    "unixEpoch",
    "default",
];

/// The 19 text/template builtins.
const BUILTINS: [&str; 19] = [
    "and", "call", "html", "index", "slice", "js", "len", "not", "or", "print", "printf",
    "println", "urlquery", "eq", "ge", "gt", "le", "lt", "ne",
];

#[test]
fn the_registry_is_exactly_the_67_reference_names() {
    let mut names = template::funcs::registry_names();
    names.sort_unstable();
    let mut expected: Vec<&str> = NON_BUILTINS.to_vec();
    expected.sort_unstable();
    assert_eq!(names, expected);
    // No duplicates across the union with the builtins; 86 total.
    let mut all: Vec<&str> = NON_BUILTINS
        .iter()
        .chain(BUILTINS.iter())
        .copied()
        .collect();
    all.sort_unstable();
    let before = all.len();
    all.dedup();
    assert_eq!(before, all.len(), "builtins and registry must not collide");
    assert_eq!(all.len(), 86);
    let mut callable: Vec<&str> = template::funcs::all_callable_names().to_vec();
    callable.sort_unstable();
    assert_eq!(callable, all);
}

#[test]
fn every_registered_name_compiles_and_every_other_sprig_name_rejects() {
    for name in NON_BUILTINS.iter().chain(BUILTINS.iter()) {
        // `and`/`or` etc. all parse as callables — the compile must not
        // reject any registered name.
        let tmpl = format!("{{{{ {name} }}}}");
        assert!(
            template::compile(&tmpl, TemplateKind::Line).is_ok(),
            "{name} must be callable"
        );
    }
    // Sprig names the reference does NOT expose (plan AC-10).
    for name in [
        "sha256sum",
        "sha1sum",
        "toJson",
        "toPrettyJson",
        "b32enc",
        "ternary",
        "coalesce",
    ] {
        let tmpl = format!("{{{{ {name} .x }}}}");
        let err = template::compile(&tmpl, TemplateKind::Line).expect_err(name);
        assert_eq!(
            err.to_string(),
            format!("template: line:1: function \"{name}\" not defined"),
        );
    }
    // min/max DO resolve (they are sprig entries; text/template has no
    // min/max builtin to shadow — plan AC-10).
    assert_eq!(render("{{ max 1 5 3 }}|{{ min 1 5 3 }}", 0).unwrap(), "5|1");
}

// ---------------------------------------------------------------------
// Fast-path preservation (AC-5).
// ---------------------------------------------------------------------

#[test]
fn every_b2_corpus_template_still_compiles_to_a_fast_path() {
    // Property over the committed b2 corpus: every template the
    // pre-#230 subset served must keep the Simple/Parts fast path.
    let text = include_str!("logqltest/corpus/b2_formatters.test");
    let mut checked = 0;
    for chunk in text.split(&['"', '`'][..]) {
        // Extract template-looking payloads: anything with `{{`.
        if !chunk.contains("{{") {
            continue;
        }
        let Ok(t) = template::compile(chunk, TemplateKind::Line) else {
            continue; // not a template payload (e.g. a log line)
        };
        checked += 1;
        assert!(
            !matches!(t, Template::Full(_)),
            "b2 template {chunk:?} must stay on the Simple/Parts fast path"
        );
    }
    assert!(checked >= 5, "the b2 sweep must actually find templates");
}

#[test]
fn fast_path_derivation_matches_the_template_shape() {
    assert!(matches!(
        template::compile("{{.message}}", TemplateKind::Line).unwrap(),
        Template::Simple(name) if name == "message"
    ));
    assert!(matches!(
        template::compile("a {{.x}} b", TemplateKind::Line).unwrap(),
        Template::Parts(_)
    ));
    // Trim markers change adjacent text, not the shape class.
    assert!(matches!(
        template::compile("a {{- .x -}} b", TemplateKind::Line).unwrap(),
        Template::Parts(_)
    ));
    for full in [
        "{{ upper .x }}",
        "{{ .a.b }}",
        "{{ . }}",
        "{{ if .x }}y{{ end }}",
    ] {
        assert!(
            matches!(
                template::compile(full, TemplateKind::Line).unwrap(),
                Template::Full(_)
            ),
            "{full} must compile to Full"
        );
    }
}

// ---------------------------------------------------------------------
// Method dispatch boundary (AC-17).
// ---------------------------------------------------------------------

#[test]
fn method_dispatch_resolves_nothing_outside_the_time_closure() {
    use pulsus_read::logql::template::value::{IntKind, UintKind, Value};
    use std::rc::Rc;
    let non_time: Vec<Value<'static>> = vec![
        Value::Nil,
        Value::Str(Cow::Borrowed(b"x")),
        Value::Int(1, IntKind::Int),
        Value::Uint(1, UintKind::Uint8),
        Value::Float(1.5),
        Value::Complex(1.0, 2.0),
        Value::Bool(true),
        Value::Bytes(Cow::Borrowed(b"b")),
        Value::List(Rc::new(vec![])),
        Value::Map(Rc::new(vec![])),
        Value::LabelMap,
    ];
    for v in &non_time {
        for name in ["String", "Unix", "Format", "Year", "Seconds", "Nope"] {
            assert!(
                template::methods::method(v, name).is_none(),
                "{name} must not resolve on {v:?}"
            );
        }
    }
    // And the closure itself resolves (both directions discriminate).
    use pulsus_read::logql::template::timefns::GoTime;
    let t = Value::Time(GoTime::from_unix_ns(0));
    assert!(template::methods::method(&t, "Unix").is_some());
    assert!(template::methods::method(&t, "Nope").is_none());
    assert!(template::methods::method(&Value::Duration(1), "Seconds").is_some());
    assert!(template::methods::method(&Value::Month(1), "String").is_some());
}

// ---------------------------------------------------------------------
// []byte verb split (AC-19).
// ---------------------------------------------------------------------

#[test]
fn byte_slices_print_as_numbers_under_v_and_d_but_as_strings_under_s_q_x() {
    const TS: i64 = 1_785_055_647_000_000_000;
    let tmpl = |verb: &str| format!("{{{{ printf \"%{verb}\" __timestamp__.MarshalJSON }}}}");
    let v = render(&tmpl("v"), TS).unwrap();
    let d = render(&tmpl("d"), TS).unwrap();
    assert_eq!(v, d, "%v and %d agree on the bracketed decimal form");
    assert!(v.starts_with("[34 "), "bracketed decimals: {v}");
    let s = render(&tmpl("s"), TS).unwrap();
    assert_eq!(s, "\"2026-07-26T08:47:27Z\"");
    let q = render(&tmpl("q"), TS).unwrap();
    assert_eq!(q, "\"\\\"2026-07-26T08:47:27Z\\\"\"");
    let x = render(&tmpl("x"), TS).unwrap();
    assert!(x.starts_with("22323032362d30372d3236"), "{x}");
    assert_ne!(s, v);
    assert_eq!(
        render("{{ printf \"%T\" __timestamp__.MarshalJSON }}", TS).unwrap(),
        "[]uint8"
    );
}

// ---------------------------------------------------------------------
// The pinned-address exclusion class (AC-20 / AC-21, plan v7 §D).
// ---------------------------------------------------------------------

/// The 24 argument-consuming forms of the re-derived verb domain
/// (plan v7 §A; `%y` stands for the catch-all class).
const FORMS: [&str; 24] = [
    "v", "+v", "#v", "T", "s", "q", "d", "b", "o", "O", "x", "X", "c", "U", "e", "E", "f", "F",
    "g", "G", "t", "p", "w", "y",
];

/// The 9 captured value shapes as template sub-expressions (receiver
/// bindings per plan v3/v7; `$t` = `__timestamp__`).
const SHAPES: [(&str, &str); 9] = [
    ("bytes", "$t.MarshalJSON"),
    ("time_stock", "$t"),
    ("time_utc", "$t.UTC"),
    ("time_loaded", "$paris"),
    ("duration", "$t.Sub ($t.AddDate 0 0 -1)"),
    ("month", "$t.Month"),
    ("weekday", "$t.Weekday"),
    ("loc_stock", "$t.Location"),
    ("loc_loaded", "$paris.Location"),
];

/// Plan v7 §D: exactly these (shape, form) cells may render an address.
fn is_excluded(shape: &str, form: &str) -> bool {
    match shape {
        "bytes" => form == "p",
        "time_stock" => matches!(form, "d" | "b" | "o" | "p" | "w"),
        "loc_stock" => form == "p",
        // Plan v7 §D put only the ADDRESS-carrying verbs here (6); the
        // capture showed the remaining recursing verbs dump the loaded
        // zone's ENTIRE tzdata transition table (deterministic within
        // one binary, but tzdata-coupled and NUL-carrying) — the OQ-3
        // ratified tzdata class, so they join the exclusion set (plan
        // correction 195→184 goldens; flagged in the implementation
        // notes).
        "loc_loaded" => matches!(
            form,
            "#v" | "d"
                | "b"
                | "o"
                | "O"
                | "c"
                | "U"
                | "e"
                | "E"
                | "f"
                | "F"
                | "g"
                | "G"
                | "t"
                | "p"
                | "w"
                | "y"
        ),
        "time_loaded" => matches!(
            form,
            "d" | "b"
                | "o"
                | "O"
                | "c"
                | "U"
                | "e"
                | "E"
                | "f"
                | "F"
                | "g"
                | "G"
                | "t"
                | "p"
                | "w"
                | "y"
        ),
        _ => false,
    }
}

/// The pinned stand-in's renderings across the numeric bases
/// (`PINNED_ADDR = 0xfa11ed`): any of these in an output marks an
/// address cell.
fn has_pinned_token(s: &str) -> bool {
    [
        "fa11ed",
        "FA11ED",
        "16388589",
        "76410755",
        "111110100001000111101101",
    ]
    .iter()
    .any(|t| s.contains(t))
}

#[test]
fn the_address_class_is_exactly_the_29_excluded_cells() {
    const TS: i64 = 1_785_055_647_000_000_000;
    let mut excluded_seen = 0;
    for (shape, expr) in SHAPES {
        for form in FORMS {
            let tmpl = format!(
                "{{{{ $t := __timestamp__ }}}}{{{{ $paris := toDateInZone \"2006-01-02\" \"Europe/Paris\" \"2023-01-15\" }}}}{{{{ printf \"%{form}\" ({expr}) }}}}"
            );
            let out =
                render(&tmpl, TS).unwrap_or_else(|e| panic!("cell {shape}×%{form} errored: {e}"));
            let has = has_pinned_token(&out);
            assert_eq!(
                has,
                is_excluded(shape, form),
                "cell {shape}×%{form}: pinned-address presence mismatch — output {out:?}"
            );
            if has {
                excluded_seen += 1;
            }
        }
    }
    // 29 per plan v7 §D + the 11 loc_loaded tzdata-table cells the
    // capture surfaced (see `is_excluded`).
    assert_eq!(
        excluded_seen, 40,
        "the exclusion class holds exactly 40 cells"
    );
}

#[test]
fn pinned_substitutes_keep_the_reference_shape() {
    const TS: i64 = 1_785_055_647_000_000_000;
    // time_stock %d: {wall ext loc} with the pinned loc address; wall
    // and ext are Go's REAL internal layout (differentially verified).
    let out = render("{{ printf \"%d\" __timestamp__ }}", TS).unwrap();
    assert_eq!(out, "{0 63920652447 16388589}");
    // stock UTC Location %d stays fully deterministic (a GOLDEN cell).
    let out = render(
        "{{ $t := __timestamp__ }}{{ printf \"%d\" $t.Location }}",
        TS,
    )
    .unwrap();
    assert_eq!(out, "&{%!d(string=UTC) [] [] %!d(string=) 0 0 0}");
    // loaded-zone Location %#v: typed pinned pointer, empty pinned
    // tables (tzdata-coupled in the reference — ledgered).
    let out = render(
        "{{ printf \"%#v\" (toDateInZone \"2006-01-02\" \"Europe/Paris\" \"2023-01-15\").Location }}",
        TS,
    )
    .unwrap();
    assert_eq!(
        out,
        "&time.Location{name:\"Europe/Paris\", zone:[]time.zone(nil), tx:[]time.zoneTrans(nil), \
         extend:\"\", cacheStart:0, cacheEnd:0, cacheZone:(*time.zone)(0xfa11ed)}"
    );
    // %p on a slice-kind receiver.
    let out = render("{{ printf \"%p\" (fromJson \"[1]\") }}", TS).unwrap();
    assert_eq!(out, "0xfa11ed");
}

#[test]
fn no_corpus_golden_contains_a_pinned_address_token() {
    for corpus in [
        include_str!("logqltest/corpus/t1_template_core.test"),
        include_str!("logqltest/corpus/t2_printf.test"),
        include_str!("logqltest/corpus/t3_strings.test"),
        include_str!("logqltest/corpus/t4_numeric.test"),
        include_str!("logqltest/corpus/t5_time.test"),
        include_str!("logqltest/corpus/t6_errors_edges.test"),
    ] {
        for (i, line) in corpus.lines().enumerate() {
            assert!(
                !has_pinned_token(line),
                "corpus line {} carries a pinned-address token: {line}",
                i + 1
            );
        }
    }
}

// ---------------------------------------------------------------------
// Injectable clock (AC-9's `now` leg) + zone environment.
// ---------------------------------------------------------------------

#[test]
fn now_renders_the_injected_wall_clock() {
    let env = TemplateEnv {
        local: None,
        local_name: None,
        now_ns: Some(1_700_000_000_123_456_789),
    };
    assert_eq!(
        render_env("{{ now.UnixNano }}", 0, &env).unwrap(),
        "1700000000123456789"
    );
    assert_eq!(
        render_env("{{ unixEpoch now }}", 0, &env).unwrap(),
        "1700000000"
    );
    // `date` with a non-time operand falls back to now (sprig
    // dateInZone's default arm).
    assert_eq!(
        render_env("{{ date \"2006-01-02\" \"not-a-time\" }}", 0, &env).unwrap(),
        "2023-11-14"
    );
}

// ---------------------------------------------------------------------
// Depth cap (pinned at the wasm-tier 1000 — ledgered divergence from
// the reference's goroutine-stack-backed 100000).
// ---------------------------------------------------------------------

#[test]
fn runaway_template_recursion_is_a_bounded_per_line_error() {
    let err = render(
        "{{ define \"R\" }}{{ template \"R\" }}{{ end }}{{ template \"R\" }}",
        0,
    )
    .expect_err("must exceed the depth cap");
    assert!(
        err.contains("exceeded maximum template depth (1000)"),
        "{err}"
    );
}

// ---------------------------------------------------------------------
// Pipeline error-flow contracts (`fmt.go:252-256`, `:426-429`).
// ---------------------------------------------------------------------

#[test]
fn a_failing_line_format_keeps_the_line_and_tags_template_format_err() {
    let pipeline = compiled(r#"{a="b"} | line_format "{{ divf 1 0 }}""#);
    let base = base();
    let out = pipeline
        .run("the original line", &base, 0)
        .expect("no budget breach")
        .expect("kept");
    assert_eq!(out.line, "the original line");
    let get = |name: &str| {
        out.labels
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.to_string())
    };
    assert_eq!(get("__error__").as_deref(), Some("TemplateFormatErr"));
    let details = get("__error_details__").expect("details set");
    assert!(
        details.contains("error calling divf: decimal division by 0"),
        "{details}"
    );
}

#[test]
fn a_failing_label_format_leaves_the_destination_unset_and_last_error_wins() {
    let pipeline = compiled(
        r#"{a="b"} | label_format one="{{ divf 1 0 }}", two="{{ substr 9 2 .env }}", ok="{{ .env }}""#,
    );
    let base = base();
    let out = pipeline
        .run("line", &base, 0)
        .expect("no budget breach")
        .expect("kept");
    let get = |name: &str| {
        out.labels
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.to_string())
    };
    assert_eq!(get("one"), None, "failed destination must stay unset");
    assert_eq!(get("two"), None);
    assert_eq!(
        get("ok").as_deref(),
        Some("prod"),
        "later elements still run"
    );
    assert_eq!(get("__error__").as_deref(), Some("TemplateFormatErr"));
    let details = get("__error_details__").expect("details");
    assert!(
        details.contains("error calling substr"),
        "LAST error wins (SetErr overwrites): {details}"
    );
}

#[test]
fn the_label_format_data_map_freezes_the_error_pair_at_map_build() {
    // `c` fails AFTER the map was built; `d` renders from the FROZEN
    // map, so it must NOT see c's TemplateFormatErr (the reference's
    // once-per-stage `IntoMap` — plan §Δ StageMap freeze).
    let pipeline =
        compiled(r#"{a="b"} | label_format c="{{ divf 1 0 }}", d="E[{{ .__error__ }}]""#);
    let base = base();
    let out = pipeline
        .run("line", &base, 0)
        .expect("no budget breach")
        .expect("kept");
    let get = |name: &str| {
        out.labels
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.to_string())
    };
    assert_eq!(get("c"), None);
    assert_eq!(
        get("d").as_deref(),
        Some("E[]"),
        "the frozen map must not show the mid-stage template error"
    );
    assert_eq!(get("__error__").as_deref(), Some("TemplateFormatErr"));
}

#[test]
fn timestamps_thread_into_the_template_context() {
    let pipeline =
        compiled(r#"{a="b"} | line_format "{{ __timestamp__.UnixNano }}|{{ __line__ }}""#);
    let base = base();
    let out = pipeline
        .run("raw body", &base, 1_785_055_647_123_456_789)
        .expect("no budget breach")
        .expect("kept");
    assert_eq!(out.line, "1785055647123456789|raw body");
}

#[test]
fn template_compile_errors_carry_the_reference_wrapping() {
    let err = compile_err(r#"{a="b"} | line_format "{{ nosuchfn . }}""#);
    assert_eq!(
        err.to_string(),
        "invalid line template: template: line:1: function \"nosuchfn\" not defined"
    );
    let err = compile_err(r#"{a="b"} | label_format x="{{ nosuchfn . }}""#);
    assert_eq!(
        err.to_string(),
        "invalid template for label 'x': template: label:1: function \"nosuchfn\" not defined"
    );
    // Malformed templates (lex/parse classes).
    let err = compile_err(r#"{a="b"} | line_format "{{ .a""#);
    assert!(err.to_string().contains("unclosed action"), "{err}");
    let err = compile_err(r#"{a="b"} | line_format "{{ .a }""#);
    assert!(
        err.to_string().contains("unexpected \"}\" in operand"),
        "{err}"
    );
    let err = compile_err(r#"{a="b"} | line_format "{{end}}""#);
    assert!(err.to_string().contains("unexpected {{end}}"), "{err}");
    // The #231 reserved-destination rejection is unchanged.
    let err = compile_err(r#"{a="b"} | label_format __error__="x""#);
    assert!(
        err.to_string().contains("__error__ cannot be formatted"),
        "{err}"
    );
}

#[test]
fn full_templates_route_the_metric_path_through_fan_out_grouping() {
    // A Full template can fail per line (label-set change) — the
    // compile must flag mutates_labels so the metric fan-out groups by
    // final labels; the Parts shapes must NOT (fast-path preservation).
    assert!(compiled(r#"{a="b"} | line_format "{{ divf 1 0 }}""#).metric_mutates_labels());
    assert!(!compiled(r#"{a="b"} | line_format "L={{.env}}""#).metric_mutates_labels());
}

// ---------------------------------------------------------------------
// The per-render output budget (issue #230 follow-up): charge before
// allocate, breach to the bounded 422 — never an OOM (the reference is
// unbounded here; ledgered `template-output-budget`).
// ---------------------------------------------------------------------

#[test]
fn a_repeat_render_at_the_budget_succeeds_and_one_past_it_is_a_clean_query_error() {
    use pulsus_read::logql::template::MAX_TEMPLATE_RENDER_BYTES;
    let base = base();
    // Exactly AT the budget: `count × len` is charged up front and fits.
    let at = MAX_TEMPLATE_RENDER_BYTES;
    let pipeline = compiled(&format!(
        r#"{{a="b"}} | line_format "{{{{ repeat {at} \"x\" }}}}""#
    ));
    let out = pipeline
        .run("line", &base, 0)
        .expect("at-budget render must succeed")
        .expect("kept");
    assert_eq!(out.line.len() as u64, at);
    drop(out);
    // One byte PAST the budget: the whole query fails cleanly BEFORE the
    // allocation — no per-line TemplateFormatErr, no truncation.
    let over = at + 1;
    let pipeline = compiled(&format!(
        r#"{{a="b"}} | line_format "{{{{ repeat {over} \"x\" }}}}""#
    ));
    let err = pipeline
        .run("line", &base, 0)
        .expect_err("over-budget render must abort the query");
    assert_eq!(err.budget_bytes, MAX_TEMPLATE_RENDER_BYTES);
    // The metric path aborts identically.
    let mut labels = Vec::new();
    pipeline
        .run_metric_into("line", &base, 0, &mut labels)
        .expect_err("metric path must abort too");
    // And label_format renders share the same budget.
    let pipeline = compiled(&format!(
        r#"{{a="b"}} | label_format x="{{{{ repeat {over} \"y\" }}}}""#
    ));
    pipeline
        .run("line", &base, 0)
        .expect_err("label_format over-budget render must abort the query");
}

#[test]
fn printf_padding_width_is_charged_against_the_render_budget() {
    let base = base();
    // A ~953 MiB padding request (width < the 1<<30 parse cap) must be a
    // clean bounded error, never an allocation.
    let pipeline = compiled(r#"{a="b"} | line_format "{{ printf \"%999999999d\" 1 }}""#);
    pipeline
        .run("line", &base, 0)
        .expect_err("giant padding width must abort the query");
    // A sane width still renders.
    let pipeline = compiled(r#"{a="b"} | line_format "{{ printf \"%9d\" 1 }}""#);
    let out = pipeline
        .run("line", &base, 0)
        .expect("no budget breach")
        .expect("kept");
    assert_eq!(out.line, "        1");
}

#[test]
fn a_budget_breach_surfaces_as_the_bounded_query_too_broad_422_class() {
    use pulsus_read::logql::error::{ReadError, TooBroadReason};
    use pulsus_read::logql::exec::run_pipeline_rows;
    use pulsus_read::logql::rows::{SampleRow, StreamMetaRow};
    use pulsus_read::logql::template::MAX_TEMPLATE_RENDER_BYTES;
    let over = MAX_TEMPLATE_RENDER_BYTES + 1;
    let pipeline = compiled(&format!(
        r#"{{a="b"}} | line_format "{{{{ repeat {over} \"x\" }}}}""#
    ));
    let meta = std::collections::HashMap::from([(
        1u64,
        StreamMetaRow {
            fingerprint: 1,
            service: "svc".to_string(),
            labels: r#"{"env":"prod"}"#.to_string(),
        },
    )]);
    let rows = vec![SampleRow {
        fingerprint: 1,
        timestamp_ns: 0,
        body: "line".to_string(),
        structured_metadata: String::new(),
    }];
    let err = run_pipeline_rows(rows, &pipeline, &meta, 100)
        .expect_err("the streams assembly must abort");
    assert!(
        matches!(
            err,
            ReadError::QueryTooBroad(TooBroadReason::TemplateOutputBytes {
                budget_bytes
            }) if budget_bytes == MAX_TEMPLATE_RENDER_BYTES
        ),
        "{err:?}"
    );
}
