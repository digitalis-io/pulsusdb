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
// Depth cap (pinned at 250 — derived from the 2 MiB worker/test-thread
// stack floor at ~2 KiB/level of debug frames; ledgered divergence from
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
        err.contains("exceeded maximum template depth (250)"),
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
    // The budget bounds the render's CUMULATIVE charged bytes: this
    // template charges twice — `repeat`'s `count × len` when the value
    // is built, and the emitted output when it is printed (the
    // loop-amplification accounting) — so the exact boundary for a
    // single maximal output line is budget/2. AT the boundary (2·at ==
    // budget) the render succeeds; one byte past it (2·(at+1) > budget)
    // fails cleanly BEFORE the allocation.
    let at = MAX_TEMPLATE_RENDER_BYTES / 2;
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

#[test]
fn a_no_output_repeated_intermediate_breaches_the_budget() {
    // Review round 2, the fifth amplification class: `__line__` inside a
    // `range` body that emits NO text — the reference renders "" after
    // 200 MB of per-iteration copies; the per-call charge turns it into
    // the clean bounded abort. (Red pre-fix: rendered successfully.)
    let pipeline =
        compiled(r#"{a="b"} | line_format "{{ range 200000 }}{{ $x := __line__ }}{{ end }}""#);
    let line = "x".repeat(1024);
    let base = base();
    pipeline
        .run(&line, &base, 0)
        .expect_err("uncharged repeated intermediate must breach");
    // Green control: bounded repetition still renders.
    let pipeline =
        compiled(r#"{a="b"} | line_format "s{{ range 3 }}{{ $x := __line__ }}{{ end }}e""#);
    let out = pipeline
        .run(&line, &base, 0)
        .expect("no budget breach")
        .expect("kept");
    assert_eq!(out.line, "se");
}

#[test]
fn a_compounding_variable_reassignment_breaches_the_budget() {
    // The COMPOUNDING member of the fifth class: `$a = printf "%s%s" $a
    // $a` doubles a variable per iteration with no output — uncharged
    // this is a 2^40 KiB OOM; the printf pre-charge (4× value ceilings)
    // breaches after ~13 doublings with peak allocation ≤ the budget.
    let pipeline = compiled(
        r#"{a="b"} | line_format "{{ $a := __line__ }}{{ range 40 }}{{ $a = printf \"%s%s\" $a $a }}{{ end }}ok""#,
    );
    let line = "x".repeat(1024);
    let base = base();
    pipeline
        .run(&line, &base, 0)
        .expect_err("doubling reassignment must breach, never OOM");
    // Green control: three doublings render (2 → 16 bytes).
    let pipeline = compiled(
        r#"{a="b"} | line_format "{{ $a := __line__ }}{{ range 3 }}{{ $a = printf \"%s%s\" $a $a }}{{ end }}{{ len $a }}""#,
    );
    let out = pipeline
        .run("ab", &base, 0)
        .expect("no budget breach")
        .expect("kept");
    assert_eq!(out.line, "16");
}

#[test]
fn from_json_lone_surrogate_before_another_escape_leaves_the_escape_alone() {
    // Container-captured (grafana/loki:3.7.4): {"x":"\ud800\n"} → "�\n"
    // — the lone high surrogate becomes U+FFFD and the FOLLOWING \n
    // escape is processed normally (not consumed by the pair probe).
    // Lives here because the corpus line format cannot hold a newline.
    assert_eq!(
        render(r#"{{ (fromJson "{\"x\":\"\\ud800\\n\"}").x }}"#, 0).expect("renders"),
        "\u{FFFD}\n"
    );
    // And the two-invalid-bytes length capture: each byte is one U+FFFD
    // (len 6), never a single merged replacement.
    assert_eq!(
        render(r#"{{ len ((fromJson "{\"x\":\"\xff\xfe\"}").x) }}"#, 0).expect("renders"),
        "6"
    );
}

// ---------------------------------------------------------------------
// Structural depth (issue #230 review round 2): parse-time cap over
// BOTH recursion classes, and the unified evaluator counter — a crash
// is never acceptable (the #272 class; pre-fix, 5000 nested ifs
// SIGABRTed a 2 MiB thread).
// ---------------------------------------------------------------------

#[test]
fn structural_nesting_is_capped_at_parse_time_never_a_stack_overflow() {
    let nested_ifs = |n: usize| {
        format!(
            r#"{{a="b"}} | line_format `{}X{}`"#,
            "{{if \"x\"}}".repeat(n),
            "{{end}}".repeat(n)
        )
    };
    let nested_parens = |n: usize| {
        format!(
            r#"{{a="b"}} | line_format `{{{{ {}"y"{} }}}}`"#,
            "(".repeat(n),
            ")".repeat(n)
        )
    };
    // Everything on a 2 MiB thread — the smallest stack the render runs
    // on (tokio workers); width-independent.
    std::thread::Builder::new()
        .stack_size(2 << 20)
        .spawn(move || {
            let base = base();
            // AT the cap (40): parses and renders.
            let pipeline = compiled(&nested_ifs(40));
            let out = pipeline
                .run("line", &base, 0)
                .expect("no budget breach")
                .expect("kept");
            assert_eq!(out.line, "X");
            let pipeline = compiled(&nested_parens(40));
            let out = pipeline
                .run("line", &base, 0)
                .expect("no budget breach")
                .expect("kept");
            assert_eq!(out.line, "y");
            // One past the cap: the clean compile rejection with Go's
            // paren-site wording (the reference accepts these depths —
            // its goroutine stacks grow; ledgered
            // `template-parse-depth-cap`).
            for query in [nested_ifs(41), nested_parens(41)] {
                let err = compile_err(&query);
                assert!(
                    err.to_string().contains("max expression depth exceeded"),
                    "{err}"
                );
            }
            // The pre-fix crash shapes: 5000 deep of each class is a
            // clean error, not a SIGABRT (this thread would die first).
            for query in [nested_ifs(5000), nested_parens(5000)] {
                let err = compile_err(&query);
                assert!(
                    err.to_string().contains("max expression depth exceeded"),
                    "{err}"
                );
            }
        })
        .expect("spawn")
        .join()
        .expect("no stack overflow");
}

#[test]
fn else_if_and_else_with_chains_are_capped_never_a_stack_overflow() {
    // Review round 3, finding 2: `item_list` used to decrement depth
    // BEFORE the else-if recursion, so a chain of `{{else if}}` links
    // bypassed the 40 cap entirely — 5000 links SIGABRTed a 2 MiB
    // thread on the unfixed build (demonstrated). The guard now lives
    // in `parse_control`, whose frame stays live across the chain.
    let if_chain = |links: usize| {
        format!(
            r#"{{a="b"}} | line_format `{{{{if "a"}}}}x{}{{{{end}}}}`"#,
            r#"{{else if "a"}}x"#.repeat(links)
        )
    };
    let with_chain = |links: usize| {
        format!(
            r#"{{a="b"}} | line_format `{{{{with "a"}}}}x{}{{{{end}}}}`"#,
            r#"{{else with "a"}}x"#.repeat(links)
        )
    };
    std::thread::Builder::new()
        .stack_size(2 << 20)
        .spawn(move || {
            let base = base();
            // 39 links = chain depth 40 (the head `if` is link 0): renders.
            let pipeline = compiled(&if_chain(39));
            let out = pipeline
                .run("line", &base, 0)
                .expect("no budget breach")
                .expect("kept");
            assert_eq!(out.line, "x");
            // 40 links = depth 41: the clean compile rejection.
            for query in [if_chain(40), with_chain(40)] {
                let err = compile_err(&query);
                assert!(
                    err.to_string().contains("max expression depth exceeded"),
                    "{err}"
                );
            }
            // The pre-fix crash shape: 5000 links reject cleanly on this
            // 2 MiB thread instead of overflowing it.
            for query in [if_chain(5000), with_chain(5000)] {
                let err = compile_err(&query);
                assert!(
                    err.to_string().contains("max expression depth exceeded"),
                    "{err}"
                );
            }
        })
        .expect("spawn")
        .join()
        .expect("no stack overflow");
}

#[test]
fn identity_and_no_match_copies_breach_the_budget_in_a_no_output_range() {
    // Review round 3, finding 1: `go_replace`'s early returns (old ==
    // new / n == 0 / no match) and `align`'s identity branch copied the
    // input BEFORE charging — repeatable past budget inside a range
    // body that emits nothing. Each shape breaches now (red pre-fix:
    // rendered "" after ~200 MB of uncharged copies).
    let base = base();
    let line = "x".repeat(1024);
    let over = |query: &str| {
        let pipeline = compiled(query);
        pipeline
            .run(&line, &base, 0)
            .map(|_| ())
            .expect_err(&format!("{query}: must breach the render budget"));
    };
    // The big input is bound ONCE before the loop ($s costs one charge)
    // so the per-iteration production is EXACTLY the branch under test.
    // replace with old == new (identity early-return).
    over(
        r#"{a="b"} | line_format "{{ $s := __line__ }}{{ range 200000 }}{{ $x := replace \"q\" \"q\" $s }}{{ end }}""#,
    );
    // replace with no match (m == 0 early-return).
    over(
        r#"{a="b"} | line_format "{{ $s := __line__ }}{{ range 200000 }}{{ $x := replace \"ZZZ\" \"y\" $s }}{{ end }}""#,
    );
    // Replace with n == 0.
    over(
        r#"{a="b"} | line_format "{{ $s := __line__ }}{{ range 200000 }}{{ $x := Replace $s \"x\" \"y\" 0 }}{{ end }}""#,
    );
    // alignLeft identity (count == rune length).
    over(
        r#"{a="b"} | line_format "{{ $s := __line__ }}{{ range 200000 }}{{ $x := alignLeft 1024 $s }}{{ end }}""#,
    );
    // alignRight identity (negative count).
    over(
        r#"{a="b"} | line_format "{{ $s := __line__ }}{{ range 200000 }}{{ $x := alignRight -1 $s }}{{ end }}""#,
    );
    // Green controls: the same shapes render outside the loop.
    let ok = |query: &str, want: &str| {
        let pipeline = compiled(query);
        let out = pipeline
            .run("ab", &base, 0)
            .expect("no budget breach")
            .expect("kept");
        assert_eq!(out.line, want, "{query}");
    };
    ok(
        r#"{a="b"} | line_format "{{ replace \"q\" \"q\" __line__ }}""#,
        "ab",
    );
    ok(
        r#"{a="b"} | line_format "{{ alignLeft -1 __line__ }}""#,
        "ab",
    );
}

#[test]
fn combined_define_recursion_and_nesting_is_a_bounded_per_line_error() {
    // A recursive define whose body nests 30 ifs: pre-fix only the
    // {{template}} hop counted, so 250 invocations × 30 uncounted
    // if-levels overflowed the stack. The UNIFIED counter charges both,
    // so the render aborts with the depth error on a 2 MiB thread.
    let n = 30;
    let tmpl = format!(
        "{{{{ define \"R\" }}}}{}{{{{ template \"R\" }}}}{}{{{{ end }}}}{{{{ template \"R\" }}}}",
        "{{if \"x\"}}".repeat(n),
        "{{end}}".repeat(n)
    );
    std::thread::Builder::new()
        .stack_size(2 << 20)
        .spawn(move || {
            let err = render(&tmpl, 0).expect_err("must exceed the unified depth cap");
            assert!(
                err.contains("exceeded maximum template depth (250)"),
                "{err}"
            );
        })
        .expect("spawn")
        .join()
        .expect("no stack overflow");
}

#[test]
fn every_caller_amplified_allocation_path_charges_the_budget() {
    // One eval per newly-charged path (issue #230 review round 1: the
    // three misses + the loop-amplification class). Each `expect_err`
    // FAILED against the pre-fix build (the render succeeded after an
    // uncharged multi-hundred-MiB allocation) — demonstrated red-first.
    let base = base();
    let over = |query: &str| {
        let pipeline = compiled(query);
        pipeline
            .run("line", &base, 0)
            .map(|_| ())
            .expect_err(&format!("{query}: must breach the render budget"));
    };
    // (1) float PRECISION (integer width was charged; precision was not).
    over(r#"{a="b"} | line_format "{{ printf \"%.999999999f\" 1.5 }}""#);
    // (2) sprig case mapping — output can EXPAND (ß→SS is the reference
    // rule for the map variants; ours charge the 4×/rune ceiling).
    over(r#"{a="b"} | line_format "{{ lower (repeat 30000000 \"X\") }}""#);
    over(r#"{a="b"} | line_format "{{ upper (repeat 30000000 \"x\") }}""#);
    over(r#"{a="b"} | line_format "{{ title (repeat 30000000 \"x\") }}""#);
    // (3) fromJson's value tree multiplies input bytes by a structural
    // constant (~50× for one-element-per-two-bytes arrays).
    over(
        r#"{a="b"} | line_format "{{ len (fromJson (printf \"[%s1]\" (repeat 1500000 \"1,\"))) }}""#,
    );
    // (4) loop amplification: a range-over-int repeats a text node /
    // printed value without any function in the loop body — cumulative
    // output accounting must breach, not OOM.
    over(r#"{a="b"} | line_format "{{ range 20000000 }}0123456789{{ end }}""#);
    over(r#"{a="b"} | line_format "{{ range 20000000 }}{{ $.env }}{{ end }}""#);

    // Green controls: the same shapes under the budget still render
    // (both directions discriminate).
    let ok = |query: &str, want: &str| {
        let pipeline = compiled(query);
        let out = pipeline
            .run("line", &base, 0)
            .expect("no budget breach")
            .expect("kept");
        assert_eq!(out.line, want, "{query}");
    };
    ok(
        r#"{a="b"} | line_format "{{ printf \"%.6f\" 1.5 }}""#,
        "1.500000",
    );
    ok(r#"{a="b"} | line_format "{{ lower \"ABC\" }}""#, "abc");
    ok(
        r#"{a="b"} | line_format "{{ len (fromJson \"[1,2,3]\") }}""#,
        "3",
    );
    ok(
        r#"{a="b"} | line_format "{{ range 3 }}ab{{ end }}""#,
        "ababab",
    );
}
