//! Read-path JSON float decode must be correctly rounded (issue #270).
//!
//! `serde_json`'s DEFAULT number parser is not correctly rounded: without the
//! `float_roundtrip` feature it can return a neighbouring `f64` rather than
//! the nearest representable one — one ULP away on every literal measured for
//! this issue, which is a property of those vectors rather than a proven bound
//! on the parser. The workspace enables that feature (root
//! `Cargo.toml`) and `crates/pulsus-write/tests/otlp_json_float_roundtrip.rs`
//! gates the ingest side of it. This suite gates the READ side, which decodes
//! log-line JSON through the same parser and was previously ungated — the
//! ingest defect survived for exactly that reason, so a future change must not
//! be able to regress this silently.
//!
//! The read path has three `serde_json` decode sites (`git grep -nE
//! 'serde_json::(from_str|from_slice|from_reader|from_value)|Deserializer::from_'
//! -- crates/pulsus-read/src`):
//!
//! * `| json` — `run_json`, `crates/pulsus-read/src/logql/pipeline.rs:3576`,
//!   both of its arms (full flatten and targeted extraction);
//! * `| unpack` — `run_unpack`, `pipeline.rs:4043`. It promotes only STRING
//!   fields, so no float is ever decoded into an observable value; pinned
//!   below so that stays true rather than being assumed;
//! * `fromJson` — the template function,
//!   `crates/pulsus-read/src/logql/template/funcs.rs:1636`.
//!
//! # Vectors
//!
//! Same table as the write-side suite (duplicated because test-only constants
//! do not cross a crate boundary). Every literal is one on which the two
//! parsers GENUINELY disagree — a fixture of round numbers decodes identically
//! under both and gates nothing. `correct` is `str::parse::<f64>()`'s result,
//! re-derived at run time by [`assert_vectors_are_self_consistent`]; `naive`
//! is what `serde_json` 1.0.150 returns without the feature, recorded so that
//! "these vectors discriminate" is a checked property and so a reader can
//! reproduce the defect by dropping the feature.
//!
//! # Why the assertions parse the rendered text back
//!
//! Neither read-path site hands out an `f64`: `| json` writes a label STRING
//! and `fromJson` feeds a template that renders one. Both renderings are
//! shortest-round-trip, checked in the sources rather than assumed:
//!
//! * `| json` → `json_scalar_to_string` → `serde_json::Number`'s `Display` →
//!   `zmij::Buffer::format_finite` (`serde_json-1.0.150/src/number.rs:356`,
//!   the version this workspace locks), documented as "the shortest correctly
//!   rounded decimal representation" (`zmij-1.0.21/src/lib.rs:1007`);
//! * the template → `dispatch_float`, whose default verb maps to `('g', -1)`
//!   (`template/gofmt.rs:439`) and so reaches the `strconv.FormatFloat` port
//!   with `prec < 0` = shortest (`gofmt.rs:1635`).
//!
//! Shortest-round-trip rendering is by definition exact — `str::parse(render(x))
//! == x` for every finite `f64` — so parsing the output back with the
//! correctly-rounded std parser recovers the decoded bits with nothing lost.
//! The comparison is then on `to_bits()`, never on the text, since one float's
//! rendering is frequently a prefix of another's.
//!
//! What this suite does NOT pin is the SHAPE of that text. Loki returns the
//! wire lexeme unparsed (`readValue`, `pkg/logql/log/parser.go:258-259 @
//! v3.7.4`: `case jsonparser.Number: return string(v)`) where we parse and
//! re-render, so `1.50` comes back as `1.5`. That is a separate divergence,
//! flagged on issue #270 and not addressed here; #270's fix strictly reduces
//! it (17-digit lexemes now round-trip exactly) without closing it.

use std::borrow::Cow;

use pulsus_read::logql::CompiledPipeline;
use pulsus_read::logql::template::{self, Template, TemplateEnv, TemplateKind};

/// A decimal literal on which the correctly-rounded parser and `serde_json`'s
/// default (non-`float_roundtrip`) parser return different `f64`s.
#[derive(Debug, Clone, Copy)]
struct Vector {
    /// The literal exactly as it appears in the JSON line.
    lex: &'static str,
    /// Bits of the nearest representable `f64` (`str::parse::<f64>()`).
    correct: u64,
    /// Bits `serde_json` 1.0.150 returns without `float_roundtrip`.
    naive: u64,
}

const VECTORS: &[Vector] = &[
    Vector {
        lex: "0.0018322491389592419",
        correct: 0x3f5e_0502_8851_2b04,
        naive: 0x3f5e_0502_8851_2b05,
    },
    Vector {
        lex: "0.0011928087610940433",
        correct: 0x3f53_8b00_a7a2_4d96,
        naive: 0x3f53_8b00_a7a2_4d95,
    },
    Vector {
        lex: "1.2120550590194719",
        correct: 0x3ff3_6493_d877_0a2a,
        naive: 0x3ff3_6493_d877_0a2b,
    },
    Vector {
        lex: "1.9816883557688978",
        correct: 0x3fff_b4fe_d96e_434b,
        naive: 0x3fff_b4fe_d96e_434a,
    },
    Vector {
        lex: "-1774.1730603736187",
        correct: 0xc09b_b8b1_36bd_13b4,
        naive: 0xc09b_b8b1_36bd_13b5,
    },
    Vector {
        lex: "-1359.8582046894405",
        correct: 0xc095_3f6e_cd35_c9af,
        naive: 0xc095_3f6e_cd35_c9ae,
    },
    Vector {
        lex: "1040930.8800823967",
        correct: 0x412f_c445_c29a_28ef,
        naive: 0x412f_c445_c29a_28f0,
    },
    Vector {
        lex: "1798120.4400873021",
        correct: 0x413b_6fe8_70a9_8fba,
        naive: 0x413b_6fe8_70a9_8fb9,
    },
    Vector {
        lex: "1066074736.6241531",
        correct: 0x41cf_c581_384f_e440,
        naive: 0x41cf_c581_384f_e441,
    },
    Vector {
        lex: "1883265621.1407897",
        correct: 0x41dc_1016_9549_02b3,
        naive: 0x41dc_1016_9549_02b2,
    },
    // Largest subnormal vs smallest normal: the default parser returns
    // 0x0010_0000_0000_0000, one ULP up and across the boundary.
    Vector {
        lex: "2.2250738585072011e-308",
        correct: 0x000f_ffff_ffff_ffff,
        naive: 0x0010_0000_0000_0000,
    },
];

/// The table is only worth anything if each row's `correct` really is the
/// nearest-representable value and really differs from `naive`. Both are
/// checked here rather than trusted, so a mistyped constant fails loudly
/// instead of turning a discriminating vector into a vacuous one.
#[test]
fn assert_vectors_are_self_consistent() {
    assert!(!VECTORS.is_empty());
    for v in VECTORS {
        let parsed: f64 = v.lex.parse().expect("vector literal parses as f64");
        assert_eq!(
            parsed.to_bits(),
            v.correct,
            "vector {}: `correct` is not str::parse's result",
            v.lex
        );
        assert_ne!(
            v.correct, v.naive,
            "vector {} does not discriminate the two parsers, so it gates nothing",
            v.lex
        );
        assert_eq!(
            v.correct.abs_diff(v.naive),
            1,
            "vector {}: the recorded default-parser result should be exactly 1 ULP away",
            v.lex
        );
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Field name carrying vector `i` in the fixture lines below.
fn field(i: usize) -> String {
    format!("v{i}")
}

/// A flat JSON object with one field per vector, each value written as the
/// bare JSON number a client would have serialised.
fn flat_line() -> String {
    let fields: Vec<String> = VECTORS
        .iter()
        .enumerate()
        .map(|(i, v)| format!(r#""{}":{}"#, field(i), v.lex))
        .collect();
    format!("{{{}}}", fields.join(","))
}

/// Recovers the `f64` that produced a rendered decimal. Exact: both read-path
/// renderings are shortest-round-trip, so `str::parse` (correctly rounded)
/// returns the very bits that were rendered.
fn bits_of(rendered: &str) -> u64 {
    rendered
        .parse::<f64>()
        .unwrap_or_else(|e| panic!("read path rendered {rendered:?}, which is not an f64: {e}"))
        .to_bits()
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

/// Runs one line through a pipeline and returns its final label set.
fn run_labels(query: &str, line: &str) -> Vec<(String, String)> {
    let base = vec![("app".to_string(), "checkout".to_string())];
    let pipeline = compiled(query);
    let out = pipeline
        .run(line, &base, 0)
        .expect("no template budget breach")
        .expect("line kept");
    out.labels
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Runs one line through a pipeline and returns its final line.
fn run_line(query: &str, line: &str) -> String {
    let base = vec![("app".to_string(), "checkout".to_string())];
    compiled(query)
        .run(line, &base, 0)
        .expect("no template budget breach")
        .expect("line kept")
        .line
        .into_owned()
}

fn label<'a>(labels: &'a [(String, String)], name: &str) -> &'a str {
    labels
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
        .unwrap_or_else(|| panic!("label {name} not extracted; got {labels:?}"))
}

// ---------------------------------------------------------------------------
// `| json` — pipeline.rs:3576
// ---------------------------------------------------------------------------

/// The full-flatten arm: every bare number in the line reaches
/// `json_scalar_to_string` through `flatten_json`.
#[test]
fn json_full_flatten_preserves_the_exact_bits_of_every_vector() {
    let labels = run_labels(r#"{app="checkout"} | json"#, &flat_line());
    for (i, v) in VECTORS.iter().enumerate() {
        let got = label(&labels, &field(i));
        assert_eq!(
            bits_of(got),
            v.correct,
            "| json decoded {} to {got} ({:#018x}), want {:#018x}",
            v.lex,
            bits_of(got),
            v.correct
        );
        assert_ne!(
            bits_of(got),
            v.naive,
            "| json decoded {} with the non-round-trip parser",
            v.lex
        );
    }
}

/// Nested objects take a different path through `flatten_json` (the key
/// budget and the prefix join) before reaching the same scalar rendering.
#[test]
fn json_nested_object_preserves_the_exact_bits_of_every_vector() {
    let fields: Vec<String> = VECTORS
        .iter()
        .enumerate()
        .map(|(i, v)| format!(r#""outer":{{"{}":{}}}"#, field(i), v.lex))
        .collect();
    for (i, (obj, v)) in fields.iter().zip(VECTORS).enumerate() {
        let labels = run_labels(r#"{app="checkout"} | json"#, &format!("{{{obj}}}"));
        let got = label(&labels, &format!("outer_{}", field(i)));
        assert_eq!(
            bits_of(got),
            v.correct,
            "nested | json decoded {} to {got}",
            v.lex
        );
    }
}

/// The targeted-extraction arm: `lookup_json_path` + `json_scalar_to_string`,
/// which never calls `flatten_json` at all.
#[test]
fn json_targeted_extraction_preserves_the_exact_bits_of_every_vector() {
    let exprs: Vec<String> = VECTORS
        .iter()
        .enumerate()
        .map(|(i, _)| format!(r#"got{i}="{}""#, field(i)))
        .collect();
    let query = format!(r#"{{app="checkout"}} | json {}"#, exprs.join(","));
    let labels = run_labels(&query, &flat_line());
    for (i, v) in VECTORS.iter().enumerate() {
        let got = label(&labels, &format!("got{i}"));
        assert_eq!(
            bits_of(got),
            v.correct,
            "| json {}=\"{}\" decoded {} to {got}",
            format_args!("got{i}"),
            field(i),
            v.lex
        );
    }
}

/// An array element reached by index — the one number-bearing shape the
/// flatten arm skips, so only the targeted path can observe it.
#[test]
fn json_array_index_extraction_preserves_the_exact_bits_of_every_vector() {
    let items: Vec<&str> = VECTORS.iter().map(|v| v.lex).collect();
    let line = format!(r#"{{"arr":[{}]}}"#, items.join(","));
    let exprs: Vec<String> = (0..VECTORS.len())
        .map(|i| format!(r#"got{i}="arr[{i}]""#))
        .collect();
    let query = format!(r#"{{app="checkout"}} | json {}"#, exprs.join(","));
    let labels = run_labels(&query, &line);
    for (i, v) in VECTORS.iter().enumerate() {
        let got = label(&labels, &format!("got{i}"));
        assert_eq!(
            bits_of(got),
            v.correct,
            "arr[{i}] decoded {} to {got}",
            v.lex
        );
    }
}

// ---------------------------------------------------------------------------
// `| unpack` — pipeline.rs:4043
// ---------------------------------------------------------------------------

/// `unpack` decodes through the same parser but promotes only STRING fields,
/// so no decoded float is observable. Pinned rather than assumed: if that ever
/// changes, the new leaf needs the treatment the two above get.
#[test]
fn unpack_never_exposes_a_decoded_float() {
    let v = VECTORS[0];
    let line = format!(r#"{{"_entry":"hello","num":{},"str":"{}"}}"#, v.lex, v.lex);
    let labels = run_labels(r#"{app="checkout"} | unpack"#, &line);
    assert!(
        !labels.iter().any(|(k, _)| k == "num"),
        "unpack promoted a numeric field, which would put it on the float path: {labels:?}"
    );
    // The string-valued twin passes through byte-for-byte — no parse, so no
    // rounding to get wrong.
    assert_eq!(label(&labels, "str"), v.lex);
    assert_eq!(run_line(r#"{app="checkout"} | unpack"#, &line), "hello");
}

// ---------------------------------------------------------------------------
// `fromJson` — template/funcs.rs:1636
// ---------------------------------------------------------------------------

/// Renders one `line_format` body through the FULL engine surface.
fn render(tmpl: &str) -> String {
    let compiled = template::compile(tmpl, TemplateKind::Line).expect("template compiles");
    let labels: Vec<(Cow<'_, str>, Cow<'_, str>)> =
        vec![(Cow::Borrowed("app"), Cow::Borrowed("checkout"))];
    let Template::Full(prog) = compiled else {
        panic!("expected a full template program for {tmpl}");
    };
    let budget = template::RenderBudget::default();
    template::render_full(
        &prog,
        &labels,
        None,
        None,
        "the line",
        0,
        &TemplateEnv::default(),
        &budget,
    )
    .unwrap_or_else(|e| panic!("render failed for {tmpl}: {}", e.msg))
    .as_str()
    .to_string()
}

/// `fromJson` over a literal argument: `json_to_value` turns every number into
/// a `Value::Float`, which the printer renders shortest-round-trip.
#[test]
fn from_json_preserves_the_exact_bits_of_every_vector() {
    for v in VECTORS {
        let tmpl = format!(r#"{{{{ (fromJson "{{\"v\":{}}}").v }}}}"#, v.lex);
        let got = render(&tmpl);
        assert_eq!(
            bits_of(&got),
            v.correct,
            "fromJson decoded {} to {got} ({:#018x}), want {:#018x}",
            v.lex,
            bits_of(&got),
            v.correct
        );
        assert_ne!(
            bits_of(&got),
            v.naive,
            "fromJson decoded {} with the non-round-trip parser",
            v.lex
        );
    }
}

/// The same decode reached the way a query actually reaches it: the log line
/// itself, through `| line_format` and `__line__`.
#[test]
fn from_json_over_the_log_line_preserves_the_exact_bits_of_every_vector() {
    for (i, v) in VECTORS.iter().enumerate() {
        let query = format!(
            "{{app=\"checkout\"}} | line_format `{{{{ (fromJson __line__).{} }}}}`",
            field(i)
        );
        let got = run_line(&query, &flat_line());
        assert_eq!(
            bits_of(&got),
            v.correct,
            "fromJson __line__ decoded {} to {got}",
            v.lex
        );
    }
}

/// Nested inside an array, reached with `index` — a different arm of
/// `json_to_value` (the `List` branch) feeding the same printer.
#[test]
fn from_json_array_element_preserves_the_exact_bits_of_every_vector() {
    for (i, v) in VECTORS.iter().enumerate() {
        let items: Vec<&str> = VECTORS.iter().map(|x| x.lex).collect();
        let tmpl = format!(r#"{{{{ index (fromJson "[{}]") {i} }}}}"#, items.join(","));
        let got = render(&tmpl);
        assert_eq!(
            bits_of(&got),
            v.correct,
            "fromJson array element {i} decoded {} to {got}",
            v.lex
        );
    }
}
