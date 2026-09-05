//! Issue #392 — which runes may form a LogQL identifier.
//!
//! **The gap this closes.** PulsusDB's lexer accepted
//! `[A-Za-z_][A-Za-z0-9_]*` and nothing else, so `| logfmt éx="b"`,
//! `| json éx="b"`, `| label_format éx="y"`, `| drop éx` and
//! `sum by (éx)` — every one of them served by the reference — were a
//! 400 here. A user migrating a working query found it broken.
//!
//! **The rule, from the reference's source rather than inferred from
//! `é` working.** grafana/loki v3.7.4 builds its LogQL scanner on a
//! vendored Go `text/scanner` and never assigns `IsIdentRune`
//! (`pkg/logql/syntax/query_scanner.go:157` declares it, `:339-340` is
//! its only use, and `git grep IsIdentRune pkg/` @ v3.7.4 finds nothing
//! else), so the default predicate at `:338-343` is the rule verbatim:
//! `ch == '_' || unicode.IsLetter(ch) || unicode.IsDigit(ch) && i > 0`,
//! with the leading rune taken through it at `i == 0` (`:675`).
//!
//! That is **strictly narrower than "allow non-ASCII"**, in three ways
//! this file discriminates: combining marks are refused, `Nl` and `No`
//! are refused, and a decimal digit may not lead.
//!
//! **Every verdict below was measured**, black box, by HTTP status on
//! `/loki/api/v1/query_range` against the digest-pinned v3.7.4 container
//! (`.github/workflows/ci.yml`; `/loki/api/v1/status/buildinfo` reported
//! `{"version":"3.7.4","revision":"b318f282"}`), and
//! [`the_identifier_charset_agrees_with_the_reference`] replays them so
//! they cannot rot.
//!
//! **Where we deliberately differ.** Two grammar positions carry the
//! identifier fine at the reference's lexer and then fail below it, for
//! reasons that are the reference's own defects — see
//! [`GRAMMAR_POSITIONS`]. By the ruling on #392 we serve both; the
//! divergences are censused in `tests/case_folding.rs` and written up in
//! `docs/benchmarks/logs-differential-ledger.md`.

use pulsus_logql::parse;

/// The Unicode 15.0.0 baseline the reference's Go runtime carries — see
/// the file's own header for why it is committed and how it was made.
/// Used only by [`the_unicode_version_skew_is_one_directional`] and
/// [`the_measured_unicode_16_additions_are_the_numbers_the_docs_quote`].
const GO_15_0_0_CATEGORIES: &str = include_str!("unicode15/go-1.25.5-general-categories.txt");

/// One category's maximal ranges out of [`GO_15_0_0_CATEGORIES`].
///
/// Panics rather than returning empty on a malformed or missing
/// category: an empty baseline would make the skew tripwire pass
/// vacuously, which is the one failure this data exists to prevent. The
/// range counts are asserted for the same reason.
fn go_15_0_0(category: &str) -> Vec<(char, char)> {
    let ranges: Vec<(char, char)> = GO_15_0_0_CATEGORIES
        .lines()
        .filter(|l| !l.starts_with('#') && !l.trim().is_empty())
        .filter_map(|l| {
            let mut f = l.split_whitespace();
            let cat = f.next()?;
            if cat != category {
                return None;
            }
            let lo = u32::from_str_radix(f.next().expect("a range needs a start"), 16)
                .expect("hex start");
            let hi =
                u32::from_str_radix(f.next().expect("a range needs an end"), 16).expect("hex end");
            Some((
                char::from_u32(lo).expect("start is a scalar value"),
                char::from_u32(hi).expect("end is a scalar value"),
            ))
        })
        .collect();
    let expected = match category {
        "L" => 659,
        "Nd" => 64,
        other => panic!("no such category in the baseline: {other}"),
    };
    assert_eq!(
        ranges.len(),
        expected,
        "the Go 1.25.5 baseline for {category} has {} ranges, not {expected} — the data          file is truncated or the parser is dropping lines, and a short baseline makes          the version-skew tripwire pass vacuously",
        ranges.len()
    );
    ranges
}

/// The committed tables themselves, included by path rather than
/// exported: `src/unicode_ident_tables.rs` is `pub(crate)` in the
/// library and #392 must not widen that crate's public API to let a test
/// read it. Compiling the file a second time here also proves it is
/// valid Rust exactly as generated.
#[path = "../src/unicode_ident_tables.rs"]
mod committed_tables;

fn accepts(query: &str) -> bool {
    parse(query).is_ok()
}

/// A syntax verdict as the reference reports it: 2xx or exactly 400.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verdict {
    Accept,
    Reject,
}

/// Whether the live leg may put a row to the shared oracle.
#[derive(Clone, Copy, Debug)]
enum LiveProbe {
    Replay,
    /// Recorded with its reason and COUNTED, so dropping a row silently
    /// changes a pinned total rather than shrinking the leg unnoticed.
    Skip(&'static str),
}

/// The selector every probe in this file uses.
///
/// It is unique to #392 on purpose. The oracle container is SHARED by
/// every reference-facing CI step, and
/// `crates/pulsus-read/tests/logql_regex_accept_matrix.rs:3347` pushes
/// `{"app":"x","job":"pulsus_it246"}` into it. A leg probing
/// `{app="x"} | logfmt <non-ascii>="…"` therefore returns **500**, not
/// 200, purely by CI step order — inconclusiveness that looks exactly
/// like a finding. [`the_identifier_charset_agrees_with_the_reference`]
/// asserts up front that this selector matches nothing, so a future leg
/// that pushes to it fails loudly instead of silently turning every
/// accept into a 500.
const SELECTOR: &str = r#"{app="pulsus_lex392"}"#;

// ---------------------------------------------------------------------
// 1. Every grammar position that carries IDENTIFIER.
// ---------------------------------------------------------------------

/// What the reference does with a non-ASCII identifier at a position —
/// three states, because two positions are neither an accept nor a
/// syntax rejection.
#[derive(Clone, Copy, Debug)]
enum Reference {
    /// 2xx.
    Serves,
    /// 400, decided BELOW the lexer. The reference tokenises the
    /// identifier here too; the string names what actually refuses it.
    RefusesBelowTheLexer(&'static str),
    /// No HTTP status at all.
    NoAnswer(&'static str),
}

/// Every production in `pkg/logql/syntax/syntax.y` @ v3.7.4 that carries
/// the `IDENTIFIER` token — enumerated from the REFERENCE's grammar, not
/// from our source, because a list read off our lexer cannot show a
/// position we never implemented. `git show v3.7.4:pkg/logql/syntax/syntax.y
/// | grep -n IDENTIFIER` yields exactly the lines named below (`:79` is
/// the `%token` declaration itself).
const GRAMMAR_POSITIONS: &[(&str, &str, &str, Reference, LiveProbe)] = &[
    // (id, syntax.y production, query template, reference, live)
    (
        "unwrap",
        "unwrapExpr :158",
        r#"sum_over_time(SEL | unwrap éx [5m])"#,
        Reference::Serves,
        LiveProbe::Replay,
    ),
    (
        "unwrap_conversion",
        "unwrapExpr :159",
        r#"sum_over_time(SEL | unwrap duration(éx) [5m])"#,
        Reference::Serves,
        LiveProbe::Replay,
    ),
    (
        "label_format_rename",
        "labelFormatExpr :289",
        r#"SEL | label_format éx=ax"#,
        Reference::Serves,
        LiveProbe::Replay,
    ),
    (
        "label_format_template",
        "labelFormatExpr :290",
        r#"SEL | label_format éx="tmpl""#,
        Reference::Serves,
        LiveProbe::Replay,
    ),
    (
        "logfmt_extraction",
        "labelExtractionExpression :315",
        r#"SEL | logfmt éx="p""#,
        Reference::Serves,
        LiveProbe::Replay,
    ),
    (
        "json_extraction",
        "labelExtractionExpression :315",
        r#"SEL | json éx="p""#,
        Reference::Serves,
        LiveProbe::Replay,
    ),
    (
        "ip_label_filter",
        "ipLabelFilter :324",
        r#"SEL | logfmt | éx = ip("1.2.3.4")"#,
        Reference::Serves,
        LiveProbe::Replay,
    ),
    (
        "duration_filter",
        "durationFilter :333",
        r#"SEL | logfmt | éx > 1s"#,
        Reference::Serves,
        LiveProbe::Replay,
    ),
    (
        "bytes_filter",
        "bytesFilter :343",
        r#"SEL | logfmt | éx > 1KB"#,
        Reference::Serves,
        LiveProbe::Replay,
    ),
    (
        "number_filter",
        "numberFilter :353",
        r#"SEL | logfmt | éx > 1"#,
        Reference::Serves,
        LiveProbe::Replay,
    ),
    (
        "drop",
        "namedMatcher :363",
        r#"SEL | drop éx"#,
        Reference::Serves,
        LiveProbe::Replay,
    ),
    (
        "keep",
        "namedMatcher :363",
        r#"SEL | keep éx"#,
        Reference::Serves,
        LiveProbe::Replay,
    ),
    (
        "grouping_by",
        "labels :514 via grouping :519",
        r#"sum by (éx) (count_over_time(SEL[5m]))"#,
        Reference::Serves,
        LiveProbe::Replay,
    ),
    (
        "grouping_without",
        "labels :514 via grouping :519",
        r#"sum without (éx) (count_over_time(SEL[5m]))"#,
        Reference::Serves,
        LiveProbe::Replay,
    ),
    (
        "vector_matching_on",
        "labels :514 via onOrIgnoringModifier :404",
        r#"sum by (a) (count_over_time(SEL[5m])) + on (éx) sum by (a) (count_over_time(SEL[5m]))"#,
        Reference::Serves,
        LiveProbe::Replay,
    ),
    (
        "vector_matching_ignoring",
        "labels :514 via onOrIgnoringModifier :404",
        r#"sum by (a) (count_over_time(SEL[5m])) + ignoring (éx) sum by (a) (count_over_time(SEL[5m]))"#,
        Reference::Serves,
        LiveProbe::Replay,
    ),
    // The two positions the reference does not serve. Both share the
    // `matcher` production at :204-207, and NEITHER refusal comes from
    // its lexer — see the module doc and the ledger.
    (
        "stream_selector",
        "matcher :204",
        r#"{éx="m"}"#,
        Reference::RefusesBelowTheLexer(
            "the query-frontend re-serialises the parsed AST and vendored Prometheus \
             labels.Matcher.String() quotes any name outside [A-Za-z_][A-Za-z0-9_]* \
             (vendor/github.com/prometheus/prometheus/model/labels/matcher.go:81-104, \
             shouldQuoteName at :97-104), producing a form LogQL's own grammar has no \
             production for. PROOF that this is a round trip and not the lexer: \
             {\"éx\"=\"m\"} returns the BYTE-IDENTICAL error at the IDENTICAL column \
             (`parse error at line 1, col 2: syntax error: unexpected STRING, expecting \
             IDENTIFIER or }`). We serve it — ruling on #392.",
        ),
        LiveProbe::Replay,
    ),
    (
        "string_label_filter",
        "matcher :204 reused by labelFilter :153",
        r#"SEL | logfmt | éx="v""#,
        Reference::NoAnswer(
            "the same re-serialisation, but here the rewritten query goes downstream and \
             500s, and the frontend retries it. Measured on the pinned container: the \
             first probe returned 500 after 28.1 s; four further probes returned NO HTTP \
             status at all (curl exit 52, `Empty reply from server`) after 37.4, 37.5, \
             39.0 and 37.4 s. The container log shows \
             `query=\"{app=...} | \\\"éx\\\"=\\\"b\\\"\" ... code=Code(500)` on try=0..4 \
             then `(500) 37.39s Response: \"failed to enqueue request\"`. NEVER PROBE \
             THIS LIVE: it burns ~40 s and floods the shared oracle's scheduler with \
             retries, which perturbs the legs that run after it.",
        ),
        LiveProbe::Skip("no HTTP status in ~40 s — measured five times, see the reason above"),
    ),
];

/// **The issue's claim, put to every position the reference's grammar
/// has.** Fails on `ff0fb09`: all eighteen reject in the lexer.
#[test]
fn every_reference_grammar_identifier_position_accepts_a_non_ascii_letter() {
    let mut refused = Vec::new();
    for (id, production, template, _, _) in GRAMMAR_POSITIONS {
        let query = template.replace("SEL", SELECTOR);
        if let Err(e) = parse(&query) {
            refused.push(format!("{id} ({production}): {query:?} — {e}"));
        }
    }
    assert!(
        refused.is_empty(),
        "a non-ASCII identifier is refused at {} of {} grammar positions:\n{}",
        refused.len(),
        GRAMMAR_POSITIONS.len(),
        refused.join("\n")
    );
}

/// The enumeration is pinned so a position cannot be dropped silently,
/// and the two non-serving positions are pinned SEPARATELY — they are
/// the whole reason this issue needed a ruling.
#[test]
fn the_position_enumeration_and_its_two_reference_defects_are_pinned() {
    assert_eq!(
        GRAMMAR_POSITIONS.len(),
        18,
        "the position enumeration changed size — re-derive it from \
         `git show v3.7.4:pkg/logql/syntax/syntax.y | grep -n IDENTIFIER`, not from our lexer"
    );
    let not_served = GRAMMAR_POSITIONS
        .iter()
        .filter(|(_, _, _, r, _)| !matches!(r, Reference::Serves))
        .map(|(id, _, _, _, _)| *id)
        .collect::<Vec<_>>();
    assert_eq!(
        not_served,
        vec!["stream_selector", "string_label_filter"],
        "the set of positions the reference does not serve changed — a new one needs a \
         ledger entry and a census row, not a silent addition"
    );
}

/// Every carve-out states its mechanism, and the statement is CHECKED
/// rather than merely written: a `Reference` that is not `Serves` and a
/// `LiveProbe::Skip` are the two places this file stops measuring, and
/// "the defect lives in the exemption". An empty or placeholder reason
/// would make the exemption unreviewable.
#[test]
fn every_exemption_states_the_mechanism_that_earns_it() {
    for (id, _, _, reference, live) in GRAMMAR_POSITIONS {
        let reason = match reference {
            Reference::Serves => None,
            Reference::RefusesBelowTheLexer(why) | Reference::NoAnswer(why) => Some(*why),
        };
        if let Some(why) = reason {
            assert!(
                why.len() > 120,
                "position/{id}: a reference verdict that is NOT a plain accept must name \
                 the mechanism and where it was measured; got {why:?}"
            );
        }
        if let LiveProbe::Skip(why) = live {
            assert!(
                why.len() > 20,
                "position/{id}: a row withheld from the live leg must say why; got {why:?}"
            );
        }
    }
    for (id, _, _, live) in BOUNDARY {
        if let LiveProbe::Skip(why) = live {
            assert!(
                why.len() > 20,
                "boundary/{id}: a row withheld from the live leg must say why; got {why:?}"
            );
        }
    }
}

// ---------------------------------------------------------------------
// 2. The rune-set boundary.
// ---------------------------------------------------------------------

/// The measured boundary, all at `| drop X` so the verdict depends on
/// one thing only. Every row's [`Verdict`] is the HTTP status the pinned
/// container returned; PulsusDB's verdict is DERIVED by running the
/// parser, so this table fails the moment the two disagree.
///
/// Rows are `(id, query, reference, live)`. Every non-ASCII rune is
/// written as an explicit `\u{...}` escape rather than pasted, so a
/// reader can check which code point is meant without trusting a font.
const BOUNDARY: &[(&str, &str, Verdict, LiveProbe)] = &[
    // --- ASCII, unchanged by this issue but the control group ---
    ("ascii_letter", "x", Verdict::Accept, LiveProbe::Replay),
    (
        "ascii_underscore_leads",
        "_x",
        Verdict::Accept,
        LiveProbe::Replay,
    ),
    (
        "ascii_underscore_alone",
        "_",
        Verdict::Accept,
        LiveProbe::Replay,
    ),
    (
        "ascii_digit_continues",
        "x3",
        Verdict::Accept,
        LiveProbe::Replay,
    ),
    (
        "ascii_digit_may_not_lead",
        "3x",
        Verdict::Reject,
        LiveProbe::Replay,
    ),
    // --- General category L, in several scripts and subcategories ---
    // Ll
    (
        "latin1_letter_leads",
        "\u{e9}x",
        Verdict::Accept,
        LiveProbe::Replay,
    ),
    (
        "latin1_letter_alone",
        "\u{e9}",
        Verdict::Accept,
        LiveProbe::Replay,
    ),
    (
        "latin1_letter_then_digit",
        "\u{e9}x3",
        Verdict::Accept,
        LiveProbe::Replay,
    ),
    ("greek_ll", "\u{3bb}x", Verdict::Accept, LiveProbe::Replay),
    (
        "cyrillic_ll",
        "\u{43f}\u{440}\u{438}\u{432}\u{435}\u{442}",
        Verdict::Accept,
        LiveProbe::Replay,
    ),
    // Lo
    (
        "cjk_han_lo",
        "\u{65e5}\u{672c}\u{8a9e}",
        Verdict::Accept,
        LiveProbe::Replay,
    ),
    (
        "hiragana_lo",
        "\u{3072}\u{3089}\u{304c}\u{306a}",
        Verdict::Accept,
        LiveProbe::Replay,
    ),
    (
        "hebrew_lo",
        "\u{5e9}\u{5dc}\u{5d5}\u{5dd}",
        Verdict::Accept,
        LiveProbe::Replay,
    ),
    (
        "arabic_lo",
        "\u{645}\u{631}\u{62d}\u{628}\u{627}",
        Verdict::Accept,
        LiveProbe::Replay,
    ),
    // Lm and Lt are `L` too, so they lead as well as continue.
    (
        "modifier_letter_lm_leads",
        "\u{2b0}x",
        Verdict::Accept,
        LiveProbe::Replay,
    ),
    (
        "modifier_letter_lm_continues",
        "x\u{2b0}",
        Verdict::Accept,
        LiveProbe::Replay,
    ),
    (
        "title_case_lt_leads",
        "\u{1c5}x",
        Verdict::Accept,
        LiveProbe::Replay,
    ),
    // --- General category Nd: continues, never leads ---
    (
        "arabic_indic_digit_continues",
        "x\u{663}",
        Verdict::Accept,
        LiveProbe::Replay,
    ),
    (
        "arabic_indic_digit_may_not_lead",
        "\u{663}x",
        Verdict::Reject,
        LiveProbe::Replay,
    ),
    (
        "devanagari_digit_continues",
        "x\u{969}",
        Verdict::Accept,
        LiveProbe::Replay,
    ),
    // --- NOT identifier runes. Each rules out a plausible wrong fix ---
    // Mc. `char::is_alphabetic('\u{93e}')` is TRUE.
    (
        "devanagari_vowel_sign_mc",
        "\u{915}\u{93e}",
        Verdict::Reject,
        LiveProbe::Replay,
    ),
    // Mn. Also `is_alphabetic`-adjacent: this is the NFD spelling of `é`,
    // so it discriminates "the query text happens to contain é" from
    // "the rune is a letter".
    (
        "combining_acute_mn",
        "e\u{301}x",
        Verdict::Reject,
        LiveProbe::Replay,
    ),
    // Nl. `char::is_alphabetic('\u{2167}')` is TRUE.
    (
        "roman_numeral_nl",
        "x\u{2167}",
        Verdict::Reject,
        LiveProbe::Replay,
    ),
    // No. `char::is_numeric` is TRUE for both.
    (
        "vulgar_fraction_no",
        "x\u{bd}",
        Verdict::Reject,
        LiveProbe::Replay,
    ),
    (
        "superscript_three_no",
        "x\u{b3}",
        Verdict::Reject,
        LiveProbe::Replay,
    ),
    // So / Cf / Co / Cn — the "any non-ASCII byte" fix would take all four.
    ("emoji_so", "x\u{1f642}", Verdict::Reject, LiveProbe::Replay),
    (
        "zero_width_joiner_cf",
        "x\u{200d}y",
        Verdict::Reject,
        LiveProbe::Replay,
    ),
    (
        "private_use_co",
        "x\u{e000}",
        Verdict::Reject,
        LiveProbe::Replay,
    ),
    (
        "unassigned_cn",
        "x\u{378}",
        Verdict::Reject,
        LiveProbe::Replay,
    ),
];

fn boundary_query(rune_form: &str) -> String {
    format!("{SELECTOR} | drop {rune_form}")
}

/// **The table, checked against the parser.** Fails on `ff0fb09`: every
/// non-ASCII accept row rejects there.
#[test]
fn the_identifier_charset_matches_the_references_measured_boundary() {
    let mut drift = Vec::new();
    for (id, rune_form, reference, _) in BOUNDARY {
        let query = boundary_query(rune_form);
        let ours = if accepts(&query) {
            Verdict::Accept
        } else {
            Verdict::Reject
        };
        if ours != *reference {
            drift.push(format!(
                "{id}: reference={reference:?}, ours={ours:?} ({query:?})"
            ));
        }
    }
    assert!(
        drift.is_empty(),
        "the identifier charset no longer matches the reference's measured boundary:\n{}",
        drift.join("\n")
    );
}

/// **The discriminator.** Every one of these is accepted by at least one
/// of the three plausible-but-wrong fixes, and refused by the reference:
///
/// * "allow any non-ASCII rune" takes all six rejects;
/// * `char::is_alphabetic()` takes `का` and `xⅧ` (it is true for U+093E
///   and U+2167 — 147,421 code points against Go's `L` 136,104);
/// * `char::is_alphanumeric()` / `is_numeric()` additionally take `x½`
///   and `x³`.
///
/// The accept half (`x٣`) is what stops the fix from being "refuse all
/// non-ASCII", which would also pass the reject half.
#[test]
fn combining_marks_and_non_decimal_numbers_are_not_identifier_runes() {
    for form in [
        "\u{915}\u{93e}", // Mc
        "e\u{301}x",      // Mn
        "x\u{2167}",      // Nl
        "x\u{bd}",        // No
        "x\u{b3}",        // No
        "x\u{1f642}",     // So
        "x\u{200d}y",     // Cf
    ] {
        let q = boundary_query(form);
        assert!(
            !accepts(&q),
            "{q:?} must be refused: the reference refuses it (400), and accepting it \
             would mean the rule is wider than general category L ∪ Nd"
        );
    }
    let nd = boundary_query("x\u{663}");
    assert!(
        accepts(&nd),
        "{nd:?} must be accepted: U+0663 is general category Nd, which the reference \
         accepts in non-leading position (200)"
    );
}

/// Rules out "L ∪ Nd in every position", which the reject half of the
/// test above would not catch.
#[test]
fn a_decimal_digit_may_not_lead_an_identifier() {
    for lead in ["3x", "\u{663}x"] {
        let q = boundary_query(lead);
        assert!(!accepts(&q), "{q:?} must be refused: a digit may not lead");
    }
    for cont in ["x3", "x\u{663}"] {
        let q = boundary_query(cont);
        assert!(accepts(&q), "{q:?} must be accepted: a digit may continue");
    }
}

/// Widening the accept surface has an obligation: say exactly what
/// becomes accepted, and confirm nothing that should still be refused
/// now parses. The rune classes are covered above; this covers the
/// STRUCTURAL refusals a wider identifier rule could have loosened by
/// accident — operators, punctuation and stray bytes must still be
/// lexer errors, and a keyword must still be a keyword.
#[test]
fn widening_the_identifier_rule_did_not_loosen_anything_structural() {
    for q in [
        // Stray ASCII that is not an identifier rune.
        format!("{SELECTOR} | drop @"),
        format!("{SELECTOR} | drop #"),
        format!("{SELECTOR} | drop $x"),
        format!("{SELECTOR} | drop x!y"),
        format!("{SELECTOR} | drop x.y"),
        // A non-ASCII rune that is not an identifier rune, standing alone.
        format!("{SELECTOR} | drop \u{bd}"),
        format!("{SELECTOR} | drop \u{1f642}"),
        // Non-ASCII where a keyword or an operator belongs.
        format!("{SELECTOR} | logfmt \u{e9}x = \u{e9}"),
        format!("sum b\u{e9} (count_over_time({SELECTOR}[5m]))"),
        // Unterminated string with a non-ASCII body still fails as a
        // string error, not as an identifier.
        format!("{SELECTOR} |= \"\u{e9}"),
    ] {
        assert!(
            !accepts(&q),
            "{q:?} must still be refused — the #392 widening is the identifier RUNE SET \
             only, not a general loosening"
        );
    }
}

// ---------------------------------------------------------------------
// 3. The committed Unicode tables.
// ---------------------------------------------------------------------

/// Re-derives `\p{L}` / `\p{Nd}` from `regex-syntax` and compares them
/// range for range with `src/unicode_ident_tables.rs`.
///
/// Deliberate break, run while this landed: deleting the last range of
/// `LETTER` fails with `LETTER: 676 ranges committed, 677 derived`.
#[test]
fn the_committed_unicode_tables_are_regex_syntax_general_category() {
    for (name, category, committed) in [
        ("LETTER", "L", committed_tables::LETTER),
        ("DECIMAL_NUMBER", "Nd", committed_tables::DECIMAL_NUMBER),
    ] {
        let derived = general_category(category);
        assert_eq!(
            committed.len(),
            derived.len(),
            "{name}: {} ranges committed, {} derived from regex-syntax \\p{{{category}}} — \
             the committed table is stale. Regenerate it with \
             `cargo test -p pulsus-logql --test identifier_charset -- --ignored \
             regenerate_the_unicode_tables`, then re-derive the Unicode version skew \
             (see the header of tests/unicode15/go-1.25.5-general-categories.txt) before trusting the result.",
            committed.len(),
            derived.len(),
        );
        assert_eq!(
            committed, derived,
            "{name}: the committed ranges differ from regex-syntax \\p{{{category}}} — \
             regenerate as above"
        );
    }
}

/// **The tripwire the ruling on #392 requires.**
///
/// Our tables are Unicode **16.0.0** (`regex-syntax` 0.8.11); the
/// reference's Go runtime is Unicode **15.0.0**. That was measured as a
/// strict ONE-DIRECTIONAL superset — 4,924 code points are `L` here and
/// not there, 80 are `Nd` here and not there, and ZERO go the other way
/// — which is the entire justification for not pinning an old table:
/// nothing the reference accepts can be refused here.
///
/// A remembered measurement has no failure mode, so the 15.0.0 baseline
/// is committed (`tests/unicode15/go-1.25.5-general-categories.txt`) and the claim is re-checked
/// on every run. If a future `regex-syntax` ever drops a code point that
/// Unicode 15.0 called a letter or a decimal digit, the skew becomes
/// two-directional, this fails, and the decision has to be retaken.
///
/// Deliberate break, run while this landed: removing `('\u{41}',
/// '\u{5a}')` from `LETTER` fails with 26 refused code points starting
/// at U+0041.
#[test]
fn the_unicode_version_skew_is_one_directional() {
    let mut lost = Vec::new();
    for (name, baseline, ours) in [
        ("L", go_15_0_0("L"), committed_tables::LETTER),
        ("Nd", go_15_0_0("Nd"), committed_tables::DECIMAL_NUMBER),
    ] {
        for (lo, hi) in &baseline {
            for c in (*lo as u32)..=(*hi as u32) {
                let Some(c) = char::from_u32(c) else { continue };
                if !ours.iter().any(|(l, h)| c >= *l && c <= *h) {
                    lost.push(format!("{name}: U+{:04X}", c as u32));
                }
            }
        }
    }
    assert!(
        lost.is_empty(),
        "the Unicode version skew is no longer one-directional: {} code point(s) are in \
         general category L/Nd at the reference (Go 1.25.5, Unicode 15.0.0) and NOT in \
         our tables, so a query the reference serves would be refused here. The #392 \
         ruling rests on this being empty — re-take the decision, do not update the \
         baseline. First few: {:?}",
        lost.len(),
        &lost[..lost.len().min(8)]
    );
}

/// The measured skew in the other direction, pinned as the number the
/// ledger and `docs/api.md` quote. Not a correctness gate — it is what
/// stops the three documents from drifting apart.
#[test]
fn the_measured_unicode_16_additions_are_the_numbers_the_docs_quote() {
    for (name, baseline, ours, expected) in [
        ("L", go_15_0_0("L"), committed_tables::LETTER, 4_924usize),
        ("Nd", go_15_0_0("Nd"), committed_tables::DECIMAL_NUMBER, 80),
    ] {
        let added = ours
            .iter()
            .flat_map(|(lo, hi)| (*lo as u32)..=(*hi as u32))
            .filter_map(char::from_u32)
            .filter(|c| !baseline.iter().any(|(l, h)| c >= l && c <= h))
            .count();
        assert_eq!(
            added, expected,
            "{name}: Unicode 16.0.0 adds {added} code points over 15.0.0, but docs/api.md \
             §10 and docs/benchmarks/logs-differential-ledger.md quote {expected}"
        );
    }
}

/// Derives one general category's ranges from `regex-syntax` — the
/// single place this crate's tests read Unicode data from, so the
/// generator and the checker cannot use different sources.
fn general_category(name: &str) -> Vec<(char, char)> {
    use regex_syntax::hir::{Class, HirKind};
    let hir = regex_syntax::parse(&format!(r"\p{{{name}}}"))
        .unwrap_or_else(|e| panic!("regex-syntax must know \\p{{{name}}}: {e}"));
    match hir.into_kind() {
        HirKind::Class(Class::Unicode(class)) => class
            .ranges()
            .iter()
            .map(|r| (r.start(), r.end()))
            .collect(),
        other => panic!("\\p{{{name}}} is not a unicode class: {other:?}"),
    }
}

/// Rewrites `src/unicode_ident_tables.rs`. Run only on purpose:
/// `cargo test -p pulsus-logql --test identifier_charset -- --ignored
/// regenerate_the_unicode_tables`, then re-derive the version skew as
/// [`the_unicode_version_skew_is_one_directional`]'s message says.
#[test]
#[ignore = "writes src/unicode_ident_tables.rs; run deliberately, see the doc comment"]
fn regenerate_the_unicode_tables() {
    let mut out = String::from(TABLES_HEADER);
    out.push_str(&render_table(
        &general_category("L"),
        "LETTER",
        "L",
        "Go `unicode.IsLetter`, the reference's leading and continuing identifier rune.",
    ));
    out.push_str(&render_table(
        &general_category("Nd"),
        "DECIMAL_NUMBER",
        "Nd",
        "Go `unicode.IsDigit`, a continuing identifier rune only.",
    ));
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/src/unicode_ident_tables.rs");
    std::fs::write(path, out).expect("write the generated tables");
    eprintln!("wrote {path}");
}

fn render_table(table: &[(char, char)], name: &str, category: &str, doc: &str) -> String {
    let mut out = format!(
        "\n/// General category `{category}` — {doc}\n/// {} ranges, {} code points.\npub(crate) static {name}: &[(char, char)] = &[\n",
        table.len(),
        table
            .iter()
            .map(|(lo, hi)| *hi as u32 - *lo as u32 + 1)
            .sum::<u32>(),
    );
    for (lo, hi) in table {
        out.push_str(&format!(
            "    ('\\u{{{:x}}}', '\\u{{{:x}}}'),\n",
            *lo as u32, *hi as u32
        ));
    }
    out.push_str("];\n");
    out
}

const TABLES_HEADER: &str = r#"// @generated by `cargo test -p pulsus-logql --test identifier_charset -- --ignored regenerate_the_unicode_tables`
//
// DO NOT EDIT BY HAND. Issue #392.
//
// General categories `L` and `Nd`, emitted from `regex-syntax`'s
// `\p{L}` / `\p{Nd}`. These are the two categories Go's
// `unicode.IsLetter` / `unicode.IsDigit` test, which is the rule
// grafana/loki v3.7.4's LogQL scanner applies to identifier runes
// (`pkg/logql/syntax/query_scanner.go:338-343` @ v3.7.4) — see
// `crate::unicode_ident` for the citation trail.
//
// UNICODE VERSION SKEW, MEASURED AND DELIBERATE. `regex-syntax` 0.8.11
// carries Unicode 16.0.0; Go 1.25.5 (`unicode.Version`) carries 15.0.0.
// The difference is a STRICT ONE-DIRECTIONAL SUPERSET: 4,924 code
// points are `L` here and not there, 80 are `Nd` here and not there,
// and ZERO go the other way. So no identifier the reference accepts is
// refused here; the only effect is that we accept some code points
// Unicode 15.0 leaves unassigned. `tests/identifier_charset.rs`'s
// `the_unicode_version_skew_is_one_directional` fails if that ever
// stops being true, which is what makes this a decision rather than an
// oversight.
"#;

// ---------------------------------------------------------------------
// 4. The live leg.
// ---------------------------------------------------------------------

/// Replays every recorded reference verdict against a live container.
///
/// Gate: skips cleanly unless `PULSUSDB_LOGQL_DIFF_URL` is set — the
/// same gate, container and CI job `tests/logql_differential.rs` and
/// `tests/case_folding.rs` use. Anything other than 2xx or exactly 400
/// panics as inconclusive rather than being scored as a rejection.
#[test]
fn the_identifier_charset_agrees_with_the_reference() {
    let Some(base) = pulsus_testkit::live_endpoint("PULSUSDB_LOGQL_DIFF_URL") else {
        eprintln!("skipping: set PULSUSDB_LOGQL_DIFF_URL to replay against the reference");
        return;
    };
    let base = base.trim_end_matches('/').to_string();

    // THE OTHER LEGS SHARE THIS CONTAINER AND PUSH INTO IT. If anything
    // has pushed to our selector, an accepted query stops being a 200
    // (the reference 500s when it has to render a non-ASCII label name)
    // and every accept row would score as inconclusive for a reason that
    // has nothing to do with #392. Fail here, loudly, with the fix.
    let (code, body) = get(&base, &format!("{SELECTOR} | logfmt"));
    assert_eq!(
        code, 200,
        "readiness/contamination check: {SELECTOR} must be a 200, got {code} — body: {body}"
    );
    assert!(
        body.contains(r#""result":[]"#),
        "another suite has pushed into {SELECTOR} on this shared container. Every accept \
         row below would become a 500 (the reference cannot render a non-ASCII label \
         name), which reads as a finding but is only CI step order. Give #392 its own \
         selector again, or stop the other suite pushing to this one. Body: {body}"
    );

    let mut drift = Vec::new();
    let mut skipped = 0usize;
    let mut probed = 0usize;

    for (id, rune_form, expected, live) in BOUNDARY {
        match live {
            LiveProbe::Skip(_) => {
                skipped += 1;
                continue;
            }
            LiveProbe::Replay => {}
        }
        probed += 1;
        let query = boundary_query(rune_form);
        let got = verdict(&base, &query);
        if got != *expected {
            drift.push(format!(
                "boundary/{id}: recorded {expected:?}, live {got:?}"
            ));
        }
    }

    for (id, _, template, reference, live) in GRAMMAR_POSITIONS {
        match live {
            LiveProbe::Skip(_) => {
                skipped += 1;
                continue;
            }
            LiveProbe::Replay => {}
        }
        probed += 1;
        let query = template.replace("SEL", SELECTOR);
        let got = verdict(&base, &query);
        let expected = match reference {
            Reference::Serves => Verdict::Accept,
            Reference::RefusesBelowTheLexer(_) => Verdict::Reject,
            Reference::NoAnswer(_) => unreachable!("a NoAnswer row is never Replay"),
        };
        if got != expected {
            drift.push(format!(
                "position/{id}: recorded {expected:?}, live {got:?}"
            ));
        }
    }

    // Pinned so a row cannot be quietly turned into a Skip, or dropped,
    // without the count moving.
    assert_eq!(probed, 46, "the number of live probes changed");
    assert_eq!(
        skipped, 1,
        "the number of rows deliberately NOT put to the shared container changed — each \
         one costs ~40 s and a retry storm, so adding another needs its reason recorded"
    );

    assert!(
        drift.is_empty(),
        "the recorded reference verdicts no longer match the container:\n{}",
        drift.join("\n")
    );
}

fn get(base: &str, query: &str) -> (u32, String) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let out = std::process::Command::new("curl")
        .args(["-s", "-w", "\n%{http_code}", "-G", "--max-time", "20"])
        .args(["--data-urlencode", &format!("query={query}")])
        .args([
            "--data-urlencode",
            &format!("start={}", now.saturating_sub(60)),
        ])
        .args(["--data-urlencode", &format!("end={now}")])
        .args(["--data-urlencode", "step=60s"])
        .args(["--data-urlencode", "limit=1"])
        .arg(format!("{base}/loki/api/v1/query_range"))
        .output()
        .expect("curl must be on PATH");
    let text = String::from_utf8_lossy(&out.stdout);
    let (body, code) = text.rsplit_once('\n').unwrap_or(("", "0"));
    (code.trim().parse().unwrap_or(0), body.to_string())
}

fn verdict(base: &str, query: &str) -> Verdict {
    let (code, body) = get(base, query);
    match code {
        200..=299 => Verdict::Accept,
        400 => Verdict::Reject,
        other => panic!(
            "inconclusive: the reference returned {other} for {query:?} — only 2xx and 400 \
             are syntax verdicts. Body: {body}"
        ),
    }
}
