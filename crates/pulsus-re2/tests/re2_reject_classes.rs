//! Issue #400 Stage 2: the reject-only pre-check
//! [`pulsus_re2::re2_definitely_rejects`], rule by rule.
//!
//! # Why this file is separate from `lib.rs`'s unit tests
//!
//! Every assertion here is about a pattern the REFERENCE refuses and the
//! Rust `regex` crate serves — nine of issue #400's eighteen classes plus
//! three found by sweeping. Two properties of that make it its own file:
//!
//! * **The harmful direction is over-rejection.** A `true` refuses a
//!   query grafana/loki v3.7.4 answers, so every rule below carries a
//!   CONTROL SET the reference is measured to serve, and
//!   [`the_flagged_set_over_the_frozen_corpus_is_exactly_the_committed_one`]
//!   sweeps the whole 4,315-pattern corpus so the claim's domain is the
//!   set and not the examples.
//! * **[`the_rust_crate_reads_these_as_a_different_pattern`] must outlive
//!   the divergence rows it justifies.** Stage 2 retires
//!   `engine_dir_b_read_as_a_different_pattern` and
//!   `engine_dir_b_class_forms` from
//!   `pulsus-read/tests/logql_regex_accept_matrix.rs`; a pin hanging off
//!   those rows would leave the tree in the same commit that introduces
//!   the rejection it explains.
//!
//! # Where the measurements come from
//!
//! Every `200`/`400` in a comment below was taken from the pinned
//! reference container — `grafana/loki@sha256:87f0a067…` on
//! `ci/logql/config.yaml`, started as `pulsus-c400-loki` on port 13400,
//! 2026-08-12 — at
//! `{app="x"} | line_format "{{ __line__ }}" |~ "PATTERN"` over a window
//! ending at `now`. The `line_format` matters: without it the filter is
//! pushed into storage and the reference's own pipeline-build error never
//! runs (Stage 1's finding).

use pulsus_re2::{Re2Verdict, re2_definitely_rejects, re2_verdict};

// ---------------------------------------------------------------------
// Criterion 10 — every rule is named, reached, and bounded
// ---------------------------------------------------------------------

/// One rule representative: the rule letter, the pattern, and the
/// reference's measured status.
type Rep = (char, &'static str);

/// **Rules (a)-(h): each is REACHED by a named representative, and each
/// representative is `Rejects` at the verdict level too.**
///
/// The second half is not redundant: `re2_verdict` consults
/// `re2_definitely_rejects` first, so a rule that fires without moving
/// the verdict would mean the wiring was dropped.
#[test]
fn each_rule_is_named_reached_and_bounded() {
    // Every row measured `400` at the reference, 2026-08-12.
    const REPRESENTATIVES: &[Rep] = &[
        // (a) a repetition applied to a repetition — `ErrInvalidRepeatOp`
        ('a', "a**"),
        ('a', "a*+"),
        ('a', "a++"),
        ('a', "a?*"),
        ('a', "a{2}{3}"),
        ('a', "a*??"),
        ('a', "a{2,3}+"),
        ('a', "a{1000}{1}"),
        // (b) a bound above RE2_MAX_REPEAT — `ErrInvalidRepeatSize`
        ('b', "a{1001}"),
        ('b', "a{0,1001}"),
        ('b', "a{1001,}"),
        // (c) a flag run carrying `u`, `x` or `R` — `ErrInvalidPerlOp`
        ('c', "(?x)a"),
        ('c', "(?x:a)"),
        ('c', "(?u)a"),
        ('c', "(?i-u)a"),
        ('c', "(?R)a"),
        ('c', "(?R:a)"),
        // (d) a `\u`/`\U` escape, every spelling — `ErrInvalidEscape`.
        //
        // The ESCAPE, never its decoded character: the literal `A` and
        // `[A]` are `200` there and are in the control set below.
        ('d', r"\u0041"),
        ('d', r"[\u0041]"),
        ('d', r"\U00000041"),
        ('d', r"\u{263A}"),
        ('d', r"\U{1F600}"),
        ('d', r"\U0001F600"),
        ('d', r"[\u{263A}]"),
        ('d', r"[\U0001F600]"),
        // (e) an unknown POSIX class name — `ErrInvalidCharRange`
        ('e', "[[:foo:]]"),
        ('e', "[a[:zzz:]]"),
        ('e', "[[:^foo:]]"),
        ('e', "[[:alpha:][:zzz:]]"),
        // (f) a property name outside the 202 — `ErrInvalidCharRange`
        ('f', r"\p{Alphabetic}"),
        ('f', r"[\p{Alphabetic}]"),
        ('f', r"\p{Lc}"),
        ('f', r"\p{Garay}"),
        ('f', r"\p{}"),
        ('f', r"\pX"),
        // (g) the range rule — `ErrInvalidCharRange`
        ('g', "[a--b]"),
        // (h) an invalid capture NAME — `ErrInvalidNamedCapture`
        ('h', "(?P<n.x>a)"),
        ('h', "(?<n.x>a)"),
        ('h', "(?P<n[>a)"),
        ('h', "(?P<\u{e9}>a)"),
    ];

    for (rule, pattern) in REPRESENTATIVES {
        assert!(
            re2_definitely_rejects(pattern),
            "rule ({rule}): {pattern:?} is a `400` at the reference and must be flagged"
        );
        assert_eq!(
            re2_verdict(pattern),
            Re2Verdict::Rejects,
            "rule ({rule}): {pattern:?} must reach the verdict, not just the pre-check"
        );
    }
    // Every rule letter is reached, so a rule cannot be dropped from the
    // implementation and pass here by having no representative.
    for rule in ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'] {
        assert!(
            REPRESENTATIVES.iter().any(|(r, _)| *r == rule),
            "rule ({rule}) has no representative"
        );
    }
}

/// **The control set: patterns the reference SERVES (or already refuses
/// for a reason both engines share), which no rule may claim.**
///
/// This is the half that matters. Over-rejection breaks a query the
/// reference answers, so each row is annotated with what was measured.
#[test]
fn the_control_set_reaches_no_rule() {
    // 47 of these measured `200` at the reference, 2026-08-12. The three
    // marked AGREEMENT are `400` there AND an error in the Rust crate, so
    // no rule needs to claim them and none does.
    const CONTROLS: &[&str] = &[
        "a.*b",
        // Rule (b)'s boundary. `a{1000}` is the last served bound.
        "a{999}",
        "a{1000}",
        // Rule (c)'s flag vocabulary: `U` is RE2's own NonGreedy flag.
        "(?i)abc",
        "(?U)a",
        "(?ms)a",
        "(?-i:a)",
        "(?)a",
        // `(?R)` inside a class is five literal members.
        "[(?R)]",
        // Rule (e)'s accepted names, positive and negated forms.
        "[[:alnum:]]",
        "[[:^alpha:]]",
        "[a[:digit:]]",
        // ...and the same text OUTSIDE a class, where it is not a POSIX
        // class at all but a class of literals.
        "[:alpha:]",
        "[:zzz:]",
        // Rule (h): valid names, including the digit-leading ones Go
        // accepts and the Rust crate refuses (Class E — must NOT fire).
        "(?P<n>a)",
        "(?<n>a)",
        "(?P<u>a)",
        "(?<x>a)",
        "(?P<n_1>a)",
        "(?P<1n>a)",
        "(?P<0>a)",
        // Rule (f): names that ARE in the table.
        r"\p{L}",
        r"\p{LC}",
        r"\p{Any}",
        r"\p{Greek}",
        r"\p{Cn}",
        r"\p{Cs}",
        r"\pL",
        r"\P{L}",
        r"\p{Latin}",
        // ...and the negated spelling, which `parse.go:1698-1701 @
        // v3.7.4` strips before the lookup, so `\p{^L}` is `\P{L}` and
        // is `200`. Flagging it was a live over-rejection in the
        // harmful direction.
        r"\p{^L}",
        r"\P{^L}",
        r"[\p{^L}]",
        r"\p{^Greek}",
        // Rule (d) is about the ESCAPE. Its DECODED character is an
        // ordinary literal and is `200` there — flagging it would refuse
        // every pattern containing an `A`.
        "A",
        "[A]",
        // Rule 0's bail-out.
        r"\Qa*\E",
        // Direction A: the Rust crate refuses these, the reference
        // serves them. The pre-check must stay silent so the compile
        // keeps deciding (Class E, out of scope).
        r"\101",
        "a{bbb}c",
        "a{,5}",
        "a{}",
        "(?ss:ab)",
        "a(?i){2}",
        "(?P<n>a)(?P<n>b)",
        // AGREEMENT: `400` at the reference AND an error in the Rust
        // crate. Named so a future reader does not "fix" the rule set to
        // claim them.
        "(?#c)a",
        "(?'n'a)",
        "(?-)a",
        // Correction (ii): a brace-form escape consumed WHOLE.
        r"\x{41}",
        r"\x{41}+a",
        r"\x{41}{2}",
    ];
    for pattern in CONTROLS {
        assert!(
            !re2_definitely_rejects(pattern),
            "{pattern:?} is served by the reference (or already refused by both engines) and \
             must reach no rule — a `true` here refuses a query grafana/loki v3.7.4 answers"
        );
    }
    assert_eq!(
        CONTROLS.len(),
        50,
        "the plan's 44 controls plus six added by measurement while implementing: the four \
         `\\p{{^…}}` spellings the reference strips before its table lookup, and the literal \
         `A`/`[A]` that rule (d)'s write-up had in place of its escape spellings"
    );
}

// ---------------------------------------------------------------------
// Criterion 11 — the property table is the fork's, not a PCRE engine's
// ---------------------------------------------------------------------

/// **`unicodeTable` is `unicode.Categories` + `unicode.Scripts` + `Any`,
/// and nothing else.**
///
/// The discriminators are chosen against the three plausible wrong
/// tables: a PCRE-style property list (`Alphabetic`, `Assigned`, `ASCII`,
/// `Uppercase_Letter`), a newer Unicode (`Garay`, added in Unicode 16),
/// and a `canonicalName`-style case fold (`Lc` for `LC`). Every verdict
/// below was measured on the pinned container, 2026-08-12.
#[test]
fn the_property_table_is_the_forks_and_not_a_pcre_engines() {
    // `200` there.
    assert!(!re2_definitely_rejects(r"\p{LC}"));
    // `400` there. `Lc` is the case-fold discriminator: an implementation
    // that canonicalised the name would serve it.
    for pattern in [
        r"\p{Lc}",
        r"\p{Assigned}",
        r"\p{Ascii}",
        r"\p{ASCII}",
        r"\p{Alphabetic}",
        r"\p{Garay}",
        r"\p{Uppercase_Letter}",
    ] {
        assert!(
            re2_definitely_rejects(pattern),
            "{pattern:?} is `invalid character class range` at the reference"
        );
    }
    // And the premise that makes those SEVEN a divergence rather than
    // agreement: the Rust crate compiles every one of them.
    for pattern in [
        r"\p{Assigned}",
        r"\p{Ascii}",
        r"\p{ASCII}",
        r"\p{Alphabetic}",
        r"\p{Garay}",
        r"\p{Uppercase_Letter}",
    ] {
        assert!(
            regex::Regex::new(pattern).is_ok(),
            "premise: the Rust crate must ACCEPT {pattern:?} for this to be a divergence"
        );
    }
}

// ---------------------------------------------------------------------
// Criterion 25 — the 202-name table, hermetically
// ---------------------------------------------------------------------

/// **The committed table has exactly 202 names, and the names that decide
/// which toolchain and which engine produced it are in or out.**
///
/// The COUNT is the detector for a toolchain that adds a script (Go 1.27
/// picking up Unicode 16 would move it); the discriminators are the
/// detector for the table having been taken from the wrong engine. 202
/// live probes buy nothing this does not — see
/// [`live_probe_every_property_name`], which keeps them available and
/// out of CI.
#[test]
fn the_committed_property_table_is_exactly_the_go_one() {
    assert_eq!(
        pulsus_re2::go_unicode_property_name_count(),
        202,
        "`unicode.Categories` (38) + `unicode.Scripts` (163) + the `Any` special case. A move \
         here means the extraction toolchain changed; re-probe the whole set before editing \
         this number"
    );
    for name in ["LC", "Any", "Cs", "Greek", "Latin", "Cn", "Han"] {
        assert!(
            pulsus_re2::is_go_unicode_property_name(name),
            "{name:?} is a `unicodeTable` key and `\\p{{{name}}}` is `200` at the reference"
        );
    }
    for name in [
        "Lc",
        "Assigned",
        "Ascii",
        "ASCII",
        "Alphabetic",
        "Word",
        "Garay",
        "Uppercase_Letter",
        "Letter",
    ] {
        assert!(
            !pulsus_re2::is_go_unicode_property_name(name),
            "{name:?} is NOT a `unicodeTable` key and `\\p{{{name}}}` is `400` at the reference"
        );
    }
}

/// **The exhaustive live re-probe of all 202 names — kept, and kept out
/// of CI.**
///
/// ```text
/// PULSUSDB_LOGQL_DIFF_URL=http://localhost:13400 \
///   cargo test -p pulsus-re2 --test re2_reject_classes -- \
///   --ignored --nocapture live_probe_every_property_name
/// ```
///
/// Measured 2026-08-12 against `pulsus-c400-loki`: **202 accepted, 0
/// rejected**, 86 s. It is `#[ignore]`d because
/// [`the_committed_property_table_is_exactly_the_go_one`] detects the
/// same movement hermetically and in milliseconds; this exists so the
/// claim in `unicode_property_names.rs`'s module doc can be re-run rather
/// than believed.
#[test]
#[ignore = "202 live HTTP probes against the pinned reference; ~86 s"]
fn live_probe_every_property_name() {
    let Ok(base) = std::env::var("PULSUSDB_LOGQL_DIFF_URL") else {
        eprintln!("PULSUSDB_LOGQL_DIFF_URL unset — skipping the 202-name live probe");
        return;
    };
    let mut rejected = Vec::new();
    for name in pulsus_re2::go_unicode_property_names() {
        let query = format!(r#"{{app="x"}} | line_format "{{{{ __line__ }}}}" |~ "\\p{{{name}}}""#);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs();
        let out = std::process::Command::new("curl")
            .args([
                "-s",
                "-S",
                "--max-time",
                "60",
                "-o",
                "/dev/null",
                "-w",
                "%{http_code}",
                "-G",
                &format!("{base}/loki/api/v1/query_range"),
                "--data-urlencode",
                &format!("query={query}"),
                "--data-urlencode",
                &format!("start={}", (now - 300) * 1_000_000_000),
                "--data-urlencode",
                &format!("end={}", now * 1_000_000_000),
                "--data-urlencode",
                "step=60s",
            ])
            .output()
            .expect("curl");
        let code = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if code != "200" {
            rejected.push(format!("{name}: {code}"));
        }
    }
    println!(
        "PROPERTY_NAME_PROBE accepted={} rejected={}",
        pulsus_re2::go_unicode_property_names().len() - rejected.len(),
        rejected.len()
    );
    assert!(
        rejected.is_empty(),
        "the reference refuses {} of the committed names: {rejected:#?}",
        rejected.len()
    );
}

// ---------------------------------------------------------------------
// Criterion 19 — rule (g) is a RANGE rule
// ---------------------------------------------------------------------

/// **Rule (g) is `X--` read as the range `X`..`-`, not "a literal `--` is
/// invalid".**
///
/// The wrong implementation is plausible and its cost is eleven measured
/// `200`s turned into rejections, so both columns are asserted. All 21
/// shapes were measured on the pinned container, 2026-08-12.
///
/// **Nine of the reference's TEN rejections fire, and the tenth is
/// deliberate.** `[a-\-b]` is `400` there (`invalid character class
/// range: `a-\-``) because `parseClassChar` decodes `\-` to the same
/// U+002D, but the rule as specified fires only on an UNESCAPED `-`, so
/// it declines. That costs nothing: `[a-\-b]` is an invalid range in the
/// Rust crate too, so it was already a joint rejection — asserted below,
/// rather than left as a difference between two counts.
#[test]
fn rule_g_is_the_range_rule_and_not_a_double_dash_rule() {
    // `400` at the reference; the rule fires.
    for pattern in [
        "[a--b]",
        "[a--]",
        "[^a--b]",
        "[a---b]",
        "[.--z]",
        "[0--9]",
        "[-a--b]",
        "[\u{e9}--b]",
        r"[\x41--b]",
    ] {
        assert!(
            re2_definitely_rejects(pattern),
            "{pattern:?} is `invalid character class range` at the reference"
        );
    }
    // The tenth reference rejection. The rule declines, and the compile
    // decides — which it does the same way.
    //
    // The pattern is assembled rather than written as a literal for one
    // reason: `clippy::invalid_regex` rejects a literal it can prove
    // uncompilable, and the point of this assertion is that this one IS
    // uncompilable. Assembling it puts the string beyond the lint's
    // reach without an `#[allow]` that would also cover a future typo.
    let escaped_dash_range = format!("[a-{}-b]", r"\");
    assert_eq!(escaped_dash_range, r"[a-\-b]");
    assert!(!re2_definitely_rejects(&escaped_dash_range));
    assert!(
        regex::Regex::new(&escaped_dash_range).is_err(),
        "`[a-\\-b]` is `400` at the reference and an invalid range in the Rust crate too, so \
         rule (g) declining it costs no parity"
    );

    // `200` at the reference; the rule must NOT fire. A "literal `--`"
    // implementation refuses every one of these.
    for pattern in [
        "[!--b]",
        "[+--b]",
        "[ --a]",
        "[--a]",
        "[^--a]",
        "[--]",
        "[a-z--b]",
        r"[\w--a]",
        r"[a\--b]",
        "[[:alpha:]--b]",
        r"[\n--b]",
    ] {
        assert!(
            !re2_definitely_rejects(pattern),
            "{pattern:?} is `200` at the reference — the `-` is not in range-operator position, \
             or the range it opens is valid"
        );
    }

    // And the two class-algebra spellings that must never be added to
    // this rule: both sides serve them (with different rows — that
    // family is a WRONG-ROWS defect, not an acceptance one).
    for pattern in ["[a&&b]", "[a~~b]"] {
        assert!(
            !re2_definitely_rejects(pattern),
            "{pattern:?} is `200` on both sides; it is a wrong-rows divergence and rule (g) \
             must not claim it"
        );
    }
}

// ---------------------------------------------------------------------
// Criterion 20 — `\Q…\E` and the brace-form escapes
// ---------------------------------------------------------------------

/// **A literal-quoting region disables every rule, and a brace-form
/// escape is consumed whole.**
///
/// These are the two omissions that produced the first draft's false
/// rejections, and each is asserted with the shape that reintroduces it.
#[test]
fn a_literal_quoting_region_disables_every_rule() {
    // Cause (i): `\Q…\E` makes everything inside LITERAL. Each of these
    // is `200` at the reference; each carries a construct that fires a
    // rule when it is not inside a quoting region.
    for pattern in [
        r"\Q(?x)\E",
        r"\Qa(?x)b",
        r"\Qa**",
        r"\Q{1001}",
        r"\Q\u{41}",
        r"\Q\p{Alphabetic}",
        r"\Q[[:foo:]]",
    ] {
        assert!(
            !re2_definitely_rejects(pattern),
            "{pattern:?} is `200` at the reference: inside `\\Q…\\E` the construct is text"
        );
    }
    // The bail-out is containment, not region tracking, so the region's
    // END does not re-enable the rules either. `\Qa\E(?x)b` is `400`
    // there — a `false` here is a no-opinion, which costs nothing.
    assert!(!re2_definitely_rejects(r"\Qa\E(?x)b"));

    // Cause (ii): a brace-form escape consumed whole. A naive `i += 2`
    // leaves `{41}` to be read as a repetition, and `{2}` after it then
    // fires rule (a). Every one of these is `200` at the reference.
    for pattern in [
        r"\x{41}",
        r"\x{41}+a",
        r"\x{41}{2}",
        r"\p{L}{2}",
        r"\b{start}a",
        r"\B{end}a",
    ] {
        assert!(
            !re2_definitely_rejects(pattern),
            "{pattern:?} is `200` at the reference — the brace belongs to the escape"
        );
    }
    // And the other direction, so "consume it whole" cannot degenerate
    // into "swallow whatever follows": `\x{41}{1001}` IS `400` there
    // (`invalid repeat count: {1001}`).
    assert!(re2_definitely_rejects(r"\x{41}{1001}"));
}

// ---------------------------------------------------------------------
// Criterion 21 — the whole frozen corpus
// ---------------------------------------------------------------------

/// The `re2_screen` corpus, curated then generated, in file order — the
/// same load `pulsus-read/tests/re2_screen_differential.rs` performs.
fn frozen_corpus() -> Vec<String> {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../pulsus-read/tests/fixtures/re2_screen");
    let mut out = Vec::new();
    for name in ["curated.txt", "generated.txt"] {
        let path = dir.join(name);
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        out.extend(
            text.lines()
                .filter(|l| !l.is_empty() && !l.starts_with('#'))
                .map(str::to_string),
        );
    }
    out
}

/// The flagged set's digest: SHA-256 over the flagged patterns, sorted,
/// each followed by a newline. Stated rather than implied so the number
/// can be reproduced outside this test.
fn flagged_digest(flagged: &[String]) -> String {
    use sha2::{Digest, Sha256};
    let mut sorted: Vec<&String> = flagged.iter().collect();
    sorted.sort();
    let mut h = Sha256::new();
    for p in sorted {
        h.update(p.as_bytes());
        h.update(b"\n");
    }
    format!("{:x}", h.finalize())
}

/// The corpus size, asserted so a shrunk corpus cannot make the flagged
/// count fall for a reason that has nothing to do with the rules.
const CORPUS_PATTERNS: usize = 4315;

/// How many of [`CORPUS_PATTERNS`] the rules flag.
///
/// **Every one of these was put to the pinned reference individually and
/// answered `400` — 0 served, i.e. 0 false rejections over the whole
/// flagged set.** Container `pulsus-c400-loki`
/// (`grafana/loki@sha256:87f0a067…`, `ci/logql/config.yaml`) on port
/// 13400, 2026-08-12, at
/// `{app="x"} | line_format "{{ __line__ }}" |~ "PATTERN"` over a window
/// ending at `now`; 1,160 probes in 20.2 s, `1160 400` and nothing else.
///
/// The plan measured 1,130 from its prototype. The 30-pattern gap is
/// this implementation flagging MORE, not the corpus moving — the
/// corpus size is asserted above — and all 1,160 are `400` there, so
/// the extra 30 are rejections the prototype declined rather than
/// rejections the reference does not make.
const FLAGGED_PATTERNS: usize = 1160;

/// SHA-256 of the sorted flagged set, one pattern per line.
///
/// ```text
/// # regenerated at 06de475 (branch base), corpus = curated.txt ++ generated.txt in file order:
/// #   cargo test -p pulsus-re2 --test re2_reject_classes -- --ignored --nocapture print_flagged_digest
/// ```
///
/// `--nocapture` is not optional: without it a passing test prints
/// nothing. Filter position relative to `--` does not matter.
///
/// The method is `sha256(pattern + "\n" for each, sorted)`, which is
/// exactly `LC_ALL=C sort <flagged> | sha256sum` — cross-checked against
/// the shell so the value is reproducible without this test.
const FLAGGED_DIGEST: &str = "dacfaafa6f9a03b99b8b3ba20d681d62b6e1ac5acb0186c8f7a50fc66661fbf6";

/// **The flagged set over the frozen corpus is exactly the committed
/// one.**
///
/// A count alone would pass under a rule that flagged a different 1,130
/// patterns, which is why the digest is here too. Hermetic: the live
/// provenance is in [`FLAGGED_PATTERNS`]'s doc.
#[test]
fn the_flagged_set_over_the_frozen_corpus_is_exactly_the_committed_one() {
    let corpus = frozen_corpus();
    assert_eq!(
        corpus.len(),
        CORPUS_PATTERNS,
        "the frozen corpus changed size; the flagged figures below are scoped to it"
    );
    let flagged: Vec<String> = corpus
        .into_iter()
        .filter(|p| re2_definitely_rejects(p))
        .collect();
    assert_eq!(
        flagged.len(),
        FLAGGED_PATTERNS,
        "the flagged COUNT moved. If it grew, the new members must be put to the reference \
         before this number is edited — a false rejection refuses a query it serves"
    );
    assert_eq!(
        flagged_digest(&flagged),
        FLAGGED_DIGEST,
        "the flagged SET moved without its count moving — the same number of different patterns"
    );
}

/// Prints the count and digest for [`FLAGGED_DIGEST`]'s regeneration
/// command. `#[ignore]`d because it asserts nothing.
#[test]
#[ignore = "a regeneration helper, not a check"]
fn print_flagged_digest() {
    let corpus = frozen_corpus();
    let flagged: Vec<String> = corpus
        .iter()
        .filter(|p| re2_definitely_rejects(p))
        .cloned()
        .collect();
    println!(
        "FLAGGED corpus={} count={} digest={}",
        corpus.len(),
        flagged.len(),
        flagged_digest(&flagged)
    );
}

// ---------------------------------------------------------------------
// Criterion 26 — what the Rust crate reads these as
// ---------------------------------------------------------------------

/// **Why the rejections exist: the Rust crate does not merely accept
/// these, it reads them as a DIFFERENT pattern.**
///
/// Measured over eleven subjects at the locked `regex` version. This is
/// the pin that survives the retirement of
/// `engine_dir_b_read_as_a_different_pattern` and
/// `engine_dir_b_class_forms` from
/// `pulsus-read/tests/logql_regex_accept_matrix.rs`: those rows carried
/// the only record of this, and a divergence row cannot document the
/// change that removes it.
#[test]
fn the_rust_crate_reads_these_as_a_different_pattern() {
    const SUBJECTS: &[&str] = &[
        "",
        "a",
        "b",
        "-",
        ":",
        "f",
        "o",
        "ab",
        "101",
        "\u{1F600}",
        "a-b",
    ];

    /// `(pattern, the subjects it matches)`. Everything not listed must
    /// NOT match, so the table is exhaustive over `SUBJECTS`.
    const READINGS: &[(&str, &[&str])] = &[
        // Read as `(a*)*`, which matches EVERY subject including the
        // empty string — a line filter carrying it returns the whole
        // stream while the reference answers 400.
        ("a**", SUBJECTS),
        // Read as the class DIFFERENCE `[a] - [b]`, so it contains `a`
        // and neither `b` nor `-`.
        ("[a--b]", &["a", "ab", "a-b"]),
        // Read as a nested class of the literal members `:`, `f`, `o`.
        ("[[:foo:]]", &[":", "f", "o"]),
        ("[a[:zzz:]]", &["a", ":", "ab", "a-b"]),
        // `[[:^foo:]]` too — the `^` is a member, not a negation, so it
        // is NOT the complement of anything.
        ("[[:^foo:]]", &[":", "f", "o"]),
    ];

    for (pattern, matching) in READINGS {
        let re = regex::Regex::new(pattern)
            .unwrap_or_else(|e| panic!("premise: the Rust crate must COMPILE {pattern:?}: {e}"));
        for subject in SUBJECTS {
            let want = matching.contains(subject);
            assert_eq!(
                re.is_match(subject),
                want,
                "{pattern:?} against {subject:?}: the Rust crate's reading moved"
            );
        }
        // ...and the reference refuses the pattern outright, which is
        // what makes the reading a WRONG ANSWER rather than a permissive
        // one.
        assert!(
            re2_definitely_rejects(pattern),
            "{pattern:?} must be flagged — that is the whole point of the reading above"
        );
    }
}
