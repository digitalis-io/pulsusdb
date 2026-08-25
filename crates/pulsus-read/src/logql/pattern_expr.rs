//! Issue #388: the `| pattern "…"` sub-grammar — which patterns are
//! refused, and which `<name>` is a capture at all.
//!
//! # Why a second grammar exists at all
//!
//! The reference parses a `| pattern` argument with a **separate, tiny
//! grammar**: a Ragel lexer whose whole token set is two lines
//! (`pkg/logql/log/pattern/lexer.rl:32-35 @ v3.7.4`), a yacc grammar of
//! five productions (`pattern/expr.y:29-45 @ v3.7.4`) and three
//! validations (`pattern/ast.go:18-58 @ v3.7.4`), driven by `pattern.New`
//! (`pattern/pattern.go:21-33 @ v3.7.4`).
//!
//! It is reached from `NewPatternParser` (`log/parser.go:456-470 @
//! v3.7.4`), and — unlike the `| logfmt` and `| json` expression
//! sub-grammars, which run at `Stage()` — that call happens inside
//! `newLabelParserExpr` (`syntax/ast.go:730-741 @ v3.7.4`), which
//! **panics with a `logqlmodel.ParseError` during `ParseExpr`**. So a
//! malformed pattern is a 400 in EVERY window, where a malformed json
//! expression is a 200 once the window's end is older than
//! `query_ingesters_within`. Measured on the pinned container over a
//! window ending 24 h back: `| pattern "<a> <a>"` stays 400 while
//! `| json v="b-c"` and `| logfmt a="b.c"` are 200. That is why this
//! module and [`super::json_expr`] share no code, no error type and no
//! test harness, and why the two facts are pinned by separate live legs.
//!
//! PulsusDB had an approximation, `compile_pattern`, which got two things
//! wrong and both were user-visible:
//!
//! 1. **A repeated capture name was served.** `| pattern "<a> <a>"` over
//!    `one two three` answered `a="one"` — the second capture silently
//!    discarded by #334's first-wins rule. The user asked for two
//!    captures and got one, with no error.
//! 2. **A capture name could start with a digit, and we INVENTED a
//!    label.** Our test was `[A-Za-z0-9_]+`; the reference's lexer is
//!    `'<' (alpha|'_') (alnum|'_')* '>'` (`lexer.rl:33`). So
//!    `| pattern "<1a> x"` over `9 x` answered `_1a="9"` here — a label
//!    the user never wrote — while the reference reads `<1a>` as literal
//!    text, leaves the pattern with no capture at all, and refuses it.
//!
//! # The rules, read off the reference's source and then measured
//!
//! One row per line of the committed
//! `tests/logql_pattern_expr_reference_error_sites.txt`, which is the
//! literal output of one `git grep` whose scope that file's header
//! states. `err_class` and `probeable` are READINGS (R1, R2 below); the
//! probe column is a capture from the pinned digest.
//!
//! | site @ v3.7.4 | err_class | probeable | rule | probe → status |
//! |---|---|---|---|---|
//! | `ast.go:20` | Producer | yes | no NAMED capture (`<_>` does not count — `captures()` skips `isUnnamed`, `ast.go:60-68`, `:83-85`) | `foo`, `<_> <_>`, `<1a> x` → 400 `at least one capture is required` |
//! | `ast.go:30` | Producer | yes | a repeated NAMED capture | `<a> <a>`, `<a> x <a>`, `<__> x <__>` → 400 `duplicate capture name (…): invalid expression` |
//! | `ast.go:44` | Producer | yes | two ADJACENT capture nodes, named or unnamed | `<a><b>`, `<a><a>` → 400 `found consecutive capture '<a><b>': invalid expression` |
//! | `ast.go:54` | Producer | **no** — `ConstructAbsentFromOurGrammar` | named captures forbidden in a `\|>`/`!>` line-filter pattern | `{a="b"} \|> "<a> foo"` → 400 `named captures are not allowed: found '<a>'` — evidence about the REFERENCE, not about us |
//! | `lexer.go:32` | Producer | yes | the yacc `Error` callback | `""` → 400 `parse error at line 1, col 1: syntax error: unexpected $end, expecting IDENTIFIER or LITERAL` |
//! | `parser.go:68` | Constructor | — | builds `parseError`; produces nothing itself | — |
//! | `pattern.go:9` | ErrValueDecl | — | `ErrNoCapture`'s declaration | — |
//! | `pattern.go:10` | ErrValueDecl | — | `ErrCaptureNotAllowed`'s declaration | — |
//! | `pattern.go:11` | ErrValueDecl | — | `ErrInvalidExpr`'s declaration | — |
//!
//! **The ORDER is observable and is part of the rule.** `validate()`
//! checks no-capture, then consecutive, then duplicate (`ast.go:18-35`),
//! so `<a><a>` is `found consecutive capture` (not `duplicate`) and
//! `<_><_>` is `at least one capture is required` (not `consecutive`).
//! `compile_pattern` validated INSIDE its lex loop and got the second
//! wrong. [`parse_pattern`] validates after lexing, and
//! `rule_order_is_observable` reddens if that is undone.
//!
//! # The two stages' error sites, and the two DIFFERENT reasons a rule
//! carries no probe
//!
//! Across both halves of #388 there are ten `Producer` rows and four
//! dispositions. The two zero-probe reasons are **different claims** and
//! are kept in separate rows: one says our grammar cannot express the
//! construct, the other says no input reaches the line at all.
//!
//! | disposition | rows | count |
//! |---|---|---|
//! | probeable through this issue's matrices, text observable | pattern `ast.go:20`, `:30`, `:44`, `lexer.go:32`; json `lexer.go:26`, `:80`, `:138`, `:147` | **8** |
//! | reachable in the reference, but only via a construct our grammar lacks (`ConstructAbsentFromOurGrammar`) | pattern `ast.go:54` (`ParseLineFilter` ← `log/filter.go:859` ← `\|>`/`!>`) | 1 |
//! | unreachable by any input (`UnreachableByAnyInput`) | json `jsonexpr/lexer.go:114` (`scanStr` is entered only after `unread()` of a `"`, `lexer.go:75-76`) — **the one row in either table with no empirical support of any kind** | 1 |
//! | not a Producer | pattern `parser.go:68` (Constructor), `pattern.go:9-11` (ErrValueDecl); json `lexer.go:56` (Relay) | 5 |
//!
//! Eight, not nine: an earlier revision of this count had `ast.go:54`
//! among the probeable rows, because a fact established in one round did
//! not survive into a list assembled in a later one. The count is now
//! computed from the tables by
//! `every_reachable_rule_has_a_probe_and_every_probe_names_one_rule` in
//! each matrix rather than written in a sentence.
//!
//! **A rule found OUTSIDE both enumerations.** The json half has an
//! eleventh Producer that no command in this repository could have
//! listed: `strconv.Atoi`'s range error, relayed by `jsonexpr/lexer.go:56`.
//! It is recorded in [`super::json_expr`]'s docs and in
//! `logql_json_expr_sites.tsv` with `source=FoundByProbe`, and it changes
//! a rule we implement (the index bound is Go's `int`, i.e. i64).
//!
//! # `IsValidLabelName` is live code the grammar cannot reach — and no
//! test can tell it apart from its absence
//!
//! `NewPatternParser` runs `model.UTF8Validation.IsValidLabelName(name)`
//! on every capture name (`log/parser.go:462 @ v3.7.4`). For the UTF-8
//! scheme that predicate is "non-empty and valid UTF-8"
//! (`vendor/github.com/prometheus/common/model/metric.go:197-201 @
//! v3.7.4`). Every name the lexer can produce matches
//! `[A-Za-z_][A-Za-z0-9_]*` (`lexer.rl:33`, a byte machine — confirmed
//! live: `| pattern "<é> x"` is 400 `at least one capture is required`,
//! so `é` is not in the identifier class), and non-empty ASCII is always
//! valid UTF-8. **So the predicate is total over the lexer's output, and
//! no probe can distinguish it present from absent.** The same holds for
//! the `LegacyValidation` branch (`metric.go:186-196`), whose charset is
//! character-for-character the lexer's.
//!
//! It is therefore **recorded, not implemented, and not gated**. A gate
//! that cannot fail for the property it names is worse than an honest
//! limit. What IS gated is the property that protects us — that our lexer
//! does not drift outside the reference's charset — by
//! `the_capture_charset_is_exactly_the_references`, which was RED against
//! `5d91ef1`.
//!
//! # `ParseLineFilter` is a fourth rule set, recorded and not implemented
//!
//! `pattern.ParseLineFilter` (`pattern.go:36-51 @ v3.7.4`) validates a
//! pattern with DIFFERENT rules: no capture is required, and named
//! captures are REFUSED (`ast.go:51-57`). It is reached only from the
//! `|>` / `!>` line filter (`log/filter.go:859`), which our grammar has
//! no syntax for. Measured both ways: `{a="b"} |> "<a> foo"` is 400
//! `named captures are not allowed: found '<a>'` on the reference, and
//! `pulsus_logql::parse` of the same query is an `Err`. The matrix's
//! `the_line_filter_rule_set_is_out_of_reach` pins the second half, and
//! reddens the day someone adds the operator — which is exactly when this
//! note stops being true.
//!
//! # The fourteen readings no test can contradict
//!
//! Every judgement behind #388's two halves that would survive CI if it
//! were wrong, enumerated in one table so the next reader inherits the
//! list rather than re-finding it an item at a time. R11–R14 came from
//! reconciling the schema walk against this issue's own earlier rounds;
//! R1–R10 from walking the dataset schema column by column.
//!
//! | # | the reading | where it lives | why nothing reddens it |
//! |---|---|---|---|
//! | R1 | `err_class` (`Producer`/`Relay`/`Constructor`/`ErrValueDecl`) | RefRule rows | derived from reading the line; no check computes it from source |
//! | R2 | `probeable` / `why_not` | RefRule rows | includes `ast.go:54` being `ParseLineFilter`-only and `jsonexpr/lexer.go:114` being unreachable |
//! | R3 | the **line→rule** binding | RefRule rows | the rule↔probe checks tie *rule to probe*; nothing ties a rule id to the source line it came from |
//! | R4 | the generator's domain on constructed sites | Site rows | read out of the generator (`v0…vN`, `arr[0]…arr[N]`, `a…a.k00000`); not derived from running it |
//! | R5 | sweep C's out-of-domain classification of `ast_walk_characterization.rs:887` | sweep C | a type fact read from `build_lf`'s return type, checked by no test |
//! | R6 | the import-list bound (no error arrives from a Loki-internal callee) | both `*-reference-error-sites.txt` headers | falsified only by a re-pin, and only if someone looks. **It did NOT hold for `jsonexpr/`** — `strconv` is a stdlib callee that does return one |
//! | R7 | `IsValidLabelName` unreachability, both halves | this section, and [`super::json_expr`]'s | proven indistinguishable from its negation by any test |
//! | R8 | the enumeration command's SPELLING coverage | both file headers | a constructor under another spelling is absent, not classified |
//! | R9 | the A/B/C/D residual — a query neither literal, nor `format!`/`replace`-built, nor in sweep D's files | both matrices' sweep tests | already named; listed here so the inventory is one list |
//! | R10 | `reference_status` / `reference_text` | Probe rows | **conditionally** reddening: re-derived when the live leg runs, inert committed constants in the hermetic `ci` job |
//! | R11 | `ParseLiterals` has no non-test caller at `v3.7.4`, so it is not a rule | this module | re-verified (`git grep -n ParseLiterals v3.7.4 -- '*.go'` → one declaration, two `pattern_test.go` lines), but no test of ours re-checks it, and if it gained a caller a rule would be missing from the table |
//! | R12 | no client composes a pattern or a json path programmatically — they are hand-typed | the issue's reachability argument | a claim about the client ecosystem; no test can see it |
//! | R13 | the `]`-key usage search was scoped to `crates/pulsus-read/tests/logqltest/corpus` | the `json-expression-bracket-key-unreachable` ledger row | it says nothing about the rest of the tree or about user data, and the withdrawal is taken anyway |
//! | R14 | the json sub-grammar's SECOND reference consumer, `pkg/distributor/field_detection.go:275`, adds no rule to the query path | [`super::json_expr`] | it is an ingest-side call on `allowedLabel`, alongside the query-path call at `log/parser.go:638`; that it cannot influence `\| json` is read, not measured |
//!
//! # What is NOT here
//!
//! `PatternTok` and `walk_pattern` stay in [`super::pipeline`]: this
//! module is the GRAMMAR, not the matcher. `#334`'s first-wins collision
//! rule is untouched — after this change `<a> <a>` never reaches it,
//! while other collisions still do.

use super::pipeline::PatternTok;

/// Every way `pattern.New` (`pkg/logql/log/pattern/pattern.go:21-33 @
/// v3.7.4`) can fail.
///
/// A TYPE rather than a `String` (which is what [`super::logfmt_expr`]
/// returns) for one reason: [`PatternError::message`] matches every
/// variant with no `_` arm, and it sits directly beside the rule table
/// above — so a rule added to this module without a row in that table
/// fails to compile. That check proves **table and code cannot drift
/// apart**; it proves nothing about the reference, which is what the
/// committed enumeration is for, and conflating the two is what an
/// earlier revision of this plan did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PatternError {
    /// `expr.y:32-35` — an empty pattern reduces no `node`. The ONLY
    /// reachable syntax error: every byte of a `&str` lexes as an
    /// identifier or as a literal rune, so no other input can fail to
    /// parse. (The Ragel `utf8` machine at `lexer.rl:17-27` can fail on
    /// an invalid byte; a Rust `&str` cannot carry one.)
    Empty,
    /// `ast.go:19-21` (`ErrNoCapture`) — no NAMED capture. `<_>` does not
    /// count: `captures()` skips `isUnnamed` (`ast.go:60-68`, `:83-85`).
    NoCapture,
    /// `ast.go:37-49` — two adjacent capture nodes, named or unnamed.
    /// Carries the reference's own rendering of the pair, `<a><b>`.
    ConsecutiveCaptures { rendered: String },
    /// `ast.go:26-33` — a repeated NAMED capture.
    DuplicateCapture { name: String },
}

impl PatternError {
    /// The reference's own message for this rule, which the call site
    /// wraps. Matching it is free here — every one of the four is a fixed
    /// string or one `%s` — and it means a user who moves a query between
    /// the two systems reads the same sentence.
    pub(super) fn message(&self) -> String {
        match self {
            PatternError::Empty => "parse error at line 1, col 1: syntax error: unexpected $end, \
                                    expecting IDENTIFIER or LITERAL"
                .to_string(),
            // `pattern.go:9`.
            PatternError::NoCapture => "at least one capture is required".to_string(),
            // `ast.go:44`, with `ErrInvalidExpr` (`pattern.go:11`)
            // appended by `%w`.
            PatternError::ConsecutiveCaptures { rendered } => {
                format!("found consecutive capture '{rendered}': invalid expression")
            }
            // `ast.go:30`, likewise.
            PatternError::DuplicateCapture { name } => {
                format!("duplicate capture name ({name}): invalid expression")
            }
        }
    }
}

/// `identifier = '<' (alpha|'_') (alnum|'_')* '>'` (`lexer.rl:33 @
/// v3.7.4`), matched at the start of `rest`; returns the NAME and the
/// number of bytes consumed.
///
/// **Longest-match is not at issue and that is checkable rather than
/// asserted:** the Ragel scanner takes the longest token, but the
/// identifier's body class excludes `>`, so at any position at most one
/// identifier can match and the greedy scan below is that one. Where no
/// identifier matches, the scanner falls back to its other token —
/// `literal = utf8`, one rune.
fn lex_identifier(rest: &str) -> Option<(&str, usize)> {
    let body = rest.strip_prefix('<')?;
    // The body class excludes `>`, so the FIRST `>` is exactly where the
    // machine would terminate; everything before it must be in the class
    // or there is no identifier here.
    let close = body.find('>')?;
    let name = &body[..close];
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return None,
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return None;
    }
    Some((name, 1 + close + 1))
}

/// Lexes and validates a `| pattern` argument exactly as `pattern.New`
/// does, and in the reference's ORDER: parse, then no-capture, then
/// consecutive, then duplicate (`pattern.go:21-33`, `ast.go:18-35 @
/// v3.7.4`).
///
/// The order is observable — `<a><a>` is `ConsecutiveCaptures` and
/// `<_><_>` is `NoCapture` — and validating during the lex loop (what
/// `compile_pattern` did) cannot reproduce it. That is why the token
/// vector is built first and inspected afterwards.
pub(super) fn parse_pattern(input: &str) -> Result<Vec<PatternTok>, PatternError> {
    // --- the lexer + `expr: node | expr node` (`expr.y:32-40`).
    // Adjacent literal runes reduce into ONE `literals` node
    // (`expr.y:42-44`), which is what makes "two adjacent captures"
    // well defined.
    let mut tokens: Vec<PatternTok> = Vec::new();
    let mut rest = input;
    while !rest.is_empty() {
        if let Some((name, len)) = lex_identifier(rest) {
            tokens.push(if name == "_" {
                // `isUnnamed` is `len(c) == 1 && c[0] == '_'`
                // (`ast.go:83-85`) — so `<__>` is a NAMED capture, and a
                // `starts_with`/`trim` spelling of this test would be
                // wrong in a way no obvious probe catches.
                PatternTok::Discard
            } else {
                PatternTok::Capture(name.to_string())
            });
            rest = &rest[len..];
            continue;
        }
        let c = rest.chars().next().expect("rest is non-empty");
        match tokens.last_mut() {
            Some(PatternTok::Literal(existing)) => existing.push(c),
            _ => tokens.push(PatternTok::Literal(c.to_string())),
        }
        rest = &rest[c.len_utf8()..];
    }
    if tokens.is_empty() {
        return Err(PatternError::Empty);
    }

    // --- `validate()` (`ast.go:18-35`), in its own order.
    if !tokens
        .iter()
        .any(|t| matches!(t, PatternTok::Capture { .. }))
    {
        return Err(PatternError::NoCapture);
    }
    for pair in tokens.windows(2) {
        let is_capture = |t: &PatternTok| matches!(t, PatternTok::Capture(_) | PatternTok::Discard);
        if is_capture(&pair[0]) && is_capture(&pair[1]) {
            return Err(PatternError::ConsecutiveCaptures {
                rendered: format!(
                    "{}{}",
                    render_pattern_tok(&pair[0]),
                    render_pattern_tok(&pair[1])
                ),
            });
        }
    }
    let mut seen: Vec<&str> = Vec::new();
    for t in &tokens {
        if let PatternTok::Capture(name) = t {
            if seen.contains(&name.as_str()) {
                return Err(PatternError::DuplicateCapture { name: name.clone() });
            }
            seen.push(name);
        }
    }
    Ok(tokens)
}

/// `capture.String()` (`ast.go:75-77 @ v3.7.4`) — used only for the
/// consecutive-capture message, which quotes the offending pair.
///
/// **Named `render_pattern_tok` rather than `render`** because
/// `logql_post_agg_witness.rs`'s call-graph census walks `src/logql`
/// recursively and resolves free functions by BARE NAME: a second free
/// `render` (there is one in `template/eval.rs`) leaves it unable to say
/// which body a call reaches, and it refuses rather than guessing. That
/// is a SOURCE-SPELLING constraint — it is about how this function is
/// named, not what it does — and the census is the gate that enforces
/// it. Neither this branch nor `main` failed it alone; only the merge of
/// the two did, because the walk became recursive on `main` while this
/// name was added here.
///
/// **What covers the rename BEHAVIOURALLY**, because a spelling check
/// naming another spelling check as its cover is two checks of the same
/// kind claiming to be two kinds. This function's whole observable
/// output is the quoted pair inside a `ConsecutiveCaptures` message, and
/// two gates execute it and compare that output:
///
/// * `mod tests` below asserts the `rendered` field byte-for-byte for
///   each input — the tighter of the two, and in this file;
/// * `logqltest_corpus.rs::corpus_is_fully_green_and_exercises_every_directive`
///   runs `b25_pattern_expr_reject.test`'s two `eval_fail` rows end to
///   end and gates the produced error text, which carries the pair;
///
/// Both were established by BREAKING this function — perturbing its
/// output and running the whole workspace — not by reading. Measured
/// that way, they are the ONLY two tests in the workspace that redden,
/// which is also how a third suite named here in review round 2 was
/// removed: `logql_pipeline_golden.rs` stayed green, because its
/// `| pattern` cases assert successful compilation and this function
/// runs only on the failure arm.
///
/// **Reachability disposes of every read path at once**: the only two
/// calls are inside the `Err(PatternError::ConsecutiveCaptures { … })`
/// construction above, and there is no third, so a query that reaches
/// SQL has already not called it.
///
/// **Scope, stated so this doc is not extended.** It records what COVERS
/// this function and how that list was arrived at, including the one
/// suite the break took OFF it. It does not go on to inventory what
/// fails to cover this function, or to characterise any other run.
fn render_pattern_tok(tok: &PatternTok) -> String {
    match tok {
        PatternTok::Capture(name) => format!("<{name}>"),
        PatternTok::Discard => "<_>".to_string(),
        PatternTok::Literal(text) => text.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(input: &str) -> Vec<String> {
        parse_pattern(input)
            .expect("accepted")
            .iter()
            .filter_map(|t| match t {
                PatternTok::Capture(n) => Some(n.clone()),
                _ => None,
            })
            .collect()
    }

    /// `ast.go:20` — no NAMED capture. `<_>` never counts.
    #[test]
    fn no_named_capture_is_refused() {
        for input in [
            "foo", "<_>", "<_> <_>", "<> x", "<a b> x", "<a-b> x", "<é> x",
        ] {
            assert_eq!(
                parse_pattern(input),
                Err(PatternError::NoCapture),
                "{input:?}"
            );
        }
    }

    /// `ast.go:30`, and `isUnnamed`'s LENGTH test: `<__>` is named, so it
    /// can be duplicated and `<_>` cannot.
    #[test]
    fn a_repeated_named_capture_is_refused() {
        for (input, name) in [
            ("<a> <a>", "a"),
            ("<a> x <a>", "a"),
            ("<__> x <__>", "__"),
            ("<a> <b> <a>", "a"),
        ] {
            assert_eq!(
                parse_pattern(input),
                Err(PatternError::DuplicateCapture {
                    name: name.to_string()
                }),
                "{input:?}"
            );
        }
        // The unnamed capture never enters the duplicate set, so it may
        // repeat; a different CASE is a different name.
        for input in ["<a> <_> x <_>", "<A> x <a>", "<__> x <_>"] {
            assert!(parse_pattern(input).is_ok(), "{input:?}");
        }
        // …and `<_> x <_>` alone is NoCapture, not a duplicate: the
        // no-capture rule is checked first and `<_>` is not a name.
        assert_eq!(parse_pattern("<_> x <_>"), Err(PatternError::NoCapture));
    }

    /// `ast.go:44` — adjacency, named or not.
    #[test]
    fn two_adjacent_captures_are_refused() {
        for (input, rendered) in [
            ("<a><b>", "<a><b>"),
            ("<a><_>", "<a><_>"),
            ("<_><a>", "<_><a>"),
            ("x <a><b> y", "<a><b>"),
        ] {
            assert_eq!(
                parse_pattern(input),
                Err(PatternError::ConsecutiveCaptures {
                    rendered: rendered.to_string()
                }),
                "{input:?}"
            );
        }
    }

    /// **The rule ORDER is observable**, and `compile_pattern` got the
    /// second of these wrong by validating inside its lex loop:
    /// `<_><_>` is two adjacent captures AND has no named capture, and
    /// the reference reports the second. Reordering the two checks in
    /// [`parse_pattern`] reddens this.
    #[test]
    fn rule_order_is_observable() {
        assert!(matches!(
            parse_pattern("<a><a>"),
            Err(PatternError::ConsecutiveCaptures { .. })
        ));
        assert_eq!(parse_pattern("<_><_>"), Err(PatternError::NoCapture));
    }

    /// `expr.y:32-35` — the only reachable syntax error.
    #[test]
    fn the_empty_pattern_is_the_only_syntax_error() {
        assert_eq!(parse_pattern(""), Err(PatternError::Empty));
        // Everything else lexes: an unterminated `<`, a bare `>`, a NUL,
        // a lone multi-byte rune — all literal text, so they fail the
        // no-capture rule instead and never the parse.
        for input in ["<", ">", "<a", "a>", "<<a>>", "\0", "é", "日本"] {
            assert_ne!(parse_pattern(input), Err(PatternError::Empty), "{input:?}");
        }
    }

    /// **`<X>` is a capture IFF `X` matches `^[A-Za-z_][A-Za-z0-9_]*$`**
    /// (`lexer.rl:33 @ v3.7.4`), asserted exhaustively over every string
    /// of length 1–3 from `{a, Z, _, 0, 9, -, space, é, .}` — 819 names,
    /// generated here rather than listed, with the Ragel rule as the
    /// oracle so the expectation does not consult our own lexer.
    ///
    /// **This test was RED at `5d91ef1`**: `compile_pattern` tested
    /// `!name.is_empty() && all(alnum || '_')`, so `<1a>` became a
    /// capture named `1a` and the query answered `_1a="9"` — a label the
    /// user never wrote. It is the check that ties this module to the
    /// second defect the issue closes, and it is what makes "we did not
    /// implement `IsValidLabelName`" observable from the other side: any
    /// label-name validation hiding in this module, under any spelling,
    /// would reject one of the 819.
    #[test]
    fn the_capture_charset_is_exactly_the_references() {
        const ALPHABET: [char; 9] = ['a', 'Z', '_', '0', '9', '-', ' ', 'é', '.'];
        // **The oracle is spelled DIFFERENTLY from the implementation on
        // purpose.** It was `c.is_ascii_alphabetic() || c == '_'`, the
        // same expression `lex_identifier` uses — and a break test that
        // edited "the charset" moved both at once and stayed green,
        // which is a gate that cannot fail for the property it names.
        // Written out as the Ragel rule's literal ranges
        // (`lexer.rl:33`: `(alpha|'_') (alnum|'_')*`), it cannot be
        // edited together with the implementation by accident.
        let is_reference_identifier = |s: &str| {
            let head = |c: char| matches!(c, 'a'..='z' | 'A'..='Z' | '_');
            let tail = |c: char| matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '_');
            let mut cs = s.chars();
            match cs.next() {
                Some(c) if head(c) => {}
                _ => return false,
            }
            cs.all(tail)
        };
        let mut checked = 0usize;
        let mut names: Vec<String> = Vec::new();
        for a in ALPHABET {
            names.push(a.to_string());
            for b in ALPHABET {
                names.push(format!("{a}{b}"));
                for c in ALPHABET {
                    names.push(format!("{a}{b}{c}"));
                }
            }
        }
        assert_eq!(names.len(), 9 + 81 + 729);
        for name in &names {
            // A trailing `<aaaa>` — four characters, so no generated name
            // can collide with it — keeps the pattern well formed
            // whichever way the probe goes, so the check is on the TOKENS
            // and not on a verdict that the no-capture rule would decide
            // for unrelated reasons.
            let input = format!("<{name}> x <aaaa>");
            let toks = parse_pattern(&input).unwrap_or_else(|e| panic!("{input:?}: {e:?}"));
            if is_reference_identifier(name) {
                if name == "_" {
                    assert_eq!(toks.first(), Some(&PatternTok::Discard), "{input:?}");
                } else {
                    assert_eq!(
                        toks.first(),
                        Some(&PatternTok::Capture(name.clone())),
                        "{input:?}"
                    );
                }
            } else {
                // Not an identifier: `<name>` is LITERAL TEXT, which is
                // the whole of the second defect this issue closes.
                let Some(PatternTok::Literal(lit)) = toks.first() else {
                    panic!("{input:?}: expected literal text, got {:?}", toks.first());
                };
                assert!(lit.starts_with('<'), "{input:?}: {lit:?}");
            }
            checked += 1;
        }
        assert_eq!(checked, 819);
    }

    /// The accepted shapes still tokenise the way `walk_pattern`
    /// expects: literals merge, captures do not.
    #[test]
    fn accepted_patterns_tokenise_as_before() {
        assert_eq!(
            names("<method> <path> <status>"),
            ["method", "path", "status"]
        );
        assert_eq!(names("<_> <method> <path>"), ["method", "path"]);
        assert_eq!(names("level=<level> msg"), ["level"]);
        assert_eq!(
            parse_pattern("a<b>c").expect("accepted"),
            vec![
                PatternTok::Literal("a".to_string()),
                PatternTok::Capture("b".to_string()),
                PatternTok::Literal("c".to_string()),
            ]
        );
        // A `<` that opens no identifier merges into the literal run.
        assert_eq!(
            parse_pattern("<a<b>").expect("accepted"),
            vec![
                PatternTok::Literal("<a".to_string()),
                PatternTok::Capture("b".to_string()),
            ]
        );
    }
}
