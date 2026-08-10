//! Issue #246: the accept/reject surface of every LogQL construct that
//! carries a user regular expression, measured as a matrix and checked in
//! so it can be re-run.
//!
//! # What this file is, and what it deliberately is not
//!
//! #246 was filed to reproduce Go's `regexp` **error prose** byte for
//! byte. The owner rescoped it twice — 2026-07-26 ("the parity bar is
//! meaningful consumer impact, not byte identity") and 2026-08-08 ("no
//! translation table") — and this file is what the second ruling asked
//! for instead: the accept/reject decision and the status, measured
//! through the real chain on both sides. There is **no** message
//! catalogue and **no** per-`ErrorCode` mapping here, and none is owed.
//! Two measurements license that:
//!
//! * **Nothing branches on the text.** In Loki v3.7.4 itself,
//!   `grep -rn "error parsing regexp" --include=*.go` finds 8 hits: 4 in
//!   `vendor/` (the error constructors) and **all 4 non-vendor hits in its
//!   own `_test.go` files** — `clients/pkg/logentry/stages/{drop,regex,
//!   replace}_test.go` and `pkg/logql/syntax/parser_test.go:392 @ v3.7.4`.
//!   The reference pins its own wording in its own unit tests; no client
//!   code reads it.
//! * **Byte parity is unreachable without porting Go's parser**, which
//!   was refused on #331. Go's `Error.Expr` is the offending SUB-TOKEN,
//!   not the pattern — measured on the pinned container, `{app=~"[z-a]"}`
//!   answers ``invalid character class range: `z-a` `` and
//!   `{app=~"[[:foo:]]"}` answers ``: `[:foo:]` `` — so reproducing it
//!   means reproducing that parser's cursor
//!   (`vendor/github.com/grafana/regexp/syntax/parse.go:16-22 @ v3.7.4`
//!   builds the message from `Code` plus `Expr`). And `label_replace`
//!   quotes the ANCHORED form where every other site quotes the bare one,
//!   so even the quoted-expression rule is per-site.
//!
//! What IS in scope, and what this file pins: **the status**. Every LogQL
//! regex rejection is `400` on both sides, ours through
//! `PipelineError::BadRegex` → `ReadError::PipelineInvalid`
//! (`src/logql/error.rs`) → `StatusCode::BAD_REQUEST`
//! (`pulsus-server/src/logs_api/error.rs`). Where the two sides disagree
//! about the DECISION they are enumerated below and owned by **#400**;
//! this file refuses none of them and fixes none of them, it makes them
//! breakable.
//!
//! # Three ways to build this matrix so that it measures nothing
//!
//! Each of these was hit during the scouting run, and each alone produces
//! a table that passes while pinning nothing. They are in the file as
//! built:
//!
//! 1. **`| regexp "P"` masks nearly the whole pattern set.**
//!    `NewRegexpParser` requires a named capture
//!    (`pkg/logql/log/parser.go:299-301,317-319 @ v3.7.4`) and PulsusDB
//!    requires it too (`pipeline.rs`'s `ParserStage::Regexp` arm), so a
//!    probe pattern without one is a `400` on both sides for a reason
//!    that has nothing to do with the regex. Measured: the bare position
//!    disagrees on **0 of 42** patterns. The matrix therefore probes
//!    `| regexp "(?P<c>{P})"`, and the bare form is kept in [`MASKED`]
//!    with a test that it still masks.
//! 2. **`{app!~"P"}` masks everything.** A selector with no positive
//!    matcher is refused outright on both sides — the reference with
//!    `queries require at least one regexp or equality matcher…`, we with
//!    `ReadError::EmptyMatcherSet`. Measured: **0 of 42** disagree. The
//!    negated-selector position is spelled `{app="x", host!~"{P}"}`.
//! 3. **The reference's line filter is a pipeline-BUILD error, so the
//!    live leg's window is load-bearing.** Measured on the pinned
//!    container: `{app="x"} |~ "("` is `400` over a window ending now and
//!    **`200`** over one ending 30 days ago, while `{app=~"("}` is `400`
//!    in both. A stale window would score the reference as accepting a
//!    whole half of this matrix. [`live_matrix_against_the_reference`]
//!    ends its window at `now`. Ledgered as
//!    `malformed-query-refused-in-every-window` (#380).
//!
//! # The tests
//!
//! * [`pulsus_verdicts_match_the_committed_table`] — hermetic, the full
//!   cross product, PulsusDB's verdict taken at parse → plan → the
//!   pipeline compile `exec` runs before any I/O.
//! * [`live_matrix_against_the_reference`] — gated on
//!   `PULSUSDB_LOGQL_DIFF_URL`, re-measures every point's status against
//!   the pinned container so the reference column cannot rot.
//! * [`the_divergence_set_is_exactly_the_committed_enumeration`] — the
//!   disagreements are derived from the matrix and matched against
//!   [`DIVERGENCES`], each row carrying its owner.
//! * [`every_go_regexp_error_code_is_accounted_for`] — the pattern set is
//!   enumerated from the REFERENCE's error taxonomy, not from ours.
//! * [`the_masked_positions_pin_nothing_and_this_is_measured`] —
//!   the two masks above, asserted still masking.
//! * [`the_regex_compile_sites_are_enumerated_from_the_source`] — the
//!   site list is derived from `src/logql/**` **recursively**, which is
//!   how the two `src/logql/template/` sites are visible at all.
//! * [`the_template_regex_boundary_does_not_match_the_reference`] and
//!   [`live_template_axis_against_the_reference`] — the template regex
//!   functions, whose accept/reject boundary a comment in `funcs.rs` and
//!   a ledger row both claimed matched the reference. It does not: 18 of
//!   20 probes disagree, and two of them answer with a different STRING
//!   rather than merely a different verdict.
//! * [`every_go_regexp_error_code_is_accounted_for`] and
//!   [`live_reference_error_codes_are_exactly_the_covered_set`] — the
//!   census, and the half that makes it bite.
//!
//! # What this matrix cannot see
//!
//! **It scores VERDICTS.** Every position, every pattern, every committed
//! cell, the whole live leg and every divergence class answer one
//! question: is the query served, or is it a 400. That means a pattern
//! both sides ACCEPT while reading it as two different patterns — a wrong
//! answer, and the worse kind of divergence — moves no cell here and
//! reddens nothing.
//!
//! This is not a hypothetical limit. `engine_dir_b_invalid_utf8_escape`
//! shipped with a false mechanism ("our lexer decodes `\xff` to U+00FF")
//! and survived a review round, because no check in this file could
//! contradict it. What our lexer actually does is drop the backslash
//! (`pulsus-logql/src/lexer.rs:322-331`), so the pattern becomes the three
//! ASCII characters `xff` while the reference sees one 0xFF byte — and at
//! the five positions where BOTH sides serve it, the two engines match
//! different lines with every cell agreeing.
//!
//! [`the_parsed_pattern_value_is_committed_where_the_escape_changes_it`]
//! is the one check here with a value in it, and it covers exactly the
//! patterns whose LogQL escape is the construct under test. Anything
//! broader — what a pattern MATCHES, rather than whether it compiles —
//! belongs to the value-differential suites, not here. **If you add a
//! pattern hoping to pin a meaning, this file is the wrong place: it will
//! report agreement.**
//!
//! **And the escape family this matrix touches is only half of one.** An
//! escape Go's string grammar DEFINES and this lexer does not (`\xff`,
//! `\101`) is the value half above, and `invalid_utf8` is its one probe
//! here. An escape Go does **not** define (`\d`, `\w`, `\0`, `\q`) is an
//! ordinary accept-surface divergence — measured, `{app=~"\d+"}` is
//! `400 parse error at line 1, col 7: invalid char escape` on the
//! reference and parses to the pattern `d+` here — and **it is in none of
//! this file's points and none of its divergence classes**, because every
//! **other** pattern is a regex body that [`logql_quote`] escapes on the
//! way in, so it cannot carry a string escape at all. The exception is
//! `invalid_utf8`, which [`Pattern::literal`] passes through unescaped by
//! design — and it is not in this half either, because `\x` is an escape
//! Go DEFINES. **Both clauses are load-bearing**: the two prose copies of
//! this sentence dropped the "other" and became false, which is exactly
//! what made the exemption invisible. Both halves are #400's; do not
//! widen this file to cover the reject half.
//!
//! **If this file has to be split**, the seam is the template axis:
//! [`TEMPLATE_AXIS`] and its two tests have a different verdict TYPE from
//! everything else here (a `200` carrying `__error__`, never a status)
//! and share only [`logql_quote`]. Nothing else divides cleanly — the
//! positions, the patterns, the exceptions and the divergence
//! enumeration are one measurement.

use std::process::Command;

use pulsus_logql::parse;
use pulsus_read::logql::pipeline::CompiledPipeline;
use pulsus_read::logql::plan::{MetricNode, MetricNodeScc, Plan};
use pulsus_read::logql::{Direction, PlanCtx, QueryParams, QuerySpec, plan};

// ---------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------

/// What a user sees: the query is served, or it is a 400.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    Accept,
    Reject,
}

/// How a position answers, relative to the pattern's own column.
///
/// This is a COMPRESSION of the captured matrix, and it is checked rather
/// than assumed: [`live_matrix_against_the_reference`] puts every point
/// individually, and [`pulsus_verdicts_match_the_committed_table`]
/// computes every point through the real chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rule {
    /// The verdict is the pattern's own committed column.
    PerPattern,
    /// Every pattern is accepted here, whatever its column.
    AcceptsEverything,
    /// Every pattern is rejected here, whatever its column.
    RejectsEverything,
}

/// One query position. `{P}` is the pattern, LogQL-quoted.
struct Position {
    id: &'static str,
    template: &'static str,
    /// What this position exists to reach, and what would be missed if it
    /// were dropped.
    why: &'static str,
    reference: Rule,
    pulsus: Rule,
}

/// A pattern body. The generated forms are generated rather than written
/// out; [`Body::LogqlSource`] is the one form that is NOT a regex.
enum Body {
    Literal(&'static str),
    /// `n` nested groups around a literal `a` — a LIMIT probe, not a
    /// construct, and its verdict depends on the anchoring wrapper.
    NestedGroups(usize),
    /// `s` repeated `n` times — the other LIMIT probe.
    Repeated(&'static str, usize),
    /// `n` copies of `s` joined by `|` — the SIZE probe (issue #291).
    ///
    /// Not `Repeated("\\p{L}|", n)`, and the difference was measured
    /// rather than reasoned: a trailing `|` makes the alternation
    /// empty-compatible, and the reference then refuses the two selector
    /// positions with `queries require at least one regexp or equality
    /// matcher that does not have an empty-compatible value` — a
    /// different rule, which would have masked the size verdict at
    /// exactly the positions this row exists to measure.
    Alternated(&'static str, usize),
    /// **The text goes into the LogQL string literal verbatim, unescaped.**
    /// Every other body is a REGEX that gets LogQL-quoted on the way in;
    /// this one is the literal's own SOURCE, because the construct under
    /// test is what the string escape decodes to. `\xff` here is four
    /// ASCII characters in the query — which the query scanner accepts —
    /// that Go's unquoting turns into one 0xFF byte inside the pattern.
    ///
    /// Getting this wrong is how the first version of this file recorded
    /// `ErrInvalidUTF8` as unreachable: it probed a RAW `%FF` byte in the
    /// `query=` parameter, which `query_scanner.go:264 @ v3.7.4` refuses
    /// before the regex parser is reached. That measurement was true and
    /// its domain was one route into the rule, not the rule.
    LogqlSource(&'static str),
}

/// One pattern. `reference` and `pulsus` are CAPTURED columns, not
/// derived: the first from the pinned container, the second from
/// [`pulsus_verdict`] on this tree.
struct Pattern {
    id: &'static str,
    body: Body,
    /// The `ErrorCode` constant of
    /// `vendor/github.com/grafana/regexp/syntax/parse.go:28-48 @ v3.7.4`
    /// this pattern raises in the reference, read off the CONTAINER's own
    /// 400 body (`error parsing regexp: <code>: \`<expr>\``) at the
    /// `sel_re` position; `None` when the reference accepts it.
    go_code: Option<&'static str>,
    reference: Verdict,
    pulsus: Verdict,
}

/// A single (position, pattern) cell that is NOT the pattern's column and
/// is not the position's rule either. CAPTURED; every row is asserted to
/// be a real exception by
/// [`every_committed_exception_is_a_real_exception`], so a stale one
/// cannot sit here unnoticed.
struct Exception {
    position: &'static str,
    pattern: &'static str,
    reference: Option<Verdict>,
    pulsus: Option<Verdict>,
    why: &'static str,
}

/// A class of (position, pattern) points where the two sides disagree.
///
/// Rows may OVERLAP — a Direction-B pattern at `variants_variant_side`
/// is reached by two independent mechanisms — so the test asserts the
/// UNION equals the measured disagreement set and, separately, that no
/// row is redundant (dropping any one row shrinks the union).
struct Divergence {
    id: &'static str,
    /// What PulsusDB does where the reference does the other.
    pulsus: Verdict,
    patterns: &'static [&'static str],
    positions: &'static [&'static str],
    /// The issue that owns the fix. Never empty.
    owner: &'static str,
    why: &'static str,
}

// ---------------------------------------------------------------------
// Positions
// ---------------------------------------------------------------------

const POSITIONS: &[Position] = &[
    Position {
        id: "sel_re",
        template: r#"{app=~"{P}"}"#,
        why: "the stream selector's `=~`; the reference compiles it at PARSE, through \
              `mustNewMatcher` (`pkg/logql/syntax/ast.go:1102-1108 @ v3.7.4`) into \
              `NewFastRegexMatcher`. Ours is validated where it is rendered into SQL \
              (`escape.rs`'s `ch_regex_anchored_checked`).",
        reference: Rule::PerPattern,
        pulsus: Rule::PerPattern,
    },
    Position {
        id: "sel_nre",
        template: r#"{app="x", host!~"{P}"}"#,
        why: "the selector's `!~`. Spelled WITH a positive matcher on purpose: `{app!~\"P\"}` \
              alone is refused on both sides for having no positive matcher, which masks the \
              whole pattern set — see `MASKED`.",
        reference: Rule::PerPattern,
        pulsus: Rule::PerPattern,
    },
    Position {
        id: "line_re",
        template: r#"{app="x"} |~ "{P}""#,
        why: "the line filter, the one construct the reference raises at pipeline BUILD rather \
              than parse (`pkg/logql/log/filter.go:646 @ v3.7.4`), hence window-dependent there.",
        reference: Rule::PerPattern,
        pulsus: Rule::PerPattern,
    },
    Position {
        id: "line_nre",
        template: r#"{app="x"} !~ "{P}""#,
        why: "the line filter's negated form. Kept as its own position because `!~` reaches a \
              different `LineMatcher` arm and a different SQL rendering than `|~`, so a \
              validation added to one and not the other would show up only here.",
        reference: Rule::PerPattern,
        pulsus: Rule::PerPattern,
    },
    Position {
        id: "line_after_line_format",
        template: r#"{app="x"} | line_format "{{.x}}" |~ "{P}""#,
        why: "the same filter on the OTHER side of docs/api.md \u{a7}9.1's split: after a \
              `line_format` PulsusDB compiles it in process instead of pushing it into \
              ClickHouse, so the compiling engine differs from `line_re`'s.",
        reference: Rule::PerPattern,
        pulsus: Rule::PerPattern,
    },
    Position {
        id: "regexp_named",
        template: r#"{app="x"} | regexp "(?P<c>{P})""#,
        why: "the `| regexp` parser. Wrapped in a named capture because BOTH sides refuse a \
              pattern without one, for a reason that has nothing to do with the regex — the \
              bare form is in `MASKED`.",
        reference: Rule::PerPattern,
        pulsus: Rule::PerPattern,
    },
    Position {
        id: "labelfilter_re",
        template: r#"{app="x"} | logfmt | a=~"{P}""#,
        why: "a label filter over a parser-produced label: compiled in process, anchored, on \
              both sides.",
        reference: Rule::PerPattern,
        pulsus: Rule::PerPattern,
    },
    Position {
        id: "labelfilter_nre",
        template: r#"{app="x"} | logfmt | a!~"{P}""#,
        why: "the label filter's negated form: `MatchOp::Nre` is a separate arm of the same \
              `compile_anchored_regex` call, and a rule applied to `=~` alone would pass every \
              other position in this table.",
        reference: Rule::PerPattern,
        pulsus: Rule::PerPattern,
    },
    Position {
        id: "drop",
        template: r#"{app="x"} | drop a=~"{P}""#,
        why: "`| drop`'s matcher. A separate compile site from the label filter's \
              (`compile_drop_keep`, its own function), so neither position covers the other.",
        reference: Rule::PerPattern,
        pulsus: Rule::PerPattern,
    },
    Position {
        id: "keep",
        template: r#"{app="x"} | keep a=~"{P}""#,
        why: "`| keep`'s matcher, the second `compile_drop_keep` caller. Present because two \
              callers of one helper are two places a future change could touch one and not the \
              other.",
        reference: Rule::PerPattern,
        pulsus: Rule::PerPattern,
    },
    Position {
        id: "metric_line",
        template: r#"count_over_time({app="x"} |~ "{P}" [5m])"#,
        why: "the same line filter inside a range aggregation — a different plan shape \
              (`Plan::Metric`) and therefore a different set of compile calls.",
        reference: Rule::PerPattern,
        pulsus: Rule::PerPattern,
    },
    Position {
        id: "metric_binary",
        template: r#"count_over_time({app="x"} |~ "{P}" [5m]) + count_over_time({app="x"}[5m])"#,
        why: "`Plan::MetricBinary`: the malformed side is one leaf of a binary tree, so the \
              verdict depends on every leaf being walked rather than just the first.",
        reference: Rule::PerPattern,
        pulsus: Rule::PerPattern,
    },
    Position {
        id: "label_replace",
        template: r#"label_replace(rate({app="x"}[5m]),"d","$1","s","{P}")"#,
        why: "the ONE LogQL site that routes through `pulsus_promql::re2_pattern_to_rust` \
              (`plan.rs`'s `LabelReplaceSpec::compile`), so the brace and class-difference \
              forms answer differently here than at every other position — the exceptions \
              below.",
        reference: Rule::PerPattern,
        pulsus: Rule::PerPattern,
    },
    Position {
        id: "variants_variant_side",
        template: r#"variants(count_over_time({app="x"} |~ "{P}" [5m])) of ({app="x"}[5m])"#,
        why: "a `variants(...)` VARIANT's own pipeline, with a PUSHABLE line filter. The \
              reference builds it purely to count the extractors \
              (`pkg/logql/evaluator.go:1417,1422 @ v3.7.4`) and so refuses a malformed stage \
              there. PulsusDB validates the variant's discarded prefix in `VariantSpec::try_new` \
              (`plan.rs:2641`) — but through `CompiledPipeline::compile`, whose `compile_stage` \
              returns `Ok(None)` for a PUSHABLE line filter (`pipeline.rs:986-996`) before it \
              reaches `compile_regex` at `:1013`; the regex of a pushable filter is validated on \
              the SQL-rendering path instead, and a discarded prefix renders no SQL. Hence \
              `AcceptsEverything` here, and a divergence row. The escape is the PUSHDOWN, not \
              the construct — `variants_variant_after_line_format` is the same filter with \
              pushdown cleared, and it agrees.",
        reference: Rule::PerPattern,
        pulsus: Rule::AcceptsEverything,
    },
    Position {
        id: "variants_variant_after_line_format",
        template: r#"variants(count_over_time({app="x"} | line_format "{{.x}}" |~ "{P}" [5m])) of ({app="x"}[5m])"#,
        why: "the SAME variant position with the line filter after a `line_format`, which \
              clears `seen_line_format`'s pushdown so `compile_stage` compiles the filter in \
              process. It agrees with the reference at every pattern, which is what bounds the \
              row above: the gap is pushable line filters, not line filters. Without this \
              position nothing in the matrix would contradict the wider wording, and the first \
              version of this file stated it that way.",
        reference: Rule::PerPattern,
        pulsus: Rule::PerPattern,
    },
    Position {
        id: "variants_common_side",
        template: r#"variants(count_over_time({app="x"}[5m])) of ({app="x"} |~ "{P}" [5m])"#,
        why: "a `variants(...)` COMMON pipeline. The reference's querier swallows the common \
              build error and hands back an empty 200 in EVERY window — the stronger form of \
              the `malformed-query-refused-in-every-window` class (#380), already ledgered \
              for `| logfmt` on #247 and measured again here for a line filter.",
        reference: Rule::AcceptsEverything,
        pulsus: Rule::PerPattern,
    },
];

/// Positions that answer the same way for (almost) every pattern, and so
/// pin nothing about the divergence classes. They are here, with their
/// measurement, so nobody re-adds them to [`POSITIONS`] believing they
/// cover something — and
/// [`the_masked_positions_pin_nothing_and_this_is_measured`] fails if
/// either stops masking, at which point it should be promoted.
const MASKED: &[Position] = &[
    Position {
        id: "MASKED_sel_nre_alone",
        template: r#"{app!~"{P}"}"#,
        why: "a selector with no positive matcher is refused before the regex is looked at: \
              the reference with `queries require at least one regexp or equality matcher \
              that does not have an empty-compatible value`, PulsusDB with \
              `ReadError::EmptyMatcherSet`. Measured: 0 of 42 points disagree.",
        reference: Rule::RejectsEverything,
        pulsus: Rule::RejectsEverything,
    },
    Position {
        id: "MASKED_regexp_bare",
        template: r#"{app="x"} | regexp "{P}""#,
        why: "`| regexp` without a named capture is refused on both sides for that reason \
              alone (`errMissingCapture`, `pkg/logql/log/parser.go:299-301,317-319 @ v3.7.4`). \
              Measured: 0 of 42 disagree — 41 reject-on-both, and the single pattern that \
              carries a named capture of its own (`ok_named`) accepts on both.",
        reference: Rule::RejectsEverything,
        pulsus: Rule::RejectsEverything,
    },
];

// ---------------------------------------------------------------------
// Patterns
// ---------------------------------------------------------------------

/// The nesting depth `nest_999` uses. Under 1000 so that the reference's
/// unanchored sites are inside `maxHeight`
/// (`vendor/github.com/grafana/regexp/syntax/parse.go:93 @ v3.7.4`) and
/// its `^(?s:…)$`-wrapping sites are not.
const NEST_DEPTH: usize = 999;

const PATTERNS: &[Pattern] = &[
    // --- well-formed on both: without these the matrix could pass by
    //     rejecting everything.
    p("ok_dotstar", "a.*b", None, Verdict::Accept, Verdict::Accept),
    p(
        "ok_named",
        "(?P<n>a)",
        None,
        Verdict::Accept,
        Verdict::Accept,
    ),
    p(
        "ok_unicode_class",
        r"\p{L}",
        None,
        Verdict::Accept,
        Verdict::Accept,
    ),
    p("ok_class", "[a-z]+", None, Verdict::Accept, Verdict::Accept),
    p(
        "ok_repeat",
        "a{2,3}",
        None,
        Verdict::Accept,
        Verdict::Accept,
    ),
    p(
        "ok_flag_i",
        "(?i)abc",
        None,
        Verdict::Accept,
        Verdict::Accept,
    ),
    p("ok_alt", "a|b", None, Verdict::Accept, Verdict::Accept),
    p("ok_digit", r"\d+", None, Verdict::Accept, Verdict::Accept),
    p(
        "ok_negclass",
        "[^a]",
        None,
        Verdict::Accept,
        Verdict::Accept,
    ),
    p(
        "ok_groups",
        "(a)(b)",
        None,
        Verdict::Accept,
        Verdict::Accept,
    ),
    // --- one per reachable Go `ErrorCode`; both sides reject.
    p(
        "missing_paren",
        "(",
        Some("ErrMissingParen"),
        Verdict::Reject,
        Verdict::Reject,
    ),
    p(
        "missing_bracket",
        "[a",
        Some("ErrMissingBracket"),
        Verdict::Reject,
        Verdict::Reject,
    ),
    p(
        "missing_repeat_arg",
        "*a",
        Some("ErrMissingRepeatArgument"),
        Verdict::Reject,
        Verdict::Reject,
    ),
    p(
        "trailing_backslash",
        "a\\",
        Some("ErrTrailingBackslash"),
        Verdict::Reject,
        Verdict::Reject,
    ),
    p(
        "unexpected_paren",
        "a)b",
        Some("ErrUnexpectedParen"),
        Verdict::Reject,
        Verdict::Reject,
    ),
    p(
        "bad_char_range",
        "[z-a]",
        Some("ErrInvalidCharRange"),
        Verdict::Reject,
        Verdict::Reject,
    ),
    p(
        "bad_escape",
        r"\q",
        Some("ErrInvalidEscape"),
        Verdict::Reject,
        Verdict::Reject,
    ),
    p(
        "bad_named_capture",
        "(?P<>a)",
        Some("ErrInvalidNamedCapture"),
        Verdict::Reject,
        Verdict::Reject,
    ),
    p(
        "lookahead",
        "(?!a)",
        Some("ErrInvalidPerlOp"),
        Verdict::Reject,
        Verdict::Reject,
    ),
    p(
        "repeat_size_inverted",
        "a{2,1}",
        Some("ErrInvalidRepeatSize"),
        Verdict::Reject,
        Verdict::Reject,
    ),
    p(
        "adjacent_repeats",
        "a{100}{100}{100}",
        Some("ErrInvalidRepeatOp"),
        Verdict::Reject,
        Verdict::Reject,
    ),
    // A NESTED repeat does not reach `ErrLarge`: the repeat-PRODUCT cap
    // fires first (`repeatIsValid(re, 1000)`, parse.go:434-437), so this
    // is `ErrInvalidRepeatSize`. Kept because the first version of this
    // file used it to argue `ErrLarge` was unreachable — a true
    // measurement over one route, and `too_large` below is the route it
    // did not try.
    p(
        "repeat_product",
        "((a{100}){100}){100}",
        Some("ErrInvalidRepeatSize"),
        Verdict::Reject,
        Verdict::Reject,
    ),
    p(
        "class_escape_range",
        r"[a-\d]",
        Some("ErrInvalidEscape"),
        Verdict::Reject,
        Verdict::Reject,
    ),
    // `ErrLarge`, REACHED. 4,000 copies of `a{999}` — 24,000 characters,
    // well inside the 131,072-byte query-text cap. Each repeat is under
    // the 1,000 product cap, so `repeatIsValid` passes and `checkSize`'s
    // `maxSize` is what fires: `128<<20 / instSize` with `instSize = 40`
    // is **3,355,443** instructions (33,554,432 is `maxRunes`, the OTHER
    // limit, and the figure the first version of this file quoted for
    // this one). Both sides reject; ours through the Rust crate's own
    // 10 MiB compiled-size limit, which is not the same budget and is
    // recorded as agreement on the verdict only.
    Pattern {
        id: "too_large",
        body: Body::Repeated("a{999}", 4_000),
        go_code: Some("ErrLarge"),
        reference: Verdict::Reject,
        pulsus: Verdict::Reject,
    },
    // `ErrInvalidUTF8`, REACHED — through the string ESCAPE, not a raw
    // byte. See [`Body::LogqlSource`]. The reference rejects it only
    // where the pattern reaches a parser: every `NewFastRegexMatcher`
    // site (the selector, label filters, `drop`/`keep`) short-circuits a
    // plain literal to a string matcher and never parses it at all
    // (`vendor/github.com/prometheus/prometheus/model/labels/regexp.go:56-72
    // @ v3.7.4` — `optimizeAlternatingLiterals` returns before
    // `syntax.Parse`), so those positions serve it. Whether that is a
    // property of the POSITION or of the literal was measured rather than
    // assumed: `{app=~"a\xffb.*"}` is a 400 at the selector and
    // `{app=~"\xff|\xfe"}` is a 200, so it is the literal short-circuit.
    //
    // **PulsusDB serves it everywhere, and NOT because it reads the same
    // pattern.** `scan_double_quoted` (`pulsus-logql/src/lexer.rs:322-331`)
    // handles an unknown escape with `Some(other) => value.push(other)` —
    // it drops the backslash and keeps the `x` — so the pattern the
    // planner receives is the three ASCII characters `xff`, pinned by
    // [`the_parsed_pattern_value_is_committed_where_the_escape_changes_it`].
    // The reference sees one 0xFF byte. So the user's pattern silently
    // becomes a DIFFERENT pattern matching different lines: at the five
    // positions where both sides serve it, the reference matches lines
    // containing byte 0xFF and we match lines containing `xff`. That is a
    // wrong answer, not a lenient accept, and this matrix scores verdicts
    // and cannot see it — see the module docs' "What this matrix cannot
    // see".
    Pattern {
        id: "invalid_utf8",
        body: Body::LogqlSource(r"\xff"),
        go_code: Some("ErrInvalidUTF8"),
        reference: Verdict::Accept,
        pulsus: Verdict::Accept,
    },
    // Issue #291: the SIZE boundary, and the one row whose divergence is
    // deliberate. 10,100 `\p{L}` atoms alternated — 70,700 query bytes,
    // inside #279's 131,072-byte cap. The reference SERVES it at all
    // eighteen positions (captured on the pinned container, 2026-08-09,
    // with 20 lines pushed and verified queryable); PulsusDB refuses it
    // at every position where the pattern is compiled, because the
    // compile-allocation cap (`pulsus_re2::MAX_REGEX_COMPILE_TRANSIENT_BYTES`,
    // 96 MiB) estimates it over budget.
    //
    // **10,100 and not 12,000 — a cost reduction, not the fix.** Any N in
    // the band shows the same divergence, but the reference's cost across
    // it is steeply non-linear, climbing towards its OWN `maxSize` limit
    // at 12,729. 12,000 shipped first and failed the `schema-it` job with
    // `unexpected status 000` — curl never obtaining a status. **Two
    // causes produce that, and neither is about N**: the reference's own
    // 30 s HTTP write timeout (see `live_probe_is_affordable`), and the
    // reference not being ready yet on a freshly created container, which
    // `wait_for_reference` closes. Reproduced
    // by constraining the container (`--cpus 2 --memory 4g`), worst
    // position per run:
    //
    // | N | 2 CPU / 4 GB | 1 CPU / 2 GB |
    // |---|---|---|
    // | 10,014 | **30.08 s, `000`** (cold) | — |
    // | 10,100 | 9.6-18.8 s | 8.9 s |
    // | 11,000 | — | 13.5 s |
    // | 12,000 | **30.2 s, `000`** | — |
    //
    // **Lowering N alone does NOT fix it, and that is the finding:** even
    // 10,014, the very bottom of the band, spikes to 30.08 s on a cold
    // container. No N here is cheap for the reference, because the
    // divergence exists precisely when the pattern is enormous — at the
    // four positions that build a `NewFastRegexMatcher` (the two label
    // filters, `drop`, `keep`) Go pays 10-27 s whatever N is. The other
    // shape the cap newly refuses, `(?i)[\p{L}x20000]`, was measured too
    // and costs 16-27 s at the same positions, so it buys nothing either.
    // What ACTUALLY resolves this row is not a number here at all: the
    // reference cannot answer any of it in more than 30 s, by its own
    // configuration. The live probe moved to `line_re` — 0.6 s here,
    // ~48x margin — and **failed there too on the runner, at 31.45 s**.
    // Three positions, four rounds, one wall. **This row is now pinned
    // from the measurements already taken and is re-probed at NO
    // position** (owner ruling, 2026-08-10); what that costs, what is
    // known and what is still not known are all on
    // `live_probe_is_affordable`. 10,100 is kept because it is the
    // cheapest N that carries the divergence, which still matters to
    // anyone re-measuring by hand.
    //
    // 10,100 sits 87 atoms above OUR boundary of 10,013. That margin is
    // thin deliberately, and it is now the ONLY automatic detector this
    // row has: if the boundary moved (a `regex-syntax` bump changing
    // `\p{L}`'s range count would do it) our verdict flips to `Accept`
    // and `pulsus_verdicts_match_the_committed_table` fails hermetically
    // and loudly. The reference side has no detector at all any more —
    // that is the cost, stated where the number lives.
    Pattern {
        id: "class_alt_over_budget",
        body: Body::Alternated(r"\p{L}", 10_100),
        go_code: None,
        reference: Verdict::Accept,
        pulsus: Verdict::Reject,
    },
    // --- Direction A: PulsusDB rejects, the reference serves.
    p(
        "dup_capture_name",
        "(?P<n>a)(?P<n>b)",
        None,
        Verdict::Accept,
        Verdict::Reject,
    ),
    p(
        "quote_literal",
        r"\Qa*\E",
        None,
        Verdict::Accept,
        Verdict::Reject,
    ),
    p(
        "octal_escape",
        r"\101",
        None,
        Verdict::Accept,
        Verdict::Reject,
    ),
    p(
        "flag_after_atom",
        "a(?i){2}",
        None,
        Verdict::Accept,
        Verdict::Reject,
    ),
    p(
        "dup_flag_s",
        "(?ss:ab)",
        None,
        Verdict::Accept,
        Verdict::Reject,
    ),
    p(
        "empty_flags",
        "(?)a",
        None,
        Verdict::Accept,
        Verdict::Reject,
    ),
    p(
        "brace_word",
        "a{bbb}c",
        None,
        Verdict::Accept,
        Verdict::Reject,
    ),
    p(
        "brace_open_ended",
        "a{,5}",
        None,
        Verdict::Accept,
        Verdict::Reject,
    ),
    p("brace_empty", "a{}", None, Verdict::Accept, Verdict::Reject),
    Pattern {
        id: "nest_999",
        body: Body::NestedGroups(NEST_DEPTH),
        go_code: Some("ErrNestingDepth"),
        reference: Verdict::Reject,
        pulsus: Verdict::Reject,
    },
    // --- Direction B: PulsusDB serves, the reference rejects.
    p(
        "big_u_escape",
        r"\U0001F600",
        Some("ErrInvalidEscape"),
        Verdict::Reject,
        Verdict::Accept,
    ),
    p(
        "perl_R",
        "(?R)a",
        Some("ErrInvalidPerlOp"),
        Verdict::Reject,
        Verdict::Accept,
    ),
    p(
        "flag_x",
        "(?x)a",
        Some("ErrInvalidPerlOp"),
        Verdict::Reject,
        Verdict::Accept,
    ),
    p(
        "nested_star",
        "a**",
        Some("ErrInvalidRepeatOp"),
        Verdict::Reject,
        Verdict::Accept,
    ),
    p(
        "bad_posix_class",
        "[[:foo:]]",
        Some("ErrInvalidCharRange"),
        Verdict::Reject,
        Verdict::Accept,
    ),
    p(
        "unicode_prop_long",
        r"\p{Alphabetic}",
        Some("ErrInvalidCharRange"),
        Verdict::Reject,
        Verdict::Accept,
    ),
    p(
        "repeat_1001",
        "a{1001}",
        Some("ErrInvalidRepeatSize"),
        Verdict::Reject,
        Verdict::Accept,
    ),
    p(
        "class_double_dash",
        "[a--b]",
        Some("ErrInvalidCharRange"),
        Verdict::Reject,
        Verdict::Accept,
    ),
    p(
        "brace_unicode",
        r"\u{263A}",
        Some("ErrInvalidEscape"),
        Verdict::Reject,
        Verdict::Accept,
    ),
];

const fn p(
    id: &'static str,
    body: &'static str,
    go_code: Option<&'static str>,
    reference: Verdict,
    pulsus: Verdict,
) -> Pattern {
    Pattern {
        id,
        body: Body::Literal(body),
        go_code,
        reference,
        pulsus,
    }
}

// ---------------------------------------------------------------------
// Exceptions: the cells that are neither the pattern's column nor the
// position's rule. All CAPTURED.
// ---------------------------------------------------------------------

const EXCEPTIONS: &[Exception] = &[
    // `nest_999` is a LIMIT, and the limit is spent by the wrapper. The
    // reference's `maxHeight` is 1000; every site that compiles
    // `^(?s:…)$` or `^(?:…)$` spends part of it, so 999 nested groups is
    // `ErrNestingDepth` there and inside the budget at the four
    // unanchored line-filter positions. Measured per position, never
    // compressed to "the verdict depends only on the pattern".
    ex_ref(
        "line_re",
        "nest_999",
        Verdict::Accept,
        "unanchored: inside the reference's maxHeight",
    ),
    ex_ref("line_nre", "nest_999", Verdict::Accept, "unanchored"),
    ex_ref(
        "line_after_line_format",
        "nest_999",
        Verdict::Accept,
        "unanchored",
    ),
    ex_ref("metric_line", "nest_999", Verdict::Accept, "unanchored"),
    ex_ref("metric_binary", "nest_999", Verdict::Accept, "unanchored"),
    ex_ref(
        "variants_variant_side",
        "nest_999",
        Verdict::Accept,
        "unanchored",
    ),
    ex_ref(
        "variants_variant_after_line_format",
        "nest_999",
        Verdict::Accept,
        "unanchored",
    ),
    // `invalid_utf8` is the mirror image: the reference REJECTS it only
    // where the pattern reaches a parser. Every `NewFastRegexMatcher`
    // site short-circuits a plain literal before `syntax.Parse`
    // (regexp.go:56-72 @ v3.7.4), so the selector, both label filters and
    // `drop`/`keep` serve it — those are the base column — and the line
    // filter, `| regexp`, `label_replace` and the variant positions,
    // which call the parser directly, refuse it.
    ex_ref(
        "line_re",
        "invalid_utf8",
        Verdict::Reject,
        "the line filter calls `syntax.Parse` directly (`log/filter.go:646 @ v3.7.4`)",
    ),
    ex_ref("line_nre", "invalid_utf8", Verdict::Reject, "as `line_re`"),
    ex_ref(
        "line_after_line_format",
        "invalid_utf8",
        Verdict::Reject,
        "as `line_re`",
    ),
    ex_ref(
        "regexp_named",
        "invalid_utf8",
        Verdict::Reject,
        "`NewRegexpParser` calls `regexp.Compile` directly (`log/parser.go:295 @ v3.7.4`)",
    ),
    ex_ref(
        "metric_line",
        "invalid_utf8",
        Verdict::Reject,
        "as `line_re`",
    ),
    ex_ref(
        "metric_binary",
        "invalid_utf8",
        Verdict::Reject,
        "as `line_re`",
    ),
    ex_ref(
        "label_replace",
        "invalid_utf8",
        Verdict::Reject,
        "`mustNewLabelReplaceExpr` compiles `^(?:…)$` directly (`syntax/ast.go:2225-2233 @ \
         v3.7.4`), and the body quotes that WRAPPED form",
    ),
    ex_ref(
        "variants_variant_side",
        "invalid_utf8",
        Verdict::Reject,
        "as `line_re`",
    ),
    ex_ref(
        "variants_variant_after_line_format",
        "invalid_utf8",
        Verdict::Reject,
        "as `line_re`",
    ),
    Exception {
        position: "regexp_named",
        pattern: "dup_capture_name",
        reference: Some(Verdict::Reject),
        pulsus: None,
        why: "not a regex verdict: the reference's own `NewRegexpParser` refuses a repeated \
              extracted label name (`duplicate extracted label name 'n'`, \
              `pkg/logql/log/parser.go:309-311 @ v3.7.4`). Its vendored regex parser has no \
              duplicate-name check at all — `grep -n duplicate \
              vendor/github.com/grafana/regexp/syntax/parse.go @ v3.7.4` finds only an \
              unrelated comment — which is why the pattern is served at every other position.",
    },
    Exception {
        position: "MASKED_regexp_bare",
        pattern: "ok_named",
        reference: Some(Verdict::Accept),
        pulsus: Some(Verdict::Accept),
        why: "the one pattern in the set that carries a named capture of its own, so the \
              missing-capture refusal that masks this position does not apply to it.",
    },
    // `label_replace` is the single LogQL site that translates the
    // pattern before compiling it (`re2_pattern_to_rust`), so it answers
    // differently from every other position in BOTH directions.
    ex_pulsus(
        "label_replace",
        "brace_word",
        Verdict::Accept,
        "the rewrite escapes the braces",
    ),
    ex_pulsus(
        "label_replace",
        "brace_open_ended",
        Verdict::Accept,
        "the rewrite escapes the braces",
    ),
    ex_pulsus(
        "label_replace",
        "brace_empty",
        Verdict::Accept,
        "the rewrite escapes the braces",
    ),
    ex_pulsus(
        "label_replace",
        "class_double_dash",
        Verdict::Reject,
        "the rewrite gives `[a--b]` RE2's meaning, which the Rust crate then refuses — so this \
         is the one LogQL position that AGREES with the reference on it",
    ),
    ex_pulsus(
        "label_replace",
        "brace_unicode",
        Verdict::Reject,
        "the rewrite escapes `\\u`, which the Rust crate then refuses — agreement here, \
         divergence everywhere else",
    ),
];

const fn ex_ref(
    position: &'static str,
    pattern: &'static str,
    reference: Verdict,
    why: &'static str,
) -> Exception {
    Exception {
        position,
        pattern,
        reference: Some(reference),
        pulsus: None,
        why,
    }
}

const fn ex_pulsus(
    position: &'static str,
    pattern: &'static str,
    pulsus: Verdict,
    why: &'static str,
) -> Exception {
    Exception {
        position,
        pattern,
        reference: None,
        pulsus: Some(pulsus),
        why,
    }
}

// ---------------------------------------------------------------------
// The divergence enumeration
// ---------------------------------------------------------------------

/// The patterns both sides reject. Used by the two POSITION-scoped
/// divergence rows, where the pattern plays no part.
const BOTH_REJECT: &[&str] = &[
    "missing_paren",
    "missing_bracket",
    "missing_repeat_arg",
    "trailing_backslash",
    "unexpected_paren",
    "bad_char_range",
    "bad_escape",
    "bad_named_capture",
    "lookahead",
    "repeat_size_inverted",
    "adjacent_repeats",
    "repeat_product",
    "class_escape_range",
    "too_large",
];

const DIVERGENCES: &[Divergence] = &[
    // Issue #291: the compile-allocation cap. The ONLY divergence in this
    // file that is deliberate rather than owned as a defect — hence
    // `owner: "#291"` and not `#400`.
    Divergence {
        id: "regex_compile_budget",
        pulsus: Verdict::Reject,
        patterns: &["class_alt_over_budget"],
        positions: &[
            "sel_re",
            "sel_nre",
            "line_re",
            "line_nre",
            "line_after_line_format",
            "regexp_named",
            "labelfilter_re",
            "labelfilter_nre",
            "drop",
            "keep",
            "metric_line",
            "metric_binary",
            "label_replace",
            "variants_variant_after_line_format",
            "variants_common_side",
        ],
        owner: "#291",
        why: "the compile-allocation cap, and the one row here that is a DECISION rather than \
              a defect: bounding what compiling a pattern may allocate refuses class-heavy \
              patterns sooner than the reference does — the band where it serves and we \
              refuse is 10,014..12,728 alternated Unicode-class atoms, both endpoints \
              bisected one atom at a time rather than projected. Matching its \
              boundary would mean porting Go's `maxRunes`/`maxSize`/`maxHeight`, which admit \
              128 MB parse trees — the unboundedness the cap exists to remove. Ledgered as \
              `regex-compile-budget`; docs/api.md \u{a7}9.4. `variants_variant_side` is \
              absent for the reason every other row omits it: a PUSHABLE line filter is not \
              compiled at all there, so both sides serve it and the budget never runs.",
    },
    Divergence {
        id: "engine_dir_a_perl_and_flag_forms",
        pulsus: Verdict::Reject,
        patterns: &[
            "quote_literal",
            "octal_escape",
            "flag_after_atom",
            "dup_flag_s",
            "empty_flags",
        ],
        positions: &[
            "sel_re",
            "sel_nre",
            "line_re",
            "line_nre",
            "line_after_line_format",
            "regexp_named",
            "labelfilter_re",
            "labelfilter_nre",
            "drop",
            "keep",
            "metric_line",
            "metric_binary",
            "label_replace",
            "variants_variant_after_line_format",
            "variants_common_side",
        ],
        owner: "#400",
        why: "constructs Go's parser accepts and the Rust crate does not; the `re2_pattern_to_rust` \
              rewrite does not change them either, so `label_replace` is affected too. \
              docs/api.md \u{a7}9.4.",
    },
    Divergence {
        id: "engine_dir_a_brace_forms",
        pulsus: Verdict::Reject,
        patterns: &["brace_word", "brace_open_ended", "brace_empty"],
        positions: &[
            "sel_re",
            "sel_nre",
            "line_re",
            "line_nre",
            "line_after_line_format",
            "regexp_named",
            "labelfilter_re",
            "labelfilter_nre",
            "drop",
            "keep",
            "metric_line",
            "metric_binary",
            "variants_variant_after_line_format",
            "variants_common_side",
        ],
        owner: "#400",
        why: "literal braces. `label_replace` is absent because its rewrite escapes them — the \
              partial fix #331 deferred, applied at one site out of thirteen.",
    },
    Divergence {
        id: "engine_dir_a_duplicate_capture_name",
        pulsus: Verdict::Reject,
        patterns: &["dup_capture_name"],
        positions: &[
            "sel_re",
            "sel_nre",
            "line_re",
            "line_nre",
            "line_after_line_format",
            "labelfilter_re",
            "labelfilter_nre",
            "drop",
            "keep",
            "metric_line",
            "metric_binary",
            "label_replace",
            "variants_variant_after_line_format",
            "variants_common_side",
        ],
        owner: "#400",
        why: "NOT one of the eighteen classes #400 was filed with — found by this matrix. The \
              reference's vendored parser has no duplicate-capture-name check; the Rust crate \
              refuses it. `regexp_named` is absent because the reference refuses it there for \
              its OWN reason (`duplicate extracted label name`), so that point agrees.",
    },
    Divergence {
        id: "engine_dir_a_nesting_limit",
        pulsus: Verdict::Reject,
        patterns: &["nest_999"],
        positions: &[
            "line_re",
            "line_nre",
            "line_after_line_format",
            "metric_line",
            "metric_binary",
            "variants_variant_after_line_format",
            "variants_common_side",
        ],
        owner: "#400",
        why: "the Rust crate's `nest_limit` and Go's `maxHeight` count different trees. Only the \
              positions where the reference does NOT wrap the pattern are affected; everywhere \
              else both reject.",
    },
    Divergence {
        id: "engine_dir_b_read_as_a_different_pattern",
        pulsus: Verdict::Accept,
        patterns: &[
            "big_u_escape",
            "perl_R",
            "flag_x",
            "nested_star",
            "bad_posix_class",
            "unicode_prop_long",
            "repeat_1001",
        ],
        positions: &[
            "sel_re",
            "sel_nre",
            "line_re",
            "line_nre",
            "line_after_line_format",
            "regexp_named",
            "labelfilter_re",
            "labelfilter_nre",
            "drop",
            "keep",
            "metric_line",
            "metric_binary",
            "label_replace",
            "variants_variant_after_line_format",
            "variants_variant_side",
        ],
        owner: "#400",
        why: "not merely lenient. `(?R)` is read as the Rust crate's CRLF flag (matches \
              everything), `[[:foo:]]` as a nested class (matches `:`/`f`/`o`), and `a**` as \
              `(a*)*`, which in a template renders `zxz` from the input `x` — see \
              `the_template_regex_boundary_does_not_match_the_reference`. A wrong answer, not \
              a permissive one. docs/api.md \u{a7}9.2/\u{a7}9.3.",
    },
    Divergence {
        id: "engine_dir_b_class_forms",
        pulsus: Verdict::Accept,
        patterns: &["class_double_dash", "brace_unicode"],
        positions: &[
            "sel_re",
            "sel_nre",
            "line_re",
            "line_nre",
            "line_after_line_format",
            "regexp_named",
            "labelfilter_re",
            "labelfilter_nre",
            "drop",
            "keep",
            "metric_line",
            "metric_binary",
            "variants_variant_after_line_format",
            "variants_variant_side",
        ],
        owner: "#400",
        why: "`label_replace` is absent because its rewrite makes the Rust crate agree with the \
              reference — the same one-site partial fix as the brace forms.",
    },
    Divergence {
        id: "engine_dir_b_invalid_utf8_escape",
        pulsus: Verdict::Accept,
        patterns: &["invalid_utf8"],
        positions: &[
            "line_re",
            "line_nre",
            "line_after_line_format",
            "regexp_named",
            "metric_line",
            "metric_binary",
            "label_replace",
            "variants_variant_side",
            "variants_variant_after_line_format",
        ],
        owner: "#400",
        why: "the verdict half of something worse. `\"\\xff\"` is one 0xFF byte in the \
              reference's pattern, refused as invalid UTF-8 wherever the pattern actually \
              reaches a parser — these nine positions. Our lexer does NOT decode it to that \
              byte, nor to U+00FF: `scan_double_quoted` drops the backslash on an unknown \
              escape (`pulsus-logql/src/lexer.rs:322-331`), so the pattern becomes the three \
              ASCII characters `xff` and compiles. The positions absent from this list are the \
              `NewFastRegexMatcher` sites, which never parse a plain literal at all — they \
              serve it on both sides, and THAT is where the real damage is: the reference \
              matches lines containing byte 0xFF, we match lines containing `xff`, and no cell \
              of this matrix moves. A wrong answer, recorded on #400 as a value divergence; \
              `the_parsed_pattern_value_is_committed_where_the_escape_changes_it` is the only \
              check here that can see it.",
    },
    Divergence {
        id: "variants_common_side_hides_the_build_error",
        pulsus: Verdict::Reject,
        patterns: BOTH_REJECT,
        positions: &["variants_common_side"],
        owner: "#380 (ledgered `malformed-query-refused-in-every-window`, owner-ruled \
                deliberate: PulsusDB refuses a malformed query in every window)",
        why: "the reference's querier swallows the common pipeline's build error and answers an \
              empty 200 in every window; PulsusDB answers 400. Deliberate, and the direction \
              the owner ruled for. Measured here for a LINE FILTER; #247 measured it for \
              `| logfmt`.",
    },
    Divergence {
        id: "variants_variant_side_skips_the_line_filter",
        pulsus: Verdict::Accept,
        patterns: BOTH_REJECT,
        positions: &["variants_variant_side"],
        owner: "#400",
        why: "found by this matrix, and the direction that matters: the reference REFUSES a \
              malformed line filter in a variant (`stage '|~ \"(\"'`) and PulsusDB serves it. \
              `VariantSpec::try_new` (`plan.rs:2641`) does compile the variant's discarded \
              prefix, but `compile_stage` returns `Ok(None)` for a PUSHABLE line filter \
              (`pipeline.rs:986-996`) before reaching `compile_regex` at `:1013`; a pushable \
              filter's regex is validated on the SQL-rendering path instead, and a discarded \
              prefix renders no SQL. **The escape is the pushdown, not the construct**: the \
              same filter after a `line_format` clears the pushdown and IS refused — that is \
              the `variants_variant_after_line_format` position, which agrees at every pattern. \
              `| regexp`, `| drop`, `| logfmt` and `| line_format` in this position are refused \
              on both sides too.",
    },
];

// ---------------------------------------------------------------------
// The reference's error taxonomy — the pattern set is enumerated from IT
// ---------------------------------------------------------------------

/// Every `ErrorCode` constant of
/// `vendor/github.com/grafana/regexp/syntax/parse.go:28-48 @ v3.7.4`,
/// with its message text, written out literally and in source order.
/// That vendored fork is the authority rather than the local Go
/// toolchain: all five reference call sites import
/// `github.com/grafana/regexp` (`pkg/logql/log/parser.go:18` and
/// `pkg/logql/log/filter.go:9-10 @ v3.7.4`).
///
/// The message is here, not just the constant name, so that a code's
/// coverage can be checked against the CONTAINER's own error body rather
/// than against this file's opinion of which pattern raises what — see
/// [`CAPTURED_REFERENCE_ERRORS`] and
/// [`live_reference_error_codes_are_exactly_the_covered_set`].
const GO_ERROR_CODES: &[(&str, &str)] = &[
    ("ErrInternalError", "regexp/syntax: internal error"),
    ("ErrInvalidCharClass", "invalid character class"),
    ("ErrInvalidCharRange", "invalid character class range"),
    ("ErrInvalidEscape", "invalid escape sequence"),
    ("ErrInvalidNamedCapture", "invalid named capture"),
    ("ErrInvalidPerlOp", "invalid or unsupported Perl syntax"),
    ("ErrInvalidRepeatOp", "invalid nested repetition operator"),
    ("ErrInvalidRepeatSize", "invalid repeat count"),
    ("ErrInvalidUTF8", "invalid UTF-8"),
    ("ErrMissingBracket", "missing closing ]"),
    ("ErrMissingParen", "missing closing )"),
    (
        "ErrMissingRepeatArgument",
        "missing argument to repetition operator",
    ),
    (
        "ErrTrailingBackslash",
        "trailing backslash at end of expression",
    ),
    ("ErrUnexpectedParen", "unexpected )"),
    ("ErrNestingDepth", "expression nests too deeply"),
    ("ErrLarge", "expression too large"),
];

/// A code with no covering pattern, and why it cannot have one.
///
/// **An unreachability claim is a claim about EVERY route into the rule,
/// so the argument has to cover the rule and not one probe.** The first
/// version of this table had four rows and two of them were wrong in
/// exactly that way: `ErrInvalidUTF8` was excused on a probe that sent a
/// raw `%FF` byte (refused by the query scanner first) when the escape
/// `"\xff"` reaches the parser fine, and `ErrLarge` was excused on a
/// probe of a NESTED repeat (pre-empted by the repeat-product cap) when
/// 4,000 copies of `a{999}` reach `maxSize` fine. Both are now covered
/// patterns. The two that remain are not probe-based at all — they are
/// a grep over the reference's own source, which is a statement about
/// every route by construction, and
/// [`live_reference_error_codes_are_exactly_the_covered_set`] fails if
/// either is ever observed on the wire.
const UNREACHABLE_CODES: &[(&str, &str)] = &[
    (
        "ErrInternalError",
        "declared and never raised. `git grep -n ErrInternalError \
         vendor/github.com/grafana/regexp/ @ v3.7.4` finds exactly one hit, the declaration at \
         parse.go:30; `parse`'s recover (parse.go:889-900) maps only ErrLarge and \
         ErrNestingDepth. Not a probe result: there is no raise site to reach.",
    ),
    (
        "ErrInvalidCharClass",
        "same: declared at parse.go:33 and never raised anywhere in the package (one hit for \
         the whole vendored tree). An unrecognised POSIX class name raises ErrInvalidCharRange \
         instead — measured, `[[:foo:]]` answers ``invalid character class range: \
         `[:foo:]` `` on the container, which is the `bad_posix_class` row.",
    ),
];

/// The reference's OWN error body for each pattern that raises a code,
/// captured from the pinned container: the position it was captured at,
/// and the body text from the code onward (truncated).
///
/// This is what makes the census bite. Without it
/// [`every_go_regexp_error_code_is_accounted_for`] only checks that this
/// file agrees with itself — which is how two false unreachability rows
/// sat here passing. The live leg puts each of these back to the
/// container at its named position and requires the fragment to appear,
/// so a pattern credited with a code has to actually raise it.
///
/// The expression Go quotes is the offending **sub-token**, not the
/// pattern — visible here in `[z-a]` → `` `z-a` `` and `a{100}{100}{100}`
/// → `` `{100}{100}` `` — and `label_replace`/`nest_999` show the other
/// half of the same rule, a site that quotes the ANCHORED form. Those two
/// facts are the reason byte-parity of the message is unreachable without
/// porting the parser, so they are pinned here rather than asserted in
/// prose.
///
/// `invalid_utf8` carries no quoted expression because the expression is
/// a raw 0xFF byte, which is not representable in a Rust `&str` and which
/// the transport renders lossily.
const CAPTURED_REFERENCE_ERRORS: &[(&str, &str, &str)] = &[
    ("missing_paren", "sel_re", "missing closing ): `(`"),
    ("missing_bracket", "sel_re", "missing closing ]: `[a`"),
    (
        "missing_repeat_arg",
        "sel_re",
        "missing argument to repetition operator: `*`",
    ),
    (
        "trailing_backslash",
        "sel_re",
        "trailing backslash at end of expression: ``",
    ),
    ("unexpected_paren", "sel_re", "unexpected ): `a)b`"),
    (
        "bad_char_range",
        "sel_re",
        "invalid character class range: `z-a`",
    ),
    ("bad_escape", "sel_re", r"invalid escape sequence: `\q`"),
    (
        "bad_named_capture",
        "sel_re",
        "invalid named capture: `(?P<>`",
    ),
    (
        "lookahead",
        "sel_re",
        "invalid or unsupported Perl syntax: `(?!`",
    ),
    (
        "repeat_size_inverted",
        "sel_re",
        "invalid repeat count: `{2,1}`",
    ),
    (
        "adjacent_repeats",
        "sel_re",
        "invalid nested repetition operator: `{100}{100}`",
    ),
    ("repeat_product", "sel_re", "invalid repeat count: `{100}`"),
    (
        "class_escape_range",
        "sel_re",
        r"invalid escape sequence: `\d`",
    ),
    (
        "too_large",
        "sel_re",
        "expression too large: `a{999}a{999}a{999}",
    ),
    ("invalid_utf8", "line_re", "invalid UTF-8: `"),
    (
        "nest_999",
        "sel_re",
        "expression nests too deeply: `^(?s:(((((",
    ),
    ("big_u_escape", "sel_re", r"invalid escape sequence: `\U`"),
    (
        "perl_R",
        "sel_re",
        "invalid or unsupported Perl syntax: `(?R`",
    ),
    (
        "flag_x",
        "sel_re",
        "invalid or unsupported Perl syntax: `(?x`",
    ),
    (
        "nested_star",
        "sel_re",
        "invalid nested repetition operator: `**`",
    ),
    (
        "bad_posix_class",
        "sel_re",
        "invalid character class range: `[:foo:]`",
    ),
    (
        "unicode_prop_long",
        "sel_re",
        r"invalid character class range: `\p{Alphabetic}`",
    ),
    ("repeat_1001", "sel_re", "invalid repeat count: `{1001}`"),
    (
        "class_double_dash",
        "sel_re",
        "invalid character class range: `a--`",
    ),
    ("brace_unicode", "sel_re", r"invalid escape sequence: `\u`"),
];

// ---------------------------------------------------------------------
// The template axis — a different verdict TYPE
// ---------------------------------------------------------------------

/// A bad template regex is never a status: it is a `200` carrying
/// `__error__: TemplateFormatErr` on the affected line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RenderVerdict {
    /// The line rendered. The payload is what it rendered TO, because for
    /// two of these rows that is the finding.
    Rendered(&'static str),
    TemplateFormatErr,
}

/// `{{ regexReplaceAll `<pattern>` .app `z` }}` over a line whose `app`
/// label is `x`, both sides.
///
/// The pattern is passed as a Go template RAW string (backticks) on
/// purpose: in a double-quoted template literal Go's own scanner refuses
/// `\Q`, `\p` and `\u{` as invalid string escapes and the regex engine
/// never sees them, which measures the template lexer instead of the
/// regex boundary. Measured both ways; the backtick form is the one that
/// reaches the engine.
struct TemplateProbe {
    pattern: &'static str,
    reference: RenderVerdict,
    pulsus: RenderVerdict,
}

const TEMPLATE_AXIS: &[TemplateProbe] = &[
    // The two agreements. Without them "they disagree" would be a claim
    // about a table that only ever disagrees.
    tp(
        "a.*b",
        RenderVerdict::Rendered("x"),
        RenderVerdict::Rendered("x"),
    ),
    tp(
        "(",
        RenderVerdict::TemplateFormatErr,
        RenderVerdict::TemplateFormatErr,
    ),
    // Reference renders, PulsusDB raises TemplateFormatErr.
    tp(
        "a{bbb}c",
        RenderVerdict::Rendered("x"),
        RenderVerdict::TemplateFormatErr,
    ),
    tp(
        r"\Qa*\E",
        RenderVerdict::Rendered("x"),
        RenderVerdict::TemplateFormatErr,
    ),
    tp(
        r"\101",
        RenderVerdict::Rendered("x"),
        RenderVerdict::TemplateFormatErr,
    ),
    tp(
        "(?ss:ab)",
        RenderVerdict::Rendered("x"),
        RenderVerdict::TemplateFormatErr,
    ),
    tp(
        "(?)a",
        RenderVerdict::Rendered("x"),
        RenderVerdict::TemplateFormatErr,
    ),
    tp(
        "a{,5}",
        RenderVerdict::Rendered("x"),
        RenderVerdict::TemplateFormatErr,
    ),
    tp(
        "a{}",
        RenderVerdict::Rendered("x"),
        RenderVerdict::TemplateFormatErr,
    ),
    tp(
        "a(?i){2}",
        RenderVerdict::Rendered("x"),
        RenderVerdict::TemplateFormatErr,
    ),
    tp(
        "(?P<n>a)(?P<n>b)",
        RenderVerdict::Rendered("x"),
        RenderVerdict::TemplateFormatErr,
    ),
    // PulsusDB renders, the reference raises TemplateFormatErr. TWO of
    // these return a different STRING, not merely a different verdict:
    // `a**` is read as `(a*)*` and replaces at every position, and
    // `\p{Alphabetic}` is a property the Rust crate knows and RE2 does
    // not, so it matches `x` and replaces it.
    tp(
        "a**",
        RenderVerdict::TemplateFormatErr,
        RenderVerdict::Rendered("zxz"),
    ),
    tp(
        r"\p{Alphabetic}",
        RenderVerdict::TemplateFormatErr,
        RenderVerdict::Rendered("z"),
    ),
    tp(
        "a{1001}",
        RenderVerdict::TemplateFormatErr,
        RenderVerdict::Rendered("x"),
    ),
    tp(
        "[[:foo:]]",
        RenderVerdict::TemplateFormatErr,
        RenderVerdict::Rendered("x"),
    ),
    tp(
        r"\U0001F600",
        RenderVerdict::TemplateFormatErr,
        RenderVerdict::Rendered("x"),
    ),
    tp(
        r"\u{263A}",
        RenderVerdict::TemplateFormatErr,
        RenderVerdict::Rendered("x"),
    ),
    tp(
        "(?x)a",
        RenderVerdict::TemplateFormatErr,
        RenderVerdict::Rendered("x"),
    ),
    tp(
        "(?R)a",
        RenderVerdict::TemplateFormatErr,
        RenderVerdict::Rendered("x"),
    ),
    tp(
        "[a--b]",
        RenderVerdict::TemplateFormatErr,
        RenderVerdict::Rendered("x"),
    ),
];

const fn tp(
    pattern: &'static str,
    reference: RenderVerdict,
    pulsus: RenderVerdict,
) -> TemplateProbe {
    TemplateProbe {
        pattern,
        reference,
        pulsus,
    }
}

// ---------------------------------------------------------------------
// Matrix construction and PulsusDB's verdict
// ---------------------------------------------------------------------

impl Pattern {
    /// The text as it appears INSIDE the LogQL string literal. For every
    /// regex body that is the LogQL-quoted regex; for
    /// [`Body::LogqlSource`] it is the body verbatim, because there the
    /// escape IS the construct under test.
    fn literal(&self) -> String {
        match self.body {
            Body::Literal(s) => logql_quote(s),
            Body::NestedGroups(n) => logql_quote(&format!("{}a{}", "(".repeat(n), ")".repeat(n))),
            Body::Repeated(s, n) => logql_quote(&s.repeat(n)),
            Body::Alternated(s, n) => logql_quote(&vec![s; n].join("|")),
            Body::LogqlSource(s) => s.to_string(),
        }
    }

    /// The body as written, for identity checks only — never for building
    /// a query, which must go through [`Pattern::literal`].
    fn describe_body(&self) -> String {
        match self.body {
            Body::Literal(s) => s.to_string(),
            Body::NestedGroups(n) => format!("{}a{}", "(".repeat(n), ")".repeat(n)),
            Body::Repeated(s, n) => s.repeat(n),
            Body::Alternated(s, n) => vec![s; n].join("|"),
            Body::LogqlSource(s) => s.to_string(),
        }
    }
}

/// Escapes a regex for a double-quoted LogQL string literal. One helper,
/// so the hermetic and live halves cannot drift apart.
fn logql_quote(pattern: &str) -> String {
    pattern.replace('\\', "\\\\").replace('"', "\\\"")
}

fn exception(position: &str, pattern: &str) -> Option<&'static Exception> {
    EXCEPTIONS
        .iter()
        .find(|e| e.position == position && e.pattern == pattern)
}

fn committed(position: &Position, pattern: &Pattern, side: Side) -> Verdict {
    if let Some(e) = exception(position.id, pattern.id) {
        let v = match side {
            Side::Reference => e.reference,
            Side::Pulsus => e.pulsus,
        };
        if let Some(v) = v {
            return v;
        }
    }
    let (rule, column) = match side {
        Side::Reference => (position.reference, pattern.reference),
        Side::Pulsus => (position.pulsus, pattern.pulsus),
    };
    match rule {
        Rule::PerPattern => column,
        Rule::AcceptsEverything => Verdict::Accept,
        Rule::RejectsEverything => Verdict::Reject,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Reference,
    Pulsus,
}

struct Point {
    position: &'static Position,
    pattern: &'static Pattern,
    query: String,
}

impl Point {
    fn label(&self) -> String {
        format!("{}/{}", self.position.id, self.pattern.id)
    }
}

/// The cross product over `positions`. Never over a filtered subset: the
/// caller passes [`POSITIONS`] or [`MASKED`] whole.
fn matrix(positions: &'static [Position]) -> Vec<Point> {
    let mut out = Vec::new();
    for position in positions {
        for pattern in PATTERNS {
            out.push(Point {
                position,
                pattern,
                query: position.template.replace("{P}", &pattern.literal()),
            });
        }
    }
    out
}

fn ctx() -> PlanCtx<'static> {
    PlanCtx {
        db: "pulsus",
        streams_idx: "log_streams_idx",
        streams: "log_streams",
        samples: "log_samples",
        rollup_table: "log_metrics_5s",
        rollup_res_ns: 5_000_000_000,
        scan_budget_bytes: 50 * 1024 * 1024 * 1024,
        max_streams: 100_000,
        pipeline_scan_factor: 10,
    }
}

/// A `query_range` request, because that is the endpoint the live leg
/// puts these to — the reference refuses an instant LOG query outright,
/// which would mask the surface under test behind an unrelated 400.
fn params() -> QueryParams {
    QueryParams {
        spec: QuerySpec::Range {
            start_ns: 1_782_907_200_000_000_000,
            end_ns: 1_782_928_800_000_000_000,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    }
}

/// PulsusDB's verdict **at the layer a user meets it**: parse, then plan,
/// then the pipeline compile `exec` performs before any I/O. Asking the
/// parser alone would be a weaker layer — #247 measured those two
/// disagreeing about what is accepted.
///
/// The variants arm reproduces `VariantArena::build` (`variants.rs`)
/// rather than approximating it, the same walk
/// `logql_logfmt_expr_matrix.rs` uses and for the same reason:
/// `MetricNode::leaves()` yields only the `scan`, so a `variants(...)`
/// query measured through `leaves()` alone would be measured through one
/// of its two pipeline positions and not the other.
fn pulsus_verdict(query: &str) -> (Verdict, String) {
    let expr = match parse(query) {
        Ok(e) => e,
        Err(e) => return (Verdict::Reject, format!("parse: {e}")),
    };
    let planned = match plan(&expr, &params(), &ctx()) {
        Ok(p) => p,
        Err(e) => return (Verdict::Reject, format!("plan: {e}")),
    };
    let mut pipelines: Vec<Vec<pulsus_logql::Stage>> = Vec::new();
    match &planned {
        Plan::Streams(sp) => pipelines.push(sp.pipeline.clone()),
        Plan::Metric(mp) => pipelines.extend(mp.client.iter().map(|c| c.pipeline.clone())),
        Plan::MetricBinary(node) => {
            for leaf in node.leaves() {
                pipelines.extend(leaf.client.iter().map(|c| c.pipeline.clone()));
            }
            collect_variant_pipelines(node, &mut pipelines);
        }
    }
    for stages in &pipelines {
        if let Err(e) = CompiledPipeline::compile(stages) {
            return (Verdict::Reject, format!("compile: {e}"));
        }
    }
    (Verdict::Accept, String::new())
}

/// Every `common ++ tail` a `MetricNode::Variants` would have
/// `VariantArena::build` compile. Walks the whole node tree, because a
/// variants node can sit under a binary op or an aggregation.
fn collect_variant_pipelines(node: &MetricNode, out: &mut Vec<Vec<pulsus_logql::Stage>>) {
    pulsus_logql::walk::preorder::<MetricNodeScc>(node, |n| {
        if let MetricNode::Variants { scan, variants, .. } = n {
            let common: Vec<pulsus_logql::Stage> = scan
                .client
                .as_ref()
                .map(|c| c.pipeline.clone())
                .unwrap_or_default();
            for spec in variants {
                let mut full = common.clone();
                full.extend(spec.client().pipeline.iter().cloned());
                out.push(full);
            }
        }
    });
}

// ---------------------------------------------------------------------
// Hermetic tests
// ---------------------------------------------------------------------

/// **PulsusDB's verdicts match the committed column, point for point.**
#[test]
fn pulsus_verdicts_match_the_committed_table() {
    let points = matrix(POSITIONS);
    assert_eq!(
        points.len(),
        POSITIONS.len() * PATTERNS.len(),
        "the matrix must be the full cross product"
    );

    let mut disagree = Vec::new();
    let (mut accepts, mut rejects) = (0usize, 0usize);
    for pt in &points {
        let want = committed(pt.position, pt.pattern, Side::Pulsus);
        match want {
            Verdict::Accept => accepts += 1,
            Verdict::Reject => rejects += 1,
        }
        let (got, why) = pulsus_verdict(&pt.query);
        if got != want {
            disagree.push(format!(
                "{}\n    committed={want:?} measured={got:?} {why}\n    {}",
                pt.label(),
                pt.query.chars().take(120).collect::<String>()
            ));
        }
    }
    assert!(
        disagree.is_empty(),
        "{} of {} points no longer match the committed PulsusDB column — RE-CAPTURE the table \
         rather than editing one cell to match:\n{}",
        disagree.len(),
        points.len(),
        disagree.join("\n")
    );
    assert!(
        accepts > 0 && rejects > 0,
        "the committed column must contain both dispositions ({accepts} accept, {rejects} reject)"
    );
}

/// **The divergence set is exactly the committed enumeration.**
#[test]
fn the_divergence_set_is_exactly_the_committed_enumeration() {
    let measured = measured_disagreements();
    assert!(
        !measured.is_empty(),
        "the matrix must contain disagreements"
    );
    // The two figures docs/benchmarks/logs-differential-ledger.md's
    // `logql-regex-accept-surface-divergence` quotes. Asserted here so
    // that a prose number in the ledger cannot outlive the table it
    // describes.
    assert_eq!(
        POSITIONS.len() * PATTERNS.len(),
        720,
        "the ledger says 720 unmasked points"
    );
    assert_eq!(
        measured.len(),
        323,
        "the ledger says 323 of the 720 unmasked points disagree"
    );
    assert_eq!(
        POSITIONS.len() * PATTERNS.len() + MASKED.len() * PATTERNS.len(),
        810,
        "the ledger says 810 probed points, masked positions included"
    );

    let claimed = |skip: Option<usize>| -> Vec<(String, String)> {
        let mut v = Vec::new();
        for (i, d) in DIVERGENCES.iter().enumerate() {
            if Some(i) == skip {
                continue;
            }
            for pos in d.positions {
                for pat in d.patterns {
                    v.push(((*pos).to_string(), (*pat).to_string()));
                }
            }
        }
        v.sort();
        v.dedup();
        v
    };

    let all = claimed(None);
    let missing: Vec<_> = measured.iter().filter(|m| !all.contains(m)).collect();
    let extra: Vec<_> = all.iter().filter(|c| !measured.contains(c)).collect();
    assert!(
        missing.is_empty(),
        "{} disagreeing points are in no `Divergence` row (a divergence with no owner):\n{missing:#?}",
        missing.len()
    );
    assert!(
        extra.is_empty(),
        "{} points are claimed by a `Divergence` row and do NOT disagree:\n{extra:#?}",
        extra.len()
    );

    // No row may be redundant: dropping any one must shrink the union.
    for (i, d) in DIVERGENCES.iter().enumerate() {
        let without = claimed(Some(i));
        assert!(
            measured.iter().any(|m| !without.contains(m)),
            "`{}` claims nothing the other rows do not — a dead row",
            d.id
        );
    }

    // Direction, owner, and that both directions are represented.
    for d in DIVERGENCES {
        assert!(!d.owner.is_empty(), "`{}` has no owner", d.id);
        assert!(
            d.why.len() > 60,
            "`{}` must say what the class IS, not just name it",
            d.id
        );
        assert!(
            !d.patterns.is_empty() && !d.positions.is_empty(),
            "`{}` is empty",
            d.id
        );
        for pos in d.positions {
            for pat in d.patterns {
                let position = POSITIONS.iter().find(|p| p.id == *pos).expect("position");
                let pattern = PATTERNS.iter().find(|p| p.id == *pat).expect("pattern");
                assert_eq!(
                    committed(position, pattern, Side::Pulsus),
                    d.pulsus,
                    "`{}` declares PulsusDB {:?} but {}/{} is committed the other way",
                    d.id,
                    d.pulsus,
                    pos,
                    pat
                );
            }
        }
    }
    assert!(
        DIVERGENCES.iter().any(|d| d.pulsus == Verdict::Accept)
            && DIVERGENCES.iter().any(|d| d.pulsus == Verdict::Reject),
        "both directions must be represented; a one-directional table would hide the half that \
         serves a query the reference refuses"
    );
    eprintln!(
        "{} of {} matrix points disagree, in {} classes",
        measured.len(),
        POSITIONS.len() * PATTERNS.len(),
        DIVERGENCES.len()
    );
}

fn measured_disagreements() -> Vec<(String, String)> {
    let mut out = Vec::new();
    for position in POSITIONS {
        for pattern in PATTERNS {
            if committed(position, pattern, Side::Reference)
                != committed(position, pattern, Side::Pulsus)
            {
                out.push((position.id.to_string(), pattern.id.to_string()));
            }
        }
    }
    out.sort();
    out
}

/// **Every position says what it reaches, every pattern is distinct, and
/// no position is a duplicate of another.** The `why` fields are the only
/// record of why the enumeration is the shape it is; an empty one means
/// the next person cannot tell whether a position is load-bearing.
#[test]
fn the_enumeration_is_self_consistent_and_says_why_each_entry_exists() {
    let mut ids: Vec<&str> = Vec::new();
    let mut templates: Vec<&str> = Vec::new();
    for position in POSITIONS.iter().chain(MASKED) {
        assert!(
            position.why.len() > 60,
            "`{}` must say what it reaches and what would be missed without it",
            position.id
        );
        assert!(
            position.template.contains("{P}"),
            "`{}` has no pattern slot",
            position.id
        );
        assert!(
            !ids.contains(&position.id),
            "duplicate position `{}`",
            position.id
        );
        assert!(
            !templates.contains(&position.template),
            "`{}` duplicates another position's query shape, so it measures nothing new",
            position.id
        );
        ids.push(position.id);
        templates.push(position.template);
    }
    let mut pattern_ids: Vec<&str> = Vec::new();
    let mut bodies: Vec<String> = Vec::new();
    for pattern in PATTERNS {
        assert!(
            !pattern_ids.contains(&pattern.id),
            "duplicate pattern `{}`",
            pattern.id
        );
        let body = pattern.describe_body();
        assert!(
            !bodies.contains(&body),
            "`{}` repeats another pattern's body, so it adds no point",
            pattern.id
        );
        pattern_ids.push(pattern.id);
        bodies.push(body);
    }
    // Every divergence row must name positions and patterns that exist.
    for d in DIVERGENCES {
        for pos in d.positions {
            assert!(
                ids.contains(pos),
                "`{}` names an unknown position `{pos}`",
                d.id
            );
        }
        for pat in d.patterns {
            assert!(
                pattern_ids.contains(pat),
                "`{}` names an unknown pattern `{pat}`",
                d.id
            );
        }
    }
}

/// **The one thing in this file that is not a verdict: what the planner
/// actually receives when the LogQL string escape changes the pattern.**
///
/// Every other check here scores accept-versus-reject, so a pattern that
/// both sides SERVE while meaning different things is invisible to all of
/// them. That is not hypothetical — `engine_dir_b_invalid_utf8_escape`
/// carried a false mechanism for a whole review round ("our lexer decodes
/// `\xff` to U+00FF") and nothing in the matrix could contradict it,
/// because no cell moves either way.
///
/// So: for every [`Body::LogqlSource`] pattern — the ones whose escape is
/// the construct under test — the decoded value the planner sees is
/// committed here and asserted through the real parser, together with
/// what it does and does not match. A new `LogqlSource` pattern without
/// an entry fails rather than being scored on its verdict alone.
#[test]
fn the_parsed_pattern_value_is_committed_where_the_escape_changes_it() {
    /// `(pattern id, the value our parser hands the planner, what the
    /// REFERENCE's parser sees instead)`.
    const DECODED: &[(&str, &str, &str)] = &[(
        "invalid_utf8",
        // `scan_double_quoted` (`pulsus-logql/src/lexer.rs:322-331`)
        // handles an unknown escape with `Some(other) => value.push(other)`
        // — the backslash is dropped and the `x` kept.
        "xff",
        // Go's `strconv.Unquote` decodes `\xff` to the single byte 0xFF,
        // which is why the reference's parser raises `ErrInvalidUTF8`
        // wherever the pattern reaches it at all.
        "one 0xFF byte",
    )];

    for pattern in PATTERNS {
        let Body::LogqlSource(source) = pattern.body else {
            continue;
        };
        let entry = DECODED.iter().find(|(id, _, _)| *id == pattern.id);
        let Some((_, decoded, reference_sees)) = entry else {
            panic!(
                "`{}` is a LogqlSource pattern with no committed decoded value — its escape is \
                 the construct under test, and a verdict alone cannot show what it became",
                pattern.id
            );
        };
        assert!(!reference_sees.is_empty());

        // Through the real parser, at a position whose pattern slot is the
        // whole matcher value, so the decoded text is readable directly.
        let query = format!(r#"{{app=~"{source}"}}"#);
        let expr = parse(&query).unwrap_or_else(|e| panic!("{query}: {e}"));
        let pulsus_logql::Expr::Log(log) = expr else {
            panic!("{query}: expected a log query")
        };
        let value = &log.selector.matchers[0].value;
        assert_eq!(
            value, decoded,
            "`{}`: the planner receives {value:?}, not the committed {decoded:?}. If the lexer \
             changed, re-record the value AND the divergence's mechanism — the sentence in \
             `engine_dir_b_invalid_utf8_escape` describes exactly this string.",
            pattern.id
        );

        // And what that value MEANS, because "the pattern became `xff`"
        // is only a defect if `xff` matches something else.
        let compiled = regex::Regex::new(value).expect("the decoded pattern compiles");
        assert!(
            compiled.is_match("xff"),
            "`{}`: the decoded pattern must match its own literal text",
            pattern.id
        );
        assert!(
            !compiled.is_match("\u{00FF}"),
            "`{}`: the decoded pattern must NOT match U+00FF — a previous version of this \
             file's mechanism claimed the escape produced that character",
            pattern.id
        );
        // The reference's byte, as the transport would deliver it.
        assert!(
            !compiled.is_match(&String::from_utf8_lossy(&[0xFFu8])),
            "`{}`: the decoded pattern must not match the reference's 0xFF byte either",
            pattern.id
        );
    }
}

/// **Every committed exception is a real exception.** A row that agrees
/// with its position's rule and its pattern's column pins nothing and
/// would quietly outlive the behaviour it recorded.
#[test]
fn every_committed_exception_is_a_real_exception() {
    for e in EXCEPTIONS {
        let position = POSITIONS
            .iter()
            .chain(MASKED)
            .find(|p| p.id == e.position)
            .unwrap_or_else(|| panic!("exception names an unknown position `{}`", e.position));
        let pattern = PATTERNS
            .iter()
            .find(|p| p.id == e.pattern)
            .unwrap_or_else(|| panic!("exception names an unknown pattern `{}`", e.pattern));
        assert!(
            e.reference.is_some() || e.pulsus.is_some(),
            "`{}`/`{}` overrides neither side",
            e.position,
            e.pattern
        );
        assert!(!e.why.is_empty());
        for (side, over) in [(Side::Reference, e.reference), (Side::Pulsus, e.pulsus)] {
            let Some(over) = over else { continue };
            let (rule, column) = match side {
                Side::Reference => (position.reference, pattern.reference),
                Side::Pulsus => (position.pulsus, pattern.pulsus),
            };
            let without = match rule {
                Rule::PerPattern => column,
                Rule::AcceptsEverything => Verdict::Accept,
                Rule::RejectsEverything => Verdict::Reject,
            };
            assert_ne!(
                over, without,
                "`{}`/`{}` ({side:?}) records {over:?}, which is what the rule already gives",
                e.position, e.pattern
            );
        }
    }
}

/// **The masked positions pin nothing, and that is measured rather than
/// asserted from the shape of the query.**
///
/// If either stops masking — because a rule changed on one side — this
/// fails, and the position should be promoted into [`POSITIONS`] rather
/// than have its expectation edited.
#[test]
fn the_masked_positions_pin_nothing_and_this_is_measured() {
    for position in MASKED {
        let mut disagree = Vec::new();
        for pattern in PATTERNS {
            let r = committed(position, pattern, Side::Reference);
            let u = committed(position, pattern, Side::Pulsus);
            if r != u {
                disagree.push(pattern.id);
            }
        }
        assert!(
            disagree.is_empty(),
            "`{}` is no longer masked ({} of {} patterns disagree: {disagree:?}) — promote it \
             into POSITIONS",
            position.id,
            disagree.len(),
            PATTERNS.len()
        );
    }
    // And PulsusDB really answers what the mask says, through the chain.
    for pt in matrix(MASKED) {
        let want = committed(pt.position, pt.pattern, Side::Pulsus);
        let (got, why) = pulsus_verdict(&pt.query);
        assert_eq!(got, want, "{}: {why}", pt.label());
    }
}

fn message_of(code: &str) -> &'static str {
    GO_ERROR_CODES
        .iter()
        .find(|(c, _)| *c == code)
        .map(|(_, m)| *m)
        .unwrap_or_else(|| panic!("`{code}` is not one of the reference's codes"))
}

/// **The pattern set is enumerated from the REFERENCE's taxonomy, not
/// from ours, and each code's coverage is tied to a CAPTURED reference
/// error rather than to this file's opinion.**
///
/// An earlier version checked only that the table agreed with itself and
/// that each unreachability reason was long enough. That is why two false
/// reasons sat here passing: `ErrInvalidUTF8` and `ErrLarge` were both
/// excused on a probe whose domain was one route into the rule, and no
/// assertion here could see the difference. Now every covered code has to
/// name a pattern AND a captured body fragment that begins with that
/// code's message, and
/// [`live_reference_error_codes_are_exactly_the_covered_set`] puts every
/// fragment back to the container and checks that no excused code ever
/// appears on the wire.
#[test]
fn every_go_regexp_error_code_is_accounted_for() {
    let mut uncovered = Vec::new();
    for (code, message) in GO_ERROR_CODES {
        assert!(!message.is_empty(), "`{code}` has no message text");
        let covered = PATTERNS.iter().any(|p| p.go_code == Some(*code));
        let excused = UNREACHABLE_CODES.iter().any(|(c, _)| c == code);
        assert!(
            !(covered && excused),
            "`{code}` is both covered by a pattern and recorded unreachable"
        );
        if !covered && !excused {
            uncovered.push(*code);
        }
    }
    assert!(
        uncovered.is_empty(),
        "these reference error codes have neither a covering pattern nor a stated \
         unreachability reason: {uncovered:?}"
    );
    for (code, why) in UNREACHABLE_CODES {
        message_of(code);
        assert!(
            why.len() > 80,
            "`{code}`'s unreachability reason is too thin to check"
        );
        // No captured body may carry an excused code: if one does, the
        // code is reachable and the row is a false claim.
        // `{message}: ` and not `{message}`, because one code's message
        // is a PREFIX of another's ("invalid character class" of
        // "invalid character class range") and Go always emits the
        // delimiter (`parse.go:21 @ v3.7.4`).
        let message = format!("{}: ", message_of(code));
        for (pattern, _, fragment) in CAPTURED_REFERENCE_ERRORS {
            assert!(
                !fragment.starts_with(&message),
                "`{code}` is recorded unreachable but `{pattern}`'s captured reference error \
                 begins with its message — the reason is false, not the capture"
            );
        }
    }
    // Every `go_code` a pattern claims must be a real code, the pattern
    // must be one the reference REJECTS somewhere, and it must carry a
    // captured body that actually begins with that code's message.
    for pattern in PATTERNS {
        let Some(code) = pattern.go_code else {
            assert!(
                !CAPTURED_REFERENCE_ERRORS
                    .iter()
                    .any(|(p, _, _)| p == &pattern.id),
                "`{}` has a captured reference error but claims no code",
                pattern.id
            );
            continue;
        };
        let message = format!("{}: ", message_of(code));
        let captured: Vec<_> = CAPTURED_REFERENCE_ERRORS
            .iter()
            .filter(|(p, _, _)| p == &pattern.id)
            .collect();
        assert_eq!(
            captured.len(),
            1,
            "`{}` claims `{code}` and must have exactly one captured reference error",
            pattern.id
        );
        let (_, position, fragment) = captured[0];
        assert!(
            fragment.starts_with(&message),
            "`{}`'s captured error {fragment:?} does not begin with `{code}`'s message \
             {message:?} — the pattern is credited with a code it does not raise",
            pattern.id
        );
        let position = POSITIONS
            .iter()
            .find(|p| p.id == *position)
            .unwrap_or_else(|| panic!("`{}` captures at an unknown position", pattern.id));
        assert_eq!(
            committed(position, pattern, Side::Reference),
            Verdict::Reject,
            "`{}` captures its error at `{}`, where the committed reference verdict is Accept",
            pattern.id,
            position.id
        );
    }
    // `invalid_utf8` and `too_large` are the two the first version of
    // this table excused; if either loses its code the old claim is back.
    for id in ["invalid_utf8", "too_large"] {
        let pattern = PATTERNS.iter().find(|p| p.id == id).expect("present");
        assert!(
            pattern.go_code.is_some(),
            "`{id}` exists to cover a code the first version of this file called unreachable"
        );
    }
}

/// **The compile-site enumeration is derived from the source,
/// recursively.**
///
/// `logql_logfmt_expr_matrix.rs`'s equivalent reads `src/logql/*.rs`
/// NON-recursively, so `src/logql/template/` is invisible to it. Copying
/// that helper here would have silently exempted the two template regex
/// sites — the exact class this issue found a false claim about — so this
/// one walks the tree.
#[test]
fn the_regex_compile_sites_are_enumerated_from_the_source() {
    /// One file's regex-construction sites: the path under `src/logql`,
    /// each construction marker with the number of times it occurs in the
    /// file's production half, and what covers (or excludes) it.
    type Site = (&'static str, &'static [(&'static str, usize)], &'static str);
    const SITES: &[Site] = &[
        (
            "escape.rs",
            &[
                ("validate_anchored_regex(", 1),
                ("validate_unanchored_regex(", 1),
            ],
            "the SQL-rendering seam: every pushed-down regex is compiled in the exact form it \
             will emit, first. Covered by `sel_re`/`sel_nre` (anchored) and \
             `line_re`/`line_nre`/`metric_line`/`metric_binary` (unanchored).",
        ),
        (
            "ip.rs",
            &[("Regex::new(", 2)],
            "EXCLUDED, no user input: the two `OnceLock` IPv4/IPv6 scan patterns, both `r\"…\"` \
             literals in the call itself, both `.expect(\"static ipv4 regex\")` / \
             `(\"static ipv6 regex\")`.",
        ),
        (
            "pipeline.rs",
            &[
                ("Regex::new(", 1),
                ("compile_regex(", 5),
                ("compile_anchored_regex(", 4),
                ("validate_anchored_regex(", 1),
                ("validate_unanchored_regex(", 1),
                ("compile_drop_keep(", 3),
                ("compile_user_regex(", 1),
                ("compile_user_regex_anchored(", 1),
            ],
            "the in-process seam and its callers: the line filter compiled after a \
             `line_format` (`line_after_line_format`), `DECOLORIZE_PATTERN` (EXCLUDED, a \
             `const`), `| drop`/`| keep` (`drop`, `keep`), the `| regexp` parser \
             (`regexp_named`), the label filter (`labelfilter_re`, `labelfilter_nre`), and the \
             definitions of `compile_regex`/`compile_anchored_regex`/`validate_*`/ \
             `compile_drop_keep` themselves. Issue #291 moved the two seams onto \
             `pulsus_re2::compile_user_regex`/`_anchored`, so what used to be three \
             `Regex::new(` is now one: the survivor is inside `bad_regex`, which \
             re-compiles the user's own pattern only to choose which error text to \
             report — it decides no verdict, is reached only AFTER the budget estimate \
             passed, and issue #240 pins its rule.",
        ),
        (
            "plan.rs",
            &[("compile_user_regex_anchored(", 1)],
            "`LabelReplaceSpec::compile` — the one LogQL site that translates the pattern \
             through `re2_pattern_to_rust` before compiling it, and the one that reports the \
             WRAPPED form (issue #276). Covered by `label_replace`. Issue #291 routed it \
             through the shared budgeted entry point; the marker changed with it, which is \
             the whole reason the four #291 markers are in `MARKERS` — without them this \
             site would have vanished from the census instead of moving in it.",
        ),
        (
            "template/funcs.rs",
            &[
                ("compile_regex(", 4),
                ("compile_user_regex_with(", 1),
                ("regex_compile_transient_bound_with(", 1),
            ],
            "the template regex functions `regexReplaceAll`, `regexReplaceAllLiteral` and \
             `count`, plus their shared compile seam. NOT a status axis — a bad pattern here is \
             a 200 carrying `__error__: TemplateFormatErr` — so it is covered by \
             `the_template_regex_boundary_does_not_match_the_reference` rather than by a \
             matrix position. This file is invisible to a non-recursive walk. Issue #291 \
             replaced the bare `RegexBuilder::new(` with the budgeted \
             `compile_user_regex_with(` at the SAME 1 MiB program ceiling, and charges the \
             render budget `regex_compile_transient_bound_with(` instead of a flat 1 MiB.",
        ),
        (
            "template/mod.rs",
            &[("compile_user_regex(", 1)],
            "the compile-time prewarm of the literal-pattern cache, written `if let Ok(re) = …` \
             so a failure is dropped and the pattern is recompiled at render time by \
             `funcs.rs`'s seam, which is where the VERDICT is taken. It decides no verdict — \
             but issue #291's review measured that it decided ALLOCATION: as a bare \
             `Regex::new` it peaked 298.92 MB on a literal `\\w`x43000 inside the query-text \
             cap, so it is now budgeted like every other user-pattern compile. It was listed \
             here as EXCLUDED before that, which is why the marker moved rather than the row \
             disappearing.",
        ),
    ];

    const MARKERS: &[&str] = &[
        "Regex::new(",
        "RegexBuilder::new(",
        "compile_regex(",
        "compile_anchored_regex(",
        "validate_anchored_regex(",
        "validate_unanchored_regex(",
        "compile_drop_keep(",
        // Issue #291: the shared budgeted entry point. These four had to
        // be added the moment the sites started routing through it —
        // without them `plan.rs`'s `label_replace` compile disappeared
        // from the census entirely, which is the failure mode this test
        // exists to prevent (a compile site nothing enumerates).
        "compile_user_regex(",
        "compile_user_regex_anchored(",
        "compile_user_regex_with(",
        "regex_compile_transient_bound_with(",
    ];

    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/logql");
    let mut files = Vec::new();
    collect_rs(&root, &mut files);
    files.sort();
    assert!(
        files.len() > 5,
        "the recursive walk found almost nothing: {files:?}"
    );

    let mut found: Vec<(String, Vec<(String, usize)>)> = Vec::new();
    for path in &files {
        let src = std::fs::read_to_string(path).expect("source");
        // Production halves only, and with comment text stripped — these
        // files document their own compile sites heavily, and a census
        // that counted its own prose would move whenever a doc comment
        // was edited. Same split `logql_logfmt_expr_matrix.rs` uses.
        let production = match src.find("\n#[cfg(test)]") {
            Some(i) => &src[..i],
            None => src.as_str(),
        };
        let production: String = production
            .lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");
        let counts: Vec<(String, usize)> = MARKERS
            .iter()
            .map(|m| ((*m).to_string(), production.matches(m).count()))
            .filter(|(_, n)| *n > 0)
            .collect();
        if !counts.is_empty() {
            let rel = path
                .strip_prefix(&root)
                .expect("under src/logql")
                .to_string_lossy()
                .replace('\\', "/");
            found.push((rel, counts));
        }
    }

    let named: Vec<(String, Vec<(String, usize)>)> = SITES
        .iter()
        .map(|(f, c, _)| {
            (
                (*f).to_string(),
                c.iter().map(|(m, n)| ((*m).to_string(), *n)).collect(),
            )
        })
        .collect();
    assert_eq!(
        found, named,
        "the set of regex-construction sites under `src/logql/**` has changed. Every site is a \
         place a user pattern can be accepted or refused, so add it to `SITES` **and** decide \
         whether a matrix position must cover it or why no user input reaches it. Do not \
         adjust a count to make this pass."
    );
    for (_, _, why) in SITES {
        assert!(
            why.len() > 60,
            "a site's disposition must say something checkable"
        );
    }
}

fn collect_rs(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    let entries =
        std::fs::read_dir(dir).unwrap_or_else(|e| panic!("reading {}: {e}", dir.display()));
    for entry in entries {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            collect_rs(&path, out);
        } else if path.extension().is_some_and(|x| x == "rs") {
            out.push(path);
        }
    }
}

// ---------------------------------------------------------------------
// The template axis
// ---------------------------------------------------------------------

/// **The template regex functions' accept/reject boundary does NOT match
/// the reference's**, and this is the measurement that says so.
///
/// Until this issue, `template/funcs.rs` said of these functions that
/// "the accept/reject boundary matches (both are RE2-class engines)", and
/// the ledger row `template-error-wording-residuals` said the same. Both
/// were false: **18 of the 20 probes below disagree**, and two of them
/// answer with a different STRING rather than a different verdict —
/// `a**` renders `zxz` from the input `x`, and `\p{Alphabetic}` renders
/// `z`. Those are wrong answers, not boundary differences.
///
/// The comment and the ledger row now point here. If either grows the old
/// sentence back, this test is what contradicts it — and
/// [`the_corrected_sentences_are_not_in_the_tree`] fails on the words
/// themselves.
#[test]
fn the_template_regex_boundary_does_not_match_the_reference() {
    use std::borrow::Cow;

    use pulsus_read::logql::template::{self, Template, TemplateEnv, TemplateKind};

    let mut disagree = 0usize;
    let mut agree = 0usize;
    for probe in TEMPLATE_AXIS {
        let body = format!("{{{{ regexReplaceAll `{}` .app `z` }}}}", probe.pattern);
        let compiled = template::compile(&body, TemplateKind::Line)
            .unwrap_or_else(|e| panic!("{:?}: template compile: {e}", probe.pattern));
        let Template::Full(prog) = compiled else {
            panic!("{:?}: expected a Full template", probe.pattern)
        };
        let labels: Vec<(Cow<'_, str>, Cow<'_, str>)> =
            vec![(Cow::Borrowed("app"), Cow::Borrowed("x"))];
        let budget = template::RenderBudget::default();
        let env = TemplateEnv::default();
        let got =
            match template::render_full(&prog, &labels, None, None, "the line", 0, &env, &budget) {
                Ok(r) => RenderVerdictOwned::Rendered(r.as_str().to_string()),
                Err(e) => {
                    // The pipeline turns exactly this into the per-line
                    // `__error__: TemplateFormatErr` the reference emits.
                    assert!(
                        e.msg.contains("error parsing regexp"),
                        "{:?}: a template exec error that is not a regex one: {}",
                        probe.pattern,
                        e.msg
                    );
                    RenderVerdictOwned::TemplateFormatErr
                }
            };
        assert_eq!(
            got,
            owned(probe.pulsus),
            "{:?}: PulsusDB's committed render verdict no longer holds",
            probe.pattern
        );
        if probe.reference == probe.pulsus {
            agree += 1;
        } else {
            disagree += 1;
        }
    }
    assert!(
        agree > 0 && disagree > 0,
        "the probe set must contain both agreements and disagreements ({agree} agree, \
         {disagree} disagree) — a table that only ever disagrees would prove nothing about the \
         boundary"
    );
    // The two wrong-ANSWER witnesses, named rather than counted: these
    // are not "we are more permissive", they are a different string.
    for (pattern, rendered) in [("a**", "zxz"), (r"\p{Alphabetic}", "z")] {
        let probe = TEMPLATE_AXIS
            .iter()
            .find(|t| t.pattern == pattern)
            .expect("witness present");
        assert_eq!(probe.reference, RenderVerdict::TemplateFormatErr);
        assert_eq!(probe.pulsus, RenderVerdict::Rendered(rendered));
    }
    eprintln!(
        "template regex axis: {disagree} of {} probes disagree",
        TEMPLATE_AXIS.len()
    );
}

#[derive(Debug, PartialEq, Eq)]
enum RenderVerdictOwned {
    Rendered(String),
    TemplateFormatErr,
}

fn owned(v: RenderVerdict) -> RenderVerdictOwned {
    match v {
        RenderVerdict::Rendered(s) => RenderVerdictOwned::Rendered(s.to_string()),
        RenderVerdict::TemplateFormatErr => RenderVerdictOwned::TemplateFormatErr,
    }
}

/// **The corrected sentences are gone from the tree, and cannot come back
/// by being re-wrapped.** A comment that points at a test is not closed
/// until the words it replaced are themselves an assertion.
///
/// The search is over the source with comment markers and **all
/// whitespace removed**, because both false sentences were line-wrapped
/// mid-phrase — `funcs.rs` broke `accept/reject` across two `//` lines
/// and the ledger broke `boundaries match` across two — so a literal
/// `contains` would have missed the very text it was written to forbid.
#[test]
fn the_corrected_sentences_are_not_in_the_tree() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    /// The three sentences this issue removed, whitespace-free and
    /// lowercased, each with what replaced it.
    const FORBIDDEN: &[(&str, &str)] = &[
        (
            "accept/rejectboundarymatch",
            "the boundary does NOT match — 18 of 20 probes disagree; see \
             `the_template_regex_boundary_does_not_match_the_reference`",
        ),
        (
            "accept/rejectboundariesmatch",
            "the boundary does NOT match — see the corrected \
             `template-error-wording-residuals` ledger row",
        ),
        (
            "issue#246replacesthisbody",
            "that instruction was withdrawn by the owner's 2026-08-08 ruling; #246 ships the \
             status pin, not a translation table",
        ),
    ];
    let files = [
        "crates/pulsus-read/src/logql/template/funcs.rs",
        "crates/pulsus-read/src/logql/pipeline.rs",
        "docs/benchmarks/logs-differential-ledger.md",
    ];
    for rel in files {
        let path = root.join(rel);
        let src = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
        let flat: String = src
            .replace("///", " ")
            .replace("//", " ")
            .chars()
            .filter(|c| !c.is_whitespace())
            .flat_map(|c| c.to_lowercase())
            .collect();
        for (needle, replacement) in FORBIDDEN {
            assert!(
                !flat.contains(needle),
                "{rel} has grown back a sentence issue #246 removed ({needle:?}) — {replacement}"
            );
        }
    }
}

// ---------------------------------------------------------------------
// The live legs (gated on PULSUSDB_LOGQL_DIFF_URL)
// ---------------------------------------------------------------------

/// Seconds curl will wait for the reference, and **not a round number
/// chosen for comfort**.
///
/// It was 30, and `class_alt_over_budget` failed the `schema-it` job with
/// `unexpected status 000` — curl never obtaining a status, rather than a
/// status the reference returned.
///
/// **`000` has TWO causes and this constant fixes NEITHER.** One is the
/// reference not being READY yet, which [`wait_for_reference`] closes.
/// The other is the reference's own 30 s HTTP write timeout, explained in
/// full on [`wait_for_reference`] — raising this from 30 s to 120 s did
/// not fix that either; it only stopped OUR deadline from racing the
/// server's, so the server's behaviour surfaces as what it is (curl 52 /
/// 56) instead of as our timeout (28). What this constant buys is that a
/// slow-but-successful answer is not reported as a failure.
///
/// The deadline half was diagnosed by constraining the container rather than guessing
/// (`podman run --cpus 2 --memory 4g`), which reproduces it exactly, and
/// the cause is that the reference genuinely needs that long: building a
/// ten-thousand-branch Unicode-class matcher is real work, and at the
/// four positions that construct a `NewFastRegexMatcher` — the two label
/// filters, `drop` and `keep` — it costs 10-27 s per query on two cores.
/// A cold container spikes past 30 s on its first few queries and settles
/// afterwards.
///
/// **Raising this does not weaken anything.** Every verdict is still
/// asserted at every position; what changes is that slow-but-successful
/// stops being reported as a failure. A genuine hang still fails, just
/// later. Lowering it back reintroduces a flake whose symptom (`000`)
/// looks nothing like its cause.
///
/// Measured alternatives, both rejected: no N inside the divergent band
/// `10,014..12,728` is cheap for the reference (10,014, the very bottom,
/// still peaks at 30.08 s cold), and the one other shape the cap newly
/// refuses — `(?i)[\p{L}x20000]` — costs 16-27 s at the same positions,
/// so it buys nothing.
///
/// **This is a hang-guard, not a performance assertion.** It says only
/// that the reference eventually answered; it says nothing about how
/// fast, and no row's passing time may be read as evidence the reference
/// was quick. The times above are the measurement — this constant is not.
///
/// **Verified on COLD containers, freshly created rather than restarted,
/// because cold start is where the spikes are:** four runs green, three
/// at 2 CPU / 4 GB and one at **1 CPU / 2 GB**, tighter than any CI
/// runner.
const REFERENCE_MAX_SECONDS: &str = "120";

/// How long [`wait_for_reference`] will wait for the container to become
/// serviceable before failing the run.
const REFERENCE_READY_SECONDS: u64 = 180;

/// **Waits for the reference to be serviceable before the first probe.**
///
/// The second cause of `000`, and the one a deadline cannot touch. The
/// evidence for it is a LOG LINE, not a duration: review saw a cold first
/// probe fail on a container that was not OOM-killed and had
/// `RestartCount=0`, whose log carried `empty ring`. Reproduced here on a
/// freshly created container: `ratestore.go:110 msg="error getting
/// ingester clients" err="empty ring"`, with the HTTP port refusing
/// connections for the first ~3 s and `/ready` answering 503 for ~23 s.
///
/// That probe took 47.7 s, and **that number is not cited as evidence
/// here, because nothing we have measured explains it** — see [`Probe`].
/// Warm re-probes on the same container answered 200. **CI creates a
/// fresh container every run, so the first probe is exactly where this
/// lives** — a longer deadline makes it less likely to coincide, not
/// gone.
///
/// **This closes the READINESS mode. It does not close every `000`, and
/// after four rounds of inferring from the symptom the rest turned out to
/// be ONE mechanism, not three.**
///
/// The branch used to describe three modes here — 28 (timed out), 56
/// (reset by peer), 52 (empty reply) — with one of them "unexplained".
/// That was wrong. All three are **the reference's own HTTP write
/// timeout**: `server.http-write-timeout` defaults to **30 s**
/// (`vendor/github.com/grafana/dskit/server/server.go:217 @ v3.7.4`,
/// wired into Go's `http.Server.WriteTimeout` at `:544`), and
/// `ci/logql/config.yaml` sets no timeout, so the default is what runs.
/// When it expires with nothing written, Go closes the connection: the
/// client sees an empty reply (52), or a reset (56) if it reads after the
/// RST, or — back when our own deadline was also 30 s — our timeout (28)
/// winning the race.
///
/// **Found by reading the reference's source and confirmed by moving its
/// timeout**, not by reasoning about the symptom: the same query at the
/// same N answers `000 | curl exit 52 | Empty reply from server` after
/// **6.28 s** with `http_server_write_timeout: 5s` and after **30.48 s**
/// with the shipped 30 s default, which is exactly the CI failure. The
/// symptom tracks the SERVER's timeout, not our client deadline — which
/// is why every round that raised OUR patience produced "another mode".
///
/// **Scope of that attribution:** it covers the `000`s whose exit code
/// was captured. The two 47.7 s / 47.8 s observations from before
/// [`Probe`] existed are NOT attributed to it and do not fit it; see
/// [`Probe`].
///
/// Runs once per test binary, not once per test: the three live legs
/// share a container, and this is a property of the container.
///
/// If readiness never arrives it PANICS with the last status and body
/// rather than proceeding — an unready reference would otherwise be
/// scored as a wall of `000` and read as a verdict mismatch.
fn wait_for_reference(base: &str) {
    static READY: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    READY.get_or_init(|| {
        let deadline =
            std::time::Instant::now() + std::time::Duration::from_secs(REFERENCE_READY_SECONDS);
        let mut last = String::from("no attempt completed");
        while std::time::Instant::now() < deadline {
            let ready = curl_probe("5", &[&format!("{base}/ready")]);
            let now = now_s();
            let probe = curl_probe(
                "15",
                &[
                    "-G",
                    &format!("{base}/loki/api/v1/query_range"),
                    "--data-urlencode",
                    "query={app=\"pulsus_readiness_probe\"}",
                    "--data-urlencode",
                    &format!("start={}", (now - 300) * 1_000_000_000),
                    "--data-urlencode",
                    &format!("end={}", now * 1_000_000_000),
                    "--data-urlencode",
                    "step=60s",
                ],
            );
            if ready.http_code == "200" && probe.http_code == "200" {
                return;
            }
            // Both CAUSES, not just both statuses — see [`Probe`].
            last = format!(
                "/ready: {} | query_range: {}",
                ready.describe(),
                probe.describe()
            );
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        panic!(
            "the reference at {base} never became serviceable within \
             {REFERENCE_READY_SECONDS}s (last: {last}). It must answer BOTH `/ready` 200 and a \
             trivial `query_range` 200 before any verdict is probed — an unready container \
             answers `000` on every position, which this file would otherwise score as a wall \
             of verdict mismatches"
        );
    });
}

/// **What one curl invocation observed, cause included.**
///
/// `000` is not a status. It is curl declining to produce one, and until
/// this struct existed the suite recorded it with no way to tell WHICH
/// refusal it was. That blindness is why issue #291's CI failure took
/// four rounds: a `000` at 47.8 s under a 120 s deadline, on a container
/// whose readiness gates were both already 200, is neither a timeout nor
/// unreadiness — and nobody could say what it was, because curl's exit
/// code and message were thrown away.
///
/// **Those two 2026-08-09 observations — 47.7 s and 47.8 s — remain
/// unattributed, deliberately.** They predate this struct, so no exit
/// code was captured for either, and they do not fit the mechanism the
/// later ones did: the write timeout's measured wall is **30.48 s** at
/// the shipped 30 s setting and **6.28 s** at a 5 s setting, tracking the
/// setting closely both times, and 47.7 fits neither. An attempt to
/// explain the gap by firing a heavy query before the container was
/// ready — on the theory that Go's write deadline starts when the request
/// is read, so ~17 s of startup plus 30 s would land near 47 — did not
/// reproduce it: the query was served in 13.68 s. They are dated
/// observations whose cause was not captured, and they are recorded that
/// way rather than assigned to the nearest known mechanism. That
/// assignment is the habit this issue spent four rounds paying for.
///
/// Curl distinguishes these, and the distinction is what produced the
/// diagnosis: `7` couldn't connect (nothing listening yet), `28`
/// operation timed out (our own [`REFERENCE_MAX_SECONDS`] expiring),
/// `52` empty reply from server, `56` recv failure / connection reset by
/// peer.
///
/// **52 and 56 are the diagnosed cause, not an open question.** They are
/// the reference's own 30 s HTTP write timeout closing the connection
/// with nothing written — 52 when the client reads the FIN, 56 when it
/// reads after the RST — and 28 was the same wall reached by our
/// deadline first, back when ours was also 30 s. One mechanism, three
/// presentations; [`wait_for_reference`] carries the source citation and
/// the two boundary measurements. This struct is how that was found, so
/// its own doc had better not still call it unexplained.
///
/// Carried on every probe and printed on every failure. Deliberately NOT
/// paired with a retry: retrying a `000` would turn CI green and destroy
/// the only signal there is.
#[derive(Debug)]
struct Probe {
    /// `"000"` when curl never got one.
    http_code: String,
    /// Curl's exit code — `0` on success. See the struct docs.
    exit_code: String,
    /// Curl's own error text, empty on success.
    err_msg: String,
    /// The response body, empty when there was none.
    body: String,
}

impl Probe {
    /// A one-line rendering for a panic or a failure record. Always names
    /// the cause when there is one.
    fn describe(&self) -> String {
        if self.err_msg.is_empty() && self.exit_code == "0" {
            format!("status {} ({})", self.http_code, truncate(&self.body, 200))
        } else {
            format!(
                "status {} — curl exit {} ({}); body {}",
                self.http_code,
                self.exit_code,
                self.err_msg,
                truncate(&self.body, 200)
            )
        }
    }
}

fn truncate(s: &str, n: usize) -> String {
    let s = s.trim();
    match s.char_indices().nth(n) {
        Some((i, _)) => format!("{}…", &s[..i]),
        None => s.to_string(),
    }
}

/// Runs curl with `args`, capturing the status, the body **and curl's own
/// exit code and message**.
///
/// The three write-out fields are one tab-separated line on stdout, so
/// the body can keep going to stderr (`-o /dev/stderr`) exactly as it did
/// before. `%{exitcode}` and `%{errormsg}` need curl 7.75+; the CI image
/// and this machine are both well past that, and if they were not the
/// fields would render literally and the assertion below would say so
/// rather than silently reporting nothing.
fn curl_probe(max_time: &str, args: &[&str]) -> Probe {
    let out = Command::new("curl")
        .args([
            "-s",
            "-S",
            "--max-time",
            max_time,
            "-o",
            "/dev/stderr",
            "-w",
        ])
        .arg("%{http_code}\t%{exitcode}\t%{errormsg}")
        .args(args)
        .output()
        .expect("curl");
    let written = String::from_utf8_lossy(&out.stdout).to_string();
    let mut fields = written.trim_end_matches('\n').split('\t');
    let http_code = fields.next().unwrap_or("").trim().to_string();
    let exit_code = fields.next().unwrap_or("").trim().to_string();
    let err_msg = fields.next().unwrap_or("").trim().to_string();
    assert!(
        !exit_code.is_empty() && !exit_code.contains("exitcode"),
        "curl did not expand `%{{exitcode}}` — this suite needs curl 7.75+ to record WHY a \
         probe failed, and recording `000` with no cause is what issue #291 spent four \
         rounds paying for. Got: {written:?}"
    );
    Probe {
        http_code,
        exit_code,
        err_msg,
        // Curl's own diagnostics go to stderr under `-S` too, so the body
        // is whatever precedes them; keeping the whole thing is right —
        // it is only ever read by a human diagnosing a failure.
        body: String::from_utf8_lossy(&out.stderr).trim().to_string(),
    }
}

fn now_s() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs()
}

/// One `query_range` probe. **The window ends at `now`, and that is
/// load-bearing** — see the module docs' trap 3.
fn reference_verdict(base: &str, query: &str) -> (Verdict, String) {
    let now = now_s();
    let probe = curl_probe(
        REFERENCE_MAX_SECONDS,
        &[
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
        ],
    );
    let body = probe.body.clone();
    match probe.http_code.as_str() {
        "200" => (Verdict::Accept, body),
        "400" => (Verdict::Reject, body),
        // Never `000` without its cause — see [`Probe`].
        _ => panic!("unexpected {} for {query:?}", probe.describe()),
    }
}

/// **Which (position, pattern) cells the LIVE legs re-probe, and the one
/// row they do not.**
///
/// Every cell is asserted from the committed table by the hermetic tests;
/// this decides only which are additionally re-measured against the
/// container on every run.
///
/// `class_alt_over_budget` is **not re-probed at any position** (owner
/// ruling, 2026-08-10, after four rounds and three positions). The reason
/// is a hard wall in the reference: **`server.http-write-timeout`
/// defaults to 30 s** (`vendor/github.com/grafana/dskit/server/server.go:217
/// @ v3.7.4`, wired into Go's `http.Server.WriteTimeout` at `:544`), and
/// `ci/logql/config.yaml` sets no timeout, so the default is what runs.
/// When it expires with nothing written, Go closes the connection and the
/// client gets an empty reply, never a status.
///
/// # What is known, and what is not
///
/// **Reproduced, on three machines.** The divergence itself: we refuse
/// `\p{L}|…` alternations from 10,014 atoms, the reference serves to
/// 12,728, band `10,014..12,728` — both endpoints bisected one atom at a
/// time, here and on the reviewer's hardware.
///
/// **The reference's wall, measured by moving it.** The same query at the
/// same N answers `000 | curl exit 52 | Empty reply from server` after
/// **6.28 s** with `http_server_write_timeout: 5s` and after **30.48 s**
/// with the shipped 30 s default.
///
/// **Refuted, each with a measurement, not an argument:** runner speed
/// alone; store contents (empty, one line, and ~10,000 lines all answer
/// in 0.65 s); CPU starvation (0.62 s at 0.25 CPU with data); container
/// death, restart or memory pressure (`OOMKilled=false`,
/// `RestartCount=0`, 0.42% CPU, 105 MiB at the moment of failure); our
/// own client deadline (the failure is the server's close, not ours); and
/// the theory that Go's write deadline starts when the request is READ,
/// so ~17 s of startup plus 30 s would land near 47 s — a heavy query
/// fired at a cold container was served in 13.68 s.
///
/// **NOT KNOWN: why `line_re` costs 0.6 s here and past 30 s on the CI
/// runner.** Four rounds, three positions, and the reference's own log is
/// silent on it — `caller=metrics.go` logs on query COMPLETION, so a
/// request the server closes without responding can never appear there.
/// With `log_level: info` shipped, a failing run captured 3,024 per-query
/// entries and **none** for this query: no `latency=slow`, nothing with a
/// query text over 60,000 chars, and a 29-second gap between the last
/// entry and the failure. That residue is recorded rather than resolved,
/// because an investigation that ends without naming what it did not
/// learn teaches nothing.
///
/// # What this costs
///
/// **This row is no longer re-verified against the reference on any run**,
/// so a change in the reference's behaviour here would go unnoticed until
/// someone re-measures by hand. It stays asserted from the recorded
/// measurement — 2026-08-09, pinned container, `200` at all eighteen
/// positions — and the whole row stays in `PATTERNS` and in the
/// `regex_compile_budget` divergence enumeration. Only the live re-check
/// is dropped, and only for this row: every other pattern is still put to
/// the container at every position on every run.
///
/// Nothing the investigation bought is given up with it — `log_level:
/// info`, the `docker logs` capture on failure, [`Probe`],
/// [`wait_for_reference`] and the wall-clock deadline all stay. Each was
/// earned by a failure and prevents a different one.
fn live_probe_is_affordable(_position: &str, pattern: &str) -> bool {
    pattern != "class_alt_over_budget"
}

/// **Re-measures every committed reference verdict against the pinned
/// container**, so the captured column cannot silently rot — including
/// the masked positions, whose whole value is the measurement that they
/// mask.
#[test]
fn live_matrix_against_the_reference() {
    let Ok(base) = std::env::var("PULSUSDB_LOGQL_DIFF_URL") else {
        eprintln!("PULSUSDB_LOGQL_DIFF_URL unset — skipping the live regex accept matrix");
        return;
    };
    wait_for_reference(&base);
    let mut points = matrix(POSITIONS);
    points.extend(matrix(MASKED));

    let mut disagree = Vec::new();
    let (mut accepts, mut rejects) = (0usize, 0usize);
    let mut skipped = 0usize;
    for pt in &points {
        let want = committed(pt.position, pt.pattern, Side::Reference);
        match want {
            Verdict::Accept => accepts += 1,
            Verdict::Reject => rejects += 1,
        }
        if !live_probe_is_affordable(pt.position.id, pt.pattern.id) {
            skipped += 1;
            continue;
        }
        let (got, body) = reference_verdict(&base, &pt.query);
        if got != want {
            disagree.push(format!(
                "{}\n    committed={want:?} container={got:?} {}",
                pt.label(),
                body.chars().take(160).collect::<String>()
            ));
        }
    }
    assert!(
        disagree.is_empty(),
        "{} of {} committed reference verdicts no longer match the pinned container — \
         re-capture the table, do not edit it to match one point:\n{}",
        disagree.len(),
        points.len(),
        disagree.join("\n")
    );
    assert!(
        accepts > 0 && rejects > 0,
        "both dispositions must be present on the reference side ({accepts} accept, \
         {rejects} reject); if they are not, the window is wrong"
    );
    // The window trap, checked rather than described: a line filter is a
    // pipeline-BUILD error there, so it must be 400 in THIS window and
    // 200 over one older than `query_ingesters_within`, while a selector
    // regex is 400 in both.
    let stale = now_s() - 30 * 24 * 3600;
    assert_eq!(
        reference_verdict(&base, r#"{app="x"} |~ "(""#).0,
        Verdict::Reject,
        "a malformed line filter must be refused in a window ending now"
    );
    assert_eq!(
        stale_verdict(&base, r#"{app="x"} |~ "(""#, stale),
        Verdict::Accept,
        "the reference must still SERVE a malformed line filter over a stale window — if it \
         does not, this leg's window is no longer load-bearing and the note saying it is must \
         be re-measured"
    );
    assert_eq!(
        stale_verdict(&base, r#"{app=~"("}"#, stale),
        Verdict::Reject,
        "a malformed selector regex is a parse error and must be refused in every window"
    );
    // The skip count is asserted, not merely reported: this narrowing is
    // scoped to ONE pattern at every position, and if it ever grew
    // silently the live leg would quietly stop measuring the matrix.
    assert_eq!(
        skipped, 18,
        "exactly the 18 cells of `class_alt_over_budget` — one per position, masked \
         included — may skip the live probe, and nothing else may. See \
         `live_probe_is_affordable`"
    );
    eprintln!(
        "re-measured {} of {} points against the reference ({skipped} skipped, all one \
         pattern the reference cannot answer inside its own 30s write timeout); the \
         committed column holds",
        points.len() - skipped,
        points.len()
    );
}

/// **The error-code census, put back to the container.**
///
/// This is the half that makes [`every_go_regexp_error_code_is_accounted_for`]
/// more than a self-consistency check. Two things:
///
/// 1. every captured fragment must still appear in the container's body
///    at the position it was captured from, so a pattern credited with a
///    code has to keep raising it;
/// 2. the SET of codes observed across the whole matrix must contain no
///    code this file calls unreachable. That is the assertion that would
///    have caught the first version's two false unreachability rows the
///    moment a pattern reaching them entered the set — and it is why
///    `ErrInvalidUTF8` and `ErrLarge` are covered rows now rather than
///    excuses.
#[test]
fn live_reference_error_codes_are_exactly_the_covered_set() {
    let Ok(base) = std::env::var("PULSUSDB_LOGQL_DIFF_URL") else {
        eprintln!("PULSUSDB_LOGQL_DIFF_URL unset — skipping the live error-code census");
        return;
    };
    wait_for_reference(&base);

    // (1) Each captured fragment, at its captured position.
    let mut wrong = Vec::new();
    for (pattern_id, position_id, fragment) in CAPTURED_REFERENCE_ERRORS {
        let position = POSITIONS
            .iter()
            .find(|p| p.id == *position_id)
            .expect("known position");
        let pattern = PATTERNS
            .iter()
            .find(|p| p.id == *pattern_id)
            .expect("known pattern");
        let query = position.template.replace("{P}", &pattern.literal());
        let (verdict, body) = reference_verdict(&base, &query);
        if verdict != Verdict::Reject || !body.contains(fragment) {
            wrong.push(format!(
                "{position_id}/{pattern_id}: expected a 400 containing {fragment:?}, got \
                 {verdict:?} {}",
                body.chars().take(160).collect::<String>()
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} captured reference errors no longer hold — re-capture, do not relax the \
         fragment:\n{}",
        wrong.len(),
        wrong.join("\n")
    );

    // (2) No excused code may appear anywhere in the matrix's reference
    //     bodies. Scanning every point, not just the captured ones.
    let mut points = matrix(POSITIONS);
    points.extend(matrix(MASKED));
    let mut observed: Vec<&str> = Vec::new();
    for pt in &points {
        // Same wall, same scope — see `live_probe_is_affordable`. This
        // census only ever learns from cells the reference REJECTS, and
        // this row is a `200` at every position, so skipping the
        // whole row removes no code from the observed set.
        if !live_probe_is_affordable(pt.position.id, pt.pattern.id) {
            continue;
        }
        let (verdict, body) = reference_verdict(&base, &pt.query);
        if verdict != Verdict::Reject {
            continue;
        }
        for (code, message) in GO_ERROR_CODES {
            if body.contains(&format!("error parsing regexp: {message}: "))
                && !observed.contains(code)
            {
                observed.push(code);
            }
        }
    }
    for (code, why) in UNREACHABLE_CODES {
        assert!(
            !observed.contains(code),
            "`{code}` is recorded unreachable — \"{why}\" — but the container raised it inside \
             this matrix. The reason is false; cover the code with a pattern instead of \
             excusing it."
        );
    }
    let mut covered: Vec<&str> = PATTERNS.iter().filter_map(|p| p.go_code).collect();
    covered.sort_unstable();
    covered.dedup();
    let mut observed_sorted = observed.clone();
    observed_sorted.sort_unstable();
    assert_eq!(
        observed_sorted, covered,
        "the codes the container actually raised across this matrix are not the codes the \
         patterns claim to cover"
    );
    eprintln!(
        "observed {} distinct reference error codes across {} points; {} excused, none observed",
        observed.len(),
        points.len(),
        UNREACHABLE_CODES.len()
    );
}

fn stale_verdict(base: &str, query: &str, end_s: u64) -> Verdict {
    let probe = curl_probe(
        REFERENCE_MAX_SECONDS,
        &[
            "-G",
            &format!("{base}/loki/api/v1/query_range"),
            "--data-urlencode",
            &format!("query={query}"),
            "--data-urlencode",
            &format!("start={}", (end_s - 3600) * 1_000_000_000),
            "--data-urlencode",
            &format!("end={}", end_s * 1_000_000_000),
            "--data-urlencode",
            "step=60s",
        ],
    );
    match probe.http_code.as_str() {
        "200" => Verdict::Accept,
        "400" => Verdict::Reject,
        _ => panic!("unexpected {} for {query:?}", probe.describe()),
    }
}

/// **Re-measures the reference half of the template axis**: pushes one
/// line and reads the `__error__` label back, so the committed
/// `RenderVerdict::reference` column is a measurement rather than a
/// memory.
#[test]
fn live_template_axis_against_the_reference() {
    let Ok(base) = std::env::var("PULSUSDB_LOGQL_DIFF_URL") else {
        eprintln!("PULSUSDB_LOGQL_DIFF_URL unset — skipping the live template axis");
        return;
    };
    wait_for_reference(&base);
    let ts = (now_s() - 10) * 1_000_000_000;
    let payload = format!(
        r#"{{"streams":[{{"stream":{{"app":"x","job":"pulsus_it246"}},"values":[["{ts}","x"]]}}]}}"#
    );
    let push = curl_probe(
        REFERENCE_MAX_SECONDS,
        &[
            "-H",
            "Content-Type: application/json",
            "--data-binary",
            &payload,
            &format!("{base}/loki/api/v1/push"),
        ],
    );
    assert_eq!(push.http_code, "204", "push failed: {}", push.describe());
    std::thread::sleep(std::time::Duration::from_secs(2));

    let mut wrong = Vec::new();
    for probe in TEMPLATE_AXIS {
        let query = format!(
            r#"{{job="pulsus_it246"}} | line_format "{}""#,
            logql_quote(&format!(
                "{{{{ regexReplaceAll `{}` .app `z` }}}}",
                probe.pattern
            ))
        );
        let now = now_s();
        let out = curl_probe(
            REFERENCE_MAX_SECONDS,
            &[
                "-G",
                &format!("{base}/loki/api/v1/query_range"),
                "--data-urlencode",
                &format!("query={query}"),
                "--data-urlencode",
                &format!("start={}", (now - 300) * 1_000_000_000),
                "--data-urlencode",
                &format!("end={}", (now + 60) * 1_000_000_000),
                "--data-urlencode",
                "limit=10",
                "--data-urlencode",
                "direction=backward",
            ],
        );
        let body = out.body.clone();
        assert!(
            body.contains("\"status\":\"success\""),
            "{:?}: the template axis is a 200 axis, never a status — {}",
            probe.pattern,
            out.describe()
        );
        // A bad regex surfaces as the per-line `__error__` label, not as
        // a status and not as a body.
        let got = if body.contains(r#""__error__":"TemplateFormatErr""#) {
            RenderVerdict::TemplateFormatErr
        } else {
            match &probe.reference {
                // Only the verdict is re-measured live; the rendered
                // string is pinned hermetically for OUR side, where it is
                // the finding.
                RenderVerdict::Rendered(s) => RenderVerdict::Rendered(s),
                RenderVerdict::TemplateFormatErr => RenderVerdict::Rendered("<no error label>"),
            }
        };
        if got != probe.reference {
            wrong.push(format!(
                "{:?}: committed {:?} got {got:?}",
                probe.pattern, probe.reference
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} committed reference template verdicts no longer hold:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
    eprintln!(
        "re-measured {} template probes against the reference",
        TEMPLATE_AXIS.len()
    );
}
