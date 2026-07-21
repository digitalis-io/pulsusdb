//! `.test`-file grammar: the **executed subset** the M6-01 driver commits
//! to (issue #64 plan v2 Δ2), mirroring the upstream promqltest directive
//! regexes at the pinned v3.13.0 SHA (`promql/promqltest/test.go`):
//!
//! - `clear`
//! - `load <step>` (float series only; base epoch `T0 = 0 ms`)
//! - `eval[_ordered|_fail] instant at <dur> <expr>` with result lines,
//!   `expected_fail_message <msg>` / `expected_fail_regexp <pat>` for
//!   `eval_fail`
//! - `eval[_fail] range from <dur> to <dur> step <dur> <expr>`
//! - block-form `expect fail [msg:<s>|regex:<p>]` and
//!   `expect string <quoted>` result lines (issue #86, M6-08d — the
//!   executable subset of upstream's `expect` family; `parseExpect`/
//!   `parseAsStringLiteral` at the pinned SHA)
//! - block-form `expect warn|no_warn|info|no_info [msg:<s>|regex:<p>]`
//!   annotation assertions (issue #124, M7-A6 — checked by `runner.rs`
//!   against the captured [`pulsus_promql::Annotations`] channel), and
//!   `{{…}}` native-histogram sample literals in `load`/result lines
//!   ([`super::histogram_literal`])
//!
//! Everything else in the upstream grammar (`eval_warn`/`eval_info`, the
//! block `expect ordered` form and `expect range vector`,
//! `load_with_nhcb`, `@st` start-timestamp lines) is a **deferred
//! directive**: [`scan_deferred_directives`] detects them before grammar
//! parsing, and the corpus test requires any file using one to be listed
//! — loudly, wholesale — in `corpus/skip-manifest.json` with an
//! activation issue (plan v2 Δ2's skip-manifest contract). A directive
//! recognised by *neither* the executed subset nor the deferred scan is a
//! hard parse error, never a silent skip. (The pre-existing `eval_ordered`
//! PREFIX directive stays executable; only the block `expect ordered`
//! form is deferred — issue #86 plan v2 Δ3.)

use std::collections::{BTreeMap, BTreeSet};

use super::series::{SeqValue, parse_series_line, scan_signed_number};

/// Milliseconds in each Prometheus duration unit (`model.ParseDuration`).
const UNIT_MS: &[(&str, i64)] = &[
    ("ms", 1),
    ("s", 1_000),
    ("m", 60_000),
    ("h", 3_600_000),
    ("d", 24 * 3_600_000),
    ("w", 7 * 24 * 3_600_000),
    ("y", 365 * 24 * 3_600_000),
];

/// Parses a Prometheus duration (`1h30m`, `5m`, `100ms`, bare `0`) to
/// milliseconds. Unsigned, like upstream `model.ParseDuration`.
pub fn parse_duration_ms(s: &str) -> Result<i64, String> {
    let s = s.trim();
    if s == "0" {
        return Ok(0);
    }
    let mut total: i64 = 0;
    let mut rest = s;
    if rest.is_empty() {
        return Err("empty duration".to_string());
    }
    while !rest.is_empty() {
        let digits_end = rest
            .char_indices()
            .find(|(_, c)| !c.is_ascii_digit())
            .map(|(i, _)| i)
            .unwrap_or(rest.len());
        if digits_end == 0 {
            return Err(format!("invalid duration {s:?} (expected digits)"));
        }
        let n: i64 = rest[..digits_end]
            .parse()
            .map_err(|e| format!("invalid duration {s:?}: {e}"))?;
        rest = &rest[digits_end..];
        // Longest-match the unit: `ms` before `m`.
        let Some((unit, ms)) = UNIT_MS
            .iter()
            .filter(|(u, _)| rest.starts_with(u))
            .max_by_key(|(u, _)| u.len())
        else {
            return Err(format!("invalid duration {s:?} (missing/unknown unit)"));
        };
        total += n * ms;
        rest = &rest[unit.len()..];
    }
    Ok(total)
}

/// One `load` block series.
#[derive(Debug, Clone)]
pub struct LoadSeries {
    pub labels: BTreeMap<String, String>,
    pub values: Vec<SeqValue>,
}

/// One expected result series of an `eval` block (labels include
/// `__name__` when the result line carries a metric name).
#[derive(Debug, Clone)]
pub struct ExpectedSeries {
    pub labels: BTreeMap<String, String>,
    pub values: Vec<SeqValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvalMode {
    Pass,
    /// `eval_ordered` — instant-vector results compared as an ordered list.
    Ordered,
    /// `eval_fail` — the query must error; `expected_fail_message` is a
    /// substring assertion, `expected_fail_regexp` a regex match, both
    /// against the error `Display` (plan v2 Δ2).
    Fail,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EvalKind {
    Instant {
        at_ms: i64,
    },
    Range {
        from_ms: i64,
        to_ms: i64,
        step_ms: i64,
    },
}

#[derive(Debug, Clone)]
pub enum Expected {
    /// Instant-vector result lines (possibly empty = expects an empty
    /// vector).
    Vector(Vec<ExpectedSeries>),
    /// A single bare-number result line (scalar-typed query).
    Scalar(f64),
    /// Range result lines: per-series value sequences, positionally one
    /// per step (`_` = the series has no point at that step).
    Matrix(Vec<ExpectedSeries>),
    /// `eval_fail`'s (or the block `expect fail` directive's, issue #86)
    /// expectation.
    Fail {
        message: Option<String>,
        regexp: Option<String>,
    },
    /// `expect string <quoted>` (issue #86): the instant query must
    /// evaluate to exactly these bytes (a Go string is a byte slice, so
    /// the literal channel is byte-exact — issue #86's fix; see
    /// [`go_unquote`]).
    String(Vec<u8>),
}

/// One block-form `expect warn|no_warn|info|no_info`'s optional
/// `msg:`/`regex:` match tag (issue #124, M7-A6) — mirrors upstream
/// `expectCmd`/`CheckMatch` (`promqltest/test.go:1096-1111`): a bare
/// directive matches any non-empty annotation of that kind, `msg:`
/// requires an exact-text match, `regex:` a pattern match.
#[derive(Debug, Clone)]
pub enum AnnotationMatch {
    Any,
    Message(String),
    Regex(String),
}

#[derive(Debug, Clone)]
pub struct EvalCmd {
    pub line: usize,
    pub query: String,
    pub kind: EvalKind,
    pub mode: EvalMode,
    pub expected: Expected,
    /// `true` for the bare `eval instant <expr>` form (no `at` clause,
    /// eval time defaults to `T0`) — counted so the proof corpus provably
    /// exercises it.
    pub bare_instant: bool,
    /// `true` when the block used the `expect fail` directive (issue
    /// #86) — the mode was upgraded to [`EvalMode::Fail`], but the
    /// directive counts track it separately from `eval_fail`.
    pub expect_fail: bool,
    /// `true` when the block used `expect string` (issue #86).
    pub expect_string: bool,
    /// `expect warn [msg:/regex:]` directives (issue #124, M7-A6) — every
    /// entry must match at least one actual warning, and every actual
    /// warning must match at least one entry (upstream
    /// `validateExpectedAnnotationsOfType`).
    pub expect_warn: Vec<AnnotationMatch>,
    /// `true` when the block used bare `expect no_warn` — asserts zero
    /// warning annotations. Mutually exclusive with `expect_warn`
    /// (upstream `validateExpectedCmds`).
    pub expect_no_warn: bool,
    /// `expect info [msg:/regex:]` directives — see `expect_warn`.
    pub expect_info: Vec<AnnotationMatch>,
    /// `true` when the block used bare `expect no_info`. Mutually
    /// exclusive with `expect_info`.
    pub expect_no_info: bool,
}

#[derive(Debug, Clone)]
pub enum Command {
    Clear,
    Load {
        step_ms: i64,
        series: Vec<LoadSeries>,
    },
    Eval(EvalCmd),
}

/// The deferred-directive inventory (plan v2 Δ2): each variant names one
/// upstream directive family the executed subset does not run, with a
/// committed activation home in `corpus/skip-manifest.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DeferredDirective {
    /// The still-deferred block-`expect` forms: `expect ordered` (no
    /// activatable file uses it — every carrier is also blocked by another
    /// directive; plan v2 Δ3) and `expect range vector`. `expect fail`/
    /// `expect string` (issue #86) and `expect warn|no_warn|info|no_info`
    /// (issue #124, M7-A6) are EXECUTABLE and never route here.
    ExpectLine,
    /// `eval_warn …` (annotation assertion).
    EvalWarn,
    /// `eval_info …` (annotation assertion).
    EvalInfo,
    /// `load_with_nhcb …` (native-histogram-compatible bucket conversion).
    LoadWithNhcb,
    /// `metric@st …` start-timestamp definition lines.
    StartTimestampLine,
}

impl DeferredDirective {
    /// The stable name used in `corpus/skip-manifest.json`.
    pub fn name(self) -> &'static str {
        match self {
            DeferredDirective::ExpectLine => "expect",
            DeferredDirective::EvalWarn => "eval_warn",
            DeferredDirective::EvalInfo => "eval_info",
            DeferredDirective::LoadWithNhcb => "load_with_nhcb",
            DeferredDirective::StartTimestampLine => "@st",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        [
            DeferredDirective::ExpectLine,
            DeferredDirective::EvalWarn,
            DeferredDirective::EvalInfo,
            DeferredDirective::LoadWithNhcb,
            DeferredDirective::StartTimestampLine,
        ]
        .into_iter()
        .find(|d| d.name() == name)
    }
}

/// Comment-strips and trims a file's lines, exactly like upstream
/// `getLines`: whole-line `#` comments become blank lines (block
/// separators are preserved positionally).
pub fn clean_lines(text: &str) -> Vec<String> {
    text.lines()
        .map(|l| {
            let t = l.trim();
            if t.starts_with('#') {
                String::new()
            } else {
                t.to_string()
            }
        })
        .collect()
}

/// Scans a whole file for deferred directives (before any grammar
/// parsing). A non-empty result routes the file to the skip-manifest
/// contract; an empty result means the file is fully executable under the
/// committed subset.
pub fn scan_deferred_directives(text: &str) -> BTreeSet<DeferredDirective> {
    let mut out = BTreeSet::new();
    for line in clean_lines(text) {
        if line.is_empty() {
            continue;
        }
        let first_word = line.split_ascii_whitespace().next().unwrap_or("");
        if first_word == "expect" {
            // `expect` is deferred IFF its second token is `ordered` or
            // `range` (`expect range vector`); `expect fail`/`expect
            // string` (issue #86) and `expect warn|no_warn|info|no_info`
            // (issue #124, M7-A6) are part of the executed subset. Any
            // OTHER second token is left for the grammar parser, which
            // hard-errors on it (loud, never skipped).
            let second = line.split_ascii_whitespace().nth(1).unwrap_or("");
            if matches!(second, "ordered" | "range") {
                out.insert(DeferredDirective::ExpectLine);
            }
        }
        if first_word == "eval_warn" {
            out.insert(DeferredDirective::EvalWarn);
        }
        if first_word == "eval_info" {
            out.insert(DeferredDirective::EvalInfo);
        }
        if first_word == "load_with_nhcb" {
            out.insert(DeferredDirective::LoadWithNhcb);
        }
        // Upstream `isSTLine`: the first whitespace-delimited token ends
        // with `@st` (no space before it).
        if first_word.ends_with("@st") {
            out.insert(DeferredDirective::StartTimestampLine);
        }
    }
    out
}

fn err_at(file: &str, line_no: usize, msg: impl std::fmt::Display) -> String {
    format!("{file}:{}: {msg}", line_no + 1)
}

/// Parses a fully-executable `.test` file into commands. Any directive or
/// malformed line outside the executed subset is a hard error carrying
/// `file:line` (loud, never skipped). Callers must run
/// [`scan_deferred_directives`] first — a deferred directive reaching this
/// parser is also a hard error (defense in depth).
pub fn parse_file(file: &str, text: &str) -> Result<Vec<Command>, String> {
    let lines = clean_lines(text);
    let mut commands = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let line = &lines[i];
        if line.is_empty() {
            i += 1;
            continue;
        }
        let first_word = line.split_ascii_whitespace().next().unwrap_or("");
        match first_word {
            "clear" => {
                commands.push(Command::Clear);
                i += 1;
            }
            "load" => {
                let (cmd, next) = parse_load(file, &lines, i)?;
                commands.push(cmd);
                i = next;
            }
            w if w == "eval"
                || w == "eval_ordered"
                || w == "eval_fail"
                || w == "eval_warn"
                || w == "eval_info" =>
            {
                let (cmd, next) = parse_eval(file, &lines, i)?;
                commands.push(Command::Eval(cmd));
                i = next;
            }
            other => {
                return Err(err_at(
                    file,
                    i,
                    format!(
                        "unrecognised directive {other:?} — not in the executed subset and \
                         not a known deferred directive (extend the driver or classify it)"
                    ),
                ));
            }
        }
    }
    Ok(commands)
}

fn parse_load(file: &str, lines: &[String], start: usize) -> Result<(Command, usize), String> {
    let header = &lines[start];
    let step = header
        .strip_prefix("load")
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| err_at(file, start, "invalid load command (load <step:duration>)"))?;
    let step_ms = parse_duration_ms(step).map_err(|e| err_at(file, start, e))?;

    let mut series = Vec::new();
    let mut i = start + 1;
    while i < lines.len() && !lines[i].is_empty() {
        let (labels, values) = parse_series_line(&lines[i]).map_err(|e| err_at(file, i, e))?;
        series.push(LoadSeries { labels, values });
        i += 1;
    }
    Ok((Command::Load { step_ms, series }, i))
}

/// Strips a leading `word` from `s` only when it stands alone (followed
/// by whitespace or end-of-string), returning the whitespace-trimmed
/// remainder — so `instant`/`at` never match a longer identifier prefix.
fn strip_word<'a>(s: &'a str, word: &str) -> Option<&'a str> {
    let rest = s.strip_prefix(word)?;
    if rest.is_empty() || rest.starts_with(char::is_whitespace) {
        Some(rest.trim_start())
    } else {
        None
    }
}

fn parse_eval(file: &str, lines: &[String], start: usize) -> Result<(EvalCmd, usize), String> {
    let header = &lines[start];
    let mut words = header.split_ascii_whitespace();
    let directive = words.next().unwrap_or("");

    let mode = match directive {
        "eval" => EvalMode::Pass,
        "eval_ordered" => EvalMode::Ordered,
        "eval_fail" => EvalMode::Fail,
        // Defense in depth: `scan_deferred_directives` routes these files
        // to the skip-manifest before this parser ever runs.
        other => {
            return Err(err_at(
                file,
                start,
                format!("deferred eval directive {other:?} reached the executed-grammar parser"),
            ));
        }
    };

    let rest = header[directive.len()..].trim_start();
    let (kind, query, bare_instant) = if let Some(rest) = strip_word(rest, "instant") {
        // `at <duration>` is optional per the binding grammar
        // (`eval [...] instant [at <dur>] <expr>`); when absent the eval
        // time is `T0 = 0 ms`. Verified against upstream at the pinned
        // SHA (#64 code-review fix 1): upstream's `patEvalInstant` regex
        // also makes the clause optional, but its follow-up
        // `model.ParseDuration("")` then rejects the empty capture
        // ("empty duration string", common v0.69.0 `model/time.go`) — so
        // upstream has no *reachable* default; the plan grammar's
        // optional form governs here, and `T0` (upstream's
        // `testStartTime`) is its natural anchor. Like upstream's regex,
        // a query whose first token is literally `at` cannot be written
        // in the bare form (the clause parse wins).
        let (at_ms, query, bare) = match strip_word(rest, "at") {
            Some(rest) => {
                let (at_str, query) = rest.split_once(char::is_whitespace).ok_or_else(|| {
                    err_at(file, start, "eval instant at <duration> requires a query")
                })?;
                let at_ms = parse_duration_ms(at_str).map_err(|e| err_at(file, start, e))?;
                (at_ms, query.trim().to_string(), false)
            }
            None => (0, rest.to_string(), true),
        };
        if query.is_empty() {
            return Err(err_at(file, start, "eval instant requires a query"));
        }
        (EvalKind::Instant { at_ms }, query, bare)
    } else if let Some(rest) = strip_word(rest, "range") {
        if mode == EvalMode::Ordered {
            return Err(err_at(
                file,
                start,
                "eval_ordered is not valid for range queries (upstream regex excludes it)",
            ));
        }
        let rest = rest.trim_start();
        let toks: Vec<&str> = rest.split_ascii_whitespace().collect();
        // from <dur> to <dur> step <dur> <query...>
        if toks.len() < 7 || toks[0] != "from" || toks[2] != "to" || toks[4] != "step" {
            return Err(err_at(
                file,
                start,
                "eval range must be 'eval range from <dur> to <dur> step <dur> <query>'",
            ));
        }
        let from_ms = parse_duration_ms(toks[1]).map_err(|e| err_at(file, start, e))?;
        let to_ms = parse_duration_ms(toks[3]).map_err(|e| err_at(file, start, e))?;
        let step_ms = parse_duration_ms(toks[5]).map_err(|e| err_at(file, start, e))?;
        if to_ms < from_ms {
            return Err(err_at(file, start, "eval range: 'to' is before 'from'"));
        }
        if step_ms <= 0 {
            return Err(err_at(file, start, "eval range: step must be positive"));
        }
        // The query is everything after the 6th token. Token offsets are
        // recovered progressively (a plain `find` of the step token could
        // land on an identical earlier duration token).
        let mut pos = 0usize;
        let mut token_starts = Vec::with_capacity(7);
        for tok in rest.split_ascii_whitespace().take(7) {
            let idx = rest[pos..]
                .find(tok)
                .expect("token came from this same string")
                + pos;
            token_starts.push(idx);
            pos = idx + tok.len();
        }
        let query = rest[token_starts[6]..].trim().to_string();
        if query.is_empty() {
            return Err(err_at(file, start, "eval range requires a query"));
        }
        (
            EvalKind::Range {
                from_ms,
                to_ms,
                step_ms,
            },
            query,
            false,
        )
    } else {
        return Err(err_at(
            file,
            start,
            "eval must be 'instant [at …]' or 'range from … to … step …'",
        ));
    };

    // Result lines until the next blank line.
    let mut i = start + 1;
    let mut mode = mode;
    // The PREFIX directive's own fail mode (`eval_fail`) — kept separate
    // from a block-`expect fail` upgrade so `expected_fail_message`/
    // `expected_fail_regexp` lines stay legal ONLY under `eval_fail`
    // (upstream pairs `expect fail` with inline `msg:`/`regex:` instead).
    let directive_fail = mode == EvalMode::Fail;
    let mut expect_fail = false;
    let mut expect_string: Option<Vec<u8>> = None;
    let mut fail_message: Option<String> = None;
    let mut fail_regexp: Option<String> = None;
    let mut expect_warn: Vec<AnnotationMatch> = Vec::new();
    let mut expect_no_warn = false;
    let mut expect_info: Vec<AnnotationMatch> = Vec::new();
    let mut expect_no_info = false;
    let mut result_series: Vec<ExpectedSeries> = Vec::new();
    let mut scalar: Option<f64> = None;

    while i < lines.len() && !lines[i].is_empty() {
        let line = &lines[i];
        if directive_fail {
            if let Some(msg) = line.strip_prefix("expected_fail_message") {
                fail_message = Some(msg.trim().to_string());
                i += 1;
                continue;
            }
            if let Some(pat) = line.strip_prefix("expected_fail_regexp") {
                fail_regexp = Some(pat.trim().to_string());
                i += 1;
                continue;
            }
            return Err(err_at(
                file,
                i,
                "eval_fail accepts only expected_fail_message/expected_fail_regexp lines",
            ));
        }

        // Block-form `expect` directives (issue #86; issue #124 M7-A6 adds
        // the annotation forms). Like upstream, only a line whose FIRST
        // whitespace-delimited token is literally `expect` routes here —
        // a metric named `expect` is still writable as `expect{}`.
        if line.split_ascii_whitespace().next() == Some("expect") {
            if scalar.is_some() || !result_series.is_empty() {
                return Err(err_at(
                    file,
                    i,
                    "an `expect` directive cannot follow result lines in one eval block",
                ));
            }
            parse_expect_line(
                file,
                i,
                line,
                &kind,
                &mut expect_fail,
                &mut fail_message,
                &mut fail_regexp,
                &mut expect_string,
                &mut expect_warn,
                &mut expect_no_warn,
                &mut expect_info,
                &mut expect_no_info,
            )?;
            i += 1;
            continue;
        }
        if expect_fail || expect_string.is_some() {
            return Err(err_at(
                file,
                i,
                "result lines cannot follow an `expect fail`/`expect string` directive",
            ));
        }

        // A bare number = scalar expectation (upstream tries parseNumber
        // first).
        if let Some(v) = parse_scalar_line(line) {
            if !result_series.is_empty() || scalar.is_some() {
                return Err(err_at(
                    file,
                    i,
                    "a scalar result line must be the only result line",
                ));
            }
            scalar = Some(v);
            i += 1;
            continue;
        }

        let (labels, values) = parse_series_line(line).map_err(|e| err_at(file, i, e))?;
        if values.is_empty() {
            return Err(err_at(file, i, "result series has no values"));
        }
        if values.contains(&SeqValue::Stale) {
            return Err(err_at(file, i, "'stale' is not valid in a result line"));
        }
        if matches!(kind, EvalKind::Instant { .. }) && values.len() != 1 {
            return Err(err_at(
                file,
                i,
                "multiple values in an instant expectation are not allowed (upstream requires \
                 the deferred 'expect range vector' directive for that)",
            ));
        }
        result_series.push(ExpectedSeries { labels, values });
        i += 1;
    }

    if expect_fail {
        mode = EvalMode::Fail;
    }
    let used_expect_string = expect_string.is_some();
    let expected = if mode == EvalMode::Fail {
        Expected::Fail {
            message: fail_message,
            regexp: fail_regexp,
        }
    } else if let Some(s) = expect_string {
        Expected::String(s)
    } else if let Some(v) = scalar {
        Expected::Scalar(v)
    } else {
        match kind {
            EvalKind::Instant { .. } => Expected::Vector(result_series),
            EvalKind::Range { .. } => Expected::Matrix(result_series),
        }
    };

    Ok((
        EvalCmd {
            line: start + 1,
            query,
            kind,
            mode,
            expected,
            bare_instant,
            expect_fail,
            expect_string: used_expect_string,
            expect_warn,
            expect_no_warn,
            expect_info,
            expect_no_info,
        },
        i,
    ))
}

/// Parses one block-form `expect …` line (issue #86). Executable forms:
/// `expect fail [msg:<s>|regex:<p>]` (upstream `patExpect`, test.go:55 —
/// the optional tail must be `msg:`/`regex:`-tagged) and
/// `expect string <quoted>` (upstream `parseAsStringLiteral`). The
/// deferred forms (`ordered`/`warn`/`no_warn`/`info`/`no_info`/`range
/// vector`) are routed to the skip-manifest by
/// [`scan_deferred_directives`] before this parser runs — reaching here
/// is a hard error (defense in depth), as is any unrecognised form.
#[allow(clippy::too_many_arguments)]
fn parse_expect_line(
    file: &str,
    line_no: usize,
    line: &str,
    kind: &EvalKind,
    expect_fail: &mut bool,
    fail_message: &mut Option<String>,
    fail_regexp: &mut Option<String>,
    expect_string: &mut Option<Vec<u8>>,
    expect_warn: &mut Vec<AnnotationMatch>,
    expect_no_warn: &mut bool,
    expect_info: &mut Vec<AnnotationMatch>,
    expect_no_info: &mut bool,
) -> Result<(), String> {
    if line == "expect string" {
        return Err(err_at(
            file,
            line_no,
            "expected string literal not valid - a quoted string literal is required",
        ));
    }
    if let Some(literal) = line.strip_prefix("expect string ") {
        if matches!(kind, EvalKind::Range { .. }) {
            return Err(err_at(
                file,
                line_no,
                "expect string is only valid for an instant eval",
            ));
        }
        if expect_string.is_some() || *expect_fail {
            return Err(err_at(
                file,
                line_no,
                "expect string cannot repeat or combine with expect fail",
            ));
        }
        *expect_string = Some(go_unquote(literal).map_err(|e| err_at(file, line_no, e))?);
        return Ok(());
    }

    let mut words = line.split_ascii_whitespace();
    let _expect = words.next();
    match words.next() {
        Some("fail") => {
            if *expect_fail || expect_string.is_some() {
                return Err(err_at(
                    file,
                    line_no,
                    "invalid expect lines, multiple expect fail lines are not allowed",
                ));
            }
            *expect_fail = true;
            match parse_optional_tag(line, "fail").map_err(|e| err_at(file, line_no, e))? {
                AnnotationMatch::Any => {}
                AnnotationMatch::Message(m) => *fail_message = Some(m),
                AnnotationMatch::Regex(p) => *fail_regexp = Some(p),
            }
            Ok(())
        }
        // Issue #124 (M7-A6): the annotation directives — checked in
        // `runner.rs` against the [`pulsus_promql::Annotations`] channel
        // `evaluate()` returns. `warn`/`info` accumulate match patterns
        // (multiple lines build a set, upstream
        // `validateExpectedAnnotationsOfType`); `no_warn`/`no_info` are
        // bare presence assertions (upstream's tag-less regex branch —
        // no vendored corpus file tags them, so a tail there is a loud
        // error rather than a silently-ignored extension).
        Some("warn") => {
            if *expect_no_warn {
                return Err(err_at(
                    file,
                    line_no,
                    "invalid expect lines, warn and no_warn cannot be used together",
                ));
            }
            expect_warn
                .push(parse_optional_tag(line, "warn").map_err(|e| err_at(file, line_no, e))?);
            Ok(())
        }
        Some("no_warn") => {
            if !expect_warn.is_empty() {
                return Err(err_at(
                    file,
                    line_no,
                    "invalid expect lines, warn and no_warn cannot be used together",
                ));
            }
            if line.trim() != "expect no_warn" {
                return Err(err_at(
                    file,
                    line_no,
                    "expect no_warn takes no msg:/regex: tail",
                ));
            }
            *expect_no_warn = true;
            Ok(())
        }
        Some("info") => {
            if *expect_no_info {
                return Err(err_at(
                    file,
                    line_no,
                    "invalid expect lines, info and no_info cannot be used together",
                ));
            }
            expect_info
                .push(parse_optional_tag(line, "info").map_err(|e| err_at(file, line_no, e))?);
            Ok(())
        }
        Some("no_info") => {
            if !expect_info.is_empty() {
                return Err(err_at(
                    file,
                    line_no,
                    "invalid expect lines, info and no_info cannot be used together",
                ));
            }
            if line.trim() != "expect no_info" {
                return Err(err_at(
                    file,
                    line_no,
                    "expect no_info takes no msg:/regex: tail",
                ));
            }
            *expect_no_info = true;
            Ok(())
        }
        Some(deferred @ ("ordered" | "range")) => Err(err_at(
            file,
            line_no,
            format!(
                "deferred `expect {deferred}` directive reached the executed-grammar \
                 parser — scan_deferred_directives must route this file to the \
                 skip-manifest first"
            ),
        )),
        other => Err(err_at(
            file,
            line_no,
            format!(
                "invalid expect statement {other:?} — executable forms are `expect fail`, \
                 `expect string`, and `expect warn|no_warn|info|no_info`"
            ),
        )),
    }
}

/// The optional `msg:<s>`/`regex:<p>` tail shared by `expect
/// fail|warn|info` (upstream `patExpect`'s single capture group,
/// `test.go:55`): everything after the FIRST occurrence of `keyword` in
/// the line, trimmed. `keyword` is always the second whitespace-delimited
/// token, immediately after `expect `, so its first occurrence in the
/// line is unambiguous for every corpus message body (none begins with
/// the bare keyword text before the tag).
fn parse_optional_tag(line: &str, keyword: &str) -> Result<AnnotationMatch, String> {
    let tail = line
        .split_once(keyword)
        .map(|(_, t)| t.trim())
        .unwrap_or("");
    if tail.is_empty() {
        return Ok(AnnotationMatch::Any);
    }
    if let Some(msg) = tail.strip_prefix("msg:") {
        return Ok(AnnotationMatch::Message(msg.trim().to_string()));
    }
    if let Some(pat) = tail.strip_prefix("regex:") {
        return Ok(AnnotationMatch::Regex(pat.trim().to_string()));
    }
    Err(format!(
        "invalid token after expect {keyword}: {tail:?} (want msg:/regex:)"
    ))
}

/// Go `strconv.Unquote` for the forms upstream `.test` files use
/// (`parseAsStringLiteral`): a backquoted RAW string (no escapes; may not
/// contain a backquote; Go drops carriage returns) and a double-quoted
/// string with the Go escape set (`\a \b \f \n \r \t \v \\ \' \" \xHH
/// \OOO \uXXXX \UXXXXXXXX`). Go semantics per escape class
/// (`strconv.unquoteChar` at the pin): `\xHH` and octal `\OOO` produce
/// one raw BYTE each (a Go string is a byte slice — `"\xe4\xb8\x96"` is
/// the three UTF-8 bytes of `世`, never three separate code points), and
/// an octal value above 255 is a syntax error; `\uXXXX`/`\UXXXXXXXX`
/// produce one CODE POINT, UTF-8-encoded. The result is `Vec<u8>` (issue
/// #86): a Go string is an arbitrary byte slice, so byte escapes that do
/// not form valid UTF-8 (`"\xff"`) are legal Go output and must survive
/// here too — only the `Expected::String` comparison channel carries
/// these bytes, everything else in the driver stays `String` (see the
/// call site). Single-quoted rune literals are not used by any vendored
/// file and are rejected loudly (extend if a corpus file legitimately
/// needs them — the `series.rs::scan_quoted_string` convention).
fn go_unquote(s: &str) -> Result<Vec<u8>, String> {
    let bytes = s.as_bytes();
    if bytes.len() < 2 {
        return Err(format!("invalid quoted string {s:?}"));
    }
    let quote = bytes[0];
    if bytes[bytes.len() - 1] != quote {
        return Err(format!("unbalanced quotes in {s:?}"));
    }
    let inner = &s[1..s.len() - 1];
    match quote {
        b'`' => {
            if inner.contains('`') {
                return Err(format!("backquoted string contains a backquote: {s:?}"));
            }
            Ok(inner.replace('\r', "").into_bytes())
        }
        b'"' => {
            // Accumulate BYTES, not chars: `\xHH`/`\OOO` are raw bytes in
            // Go, so multibyte UTF-8 sequences spelled byte-by-byte must
            // concatenate before decoding.
            let mut out: Vec<u8> = Vec::with_capacity(inner.len());
            let mut chars = inner.chars();
            while let Some(c) = chars.next() {
                if c == '"' {
                    return Err(format!("unescaped quote inside {s:?}"));
                }
                if c != '\\' {
                    let mut buf = [0u8; 4];
                    out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                    continue;
                }
                let esc = chars
                    .next()
                    .ok_or_else(|| format!("dangling backslash in {s:?}"))?;
                let simple: Option<u8> = match esc {
                    'a' => Some(0x07),
                    'b' => Some(0x08),
                    'f' => Some(0x0C),
                    'n' => Some(b'\n'),
                    'r' => Some(b'\r'),
                    't' => Some(b'\t'),
                    'v' => Some(0x0B),
                    '\\' => Some(b'\\'),
                    '\'' => Some(b'\''),
                    '"' => Some(b'"'),
                    _ => None,
                };
                if let Some(b) = simple {
                    out.push(b);
                    continue;
                }
                match esc {
                    // `\xHH`: one raw byte (Go `if c == 'x' { value = v }`
                    // with multibyte=false — appended as a single byte).
                    'x' => {
                        let hex: String = (0..2).filter_map(|_| chars.next()).collect();
                        if hex.len() != 2 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
                            return Err(format!("invalid \\x escape in {s:?}"));
                        }
                        let b = u8::from_str_radix(&hex, 16)
                            .map_err(|e| format!("invalid \\x escape in {s:?}: {e}"))?;
                        out.push(b);
                    }
                    // `\uXXXX`/`\UXXXXXXXX`: one code point, UTF-8-encoded.
                    'u' | 'U' => {
                        let width = if esc == 'u' { 4 } else { 8 };
                        let hex: String = (0..width).filter_map(|_| chars.next()).collect();
                        if hex.len() != width || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
                            return Err(format!("invalid \\{esc} escape in {s:?}"));
                        }
                        let code = u32::from_str_radix(&hex, 16)
                            .map_err(|e| format!("invalid \\{esc} escape in {s:?}: {e}"))?;
                        let c = char::from_u32(code)
                            .ok_or_else(|| format!("invalid code point in {s:?}"))?;
                        let mut buf = [0u8; 4];
                        out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                    }
                    // Octal `\OOO`: exactly three digits, one raw byte;
                    // a value above 255 is a syntax error (Go
                    // `if v > 255 { return ErrSyntax }`), so `\400` is
                    // rejected — never truncated or widened.
                    d @ '0'..='7' => {
                        let mut val = d as u32 - '0' as u32;
                        for _ in 0..2 {
                            let n = chars
                                .next()
                                .filter(|c| ('0'..='7').contains(c))
                                .ok_or_else(|| format!("invalid octal escape in {s:?}"))?;
                            val = val * 8 + (n as u32 - '0' as u32);
                        }
                        if val > 255 {
                            return Err(format!(
                                "octal escape above \\377 in {s:?} (Go rejects octal > 255)"
                            ));
                        }
                        out.push(val as u8);
                    }
                    other => return Err(format!("unsupported escape \\{other} in {s:?}")),
                }
            }
            Ok(out)
        }
        _ => Err(format!(
            "unsupported quote style in {s:?} — extend grammar.rs::go_unquote if the corpus \
             legitimately needs it"
        )),
    }
}

/// Parses a lone-number result line (scalar expectation). Rejects
/// anything with labels/multiple items.
fn parse_scalar_line(line: &str) -> Option<f64> {
    let t = line.trim();
    if t.split_ascii_whitespace().count() != 1 {
        return None;
    }
    // Reuse the sequence-value scanner so NaN/Inf forms parse identically,
    // but require the whole token to be consumed and un-suffixed.
    match scan_signed_number(t) {
        Some((v, "")) => Some(v),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::go_unquote;

    /// Finding 1 (code review round 1): byte-escape semantics match Go
    /// `strconv.Unquote` — `\xHH` is a raw BYTE, so a multibyte UTF-8
    /// sequence spelled byte-by-byte decodes to ONE character (Go:
    /// `strconv.Unquote("\"\\xe4\\xb8\\x96\"")` == `"世"`), never three
    /// mojibake code points.
    #[test]
    fn go_unquote_hex_escapes_are_raw_bytes_multibyte_sequences_decode_as_utf8() {
        assert_eq!(go_unquote(r#""\xe4\xb8\x96""#).unwrap(), "世".as_bytes());
        assert_eq!(go_unquote(r#""\x41\x42""#).unwrap(), b"AB");
    }

    /// Issue #86: `\xHH`/octal byte escapes that do NOT form valid UTF-8
    /// are legal Go output (a Go string is an arbitrary byte slice) and
    /// must survive to the exact bytes — no UTF-8 gate on the result.
    /// `"a\xc5z"` is the exact vendored literal at `aggregators.test:481`.
    #[test]
    fn go_unquote_non_utf8_byte_escapes_survive_as_exact_bytes() {
        assert_eq!(go_unquote(r#""\xff""#).unwrap(), vec![0xffu8]);
        assert_eq!(go_unquote(r#""a\xc5z""#).unwrap(), vec![b'a', 0xc5, b'z']);
    }

    /// Octal escapes are raw bytes too (Go: `"\101\102"` == `"AB"`,
    /// `"\344\270\226"` == `"世"`), and out-of-range octal (> `\377`) is
    /// a syntax error exactly like Go's `v > 255` check — never accepted
    /// as a widened code point.
    #[test]
    fn go_unquote_octal_escapes_are_raw_bytes_and_reject_values_above_255() {
        assert_eq!(go_unquote(r#""\101\102""#).unwrap(), b"AB");
        assert_eq!(go_unquote(r#""\344\270\226""#).unwrap(), "世".as_bytes());
        assert!(go_unquote(r#""\400""#).unwrap_err().contains("377"));
        assert!(go_unquote(r#""\777""#).unwrap_err().contains("377"));
    }

    /// `\u`/`\U` stay CODE-POINT escapes (Go multibyte=true path), and
    /// the raw/simple forms round-trip.
    #[test]
    fn go_unquote_unicode_escapes_are_code_points_and_raw_strings_pass_through() {
        assert_eq!(go_unquote(r#""世""#).unwrap(), "世".as_bytes());
        assert_eq!(go_unquote(r#""\U0001F600""#).unwrap(), "😀".as_bytes());
        assert_eq!(go_unquote("`a\\n b`").unwrap(), b"a\\n b");
        assert_eq!(go_unquote(r#""a\tb\"c""#).unwrap(), b"a\tb\"c");
    }

    /// Per-escape syntax checks are untouched by the byte-result change:
    /// a short `\x` hex sequence and unsupported escape characters still
    /// reject loudly.
    #[test]
    fn go_unquote_invalid_escape_syntax_still_rejects() {
        assert!(go_unquote(r#""\xf""#).is_err());
        assert!(go_unquote(r#""\q""#).is_err());
    }

    /// Surrogate code points must stay rejected (Go `utf8.ValidRune`
    /// parity) — dropping the whole-result UTF-8 gate must not widen the
    /// per-escape `\u`/`\U` code-point path, which already rejects them
    /// via `char::from_u32`.
    #[test]
    fn go_unquote_surrogate_unicode_escape_rejects() {
        assert!(go_unquote(r#""\ud800""#).is_err());
    }
}
