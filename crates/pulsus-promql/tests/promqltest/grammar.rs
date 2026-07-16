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
//!
//! Everything else in the upstream grammar (`eval_warn`/`eval_info`,
//! `expect …` annotation/error lines, `load_with_nhcb`, `{{…}}`
//! native-histogram sample syntax, `@st` start-timestamp lines) is a
//! **deferred directive**: [`scan_deferred_directives`] detects them
//! before grammar parsing, and the corpus test requires any file using one
//! to be listed — loudly, wholesale — in `corpus/skip-manifest.json` with
//! an activation issue (plan v2 Δ2's skip-manifest contract). A directive
//! recognised by *neither* the executed subset nor the deferred scan is a
//! hard parse error, never a silent skip.

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
    /// `eval_fail`'s expectation.
    Fail {
        message: Option<String>,
        regexp: Option<String>,
    },
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
    /// `expect fail|warn|no_warn|info|no_info|ordered …` assertion lines.
    ExpectLine,
    /// `eval_warn …` (annotation assertion).
    EvalWarn,
    /// `eval_info …` (annotation assertion).
    EvalInfo,
    /// `load_with_nhcb …` (native-histogram-compatible bucket conversion).
    LoadWithNhcb,
    /// `{{…}}` native-histogram sample literals.
    NativeHistogramValue,
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
            DeferredDirective::NativeHistogramValue => "native-histogram-value",
            DeferredDirective::StartTimestampLine => "@st",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        [
            DeferredDirective::ExpectLine,
            DeferredDirective::EvalWarn,
            DeferredDirective::EvalInfo,
            DeferredDirective::LoadWithNhcb,
            DeferredDirective::NativeHistogramValue,
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
            out.insert(DeferredDirective::ExpectLine);
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
        if line.contains("{{") {
            out.insert(DeferredDirective::NativeHistogramValue);
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
    let mut fail_message: Option<String> = None;
    let mut fail_regexp: Option<String> = None;
    let mut result_series: Vec<ExpectedSeries> = Vec::new();
    let mut scalar: Option<f64> = None;

    while i < lines.len() && !lines[i].is_empty() {
        let line = &lines[i];
        if mode == EvalMode::Fail {
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

    let expected = if mode == EvalMode::Fail {
        Expected::Fail {
            message: fail_message,
            regexp: fail_regexp,
        }
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
        },
        i,
    ))
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
