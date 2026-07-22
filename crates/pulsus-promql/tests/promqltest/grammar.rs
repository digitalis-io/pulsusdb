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
//! - `metric{...}@st <sequence>` start-timestamp lines inside `load`
//!   blocks (issue #155 — upstream `isSTLine`/`parseSTLine`/
//!   `parseSTSequence`, `promql/promqltest/test.go:296-341,345-510` at
//!   the pinned SHA): each line binds duration offsets to the
//!   immediately-following sample line with the same metric and value
//!   count (`ST = T + offset` per non-omitted position)
//! - `load_with_nhcb <step>` (issue #154 — upstream `patLoad`,
//!   test.go:52): a `load` block that ALSO appends NHCB-converted
//!   twins of its classic `_bucket`/`_count`/`_sum` series (see
//!   [`super::nhcb`])
//! - block-form `expect ordered [msg:<s>|regex:<p>]` (issue #154 —
//!   upstream `patExpect`, test.go:55): the ordered flag in block form;
//!   the optional tail parses (an invalid `regex:` pattern is a parse
//!   error, upstream `parseExpect` → `regexp.Compile`) and is then
//!   DISCARDED — upstream stores it in `expectedCmds[Ordered]` which
//!   nothing ever reads (`isOrdered`, test.go:1068-1070)
//! - `expect range vector from <dur> to <dur> step <dur>` (issue #154 —
//!   upstream `parseExpectRangeVector`, test.go:571-595,723-737):
//!   permits a range-vector (matrix) result for an INSTANT eval; the
//!   expected sample grid is `from + k*step` and the eval time is
//!   OVERRIDDEN to `to` (`cmd.eval = *end`, test.go:733). In a RANGE
//!   block the directive REPLACES the block's own from/to/step (the
//!   same `cmd.start/end/step` writes; `cmd.eval` is unread there);
//!   repeats overwrite (last wins) and combinations are unrestricted,
//!   exactly like the oracle's un-gated prefix branch
//!
//! Everything else in the upstream grammar (`eval_warn`/`eval_info`) is a
//! **deferred directive**: [`scan_deferred_directives`] detects them
//! before grammar parsing, and the corpus test requires any file using
//! one to be listed — loudly, wholesale — in `corpus/skip-manifest.json`
//! with an activation issue (plan v2 Δ2's skip-manifest contract). A
//! directive recognised by *neither* the executed subset nor the deferred
//! scan is a hard parse error, never a silent skip.

use std::collections::{BTreeMap, BTreeSet};

use super::series::{SeqValue, parse_metric, parse_series_line, scan_signed_number};

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

/// An expanded `@st` offset sequence (issue #155): one item per sample
/// position, `None` = omitted (`_`).
pub type StOffsets = Vec<Option<i64>>;

/// One `load` block series.
#[derive(Debug, Clone)]
pub struct LoadSeries {
    pub labels: BTreeMap<String, String>,
    pub values: Vec<SeqValue>,
    /// `@st` offsets (issue #155): milliseconds relative to the sample's
    /// own timestamp (`ST = t + offset`; offsets are typically negative);
    /// a `None` item = omitted (`_`). Invariant guaranteed at parse:
    /// `st.as_ref().map_or(true, |s| s.len() == values.len())`.
    pub st: Option<StOffsets>,
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

/// An `expect range vector from <A> to <B> step <C>` directive (issue
/// #154 — upstream `parseExpectRangeVector`, test.go:571-595): the
/// expected matrix grid for an instant eval whose query returns a range
/// vector. The parser also OVERRIDES the block's eval time to `to_ms`
/// (upstream `cmd.eval = *end`, test.go:733).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeVectorExpectation {
    pub from_ms: i64,
    pub to_ms: i64,
    pub step_ms: i64,
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
    /// `true` when the block used the block-form `expect ordered`
    /// directive (issue #154) — upgrades a `Pass` mode to
    /// [`EvalMode::Ordered`] (upstream `isOrdered`: prefix flag OR
    /// non-empty `expectedCmds[Ordered]`, test.go:1068-1070); counted
    /// separately from the `eval_ordered` prefix.
    pub expect_ordered: bool,
    /// The `expect range vector …` directive (issue #154): on an instant
    /// block, `Some` flips the expectation to [`Expected::Matrix`] on
    /// this grid and the eval time to `to_ms`; on a range block it
    /// REPLACES the block's own from/to/step (upstream's un-gated
    /// `cmd.start/end/step` writes, test.go:730-733).
    pub expect_range_vector: Option<RangeVectorExpectation>,
}

#[derive(Debug, Clone)]
pub enum Command {
    Clear,
    Load {
        step_ms: i64,
        series: Vec<LoadSeries>,
        /// `true` for `load_with_nhcb` (issue #154): the store ALSO
        /// appends the NHCB-converted twins of the block's classic
        /// `_bucket`/`_count`/`_sum` series (upstream
        /// `appendCustomHistogram`, test.go:917-1029).
        with_nhcb: bool,
    },
    Eval(EvalCmd),
}

/// The deferred-directive inventory (plan v2 Δ2): each variant names one
/// upstream directive family the executed subset does not run, with a
/// committed activation home in `corpus/skip-manifest.json`. Issue #154
/// dropped `ExpectLine` (block `expect ordered` + `expect range vector`)
/// and `LoadWithNhcb` — both are executable now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DeferredDirective {
    /// `eval_warn …` (annotation assertion).
    EvalWarn,
    /// `eval_info …` (annotation assertion).
    EvalInfo,
}

impl DeferredDirective {
    /// The stable name used in `corpus/skip-manifest.json`.
    pub fn name(self) -> &'static str {
        match self {
            DeferredDirective::EvalWarn => "eval_warn",
            DeferredDirective::EvalInfo => "eval_info",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        [DeferredDirective::EvalWarn, DeferredDirective::EvalInfo]
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
        if first_word == "eval_warn" {
            out.insert(DeferredDirective::EvalWarn);
        }
        if first_word == "eval_info" {
            out.insert(DeferredDirective::EvalInfo);
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
            "load" | "load_with_nhcb" => {
                let (cmd, next) = parse_load(file, &lines, i, first_word)?;
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

fn parse_load(
    file: &str,
    lines: &[String],
    start: usize,
    directive: &str,
) -> Result<(Command, usize), String> {
    let header = &lines[start];
    // `directive` is `load` or `load_with_nhcb` (upstream `patLoad`,
    // test.go:52 — the only two forms the regex admits).
    let with_nhcb = directive == "load_with_nhcb";
    let step = header
        .strip_prefix(directive)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            err_at(
                file,
                start,
                format!("invalid load command ({directive} <step:duration>)"),
            )
        })?;
    let step_ms = parse_duration_ms(step).map_err(|e| err_at(file, start, e))?;

    let mut series = Vec::new();
    let mut i = start + 1;
    // Issue #155: the pending-`@st` state machine (upstream `parseLoad`,
    // test.go:296-341 at the pin) — an `@st` line binds to the
    // IMMEDIATELY-following sample line, which must carry the same metric
    // and the same expanded value count; every violation is a loud,
    // oracle-shaped error raised at the `@st` line.
    struct PendingSt {
        labels: BTreeMap<String, String>,
        vals: StOffsets,
        line: usize,
    }
    let mut pending_st: Option<PendingSt> = None;
    while i < lines.len() && !lines[i].is_empty() {
        if is_st_line(&lines[i]) {
            if let Some(p) = &pending_st {
                return Err(err_at(
                    file,
                    p.line,
                    "@st line has no following sample line",
                ));
            }
            let (st_labels, st_vals) = parse_st_line(&lines[i]).map_err(|e| err_at(file, i, e))?;
            pending_st = Some(PendingSt {
                labels: st_labels,
                vals: st_vals,
                line: i,
            });
            i += 1;
            continue;
        }
        let (labels, values) = parse_series_line(&lines[i]).map_err(|e| err_at(file, i, e))?;
        let st = match pending_st.take() {
            None => None,
            Some(p) => {
                if p.labels != labels {
                    return Err(err_at(
                        file,
                        p.line,
                        "@st metric does not match the following sample line metric",
                    ));
                }
                if p.vals.len() != values.len() {
                    return Err(err_at(
                        file,
                        p.line,
                        format!(
                            "@st line has {} values but sample line has {}",
                            p.vals.len(),
                            values.len()
                        ),
                    ));
                }
                Some(p.vals)
            }
        };
        series.push(LoadSeries { labels, values, st });
        i += 1;
    }
    if let Some(p) = pending_st {
        return Err(err_at(
            file,
            p.line,
            "@st line has no following sample line",
        ));
    }
    Ok((
        Command::Load {
            step_ms,
            series,
            with_nhcb,
        },
        i,
    ))
}

/// Upstream `isSTLine` (test.go:345-354): the line's FIRST
/// whitespace-delimited token ends in `@st` (no space before it) and a
/// value sequence follows.
fn is_st_line(line: &str) -> bool {
    let line = line.trim();
    match line.split_ascii_whitespace().next() {
        Some(first) => first.ends_with("@st") && line.len() > first.len(),
        None => false,
    }
}

/// Upstream `parseSTLine` (test.go:359-382): `metric{labels}@st
/// <st_sequence>` — the metric part (with the `@st` suffix stripped)
/// reuses the series metric parser; each non-omitted sequence item is the
/// ST offset in milliseconds relative to the corresponding sample's
/// timestamp.
fn parse_st_line(line: &str) -> Result<(BTreeMap<String, String>, StOffsets), String> {
    let line = line.trim();
    let Some(space_idx) = line.find([' ', '\t']) else {
        return Err("invalid @st line: missing value sequence".to_string());
    };
    let metric_part = line[..space_idx]
        .strip_suffix("@st")
        .expect("is_st_line guaranteed the @st suffix");
    let vals_part = line[space_idx + 1..].trim();

    let (labels, remainder) = parse_metric(metric_part)
        .map_err(|e| format!("invalid @st line metric {metric_part:?}: {e}"))?;
    if !remainder.trim().is_empty() {
        return Err(format!(
            "invalid @st line metric {metric_part:?}: trailing input {remainder:?}"
        ));
    }
    let st_vals = parse_st_sequence(vals_part).map_err(|e| format!("invalid @st sequence: {e}"))?;
    Ok((labels, st_vals))
}

/// Upstream `parseSTSequence` (test.go:384-407): a space-separated
/// sequence of ST offset items. Item grammar (`parseSTItem`,
/// test.go:409-470):
///
/// - `_` — one omitted position; `_xN` — N omitted positions (N=0 is an
///   error);
/// - `<dur>` — one position; `<dur>xN` — N+1 positions;
/// - `<dur>+<dur>xN` / `<dur>-<dur>xN` — N+1 positions stepping by the
///   signed delta.
fn parse_st_sequence(input: &str) -> Result<StOffsets, String> {
    let mut result = Vec::new();
    for item in input.split_ascii_whitespace() {
        let vals = parse_st_item(item).map_err(|e| format!("invalid ST item {item:?}: {e}"))?;
        result.extend(vals);
    }
    Ok(result)
}

fn parse_st_item(item: &str) -> Result<StOffsets, String> {
    if item == "_" {
        return Ok(vec![None]);
    }
    if let Some(count) = item.strip_prefix("_x") {
        let n: u64 = count
            .parse()
            .map_err(|_| "invalid repeat count".to_string())?;
        if n == 0 {
            return Err("invalid repeat count".to_string());
        }
        return Ok(vec![None; n as usize]);
    }

    let (base, rest) = scan_st_duration_prefix(item)?;
    // No step: `<dur>` or `<dur>xN` (N+1 positions).
    if rest.is_empty() {
        return Ok(vec![Some(base)]);
    }
    if let Some(count) = rest.strip_prefix('x') {
        let n: u64 = count
            .parse()
            .map_err(|_| "invalid repeat count".to_string())?;
        return Ok(vec![Some(base); n as usize + 1]);
    }
    // Step: `<dur>+<dur>xN` or `<dur>-<dur>xN`.
    let negative = match rest.as_bytes()[0] {
        b'+' => false,
        b'-' => true,
        c => {
            return Err(format!(
                "unexpected character {:?} after duration",
                c as char
            ));
        }
    };
    let (delta, rest2) =
        scan_st_duration_prefix(&rest[1..]).map_err(|e| format!("invalid step duration: {e}"))?;
    let delta = if negative { -delta } else { delta };
    let Some(count) = rest2.strip_prefix('x') else {
        return Err("expected 'x<count>' after step duration".to_string());
    };
    let n: u64 = count
        .parse()
        .map_err(|e| format!("invalid repeat count: {e}"))?;
    let mut vals = Vec::with_capacity(n as usize + 1);
    let mut offset = base;
    for _ in 0..=n {
        vals.push(Some(offset));
        offset += delta;
    }
    Ok(vals)
}

/// Upstream `parseDurationPrefix` (test.go:473-510): a single-segment
/// Prometheus duration with an optional leading sign at the start of `s`
/// — digits, then unit letters, the unit scan stopping at `x` (the
/// repeat-count separator, never part of a valid unit — so `-1mx2`
/// parses as `-1m` + `x2`). Returns milliseconds and the unparsed rest.
fn scan_st_duration_prefix(s: &str) -> Result<(i64, &str), String> {
    if s.is_empty() {
        return Err("empty duration".to_string());
    }
    let bytes = s.as_bytes();
    let mut i = 0;
    let negative = match bytes[0] {
        b'-' => {
            i += 1;
            true
        }
        b'+' => {
            i += 1;
            false
        }
        _ => false,
    };
    let digits_start = i;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i == digits_start {
        return Err(format!("expected digits in duration {s:?}"));
    }
    while i < bytes.len() && bytes[i].is_ascii_alphabetic() && bytes[i] != b'x' {
        i += 1;
    }
    // `parse_duration_ms` accepts the scanned single segment (and the
    // bare-`0` special case), exactly like upstream's
    // `model.ParseDuration` on the scanned prefix.
    let ms = parse_duration_ms(&s[digits_start..i])?;
    Ok((if negative { -ms } else { ms }, &s[i..]))
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
    let mut kind = kind;
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
    let mut expect_ordered = false;
    let mut expect_range_vector: Option<RangeVectorExpectation> = None;
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
        // the annotation forms; issue #154 adds `expect ordered` and
        // `expect range vector`). Like upstream, only a line whose FIRST
        // whitespace-delimited token is literally `expect` routes here —
        // a metric named `expect` is still writable as `expect{}`.
        //
        // Issue #154 (code-review round 2): the oracle's per-line loop is
        // UNCONDITIONAL on position (test.go:723-762 — the expect
        // branches `continue` regardless of earlier series lines), so an
        // `expect` directive after SERIES result lines parses; the #86
        // ordering gate that rejected it is gone. The one positional
        // error the oracle DOES produce is after a SCALAR line:
        // `parseNumber` BREAKs the block loop (test.go:764-767), so any
        // following line falls out to the outer command parser and fails
        // ("invalid command") — mirrored here as a loud block-level
        // error.
        if line.split_ascii_whitespace().next() == Some("expect") {
            if scalar.is_some() {
                return Err(err_at(
                    file,
                    i,
                    "no line may follow a scalar result line (upstream's parseNumber ends the \
                     eval block there)",
                ));
            }
            // `expect range vector …` routes BEFORE the generic `expect`
            // split, exactly like upstream's `rangeVectorPrefix`
            // HasPrefix check (test.go:723). Oracle-faithful (the code-
            // review round-1 fix, the `expect ordered` Δ1 treatment
            // extended here): upstream's branch is NOT instant-gated, a
            // REPEAT simply overwrites `cmd.start/end/step/eval` (last
            // one wins), and no combination with fail/string is
            // restricted (`validateExpectedCmds` never mentions it) —
            // at exec time `isFail()`/`expectedString` win exactly like
            // they do upstream, because the expectation SHAPE is decided
            // after the loop below. The only parse-time errors are the
            // oracle's own: a malformed definition and `to < from`
            // (`parseExpectRangeVector`/`parseDurations`).
            if line.starts_with("expect range vector") {
                expect_range_vector =
                    Some(parse_expect_range_vector(line).map_err(|e| err_at(file, i, e))?);
            } else {
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
                    &mut expect_ordered,
                )?;
            }
            i += 1;
            continue;
        }
        // Issue #154 (code-review round 2): result lines AFTER an
        // `expect fail`/`expect string` directive also parse — the
        // oracle's loop consumes them into `cmd.expected` where the
        // fail/string expectation shape simply never reads them (isFail/
        // expectedString win in `compareResult`); the #86 reverse gate
        // here was the same non-oracle ordering tightening. The
        // expectation-shape precedence after the loop (Fail > String >
        // Scalar > Vector/Matrix) reproduces that unread-ness exactly.

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
        // Issue #154: `expect range vector` permits a multi-value
        // (matrix) expectation for an instant eval — upstream's exact
        // gate and message (test.go:773-776).
        if matches!(kind, EvalKind::Instant { .. })
            && values.len() != 1
            && expect_range_vector.is_none()
        {
            return Err(err_at(
                file,
                i,
                "expecting multiple values in instant evaluation not allowed. consider using \
                 'expect range vector' directive to enable a range vector result for an \
                 instant query",
            ));
        }
        result_series.push(ExpectedSeries { labels, values });
        i += 1;
    }

    if expect_fail {
        mode = EvalMode::Fail;
    }
    // Issue #154 (plan v2 Δ1): the block `expect ordered` upgrades a
    // plain block to Ordered exactly like the prefix (upstream
    // `isOrdered` ORs the two flags); a Fail block stays Fail —
    // upstream's error branch runs before any ordered comparison.
    if expect_ordered && mode == EvalMode::Pass {
        mode = EvalMode::Ordered;
    }
    // Issue #154: `expect range vector` overrides the block's grid —
    // upstream sets `cmd.start/end/step` from the directive and the eval
    // time to `to` (`cmd.eval = *end`, test.go:730-733) regardless of
    // block form: an INSTANT block becomes an instant eval AT `to`
    // (compared on the directive grid); a RANGE block's from/to/step are
    // simply REPLACED by the directive's (`cmd.eval` is unread for a
    // range eval).
    if let Some(rv) = &expect_range_vector {
        kind = match kind {
            EvalKind::Instant { .. } => EvalKind::Instant { at_ms: rv.to_ms },
            EvalKind::Range { .. } => EvalKind::Range {
                from_ms: rv.from_ms,
                to_ms: rv.to_ms,
                step_ms: rv.step_ms,
            },
        };
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
            // Issue #154: under `expect range vector`, an instant
            // block's result lines are a MATRIX expectation on the
            // directive's grid (empty lines ⇒ an empty matrix).
            EvalKind::Instant { .. } if expect_range_vector.is_some() => {
                Expected::Matrix(result_series)
            }
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
            expect_ordered,
            expect_range_vector,
        },
        i,
    ))
}

/// Parses the tail of one `expect range vector from <dur> to <dur> step
/// <dur>` line (issue #154 — upstream `parseExpectRangeVector` +
/// `parseDurations`, test.go:571-620). The only errors are the oracle's
/// own: a malformed definition, an invalid duration, and `to < from`.
/// A zero step parses (upstream's unsigned `model.ParseDuration` admits
/// bare `0` and never re-checks it) — the expected-grid generation is a
/// bounded walk over the RESULT values either way, exactly like the
/// oracle's `for i, e := range exp.vals`.
fn parse_expect_range_vector(line: &str) -> Result<RangeVectorExpectation, String> {
    let tail = line
        .strip_prefix("expect range vector")
        .expect("caller matched the prefix");
    let toks: Vec<&str> = tail.split_ascii_whitespace().collect();
    if toks.len() != 6 || toks[0] != "from" || toks[2] != "to" || toks[4] != "step" {
        return Err(format!("invalid range vector definition {line:?}"));
    }
    let from_ms = parse_duration_ms(toks[1])
        .map_err(|e| format!("invalid start timestamp definition {:?}: {e}", toks[1]))?;
    let to_ms = parse_duration_ms(toks[3])
        .map_err(|e| format!("invalid end timestamp definition {:?}: {e}", toks[3]))?;
    if to_ms < from_ms {
        return Err(format!(
            "invalid test definition, end timestamp ({}) is before start timestamp ({})",
            toks[3], toks[1]
        ));
    }
    let step_ms = parse_duration_ms(toks[5])
        .map_err(|e| format!("invalid step definition {:?}: {e}", toks[5]))?;
    Ok(RangeVectorExpectation {
        from_ms,
        to_ms,
        step_ms,
    })
}

/// Parses one block-form `expect …` line (issue #86). Executable forms:
/// `expect fail [msg:<s>|regex:<p>]` (upstream `patExpect`, test.go:55 —
/// the optional tail must be `msg:`/`regex:`-tagged),
/// `expect string <quoted>` (upstream `parseAsStringLiteral`),
/// `expect warn|no_warn|info|no_info [msg:|regex:]` (issue #124), and
/// `expect ordered [msg:|regex:]` (issue #154 — the tail parses like
/// every other `patExpect` member and is then DISCARDED; only the flag
/// matters, upstream `isOrdered`). `expect range vector` is routed by
/// the caller before this parser runs. Any unrecognised form is a hard
/// error.
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
    expect_ordered: &mut bool,
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
        // Issue #154 (plan v2 Δ1): oracle-faithful `expect ordered` — the
        // optional `msg:`/`regex:` tail PARSES exactly like every other
        // `patExpect` member (an invalid `regex:` pattern is a parse
        // error, upstream `parseExpect` → `regexp.Compile`,
        // test.go:543-547) and is then DISCARDED: upstream stores an
        // `expectCmd` in `expectedCmds[Ordered]` that nothing ever reads
        // — only `len(...) > 0` matters (`isOrdered`, test.go:1068-1070).
        // Repeats and combinations parse freely (`validateExpectedCmds`,
        // test.go:558-568, never restricts `ordered`); range blocks parse
        // too — enforcement happens at COMPARE time (a matrix result
        // fails the case with the oracle's exact message, runner.rs).
        Some("ordered") => {
            match parse_optional_tag(line, "ordered").map_err(|e| err_at(file, line_no, e))? {
                AnnotationMatch::Any | AnnotationMatch::Message(_) => {}
                AnnotationMatch::Regex(pattern) => {
                    regex::Regex::new(&pattern).map_err(|_| {
                        err_at(
                            file,
                            line_no,
                            format!("invalid regex {pattern} for ordered"),
                        )
                    })?;
                }
            }
            *expect_ordered = true;
            Ok(())
        }
        other => Err(err_at(
            file,
            line_no,
            format!(
                "invalid expect statement {other:?} — executable forms are `expect fail`, \
                 `expect string`, `expect ordered`, `expect range vector`, and \
                 `expect warn|no_warn|info|no_info`"
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

    // -- issue #155 (AC3/AC4): `@st` loader grammar — item expansion and
    //    the oracle-shaped loud errors (test.go:313,330,333,363,379 at
    //    the pin) --

    use super::{Command, parse_file, parse_st_item, parse_st_line};

    fn parse_err(text: &str) -> String {
        parse_file("t.test", text).expect_err("must fail loudly")
    }

    #[test]
    fn st_line_binds_offsets_to_the_following_sample_line() {
        let cmds = parse_file(
            "t.test",
            "load 1m\n\tm{a=\"b\"}@st _ -1m -30sx1\n\tm{a=\"b\"} 1 2 3 4\n",
        )
        .unwrap();
        let Command::Load { series, .. } = &cmds[0] else {
            panic!("expected a load command");
        };
        assert_eq!(series.len(), 1);
        assert_eq!(
            series[0].st,
            Some(vec![None, Some(-60_000), Some(-30_000), Some(-30_000)]),
            "`_`=omitted, `-1m`=one offset, `-30sx1`=2 positions (N+1)"
        );
        // A plain series in the same block carries no channel.
        let cmds = parse_file("t.test", "load 1m\n\tm 1 2\n").unwrap();
        let Command::Load { series, .. } = &cmds[0] else {
            panic!("expected a load command");
        };
        assert_eq!(series[0].st, None);
    }

    /// test.go:409-470: `_xN` = N omitted (N=0 error), `<dur>xN` = N+1,
    /// `<dur>±<dur>xN` = N+1 stepping; the unit scan stops at `x`
    /// (`-1mx2` = `-1m` × 3, never a `mx` unit); `-0ms` is legal.
    #[test]
    fn st_item_expansion_matches_the_pin() {
        assert_eq!(parse_st_item("_").unwrap(), vec![None]);
        assert_eq!(parse_st_item("_x3").unwrap(), vec![None, None, None]);
        assert!(
            parse_st_item("_x0")
                .unwrap_err()
                .contains("invalid repeat count")
        );
        assert_eq!(parse_st_item("-1m").unwrap(), vec![Some(-60_000)]);
        assert_eq!(parse_st_item("-0ms").unwrap(), vec![Some(0)]);
        assert_eq!(
            parse_st_item("-1mx2").unwrap(),
            vec![Some(-60_000); 3],
            "unit scan stops at `x`: -1m repeated N+1 times"
        );
        assert_eq!(
            parse_st_item("-30s-1mx2").unwrap(),
            vec![Some(-30_000), Some(-90_000), Some(-150_000)]
        );
        assert_eq!(
            parse_st_item("-59999ms+1sx1").unwrap(),
            vec![Some(-59_999), Some(-58_999)]
        );
        assert!(
            parse_st_item("1h30m")
                .unwrap_err()
                .contains("unexpected character")
        );
        assert!(
            parse_st_item("bogus")
                .unwrap_err()
                .contains("expected digits")
        );
    }

    /// test.go:313: a second `@st` line before any sample line.
    #[test]
    fn st_line_followed_by_another_st_line_fails_loudly() {
        let err = parse_err("load 1m\n\tm@st -1m\n\tm@st -1m\n\tm 1\n");
        assert!(
            err.contains("@st line has no following sample line"),
            "got {err:?}"
        );
        // The error carries the @st line's own location (1-based line 2).
        assert!(err.starts_with("t.test:2:"), "got {err:?}");
    }

    /// test.go:339-341: an `@st` line at the end of the load block.
    #[test]
    fn st_line_at_the_end_of_a_load_block_fails_loudly() {
        let err = parse_err("load 1m\n\tm@st -1m\n");
        assert!(
            err.contains("@st line has no following sample line"),
            "got {err:?}"
        );
    }

    /// test.go:330: the following sample line has a different metric.
    #[test]
    fn st_line_with_a_mismatched_metric_fails_loudly() {
        let err = parse_err("load 1m\n\tm{a=\"b\"}@st -1m\n\tm{a=\"c\"} 1\n");
        assert!(
            err.contains("@st metric does not match the following sample line metric"),
            "got {err:?}"
        );
    }

    /// test.go:333: expanded value counts differ.
    #[test]
    fn st_line_with_a_mismatched_value_count_fails_loudly() {
        let err = parse_err("load 1m\n\tm@st -1mx1\n\tm 1 2 3\n");
        assert!(
            err.contains("@st line has 2 values but sample line has 3"),
            "got {err:?}"
        );
    }

    /// test.go:379: a malformed ST item fails the sequence parse.
    #[test]
    fn st_line_with_an_invalid_item_fails_loudly() {
        let err = parse_err("load 1m\n\tm@st nope\n\tm 1\n");
        assert!(err.contains("invalid @st sequence:"), "got {err:?}");
        assert!(err.contains("invalid ST item \"nope\""), "got {err:?}");
    }

    /// test.go:363 (defensive in upstream too — `isSTLine` requires the
    /// whitespace): `parse_st_line` on a sequence-less line reports the
    /// oracle's message.
    #[test]
    fn st_line_without_a_value_sequence_reports_the_oracle_error() {
        let err = parse_st_line("m@st").unwrap_err();
        assert_eq!(err, "invalid @st line: missing value sequence");
        // ...and via `parse_load`, a lone `metric@st` token is NOT an ST
        // line (upstream `isSTLine` returns false) — it fails as a
        // malformed series line instead, still loudly.
        assert!(parse_file("t.test", "load 1m\n\tm@st\n\tm 1\n").is_err());
    }

    /// A malformed metric part before `@st` is loud, with the upstream
    /// error shape (test.go:373-376).
    #[test]
    fn st_line_with_a_malformed_metric_part_fails_loudly() {
        let err = parse_err("load 1m\n\tm{a=\"b\"canary@st -1m\n\tm 1\n");
        assert!(err.contains("invalid @st line metric"), "got {err:?}");
    }

    // -- issue #154: `load_with_nhcb`, block `expect ordered`, and
    //    `expect range vector` grammar --

    use super::{EvalKind, EvalMode, Expected};

    #[test]
    fn load_with_nhcb_parses_as_a_load_with_the_flag_set() {
        let cmds = parse_file("t.test", "load_with_nhcb 5m\n\tm_bucket{le=\"1\"} 1 2\n").unwrap();
        let Command::Load {
            step_ms,
            series,
            with_nhcb,
        } = &cmds[0]
        else {
            panic!("expected a load command");
        };
        assert_eq!(*step_ms, 300_000);
        assert_eq!(series.len(), 1);
        assert!(*with_nhcb);
        // The plain form still parses with the flag unset.
        let cmds = parse_file("t.test", "load 5m\n\tm 1\n").unwrap();
        let Command::Load { with_nhcb, .. } = &cmds[0] else {
            panic!("expected a load command");
        };
        assert!(!*with_nhcb);
    }

    /// The block `expect ordered` upgrades a plain instant block to the
    /// ordered mode; a fail block stays fail (upstream checks `isFail`
    /// before any ordered comparison).
    #[test]
    fn expect_ordered_upgrades_pass_mode_and_never_downgrades_fail() {
        let cmds = parse_file("t.test", "eval instant at 0 m\n\texpect ordered\n\tm 1\n").unwrap();
        let Command::Eval(cmd) = &cmds[0] else {
            panic!("expected an eval");
        };
        assert!(cmd.expect_ordered);
        assert_eq!(cmd.mode, EvalMode::Ordered);

        let cmds = parse_file(
            "t.test",
            "eval instant at 0 m\n\texpect ordered\n\texpect fail\n",
        )
        .unwrap();
        let Command::Eval(cmd) = &cmds[0] else {
            panic!("expected an eval");
        };
        assert!(cmd.expect_ordered);
        assert_eq!(cmd.mode, EvalMode::Fail, "fail wins over the ordered flag");
    }

    /// `expect range vector from A to B step C` flips the expectation to
    /// a matrix on the directive's grid and OVERRIDES the eval time to
    /// `to` (upstream `cmd.eval = *end`, test.go:733) — multi-value
    /// instant result lines become legal under it.
    #[test]
    fn expect_range_vector_parses_overrides_the_eval_time_and_permits_multi_values() {
        let cmds = parse_file(
            "t.test",
            "eval instant at 0 m[1m]\n\texpect range vector from 10s to 1m step 10s\n\tm 1 2 3 4 5 6\n",
        )
        .unwrap();
        let Command::Eval(cmd) = &cmds[0] else {
            panic!("expected an eval");
        };
        let rv = cmd.expect_range_vector.expect("directive parsed");
        assert_eq!((rv.from_ms, rv.to_ms, rv.step_ms), (10_000, 60_000, 10_000));
        assert_eq!(
            cmd.kind,
            EvalKind::Instant { at_ms: 60_000 },
            "eval time overridden to `to`"
        );
        assert!(matches!(&cmd.expected, Expected::Matrix(series) if series.len() == 1));
        // Empty result lines ⇒ an empty matrix expectation.
        let cmds = parse_file(
            "t.test",
            "eval instant at 1m m[1m]\n\texpect range vector from 10s to 1m step 10s\n",
        )
        .unwrap();
        let Command::Eval(cmd) = &cmds[0] else {
            panic!("expected an eval");
        };
        assert!(matches!(&cmd.expected, Expected::Matrix(series) if series.is_empty()));
    }

    /// Without the directive, a multi-value instant expectation keeps
    /// upstream's exact error text (test.go:773-776).
    #[test]
    fn multi_value_instant_expectation_without_the_directive_keeps_the_oracle_error() {
        let err = parse_err("eval instant at 1m m[1m]\n\tm 1 2 3\n");
        assert!(
            err.contains(
                "expecting multiple values in instant evaluation not allowed. consider using \
                 'expect range vector' directive"
            ),
            "got {err:?}"
        );
    }

    /// The only `expect range vector` parse errors are the ORACLE's own
    /// (`parseExpectRangeVector`/`parseDurations`, test.go:571-620): a
    /// malformed definition and `to < from`. Everything else the oracle
    /// parses, parses here (code-review round 1 — the `expect ordered`
    /// Δ1 treatment extended to this directive).
    #[test]
    fn expect_range_vector_keeps_only_the_oracle_parse_errors() {
        let backwards =
            parse_err("eval instant at 1m m[1m]\n\texpect range vector from 1m to 10s step 10s\n");
        assert!(
            backwards.contains("end timestamp (10s) is before start timestamp (1m)"),
            "got {backwards:?}"
        );
        let malformed = parse_err("eval instant at 1m m[1m]\n\texpect range vector 10s 1m 10s\n");
        assert!(
            malformed.contains("invalid range vector definition"),
            "got {malformed:?}"
        );
        // A zero step PARSES (upstream never re-checks the unsigned
        // duration) — the expected-grid walk is bounded by the result
        // values either way.
        let cmds = parse_file(
            "t.test",
            "eval instant at 1m m[1m]\n\texpect range vector from 10s to 1m step 0\n",
        )
        .unwrap();
        let Command::Eval(cmd) = &cmds[0] else {
            panic!("expected an eval");
        };
        assert_eq!(cmd.expect_range_vector.unwrap().step_ms, 0);
    }

    /// Oracle-faithful non-rejections (test.go:723-737 is un-gated): a
    /// RANGE block's grid is REPLACED by the directive, a repeat
    /// overwrites (last wins), and fail/string combinations parse — the
    /// expectation SHAPE is decided after the loop, so `expect fail`
    /// wins at judge time exactly like upstream's `isFail()`.
    #[test]
    fn expect_range_vector_parses_everything_the_oracle_parses() {
        // Range block: from/to/step replaced (cmd.start/end/step writes).
        let cmds = parse_file(
            "t.test",
            "eval range from 0 to 5m step 5m m\n\texpect range vector from 1m to 2m step 1m\n\tm 1 2\n",
        )
        .unwrap();
        let Command::Eval(cmd) = &cmds[0] else {
            panic!("expected an eval");
        };
        assert_eq!(
            cmd.kind,
            EvalKind::Range {
                from_ms: 60_000,
                to_ms: 120_000,
                step_ms: 60_000
            },
            "the directive replaces the range block's own grid"
        );
        assert!(matches!(&cmd.expected, Expected::Matrix(series) if series.len() == 1));

        // Repeat: last one wins (upstream overwrites cmd.start/end/step).
        let cmds = parse_file(
            "t.test",
            "eval instant at 2m m[1m]\n\texpect range vector from 0 to 1m step 10s\n\
             \texpect range vector from 1m to 2m step 30s\n",
        )
        .unwrap();
        let Command::Eval(cmd) = &cmds[0] else {
            panic!("expected an eval");
        };
        let rv = cmd.expect_range_vector.unwrap();
        assert_eq!(
            (rv.from_ms, rv.to_ms, rv.step_ms),
            (60_000, 120_000, 30_000)
        );
        assert_eq!(cmd.kind, EvalKind::Instant { at_ms: 120_000 });

        // Combination with `expect fail`: parses; the fail mode wins the
        // expectation shape (upstream's error branch runs first).
        let cmds = parse_file(
            "t.test",
            "eval instant at 1m m[1m]\n\texpect fail\n\
             \texpect range vector from 0 to 1m step 10s\n",
        )
        .unwrap();
        let Command::Eval(cmd) = &cmds[0] else {
            panic!("expected an eval");
        };
        assert_eq!(cmd.mode, EvalMode::Fail);
        assert!(matches!(&cmd.expected, Expected::Fail { .. }));
        assert!(cmd.expect_range_vector.is_some());
    }

    /// Issue #154 (code-review round 2): the oracle's per-line loop is
    /// position-unconditional (test.go:723-762) — `expect` directives
    /// parse AFTER series result lines, and result lines parse after
    /// `expect fail`/`expect string` (they land in the oracle's
    /// `cmd.expected`, which the fail/string shapes never read). The one
    /// positional error the oracle keeps is after a SCALAR line
    /// (`parseNumber` BREAKs the block, test.go:764-767).
    #[test]
    fn expect_directives_and_result_lines_are_position_unconditional_like_the_oracle() {
        // `expect ordered` after a series line.
        let cmds = parse_file(
            "t.test",
            "eval instant at 0 sort(m)\n\tm{env=\"a\"} 1\n\texpect ordered\n",
        )
        .unwrap();
        let Command::Eval(cmd) = &cmds[0] else {
            panic!("expected an eval");
        };
        assert!(cmd.expect_ordered);
        assert_eq!(cmd.mode, EvalMode::Ordered);
        assert!(matches!(&cmd.expected, Expected::Vector(series) if series.len() == 1));

        // `expect range vector` after a (single-value) series line: the
        // directive still flips the expectation to the matrix grid.
        let cmds = parse_file(
            "t.test",
            "eval instant at 2m m[2m]\n\tm 3\n\texpect range vector from 2m to 2m step 1m\n",
        )
        .unwrap();
        let Command::Eval(cmd) = &cmds[0] else {
            panic!("expected an eval");
        };
        assert!(matches!(&cmd.expected, Expected::Matrix(series) if series.len() == 1));

        // ...but a MULTI-value series line BEFORE the directive keeps the
        // oracle's error — test.go:774 consults `expectRangeVector` at
        // series-line time, so the directive must precede such lines.
        let err = parse_err(
            "eval instant at 1m m[1m]\n\tm 1 2\n\
             \texpect range vector from 10s to 1m step 10s\n",
        );
        assert!(
            err.contains("expecting multiple values in instant evaluation not allowed"),
            "got {err:?}"
        );

        // `expect fail` after a series line (fail wins the shape), and a
        // series line after `expect fail` (parsed, never read) — both
        // orders parse.
        for text in [
            "eval instant at 0 m\n\tm 1\n\texpect fail\n",
            "eval instant at 0 m\n\texpect fail\n\tm 1\n",
        ] {
            let cmds = parse_file("t.test", text).unwrap();
            let Command::Eval(cmd) = &cmds[0] else {
                panic!("expected an eval");
            };
            assert_eq!(cmd.mode, EvalMode::Fail);
            assert!(matches!(&cmd.expected, Expected::Fail { .. }));
        }

        // After a SCALAR line the oracle's block is OVER (parseNumber
        // breaks; a following line is an outer-parser error) — loud here.
        let err = parse_err("eval instant at 0 vector(1)\n\t1\n\texpect ordered\n");
        assert!(
            err.contains("no line may follow a scalar result line"),
            "got {err:?}"
        );
    }
}
