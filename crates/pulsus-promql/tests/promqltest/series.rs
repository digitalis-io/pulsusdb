//! Series-description parsing for the upstream `.test` grammar: a metric
//! (`name{label="value", ...}`, either part optional) followed by a
//! whitespace-separated sequence of value items. Mirrors the upstream
//! promqltest series grammar at the pinned v3.13.0 SHA
//! (`promql/parser/generated_parser.y`, `series_item` productions — see
//! `corpus/upstream/PROVENANCE.md`):
//!
//! - `_` — one omitted sample; `_xN` — **N** omitted samples;
//! - `stale` — one staleness-marker sample (`STALE_NAN_BITS`);
//! - `v` — one value; `vxN` — **N+1** copies of `v`;
//! - `v+dxN` / `v-dxN` — **N+1** values stepping by the signed delta `d`;
//! - values accept `NaN`, `Inf`, `+Inf`, `-Inf` (case-insensitive, like
//!   Go's `strconv.ParseFloat`), decimals, and exponents. **No hex**:
//!   upstream's lexer disables `0x` in series descriptions ("Disallow
//!   hexadecimal in series descriptions as the syntax is ambiguous",
//!   `promql/parser/lex.go::scanNumber`), so `0x8` means eight+1 zeros,
//!   never 8.
//!
//! Every parse error is a hard, descriptive error — the driver never
//! guesses at a malformed line (issue #64 plan: unrecognised input is
//! loud, not skipped).

use std::collections::BTreeMap;

use pulsus_model::FloatHistogram;

use super::histogram_literal::parse_histogram_series_item;

/// One position in a series' value sequence.
#[derive(Debug, Clone)]
pub enum SeqValue {
    Value(f64),
    /// `_` — no sample at this position.
    Gap,
    /// `stale` — an explicit staleness marker sample.
    Stale,
    /// A `{{...}}` native-histogram literal (issue #124, M7-A6).
    Histogram(FloatHistogram),
}

/// Hand-written (mirrors [`pulsus_model::FloatHistogram::bits_eq`] for the
/// `Histogram` arm, since `FloatHistogram` has no `PartialEq` derive —
/// NaN-bearing fields). The only existing use site
/// (`grammar.rs`'s `values.contains(&SeqValue::Stale)`) never compares two
/// `Histogram` values against each other.
impl PartialEq for SeqValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SeqValue::Value(a), SeqValue::Value(b)) => a == b,
            (SeqValue::Gap, SeqValue::Gap) => true,
            (SeqValue::Stale, SeqValue::Stale) => true,
            (SeqValue::Histogram(a), SeqValue::Histogram(b)) => a.bits_eq(b),
            _ => false,
        }
    }
}

/// Parses a full series-description line: metric part + value items.
/// Returns the label set (including `__name__` when a metric name is
/// present) and the expanded value sequence.
pub fn parse_series_line(line: &str) -> Result<(BTreeMap<String, String>, Vec<SeqValue>), String> {
    let (labels, rest) = parse_metric(line)?;
    let values = parse_sequence(rest)?;
    Ok((labels, values))
}

/// Parses the metric part (`name`, `name{...}`, or `{...}`) off the front
/// of `input`, returning the label set and the remainder of the line.
pub fn parse_metric(input: &str) -> Result<(BTreeMap<String, String>, &str), String> {
    let mut labels = BTreeMap::new();
    let s = input.trim_start();

    let (name, s) = scan_metric_name(s);
    if let Some(name) = name {
        labels.insert("__name__".to_string(), name);
    }

    let mut rest = s;
    let mut saw_braces = false;
    if let Some(after_brace) = s.strip_prefix('{') {
        saw_braces = true;
        let (pairs, remainder) = scan_label_pairs(after_brace)?;
        for (k, v) in pairs {
            if labels.contains_key(&k) {
                return Err(format!("label {k:?} set twice in metric {input:?}"));
            }
            labels.insert(k, v);
        }
        rest = remainder;
    }

    // `{}` (empty braces) is a valid metric part in *result* lines — an
    // aggregated/computed series with no labels left. Only a part with
    // neither a name nor a brace section is malformed.
    if labels.is_empty() && !saw_braces {
        return Err(format!(
            "metric part of {input:?} has neither a name nor labels"
        ));
    }
    Ok((labels, rest))
}

fn is_metric_name_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_' || c == ':'
}

fn is_metric_name_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == ':'
}

fn scan_metric_name(s: &str) -> (Option<String>, &str) {
    let mut chars = s.char_indices();
    match chars.next() {
        Some((_, c)) if is_metric_name_start(c) => {}
        _ => return (None, s),
    }
    let end = s
        .char_indices()
        .find(|&(_, c)| !is_metric_name_char(c))
        .map(|(i, _)| i)
        .unwrap_or(s.len());
    (Some(s[..end].to_string()), &s[end..])
}

fn is_label_name_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_label_name_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Sorted `(key, value)` label pairs.
type LabelPairs = Vec<(String, String)>;

/// Scans `key="value"` pairs after an opening `{`, through the closing
/// `}`. Returns the pairs and the remainder after `}`.
///
/// Issue #85 (M6-08c): the UTF-8 quoted-name forms of the upstream series
/// grammar are supported too — a **bare quoted string** entry is the
/// metric name (`{"utf8.metric", ...}` ⇒ a `__name__` pair), and a quoted
/// string followed by `=` is a quoted **label name** (`{"label.dot"="x"}`).
fn scan_label_pairs(mut s: &str) -> Result<(LabelPairs, &str), String> {
    let mut pairs = Vec::new();
    loop {
        s = s.trim_start();
        if let Some(rest) = s.strip_prefix('}') {
            return Ok((pairs, rest));
        }
        if s.is_empty() {
            return Err("unterminated label set (no closing '}')".to_string());
        }

        let key = if s.starts_with(['"', '\'']) {
            let (quoted, rest) = scan_quoted_string(s)?;
            s = rest.trim_start();
            if !s.starts_with('=') {
                // A bare quoted entry is the metric name itself.
                pairs.push(("__name__".to_string(), quoted));
                if let Some(after_comma) = s.strip_prefix(',') {
                    s = after_comma;
                } else if !s.starts_with('}') {
                    return Err(format!(
                        "expected ',' or '}}' after quoted metric name, at {s:?}"
                    ));
                }
                continue;
            }
            quoted
        } else {
            let name_end = s
                .char_indices()
                .find(|&(_, c)| !is_label_name_char(c))
                .map(|(i, _)| i)
                .unwrap_or(s.len());
            if name_end == 0 || !s.starts_with(is_label_name_start) {
                return Err(format!("invalid label name at {s:?}"));
            }
            let key = s[..name_end].to_string();
            s = &s[name_end..];
            key
        };
        s = s.trim_start();

        let Some(after_eq) = s.strip_prefix('=') else {
            return Err(format!("expected '=' after label name {key:?}, at {s:?}"));
        };
        s = after_eq.trim_start();

        let (value, rest) = scan_quoted_string(s)?;
        pairs.push((key, value));
        s = rest.trim_start();

        if let Some(rest) = s.strip_prefix(',') {
            s = rest;
            continue;
        }
        // Next iteration must find `}` (or it's an error).
    }
}

/// Scans a `"..."` or `'...'` quoted string with the common PromQL escape
/// sequences. Unknown escapes are a hard error (loud, never guessed).
fn scan_quoted_string(s: &str) -> Result<(String, &str), String> {
    let mut chars = s.char_indices();
    let quote = match chars.next() {
        Some((_, c @ ('"' | '\''))) => c,
        _ => return Err(format!("expected a quoted label value at {s:?}")),
    };
    let mut out = String::new();
    while let Some((i, c)) = chars.next() {
        if c == quote {
            return Ok((out, &s[i + c.len_utf8()..]));
        }
        if c != '\\' {
            out.push(c);
            continue;
        }
        let Some((_, esc)) = chars.next() else {
            return Err(format!("dangling backslash in string {s:?}"));
        };
        match esc {
            '\\' => out.push('\\'),
            '"' => out.push('"'),
            '\'' => out.push('\''),
            'n' => out.push('\n'),
            't' => out.push('\t'),
            'r' => out.push('\r'),
            other => {
                return Err(format!(
                    "unsupported escape sequence '\\{other}' in string {s:?} — extend \
                     series.rs::scan_quoted_string if the corpus legitimately needs it"
                ));
            }
        }
    }
    Err(format!("unterminated string in {s:?}"))
}

/// Parses the whitespace-separated value items of a series description.
/// Not a plain `split_whitespace` (issue #124, M7-A6): a `{{...}}`
/// histogram literal — and its optional `+{{...}}xN`/`-{{...}}xN`/`xN`
/// combinator tail — legitimately contains internal whitespace
/// (`buckets:[1 2 1]`), so histogram items are scanned by their own
/// matching-`}}` grammar instead of being split on spaces first.
pub fn parse_sequence(s: &str) -> Result<Vec<SeqValue>, String> {
    let mut out = Vec::new();
    let mut rest = s.trim_start();
    while !rest.is_empty() {
        if rest.starts_with("{{") {
            let (mut vals, r) = parse_histogram_series_item(rest)?;
            out.append(&mut vals);
            rest = r.trim_start();
            continue;
        }
        let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        let (item, r) = rest.split_at(end);
        out.extend(parse_sequence_item(item)?);
        rest = r.trim_start();
    }
    Ok(out)
}

fn parse_uint(s: &str, item: &str) -> Result<u64, String> {
    s.parse::<u64>()
        .map_err(|e| format!("invalid repeat count {s:?} in item {item:?}: {e}"))
}

fn parse_sequence_item(item: &str) -> Result<Vec<SeqValue>, String> {
    if item == "_" {
        return Ok(vec![SeqValue::Gap]);
    }
    if let Some(n) = item.strip_prefix("_x") {
        // `_xN` = N omitted positions (upstream: `BLANK TIMES uint` appends
        // exactly N, unlike the value form's N+1).
        let n = parse_uint(n, item)?;
        return Ok(vec![SeqValue::Gap; n as usize]);
    }
    if item == "stale" {
        return Ok(vec![SeqValue::Stale]);
    }

    let (v, rest) = scan_signed_number(item)
        .ok_or_else(|| format!("invalid series value item {item:?} (expected a number)"))?;
    if rest.is_empty() {
        return Ok(vec![SeqValue::Value(v)]);
    }
    if let Some(count) = rest.strip_prefix('x') {
        // `vxN` = N+1 copies ("an additional value for time 0" — upstream
        // grammar action).
        let n = parse_uint(count, item)?;
        return Ok(vec![SeqValue::Value(v); n as usize + 1]);
    }
    if rest.starts_with('+') || rest.starts_with('-') {
        let (delta, rest2) = scan_signed_number(rest)
            .ok_or_else(|| format!("invalid step delta in series value item {item:?}"))?;
        let Some(count) = rest2.strip_prefix('x') else {
            return Err(format!(
                "expected 'x<count>' after step delta in series value item {item:?}"
            ));
        };
        let n = parse_uint(count, item)?;
        let mut vals = Vec::with_capacity(n as usize + 1);
        let mut cur = v;
        for _ in 0..=n {
            vals.push(SeqValue::Value(cur));
            cur += delta;
        }
        return Ok(vals);
    }
    Err(format!(
        "unexpected trailing {rest:?} in series value item {item:?}"
    ))
}

/// Scans a signed float (decimal/exponent form, or `Inf`/`NaN`,
/// case-insensitive) off the front of `s`. Deliberately stops before a
/// bare `x` (the repeat separator) or a following signed delta; never
/// consumes hex (see the module doc). Returns `None` if no number starts
/// at the front.
pub fn scan_signed_number(s: &str) -> Option<(f64, &str)> {
    let (sign, body) = match s.as_bytes().first() {
        Some(b'+') => (1.0, &s[1..]),
        Some(b'-') => (-1.0, &s[1..]),
        _ => (1.0, s),
    };

    // Inf / NaN keywords.
    for (kw, val) in [("inf", f64::INFINITY), ("nan", f64::NAN)] {
        if body.len() >= 3 && body[..3].eq_ignore_ascii_case(kw) {
            return Some((sign * val, &body[3..]));
        }
    }

    let bytes = body.as_bytes();
    let mut i = 0;
    let mut saw_digit = false;
    while i < bytes.len() {
        match bytes[i] {
            b'0'..=b'9' => {
                saw_digit = true;
                i += 1;
            }
            b'.' => i += 1,
            b'e' | b'E' => {
                // Only consume an exponent if it is followed by digits
                // (optionally signed) — otherwise stop (e.g. `1e` alone is
                // a parse error caught below).
                let mut j = i + 1;
                if j < bytes.len() && (bytes[j] == b'+' || bytes[j] == b'-') {
                    j += 1;
                }
                if j < bytes.len() && bytes[j].is_ascii_digit() {
                    i = j;
                } else {
                    break;
                }
            }
            _ => break,
        }
    }
    if !saw_digit {
        return None;
    }
    let num: f64 = body[..i].parse().ok()?;
    Some((sign * num, &body[i..]))
}
