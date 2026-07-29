//! Go reference-layout engine (issue #230, plan v1 Wave 5): a faithful
//! port of `time/format.go`'s `nextStdChunk` + `appendFormat` +
//! `parse`, driving `date`/`toDate`/`toDateInZone` and the `Format`/
//! `AppendFormat`/`Marshal*` `time.Time` methods. Parse's ERROR TEXT is
//! unobservable from LogQL (`toDate`/`toDateInZone` discard it,
//! `fmt.go:178`, `sprig/date.go:142`), so parsing returns `Result<_,
//! ()>` — only the accept/reject boundary and the zero-time fallback
//! must match.

use super::timefns::{GoTime, TemplateEnv, go_date, zone_at};
use super::value::GoLoc;

/// Layout-parse failure marker — the reference discards the error text
/// (`toDate`/`toDateInZone`), so nothing is carried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseFail;

impl From<()> for ParseFail {
    fn from((): ()) -> Self {
        ParseFail
    }
}

// std* codes (format.go).
const STD_LONG_MONTH: u32 = 1;
const STD_MONTH: u32 = 2;
const STD_NUM_MONTH: u32 = 3;
const STD_ZERO_MONTH: u32 = 4;
const STD_LONG_WEEKDAY: u32 = 5;
const STD_WEEKDAY: u32 = 6;
const STD_DAY: u32 = 7;
const STD_UNDER_DAY: u32 = 8;
const STD_ZERO_DAY: u32 = 9;
const STD_UNDER_YEAR_DAY: u32 = 10;
const STD_ZERO_YEAR_DAY: u32 = 11;
const STD_HOUR: u32 = 12;
const STD_HOUR12: u32 = 13;
const STD_ZERO_HOUR12: u32 = 14;
const STD_MINUTE: u32 = 15;
const STD_ZERO_MINUTE: u32 = 16;
const STD_SECOND: u32 = 17;
const STD_ZERO_SECOND: u32 = 18;
const STD_LONG_YEAR: u32 = 19;
const STD_YEAR: u32 = 20;
const STD_PM: u32 = 21;
const STD_PM_LOWER: u32 = 22;
const STD_TZ: u32 = 23;
const STD_ISO8601_TZ: u32 = 24;
const STD_ISO8601_SECONDS_TZ: u32 = 25;
const STD_ISO8601_SHORT_TZ: u32 = 26;
const STD_ISO8601_COLON_TZ: u32 = 27;
const STD_ISO8601_COLON_SECONDS_TZ: u32 = 28;
const STD_NUM_TZ: u32 = 29;
const STD_NUM_SECONDS_TZ: u32 = 30;
const STD_NUM_SHORT_TZ: u32 = 31;
const STD_NUM_COLON_TZ: u32 = 32;
const STD_NUM_COLON_SECONDS_TZ: u32 = 33;
const STD_FRAC_SECOND_0: u32 = 34;
const STD_FRAC_SECOND_9: u32 = 35;

/// One layout chunk: literal prefix + std token (with fractional-second
/// arg bits folded into the struct instead of Go's packed int).
#[derive(Debug, Clone, Copy)]
struct Std {
    code: u32,
    /// Fractional-second digits.
    frac_digits: usize,
    /// Fractional-second separator (`.` or `,`).
    comma: bool,
}

const STD_NONE: Std = Std {
    code: 0,
    frac_digits: 0,
    comma: false,
};

fn starts_with_lower_case(s: &[u8]) -> bool {
    !s.is_empty() && s[0].is_ascii_lowercase()
}

/// `nextStdChunk`.
fn next_std_chunk(layout: &[u8]) -> (&[u8], Std, &[u8]) {
    let std0x = [
        STD_ZERO_MONTH,
        STD_ZERO_DAY,
        STD_ZERO_HOUR12,
        STD_ZERO_MINUTE,
        STD_ZERO_SECOND,
        STD_YEAR,
    ];
    let n = layout.len();
    for i in 0..n {
        let rest = &layout[i..];
        let std = |code: u32, len: usize| -> Option<(&[u8], Std, &[u8])> {
            Some((
                &layout[..i],
                Std {
                    code,
                    frac_digits: 0,
                    comma: false,
                },
                &layout[i + len..],
            ))
        };
        let hit = match layout[i] {
            b'J' => {
                if rest.starts_with(b"January") {
                    std(STD_LONG_MONTH, 7)
                } else if rest.starts_with(b"Jan") && !starts_with_lower_case(&rest[3..]) {
                    std(STD_MONTH, 3)
                } else {
                    None
                }
            }
            b'M' => {
                if rest.starts_with(b"Monday") {
                    std(STD_LONG_WEEKDAY, 6)
                } else if rest.starts_with(b"Mon") && !starts_with_lower_case(&rest[3..]) {
                    std(STD_WEEKDAY, 3)
                } else if rest.starts_with(b"MST") {
                    std(STD_TZ, 3)
                } else {
                    None
                }
            }
            b'0' => {
                if rest.len() >= 2 && (b'1'..=b'6').contains(&rest[1]) {
                    std(std0x[(rest[1] - b'1') as usize], 2)
                } else if rest.starts_with(b"002") {
                    std(STD_ZERO_YEAR_DAY, 3)
                } else {
                    None
                }
            }
            b'1' => {
                if rest.len() >= 2 && rest[1] == b'5' {
                    std(STD_HOUR, 2)
                } else {
                    std(STD_NUM_MONTH, 1)
                }
            }
            b'2' => {
                if rest.starts_with(b"2006") {
                    std(STD_LONG_YEAR, 4)
                } else {
                    std(STD_DAY, 1)
                }
            }
            b'_' => {
                if rest.len() >= 2 && rest[1] == b'2' {
                    if rest.len() >= 5 && &rest[1..5] == b"2006" {
                        // literal _, followed by stdLongYear
                        return (
                            &layout[..i + 1],
                            Std {
                                code: STD_LONG_YEAR,
                                frac_digits: 0,
                                comma: false,
                            },
                            &layout[i + 5..],
                        );
                    }
                    std(STD_UNDER_DAY, 2)
                } else if rest.starts_with(b"__2") {
                    std(STD_UNDER_YEAR_DAY, 3)
                } else {
                    None
                }
            }
            b'3' => std(STD_HOUR12, 1),
            b'4' => std(STD_MINUTE, 1),
            b'5' => std(STD_SECOND, 1),
            b'P' => {
                if rest.starts_with(b"PM") {
                    std(STD_PM, 2)
                } else {
                    None
                }
            }
            b'p' => {
                if rest.starts_with(b"pm") {
                    std(STD_PM_LOWER, 2)
                } else {
                    None
                }
            }
            b'-' => {
                if rest.starts_with(b"-070000") {
                    std(STD_NUM_SECONDS_TZ, 7)
                } else if rest.starts_with(b"-07:00:00") {
                    std(STD_NUM_COLON_SECONDS_TZ, 9)
                } else if rest.starts_with(b"-0700") {
                    std(STD_NUM_TZ, 5)
                } else if rest.starts_with(b"-07:00") {
                    std(STD_NUM_COLON_TZ, 6)
                } else if rest.starts_with(b"-07") {
                    std(STD_NUM_SHORT_TZ, 3)
                } else {
                    None
                }
            }
            b'Z' => {
                if rest.starts_with(b"Z070000") {
                    std(STD_ISO8601_SECONDS_TZ, 7)
                } else if rest.starts_with(b"Z07:00:00") {
                    std(STD_ISO8601_COLON_SECONDS_TZ, 9)
                } else if rest.starts_with(b"Z0700") {
                    std(STD_ISO8601_TZ, 5)
                } else if rest.starts_with(b"Z07:00") {
                    std(STD_ISO8601_COLON_TZ, 6)
                } else if rest.starts_with(b"Z07") {
                    std(STD_ISO8601_SHORT_TZ, 3)
                } else {
                    None
                }
            }
            b'.' | b',' => {
                if rest.len() >= 2 && (rest[1] == b'0' || rest[1] == b'9') {
                    let ch = rest[1];
                    let mut j = 1;
                    while j < rest.len() && rest[j] == ch {
                        j += 1;
                    }
                    // Must not be followed by another digit.
                    if j >= rest.len() || !rest[j].is_ascii_digit() {
                        let code = if ch == b'9' {
                            STD_FRAC_SECOND_9
                        } else {
                            STD_FRAC_SECOND_0
                        };
                        return (
                            &layout[..i],
                            Std {
                                code,
                                frac_digits: j - 1,
                                comma: layout[i] == b',',
                            },
                            &layout[i + j..],
                        );
                    }
                    None
                } else {
                    None
                }
            }
            _ => None,
        };
        if let Some(hit) = hit {
            return hit;
        }
    }
    (layout, STD_NONE, b"")
}

fn append_int(b: &mut Vec<u8>, x: i64, width: usize) {
    let mut u = x.unsigned_abs();
    if x < 0 {
        b.push(b'-');
    }
    let digits = u.to_string();
    for _ in digits.len()..width {
        b.push(b'0');
    }
    b.extend_from_slice(digits.as_bytes());
    let _ = &mut u;
}

fn append_nano(b: &mut Vec<u8>, nanosec: i64, std: Std) {
    let trim = std.code == STD_FRAC_SECOND_9;
    let n = std.frac_digits;
    if trim && (n == 0 || nanosec == 0) {
        return;
    }
    let dot = if std.comma { b',' } else { b'.' };
    b.push(dot);
    let start = b.len();
    append_int(b, nanosec, 9);
    if n < 9 {
        b.truncate(start + n);
    }
    if trim {
        while b.last() == Some(&b'0') {
            b.pop();
        }
        if b.last() == Some(&dot) {
            b.pop();
        }
    }
}

/// `Time.AppendFormat` / `Format`.
pub fn format_layout(t: &GoTime, layout: &[u8], env: &TemplateEnv) -> Vec<u8> {
    let mut b = Vec::with_capacity(layout.len() + 10);
    let (name, offset) = t.zone(env);
    let (year, month, day) = t.date(env);
    let (hour, min, sec) = t.clock(env);
    let mut layout = layout;
    loop {
        let (prefix, std, suffix) = next_std_chunk(layout);
        b.extend_from_slice(prefix);
        if std.code == 0 {
            break;
        }
        layout = suffix;
        match std.code {
            STD_YEAR => append_int(&mut b, year.abs() % 100, 2),
            STD_LONG_YEAR => append_int(&mut b, year, 4),
            STD_MONTH => b.extend_from_slice(&super::timefns::month_string(month).as_bytes()[..3]),
            STD_LONG_MONTH => b.extend_from_slice(super::timefns::month_string(month).as_bytes()),
            STD_NUM_MONTH => append_int(&mut b, month, 0),
            STD_ZERO_MONTH => append_int(&mut b, month, 2),
            STD_WEEKDAY => {
                b.extend_from_slice(&super::timefns::weekday_string(t.weekday(env)).as_bytes()[..3])
            }
            STD_LONG_WEEKDAY => {
                b.extend_from_slice(super::timefns::weekday_string(t.weekday(env)).as_bytes())
            }
            STD_DAY => append_int(&mut b, day, 0),
            STD_UNDER_DAY => {
                if day < 10 {
                    b.push(b' ');
                }
                append_int(&mut b, day, 0);
            }
            STD_ZERO_DAY => append_int(&mut b, day, 2),
            STD_UNDER_YEAR_DAY => {
                let yday = t.year_day(env);
                if yday < 100 {
                    b.push(b' ');
                    if yday < 10 {
                        b.push(b' ');
                    }
                }
                append_int(&mut b, yday, 0);
            }
            STD_ZERO_YEAR_DAY => append_int(&mut b, t.year_day(env), 3),
            STD_HOUR => append_int(&mut b, hour, 2),
            STD_HOUR12 | STD_ZERO_HOUR12 => {
                let mut hr = hour % 12;
                if hr == 0 {
                    hr = 12;
                }
                append_int(&mut b, hr, if std.code == STD_ZERO_HOUR12 { 2 } else { 0 });
            }
            STD_MINUTE => append_int(&mut b, min, 0),
            STD_ZERO_MINUTE => append_int(&mut b, min, 2),
            STD_SECOND => append_int(&mut b, sec, 0),
            STD_ZERO_SECOND => append_int(&mut b, sec, 2),
            STD_PM => b.extend_from_slice(if hour >= 12 { b"PM" } else { b"AM" }),
            STD_PM_LOWER => b.extend_from_slice(if hour >= 12 { b"pm" } else { b"am" }),
            STD_ISO8601_TZ
            | STD_ISO8601_COLON_TZ
            | STD_ISO8601_SECONDS_TZ
            | STD_ISO8601_SHORT_TZ
            | STD_ISO8601_COLON_SECONDS_TZ
            | STD_NUM_TZ
            | STD_NUM_COLON_TZ
            | STD_NUM_SECONDS_TZ
            | STD_NUM_SHORT_TZ
            | STD_NUM_COLON_SECONDS_TZ => {
                let iso = matches!(
                    std.code,
                    STD_ISO8601_TZ
                        | STD_ISO8601_COLON_TZ
                        | STD_ISO8601_SECONDS_TZ
                        | STD_ISO8601_SHORT_TZ
                        | STD_ISO8601_COLON_SECONDS_TZ
                );
                if offset == 0 && iso {
                    b.push(b'Z');
                    continue;
                }
                let mut zone = offset / 60;
                let mut absoffset = offset;
                if zone < 0 {
                    b.push(b'-');
                    zone = -zone;
                    absoffset = -absoffset;
                } else {
                    b.push(b'+');
                }
                append_int(&mut b, (zone / 60) as i64, 2);
                if matches!(
                    std.code,
                    STD_ISO8601_COLON_TZ
                        | STD_NUM_COLON_TZ
                        | STD_ISO8601_COLON_SECONDS_TZ
                        | STD_NUM_COLON_SECONDS_TZ
                ) {
                    b.push(b':');
                }
                if !matches!(std.code, STD_NUM_SHORT_TZ | STD_ISO8601_SHORT_TZ) {
                    append_int(&mut b, (zone % 60) as i64, 2);
                }
                if matches!(
                    std.code,
                    STD_ISO8601_SECONDS_TZ
                        | STD_NUM_SECONDS_TZ
                        | STD_NUM_COLON_SECONDS_TZ
                        | STD_ISO8601_COLON_SECONDS_TZ
                ) {
                    if matches!(
                        std.code,
                        STD_NUM_COLON_SECONDS_TZ | STD_ISO8601_COLON_SECONDS_TZ
                    ) {
                        b.push(b':');
                    }
                    append_int(&mut b, (absoffset % 60) as i64, 2);
                }
            }
            STD_TZ => {
                if !name.is_empty() {
                    b.extend_from_slice(name.as_bytes());
                } else {
                    let mut zone = offset / 60;
                    if zone < 0 {
                        b.push(b'-');
                        zone = -zone;
                    } else {
                        b.push(b'+');
                    }
                    append_int(&mut b, (zone / 60) as i64, 2);
                    append_int(&mut b, (zone % 60) as i64, 2);
                }
            }
            STD_FRAC_SECOND_0 | STD_FRAC_SECOND_9 => {
                append_nano(&mut b, t.nsec as i64, std);
            }
            _ => {}
        }
    }
    b
}

// ---------------------------------------------------------------------
// Parse
// ---------------------------------------------------------------------

fn is_digit(s: &[u8], i: usize) -> bool {
    i < s.len() && s[i].is_ascii_digit()
}

fn getnum(s: &[u8], fixed: bool) -> Result<(i64, &[u8]), ()> {
    if !is_digit(s, 0) {
        return Err(());
    }
    if !is_digit(s, 1) {
        if fixed {
            return Err(());
        }
        return Ok(((s[0] - b'0') as i64, &s[1..]));
    }
    Ok((((s[0] - b'0') * 10 + (s[1] - b'0')) as i64, &s[2..]))
}

fn getnum3(s: &[u8], fixed: bool) -> Result<(i64, &[u8]), ()> {
    let mut n: i64 = 0;
    let mut i = 0;
    while i < 3 && is_digit(s, i) {
        n = n * 10 + (s[i] - b'0') as i64;
        i += 1;
    }
    if i == 0 || (fixed && i != 3) {
        return Err(());
    }
    Ok((n, &s[i..]))
}

fn atoi(s: &[u8]) -> Result<i64, ()> {
    let (neg, body) = match s.first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    if body.is_empty() || !body.iter().all(|b| b.is_ascii_digit()) {
        return Err(());
    }
    let text = std::str::from_utf8(body).map_err(|_| ())?;
    let v: i64 = text.parse().map_err(|_| ())?;
    Ok(if neg { -v } else { v })
}

fn cutspace(s: &[u8]) -> &[u8] {
    let mut s = s;
    while s.first() == Some(&b' ') {
        s = &s[1..];
    }
    s
}

fn skip<'v>(mut value: &'v [u8], mut prefix: &[u8]) -> Result<&'v [u8], ()> {
    while !prefix.is_empty() {
        if prefix[0] == b' ' {
            if !value.is_empty() && value[0] != b' ' {
                return Err(());
            }
            prefix = cutspace(prefix);
            value = cutspace(value);
            continue;
        }
        if value.is_empty() || value[0] != prefix[0] {
            return Err(());
        }
        prefix = &prefix[1..];
        value = &value[1..];
    }
    Ok(value)
}

/// Case-insensitive ASCII match (Go `match`).
fn match_ci(s1: &[u8], s2: &[u8]) -> bool {
    for i in 0..s1.len() {
        let (mut c1, mut c2) = (s1[i], s2[i]);
        if c1 != c2 {
            c1 |= b'a' - b'A';
            c2 |= b'a' - b'A';
            if c1 != c2 || !c1.is_ascii_lowercase() {
                return false;
            }
        }
    }
    true
}

fn lookup_name<'v>(tab: &[&str], val: &'v [u8]) -> Result<(i64, &'v [u8]), ()> {
    for (i, v) in tab.iter().enumerate() {
        let vb = v.as_bytes();
        if val.len() >= vb.len() && match_ci(&val[..vb.len()], vb) {
            return Ok((i as i64, &val[vb.len()..]));
        }
    }
    Err(())
}

fn parse_nanoseconds(value: &[u8], nbytes: usize) -> Result<i64, ()> {
    if value[0] != b'.' && value[0] != b',' {
        return Err(());
    }
    let mut nbytes = nbytes;
    let mut value = value;
    if nbytes > 10 {
        value = &value[..10];
        nbytes = 10;
    }
    let mut ns = atoi(&value[1..nbytes])?;
    if !(0..1_000_000_000).contains(&ns) {
        return Err(());
    }
    let scale_digits = 10 - nbytes;
    for _ in 0..scale_digits {
        ns *= 10;
    }
    Ok(ns)
}

/// `parseTimeZone` — how many bytes of `value` look like a zone
/// abbreviation.
fn parse_time_zone(value: &[u8]) -> Option<usize> {
    if value.len() < 3 {
        return None;
    }
    if value.len() >= 4 && (&value[..4] == b"ChST" || &value[..4] == b"MeST") {
        return Some(4);
    }
    if &value[..3] == b"GMT" {
        return Some(parse_gmt(value));
    }
    if value[0] == b'+' || value[0] == b'-' {
        let length = parse_signed_offset(value);
        if length > 0 {
            return Some(length);
        }
        return None;
    }
    let mut n_upper = 0;
    while n_upper < 6 {
        if n_upper >= value.len() || !value[n_upper].is_ascii_uppercase() {
            break;
        }
        n_upper += 1;
    }
    match n_upper {
        0 | 1 | 2 | 6 => None,
        5 => {
            if value[4] == b'T' {
                Some(5)
            } else {
                None
            }
        }
        4 => {
            if value[3] == b'T' || &value[..4] == b"WITA" {
                Some(4)
            } else {
                None
            }
        }
        3 => Some(3),
        _ => None,
    }
}

fn parse_gmt(value: &[u8]) -> usize {
    let value = &value[3..];
    if value.is_empty() {
        return 3;
    }
    3 + parse_signed_offset(value)
}

fn parse_signed_offset(value: &[u8]) -> usize {
    let sign = value[0];
    if sign != b'-' && sign != b'+' {
        return 0;
    }
    let rest = &value[1..];
    let mut i = 0;
    let mut x: i64 = 0;
    while i < rest.len() && rest[i].is_ascii_digit() {
        x = x.saturating_mul(10) + (rest[i] - b'0') as i64;
        i += 1;
    }
    if i == 0 || x > 24 {
        return 0;
    }
    1 + i
}

fn days_before(m: i64) -> i64 {
    let adj = if m >= 3 { -2 } else { 0 };
    (214 * m - 211) / 7 + adj
}

fn is_leap(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn days_in(m: i64, year: i64) -> i64 {
    if m == 2 {
        if is_leap(year) { 29 } else { 28 }
    } else {
        // 30 + ((m + m>>3) & 1) — standard trick; keep the table form.
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][((m - 1).rem_euclid(12)) as usize]
    }
}

/// `time.ParseInLocation(layout, value, loc)` — `default_loc` doubles as
/// Go's `local` for zone matching (exactly what ParseInLocation does).
/// Errors are unobservable upstream (`toDate`/`toDateInZone` discard
/// them), so the failure type carries no text.
pub fn parse_in_location(
    layout: &[u8],
    value: &[u8],
    default_loc: &GoLoc,
    env: &TemplateEnv,
) -> Result<GoTime, ParseFail> {
    let mut layout = layout;
    let mut value = value;

    let mut year: i64 = 0;
    let mut month: i64 = -1;
    let mut day: i64 = -1;
    let mut yday: i64 = -1;
    let mut hour: i64 = 0;
    let mut min: i64 = 0;
    let mut sec: i64 = 0;
    let mut nsec: i64 = 0;
    let mut z: Option<GoLoc> = None;
    let mut zone_offset: i64 = -1;
    let mut zone_name: Vec<u8> = Vec::new();
    let mut am_set = false;
    let mut pm_set = false;

    loop {
        let (prefix, std, suffix) = next_std_chunk(layout);
        value = skip(value, prefix)?;
        if std.code == 0 {
            if !value.is_empty() {
                return Err(ParseFail);
            }
            break;
        }
        layout = suffix;
        match std.code {
            STD_YEAR => {
                if value.len() < 2 {
                    return Err(ParseFail);
                }
                let p = &value[..2];
                value = &value[2..];
                year = atoi(p)?;
                if year >= 69 {
                    year += 1900;
                } else {
                    year += 2000;
                }
            }
            STD_LONG_YEAR => {
                if value.len() < 4 || !is_digit(value, 0) {
                    return Err(ParseFail);
                }
                let p = &value[..4];
                value = &value[4..];
                year = atoi(p)?;
            }
            STD_MONTH => {
                let (m, rest) = lookup_name(&SHORT_MONTHS, value)?;
                month = m + 1;
                value = rest;
            }
            STD_LONG_MONTH => {
                let (m, rest) = lookup_name(&super::timefns::MONTH_NAMES, value)?;
                month = m + 1;
                value = rest;
            }
            STD_NUM_MONTH | STD_ZERO_MONTH => {
                let (m, rest) = getnum(value, std.code == STD_ZERO_MONTH)?;
                if m <= 0 || m > 12 {
                    return Err(ParseFail);
                }
                month = m;
                value = rest;
            }
            STD_WEEKDAY => {
                let (_, rest) = lookup_name(&SHORT_DAYS, value)?;
                value = rest;
            }
            STD_LONG_WEEKDAY => {
                let (_, rest) = lookup_name(&super::timefns::WEEKDAY_NAMES, value)?;
                value = rest;
            }
            STD_DAY | STD_UNDER_DAY | STD_ZERO_DAY => {
                if std.code == STD_UNDER_DAY && value.first() == Some(&b' ') {
                    value = &value[1..];
                }
                let (d, rest) = getnum(value, std.code == STD_ZERO_DAY)?;
                day = d;
                value = rest;
            }
            STD_UNDER_YEAR_DAY | STD_ZERO_YEAR_DAY => {
                for _ in 0..2 {
                    if std.code == STD_UNDER_YEAR_DAY && value.first() == Some(&b' ') {
                        value = &value[1..];
                    }
                }
                let (d, rest) = getnum3(value, std.code == STD_ZERO_YEAR_DAY)?;
                yday = d;
                value = rest;
            }
            STD_HOUR => {
                let (h, rest) = getnum(value, false)?;
                if !(0..24).contains(&h) {
                    return Err(ParseFail);
                }
                hour = h;
                value = rest;
            }
            STD_HOUR12 | STD_ZERO_HOUR12 => {
                let (h, rest) = getnum(value, std.code == STD_ZERO_HOUR12)?;
                if !(0..=12).contains(&h) {
                    return Err(ParseFail);
                }
                hour = h;
                value = rest;
            }
            STD_MINUTE | STD_ZERO_MINUTE => {
                let (m, rest) = getnum(value, std.code == STD_ZERO_MINUTE)?;
                if !(0..60).contains(&m) {
                    return Err(ParseFail);
                }
                min = m;
                value = rest;
            }
            STD_SECOND | STD_ZERO_SECOND => {
                let (s, rest) = getnum(value, std.code == STD_ZERO_SECOND)?;
                if !(0..60).contains(&s) {
                    return Err(ParseFail);
                }
                sec = s;
                value = rest;
                // Fractional second in the input but not the layout.
                if value.len() >= 2 && (value[0] == b'.' || value[0] == b',') && is_digit(value, 1)
                {
                    let (_, peek, _) = next_std_chunk(layout);
                    if peek.code != STD_FRAC_SECOND_0 && peek.code != STD_FRAC_SECOND_9 {
                        let mut n = 2;
                        while n < value.len() && is_digit(value, n) {
                            n += 1;
                        }
                        nsec = parse_nanoseconds(value, n)?;
                        value = &value[n..];
                    }
                }
            }
            STD_PM => {
                if value.len() < 2 {
                    return Err(ParseFail);
                }
                match &value[..2] {
                    b"PM" => pm_set = true,
                    b"AM" => am_set = true,
                    _ => return Err(ParseFail),
                }
                value = &value[2..];
            }
            STD_PM_LOWER => {
                if value.len() < 2 {
                    return Err(ParseFail);
                }
                match &value[..2] {
                    b"pm" => pm_set = true,
                    b"am" => am_set = true,
                    _ => return Err(ParseFail),
                }
                value = &value[2..];
            }
            STD_ISO8601_TZ
            | STD_ISO8601_SHORT_TZ
            | STD_ISO8601_COLON_TZ
            | STD_ISO8601_SECONDS_TZ
            | STD_ISO8601_COLON_SECONDS_TZ
            | STD_NUM_TZ
            | STD_NUM_SHORT_TZ
            | STD_NUM_COLON_TZ
            | STD_NUM_SECONDS_TZ
            | STD_NUM_COLON_SECONDS_TZ => {
                let iso = matches!(
                    std.code,
                    STD_ISO8601_TZ
                        | STD_ISO8601_SHORT_TZ
                        | STD_ISO8601_COLON_TZ
                        | STD_ISO8601_SECONDS_TZ
                        | STD_ISO8601_COLON_SECONDS_TZ
                );
                if iso && value.first() == Some(&b'Z') {
                    value = &value[1..];
                    z = Some(GoLoc::Utc);
                    continue;
                }
                let (sign, hh, mm, ss, rest): (u8, &[u8], &[u8], &[u8], &[u8]) = match std.code {
                    STD_ISO8601_COLON_TZ | STD_NUM_COLON_TZ => {
                        if value.len() < 6 || value[3] != b':' {
                            return Err(ParseFail);
                        }
                        (value[0], &value[1..3], &value[4..6], b"00", &value[6..])
                    }
                    STD_NUM_SHORT_TZ | STD_ISO8601_SHORT_TZ => {
                        if value.len() < 3 {
                            return Err(ParseFail);
                        }
                        (value[0], &value[1..3], b"00", b"00", &value[3..])
                    }
                    STD_ISO8601_COLON_SECONDS_TZ | STD_NUM_COLON_SECONDS_TZ => {
                        if value.len() < 9 || value[3] != b':' || value[6] != b':' {
                            return Err(ParseFail);
                        }
                        (
                            value[0],
                            &value[1..3],
                            &value[4..6],
                            &value[7..9],
                            &value[9..],
                        )
                    }
                    STD_ISO8601_SECONDS_TZ | STD_NUM_SECONDS_TZ => {
                        if value.len() < 7 {
                            return Err(ParseFail);
                        }
                        (
                            value[0],
                            &value[1..3],
                            &value[3..5],
                            &value[5..7],
                            &value[7..],
                        )
                    }
                    _ => {
                        if value.len() < 5 {
                            return Err(ParseFail);
                        }
                        (value[0], &value[1..3], &value[3..5], b"00", &value[5..])
                    }
                };
                value = rest;
                let (hr, _) = getnum(hh, true)?;
                let (mm, _) = getnum(mm, true)?;
                let (ss, _) = getnum(ss, true)?;
                if hr > 24 || mm > 60 || ss > 60 {
                    return Err(ParseFail);
                }
                zone_offset = (hr * 60 + mm) * 60 + ss;
                match sign {
                    b'+' => {}
                    b'-' => zone_offset = -zone_offset,
                    _ => return Err(ParseFail),
                }
            }
            STD_TZ => {
                if value.len() >= 3 && &value[..3] == b"UTC" {
                    z = Some(GoLoc::Utc);
                    value = &value[3..];
                    continue;
                }
                let n = parse_time_zone(value).ok_or(())?;
                zone_name = value[..n].to_vec();
                value = &value[n..];
            }
            STD_FRAC_SECOND_0 => {
                let ndigit = 1 + std.frac_digits;
                if value.len() < ndigit {
                    return Err(ParseFail);
                }
                nsec = parse_nanoseconds(value, ndigit)?;
                value = &value[ndigit..];
            }
            STD_FRAC_SECOND_9 => {
                if value.len() < 2
                    || (value[0] != b'.' && value[0] != b',')
                    || !value[1].is_ascii_digit()
                {
                    continue;
                }
                let mut i = 0;
                while i + 1 < value.len() && value[i + 1].is_ascii_digit() {
                    i += 1;
                }
                nsec = parse_nanoseconds(value, 1 + i)?;
                value = &value[1 + i..];
            }
            _ => return Err(ParseFail),
        }
    }

    if pm_set && hour < 12 {
        hour += 12;
    } else if am_set && hour == 12 {
        hour = 0;
    }

    // Convert yday to day/month.
    if yday >= 0 {
        let mut d: i64 = 0;
        let mut m: i64 = 0;
        let mut yday = yday;
        if is_leap(year) {
            if yday == 31 + 29 {
                m = 2;
                d = 29;
            } else if yday > 31 + 29 {
                yday -= 1;
            }
        }
        if !(1..=365).contains(&yday) {
            return Err(ParseFail);
        }
        if m == 0 {
            m = (yday - 1) / 31 + 1;
            if days_before(m + 1) < yday {
                m += 1;
            }
            d = yday - days_before(m);
        }
        if month >= 0 && month != m {
            return Err(ParseFail);
        }
        month = m;
        if day >= 0 && day != d {
            return Err(ParseFail);
        }
        day = d;
    } else {
        if month < 0 {
            month = 1;
        }
        if day < 0 {
            day = 1;
        }
    }

    if day < 1 || day > days_in(month, year) {
        return Err(ParseFail);
    }

    if let Some(z) = z {
        return Ok(go_date(year, month, day, hour, min, sec, nsec, z, env));
    }

    if zone_offset != -1 {
        let t = go_date(year, month, day, hour, min, sec, nsec, GoLoc::Utc, env);
        let t = GoTime {
            sec: t.sec.wrapping_sub(zone_offset),
            nsec: t.nsec,
            loc: t.loc,
        };
        // Does the default location use this offset at that instant?
        let (name, offset) = zone_at(default_loc, t.sec, env);
        if offset as i64 == zone_offset && (zone_name.is_empty() || name.as_bytes() == zone_name) {
            return Ok(t.in_loc(default_loc.clone()));
        }
        return Ok(t.in_loc(GoLoc::Fixed {
            name: String::from_utf8_lossy(&zone_name).into_owned(),
            offset: zone_offset as i32,
        }));
    }

    if !zone_name.is_empty() {
        let t = go_date(year, month, day, hour, min, sec, nsec, GoLoc::Utc, env);
        // Look for the abbreviation in the default location around that
        // instant (Go `lookupName`; probed at the instant and ±6 months
        // to cover the DST phases — the practical zone table).
        let name_str = String::from_utf8_lossy(&zone_name).into_owned();
        for probe in [0i64, -182 * 86_400, 182 * 86_400] {
            let (name, offset) = zone_at(default_loc, t.sec.wrapping_add(probe), env);
            if name == name_str {
                let t = GoTime {
                    sec: t.sec.wrapping_sub(offset as i64),
                    nsec: t.nsec,
                    loc: t.loc,
                };
                return Ok(t.in_loc(default_loc.clone()));
            }
        }
        let mut offset = 0i32;
        if zone_name.len() > 3
            && &zone_name[..3] == b"GMT"
            && let Ok(off) = atoi(&zone_name[3..])
        {
            offset = (off * 3600) as i32;
        }
        return Ok(t.in_loc(GoLoc::Fixed {
            name: name_str,
            offset,
        }));
    }

    Ok(go_date(
        year,
        month,
        day,
        hour,
        min,
        sec,
        nsec,
        default_loc.clone(),
        env,
    ))
}

const SHORT_MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];
const SHORT_DAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
