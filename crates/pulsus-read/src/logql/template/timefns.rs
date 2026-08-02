//! The Go `time` domain for templates (issue #230, plan v2 Δ1): the
//! `time.Time` value model with Go's INTERNAL representation semantics
//! (wall/ext for the `%d` struct dumps, nil-`loc` UTC normalisation),
//! the civil-time math ported from `time.go` (Neri-Schneider absolute
//! days — works for every representable instant, year 0 included), the
//! Stringers (`Time`/`Duration`/`Month`/`Weekday`/`Location`), and the
//! zone model. IANA zone offsets come from `chrono-tz` (pinned 0.10.4;
//! tzdata skew vs the reference's Go 1.26.5 database is a ledgered
//! residual — plan v1 §8.3).
//!
//! `TemplateEnv` carries the `Local` zone and the wall clock (`now_ns`
//! injectable for tests, per plan v1 §5). The reference resolves the zone
//! from the *process* (`initLocal`: `$TZ`, else `/etc/localtime`);
//! PulsusDB resolves it from *server configuration* instead (issue #311 —
//! see [`super::install_template_timezone`]), so nothing here reads the
//! host environment. The default zone, `UTC`, is Go's degenerate all-nil
//! `Local` form (`local: None` here) — the PROVENANCE capture
//! precondition, and what the stock reference container also produces.

use std::str::FromStr;

use super::value::GoLoc;

/// Seconds from Jan 1 year 1 (Go's internal epoch) to Jan 1 1970.
pub const UNIX_TO_INTERNAL: i64 = 62_135_596_800;
/// `unixToAbsolute` as a wrapping u64 (Go: `uint64(unixToInternal -
/// absoluteToInternal)`, probed from the toolchain).
const UNIX_TO_ABSOLUTE: u64 = 9_223_372_028_741_760_000;
const SECONDS_PER_DAY: u64 = 86_400;
/// `absoluteYears` (Go `time.go`).
const ABSOLUTE_YEARS: u64 = 292_277_022_400;
/// Days from March 1 through December 31.
const MARCH_THRU_DECEMBER: u64 = 306;

pub const MIN_DURATION: i64 = i64::MIN;
pub const MAX_DURATION: i64 = i64::MAX;

/// The execution environment (plan v1 §5). `local`: `None` = the
/// degenerate "UTC" Local (Go's all-nil form); `Some(tz)` = a resolved
/// Local whose location NAME is the zone's own IANA name.
#[derive(Debug, Clone, Default)]
pub struct TemplateEnv {
    pub local: Option<chrono_tz::Tz>,
    /// What `time.Local.String()` reports: the configured zone's IANA
    /// name, matching the reference's `$TZ=<name>` branch (which keeps
    /// the loaded zone's own name — only its `/etc/localtime` branch
    /// renames the result to `"Local"`, and PulsusDB has no such
    /// branch). `None` falls back to "UTC".
    pub local_name: Option<String>,
    /// Injectable wall clock for `now` (tests); `None` = system time.
    pub now_ns: Option<i64>,
}

impl TemplateEnv {
    /// The environment for an explicitly chosen zone (issue #311): the
    /// server-configured `reader.template_timezone`, never anything read
    /// from the host.
    ///
    /// `UTC` maps to the all-nil degenerate `Local` Go itself produces
    /// when no zone is configured, so the default deployment's rendered
    /// output is byte-identical to what it was before the setting
    /// existed. Every other zone keeps its own IANA name, matching the
    /// reference's `$TZ=<name>` resolution — a fleet that configures its
    /// zone gets exactly what the reference gave it.
    pub fn for_timezone(tz: chrono_tz::Tz) -> Self {
        if tz == chrono_tz::Tz::UTC {
            return TemplateEnv::default();
        }
        TemplateEnv {
            local: Some(tz),
            local_name: Some(tz.name().to_string()),
            now_ns: None,
        }
    }

    pub fn now(&self) -> GoTime {
        let ns = self.now_ns.unwrap_or_else(|| {
            use std::time::{SystemTime, UNIX_EPOCH};
            match SystemTime::now().duration_since(UNIX_EPOCH) {
                Ok(d) => i64::try_from(d.as_nanos()).unwrap_or(i64::MAX),
                Err(_) => 0,
            }
        });
        GoTime::from_unix_ns(ns)
    }
}

/// A Go `time.Time` (wall-clock only — no monotonic reading is ever
/// attached on the template paths). `sec` is UNIX seconds; Go's
/// internal `ext` is `sec + UNIX_TO_INTERNAL`.
#[derive(Debug, Clone, PartialEq)]
pub struct GoTime {
    pub sec: i64,
    /// 0..=999_999_999.
    pub nsec: i32,
    pub loc: GoLoc,
}

impl GoTime {
    /// `time.Unix(0, ns)` — Local zone.
    pub fn from_unix_ns(ns: i64) -> GoTime {
        let mut sec = ns.wrapping_div(1_000_000_000);
        let mut nsec = (ns % 1_000_000_000) as i32;
        if nsec < 0 {
            nsec += 1_000_000_000;
            sec -= 1;
        }
        GoTime {
            sec,
            nsec,
            loc: GoLoc::Local,
        }
    }

    /// `time.Unix(sec, nsec)`.
    pub fn from_unix(sec: i64, nsec: i64) -> GoTime {
        let mut sec = sec;
        let mut nsec = nsec;
        if !(0..1_000_000_000).contains(&nsec) {
            let n = nsec.div_euclid(1_000_000_000);
            sec = sec.wrapping_add(n);
            nsec -= n * 1_000_000_000;
        }
        GoTime {
            sec,
            nsec: nsec as i32,
            loc: GoLoc::Local,
        }
    }

    /// The zero `time.Time` (Jan 1 year 1, UTC).
    pub fn zero() -> GoTime {
        GoTime {
            sec: -UNIX_TO_INTERNAL,
            nsec: 0,
            loc: GoLoc::Utc,
        }
    }

    /// Go's `(wall, ext)` internal pair for the struct-dump verbs
    /// (wall-clock-only: `wall` = nsec, `ext` = internal seconds).
    pub fn internal_repr(&self) -> (u64, i64) {
        (self.nsec as u64, self.sec.wrapping_add(UNIX_TO_INTERNAL))
    }

    /// Whether Go's `loc` field is nil (UTC-normalised, `setLoc`).
    pub fn loc_pointer(&self, _env: &TemplateEnv) -> Option<GoLoc> {
        match self.loc {
            GoLoc::Utc => None,
            ref l => Some(l.clone()),
        }
    }

    /// Zone abbreviation + offset seconds at this instant.
    pub fn zone(&self, env: &TemplateEnv) -> (String, i32) {
        zone_at(&self.loc, self.sec, env)
    }

    /// Absolute seconds (Go `absSec`), offset applied.
    fn abs(&self, env: &TemplateEnv) -> u64 {
        let (_, offset) = self.zone(env);
        (self.sec.wrapping_add(offset as i64) as u64).wrapping_add(UNIX_TO_ABSOLUTE)
    }

    /// (year, month 1-12, day) in the time's zone.
    pub fn date(&self, env: &TemplateEnv) -> (i64, i64, i64) {
        let days = self.abs(env) / SECONDS_PER_DAY;
        let (y, m, d) = abs_days_date(days);
        (y, m, d)
    }

    pub fn clock(&self, env: &TemplateEnv) -> (i64, i64, i64) {
        let secs = self.abs(env) % SECONDS_PER_DAY;
        (
            (secs / 3600) as i64,
            (secs / 60 % 60) as i64,
            (secs % 60) as i64,
        )
    }

    pub fn weekday(&self, env: &TemplateEnv) -> i64 {
        let days = self.abs(env) / SECONDS_PER_DAY;
        ((days + 3) % 7) as i64 // March 1 of the absolute year is a Wednesday
    }

    pub fn year_day(&self, env: &TemplateEnv) -> i64 {
        let days = self.abs(env) / SECONDS_PER_DAY;
        abs_days_year_yday(days).1
    }

    pub fn unix(&self) -> i64 {
        self.sec
    }
    pub fn unix_milli(&self) -> i64 {
        self.sec
            .wrapping_mul(1000)
            .wrapping_add((self.nsec / 1_000_000) as i64)
    }
    pub fn unix_micro(&self) -> i64 {
        self.sec
            .wrapping_mul(1_000_000)
            .wrapping_add((self.nsec / 1000) as i64)
    }
    pub fn unix_nano(&self) -> i64 {
        self.sec
            .wrapping_mul(1_000_000_000)
            .wrapping_add(self.nsec as i64)
    }

    pub fn is_zero(&self) -> bool {
        self.sec == -UNIX_TO_INTERNAL && self.nsec == 0
    }

    /// `Time.Add(d)` — wrapping like Go.
    pub fn add(&self, d: i64) -> GoTime {
        let mut sec = self.sec.wrapping_add(d / 1_000_000_000);
        let mut nsec = self.nsec + (d % 1_000_000_000) as i32;
        if nsec >= 1_000_000_000 {
            sec = sec.wrapping_add(1);
            nsec -= 1_000_000_000;
        } else if nsec < 0 {
            sec = sec.wrapping_sub(1);
            nsec += 1_000_000_000;
        }
        GoTime {
            sec,
            nsec,
            loc: self.loc.clone(),
        }
    }

    /// `Time.Sub(u)` — clamped to the Duration range.
    pub fn sub(&self, u: &GoTime) -> i64 {
        let d = (self.sec as i128 - u.sec as i128) * 1_000_000_000
            + (self.nsec as i128 - u.nsec as i128);
        if d < MIN_DURATION as i128 {
            MIN_DURATION
        } else if d > MAX_DURATION as i128 {
            MAX_DURATION
        } else {
            d as i64
        }
    }

    pub fn before(&self, u: &GoTime) -> bool {
        self.sec < u.sec || (self.sec == u.sec && self.nsec < u.nsec)
    }
    pub fn after(&self, u: &GoTime) -> bool {
        u.before(self)
    }
    pub fn equal(&self, u: &GoTime) -> bool {
        self.sec == u.sec && self.nsec == u.nsec
    }
    pub fn compare(&self, u: &GoTime) -> i64 {
        if self.before(u) {
            -1
        } else if self.after(u) {
            1
        } else {
            0
        }
    }

    /// `Time.Truncate(d)`.
    pub fn truncate(&self, d: i64) -> GoTime {
        if d <= 0 {
            return self.clone();
        }
        let r = self.div_remainder(d);
        self.add(-r)
    }

    /// `Time.Round(d)` (half rounds up).
    pub fn round(&self, d: i64) -> GoTime {
        if d <= 0 {
            return self.clone();
        }
        let r = self.div_remainder(d);
        if less_than_half(r, d) {
            self.add(-r)
        } else {
            self.add(d - r)
        }
    }

    /// Go `div(t, d)`'s remainder: Euclidean remainder of the internal
    /// nanosecond timeline by `d`.
    fn div_remainder(&self, d: i64) -> i64 {
        let internal = self.sec.wrapping_add(UNIX_TO_INTERNAL) as i128;
        let total = internal * 1_000_000_000 + self.nsec as i128;
        total.rem_euclid(d as i128) as i64
    }

    pub fn in_loc(&self, loc: GoLoc) -> GoTime {
        GoTime {
            sec: self.sec,
            nsec: self.nsec,
            loc,
        }
    }

    pub fn utc(&self) -> GoTime {
        self.in_loc(GoLoc::Utc)
    }

    pub fn local(&self) -> GoTime {
        self.in_loc(GoLoc::Local)
    }

    /// `Time.AddDate`.
    pub fn add_date(&self, env: &TemplateEnv, years: i64, months: i64, days: i64) -> GoTime {
        let (y, m, d) = self.date(env);
        let (hh, mm, ss) = self.clock(env);
        go_date(
            y + years,
            m + months,
            d + days,
            hh,
            mm,
            ss,
            self.nsec as i64,
            self.loc.clone(),
            env,
        )
    }

    /// `Time.String()`: `Format("2006-01-02 15:04:05.999999999 -0700 MST")`.
    pub fn string(&self, env: &TemplateEnv) -> String {
        let out =
            super::golayout::format_layout(self, b"2006-01-02 15:04:05.999999999 -0700 MST", env);
        String::from_utf8_lossy(&out).into_owned()
    }

    /// `Time.GoString()`.
    pub fn go_string(&self, env: &TemplateEnv) -> String {
        let (y, m, d) = self.date(env);
        let (hh, mm, ss) = self.clock(env);
        let month = if (1..=12).contains(&m) {
            format!("time.{}", MONTH_NAMES[(m - 1) as usize])
        } else {
            // Go writes the quoted %!Month(n) text.
            format!("%!Month({m})")
        };
        let loc = match &self.loc {
            GoLoc::Utc => "time.UTC".to_string(),
            GoLoc::Local => "time.Local".to_string(),
            GoLoc::Named(tz) => format!("time.Location(\"{}\")", tz.name()),
            GoLoc::Fixed { name, .. } => format!("time.Location(\"{name}\")"),
        };
        format!(
            "time.Date({y}, {month}, {d}, {hh}, {mm}, {ss}, {}, {loc})",
            self.nsec
        )
    }
}

fn less_than_half(x: i64, y: i64) -> bool {
    (x as u64).wrapping_add(x as u64) < y as u64
}

// ---------------------------------------------------------------------
// Civil math (Neri-Schneider, ported from time.go)
// ---------------------------------------------------------------------

/// `dateToAbsDays`.
fn date_to_abs_days(year: i64, month: i64, day: i64) -> u64 {
    let mut amonth = month as u32 as u64; // Go uint32(month)
    let jan_feb: u64 = if (amonth as u32) < 3 { 1 } else { 0 };
    amonth = (amonth as u32).wrapping_add(12 * jan_feb as u32) as u64;
    let y = (year as u64)
        .wrapping_sub(jan_feb)
        .wrapping_add(ABSOLUTE_YEARS);
    let ayday = (979u64.wrapping_mul(amonth).wrapping_sub(2919) as u32 >> 5) as u64;
    let century = y / 100;
    let cyear = (y % 100) as u32;
    let cday = 1461u64 * cyear as u64 / 4;
    let centurydays = 146097u64.wrapping_mul(century) / 4;
    centurydays.wrapping_add((cday as i64 + ayday as i64 + day - 1) as u64)
}

/// `absDays.split` + `.date()`: (year, month 1-12, day).
fn abs_days_date(days: u64) -> (i64, i64, i64) {
    let (century, cyear, ayday) = split_days(days);
    let d = 2141u32.wrapping_mul(ayday) + 197_913;
    let amonth = (d >> 16) as i64;
    let mday = 1 + ((d & 0xFFFF) / 2141) as i64;
    let jan_feb: i64 = if ayday as u64 >= MARCH_THRU_DECEMBER {
        1
    } else {
        0
    };
    let year =
        (century.wrapping_mul(100).wrapping_sub(ABSOLUTE_YEARS) as i64) + cyear as i64 + jan_feb;
    let month = amonth - jan_feb * 12;
    (year, month, mday)
}

/// `absDays.yearYday`.
fn abs_days_year_yday(days: u64) -> (i64, i64) {
    let (century, cyear, ayday) = split_days(days);
    let jan_feb: i64 = if ayday as u64 >= MARCH_THRU_DECEMBER {
        1
    } else {
        0
    };
    let year =
        (century.wrapping_mul(100).wrapping_sub(ABSOLUTE_YEARS) as i64) + cyear as i64 + jan_feb;
    // leap(century, cyear)
    let y4ok = (cyear % 4 == 0) as i64;
    let y100ok = (cyear != 0) as i64;
    let y400ok = (century % 4 == 0) as i64;
    let leap = y4ok & (y100ok | y400ok);
    let yday = ayday as i64 + (1 + 31 + 28) + (leap & !jan_feb) - 365 * jan_feb;
    (year, yday)
}

fn split_days(days: u64) -> (u64, u32, u32) {
    let d = 4u64.wrapping_mul(days).wrapping_add(3);
    let century = d / 146_097;
    let cd = (d % 146_097) as u32 | 3;
    let prod = 2_939_745u64 * cd as u64;
    let cyear = (prod >> 32) as u32;
    let ayday = ((prod & 0xFFFF_FFFF) as u32) / 2_939_745 / 4;
    (century, cyear, ayday)
}

/// `time.Date` — normalisation + the reference's two-step zone-offset
/// resolution (plan v1 §"port that with chrono-tz lookups").
#[allow(clippy::too_many_arguments)]
pub fn go_date(
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    min: i64,
    sec: i64,
    nsec: i64,
    loc: GoLoc,
    env: &TemplateEnv,
) -> GoTime {
    // Normalize month, overflowing into year.
    let (year, m) = norm(year, month - 1, 12);
    let month = m + 1;
    // Normalize nsec/sec/min/hour, overflowing upward.
    let (sec, nsec) = norm(sec, nsec, 1_000_000_000);
    let (min, sec) = norm(min, sec, 60);
    let (hour, min) = norm(hour, min, 60);
    let (day, hour) = norm(day, hour, 24);

    let abs_days = date_to_abs_days(year, month, day);
    // absoluteToUnix = -unixToAbsolute (both wrap in 64 bits).
    let mut unix = (abs_days as i64)
        .wrapping_mul(86_400)
        .wrapping_add(hour * 3600 + min * 60 + sec)
        .wrapping_sub(UNIX_TO_ABSOLUTE as i64);

    // Zone-offset adjustment (Go Date():1755-1765): look up as if UTC,
    // re-look-up shifted when the first guess lands elsewhere.
    let (_, offset) = zone_at(&loc, unix, env);
    if offset != 0 {
        let utc = unix.wrapping_sub(offset as i64);
        let (_, offset2) = zone_at(&loc, utc, env);
        let off = if offset2 != offset { offset2 } else { offset };
        unix = unix.wrapping_sub(off as i64);
    }

    GoTime {
        sec: unix,
        nsec: nsec as i32,
        loc,
    }
}

/// Go `norm`: split hi/lo so 0 <= lo < base.
fn norm(mut hi: i64, mut lo: i64, base: i64) -> (i64, i64) {
    if lo < 0 {
        let n = (-lo - 1) / base + 1;
        hi -= n;
        lo += n * base;
    }
    if lo >= base {
        let n = lo / base;
        hi += n;
        lo -= n * base;
    }
    (hi, lo)
}

// ---------------------------------------------------------------------
// Zones
// ---------------------------------------------------------------------

/// Zone (abbreviation, offset seconds) for a location at a UTC instant.
pub fn zone_at(loc: &GoLoc, unix_sec: i64, env: &TemplateEnv) -> (String, i32) {
    match loc {
        GoLoc::Utc => ("UTC".to_string(), 0),
        GoLoc::Local => match &env.local {
            None => ("UTC".to_string(), 0),
            Some(tz) => tz_zone_at(tz, unix_sec),
        },
        GoLoc::Named(tz) => tz_zone_at(tz, unix_sec),
        GoLoc::Fixed { name, offset } => (name.clone(), *offset),
    }
}

fn tz_zone_at(tz: &chrono_tz::Tz, unix_sec: i64) -> (String, i32) {
    use chrono::{Offset, TimeZone};
    // chrono's NaiveDateTime range is narrower than Go's time range;
    // clamp the LOOKUP instant (the zone rules are periodic/constant
    // beyond the table — ledgered residual for absurd years).
    let clamped = unix_sec.clamp(-8_000_000_000_000, 8_000_000_000_000);
    let dt = match chrono::DateTime::from_timestamp(clamped, 0) {
        Some(dt) => dt,
        None => return ("UTC".to_string(), 0),
    };
    let offset = tz.offset_from_utc_datetime(&dt.naive_utc());
    let secs = offset.fix().local_minus_utc();
    let abbrev = zone_abbrev(&offset, secs);
    (abbrev, secs)
}

fn zone_abbrev(offset: &<chrono_tz::Tz as chrono::TimeZone>::Offset, secs: i32) -> String {
    let name = format!("{offset}");
    // chrono-tz renders numeric pseudo-abbreviations like "+04" itself;
    // trust its Display (it mirrors the tzdata name column).
    if !name.is_empty() {
        name
    } else if secs == 0 {
        "UTC".to_string()
    } else {
        let sign = if secs < 0 { '-' } else { '+' };
        let a = secs.abs();
        if a % 3600 == 0 {
            format!("{sign}{:02}", a / 3600)
        } else {
            format!("{sign}{:02}{:02}", a / 3600, (a % 3600) / 60)
        }
    }
}

/// `Time.IsDST()`.
pub fn is_dst(t: &GoTime, env: &TemplateEnv) -> bool {
    use chrono_tz::OffsetComponents;
    let tz = match &t.loc {
        GoLoc::Utc | GoLoc::Fixed { .. } => None,
        GoLoc::Local => env.local.as_ref(),
        GoLoc::Named(tz) => Some(tz),
    };
    let Some(tz) = tz else { return false };
    let clamped = t.sec.clamp(-8_000_000_000_000, 8_000_000_000_000);
    let Some(dt) = chrono::DateTime::from_timestamp(clamped, 0) else {
        return false;
    };
    use chrono::TimeZone;
    let offset = tz.offset_from_utc_datetime(&dt.naive_utc());
    !offset.dst_offset().is_zero()
}

/// `Location.String()` — what `$t.Location` prints. The degenerate
/// Local (the default, unconfigured zone) prints "UTC"; a configured
/// Local keeps the zone's own IANA name, which is what the reference's
/// `$TZ=<name>` branch also prints. Its `/etc/localtime` branch — the
/// one that prints "Local" — has no counterpart here (issue #311).
pub fn location_name(loc: &GoLoc, env: &TemplateEnv) -> String {
    match loc {
        GoLoc::Utc => "UTC".to_string(),
        GoLoc::Local => match (&env.local, &env.local_name) {
            (Some(_), Some(name)) => name.clone(),
            _ => "UTC".to_string(),
        },
        GoLoc::Named(tz) => tz.name().to_string(),
        GoLoc::Fixed { name, .. } => name.clone(),
    }
}

/// The `name` FIELD of the Location struct (same as the display name).
pub fn location_struct_name(loc: &GoLoc, env: &TemplateEnv) -> String {
    location_name(loc, env)
}

/// `time.LoadLocation` for `toDateInZone`/`In`: "" → UTC, "Local" →
/// Local, else IANA lookup; a failed lookup falls back to UTC exactly
/// like the reference's `toDateInZone` (`fmt.go:174-177`).
pub fn load_location(zone: &str, _env: &TemplateEnv) -> GoLoc {
    match zone {
        "" | "UTC" => GoLoc::Utc,
        "Local" => GoLoc::Local,
        name => match chrono_tz::Tz::from_str(name) {
            Ok(tz) => GoLoc::Named(tz),
            Err(_) => GoLoc::Utc,
        },
    }
}

// ---------------------------------------------------------------------
// Stringers
// ---------------------------------------------------------------------

pub const MONTH_NAMES: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];

pub const WEEKDAY_NAMES: [&str; 7] = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];

/// `Month.String()` (out-of-range prints `%!Month(n)` with Go's uint64
/// rendering of the value).
pub fn month_string(m: i64) -> String {
    if (1..=12).contains(&m) {
        MONTH_NAMES[(m - 1) as usize].to_string()
    } else {
        format!("%!Month({})", m as u64)
    }
}

/// `Weekday.String()`.
pub fn weekday_string(w: i64) -> String {
    if (0..=6).contains(&w) {
        WEEKDAY_NAMES[w as usize].to_string()
    } else {
        format!("%!Weekday({})", w as u64)
    }
}

/// `Duration.String()` — the exact `time.go` format algorithm.
pub fn duration_string(d: i64) -> String {
    let mut buf = [0u8; 32];
    let mut w = buf.len();
    let neg = d < 0;
    let mut u = d.unsigned_abs();

    if u < 1_000_000_000 {
        // Sub-second: ns/µs/ms.
        w -= 1;
        buf[w] = b's';
        let prec;
        if u == 0 {
            return "0s".to_string();
        } else if u < 1_000 {
            prec = 0;
            w -= 1;
            buf[w] = b'n';
        } else if u < 1_000_000 {
            prec = 3;
            // µ is two bytes.
            w -= 2;
            buf[w] = 0xC2;
            buf[w + 1] = 0xB5;
        } else {
            prec = 6;
            w -= 1;
            buf[w] = b'm';
        }
        let (nw, nu) = fmt_frac(&mut buf, w, u, prec);
        w = nw;
        u = nu;
        w = fmt_int(&mut buf, w, u);
    } else {
        w -= 1;
        buf[w] = b's';
        let (nw, nu) = fmt_frac(&mut buf, w, u, 9);
        w = nw;
        u = nu;
        // Integer seconds.
        w = fmt_int(&mut buf, w, u % 60);
        u /= 60;
        if u > 0 {
            w -= 1;
            buf[w] = b'm';
            w = fmt_int(&mut buf, w, u % 60);
            u /= 60;
            if u > 0 {
                w -= 1;
                buf[w] = b'h';
                w = fmt_int(&mut buf, w, u);
            }
        }
    }
    if neg {
        w -= 1;
        buf[w] = b'-';
    }
    String::from_utf8_lossy(&buf[w..]).into_owned()
}

/// `fmtFrac`: fraction digits with trailing zeros (and an all-zero
/// fraction's point) omitted.
fn fmt_frac(buf: &mut [u8; 32], mut w: usize, mut v: u64, prec: usize) -> (usize, u64) {
    let mut print = false;
    for _ in 0..prec {
        let digit = v % 10;
        print = print || digit != 0;
        if print {
            w -= 1;
            buf[w] = digit as u8 + b'0';
        }
        v /= 10;
    }
    if print {
        w -= 1;
        buf[w] = b'.';
    }
    (w, v)
}

fn fmt_int(buf: &mut [u8; 32], mut w: usize, mut v: u64) -> usize {
    if v == 0 {
        w -= 1;
        buf[w] = b'0';
    } else {
        while v > 0 {
            w -= 1;
            buf[w] = (v % 10) as u8 + b'0';
            v /= 10;
        }
    }
    w
}
