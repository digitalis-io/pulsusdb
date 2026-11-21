// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Changed for PulsusDB, issue #829: ported from Go to Rust, and cut to
// the value labels and percentages of the DOT writer.

//! Issue #829 part B (plan §3.2): a sample value written in its unit, and
//! its share of the total.

/// A unit's aliases and its factor to the base unit.
type Unit = (&'static [&'static str], f64);

/// The byte units, base `B`.
const BYTES: &[Unit] = &[
    (&["b", "byte"], 1.0),
    (&["kb", "kbyte", "kilobyte"], (1u64 << 10) as f64),
    (&["mb", "mbyte", "megabyte"], (1u64 << 20) as f64),
    (&["gb", "gbyte", "gigabyte"], (1u64 << 30) as f64),
    (&["tb", "tbyte", "terabyte"], (1u64 << 40) as f64),
    (&["pb", "pbyte", "petabyte"], (1u64 << 50) as f64),
];

/// The time units, base `s`.
const TIME: &[Unit] = &[
    (&["ns", "nanosecond"], 1.0),
    (&["μs", "us", "microsecond"], 1e3),
    (&["ms", "millisecond"], 1e6),
    (&["s", "sec", "second"], 1e9),
    (&["hour", "hr"], 3600e9),
];

/// The factor of `unit` in `units`: lowercased, a trailing `s` dropped when
/// the name is longer than two bytes.
fn sniff(units: &[Unit], unit: &str) -> Option<f64> {
    let lower = unit.to_lowercase();
    let name = if lower.len() > 2 {
        lower.strip_suffix('s').unwrap_or(&lower)
    } else {
        lower.as_str()
    };
    units
        .iter()
        .find(|(aliases, _)| aliases.contains(&name))
        .map(|(_, factor)| *factor)
}

/// `v` in `unit`, written in the base unit of its kind: bytes as `B`, time
/// as `s`, any other unit as the number alone; two decimals, `.00` dropped.
pub fn scaled(v: i64, unit: &str) -> String {
    let (x, suffix) = if let Some(f) = sniff(BYTES, unit) {
        (v as f64 * f, "B")
    } else if let Some(f) = sniff(TIME, unit) {
        (v as f64 * f / 1e9, "s")
    } else {
        (v as f64, "")
    };
    let s = format!("{x:.2}");
    let s = s.strip_suffix(".00").unwrap_or(&s);
    if s == "0" || s == "-0" {
        return "0".to_string();
    }
    format!("{s}{suffix}")
}

/// `v` as a percentage of `total`: `100%` within 0.05 of it, two decimals
/// from 1, else two significant digits.
pub fn percentage(v: i64, total: i64) -> String {
    let r = if total != 0 {
        (v as f64 / total as f64).abs() * 100.0
    } else {
        0.0
    };
    if (99.95..=100.05).contains(&r) {
        return "100%".to_string();
    }
    if r >= 1.0 {
        return format!("{r:.2}%");
    }
    format!("{}%", two_significant(r))
}

/// `r` with two significant digits and no trailing zeros, an exponent
/// below -4 written as `e-05`.
fn two_significant(r: f64) -> String {
    let e = format!("{r:.1e}");
    let (mantissa, exponent) = e.split_once('e').unwrap_or((&e, "0"));
    let x: i32 = exponent.parse().unwrap_or(0);
    if x < -4 {
        let m = trim_zeros(mantissa);
        let sign = if x < 0 { '-' } else { '+' };
        return format!("{m}e{sign}{:02}", x.unsigned_abs());
    }
    let decimals = usize::try_from(1 - x).unwrap_or(0);
    trim_zeros(&format!("{r:.decimals$}")).to_string()
}

/// `s` without trailing zeros after its decimal point, and without the
/// point when nothing follows it.
fn trim_zeros(s: &str) -> &str {
    if !s.contains('.') {
        return s;
    }
    s.trim_end_matches('0').trim_end_matches('.')
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A value in a larger byte unit is written in bytes, its factor
    /// applied: 1 kilobyte is `1024B`.
    #[test]
    fn a_byte_unit_is_written_in_bytes() {
        assert_eq!(scaled(1, "kilobytes"), "1024B");
        assert_eq!(scaled(3, "bytes"), "3B");
        assert_eq!(scaled(320_000_000, "nanoseconds"), "0.32s");
        assert_eq!(scaled(7, "count"), "7");
        assert_eq!(scaled(0, "nanoseconds"), "0");
    }

    #[test]
    fn a_small_share_has_two_significant_digits() {
        assert_eq!(percentage(3, 1000), "0.3%");
        assert_eq!(percentage(1, 100_000), "0.001%");
        assert_eq!(percentage(1536, 3_221_227_011), "4.8e-05%");
        assert_eq!(percentage(0, 10), "0%");
        assert_eq!(percentage(17, 32), "53.12%");
        assert_eq!(percentage(32, 32), "100%");
    }
}
