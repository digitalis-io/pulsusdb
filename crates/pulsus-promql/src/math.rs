//! [`KahanSum`]: Neumaier-compensated summation, ported (not re-derived)
//! from Prometheus v3.13.0's own `util/kahansum.Inc` (the function moved
//! there from the older `promql/floats.go`; issue #39 re-verified the
//! citation against the pinned tag while auditing every summing function
//! for bit-exactness). Used by every aggregation and `*_over_time`
//! function that sums (`sum`, `avg`, `sum_over_time`, `avg_over_time`) so
//! the accumulated result matches Prometheus's numeric behavior on inputs
//! a naive `f64` sum gets wrong (e.g. `1e100, 1.0, -1e100` — naive
//! summation loses the `1.0` entirely; Neumaier's compensation term
//! recovers it).
//!
//! **Accumulation order is pinned to ascending-fingerprint input order**
//! (the fetch `ORDER BY fingerprint, unix_milli` — docs/schemas.md §2.3),
//! per the architect plan's Open Q1: exact last-ULP parity with
//! Prometheus's own series-storage accumulation order is a #33
//! differential concern, not assumed here.

/// One Neumaier-compensated increment — ported operation-for-operation
/// from `util/kahansum.Inc` (v3.13.0): `t := sum + inc`, then either reset
/// the compensation term to exactly `0.0` (never accumulate into it) if
/// `t` overflowed to `±Inf`, or fold the rounding error into `c` via
/// whichever of `(sum-t)+inc` / `(inc-t)+sum` applies depending on which
/// operand is larger in magnitude. Exposed standalone (not only through
/// [`KahanSum`]) because `avg_over_time`'s upstream incremental-mean
/// fallback (issue #39, `crates/pulsus-promql/src/eval/functions.rs`)
/// needs the raw `(sum, c)` pair mid-computation, not a value folded
/// through [`KahanSum::value`].
///
/// The `t.is_infinite()` guard is the one piece a prior version of
/// [`KahanSum::add`] was missing (issue #39 audit): without it, a
/// mid-computation overflow to `±Inf` leaves `c` accumulating from
/// `Inf`-tainted arithmetic (`(sum - t)` becomes `-Inf` or `NaN`) instead
/// of being discarded, which upstream explicitly avoids.
pub(crate) fn kahan_inc(inc: f64, sum: f64, c: f64) -> (f64, f64) {
    let t = sum + inc;
    // NOTE: the extra parens are load-bearing, not stylistic — Go's `c +=
    // (sum - t) + inc` evaluates the RHS `(sum - t) + inc` as one value
    // *before* adding it to `c` (i.e. `c + ((sum-t)+inc)`); Rust's `+` is
    // also left-associative, so writing `c + (sum - t) + inc` here would
    // silently regroup to `(c + (sum-t)) + inc` instead, a different
    // (mis-rounding) floating-point expression (issue #39 audit finding).
    let new_c = if t.is_infinite() {
        0.0
    } else if sum.abs() >= inc.abs() {
        c + ((sum - t) + inc)
    } else {
        c + ((inc - t) + sum)
    };
    (t, new_c)
}

/// Neumaier-compensated running sum: `sum` plus a compensation term `c`
/// tracking the low-order bits lost to floating-point rounding at each
/// addition. [`KahanSum::value`] returns `sum + c`, the compensated total.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct KahanSum {
    sum: f64,
    c: f64,
}

impl KahanSum {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `x` to the running sum, updating the compensation term via
    /// [`kahan_inc`] (Neumaier's variant vs. plain Kahan: handles the case
    /// where `x` is larger in magnitude than the running sum, not just the
    /// reverse — and, since issue #39, resets `c` rather than corrupting it
    /// if the running sum overflows to `±Inf`).
    pub fn add(&mut self, x: f64) {
        let (t, c) = kahan_inc(x, self.sum, self.c);
        self.sum = t;
        self.c = c;
    }

    /// The compensated total.
    pub fn value(&self) -> f64 {
        self.sum + self.c
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The hand-derived golden case a naive `f64` sum gets wrong (AC):
    /// summing `1e100, 1.0, -1e100` in that order, a naive running sum
    /// loses the `1.0` entirely (`1e100 + 1.0 == 1e100` in `f64`, then
    /// `1e100 - 1e100 == 0.0`) — Neumaier compensation recovers it exactly.
    #[test]
    fn kahan_sum_recovers_a_value_a_naive_sum_loses() {
        let naive: f64 = 1e100_f64 + 1.0 - 1e100;
        assert_eq!(naive, 0.0, "sanity check: naive summation loses the 1.0");

        let mut k = KahanSum::new();
        k.add(1e100);
        k.add(1.0);
        k.add(-1e100);
        assert_eq!(k.value(), 1.0);
    }

    #[test]
    fn kahan_sum_of_no_values_is_zero() {
        assert_eq!(KahanSum::new().value(), 0.0);
    }

    #[test]
    fn kahan_sum_matches_naive_summation_on_well_conditioned_input() {
        let mut k = KahanSum::new();
        for v in [1.0, 2.0, 3.0, 4.0] {
            k.add(v);
        }
        assert_eq!(k.value(), 10.0);
    }

    #[test]
    fn kahan_sum_handles_a_large_value_added_after_small_values() {
        // Neumaier's variant (vs. plain Kahan) is exercised here: the
        // magnitude comparison branch fires when the newly-added value is
        // larger than the running sum, not just the reverse.
        let mut k = KahanSum::new();
        k.add(1.0);
        k.add(1e100);
        k.add(-1e100);
        assert_eq!(k.value(), 1.0);
    }

    /// Issue #39 audit finding: `util/kahansum.Inc` resets `c` to exactly
    /// `0.0` (never accumulates into it) once the running sum overflows to
    /// `±Inf` — a prior version of [`KahanSum::add`] lacked this guard, so
    /// an overflowing intermediate sum could leave `c` holding `-Inf`/`NaN`
    /// garbage that then corrupted the result even if a later value would
    /// otherwise have brought the running sum back into a meaningful
    /// range's compensation.
    #[test]
    fn kahan_sum_resets_the_compensation_term_on_overflow_rather_than_corrupting_it() {
        let mut k = KahanSum::new();
        k.add(f64::MAX);
        k.add(f64::MAX); // sum overflows to +Inf here.
        assert!(k.value().is_infinite() && k.value() > 0.0);
        // The raw `kahan_inc` primitive: once `t` is infinite, `c` is
        // exactly `0.0`, not `(sum - t) + inc` evaluated against `-Inf`.
        let (t, c) = kahan_inc(f64::MAX, 0.0, 0.0);
        assert!(t.is_finite());
        let (t2, c2) = kahan_inc(f64::MAX, t, c);
        assert!(t2.is_infinite());
        assert_eq!(c2, 0.0, "compensation term must reset to 0.0 on overflow");
    }
}
