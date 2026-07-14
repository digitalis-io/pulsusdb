//! [`KahanSum`]: Neumaier-compensated summation, ported (not re-derived)
//! from Prometheus's own `KahanSumInc` (`promql/floats.go`). Used by every
//! aggregation and `*_over_time` function that sums (`sum`, `avg`,
//! `sum_over_time`, `avg_over_time`) so the accumulated result matches
//! Prometheus's numeric behavior on inputs a naive `f64` sum gets wrong
//! (e.g. `1e100, 1.0, -1e100` — naive summation loses the `1.0` entirely;
//! Neumaier's compensation term recovers it).
//!
//! **Accumulation order is pinned to ascending-fingerprint input order**
//! (the fetch `ORDER BY fingerprint, unix_milli` — docs/schemas.md §2.3),
//! per the architect plan's Open Q1: exact last-ULP parity with
//! Prometheus's own series-storage accumulation order is a #33
//! differential concern, not assumed here.

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

    /// Adds `x` to the running sum, updating the compensation term.
    /// Neumaier's variant (vs. plain Kahan): handles the case where `x` is
    /// larger in magnitude than the running sum, not just the reverse.
    pub fn add(&mut self, x: f64) {
        let t = self.sum + x;
        if self.sum.abs() >= x.abs() {
            self.c += (self.sum - t) + x;
        } else {
            self.c += (x - t) + self.sum;
        }
        self.sum = t;
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
}
