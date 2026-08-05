//! The reference's log2 histogram model for `histogram_over_time`
//! (issue #252). Pure: no I/O, no randomness.
//!
//! Tempo v3.0.2 (`0c4b926d09234186de39833e9c7ecb5b7614c8b9`) buckets a
//! span's duration with `Log2Bucketize`
//! (`pkg/traceql/engine_metrics.go:2038-2046 @ v3.0.2`):
//!
//! ```go
//! if v < 2 { return -1 }
//! return float64(uint64(1) << (64 - bits.LeadingZeros64(v-1)))
//! ```
//!
//! — the smallest power of two `>= v` — and labels it in float SECONDS
//! (`bucketizeDuration`, `pkg/traceql/ast_metrics.go:181-188 @ v3.0.2`:
//! `Log2Bucketize(d) / float64(time.Second)`), dropping any span with
//! `d < 2` from the series entirely (`NewStaticNil(), false`, which
//! `GroupingAggregator.getGroupingValues` turns into "Totally drop this
//! span", `engine_metrics.go:766-772`).
//!
//! **There is no bucket ladder.** The bucket is an ordinary `by`-key
//! (`internalLabelBucket = "__bucket"`, wired at `ast_metrics.go:131-136`),
//! and `GroupingAggregator.getSeries` creates a series on FIRST
//! OBSERVATION of a key (`engine_metrics.go:788-793`), so a series exists
//! iff that power of two actually occurred somewhere in the window. The
//! inner aggregator is `NewCountOverTimeAggregator` (`Observe` is
//! `c.count++`, `Sample` is `count * rateMult`,
//! `engine_metrics.go:471-477`) held per-interval by an independent
//! `StepAggregator` vector (`:542-556`), so each series is a PLAIN TALLY
//! — never cumulative across buckets or across steps.
//!
//! This module owns only the label rendering and the pure bucket rule;
//! the bucketing itself is pushed down to ClickHouse as
//! `toUInt64(roundToExp2(val - 1)) * 2` under an outer `WHERE val >= 2`
//! ([`super::metrics_sql::metrics_log2_bucket_range_sql`]), so
//! `GROUP BY` returns at most 64 tallies per step and no aggregation
//! moves client-side.

/// The reference's `Log2Bucketize` for a stored span duration: the
/// smallest power of two `>= v`, or `None` when the reference drops the
/// span (`v < 2`, including every negative `v`).
///
/// **Domain: `2..=i64::MAX`, range `2..=2^63`.** `duration_ns` is
/// `Int64` (`crates/pulsus-schema/src/catalog.rs:347`), so that is
/// everything storage can hold; the result needs `u64` because
/// `v > 2^62` buckets to `2^63`, which is not representable as `i64`.
/// No claim is made above `i64::MAX` — unreachable from storage. The
/// reference's own `v - 1 >= 2^63` case (Go yields `0` for a shift at or
/// past the word width) is unreachable for the same reason and gets this
/// sentence rather than a dead branch.
///
/// The SQL twin is `toUInt64(roundToExp2(val - 1)) * 2` guarded by
/// `WHERE val >= 2` on the OUTER query (after the replay dedup). The
/// guard is what excludes `v <= 1`, not the expression: a negative `val`
/// reaching `toUInt64` would produce a large, plausible-looking bucket
/// rather than an error, which is why the guard is tested live
/// (`traces_metrics_live.rs`) and not reasoned about.
pub fn log2_bucketize_ns(v: i64) -> Option<u64> {
    if v < 2 {
        return None;
    }
    // `v >= 2` so `v - 1 >= 1` and `leading_zeros() <= 62`: the shift is
    // always in `1..=63` and never reaches the word width.
    let shift = 64 - (v - 1).leading_zeros();
    Some(1u64 << shift)
}

/// The `__bucket` label value: power-of-two nanoseconds rendered as
/// float seconds.
///
/// Issue #237 (settled — do NOT "fix" this like #232): the reference's
/// ns→seconds conversion is the SINGLE-rounding `float64(ns) / 1e9`.
/// For this site the distinction is moot regardless — every argument is
/// a power of two, and the two rounding forms are bit-identical for
/// every `2^k` (pinned below).
pub fn bucket_seconds(bucket_ns: u64) -> f64 {
    bucket_ns as f64 / 1e9
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reference's expression, transcribed over the SAME `u64`
    /// arithmetic Go uses, so the test compares two independent
    /// derivations rather than the function against itself.
    fn reference_bucketize(v: i64) -> Option<u64> {
        if v < 2 {
            return None;
        }
        let u = v as u64;
        Some(1u64 << (64 - (u - 1).leading_zeros()))
    }

    #[test]
    fn log2_bucketize_equals_the_reference_bit_scan_over_the_storable_domain() {
        // AC2.1: dense low range, then both sides of every power of two
        // the column can hold.
        for v in 2..=4096i64 {
            assert_eq!(log2_bucketize_ns(v), reference_bucketize(v), "v={v}");
        }
        for k in 1..=62u32 {
            let p = 1i64 << k;
            assert_eq!(log2_bucketize_ns(p), Some(1u64 << k), "2^{k}");
            assert_eq!(
                log2_bucketize_ns(p + 1),
                Some(1u64 << (k + 1)),
                "2^{k} + 1 rounds up"
            );
            assert_eq!(log2_bucketize_ns(p), reference_bucketize(p), "2^{k}");
            assert_eq!(
                log2_bucketize_ns(p + 1),
                reference_bucketize(p + 1),
                "2^{k} + 1"
            );
        }
    }

    #[test]
    fn the_top_of_the_storable_domain_buckets_to_two_to_the_sixty_three() {
        // AC2.2: `2^63` is why the bucket is `u64` and not `i64` — as a
        // signed value it is `i64::MIN`, i.e. a NEGATIVE `__bucket`
        // label, which is what the `Int64` form would have emitted.
        for v in [(1i64 << 62) + 1, i64::MAX - 1, i64::MAX] {
            assert_eq!(log2_bucketize_ns(v), Some(1u64 << 63), "v={v}");
        }
        assert_eq!(log2_bucketize_ns(1i64 << 62), Some(1u64 << 62));
        // No input in the domain yields a value that is negative as i64
        // EXCEPT `2^63` itself, which is precisely the case the unsigned
        // type exists for — assert the type carries it losslessly.
        assert_eq!(log2_bucketize_ns(i64::MAX), Some(9_223_372_036_854_775_808));
    }

    #[test]
    fn sub_two_nanosecond_and_negative_durations_have_no_bucket() {
        // AC2.3: the reference returns `-1` for `v < 2` and
        // `bucketizeDuration` turns that into "drop the span".
        for v in [i64::MIN, -1_000_000_000, -2, -1, 0, 1] {
            assert_eq!(log2_bucketize_ns(v), None, "v={v}");
        }
    }

    /// The two-rounding form, transcribed ONLY so this test can assert
    /// the production conversion is form-independent here (issues
    /// #237 / #232 — `exec.rs`'s `agg_value` carries the same guard).
    fn two_rounding_seconds(ns: u64) -> f64 {
        (ns / 1_000_000_000) as f64 + (ns % 1_000_000_000) as f64 / 1e9
    }

    #[test]
    fn bucket_seconds_is_rounding_form_independent_for_every_power_of_two() {
        for k in 0..=62u32 {
            let ns = 1u64 << k;
            assert_eq!(
                bucket_seconds(ns).to_bits(),
                two_rounding_seconds(ns).to_bits(),
                "2^{k} ns must render identically under both rounding forms"
            );
        }
    }

    #[test]
    fn bucket_seconds_reproduces_the_captured_reference_bucket_labels() {
        // Captured from grafana/tempo:3.0.2 (see
        // `tests/golden/traces_metrics/log2_reference_capture.json`).
        for (ns, want) in [
            (1u64 << 1, 2e-9f64),
            (1 << 2, 4e-9),
            (1 << 20, 0.001048576),
            (1 << 29, 0.536870912),
            (1 << 30, 1.073741824),
            (1 << 31, 2.147483648),
            (1 << 33, 8.589934592),
            (1 << 35, 34.359738368),
        ] {
            assert_eq!(bucket_seconds(ns).to_bits(), want.to_bits(), "2^? = {ns}");
        }
    }
}
