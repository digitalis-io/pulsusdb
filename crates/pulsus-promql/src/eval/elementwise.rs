//! Elementwise math/trig function application (issue #65, M6-02) — the
//! per-sample leaf helpers `eval_step`'s `PlanExpr::MathFn`/
//! `PlanExpr::ScalarFn` arms call. Arity is encoded **structurally**
//! (each helper takes exactly its operands — no helper indexes a
//! caller-supplied slice), so nothing here can panic on a malformed
//! plan; `eval_step` destructures the planner-guaranteed argument counts
//! and returns a descriptive error on the structurally-impossible
//! mismatch.
//!
//! Prometheus-exactness foot-guns, pinned by the unit goldens below:
//!
//! - **`sgn`** returns `v` itself for `0`/`-0`/`NaN` (upstream `funcSgn`:
//!   `if v < 0 { -1 } else if v > 0 { 1 } else { v }`) — `f64::signum`
//!   would return `1.0` for `0.0` and a sign-only `NaN`, both wrong.
//! - **`deg`/`rad`** use upstream's exact operation order
//!   (`v * 180 / π`, `v * π / 180`) — `f64::to_degrees`/`to_radians`
//!   use a different constant/op order and drift by ULPs.
//! - **[`go_min`]/[`go_max`]** mirror Go `math.Min`/`math.Max`, which
//!   check **infinity before NaN** (plan v2 Δ2): `Max(+Inf, NaN) = +Inf`
//!   but `Max(-Inf, NaN) = NaN`; both-`±0` prefers `+0` for max and
//!   `-0` for min. Rust `f64::min`/`f64::max` return the non-NaN
//!   operand — wrong for every NaN case.

use crate::plan::MathFn;

/// Go `math.Min`: `-Inf` if either operand is `-Inf`, **else** `NaN` if
/// either is `NaN`, else both-`±0` prefers `-0`, else the smaller.
pub(crate) fn go_min(x: f64, y: f64) -> f64 {
    if x == f64::NEG_INFINITY || y == f64::NEG_INFINITY {
        return f64::NEG_INFINITY;
    }
    if x.is_nan() || y.is_nan() {
        return f64::NAN;
    }
    if x == 0.0 && y == 0.0 {
        // Both are ±0 — prefer -0.
        return if x.is_sign_negative() { x } else { y };
    }
    if x < y { x } else { y }
}

/// Go `math.Max`: `+Inf` if either operand is `+Inf`, **else** `NaN` if
/// either is `NaN`, else both-`±0` prefers `+0`, else the larger.
pub(crate) fn go_max(x: f64, y: f64) -> f64 {
    if x == f64::INFINITY || y == f64::INFINITY {
        return f64::INFINITY;
    }
    if x.is_nan() || y.is_nan() {
        return f64::NAN;
    }
    if x == 0.0 && y == 0.0 {
        // Both are ±0 — prefer +0.
        return if x.is_sign_positive() { x } else { y };
    }
    if x > y { x } else { y }
}

/// One of the 23 unary [`MathFn`] discriminants applied to one sample.
/// The four non-unary discriminants (`Clamp`/`ClampMin`/`ClampMax`/
/// `Round`) are dispatched to their own structural helpers by
/// `eval_step` before this is ever called; their arms here keep the
/// match total without a panic (identity — checked by a `debug_assert`).
pub(crate) fn unary(func: MathFn, v: f64) -> f64 {
    match func {
        MathFn::Abs => v.abs(),
        MathFn::Ceil => v.ceil(),
        MathFn::Floor => v.floor(),
        MathFn::Sqrt => v.sqrt(),
        // Upstream funcSgn — NOT f64::signum (see the module doc).
        MathFn::Sgn => {
            if v < 0.0 {
                -1.0
            } else if v > 0.0 {
                1.0
            } else {
                v
            }
        }
        // Upstream funcDeg/funcRad's exact op order — NOT
        // f64::to_degrees/to_radians (see the module doc).
        MathFn::Deg => v * 180.0 / std::f64::consts::PI,
        MathFn::Rad => v * std::f64::consts::PI / 180.0,
        MathFn::Exp => v.exp(),
        MathFn::Ln => v.ln(),
        MathFn::Log2 => v.log2(),
        MathFn::Log10 => v.log10(),
        MathFn::Sin => v.sin(),
        MathFn::Cos => v.cos(),
        MathFn::Tan => v.tan(),
        MathFn::Asin => v.asin(),
        MathFn::Acos => v.acos(),
        MathFn::Atan => v.atan(),
        MathFn::Sinh => v.sinh(),
        MathFn::Cosh => v.cosh(),
        MathFn::Tanh => v.tanh(),
        MathFn::Asinh => v.asinh(),
        MathFn::Acosh => v.acosh(),
        MathFn::Atanh => v.atanh(),
        MathFn::Clamp | MathFn::ClampMin | MathFn::ClampMax | MathFn::Round => {
            debug_assert!(
                false,
                "{func:?} is not unary — eval_step dispatches it to its structural helper"
            );
            v
        }
    }
}

/// Upstream `funcClamp`'s per-sample body: `max(min, min(max, v))` with
/// Go min/max semantics. The `max < min → empty vector` short-circuit is
/// `eval_step`'s (it applies to the whole step, not per sample); a NaN
/// bound never triggers it (`NaN < x` is false) and flows through here
/// to a NaN result instead.
pub(crate) fn clamp(min: f64, max: f64, v: f64) -> f64 {
    go_max(min, go_min(max, v))
}

/// Upstream `funcClampMin`: `max(min, v)`.
pub(crate) fn clamp_min(min: f64, v: f64) -> f64 {
    go_max(min, v)
}

/// Upstream `funcClampMax`: `min(max, v)`.
pub(crate) fn clamp_max(max: f64, v: f64) -> f64 {
    go_min(max, v)
}

/// Upstream `funcRound`: `floor(v/to_nearest + 0.5) * to_nearest`,
/// computed through the reciprocal exactly as upstream does
/// (`toNearestInverse := 1.0 / toNearest; math.Floor(v*toNearestInverse
/// + 0.5) / toNearestInverse`) — same ops, same order, IEEE-exact.
pub(crate) fn round(to_nearest: f64, v: f64) -> f64 {
    let to_nearest_inverse = 1.0 / to_nearest;
    (v * to_nearest_inverse + 0.5).floor() / to_nearest_inverse
}

/// Upstream `funcMaxOf`: [`go_max`] as a scalar function.
pub(crate) fn max_of(a: f64, b: f64) -> f64 {
    go_max(a, b)
}

/// Upstream `funcMinOf`: [`go_min`] as a scalar function.
pub(crate) fn min_of(a: f64, b: f64) -> f64 {
    go_min(a, b)
}

/// Upstream `funcPi`: `math.Pi` (`3.141592653589793`).
pub(crate) fn pi() -> f64 {
    std::f64::consts::PI
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bits_eq(a: f64, b: f64) -> bool {
        a.to_bits() == b.to_bits()
    }

    // --- go_min/go_max: Go's Inf-before-NaN precedence (plan v2 Δ2),
    // both argument orders ---

    #[test]
    fn go_max_prefers_positive_infinity_over_nan_in_both_orders() {
        assert_eq!(go_max(f64::INFINITY, f64::NAN), f64::INFINITY);
        assert_eq!(go_max(f64::NAN, f64::INFINITY), f64::INFINITY);
    }

    #[test]
    fn go_max_of_negative_infinity_and_nan_is_nan_in_both_orders() {
        assert!(go_max(f64::NEG_INFINITY, f64::NAN).is_nan());
        assert!(go_max(f64::NAN, f64::NEG_INFINITY).is_nan());
    }

    #[test]
    fn go_min_prefers_negative_infinity_over_nan_in_both_orders() {
        assert_eq!(go_min(f64::NEG_INFINITY, f64::NAN), f64::NEG_INFINITY);
        assert_eq!(go_min(f64::NAN, f64::NEG_INFINITY), f64::NEG_INFINITY);
    }

    #[test]
    fn go_min_of_positive_infinity_and_nan_is_nan_in_both_orders() {
        assert!(go_min(f64::INFINITY, f64::NAN).is_nan());
        assert!(go_min(f64::NAN, f64::INFINITY).is_nan());
    }

    #[test]
    fn go_min_prefers_negative_zero_in_both_orders() {
        assert!(bits_eq(go_min(-0.0, 0.0), -0.0));
        assert!(bits_eq(go_min(0.0, -0.0), -0.0));
    }

    #[test]
    fn go_max_prefers_positive_zero_in_both_orders() {
        assert!(bits_eq(go_max(-0.0, 0.0), 0.0));
        assert!(bits_eq(go_max(0.0, -0.0), 0.0));
    }

    #[test]
    fn go_min_and_go_max_order_finite_values() {
        assert_eq!(go_min(1.0, 2.0), 1.0);
        assert_eq!(go_min(2.0, 1.0), 1.0);
        assert_eq!(go_max(1.0, 2.0), 2.0);
        assert_eq!(go_max(2.0, 1.0), 2.0);
    }

    // --- max_of/min_of goldens (finite-vs-NaN: NaN wins) ---

    #[test]
    fn min_of_and_max_of_return_nan_for_a_finite_and_nan_pair() {
        assert!(min_of(f64::NAN, 3.0).is_nan());
        assert!(min_of(3.0, f64::NAN).is_nan());
        assert!(max_of(3.0, f64::NAN).is_nan());
        assert!(max_of(f64::NAN, 3.0).is_nan());
    }

    #[test]
    fn max_of_and_min_of_order_finite_values() {
        assert_eq!(max_of(1.0, 2.0), 2.0);
        assert_eq!(min_of(1.0, 2.0), 1.0);
    }

    // --- sgn: value-preserving for 0/-0/NaN (never f64::signum) ---

    #[test]
    fn sgn_preserves_negative_zero_bit_exactly() {
        assert!(bits_eq(unary(MathFn::Sgn, -0.0), -0.0));
        assert!(bits_eq(unary(MathFn::Sgn, 0.0), 0.0));
    }

    #[test]
    fn sgn_preserves_nan() {
        assert!(unary(MathFn::Sgn, f64::NAN).is_nan());
    }

    #[test]
    fn sgn_maps_signs_to_unit_values() {
        assert_eq!(unary(MathFn::Sgn, -5.5), -1.0);
        assert_eq!(unary(MathFn::Sgn, 5.5), 1.0);
        assert_eq!(unary(MathFn::Sgn, f64::NEG_INFINITY), -1.0);
        assert_eq!(unary(MathFn::Sgn, f64::INFINITY), 1.0);
    }

    // --- deg/rad: upstream op order, pinned bit-exact against the
    // upstream corpus values (trig_functions.test: deg(10)/rad(10)) ---

    #[test]
    fn deg_matches_the_upstream_op_order_bit_exactly() {
        assert!(bits_eq(unary(MathFn::Deg, 10.0), 572.9577951308232));
        assert!(bits_eq(unary(MathFn::Deg, -10.0), -572.9577951308232));
    }

    #[test]
    fn rad_matches_the_upstream_op_order_bit_exactly() {
        assert!(bits_eq(unary(MathFn::Rad, 10.0), 0.17453292519943295));
        assert!(bits_eq(unary(MathFn::Rad, -10.0), -0.17453292519943295));
    }

    // --- clamp family ---

    #[test]
    fn clamp_bounds_a_value_between_min_and_max() {
        assert_eq!(clamp(-25.0, 75.0, -50.0), -25.0);
        assert_eq!(clamp(-25.0, 75.0, 0.0), 0.0);
        assert_eq!(clamp(-25.0, 75.0, 100.0), 75.0);
    }

    #[test]
    fn clamp_with_a_nan_bound_is_nan_for_finite_other_bounds() {
        // functions.test:642-650's semantics — a consequence of go_min/
        // go_max's NaN rule with *finite* other bounds, not a blanket
        // any-NaN rule (plan v2 Δ2).
        assert!(clamp(0.0, f64::NAN, -50.0).is_nan());
        assert!(clamp(f64::NAN, 0.0, -50.0).is_nan());
    }

    #[test]
    fn clamp_min_and_clamp_max_apply_one_sided_bounds() {
        assert_eq!(clamp_min(-25.0, -50.0), -25.0);
        assert_eq!(clamp_min(-25.0, 100.0), 100.0);
        assert_eq!(clamp_max(75.0, 100.0), 75.0);
        assert_eq!(clamp_max(75.0, -50.0), -50.0);
    }

    // --- round ---

    #[test]
    fn round_defaults_break_ties_upward() {
        assert_eq!(round(1.0, 2.5), 3.0);
        assert_eq!(round(1.0, -2.5), -2.0);
        assert_eq!(round(1.0, 0.53), 1.0);
    }

    #[test]
    fn round_honors_a_fractional_to_nearest() {
        assert_eq!(round(0.5, 2.5), 2.5);
        assert_eq!(round(0.5, 0.53), 0.5);
    }

    #[test]
    fn round_with_a_negative_to_nearest_matches_the_upstream_formula() {
        // inv = -1: floor(2.5 * -1 + 0.5) / -1 = floor(-2.0) / -1 = 2.
        assert_eq!(round(-1.0, 2.5), 2.0);
        assert_eq!(round(-1.0, -2.5), -3.0);
    }

    // --- pi ---

    #[test]
    fn pi_is_the_f64_pi_constant() {
        // 0x400921FB54442D18 is the IEEE-754 double π — the bit pattern
        // Go's math.Pi (and the upstream corpus's `3.141592653589793`
        // expected) round to.
        assert_eq!(pi().to_bits(), 0x400921FB54442D18);
    }

    // --- IEEE-exact unary spot checks ---

    #[test]
    fn abs_ceil_floor_sqrt_match_ieee_semantics() {
        assert_eq!(unary(MathFn::Abs, -3.7), 3.7);
        assert_eq!(unary(MathFn::Ceil, -3.7), -3.0);
        assert_eq!(unary(MathFn::Floor, -3.7), -4.0);
        assert!(unary(MathFn::Sqrt, -3.7).is_nan());
        assert!(bits_eq(unary(MathFn::Sqrt, 100.5), 10.024968827881711));
    }

    // --- transcendental domain edges (the upstream-copied -Inf/NaN
    // special values, functions.test:1916-1976) ---

    #[test]
    fn logarithms_of_zero_are_negative_infinity() {
        assert_eq!(unary(MathFn::Ln, 0.0), f64::NEG_INFINITY);
        assert_eq!(unary(MathFn::Log2, 0.0), f64::NEG_INFINITY);
        assert_eq!(unary(MathFn::Log10, 0.0), f64::NEG_INFINITY);
    }

    #[test]
    fn logarithms_of_negative_values_are_nan() {
        assert!(unary(MathFn::Ln, -10.0).is_nan());
        assert!(unary(MathFn::Log2, -10.0).is_nan());
        assert!(unary(MathFn::Log10, -10.0).is_nan());
    }

    #[test]
    fn inverse_trig_outside_the_domain_is_nan() {
        assert!(unary(MathFn::Asin, 9.9).is_nan());
        assert!(unary(MathFn::Acos, 9.9).is_nan());
        assert!(unary(MathFn::Acosh, 0.5).is_nan());
        assert!(unary(MathFn::Atanh, 2.0).is_nan());
    }
}
