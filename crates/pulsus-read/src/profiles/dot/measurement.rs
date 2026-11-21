//! Issue #829 part B: value labels and percentages. Stubs: the tests come
//! first.

/// Stub.
pub fn scaled(_v: i64, _unit: &str) -> String {
    String::new()
}

/// Stub.
pub fn percentage(_v: i64, _total: i64) -> String {
    String::new()
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
