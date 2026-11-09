//! Issue #778: whether any line of `/proc/self/maps` covers a byte range.

use std::ops::Range;

/// The addresses a vector's bytes occupy.
pub fn range_of(v: &[u8]) -> Range<usize> {
    let lo = v.as_ptr() as usize;
    lo..lo + v.len()
}

/// `true` when a mapping of this process covers any byte of `range`.
pub fn covered(range: &Range<usize>) -> bool {
    let maps = std::fs::read_to_string("/proc/self/maps").expect("/proc/self/maps");
    maps.lines().any(|line| {
        let span = line.split_whitespace().next().expect("an address range");
        let (lo, hi) = span.split_once('-').expect("lo-hi");
        let lo = usize::from_str_radix(lo, 16).expect("hex");
        let hi = usize::from_str_radix(hi, 16).expect("hex");
        lo < range.end && range.start < hi
    })
}
