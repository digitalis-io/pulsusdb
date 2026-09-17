//! Issue #549: the grouped instant read's result-row shapes, mirroring
//! [`super::sample_rows`]'s `#[derive(Row)]` convention — deserialized
//! straight off `ChClient::query_stream`.
//!
//! **Two shapes, not one.** `min`/`max` return a `Float64` answer beside
//! a `UInt8` flags byte; `count`/`group` return a `UInt32` answer and no
//! flags column at all, because they count a histogram member rather
//! than ignoring it and so have nothing to report about the channel a
//! group's members came from. A single five-column struct would decode a
//! four-column result set as a column-count mismatch.

use pulsus_clickhouse::Row;
use serde::{Deserialize, Serialize};

/// One run row from [`super::grouped_sql::grouped_fetch`]'s `min`/`max`
/// templates: `SELECT gid, min(gi) AS gi_start, max(gi) AS gi_end,
/// any(agg) AS agg, any(flags) AS flags`.
///
/// No `PartialEq` derive — `agg` is a NaN payload whenever the group's
/// members were all NaN, and a derived `==` would report two identical
/// rows unequal.
#[derive(Debug, Clone, Copy, Row, Serialize, Deserialize)]
pub struct GroupedRunRow {
    pub gid: u32,
    pub gi_start: u32,
    pub gi_end: u32,
    pub agg: f64,
    /// Bit 0: the group had a float member at this grid index. Bit 1: it
    /// had a histogram member. `min`/`max` ignore histogram members, so
    /// bit 0 clear is the reference's not-seen rule and the reader drops
    /// the group there.
    pub flags: u8,
}

/// One run row from the `count`/`group` templates: `SELECT gid,
/// min(gi) AS gi_start, max(gi) AS gi_end, any(agg) AS agg`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct GroupedCountRow {
    pub gid: u32,
    pub gi_start: u32,
    pub gi_end: u32,
    pub agg: u32,
}

impl GroupedRunRow {
    pub fn into_run(self) -> super::grouped::Run {
        super::grouped::Run {
            gid: self.gid,
            gi_start: self.gi_start,
            gi_end: self.gi_end,
            agg: self.agg,
            flags: Some(self.flags),
        }
    }
}

impl GroupedCountRow {
    pub fn into_run(self) -> super::grouped::Run {
        super::grouped::Run {
            gid: self.gid,
            gi_start: self.gi_start,
            gi_end: self.gi_end,
            agg: f64::from(self.agg),
            flags: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `count` answer reaches the fold as a float with no flags, and a
    /// `max` answer keeps the flags byte the drop rule reads.
    #[test]
    fn the_two_row_shapes_convert_into_one_run_type() {
        let counted = GroupedCountRow {
            gid: 3,
            gi_start: 0,
            gi_end: 7,
            agg: 42,
        }
        .into_run();
        assert_eq!(counted.gid, 3);
        assert_eq!(counted.agg.to_bits(), 42.0f64.to_bits());
        assert_eq!(counted.flags, None);

        let extremum = GroupedRunRow {
            gid: 1,
            gi_start: 2,
            gi_end: 2,
            agg: f64::NAN,
            flags: 1,
        }
        .into_run();
        assert!(extremum.agg.is_nan());
        assert_eq!(extremum.flags, Some(1));
    }
}
