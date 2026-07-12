//! Timestamp newtypes and the fingerprint type alias. Metrics store
//! millisecond timestamps; logs/traces/profiles store nanosecond
//! timestamps — both stored verbatim from the ingested sample, never
//! quantized or rounded (docs/architecture.md §2).

/// Milliseconds since the Unix epoch, stored verbatim from the ingested
/// sample — never rounded or bucketed. Metrics use millisecond resolution
/// (docs/architecture.md §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UnixMilli(pub i64);

/// Nanoseconds since the Unix epoch, stored verbatim from the ingested
/// sample — never rounded or bucketed. Logs/traces/profiles use
/// nanosecond resolution (docs/architecture.md §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UnixNano(pub i64);

/// A 64-bit series/stream fingerprint. A plain alias, not a newtype: it
/// must match `pulsus-clickhouse`'s `u64` `Row` column type 1:1 (`UInt64`
/// round-trips values above `2^63`, docs/decisions/0001-clickhouse-client.md)
/// with zero conversion overhead at the insert/query boundary.
pub type Fingerprint = u64;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_milli_stores_the_value_verbatim() {
        // Load-bearing: a quantization bug here would silently corrupt
        // ordering keys (docs/architecture.md §2) — asserted, not assumed.
        assert_eq!(UnixMilli(1_700_000_000_123).0, 1_700_000_000_123);
    }

    #[test]
    fn unix_nano_stores_the_value_verbatim() {
        assert_eq!(
            UnixNano(1_700_000_000_123_456_789).0,
            1_700_000_000_123_456_789
        );
    }

    #[test]
    fn unix_milli_ordering_is_numeric_not_reordered() {
        assert!(UnixMilli(1) < UnixMilli(2));
    }

    #[test]
    fn fingerprint_round_trips_values_above_i64_max() {
        let fp: Fingerprint = 0xFFFF_FFFF_FFFF_FFF1;
        assert!(fp > (1u64 << 63));
    }
}
