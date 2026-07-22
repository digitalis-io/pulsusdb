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

/// The default `metric_series` activity-bucket width in milliseconds
/// (docs/schemas.md §2.1, `PULSUS_SERIES_ACTIVITY_BUCKET`,
/// `pulsus_config::ReaderConfig::series_activity_bucket`'s documented
/// default, `1h`). Duplicated here as an `i64` constant — not derived from
/// `pulsus-config` (this crate does not depend on it) — so both the writer
/// (issue #26, bucket-floors `metric_series.unix_milli` at registration) and
/// the reader (issue #30, renders the same floor into its historical-bound
/// SQL) can pin their default against one source without a cross-crate
/// dependency cycle; `pulsus-config`'s own default is cross-checked against
/// this constant in `pulsus-write`'s test suite.
pub const DEFAULT_ACTIVITY_BUCKET_MS: i64 = 3_600_000;

/// Floors `unix_milli` to the nearest (lower-or-equal) multiple of
/// `bucket_ms` — the `metric_series` activity-bucket floor (docs/schemas.md
/// §2.1). **Truncating (toward-zero) division**, matching ClickHouse's
/// `intDiv` bit-for-bit — deliberately NOT [`i64::div_euclid`] (floor
/// division), which diverges from `intDiv` for negative `unix_milli` (e.g.
/// `intDiv(-1, 3_600_000) * 3_600_000 == 0`, whereas floor division would
/// give `-3_600_000`). This is the single frozen definition both the
/// writer's registration gate (issue #26) and the reader's rendered
/// historical-bound SQL (`intDiv({data_start}, {bucket_ms}) * {bucket_ms}`,
/// issue #30) must call, so the AC's cross-crate identity holds by
/// construction rather than by convention.
///
/// `bucket_ms` must be `>= 1` (config-validated by
/// `pulsus_config::validate`, mirroring its other positive-value guards);
/// violated only by a programming error, never by untrusted input, hence a
/// `debug_assert!` rather than a `Result`.
pub fn floor_to_activity_bucket(unix_milli: i64, bucket_ms: i64) -> i64 {
    debug_assert!(bucket_ms >= 1, "bucket_ms must be >= 1");
    (unix_milli / bucket_ms) * bucket_ms
}

/// Nanoseconds per day, used to floor a [`UnixNano`]-scale timestamp down to
/// a whole day before civil-calendar conversion.
const NANOS_PER_DAY: i64 = 86_400_000_000_000;

/// Milliseconds per day, used to floor a [`UnixMilli`]-scale timestamp down
/// to a whole day for [`Date::start_of_day_utc_ms`] — metric samples are
/// stored at millisecond resolution (docs/architecture.md §2), so this
/// avoids re-deriving the day from nanoseconds.
const MILLIS_PER_DAY: i64 = 86_400_000;

/// Days since the Unix epoch (1970-01-01), used for ClickHouse `Date`
/// columns — currently `log_streams.month` (docs/schemas.md §3.1,
/// `toStartOfMonth(...)`, issue #8 plan amendment). Represented as a bare
/// `u16` (not, say, an `i32` day count) so it matches ClickHouse's native
/// `Date` wire encoding 1:1 — days since epoch, valid up to `2149-06-06`
/// (`u16::MAX`) — with zero conversion overhead at the insert boundary,
/// same rationale as [`Fingerprint`]'s bare `u64`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date(u16);

impl Date {
    /// The UTC start-of-month (`toStartOfMonth`, docs/schemas.md §3.1)
    /// containing `timestamp_ns` (nanoseconds since the Unix epoch, as
    /// resolved for a `log_samples` row — issue #8 plan amendment: this
    /// must be the same per-record timestamp `log_samples` uses, so a
    /// backfilled record registers its historical month, not `now_ns`).
    ///
    /// `timestamp_ns` is floored to a whole UTC day before the civil-
    /// calendar conversion, so sub-day precision never affects the result.
    /// Returns `None` when the resulting month-start day falls outside the
    /// `u16` `Date` range (before 1970-01-01 or after 2149-06-06):
    /// clamping such a value to the nearest in-range end would silently
    /// orphan the sample into the wrong (max-`Date`) monthly partition,
    /// breaking the `(fingerprint, sample month)` registration invariant
    /// (docs/schemas.md §3.1). The caller must reject the record instead —
    /// pathological or malicious input must never crash the parser (no
    /// `.unwrap()` on untrusted data) and must never be silently saturated.
    pub fn start_of_month_utc(timestamp_ns: i64) -> Option<Date> {
        let day = timestamp_ns.div_euclid(NANOS_PER_DAY);
        let (year, month, _day_of_month) = civil_from_days(day);
        let month_start_day = days_from_civil(year, month, 1);
        u16::try_from(month_start_day).ok().map(Date)
    }

    /// The UTC start-of-day containing `timestamp_ns` — the per-**day**
    /// floor `trace_attrs_idx.date` needs (docs/schemas.md §4.1:
    /// `PARTITION BY date` is daily, unlike `log_streams.month`'s monthly
    /// [`Date::start_of_month_utc`] floor; issue #54 task-manager
    /// adjudication #1). Same representability contract as
    /// [`Date::start_of_month_utc`]: returns `None` when the day count
    /// falls outside the `u16` `Date` range (before 1970-01-01 or after
    /// 2149-06-06) rather than clamping it, so the caller rejects the record
    /// instead of orphaning it into the wrong daily partition.
    pub fn start_of_day_utc(timestamp_ns: i64) -> Option<Date> {
        let day = timestamp_ns.div_euclid(NANOS_PER_DAY);
        u16::try_from(day).ok().map(Date)
    }

    /// The UTC start-of-day containing `unix_milli` — the millisecond-scale
    /// sibling of [`Date::start_of_day_utc`] for `metric_samples` /
    /// `metric_hist_samples`, which partition on
    /// `toDate(fromUnixTimestamp64Milli(unix_milli))` (docs/schemas.md,
    /// issue #126). Deliberately NOT `start_of_day_utc(unix_milli *
    /// 1_000_000)`: that multiply overflows `i64` for `unix_milli` beyond
    /// roughly year 2262, while flooring directly at millisecond scale is
    /// overflow-free for the full `i64` range. Same representability
    /// contract as [`Date::start_of_day_utc`]: returns `None` when the day
    /// falls outside the `u16` `Date` range (before 1970-01-01 or after
    /// 2149-06-06) rather than clamping it, so the caller rejects the
    /// sample instead of orphaning it into the wrong daily partition.
    pub fn start_of_day_utc_ms(unix_milli: i64) -> Option<Date> {
        let day = unix_milli.div_euclid(MILLIS_PER_DAY);
        u16::try_from(day).ok().map(Date)
    }

    /// Days since the Unix epoch — the exact value ClickHouse's `Date`
    /// column stores on the wire.
    pub fn days_since_epoch(&self) -> u16 {
        self.0
    }

    /// Last UTC day fully inside ClickHouse's 32-bit `DateTime` domain
    /// (u32 seconds since the epoch, max `4_294_967_295` =
    /// 2106-02-07T06:28:15Z). Day `49_710` (2106-02-07) is only partially
    /// representable — its final second `49_711 * 86_400 - 1` exceeds
    /// `u32::MAX` — so the last day whose every second fits is `49_709`
    /// (2106-02-06, final second `49_710 * 86_400 - 1 = 4_294_943_999`).
    pub const LAST_DATETIME_SAFE_DAY: u16 = 49_709;

    /// [`Date::start_of_day_utc`], additionally rejecting days past
    /// [`Date::LAST_DATETIME_SAFE_DAY`] — for tables whose delete-TTL
    /// evaluates the row timestamp in the 32-bit `DateTime` domain
    /// (`trace_spans` / `trace_attrs_idx`, docs/schemas.md §4.1, issue
    /// #131): a day in `49_710..=65_535` partitions correctly (inside the
    /// `Date` range) but its TTL seconds value exceeds `u32::MAX`, so the
    /// caller must reject the record rather than store it with a
    /// wrap-prone timestamp. Full-day granularity is deliberate — the gate
    /// stays a pure day comparison; the forfeited 6h28m15s of 2106-02-07
    /// admissibility is immaterial (issue #131 plan).
    pub fn start_of_day_utc_datetime_safe(timestamp_ns: i64) -> Option<Date> {
        Date::start_of_day_utc(timestamp_ns).filter(|d| d.0 <= Date::LAST_DATETIME_SAFE_DAY)
    }

    /// [`Date::start_of_day_utc_ms`], additionally rejecting days past
    /// [`Date::LAST_DATETIME_SAFE_DAY`] — the millisecond sibling of
    /// [`Date::start_of_day_utc_datetime_safe`] (issue #137; #131
    /// precedent) for `metric_samples` / `metric_hist_samples`, whose
    /// delete-TTL evaluates `intDiv(unix_milli, 1000)` in the 32-bit
    /// `DateTime` domain: a day in `49_710..=65_535` partitions correctly
    /// (inside the `Date` range) but its TTL seconds value exceeds
    /// `u32::MAX`, so the caller must reject the sample rather than store
    /// it with a wrap-prone timestamp. Same full-day granularity rationale
    /// as the nanosecond variant.
    pub fn start_of_day_utc_ms_datetime_safe(unix_milli: i64) -> Option<Date> {
        Date::start_of_day_utc_ms(unix_milli).filter(|d| d.0 <= Date::LAST_DATETIME_SAFE_DAY)
    }
}

/// Converts a day count since the Unix epoch (1970-01-01) to a proleptic
/// Gregorian `(year, month, day)` triple, `month` and `day` both 1-based.
/// Howard Hinnant's public-domain civil-calendar algorithm
/// (<http://howardhinnant.github.io/date_algorithms.html#civil_from_days>),
/// correct for the full `i64` day range (including days before the epoch).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// The inverse of [`civil_from_days`]: a proleptic Gregorian `(year,
/// month, day)` triple (`month`/`day` 1-based) to a day count since the
/// Unix epoch. Same source algorithm
/// (<http://howardhinnant.github.io/date_algorithms.html#days_from_civil>).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = if m > 2 { m - 3 } else { m + 9 } as i64; // [0, 11]
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

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

    #[test]
    fn civil_from_days_round_trips_days_from_civil_across_a_wide_range() {
        // Epoch, ordinary/leap-year boundaries, and pre-epoch days — the
        // full set exercised by the Python cross-check used to validate
        // this Rust port before it was written.
        for days in [
            0i64, 1, -1, 30, 31, 365, 366, -365, -366, 19_800, 20_000, -700_000, 65_535, 65_536,
        ] {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days, "day {days}");
        }
    }

    #[test]
    fn civil_from_days_of_epoch_is_1970_01_01() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
    }

    #[test]
    fn days_from_civil_of_1970_01_01_is_epoch() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
    }

    #[test]
    fn start_of_month_utc_floors_a_mid_month_timestamp_to_the_first() {
        // 2024-03-15T12:34:56Z, arbitrary nanosecond precision.
        let ts_ns = 1_710_505_996_123_456_789;
        let month = Date::start_of_month_utc(ts_ns).unwrap();
        assert_eq!(
            civil_from_days(i64::from(month.days_since_epoch())),
            (2024, 3, 1)
        );
    }

    #[test]
    fn start_of_month_utc_of_the_epoch_instant_is_1970_01() {
        let month = Date::start_of_month_utc(0).unwrap();
        assert_eq!(month.days_since_epoch(), 0);
    }

    #[test]
    fn start_of_month_utc_rejects_a_pre_epoch_timestamp() {
        // 1969-06-15, well before the epoch: ClickHouse's `Date` column
        // (and this `u16` encoding) cannot represent a pre-1970 day.
        // Clamping to day 0 would orphan the sample into the wrong monthly
        // partition, so the function returns `None` and the caller rejects
        // the record rather than silently saturating.
        let ts_ns = -(NANOS_PER_DAY * 200);
        assert_eq!(Date::start_of_month_utc(ts_ns), None);
    }

    #[test]
    fn start_of_month_utc_two_timestamps_in_the_same_month_yield_equal_dates() {
        let start = Date::start_of_month_utc(1_710_000_000_000_000_000).unwrap();
        let end = Date::start_of_month_utc(1_710_999_999_000_000_000).unwrap();
        assert_eq!(start, end);
    }

    #[test]
    fn start_of_month_utc_across_a_month_boundary_differs() {
        // 2024-02-29T23:00Z (leap day) vs 2024-03-01T01:00Z.
        let feb = Date::start_of_month_utc(1_709_247_600_000_000_000).unwrap();
        let mar = Date::start_of_month_utc(1_709_262_000_000_000_000).unwrap();
        assert_ne!(feb, mar);
        assert_eq!(
            civil_from_days(i64::from(feb.days_since_epoch())),
            (2024, 2, 1)
        );
        assert_eq!(
            civil_from_days(i64::from(mar.days_since_epoch())),
            (2024, 3, 1)
        );
    }

    #[test]
    fn start_of_month_utc_rejects_out_of_range_timestamps_instead_of_saturating() {
        // Far beyond the u16 day range (year ~2149 cutoff) in both
        // directions: must return `None`, never panic and never saturate to
        // the max-`Date` partition (which would silently orphan the sample).
        assert_eq!(Date::start_of_month_utc(i64::MAX), None);
        assert_eq!(Date::start_of_month_utc(i64::MIN), None);
        // A concrete post-2106 / far-future instant well inside the i64 ns
        // range but past the 2149-06-06 `Date` cutoff (~year 2200): this is
        // the data-integrity case #8 flagged — it previously saturated to
        // day 65535, now it is rejectable.
        let year_2200_ns = NANOS_PER_DAY * 84_000;
        assert_eq!(Date::start_of_month_utc(year_2200_ns), None);
    }

    #[test]
    fn start_of_day_utc_floors_a_mid_day_timestamp_to_that_day() {
        // 2024-03-15T12:34:56Z — same instant `start_of_month_utc`'s
        // mid-month test uses, so the two floors are directly comparable.
        let ts_ns = 1_710_505_996_123_456_789;
        let day = Date::start_of_day_utc(ts_ns).unwrap();
        assert_eq!(
            civil_from_days(i64::from(day.days_since_epoch())),
            (2024, 3, 15)
        );
    }

    #[test]
    fn start_of_day_utc_two_timestamps_in_the_same_day_yield_equal_dates() {
        // 2024-03-15T00:00:00Z and 2024-03-15T23:59:59.999999999Z.
        let start = Date::start_of_day_utc(1_710_460_800_000_000_000).unwrap();
        let end = Date::start_of_day_utc(1_710_547_199_999_999_999).unwrap();
        assert_eq!(start, end);
    }

    #[test]
    fn start_of_day_utc_across_a_day_boundary_differs() {
        // 2024-03-15T23:59:59Z vs 2024-03-16T00:00:00Z.
        let before = Date::start_of_day_utc(1_710_547_199_000_000_000).unwrap();
        let after = Date::start_of_day_utc(1_710_547_200_000_000_000).unwrap();
        assert_ne!(before, after);
        assert_eq!(
            after.days_since_epoch(),
            before.days_since_epoch() + 1,
            "consecutive days differ by exactly one epoch-day"
        );
    }

    #[test]
    fn start_of_day_utc_of_the_epoch_instant_is_day_zero() {
        assert_eq!(Date::start_of_day_utc(0).unwrap().days_since_epoch(), 0);
    }

    #[test]
    fn start_of_day_utc_rejects_out_of_range_timestamps_instead_of_saturating() {
        // Both directions past the u16 day range must return `None`, never
        // panic and never saturate.
        assert_eq!(Date::start_of_day_utc(i64::MAX), None);
        assert_eq!(Date::start_of_day_utc(i64::MIN), None);
        // Pre-epoch but in the representable i64 range: still rejected, not
        // clamped to day 0.
        assert_eq!(Date::start_of_day_utc(-(NANOS_PER_DAY * 200)), None);
        // A concrete far-future (~year 2200) instant past the 2149-06-06
        // `Date` cutoff: previously saturated to day 65535, now rejectable.
        assert_eq!(Date::start_of_day_utc(NANOS_PER_DAY * 84_000), None);
    }

    #[test]
    fn start_of_day_utc_datetime_safe_accepts_the_last_datetime_safe_day() {
        // The last nanosecond of day 49_709 (2106-02-06): every second of
        // this day fits u32 (final second 4_294_943_999 <= u32::MAX).
        let last_safe_ns = NANOS_PER_DAY * 49_710 - 1;
        assert_eq!(
            Date::start_of_day_utc_datetime_safe(last_safe_ns)
                .unwrap()
                .days_since_epoch(),
            49_709
        );
        assert_eq!(Date::LAST_DATETIME_SAFE_DAY, 49_709);
    }

    #[test]
    fn start_of_day_utc_datetime_safe_rejects_the_first_datetime_unsafe_day() {
        // The first nanosecond of day 49_710 (2106-02-07): the day is still
        // inside the u16 `Date` range (start_of_day_utc accepts it) but its
        // final second exceeds u32::MAX, so the DateTime-safe gate rejects.
        let first_unsafe_ns = NANOS_PER_DAY * 49_710;
        assert_eq!(Date::start_of_day_utc_datetime_safe(first_unsafe_ns), None);
        assert_eq!(
            Date::start_of_day_utc(first_unsafe_ns)
                .unwrap()
                .days_since_epoch(),
            49_710,
            "sanity: the plain Date gate alone would have admitted this day"
        );
    }

    #[test]
    fn start_of_day_utc_datetime_safe_rejects_negative_and_i64_extremes() {
        assert_eq!(
            Date::start_of_day_utc_datetime_safe(-(NANOS_PER_DAY * 200)),
            None
        );
        assert_eq!(Date::start_of_day_utc_datetime_safe(-1), None);
        assert_eq!(Date::start_of_day_utc_datetime_safe(i64::MIN), None);
        assert_eq!(Date::start_of_day_utc_datetime_safe(i64::MAX), None);
    }

    #[test]
    fn start_of_day_utc_datetime_safe_agrees_with_start_of_day_utc_inside_the_safe_range() {
        // 2024-03-15T12:34:56Z — well inside the safe range: the two gates
        // must resolve the identical day.
        let ts_ns = 1_710_505_996_123_456_789;
        assert_eq!(
            Date::start_of_day_utc_datetime_safe(ts_ns).unwrap(),
            Date::start_of_day_utc(ts_ns).unwrap()
        );
    }

    #[test]
    fn start_of_day_utc_ms_of_the_epoch_instant_is_day_zero() {
        assert_eq!(Date::start_of_day_utc_ms(0).unwrap().days_since_epoch(), 0);
    }

    #[test]
    fn start_of_day_utc_ms_accepts_the_last_representable_day() {
        // Day 65535 = 2149-06-06, the last day `Date` can represent; the
        // last millisecond within it is 65536 * MILLIS_PER_DAY - 1.
        let last_ms = 65_536 * MILLIS_PER_DAY - 1;
        assert_eq!(last_ms, 5_662_310_399_999);
        assert_eq!(
            Date::start_of_day_utc_ms(last_ms)
                .unwrap()
                .days_since_epoch(),
            65_535
        );
    }

    #[test]
    fn start_of_day_utc_ms_rejects_the_first_unrepresentable_day() {
        // Day 65536 = 2149-06-07, the first day past the `u16` range.
        let first_bad_ms = 65_536 * MILLIS_PER_DAY;
        assert_eq!(first_bad_ms, 5_662_310_400_000);
        assert_eq!(Date::start_of_day_utc_ms(first_bad_ms), None);
    }

    #[test]
    fn start_of_day_utc_ms_rejects_a_negative_timestamp() {
        assert_eq!(Date::start_of_day_utc_ms(-1), None);
    }

    #[test]
    fn start_of_day_utc_ms_rejects_i64_extremes_instead_of_saturating() {
        assert_eq!(Date::start_of_day_utc_ms(i64::MAX), None);
        assert_eq!(Date::start_of_day_utc_ms(i64::MIN), None);
    }

    #[test]
    fn start_of_day_utc_ms_agrees_with_start_of_day_utc_on_an_in_range_instant() {
        // 2024-03-15T12:34:56.123Z, expressed at both ms and ns scale: the
        // two helpers must floor to the same day.
        let ms = 1_710_505_996_123;
        let ns = ms * 1_000_000;
        assert_eq!(
            Date::start_of_day_utc_ms(ms).unwrap(),
            Date::start_of_day_utc(ns).unwrap()
        );
    }

    #[test]
    fn start_of_day_utc_ms_datetime_safe_accepts_the_last_datetime_safe_day() {
        // The last millisecond of day 49_709 (2106-02-06), the last UTC day
        // fully inside the 32-bit DateTime domain (issue #137).
        let last_safe_ms = 49_710 * MILLIS_PER_DAY - 1;
        assert_eq!(last_safe_ms, 4_294_943_999_999);
        assert_eq!(
            Date::start_of_day_utc_ms_datetime_safe(last_safe_ms)
                .unwrap()
                .days_since_epoch(),
            Date::LAST_DATETIME_SAFE_DAY
        );
    }

    #[test]
    fn start_of_day_utc_ms_datetime_safe_rejects_the_first_datetime_unsafe_day() {
        // The first millisecond of day 49_710 (2106-02-07): still inside
        // the u16 `Date` range (start_of_day_utc_ms accepts it) but its
        // TTL seconds value exceeds u32::MAX partway through the day.
        let first_unsafe_ms = 49_710 * MILLIS_PER_DAY;
        assert_eq!(
            Date::start_of_day_utc_ms_datetime_safe(first_unsafe_ms),
            None
        );
        assert_eq!(
            Date::start_of_day_utc_ms(first_unsafe_ms)
                .unwrap()
                .days_since_epoch(),
            49_710,
            "the plain Date-range helper must still accept it — only the DateTime-safe gate rejects"
        );
    }

    #[test]
    fn start_of_day_utc_ms_datetime_safe_rejects_negative_and_i64_extremes() {
        assert_eq!(
            Date::start_of_day_utc_ms_datetime_safe(-MILLIS_PER_DAY * 200),
            None
        );
        assert_eq!(Date::start_of_day_utc_ms_datetime_safe(-1), None);
        assert_eq!(Date::start_of_day_utc_ms_datetime_safe(i64::MIN), None);
        assert_eq!(Date::start_of_day_utc_ms_datetime_safe(i64::MAX), None);
    }

    #[test]
    fn start_of_day_utc_ms_datetime_safe_agrees_with_the_ns_sibling_on_an_in_range_instant() {
        // 2024-03-15T12:34:56.123Z at both scales: same day out of both
        // DateTime-safe helpers.
        let ms = 1_710_505_996_123;
        let ns = ms * 1_000_000;
        assert_eq!(
            Date::start_of_day_utc_ms_datetime_safe(ms).unwrap(),
            Date::start_of_day_utc_datetime_safe(ns).unwrap()
        );
    }

    #[test]
    fn default_activity_bucket_ms_is_one_hour() {
        assert_eq!(DEFAULT_ACTIVITY_BUCKET_MS, 3_600_000);
    }

    #[test]
    fn floor_to_activity_bucket_floors_a_mid_bucket_timestamp_down() {
        assert_eq!(floor_to_activity_bucket(3_600_001, 3_600_000), 3_600_000);
        assert_eq!(floor_to_activity_bucket(7_199_999, 3_600_000), 3_600_000);
    }

    #[test]
    fn floor_to_activity_bucket_of_an_exact_bucket_boundary_is_a_no_op() {
        assert_eq!(floor_to_activity_bucket(3_600_000, 3_600_000), 3_600_000);
        assert_eq!(floor_to_activity_bucket(0, 3_600_000), 0);
    }

    /// Golden test (architect plan amendment 3, closing a review test gap):
    /// truncating (toward-zero) division must match ClickHouse's `intDiv`
    /// for negative inputs — `div_euclid` (floor division) would give a
    /// different, wrong answer here. `intDiv(-1, 3_600_000) * 3_600_000 ==
    /// 0`, not `-3_600_000`.
    #[test]
    fn floor_to_activity_bucket_negative_timestamp_matches_clickhouse_intdiv_truncation() {
        assert_eq!(floor_to_activity_bucket(-1, 3_600_000), 0);
        assert_ne!(
            floor_to_activity_bucket(-1, 3_600_000),
            (-1i64).div_euclid(3_600_000) * 3_600_000,
            "truncating division must diverge from floor division here"
        );

        // A negative timestamp whose magnitude exceeds one bucket:
        // intDiv(-3_600_001, 3_600_000) == -1 (truncated toward zero), so
        // the floored result is -3_600_000, not the floor-division answer
        // of -7_200_000.
        assert_eq!(floor_to_activity_bucket(-3_600_001, 3_600_000), -3_600_000);
        assert_eq!(
            (-3_600_001i64).div_euclid(3_600_000) * 3_600_000,
            -7_200_000,
            "sanity check on the diverging floor-division answer"
        );
    }

    #[test]
    fn floor_to_activity_bucket_a_negative_exact_multiple_is_unchanged() {
        assert_eq!(floor_to_activity_bucket(-3_600_000, 3_600_000), -3_600_000);
    }
}
