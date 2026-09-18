//! Row generators for the benchmark scenarios. Every shape mirrors the
//! authoritative DDL in docs/schemas.md so the benchmark measures the real
//! write/read path, not a narrowed synthetic tuple (issue #3 Codex finding 1).

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// `metric_samples` row (docs/schemas.md §2.1).
#[derive(Clone, Debug, PartialEq)]
pub struct MetricRow {
    pub metric_name: String,
    pub fingerprint: u128,
    pub unix_milli: i64,
    pub value: f64,
}

impl MetricRow {
    /// Uncompressed payload size in bytes (string bytes + fixed-width
    /// columns): `fingerprint` is 16 bytes since issue #498, then
    /// `unix_milli` and `value` at 8 each.
    pub fn payload_bytes(&self) -> usize {
        self.metric_name.len() + 16 + 8 + 8
    }
}

/// `log_samples` row (docs/schemas.md §3.1).
#[derive(Clone, Debug, PartialEq)]
pub struct LogRow {
    pub service: String,
    pub fingerprint: u128,
    pub timestamp_ns: i64,
    pub severity: i8,
    pub body: String,
}

impl LogRow {
    /// `fingerprint` is 16 bytes since issue #498, `timestamp_ns` 8 and
    /// `severity` 1, plus the two string columns' own bytes.
    pub fn payload_bytes(&self) -> usize {
        self.service.len() + 16 + 8 + 1 + self.body.len()
    }
}

/// A decoded row from the `metric_samples_5m` tier (docs/schemas.md §2.2/§2.3).
#[derive(Clone, Debug, PartialEq)]
pub struct AggRow {
    pub fingerprint: u128,
    pub val_count: u64,
    pub first_value: f64,
    pub last_value: f64,
}

/// Realistic `LowCardinality` cardinality for metric names, per the architect
/// amendment (~500 distinct values — "not 1 and not N").
pub const METRIC_NAME_CARDINALITY: u64 = 500;
/// Realistic `LowCardinality` cardinality for log services (~50 distinct values).
pub const SERVICE_CARDINALITY: u64 = 50;

/// A `fingerprint` value with the top bit of **both** 64-bit words set, so
/// the unsigned round-trip gate (docs/schemas.md §2.2, `UInt128`
/// fingerprints since issue #498) is always exercised and a signed read of
/// either word would come back negative.
///
/// The two words differ, so a transport that swapped them would be visible
/// rather than symmetric.
pub const HIGH_BIT_FINGERPRINT: u128 =
    ((0xFFFF_FFFF_FFFF_FFF1u64 as u128) << 64) | (0xFFFF_FFFF_FFFF_FFF2u64 as u128);

/// A cheap, deterministic 64-bit mix (splitmix64) used to derive fingerprints
/// from small indices without pulling in a hashing dependency.
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

/// Generates `n` metric-shaped rows starting at global index `start`, deterministic
/// given the seed. Row `start == 0` is forced to carry [`HIGH_BIT_FINGERPRINT`] so
/// every rep's first block exercises the unsigned-fingerprint round-trip gate.
pub fn gen_metric_rows(n: u64, start: u64, seed: u64) -> Vec<MetricRow> {
    let mut rng = StdRng::seed_from_u64(seed ^ start);
    let base_ts: i64 = 1_700_000_000_000;
    let mut out = Vec::with_capacity(n as usize);
    for i in 0..n {
        let idx = start + i;
        let metric_idx = idx % METRIC_NAME_CARDINALITY;
        let series_idx = (idx / METRIC_NAME_CARDINALITY) % 20_000;
        // Both 64-bit words are generated, from two independent draws of
        // the mix: the production fingerprint is a `cityHash64` half above
        // an `xxHash64` half, so a benchmark that left one word zero would
        // measure a narrower value than the schema carries.
        let fingerprint = if idx == 0 {
            HIGH_BIT_FINGERPRINT
        } else {
            let seed = metric_idx.wrapping_mul(1_000_003).wrapping_add(series_idx);
            ((splitmix64(seed) as u128) << 64) | (splitmix64(seed ^ 0x5851_F42D_4C95_7F2D) as u128)
        };
        let jitter: i64 = rng.gen_range(-2..=2);
        let unix_milli = base_ts + (idx as i64) * 10 + jitter;
        // Gorilla-friendly: slowly varying value, not white noise.
        let value = 50.0 + ((idx % 3600) as f64) * 0.05 + rng.gen_range(-0.01..0.01);
        out.push(MetricRow {
            metric_name: format!("bench_metric_{metric_idx:04}"),
            fingerprint,
            unix_milli,
            value,
        });
    }
    out
}

/// Generates `n` log-shaped rows starting at global index `start`.
pub fn gen_log_rows(n: u64, start: u64, seed: u64) -> Vec<LogRow> {
    let mut rng = StdRng::seed_from_u64(seed ^ start ^ 0xA5A5_A5A5);
    let base_ts_ns: i64 = 1_700_000_000_000_000_000;
    let padding = "x".repeat(140);
    let mut out = Vec::with_capacity(n as usize);
    for i in 0..n {
        let idx = start + i;
        let service_idx = idx % SERVICE_CARDINALITY;
        let fingerprint = if idx == 0 {
            HIGH_BIT_FINGERPRINT
        } else {
            let seed = service_idx
                .wrapping_mul(2_654_435_761)
                .wrapping_add(idx / SERVICE_CARDINALITY);
            ((splitmix64(seed) as u128) << 64) | (splitmix64(seed ^ 0x5851_F42D_4C95_7F2D) as u128)
        };
        let jitter: i64 = rng.gen_range(0..1_000);
        let timestamp_ns = base_ts_ns + (idx as i64) * 1_000 + jitter;
        let severity = ((idx % 24) + 1) as i8; // OTel SeverityNumber 1..24
        let body = format!(
            "level={severity} service=bench-{service_idx:02} idx={idx} connection established {padding}"
        );
        out.push(LogRow {
            service: format!("bench-service-{service_idx:02}"),
            fingerprint,
            timestamp_ns,
            severity,
            body,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_rows_first_row_has_high_bit_fingerprint() {
        let rows = gen_metric_rows(10, 0, 42);
        assert_eq!(rows[0].fingerprint, HIGH_BIT_FINGERPRINT);
        // Both words carry their top bit, so a signed read of either one
        // comes back negative and the round-trip gate covers the whole
        // 128-bit width rather than its low half.
        const { assert!(HIGH_BIT_FINGERPRINT > (1u128 << 127)) };
        const { assert!((HIGH_BIT_FINGERPRINT as u64) > (1u64 << 63)) };
    }

    #[test]
    fn log_rows_first_row_has_high_bit_fingerprint() {
        let rows = gen_log_rows(10, 0, 42);
        assert_eq!(rows[0].fingerprint, HIGH_BIT_FINGERPRINT);
    }

    /// **Every generated fingerprint fills both 64-bit words.** A
    /// generator that left the high word zero would measure a value the
    /// schema does not carry: the production fingerprint is a
    /// `cityHash64` half above an `xxHash64` half, and a benchmark whose
    /// rows all fit in 64 bits reports the compression and transport cost
    /// of a narrower column.
    #[test]
    fn generated_fingerprints_fill_both_words() {
        for (name, fps) in [
            (
                "metric",
                gen_metric_rows(2_000, 0, 7)
                    .iter()
                    .map(|r| r.fingerprint)
                    .collect::<Vec<u128>>(),
            ),
            (
                "log",
                gen_log_rows(2_000, 0, 7)
                    .iter()
                    .map(|r| r.fingerprint)
                    .collect::<Vec<u128>>(),
            ),
        ] {
            let zero_high = fps.iter().filter(|fp| (**fp >> 64) == 0).count();
            let zero_low = fps.iter().filter(|fp| (**fp as u64) == 0).count();
            assert_eq!(
                (zero_high, zero_low),
                (0, 0),
                "{name}: {zero_high} rows have an empty high word and {zero_low} an empty low word"
            );
            let distinct: std::collections::HashSet<u128> = fps.iter().copied().collect();
            assert!(
                distinct.len() > fps.len() / 2,
                "{name}: only {} distinct fingerprints over {} rows",
                distinct.len(),
                fps.len()
            );
        }
    }

    #[test]
    fn metric_rows_use_bounded_low_cardinality() {
        let rows = gen_metric_rows(5_000, 0, 1);
        let distinct: std::collections::HashSet<_> = rows.iter().map(|r| &r.metric_name).collect();
        assert_eq!(distinct.len() as u64, METRIC_NAME_CARDINALITY);
    }

    #[test]
    fn log_rows_use_bounded_low_cardinality() {
        let rows = gen_log_rows(5_000, 0, 1);
        let distinct: std::collections::HashSet<_> = rows.iter().map(|r| &r.service).collect();
        assert_eq!(distinct.len() as u64, SERVICE_CARDINALITY);
    }
}
