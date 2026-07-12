//! Streaming fetch scenario: the narrow §2.3 hot-path projection
//! `(fingerprint, unix_milli, value)` read out of the full `metric_samples`
//! shape (not a pre-narrowed three-column table), confirming the crate
//! streams rather than materializes the whole result (issue #3 amendment).

use std::time::Instant;

use super::{CrateUnderTest, Stats, stats};

#[derive(Clone, Debug, serde::Serialize)]
pub struct FetchReport {
    pub crate_name: &'static str,
    pub rows: u64,
    pub stats: Stats,
    pub rows_per_sec_p50: f64,
    pub checksum: u64,
    /// Peak resident set size of this process (KiB) at the time this
    /// scenario finished, read from `/proc/self/status` `VmHWM` (Linux
    /// only). Meaningful when the scenario is run in isolation
    /// (`xtask ch-bench --scenario fetch`), since it is a process-lifetime
    /// high-water mark, not a scenario-scoped delta.
    pub peak_rss_kib: Option<u64>,
}

/// Reads `VmHWM` (peak resident set size) from `/proc/self/status`. Returns
/// `None` on non-Linux or if the file cannot be parsed.
pub fn peak_rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            let digits: String = rest.chars().filter(|c| c.is_ascii_digit()).collect();
            return digits.parse().ok();
        }
    }
    None
}

pub async fn bench_metric_fetch<C: CrateUnderTest>(
    c: &C,
    table: &str,
    metric_name: &str,
    reps: usize,
) -> anyhow::Result<FetchReport> {
    let mut durations = Vec::with_capacity(reps);
    let mut rows_seen = 0u64;
    let mut checksum = 0u64;
    for _ in 0..reps {
        let start = Instant::now();
        let (rows, cksum) = c.fetch_metric_projection(table, metric_name).await?;
        durations.push(start.elapsed());
        rows_seen = rows;
        checksum = cksum;
    }
    let st = stats(&durations);
    Ok(FetchReport {
        crate_name: c.name(),
        rows: rows_seen,
        rows_per_sec_p50: rows_seen as f64 / (st.p50_ms / 1000.0),
        stats: st,
        checksum,
        peak_rss_kib: peak_rss_kib(),
    })
}
