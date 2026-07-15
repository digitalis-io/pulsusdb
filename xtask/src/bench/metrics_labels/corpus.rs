//! Deterministic `metric_series` corpus generator for `cargo xtask bench
//! metrics-labels` (issue #34). Same splitmix64-adjacent determinism
//! discipline as `super::super::dataset` (issue #16): no `rand`, so a
//! committed CI baseline stays byte-reproducible across `rand`
//! major-version bumps — but this generator needs no per-row randomness at
//! all (every series' labels are a deterministic function of its ordinal
//! `i`, per the architect plan's controlled-selectivity scheme below), so
//! there is no PRNG to seed here; `spec.seed` is carried for interface
//! parity with `dataset.rs` and reserved for a future randomized dimension,
//! not read by this module.
//!
//! **Labels** use `pulsus-model`'s frozen `LabelSet::from_verbatim`/
//! `metric_fingerprint` — the same canonicalization/fingerprint primitives
//! the product writer and `pulsus_read::metrics` agree on. **DDL** is the
//! product's own (`pulsus_schema::run_init`, run by the caller before
//! [`load`]). **Bulk load** is direct RowBinary insert
//! (`ChClient::insert_block`), the same shortcut `dataset.rs` uses,
//! licensed by `crates/pulsus-write/tests/ingest_fidelity.rs` (issue #16
//! architect plan) — not re-proven here.
//!
//! **Per-series label scheme (architect plan Interfaces), series `i` of a
//! tier at cardinality `C`:**
//! - `job = "j{i % 8}"` — broad eq, ~C/8 matching series — **omitted
//!   entirely** for series where `i % 10 == 0` (see "Absent-label series"
//!   below).
//! - `region = "r{(i/8) % 3}"`, `env = "e{(i/24) % 3}"` — not directly
//!   selected on by any benchmarked [`super::paths::SelectorKind`]; present
//!   so the label set has realistic width (label-count, not just
//!   selectivity, affects `metric_series_idx`'s ARRAY JOIN row count).
//! - `status = ["200","404","500","503"][i % 4]` — regex `"5.."` matches
//!   `500`/`503`, ~C/2 series — **omitted entirely** for series where
//!   `i % 13 == 0`.
//! - `pod = "pod-{i}"` — unique per series, always present; `pod="pod-0"`
//!   is the narrow (single-series) selector, always present since every
//!   tier has at least series `0`.
//!
//! No `__name__` label — the metric name is the separate `metric_name`
//! column (docs/schemas.md §2.1), matching [`pulsus_model::metric_fingerprint`]'s
//! own exclusion of it.
//!
//! **Absent-label series (issue #34 CODE review [medium] finding).** A
//! deterministic ~1/10 of series omit `job`, and a deterministic ~1/13
//! omit `status` — not a corpus-generation nicety, but load-bearing for the
//! cross-path correctness gate: Prometheus (and this product's own
//! matching semantics, `pulsus_read::metrics::labels::matches` /
//! `JSONExtractString`'s `''`-on-absent contract) treats an absent label as
//! `""`, so `NegBroad` (`job!="j0"`) **must** match a series carrying no
//! `job` label at all. Before this fix, every series carried every key, so
//! this case was never exercised — masking a real bug in the idx
//! prototype's resolution SQL (see `super::paths::idx_resolve_sql`'s doc
//! comment).
//!
//! **Activity staggering (load-bearing for the day-bucket over-inclusion
//! measurement).** Every series gets **exactly one** `metric_series` row —
//! not one row per bucket per series — in a bucket chosen by
//! [`assign_bucket`], which reserves `buckets[0]` as a **guard band** never
//! assigned to any series (issue #34 CODE review [high] finding — see
//! [`assign_bucket`]'s own doc comment for why). Series are never
//! *permanently* active in this generator; each is active in exactly one
//! bucket, deterministically staggered by its ordinal. This is deliberate,
//! not a simplification for its own sake: a full-window query (spanning
//! every bucket, as every §2.1 strategy-ladder path's correctness
//! comparison uses) still resolves every series identically regardless of
//! staggering — but a query *narrower* than the full window only finds the
//! series whose designated bucket falls inside it, which is exactly what
//! makes the day-bucket over-inclusion phenomenon (docs/schemas.md §2.1:
//! "a 10-minute historical query against a `1d` bucket drags that whole
//! day's series … through label matching") observable at all. An earlier
//! version of this generator gave every series a row in *every* bucket
//! (permanently active) — every series was therefore trivially "active" in
//! any window regardless of bucket size, and the over-inclusion probe
//! measured no effect (confirmed live: the `1d`/`1h` `read_rows` ratio came
//! out *below* 1, the opposite of the documented risk, purely because the
//! `1h` pass materialized 24× more total rows for the same series set).
//! Staggering fixes that confound while also shrinking the CI-scale corpus
//! by the same 24× factor.
//!
//! **Frozen reference instant (issue #34 CODE review [high] finding).**
//! `spec.ref_ms` is captured exactly **once** per bucket-size run, by the
//! caller (`metrics_labels::run`), and threaded through as the single
//! source for this corpus's bucket boundaries, the SQL-fallback/idx-
//! resolution bounds (`super::paths`), and the resolver's `DataWindow` —
//! never re-derived independently at any of those call sites. The one
//! clock reading this **cannot** control is
//! `pulsus_read::metrics::LabelCache::refresh()`'s own internal sweep,
//! which computes its own wall-clock "now" (product code, not injectable);
//! if wall-clock time elapses between this corpus's `ref_ms` and that later
//! refresh call, the cache's own `covered_from_ms` can land up to one
//! `bucket_ms` later than `ref_ms`-derived bounds would. The guard band
//! above is what actually neutralizes that drift (not `ref_ms` alone) —
//! together they make the three paths' resolved fingerprint sets identical
//! **by construction**, not by getting lucky on timing.

use std::time::Instant;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, QuerySettings, Row};
use pulsus_model::{LabelSet, floor_to_activity_bucket, metric_fingerprint};

use crate::bench::Profile;

/// Rows per bulk `INSERT` block — same rationale/value as `dataset.rs`'s
/// `INSERT_BATCH_ROWS`: large enough to amortize per-request overhead,
/// small enough to keep any one `insert_block` call's memory bounded
/// regardless of total corpus size.
const INSERT_BATCH_ROWS: usize = 100_000;

#[derive(Debug, Clone)]
pub struct MetricsCorpusSpec {
    pub profile: Profile,
    /// Reserved for a future randomized label dimension — this generator
    /// is currently fully deterministic in `i` alone (see module doc
    /// comment), so this field is not yet read.
    #[allow(dead_code)]
    pub seed: u64,
    /// Per-metric series cardinalities — one distinct metric
    /// (`metric_{cardinality}`) generated per entry.
    pub cardinalities: Vec<u64>,
    /// Activity-bucket size in milliseconds (`3_600_000` = 1h,
    /// `86_400_000` = 1d).
    pub bucket_ms: i64,
    /// Corpus window in milliseconds — `--corpus-window-hours * 3_600_000`.
    pub window_ms: i64,
    /// The frozen reference instant (milliseconds since the Unix epoch,
    /// module doc comment's "Frozen reference instant") — captured once by
    /// the caller, never derived internally by this module.
    pub ref_ms: i64,
    /// Load through the `_dist` Distributed wrapper (`metric_series_dist`)
    /// instead of the bare local table — required for `--dist` mode, same
    /// rationale as `dataset.rs::DatasetSpec::dist`.
    pub dist: bool,
}

/// One cardinality tier's identity — the fingerprint of series `0`
/// (`pod="pod-0"`, unique) is recorded so the narrow-selector path runners
/// don't need to re-derive it.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TierInfo {
    pub metric_name: String,
    pub cardinality: u64,
    /// Total distinct series in this tier — equal to `cardinality` (every
    /// ordinal `0..cardinality` gets exactly one series).
    pub series_rows: u64,
    /// `metric_fingerprint` of series `0` (`pod="pod-0"`) — the fixed
    /// narrow-selector target every path resolves against.
    pub narrow_fp: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetricsCorpusSummary {
    pub tiers: Vec<TierInfo>,
    pub total_series_rows: u64,
    pub bucket_ms: i64,
    pub window_ms: i64,
    pub end_ms: i64,
    pub load_elapsed_ms: u64,
}

/// Wire shape matching `metric_series`' physical column order exactly
/// (`pulsus_schema::catalog`'s migration id 4: `metric_name, fingerprint,
/// unix_milli, labels`) — RowBinary insert requires this order, same
/// convention as `dataset.rs`'s `SeedStreamRow`/`SeedSampleRow`.
#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct MetricSeriesRow {
    metric_name: String,
    fingerprint: u64,
    unix_milli: i64,
    labels: String,
}

/// Series `i`'s label set — see the module doc comment's controlled-
/// selectivity scheme and "Absent-label series" note. Deterministic in `i`
/// alone.
fn series_labels(i: u64) -> LabelSet {
    let region = format!("r{}", (i / 8) % 3);
    let env = format!("e{}", (i / 24) % 3);
    let pod = format!("pod-{i}");
    let mut pairs: Vec<(String, String)> = vec![
        ("region".to_string(), region),
        ("env".to_string(), env),
        ("pod".to_string(), pod),
    ];
    // Absent-label series (issue #34 CODE review [medium] finding): a
    // deterministic fraction of series omit `job` and/or `status`
    // entirely, so the cross-path correctness gate exercises Prometheus's
    // absent-label-as-`""` semantics, not just series carrying every key.
    if !i.is_multiple_of(10) {
        pairs.push(("job".to_string(), format!("j{}", i % 8)));
    }
    if !i.is_multiple_of(13) {
        let status = ["200", "404", "500", "503"][(i % 4) as usize];
        pairs.push(("status".to_string(), status.to_string()));
    }
    LabelSet::from_verbatim(pairs)
}

/// Every activity-bucket boundary in `[floor(ref_ms - window_ms), floor(ref_ms)]`,
/// ascending.
fn bucket_boundaries(ref_ms: i64, window_ms: i64, bucket_ms: i64) -> Vec<i64> {
    let start = floor_to_activity_bucket(ref_ms - window_ms, bucket_ms);
    let end = floor_to_activity_bucket(ref_ms, bucket_ms);
    let mut out = Vec::new();
    let mut b = start;
    while b <= end {
        out.push(b);
        b += bucket_ms;
    }
    out
}

/// A cheap 64-bit avalanche mix (splitmix64-style — matches `dataset.rs`'s
/// existing hand-rolled-PRNG convention, issue #16). Used **only** to
/// decorrelate [`assign_bucket`]'s bucket choice from `series_labels`' own
/// `i % 8`/`i % 10`/`i % 13` residues (found live: with the default 1h
/// bucket over a 24h window, `guard_span` is exactly 24 — a multiple of
/// `job`'s modulus 8 — so a raw `i % guard_span` bucket assignment put
/// *every* series in a given bucket at the *same* `i % 8`, i.e. the same
/// `job` value; the over-inclusion probe's `job="j0"` selector then matched
/// **zero** series in whichever bucket happened to be "current", not the
/// expected `~C / guard_span`). Mixing `i` first breaks that correlation
/// without changing anything else about the staggering scheme.
fn mix64(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Assigns series `i` to exactly one bucket, reserving `buckets[0]` as a
/// **guard band** never assigned to any series (issue #34 CODE review
/// [high] finding). `LabelCache::refresh()`'s internal sweep computes its
/// own wall-clock "now" (product code, not injectable): if measurable
/// wall-clock time elapses between this corpus's frozen `ref_ms` anchor and
/// that later refresh call, the cache's own `covered_from_ms` can land up
/// to one `bucket_ms` later than this corpus's `buckets[0]`. Reserving
/// `buckets[0]` as dead space means the corpus's true earliest active
/// bucket (`buckets[1]`) still falls at or after that drifted
/// `covered_from_ms` as long as the drift stays within one `bucket_ms` —
/// making the three paths' resolved fingerprint sets identical **by
/// construction**, not by getting lucky on timing. Requires
/// `buckets.len() >= 2` (checked by the caller, [`load`]). Indexes via
/// [`mix64`], not raw `i`, so bucket choice is decorrelated from label
/// values (see `mix64`'s own doc comment) — load-bearing for the
/// over-inclusion probe, not just cosmetic.
fn assign_bucket(i: u64, buckets: &[i64]) -> i64 {
    let guard_span = (buckets.len() - 1) as u64;
    buckets[1 + (mix64(i) % guard_span) as usize]
}

/// Polls `count_sql` until it reaches `expected` — the same
/// poll-until-visible, no-fixed-sleeps `_dist` settle guard as
/// `dataset.rs::poll_count_until_visible` (docs/architecture.md §9's
/// convention), duplicated rather than shared per this repo's "duplicate
/// rather than over-share" precedent for small, module-local helpers.
async fn poll_count_until_visible(
    client: &ChClient,
    count_sql: &str,
    expected: u64,
    label: &str,
) -> anyhow::Result<()> {
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct CountRow {
        n: u64,
    }
    for attempt in 0..120 {
        let mut stream = client
            .query_stream::<CountRow>(count_sql, &QuerySettings::new())
            .await?;
        let n = match stream.next().await {
            Some(row) => row?.n,
            None => 0,
        };
        if n >= expected {
            return Ok(());
        }
        if attempt % 10 == 0 {
            eprintln!("waiting for {label} to settle: {n}/{expected} visible");
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    anyhow::bail!(
        "{label} did not reach {expected} visible within the poll deadline (60s) — the \
         Distributed corpus failed to settle"
    )
}

/// Loads `spec`'s corpus into the database `client` is bound to (already
/// schema-initialized by the caller via `pulsus_schema::run_init`): for
/// each cardinality tier, one `metric_{cardinality}` metric with
/// `cardinality` series, each series carrying exactly one row, in a bucket
/// chosen by [`assign_bucket`] over `[spec.ref_ms - window_ms,
/// spec.ref_ms]`. `spec.ref_ms` is the caller's frozen reference instant
/// (module doc comment) — this function never reads the wall clock itself.
pub async fn load(
    client: &ChClient,
    spec: &MetricsCorpusSpec,
) -> anyhow::Result<MetricsCorpusSummary> {
    anyhow::ensure!(
        !spec.cardinalities.is_empty(),
        "--metric-cardinalities must name at least one cardinality"
    );
    if spec.profile == Profile::Full {
        eprintln!(
            "profile=full: this is the design-target 5M-active-series scale corpus shape \
             (docs/schemas.md §2.1's strategy-ladder decision gate, issue #34) — running the \
             largest tier at millions of series takes a long time and is not something this \
             invocation limits or validates on your behalf."
        );
    }

    let series_table = if spec.dist {
        "metric_series_dist"
    } else {
        "metric_series"
    };

    let start_instant = Instant::now();
    let buckets = bucket_boundaries(spec.ref_ms, spec.window_ms, spec.bucket_ms);
    anyhow::ensure!(
        buckets.len() >= 2,
        "corpus window must contain at least 2 activity buckets (got {}) so the guard band \
         (issue #34 CODE review [high] finding) has a non-window-edge bucket to assign activity \
         to — this is a rare window/ref_ms-alignment edge case; retry",
        buckets.len()
    );

    let mut tiers = Vec::with_capacity(spec.cardinalities.len());
    let mut total_series_rows: u64 = 0;
    let mut batch: Vec<MetricSeriesRow> = Vec::with_capacity(INSERT_BATCH_ROWS);

    for &cardinality in &spec.cardinalities {
        anyhow::ensure!(cardinality >= 1, "every cardinality must be >= 1");
        let metric_name = format!("metric_{cardinality}");
        let mut narrow_fp = 0u64;

        for i in 0..cardinality {
            let labels = series_labels(i);
            let fingerprint = metric_fingerprint(&labels);
            if i == 0 {
                narrow_fp = fingerprint;
            }
            let labels_json = labels.to_canonical_json();
            let bucket = assign_bucket(i, &buckets);
            batch.push(MetricSeriesRow {
                metric_name: metric_name.clone(),
                fingerprint,
                unix_milli: bucket,
                labels: labels_json,
            });
            total_series_rows += 1;
            if batch.len() == INSERT_BATCH_ROWS {
                client.insert_block(series_table, &batch).await?;
                batch.clear();
            }
        }

        // Flush at every tier boundary, not only at INSERT_BATCH_ROWS —
        // each `insert_block` call becomes its own MergeTree part, and a
        // part's own primary-key min/max lets a `metric_name = X` query
        // skip parts belonging to a *different* metric entirely. Without
        // this, a small CI-scale corpus (well under one INSERT_BATCH_ROWS
        // batch) lands every tier's rows in one shared part, so a query
        // scoped to one metric still touches granules straddling a
        // *different* metric's rows (confirmed live via `EXPLAIN indexes =
        // 1`: `Parts: 1/1` covering all three tiers) — noise this
        // benchmark's `read_rows` evidence must not carry.
        if !batch.is_empty() {
            client.insert_block(series_table, &batch).await?;
            batch.clear();
        }

        tiers.push(TierInfo {
            metric_name,
            cardinality,
            series_rows: cardinality,
            narrow_fp,
        });
    }

    if spec.dist {
        poll_count_until_visible(
            client,
            &format!("SELECT count() AS n FROM {series_table}"),
            total_series_rows,
            series_table,
        )
        .await?;
    }

    Ok(MetricsCorpusSummary {
        tiers,
        total_series_rows,
        bucket_ms: spec.bucket_ms,
        window_ms: spec.window_ms,
        end_ms: spec.ref_ms,
        load_elapsed_ms: start_instant.elapsed().as_millis() as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn series_labels_job_is_broad_across_eight_values_when_present() {
        let mut jobs: Vec<String> = (0..80u64)
            .filter_map(|i| series_labels(i).get("job").map(str::to_string))
            .collect();
        jobs.sort();
        jobs.dedup();
        assert_eq!(jobs.len(), 8);
    }

    #[test]
    fn series_labels_status_5xx_matches_exactly_two_of_four_residues() {
        let statuses: Vec<&'static str> = ["200", "404", "500", "503"].to_vec();
        let fivexx: Vec<&&str> = statuses.iter().filter(|s| s.starts_with('5')).collect();
        assert_eq!(fivexx.len(), 2);
    }

    #[test]
    fn series_labels_pod_is_unique_per_series() {
        let a = series_labels(0);
        let b = series_labels(1);
        assert_ne!(a.get("pod"), b.get("pod"));
    }

    #[test]
    fn series_labels_excludes_the_metric_name_label() {
        assert_eq!(series_labels(0).get("__name__"), None);
    }

    #[test]
    fn series_labels_omits_job_for_a_deterministic_tenth_of_series() {
        assert_eq!(series_labels(0).get("job"), None);
        assert_eq!(series_labels(10).get("job"), None);
        assert_eq!(series_labels(1).get("job"), Some("j1"));
    }

    #[test]
    fn series_labels_omits_status_for_a_deterministic_thirteenth_of_series() {
        assert_eq!(series_labels(0).get("status"), None);
        assert_eq!(series_labels(13).get("status"), None);
        assert_eq!(series_labels(1).get("status"), Some("404"));
    }

    #[test]
    fn series_labels_always_carries_pod_region_env_even_when_job_or_status_is_absent() {
        let labels = series_labels(0); // 0 % 10 == 0 && 0 % 13 == 0: omits both
        assert_eq!(labels.get("job"), None);
        assert_eq!(labels.get("status"), None);
        assert_eq!(labels.get("pod"), Some("pod-0"));
        assert!(labels.get("region").is_some());
        assert!(labels.get("env").is_some());
    }

    #[test]
    fn metric_fingerprint_is_stable_across_calls_for_the_same_series() {
        let a = metric_fingerprint(&series_labels(42));
        let b = metric_fingerprint(&series_labels(42));
        assert_eq!(a, b);
    }

    #[test]
    fn metric_fingerprint_differs_across_series() {
        let a = metric_fingerprint(&series_labels(0));
        let b = metric_fingerprint(&series_labels(1));
        assert_ne!(a, b);
    }

    #[test]
    fn bucket_boundaries_covers_the_whole_window_inclusive() {
        let bounds = bucket_boundaries(10_000, 10_000, 5_000);
        assert_eq!(bounds, vec![0, 5_000, 10_000]);
    }

    #[test]
    fn bucket_boundaries_of_a_day_bucket_over_a_24h_window_is_usually_one_or_two_buckets() {
        let day_ms = 86_400_000;
        let bounds = bucket_boundaries(day_ms * 10, 24 * 3_600_000, day_ms);
        assert!(bounds.len() <= 2, "got {bounds:?}");
    }

    #[test]
    fn assign_bucket_never_assigns_the_window_edge_guard_bucket() {
        let buckets = bucket_boundaries(1_000_000, 500_000, 50_000);
        assert!(
            buckets.len() >= 2,
            "test fixture must exercise the guard band"
        );
        for i in 0..500u64 {
            let b = assign_bucket(i, &buckets);
            assert!(
                b > buckets[0],
                "series {i} was assigned to the guard-band bucket {b} (buckets[0] = {})",
                buckets[0]
            );
        }
    }

    #[test]
    fn assign_bucket_only_ever_uses_buckets_after_the_guard() {
        let buckets = bucket_boundaries(1_000_000, 200_000, 50_000);
        let guard_span = buckets.len() as u64 - 1;
        for i in 0..200u64 {
            let b = assign_bucket(i, &buckets);
            assert!(buckets[1..].contains(&b));
            assert_eq!(b, buckets[1 + (mix64(i) % guard_span) as usize]);
        }
    }

    /// The bug this decorrelation fixes, pinned directly (issue #34 CODE
    /// review round-2 finding, discovered during this fix's own live
    /// verification): with a raw `i % guard_span` bucket assignment and the
    /// default 1h/24h shape (`guard_span == 24`, a multiple of `job`'s
    /// modulus 8), *every* series landing in a given bucket shared the
    /// *same* `job` residue — so a bucket-scoped `job="j0"` query matched
    /// zero series in any bucket not assigned residue 0. `mix64` must
    /// break this: some series with `job="j0"` land in the same bucket as
    /// series with other `job` values.
    #[test]
    fn assign_bucket_decorrelates_from_the_job_label_residue() {
        let buckets = bucket_boundaries(1_784_000_000_000, 24 * 3_600_000, 3_600_000);
        let last = buckets[buckets.len() - 1];
        let mut job_zero_in_last_bucket = 0u64;
        for i in 0..2_000u64 {
            let job_present = !i.is_multiple_of(10);
            let is_job_zero = job_present && i.is_multiple_of(8);
            if is_job_zero && assign_bucket(i, &buckets) == last {
                job_zero_in_last_bucket += 1;
            }
        }
        assert!(
            job_zero_in_last_bucket > 0,
            "expected at least one job=\"j0\" series in the last bucket — the whole point of \
             decorrelating bucket assignment from the label residues"
        );
    }
}
