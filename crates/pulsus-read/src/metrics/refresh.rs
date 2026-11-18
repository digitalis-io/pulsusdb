//! The only ClickHouse-touching code of the label cache (issue #880): one
//! pass per tenant, every `PULSUS_CACHE_TTL`. A cold tenant's first pass
//! builds its [`super::promql_label_index::LabelIndex`] from
//! `metric_labels` ([`super::sql::label_index_build_query`]); every pass
//! after reads only the series registrations `metric_landing` received
//! since the last one, less an overlap ([`super::sql::label_index_feed_query`]),
//! and applies them in place. Rows are streamed and applied in batches of
//! [`APPLY_BATCH_ROWS`], so a pass never holds its whole answer.
//!
//! A pass's header — generation, recency anchor, covered-from edge and
//! watermark — moves only when the pass is accepted
//! ([`super::labels::LabelCache::commit_sweep`]). A failed pass leaves its
//! applied rows, each a registration the store holds, and the header as it
//! was, so the recency gate and the next feed's start are unchanged.
//! [`spawn_refresh_loop`] runs a pass on an interval in a self-healing
//! shape: a failed pass logs a warning and bumps `refresh_failures_total`,
//! and the next tick tries again.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChError, QuerySettings};
use pulsus_model::{LabelSet, Tenant, floor_to_activity_bucket};

use tokio::task::JoinHandle;

use super::labels::{LabelCache, LabelCacheConfig, TenantCache};
use super::rows::RegistrationRow;

/// Wall-clock now, milliseconds since the Unix epoch. `SystemTime::now()`
/// predating the Unix epoch is a broken-clock scenario, not one that
/// happens on any deployed system; it degrades to `0` rather than panicking
/// (mirrors `pulsus_write::ingest::http::now_unix_nanos`'s precedent).
/// `pub(crate)`: also used by [`super::labels::LabelCache::age_ms`] (the
/// `/metrics` age gauge, code-review round-2 fix), so the "what time is it"
/// primitive lives in exactly one place.
pub(crate) fn now_unix_ms() -> i64 {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
}

/// A feed whose watermark is older than this builds instead.
/// `PULSUS_METRICS_LANDING_RETENTION_HOURS` is at least 1, and a landing
/// part is dropped only once its newest row is that old
/// (`ttl_only_drop_parts = 1`), so every row received in the last hour is
/// still there.
pub(crate) const FEED_MAX_AGE_MS: i64 = 3_000_000;

/// Rows applied per write lock: bounds how long one batch holds it.
pub(crate) const APPLY_BATCH_ROWS: usize = 1_024;

/// A replica whose copy lags this much or more is not read by a cache
/// statement; a shard with no fresher replica fails the pass.
pub(crate) const CACHE_MAX_REPLICA_DELAY_SECS: u64 = 30;

/// Which statement a pass sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PassKind {
    /// Every series registered in `[lower_bound_ms, now]`, from
    /// `metric_labels`.
    Build { lower_bound_ms: i64 },
    /// The registrations `metric_landing` received from `from_ms` on.
    Feed { from_ms: i64 },
}

/// `floor_to_activity_bucket(now - window)`: the edge a pass covers from.
pub(crate) fn lower_bound_ms(now_ms: i64, window_ms: i64) -> i64 {
    floor_to_activity_bucket(now_ms - window_ms, pulsus_model::ACTIVITY_BUCKET_MS)
}

/// The pass's decision: a cold tenant builds, and so does one whose
/// watermark is more than [`FEED_MAX_AGE_MS`] old; any other feeds from its
/// watermark less the overlap.
pub(crate) fn pass_kind(
    generation: u64,
    watermark_ms: i64,
    now_ms: i64,
    overlap_ms: i64,
    window_ms: i64,
) -> PassKind {
    if generation == 0 || now_ms - watermark_ms > FEED_MAX_AGE_MS {
        PassKind::Build {
            lower_bound_ms: lower_bound_ms(now_ms, window_ms),
        }
    } else {
        PassKind::Feed {
            from_ms: watermark_ms - overlap_ms,
        }
    }
}

/// What a tenant's statement ended with, carried to the commit: the pass's
/// start and its covered-from edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PassStamp {
    pub(crate) now_ms: i64,
    pub(crate) lower_bound_ms: i64,
}

/// Applies one batch of a pass to `slot`'s index, leaving `batch` empty.
///
/// Under a read lock, each row whose series is not resident is marked;
/// those rows' labels are parsed with no lock held; then, under one write
/// lock, every row is applied. A feed row whose bucket is behind
/// `lower_bound_ms` or after `now_ms` is skipped; a build row's bucket is
/// taken as at most `now_ms` (its statement already bounds it below). One
/// pass runs at a time, so nothing else inserts between the two locks; a
/// row the read lock saw as new and an earlier row of the same batch
/// inserted is applied as a touch.
pub(crate) fn apply_batch(
    slot: &TenantCache,
    batch: &mut Vec<RegistrationRow>,
    build: bool,
    now_ms: i64,
    lower_bound_ms: i64,
) {
    let bucket = |row: &RegistrationRow| -> Option<i64> {
        if build {
            Some(row.bucket_ms.min(now_ms))
        } else if row.bucket_ms < lower_bound_ms || row.bucket_ms > now_ms {
            None
        } else {
            Some(row.bucket_ms)
        }
    };
    let unknown: Vec<bool> = {
        let index = slot.read_index();
        batch
            .iter()
            .map(|row| {
                bucket(row).is_some_and(|b| b >= lower_bound_ms)
                    && index.find(row.fingerprint).is_none()
            })
            .collect()
    };
    let mut parsed: Vec<Option<LabelSet>> = batch
        .iter()
        .zip(&unknown)
        .map(|(row, &new)| {
            new.then(|| crate::canonical_labels::parse_canonical_label_set(&row.labels))
        })
        .collect();
    {
        let mut index = slot.write_index();
        for (row, labels) in batch.iter().zip(parsed.iter_mut()) {
            let Some(bucket_ms) = bucket(row) else {
                continue;
            };
            index.apply(
                row.fingerprint,
                &row.metric_name,
                bucket_ms,
                lower_bound_ms,
                || {
                    labels.take().unwrap_or_else(|| {
                        crate::canonical_labels::parse_canonical_label_set(&row.labels)
                    })
                },
            );
        }
    }
    batch.clear();
}

/// One tenant's pass (issue #880): its statement, streamed into its index
/// in batches. `Ok` carries the stamp the commit needs; the rows applied
/// before an error stay.
pub(crate) async fn pass_tenant(
    client: &ChClient,
    config: &LabelCacheConfig,
    tenant: &Tenant,
    slot: &TenantCache,
) -> Result<PassStamp, ChError> {
    let now_ms = now_unix_ms();
    let lb = lower_bound_ms(now_ms, config.window_ms);
    let (generation, watermark_ms) = {
        let index = slot.read_index();
        (index.generation, index.watermark_ms)
    };
    let kind = pass_kind(
        generation,
        watermark_ms,
        now_ms,
        config.feed_overlap_ms,
        config.window_ms,
    );
    let sql = match kind {
        PassKind::Build { lower_bound_ms } => super::sql::label_index_build_query(
            tenant,
            &config.labels_table,
            lower_bound_ms,
            now_ms,
        ),
        PassKind::Feed { from_ms } => {
            super::sql::label_index_feed_query(tenant, &config.landing_table, from_ms)
        }
    };
    let build = matches!(kind, PassKind::Build { .. });
    let settings = sweep_settings(config.read_max_memory_bytes);
    let mut stream = client
        .query_stream::<RegistrationRow>(&sql, &settings)
        .await?;
    let mut batch = Vec::with_capacity(APPLY_BATCH_ROWS);
    while let Some(row) = stream.next().await {
        batch.push(row?);
        if batch.len() == APPLY_BATCH_ROWS {
            apply_batch(slot, &mut batch, build, now_ms, lb);
        }
    }
    apply_batch(slot, &mut batch, build, now_ms, lb);
    Ok(PassStamp {
        now_ms,
        lower_bound_ms: lb,
    })
}

/// What one label-cache pass read (issue #803): every tenant whose
/// statement ended without error, with its stamp, every tenant whose
/// statement failed, and the first such failure. Committed by
/// [`LabelCache::commit_sweep`] only once the pass is accepted.
#[derive(Debug, Default)]
pub(crate) struct SweepOutcome {
    pub(crate) swept: Vec<(Tenant, PassStamp)>,
    pub(crate) failed: Vec<Tenant>,
    pub(crate) first_error: Option<ChError>,
}

/// One attempt of a label-cache pass (issue #803): every tenant's
/// statement, in order, on `client`. A tenant whose statement fails goes
/// to `failed` and the pass goes on, so the attempt itself is `Ok`; whether
/// it met a server failure is its client's to say.
pub(crate) async fn sweep_attempt(
    client: &ChClient,
    config: &LabelCacheConfig,
    slots: &[(Tenant, Arc<TenantCache>)],
) -> Result<SweepOutcome, ChError> {
    let mut outcome = SweepOutcome::default();
    for (tenant, slot) in slots {
        match pass_tenant(client, config, tenant, slot).await {
            Ok(stamp) => outcome.swept.push((tenant.clone(), stamp)),
            Err(err) => {
                outcome.failed.push(tenant.clone());
                if outcome.first_error.is_none() {
                    outcome.first_error = Some(err);
                }
            }
        }
    }
    Ok(outcome)
}

/// The settings of every label-cache statement (issues #398, #880). A free
/// function so the decision is provable without a ClickHouse connection
/// (the `read_query_settings`/`probe_fanout_bound` precedent).
///
/// - The per-query memory ceiling from `reader.promql_read_max_memory_bytes`
///   and the throw-not-spill pair `metrics::exec::metrics_read_settings`
///   sets.
/// - The answer pins with `skip_unavailable_shards = 0`, whatever
///   `PULSUS_SKIP_UNAVAILABLE_SHARDS` says: a pass must read every shard,
///   and one that cannot fails, leaving its watermark where it was.
/// - The replica guard: a replica [`CACHE_MAX_REPLICA_DELAY_SECS`] or more
///   behind is not read, and a shard with no fresher replica fails the
///   pass (`fallback_to_stale_replicas_for_distributed_queries = 0`), with
///   the local copy preferred. Each statement reads one table, so the
///   server applies the guard to it.
/// - `distributed_product_mode = 'local'` and `join_algorithm = 'hash'`,
///   the metrics reads' choices (issue #623), so a statement that nested or
///   joined would not fall to the server's defaults.
/// - `log_comment`: the task, as every background statement names it
///   (issue #778).
pub(crate) fn sweep_settings(read_max_memory_bytes: u64) -> QuerySettings {
    QuerySettings::new()
        .answer_pins(false, false)
        .set("max_memory_usage", read_max_memory_bytes)
        .set("max_bytes_before_external_group_by", 0u64)
        .set("distributed_product_mode", "local")
        .set("join_algorithm", "hash")
        .set(
            "max_replica_delay_for_distributed_queries",
            CACHE_MAX_REPLICA_DELAY_SECS,
        )
        .set("fallback_to_stale_replicas_for_distributed_queries", 0u64)
        .set("prefer_localhost_replica", 1u64)
        .set("log_comment", "background:label-cache-sweep")
}

/// Spawns the recurring refresh task: ticks every `ttl`, running one
/// [`LabelCache::refresh`] pass per tick, in a self-healing shape: a failed
/// pass only logs and bumps a failure counter (already done inside the
/// pass), it never aborts the loop or panics — the next tick simply tries
/// again. `tokio::time::interval`'s first tick fires immediately, so the
/// cache starts warming as soon as this task is spawned, not after the
/// first full `ttl` elapses.
pub fn spawn_refresh_loop(cache: Arc<LabelCache>, ttl: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(ttl);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            if let Err(err) = cache.refresh().await {
                tracing::warn!(
                    error = %err,
                    "label cache pass failed; answering from what the cache holds"
                );
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **T4 (issue #880).** A cold tenant builds; a watermark more than
    /// 50 minutes old builds; anything newer feeds from the watermark less
    /// the overlap.
    #[test]
    fn pass_kind_builds_when_cold_or_stale_and_feeds_otherwise() {
        let now = 1_788_823_800_000;
        let window = 3_600_000;
        let lb = floor_to_activity_bucket(now - window, pulsus_model::ACTIVITY_BUCKET_MS);
        assert_eq!(
            pass_kind(0, 0, now, 300_000, window),
            PassKind::Build { lower_bound_ms: lb }
        );
        assert_eq!(
            pass_kind(0, now - 1_000, now, 300_000, window),
            PassKind::Build { lower_bound_ms: lb }
        );
        assert_eq!(
            pass_kind(1, now - 3_000_001, now, 300_000, window),
            PassKind::Build { lower_bound_ms: lb }
        );
        assert_eq!(
            pass_kind(1, now - 3_000_000, now, 300_000, window),
            PassKind::Feed {
                from_ms: now - 3_300_000
            }
        );
    }

    #[test]
    fn now_unix_ms_is_a_plausible_recent_timestamp() {
        // Sanity bound: some time after 2024-01-01 in milliseconds.
        assert!(now_unix_ms() > 1_700_000_000_000);
    }
}
