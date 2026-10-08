//! The only ClickHouse-touching code in this module: the docs/architecture.md
//! §5.2 sweep (every `(metric_name, fingerprint)` in `metric_series` since
//! `floor(now - window)`, with its own label row in `metric_labels` —
//! [`super::sql::sweep_query`]), building a whole new
//! [`super::labels::CacheSnapshot`] and atomically swapping it into the
//! resident [`super::labels::LabelCache`]. [`spawn_refresh_loop`] runs this
//! on an interval in the self-healing shape of
//! `pulsus_server::serve::spawn_rotation_task` (not importable here — the
//! same *shape*, reimplemented against this crate's own types): a failed
//! sweep logs a warning and bumps `refresh_failures_total`, but **never**
//! clobbers the last good snapshot — a blanked cache would mass-false-empty
//! every in-window query.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use pulsus_clickhouse::{ChError, QuerySettings};
use pulsus_model::{Fingerprint, LabelSet, Tenant, floor_to_activity_bucket};

use super::matcher::DataWindow;
use tokio::task::JoinHandle;

use super::labels::{CacheSnapshot, LabelCache};
use super::rows::SeriesRow;

/// Renders the §5.2 sweep SQL ([`super::sql::sweep_query`]): every series
/// active since `floor(now - window)`, with its labels, no upper bound (the
/// sweep always runs "as of now"). Pure so it is snapshot-testable without a
/// clock/DB.
fn sweep_sql(
    tenant: &Tenant,
    series_table: &str,
    labels_table: &str,
    window: DataWindow,
) -> String {
    super::sql::sweep_query(tenant, series_table, labels_table, window)
}

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

/// One sweep + swap: streams [`SeriesRow`]s, builds a whole new
/// [`CacheSnapshot`], swaps it into `cache.snapshot` under a brief write
/// lock (never held across an `.await`), and updates `cache.metrics`. On a
/// `ChError`, the last good snapshot is left untouched and
/// `refresh_failures_total` is bumped — this is the self-healing contract
/// [`LabelCache::refresh`] and [`spawn_refresh_loop`] both rely on.
pub(crate) async fn run_sweep(cache: &LabelCache) -> Result<(), ChError> {
    let now_ms = now_unix_ms();
    let lower_bound_ms = floor_to_activity_bucket(
        now_ms - cache.config.window_ms,
        pulsus_model::ACTIVITY_BUCKET_MS,
    );
    let tenant = Tenant::from_header(None, false).expect("the empty tenant");
    let sql = sweep_sql(
        &tenant,
        &cache.config.series_table,
        &cache.config.labels_table,
        DataWindow {
            start_ms: lower_bound_ms,
            end_ms: now_ms,
        },
    );

    let result = fetch_rows(cache, &sql).await;
    let rows = match result {
        Ok(rows) => rows,
        Err(err) => {
            cache.metrics.record_refresh_failure();
            return Err(err);
        }
    };

    let mut by_fingerprint: HashMap<Fingerprint, LabelSet> = HashMap::with_capacity(rows.len());
    let mut by_metric: HashMap<String, Vec<Fingerprint>> = HashMap::new();
    for row in rows {
        // The fingerprint is the series ID, which includes the metric name
        // (issue #623), so one label set under two names is two IDs and
        // neither map can merge series across metrics. Statement 2 returns
        // each ID once; a repeat would carry the same row, so whichever the
        // sweep saw last overwrites with the same content.
        by_fingerprint.insert(
            row.fingerprint,
            crate::canonical_labels::parse_canonical_label_set(&row.labels),
        );
        by_metric
            .entry(row.metric_name)
            .or_default()
            .push(row.fingerprint);
    }
    for fps in by_metric.values_mut() {
        fps.sort_unstable();
        fps.dedup();
    }

    let series_count = by_fingerprint.len() as u64;
    let generation = cache.current_snapshot().generation.saturating_add(1);
    let snapshot = CacheSnapshot {
        by_fingerprint,
        by_metric,
        // The sweep's own `now_ms` (code review round-2 fix: this — not a
        // wall-clock `Instant` captured at swap time — is the recency
        // anchor `resolve_over`'s upper-bound gate and `LabelCache::age_ms`
        // both measure against; see `CacheSnapshot::sweep_time_ms`'s doc
        // comment).
        sweep_time_ms: now_ms,
        covered_from_ms: lower_bound_ms,
        generation,
    };

    {
        let mut guard = match cache.snapshot.write() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        *guard = Arc::new(snapshot);
    }
    cache
        .metrics
        .record_refresh(series_count, cache.config.cache_max_series);
    Ok(())
}

/// Issue #398: the sweep carries the same per-query memory ceiling
/// (`max_memory_usage` + `max_bytes_before_external_group_by = 0`,
/// throw-not-spill) as every request-path metrics read, from
/// `reader.promql_read_max_memory_bytes`. It sent `QuerySettings::new()`
/// before — the one metrics ClickHouse read with no bound at all.
///
/// The failure BEHAVIOUR is unchanged on purpose: a breach still returns
/// `Err` here, and [`spawn_refresh_loop`] still logs it and keeps serving
/// the last good snapshot. This only stops an unbounded sweep from
/// competing for server memory; it does not, and is not meant to, change
/// the fact that a degraded cache keeps answering `200` (recorded as
/// remaining work on issue #398).
async fn fetch_rows(cache: &LabelCache, sql: &str) -> Result<Vec<SeriesRow>, ChError> {
    let mut rows = Vec::new();
    let mut stream = cache
        .client
        .query_stream::<SeriesRow>(sql, &sweep_settings(cache.config.read_max_memory_bytes))
        .await?;
    while let Some(row) = stream.next().await {
        rows.push(row?);
    }
    Ok(rows)
}

/// The label-cache sweep's query settings (issue #398): the per-query
/// memory ceiling from `reader.promql_read_max_memory_bytes`, the same
/// throw-not-spill pair `metrics::exec::metrics_read_settings` sets. A
/// free function so the decision is provable without a ClickHouse
/// connection (the `read_query_settings`/`probe_fanout_bound` precedent).
///
/// **`distributed_product_mode = 'local'`, always** (issue #623). Clustered,
/// the sweep reads `metric_labels*_dist` for the `(metric_name,
/// fingerprint)` pairs a nested `metric_series*_dist` read names, which the
/// default `'deny'` refuses.
/// `'local'` is exact: one kind-2 row writes a series' activity row and its
/// label row on the same node, so each shard's labels cover each shard's
/// series. Single-node there is no `Distributed` table and the setting
/// changes nothing.
///
/// **`join_algorithm = 'hash'`** (issue #623). The sweep reads label rows
/// first and joins nothing, so the setting decides nothing for it today; it
/// is the metrics reads' join choice ([`super::exec`]'s series reads), kept
/// here so a sweep that did join would not fall to the server's default
/// `parallel_hash`, which reserved about 42 MiB before reading a row —
/// measured on 26.3.29.7, growing with the thread count.
pub(crate) fn sweep_settings(read_max_memory_bytes: u64) -> QuerySettings {
    QuerySettings::new()
        .set("max_memory_usage", read_max_memory_bytes)
        .set("max_bytes_before_external_group_by", 0u64)
        .set("distributed_product_mode", "local")
        .set("join_algorithm", "hash")
}

/// Spawns the recurring refresh task: ticks every `ttl`, running one
/// [`run_sweep`] per tick. Mirrors `serve::spawn_rotation_task`'s
/// self-healing shape: a failed sweep only logs and bumps a failure
/// counter (already done inside `run_sweep`), it never aborts the loop or
/// panics — the next tick simply tries again. `tokio::time::interval`'s
/// first tick fires immediately, so the cache starts warming as soon as
/// this task is spawned, not after the first full `ttl` elapses.
pub fn spawn_refresh_loop(cache: Arc<LabelCache>, ttl: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(ttl);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            if let Err(err) = run_sweep(&cache).await {
                tracing::warn!(
                    error = %err,
                    "label cache refresh sweep failed; serving the last good snapshot"
                );
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **Issue #623: the sweep is statement 2 with no matcher**: the lookup
    /// rows of every series active in the cache window, by the activity
    /// table's day and hour mask.
    #[test]
    fn sweep_sql_reads_the_lookup_for_the_active_series() {
        assert_eq!(
            sweep_sql(
                &Tenant::from_header(None, false).expect("the empty tenant"),
                "metric_series",
                "metric_labels",
                DataWindow {
                    start_ms: 1_788_820_200_000,
                    end_ms: 1_788_823_800_000,
                },
            ),
            "SELECT fingerprint, any(name) AS metric_name, any(label_text) AS labels\n\
             FROM (\n\
             \x20 SELECT fingerprint, metric_name AS name, labels AS label_text\n\
             \x20 FROM metric_labels\n\
             \x20 WHERE fingerprint IN (\n\
             \x20     SELECT fingerprint\n\
             \x20     FROM metric_series\n\
             \x20     WHERE day BETWEEN '2026-09-07' AND '2026-09-07'\n\
             \x20       AND bitAnd(hours, multiIf(day = '2026-09-07' AND day = '2026-09-07', 12582912, \
             day = '2026-09-07', 12582912, day = '2026-09-07', 16777215, 16777215)) != 0\n\
             \x20   )\n\
             )\n\
             GROUP BY fingerprint\n\
             ORDER BY metric_name, fingerprint"
        );
    }

    #[test]
    fn now_unix_ms_is_a_plausible_recent_timestamp() {
        // Sanity bound: some time after 2024-01-01 in milliseconds.
        assert!(now_unix_ms() > 1_700_000_000_000);
    }
}
