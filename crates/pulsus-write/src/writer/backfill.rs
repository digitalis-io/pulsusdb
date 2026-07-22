//! Registration backfill (issues #134/#139): heals ONLY definitely-failed
//! (`FlushOutcome::Poisoned`) registration flushes. A Poisoned outcome is
//! provably not-committed (see `writer::table::FlushPoisonedHook`'s doc
//! comment), so re-inserting the rows replays nothing that could have
//! committed — the issue-#9 "`InsertUncertain` is never replayed"
//! invariant is not engaged: uncertain-fate generation failures never
//! enter this backlog (the hook exists only in the Poisoned arm), and
//! this task's own `InsertUncertain` outcome is terminal-abandon, never
//! retried.
//!
//! Generic over [`BackfillRow`] (issue #139): one mechanism serves all
//! four registration tables —
//! - `log_streams` (`LogStreamRow`): keyed `(fingerprint, month)`,
//!   versioned on `updated_ns` (`ReplacingMergeTree(updated_ns)`);
//! - `metric_series` (`MetricSeriesRow`): keyed `(metric_name,
//!   fingerprint, bucket, value_type)`, versionless — duplicate-tolerant
//!   by design (read-side `LIMIT 1 BY` collapse);
//! - `metric_metadata` (`MetricMetadataRow`): keyed `metric_name`,
//!   versioned on `updated_ns` (`ReplacingMergeTree(updated_ns)`);
//! - `trace_attrs_idx` (`TraceAttrRow`): keyed on the full RMT ORDER BY
//!   tuple, versionless — the key determines the whole logical row.
//!
//! The append-only tables (`log_samples`, `metric_samples`,
//! `metric_hist_samples`, `trace_spans`) are structurally excluded: their
//! `TableContext`s pass `on_flush_poisoned: None`, and each backlog is
//! typed to exactly one registration row shape bound to one table name —
//! cross-table replay is unrepresentable (#9 applies in full to every
//! sample/span/rollup target).
//!
//! Each backlog is bounded (keyed dedup plus a byte cap) and memory-only:
//! no spool reads or writes happen here, ever — `uncertain/` stays
//! audit-only per #9, and the poison-spool file (when its write
//! succeeded; `spool_write_failures_total` otherwise) remains the manual
//! repair record. Cache promotion on heal is family-specific via the
//! `on_healed` hook: `StreamLru`/`SeriesLru` membership promotion is safe
//! (pure membership over the full logical identity); the metadata hook
//! only ever *invalidates* its value cache (issue #139: a heal must never
//! install a descriptor — see `writer::metric::metadata_healed_hook`);
//! traces have no cache (`on_healed: None`).

use std::sync::{Arc, Mutex};
use std::time::Instant;

use pulsus_clickhouse::{ChError, ChRow};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use tokio::sync::watch;
use tracing::warn;

use crate::writer::config::WriterRuntime;
use crate::writer::metrics::BackfillMetrics;
use crate::writer::table::{BlockInserter, bound_by_deadline};

/// A registration row a [`RegistrationBacklog`] can hold: a logical key,
/// a monotone-per-key version, and a byte estimate (delegating to the
/// row's `est_bytes`).
pub(crate) trait BackfillRow: Clone + Send + Sync + 'static {
    type Key: Eq + std::hash::Hash + Clone + Send;

    fn backfill_key(&self) -> Self::Key;

    /// Monotone-per-key version driving larger-wins replacement
    /// ([`RegistrationBacklog::enqueue`]) and version-checked removal
    /// ([`RegistrationBacklog::remove_if_version`]). Versionless families
    /// (`metric_series`, `trace_attrs_idx`) return a constant `0`:
    /// equality always holds, which is correct because their key
    /// determines the full logical row — a "newer" enqueue mid-attempt
    /// carries byte-identical content, so #134's in-flight-race fix
    /// degenerates safely to always-remove (see each impl's doc comment
    /// in `writer::rows`).
    fn backfill_version(&self) -> i64;

    /// Byte estimate for the backlog cap — delegates to the row's
    /// `est_bytes()`.
    fn backfill_bytes(&self) -> u64;
}

/// Bounded, keyed backlog of Poisoned-flush registration rows awaiting
/// re-insert. Keyed dedup on [`BackfillRow::backfill_key`]: an existing
/// key is replaced iff the incoming version is larger; a new key that
/// would exceed `max_bytes` is rejected and counted dropped.
pub(crate) struct RegistrationBacklog<R: BackfillRow> {
    entries: HashMap<R::Key, R>,
    /// Sum of `backfill_bytes` over `entries`.
    bytes: u64,
    max_bytes: u64,
}

impl<R: BackfillRow> RegistrationBacklog<R> {
    pub(crate) fn new(max_bytes: u64) -> Self {
        RegistrationBacklog {
            entries: HashMap::new(),
            bytes: 0,
            max_bytes,
        }
    }

    /// Enqueues `rows`, returning `(accepted, dropped)` row counts.
    /// Keyed dedup: an existing key is replaced iff the incoming version
    /// is larger (byte accounting adjusted; replacement — and a stale
    /// duplicate left in place — counts as accepted, never dropped). A
    /// new key whose bytes would exceed `max_bytes` is rejected and
    /// counted dropped.
    pub(crate) fn enqueue(&mut self, rows: &[R]) -> (u64, u64) {
        let mut accepted = 0u64;
        let mut dropped = 0u64;
        for row in rows {
            let key = row.backfill_key();
            match self.entries.get(&key) {
                Some(existing) => {
                    if row.backfill_version() > existing.backfill_version() {
                        let old_bytes = existing.backfill_bytes();
                        self.bytes = self.bytes - old_bytes + row.backfill_bytes();
                        self.entries.insert(key, row.clone());
                    }
                    accepted += 1;
                }
                None => {
                    let row_bytes = row.backfill_bytes();
                    if self.bytes + row_bytes > self.max_bytes {
                        dropped += 1;
                    } else {
                        self.bytes += row_bytes;
                        self.entries.insert(key, row.clone());
                        accepted += 1;
                    }
                }
            }
        }
        (accepted, dropped)
    }

    /// A snapshot of every pending row — cloned out so the caller never
    /// holds the backlog lock across the re-insert `.await`. The rows'
    /// versions double as the attempt's versions for
    /// [`Self::remove_if_version`]'s compare-and-remove.
    pub(crate) fn pending_rows(&self) -> Vec<R> {
        self.entries.values().cloned().collect()
    }

    /// Version-checked removal (compare-and-remove), the symmetric
    /// counterpart of [`Self::enqueue`]'s larger-version-wins
    /// replacement: for each attempted row, the entry is removed (byte
    /// accounting restored) only if the backlog's CURRENT version for
    /// that key equals the attempted row's — i.e. the entry the attempt
    /// actually carried. A NEWER entry enqueued by a concurrent Poisoned
    /// flush while the attempt was in flight is left in place (that newer
    /// row was never inserted; the next tick retries it). For versionless
    /// families the compare is always-equal — safe because the key
    /// determines the full logical row. Returns the entries actually
    /// removed — the only ones a caller may count healed/abandoned or
    /// pass to an `on_healed` hook (issue #139: the metadata hook needs
    /// row values, not just keys).
    pub(crate) fn remove_if_version(&mut self, attempted: &[R]) -> Vec<R> {
        let mut removed = Vec::new();
        for row in attempted {
            let key = row.backfill_key();
            if let Some(current) = self.entries.get(&key)
                && current.backfill_version() == row.backfill_version()
            {
                let entry = self
                    .entries
                    .remove(&key)
                    .expect("entry present under the same lock");
                self.bytes -= entry.backfill_bytes();
                removed.push(entry);
            }
        }
        removed
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

/// A confirmed-heal callback, invoked with exactly the entries a
/// successful re-insert attempt removed from the backlog (issue #139):
/// `log_streams` promotes `StreamLru`, `metric_series` promotes
/// `SeriesLru` (both pure membership sets — safe), `metric_metadata`
/// ONLY invalidates its value cache (never installs — see
/// `writer::metric::metadata_healed_hook`), `trace_attrs_idx` has none.
pub(crate) type BackfillHealedHook<R> = Arc<dyn Fn(&[R]) + Send + Sync>;

/// The `on_flush_poisoned` hook body shared by every registration table
/// (the per-writer closures delegate here verbatim): enqueues `rows`
/// into the backlog and bumps the enqueued/dropped totals plus the
/// pending gauge.
pub(crate) fn enqueue_failed<R: BackfillRow>(
    backlog: &Mutex<RegistrationBacklog<R>>,
    metrics: &BackfillMetrics,
    rows: &[R],
) {
    let mut guard = backlog.lock().expect("registration backlog mutex poisoned");
    let (accepted, dropped) = guard.enqueue(rows);
    metrics
        .enqueued_total
        .fetch_add(accepted, Ordering::Relaxed);
    metrics.dropped_total.fetch_add(dropped, Ordering::Relaxed);
    metrics.pending.store(guard.len() as u64, Ordering::Relaxed);
}

/// Spawns a registration-backfill task: every
/// `runtime.backfill_retry_interval` (no immediate first tick —
/// `interval_at` starts one interval out), a non-empty backlog is
/// re-inserted through `inserter` (the same table name the flush path
/// uses — the `_dist` wrapper in cluster mode where one exists) and the
/// outcome classified:
///
/// - `Ok` → version-checked remove, invoke `on_healed` with exactly the
///   removed entries (a confirmed flush), `healed_total += removed`;
/// - pre-send retryable error → keep all entries, `retries_total += 1`
///   (retried next tick);
/// - `InsertUncertain` → **terminal**: version-checked remove,
///   `abandoned_total += removed`, warn-log — commit fate unknown, never
///   retried (#9 discipline);
/// - any other (deterministic) error → version-checked remove,
///   `abandoned_total += removed` (no poison spin; a poison-spool record
///   of the abandoned rows exists iff the generation's spool write
///   succeeded — residual R5 otherwise).
///
/// "Version-checked remove" ([`RegistrationBacklog::remove_if_version`]):
/// an entry replaced by a NEWER Poisoned flush while the attempt was in
/// flight survives every terminal branch and is retried next tick with
/// the newer row — see [`classify_attempt`].
///
/// Bounded shutdown (issue #134 plan §A, mirroring
/// `writer::table::settle_generation`): each in-flight insert attempt
/// races `shutdown_rx`; once a deadline is observed (or the channel
/// closes — treated as an immediate deadline), the attempt is bounded by
/// [`bound_by_deadline`] and dropped on elapse (safe: a backfill insert
/// holds no waiters and no byte reservation), then the task exits with
/// the backlog untouched — no final drain.
pub(crate) fn spawn_backfill<R>(
    backlog: Arc<Mutex<RegistrationBacklog<R>>>,
    inserter: Arc<dyn BlockInserter<R>>,
    table: Arc<str>,
    on_healed: Option<BackfillHealedHook<R>>,
    metrics: Arc<BackfillMetrics>,
    runtime: Arc<WriterRuntime>,
    mut shutdown_rx: watch::Receiver<Option<Instant>>,
) -> tokio::task::JoinHandle<()>
where
    R: BackfillRow + ChRow,
{
    tokio::spawn(async move {
        let mut interval = tokio::time::interval_at(
            tokio::time::Instant::now() + runtime.backfill_retry_interval,
            runtime.backfill_retry_interval,
        );
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = interval.tick() => {},
                changed = shutdown_rx.changed() => {
                    // A deadline arrived, or the channel closed (writer
                    // dropped without a graceful shutdown) — either way,
                    // stop; no final drain.
                    let _ = changed;
                    return;
                }
            }
            if shutdown_rx.borrow().is_some() {
                return;
            }

            let pending = {
                backlog
                    .lock()
                    .expect("registration backlog mutex poisoned")
                    .pending_rows()
            };
            if pending.is_empty() {
                continue;
            }

            // One insert attempt of all pending rows, shutdown-race
            // bounded exactly like `settle_generation`'s insert.
            let outcome = {
                let attempt = inserter.insert(&table, &pending);
                tokio::pin!(attempt);

                let already_shutting_down = *shutdown_rx.borrow();
                match already_shutting_down {
                    Some(deadline) => bound_by_deadline(&mut attempt, deadline).await,
                    None => {
                        tokio::select! {
                            out = &mut attempt => Ok(out),
                            changed = shutdown_rx.changed() => {
                                // Closed channel == immediate deadline,
                                // matching `settle_generation`'s
                                // convention.
                                let _ = changed;
                                let deadline =
                                    shutdown_rx.borrow().unwrap_or_else(Instant::now);
                                bound_by_deadline(&mut attempt, deadline).await
                            }
                        }
                    }
                }
            };

            match outcome {
                // Deadline elapsed mid-attempt: drop the in-flight insert
                // and exit, backlog untouched — the process is exiting.
                Err(_elapsed) => return,
                Ok(result) => {
                    classify_attempt(
                        &backlog,
                        on_healed.as_ref(),
                        &metrics,
                        &table,
                        &pending,
                        result,
                    );
                }
            }

            // A bounded outcome that resolved before the deadline was
            // classified normally above; if shutdown has been observed,
            // exit now rather than waiting out another tick.
            if shutdown_rx.borrow().is_some() {
                return;
            }
        }
    })
}

/// Applies one re-insert attempt's outcome to the backlog/hook/counters —
/// see [`spawn_backfill`]'s doc comment for the classification contract.
///
/// Every terminal branch removes via the version-checked
/// [`RegistrationBacklog::remove_if_version`] (issue #134 code-review
/// fix): the `attempted` snapshot was taken BEFORE the `.await`, so a
/// NEWER Poisoned flush for the same key may have replaced the entry
/// while the attempt was in flight — that newer row was never inserted
/// and must survive for the next tick, never falsely healed/promoted
/// (success arm) or silently abandoned (failure arms). Heal/abandon
/// counting and the `on_healed` invocation apply only to the entries
/// actually removed.
fn classify_attempt<R: BackfillRow>(
    backlog: &Mutex<RegistrationBacklog<R>>,
    on_healed: Option<&BackfillHealedHook<R>>,
    metrics: &BackfillMetrics,
    table: &str,
    attempted: &[R],
    result: Result<(), ChError>,
) {
    match result {
        Ok(()) => {
            let removed = remove_matching_and_update_gauge(backlog, metrics, attempted);
            if let Some(hook) = on_healed
                && !removed.is_empty()
            {
                hook(&removed);
            }
            metrics
                .healed_total
                .fetch_add(removed.len() as u64, Ordering::Relaxed);
        }
        Err(ChError::InsertUncertain(msg)) => {
            // Terminal: commit fate unknown; not retried (#9 discipline).
            let removed = remove_matching_and_update_gauge(backlog, metrics, attempted);
            metrics
                .abandoned_total
                .fetch_add(removed.len() as u64, Ordering::Relaxed);
            warn!(
                table = %table,
                rows = removed.len(),
                error = %msg,
                "registration backfill re-insert outcome uncertain: commit fate unknown; \
                 not retried"
            );
        }
        Err(e) if e.is_retryable() => {
            // Pre-send retryable (the only retryable class that can reach
            // this task — see `writer::table::attempt_insert_with_retry`'s
            // doc comment): keep every entry for the next tick.
            metrics.retries_total.fetch_add(1, Ordering::Relaxed);
            warn!(
                table = %table,
                rows = attempted.len(),
                error = %e,
                "registration backfill re-insert failed with a retryable error; \
                 retrying next tick"
            );
        }
        Err(e) => {
            // Deterministic failure: abandon the attempted batch (no
            // poison spin); a poison-spool record of these rows exists iff
            // the generation's spool write succeeded (residual R5
            // otherwise).
            let removed = remove_matching_and_update_gauge(backlog, metrics, attempted);
            metrics
                .abandoned_total
                .fetch_add(removed.len() as u64, Ordering::Relaxed);
            warn!(
                table = %table,
                rows = removed.len(),
                error = %e,
                "registration backfill re-insert failed deterministically; abandoned"
            );
        }
    }
}

/// Version-checked compare-and-remove under the backlog lock, keeping
/// the pending gauge in sync. Returns the entries actually removed.
fn remove_matching_and_update_gauge<R: BackfillRow>(
    backlog: &Mutex<RegistrationBacklog<R>>,
    metrics: &BackfillMetrics,
    attempted: &[R],
) -> Vec<R> {
    let mut guard = backlog.lock().expect("registration backlog mutex poisoned");
    let removed = guard.remove_if_version(attempted);
    metrics.pending.store(guard.len() as u64, Ordering::Relaxed);
    removed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::registration::StreamKey;
    use crate::writer::rows::{LogStreamRow, MetricSeriesRow};

    fn row(fingerprint: u64, month: u16, updated_ns: i64) -> LogStreamRow {
        LogStreamRow {
            month,
            fingerprint,
            service: "svc".to_string(),
            labels: "{\"service_name\":\"svc\"}".to_string(),
            updated_ns,
        }
    }

    fn keys_of(rows: &[LogStreamRow]) -> Vec<StreamKey> {
        rows.iter().map(|r| (r.fingerprint, r.month)).collect()
    }

    #[test]
    fn keyed_dedup_keeps_the_larger_updated_ns() {
        let mut backlog = RegistrationBacklog::new(u64::MAX);
        let (accepted, dropped) = backlog.enqueue(&[row(1, 10, 100)]);
        assert_eq!((accepted, dropped), (1, 0));

        // A newer row for the same key replaces it (accepted, not dropped).
        let (accepted, dropped) = backlog.enqueue(&[row(1, 10, 200)]);
        assert_eq!((accepted, dropped), (1, 0));
        assert_eq!(backlog.len(), 1);
        assert_eq!(backlog.pending_rows()[0].updated_ns, 200);

        // A stale row for the same key is left in place (still accepted).
        let (accepted, dropped) = backlog.enqueue(&[row(1, 10, 150)]);
        assert_eq!((accepted, dropped), (1, 0));
        assert_eq!(backlog.len(), 1);
        assert_eq!(backlog.pending_rows()[0].updated_ns, 200);
    }

    #[test]
    fn distinct_months_for_the_same_fingerprint_are_distinct_entries() {
        let mut backlog = RegistrationBacklog::new(u64::MAX);
        backlog.enqueue(&[row(1, 10, 100), row(1, 11, 100)]);
        assert_eq!(backlog.len(), 2);
    }

    #[test]
    fn byte_cap_rejects_new_keys_and_reports_the_dropped_count() {
        let one_row_bytes = row(1, 10, 100).est_bytes();
        // Room for exactly one entry.
        let mut backlog = RegistrationBacklog::new(one_row_bytes);
        let (accepted, dropped) = backlog.enqueue(&[row(1, 10, 100), row(2, 10, 100)]);
        assert_eq!((accepted, dropped), (1, 1));
        assert_eq!(backlog.len(), 1);

        // The cap keeps rejecting new keys while full...
        let (accepted, dropped) = backlog.enqueue(&[row(3, 10, 100)]);
        assert_eq!((accepted, dropped), (0, 1));

        // ...but a replacement of the existing key is never cap-dropped.
        let (accepted, dropped) = backlog.enqueue(&[row(1, 10, 200)]);
        assert_eq!((accepted, dropped), (1, 0));
        assert_eq!(backlog.pending_rows()[0].updated_ns, 200);
    }

    #[test]
    fn remove_if_version_restores_byte_accounting_on_a_matched_version() {
        let one_row_bytes = row(1, 10, 100).est_bytes();
        let mut backlog = RegistrationBacklog::new(one_row_bytes);
        backlog.enqueue(&[row(1, 10, 100)]);
        // Full: a second key is rejected.
        assert_eq!(backlog.enqueue(&[row(2, 10, 100)]), (0, 1));

        assert_eq!(
            keys_of(&backlog.remove_if_version(&[row(1, 10, 100)])),
            vec![(1, 10)]
        );
        assert_eq!(backlog.len(), 0);
        // Bytes restored: the previously rejected key now fits.
        assert_eq!(backlog.enqueue(&[row(2, 10, 100)]), (1, 0));
        assert_eq!(backlog.len(), 1);
    }

    /// Issue #134 code-review fix: an attempt's completion must not evict
    /// a NEWER entry enqueued (larger `updated_ns`, replacing the
    /// attempted one) while the attempt was in flight — the compare
    /// fails, the newer row survives for the next tick, and nothing is
    /// reported removed (so no false heal/abandon count, no LRU
    /// promotion).
    #[test]
    fn remove_if_version_leaves_a_newer_entry_enqueued_mid_attempt() {
        let mut backlog = RegistrationBacklog::new(u64::MAX);
        let attempted = vec![row(1, 10, 100)];
        backlog.enqueue(&attempted);
        // A newer Poisoned flush replaces the entry while the attempt is
        // in flight.
        assert_eq!(backlog.enqueue(&[row(1, 10, 200)]), (1, 0));

        assert!(backlog.remove_if_version(&attempted).is_empty());
        assert_eq!(backlog.len(), 1, "the newer entry must survive");
        assert_eq!(backlog.pending_rows()[0].updated_ns, 200);

        // The newer version's own attempt removes it.
        assert_eq!(
            keys_of(&backlog.remove_if_version(&[row(1, 10, 200)])),
            vec![(1, 10)]
        );
        assert_eq!(backlog.len(), 0);
    }

    #[test]
    fn remove_if_version_of_an_absent_key_is_a_no_op() {
        let mut backlog = RegistrationBacklog::new(u64::MAX);
        backlog.enqueue(&[row(1, 10, 100)]);
        assert!(backlog.remove_if_version(&[row(9, 9, 100)]).is_empty());
        assert_eq!(backlog.len(), 1);
    }

    #[test]
    fn remove_if_version_returns_the_removed_entries_values() {
        // Issue #139: callers (the metadata heal hook) need the removed
        // ROW VALUES, not just keys.
        let mut backlog = RegistrationBacklog::new(u64::MAX);
        backlog.enqueue(&[row(1, 10, 100)]);
        let removed = backlog.remove_if_version(&[row(1, 10, 100)]);
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].service, "svc");
        assert_eq!(removed[0].updated_ns, 100);
    }

    #[test]
    fn len_tracks_enqueue_and_remove_mutations() {
        let mut backlog = RegistrationBacklog::new(u64::MAX);
        assert_eq!(backlog.len(), 0);
        backlog.enqueue(&[row(1, 10, 100), row(2, 10, 100)]);
        assert_eq!(backlog.len(), 2);
        backlog.remove_if_version(&[row(1, 10, 100)]);
        assert_eq!(backlog.len(), 1);
        backlog.remove_if_version(&[row(2, 10, 100)]);
        assert_eq!(backlog.len(), 0);
    }

    #[test]
    fn enqueue_failed_bumps_totals_and_the_pending_gauge() {
        let one_row_bytes = row(1, 10, 100).est_bytes();
        let backlog = Mutex::new(RegistrationBacklog::new(one_row_bytes));
        let metrics = BackfillMetrics::default();

        enqueue_failed(&backlog, &metrics, &[row(1, 10, 100), row(2, 10, 100)]);

        assert_eq!(metrics.enqueued_total.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.dropped_total.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.pending.load(Ordering::Relaxed), 1);
    }

    fn series_row(metric_name: &str, fingerprint: u64, bucket: i64) -> MetricSeriesRow {
        MetricSeriesRow {
            metric_name: metric_name.to_string(),
            fingerprint,
            unix_milli: bucket,
            labels: "{\"job\":\"checkout\"}".to_string(),
            value_type: 0,
        }
    }

    /// Issue #139 edge case 2: a versionless family's (constant-0) version
    /// makes the mid-attempt compare always-equal — the entry IS removed,
    /// which is safe because the key determines the full logical row (a
    /// "newer" enqueue carried byte-identical content).
    #[test]
    fn versionless_family_removal_degenerates_to_always_remove() {
        let mut backlog = RegistrationBacklog::new(u64::MAX);
        let attempted = vec![series_row("up", 1, 0)];
        backlog.enqueue(&attempted);
        // A re-enqueue mid-attempt for the same key is byte-identical
        // content ("accepted" but nothing to replace: version 0 == 0).
        assert_eq!(backlog.enqueue(&[series_row("up", 1, 0)]), (1, 0));
        assert_eq!(backlog.len(), 1);

        let removed = backlog.remove_if_version(&attempted);
        assert_eq!(removed.len(), 1, "the equal-version entry is removed");
        assert_eq!(backlog.len(), 0);
    }

    /// The versionless series key is the FULL logical identity: same
    /// `(name, fingerprint)` at a different bucket or `value_type` is a
    /// distinct entry, never a replacement.
    #[test]
    fn versionless_series_keys_distinguish_bucket_and_value_type() {
        let mut backlog = RegistrationBacklog::new(u64::MAX);
        let mut hist = series_row("up", 1, 0);
        hist.value_type = 1;
        backlog.enqueue(&[series_row("up", 1, 0), series_row("up", 1, 3_600_000), hist]);
        assert_eq!(backlog.len(), 3);
    }
}
