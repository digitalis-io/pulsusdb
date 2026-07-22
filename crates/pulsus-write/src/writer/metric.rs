//! `MetricWriter`: a generic per-table columnar writer core (issue #9's
//! architect plan, generalized), wired for the three metric tables
//! (`metric_samples`, `metric_series`, `metric_metadata` — docs/schemas.md
//! §2.1). Implements issue #26's [`crate::ingest::metrics::MetricSink`]
//! seam, paralleling [`crate::writer::LogWriter`]'s two-table shape with a
//! third flush task and two registration caches instead of one.
//!
//! **Consistency model** (architect plan amendments 1 & 2, "cross-table
//! atomicity"): `metric_samples`, `metric_series`, and `metric_metadata`
//! flush independently on three separate generations — there is no
//! cross-table atomic insert (the same eventual-consistency model
//! `LogWriter`'s module doc accepts for `log_samples`/`log_streams`, never
//! stronger). The `try_join_all` wait-join below guarantees the sync caller
//! never receives a *false success*: [`MetricSink::admit_flush`]'s `200`
//! (via the resolved [`crate::ingest::FlushWait`]) is returned only once
//! this admission's samples *and* series *and* metadata generations are all
//! durable, or it gets an `Err`. It does **not** make the three-table write
//! atomic to *other* readers: because the tables flush on independent
//! generations, a concurrent reader can observe a `metric_samples` row
//! durable **without** its `metric_series` row during the window between
//! the samples insert settling and the series insert settling-or-failing.
//! That transient cross-reader visibility window is legal (docs/schemas.md
//! §2.1's read-side `LIMIT 1 BY` already tolerates a missing/lagging
//! registration) and is part of the same accepted async/eventual-
//! consistency window as the log path's `log_samples` vs `log_streams` gap
//! — not a stronger cross-reader atomicity claim (architect plan amendment
//! 2, "Finding 1 wording precision").
//!
//! **Registration** (docs/schemas.md §2.1, architect plan): `SeriesLru`
//! promotion happens only after a confirmed `metric_series` flush (never
//! optimistically at admission), keyed `(metric_name, fingerprint,
//! bucket)` — metric-name-scoped, since `metric_fingerprint` excludes
//! `__name__` (see `writer::registration`'s doc comment for why a
//! name-less key would be a correctness bug). `MetadataCache` promotion is
//! likewise success-only, on a confirmed `metric_metadata` flush, and
//! records the *last emitted* `(metric_type, help, unit)` value per
//! `metric_name` so a value that changes and later reverts (A→B→A)
//! re-emits rather than being permanently suppressed.
//!
//! **Backpressure/shutdown**: identical shape to `LogWriter`'s — see its
//! module doc for the byte-reservation and drain/force-settle semantics,
//! generalized here to three tables sharing one `queued_bytes` counter.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pulsus_clickhouse::ChClient;
use pulsus_config::WriterConfig;
use pulsus_model::floor_to_activity_bucket;
use tokio::sync::{Notify, oneshot};
use tracing::warn;

use crate::error::LogsIngestError;
use crate::ingest::metrics::{MetricMetadata, MetricSink, ParsedMetrics, SeriesRef};
use crate::ingest::{Backpressure, FlushWait};
use crate::writer::backfill::{self, BackfillHealedHook, RegistrationBacklog};
use crate::writer::buffer;
use crate::writer::config::WriterRuntime;
use crate::writer::error::WriteError;
use crate::writer::metrics::{MetricWriterMetrics, MetricWriterMetricsSnapshot};
use crate::writer::registration::{MetadataCache, SeriesKey, SeriesLru};
use crate::writer::rows::{
    MetricHistSampleRow, MetricMetadataRow, MetricSampleRow, MetricSeriesRow,
};
use crate::writer::spool;
use crate::writer::table::{self, BlockInserter, ChBlockInserter, ShutdownSignal, TableContext};

const SAMPLES_TABLE: &str = "metric_samples";
const SERIES_TABLE: &str = "metric_series";
/// `metric_metadata` is a global catalog table (catalog id 3, `family:
/// None`) — it never carries a `_dist` suffix, unlike `metric_samples`/
/// `metric_series` (docs/schemas.md §7).
const METADATA_TABLE: &str = "metric_metadata";
/// `metric_hist_samples` (catalog id 23, M7-A4 issue #120) — a Metrics-family
/// per-shard table, co-sharded with `metric_samples`, so it carries a `_dist`
/// suffix in cluster mode exactly like `metric_samples`/`metric_series`.
const HIST_SAMPLES_TABLE: &str = "metric_hist_samples";

/// The `metric_series.value_type` discriminant for a float sample.
const VALUE_TYPE_FLOAT: u8 = 0;
/// The `metric_series.value_type` discriminant for a native-histogram sample.
const VALUE_TYPE_HISTOGRAM: u8 = 1;

/// The three target table names a [`MetricWriter`] inserts into (docs/
/// schemas.md §2.1, mirroring [`crate::writer::WriterTables`]'s issue #15
/// `_dist`-awareness): cluster-mode deployments write `metric_samples`/
/// `metric_series` through their `_dist` wrappers, but `metric_metadata`
/// NEVER carries one (it is a global/replicated catalog table). `Arc<str>`
/// (not `&'static str`): the cluster-suffixed names are computed once at
/// server startup from `Config`, not known at compile time.
#[derive(Debug, Clone)]
pub struct MetricWriterTables {
    pub samples: Arc<str>,
    pub series: Arc<str>,
    pub metadata: Arc<str>,
    /// `metric_hist_samples` (M7-A4, issue #120) — `_dist`-aware like
    /// `samples`/`series` (a co-sharded Metrics-family table).
    pub hist_samples: Arc<str>,
}

impl MetricWriterTables {
    /// Unclustered defaults: the bare local table names. Every existing
    /// caller (`new`/`with_inserters`) delegates here so single-node
    /// behavior is the default.
    pub fn metrics_default() -> Self {
        MetricWriterTables {
            samples: Arc::from(SAMPLES_TABLE),
            series: Arc::from(SERIES_TABLE),
            metadata: Arc::from(METADATA_TABLE),
            hist_samples: Arc::from(HIST_SAMPLES_TABLE),
        }
    }
}

struct Shared {
    samples: Arc<buffer::TableBuffer<MetricSampleRow>>,
    series: Arc<buffer::TableBuffer<MetricSeriesRow>>,
    metadata: Arc<buffer::TableBuffer<MetricMetadataRow>>,
    hist_samples: Arc<buffer::TableBuffer<MetricHistSampleRow>>,
    samples_notify: Arc<Notify>,
    series_notify: Arc<Notify>,
    metadata_notify: Arc<Notify>,
    hist_samples_notify: Arc<Notify>,
    queued_bytes: Arc<AtomicU64>,
    runtime: Arc<WriterRuntime>,
    metrics: Arc<MetricWriterMetrics>,
    series_lru: Arc<Mutex<SeriesLru>>,
    metadata_cache: Arc<Mutex<MetadataCache>>,
    /// The `metric_series` activity-bucket width in milliseconds
    /// (`pulsus_config::ReaderConfig::series_activity_bucket`, resolved by
    /// the caller — not read from `WriterConfig`, docs/schemas.md §2.1).
    bucket_ms: i64,
    shutdown: ShutdownSignal,
    shutting_down: AtomicBool,
    samples_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    series_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    metadata_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    hist_samples_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    series_backfill_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    metadata_backfill_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// The production `metric_metadata` heal hook (issue #139) — the ONE hook
/// body the writer wires (`with_inserters_with_tables` calls exactly
/// this); extracted so the eviction-interleaving regression test (M7a)
/// drives the real hook, not a reimplementation.
///
/// **Invalidate-only by design (#139): a heal must never install a
/// descriptor.** The backfill task confirms out of order with the flush
/// task, and after an LRU eviction there is no stored version left to
/// gate on — any upsert-like hook (plain or version-gated) would install
/// a stale descriptor A over the durable winner B and permanently
/// suppress a client revert to A while `ReplacingMergeTree(updated_ns)`
/// serves B. Invalidation is unconditionally sound under every
/// interleaving: post-heal the cache asserts nothing for the name, so the
/// next admission's decision degrades to *emit* — the safe direction; a
/// redundant row collapses under `ReplacingMergeTree(updated_ns)`.
pub(crate) fn metadata_healed_hook(
    cache: Arc<Mutex<MetadataCache>>,
) -> BackfillHealedHook<MetricMetadataRow> {
    Arc::new(move |rows: &[MetricMetadataRow]| {
        let mut guard = cache.lock().expect("metadata cache mutex poisoned");
        for row in rows {
            let name: Arc<str> = Arc::from(row.metric_name.as_str());
            guard.invalidate(&name);
        }
    })
}

/// Implements issue #26's `MetricSink` over a generic per-table columnar
/// writer core. See the module-level docs above.
pub struct MetricWriter {
    shared: Arc<Shared>,
}

impl MetricWriter {
    /// Production constructor: batches and flushes through a real
    /// ClickHouse connection, against the unclustered default table names
    /// ([`MetricWriterTables::metrics_default`]). `bucket_ms` is the
    /// `metric_series` activity-bucket width (docs/schemas.md §2.1), the
    /// caller's job to resolve from config — not read from `WriterConfig`.
    pub fn new(client: Arc<ChClient>, cfg: &WriterConfig, bucket_ms: i64) -> Self {
        Self::new_with_tables(
            client,
            cfg,
            bucket_ms,
            MetricWriterTables::metrics_default(),
        )
    }

    /// [`Self::new`], but against `tables` — the server's cluster-aware
    /// constructor for `_dist` table names (docs/schemas.md §7).
    pub fn new_with_tables(
        client: Arc<ChClient>,
        cfg: &WriterConfig,
        bucket_ms: i64,
        tables: MetricWriterTables,
    ) -> Self {
        let inserter: Arc<ChBlockInserter> = Arc::new(ChBlockInserter::new(client));
        Self::with_inserters_with_tables(
            inserter.clone(),
            inserter.clone(),
            inserter.clone(),
            inserter,
            cfg,
            bucket_ms,
            tables,
        )
    }

    /// Test/mock constructor: any [`BlockInserter`] quadruple — e.g. a
    /// scriptable mock that can fail/hang on demand — against the
    /// unclustered default table names.
    pub fn with_inserters(
        samples_inserter: Arc<dyn BlockInserter<MetricSampleRow>>,
        series_inserter: Arc<dyn BlockInserter<MetricSeriesRow>>,
        metadata_inserter: Arc<dyn BlockInserter<MetricMetadataRow>>,
        hist_samples_inserter: Arc<dyn BlockInserter<MetricHistSampleRow>>,
        cfg: &WriterConfig,
        bucket_ms: i64,
    ) -> Self {
        Self::with_inserters_with_tables(
            samples_inserter,
            series_inserter,
            metadata_inserter,
            hist_samples_inserter,
            cfg,
            bucket_ms,
            MetricWriterTables::metrics_default(),
        )
    }

    /// [`Self::with_inserters`], but against `tables`.
    pub fn with_inserters_with_tables(
        samples_inserter: Arc<dyn BlockInserter<MetricSampleRow>>,
        series_inserter: Arc<dyn BlockInserter<MetricSeriesRow>>,
        metadata_inserter: Arc<dyn BlockInserter<MetricMetadataRow>>,
        hist_samples_inserter: Arc<dyn BlockInserter<MetricHistSampleRow>>,
        cfg: &WriterConfig,
        bucket_ms: i64,
        tables: MetricWriterTables,
    ) -> Self {
        // Config-validated by `pulsus_config::validate` (issue #26 open
        // question #4) — a non-positive bucket would make
        // `floor_to_activity_bucket`'s own `debug_assert!` fire, or divide
        // by zero in a release build; catching it here too keeps the
        // invariant visible at the writer's own construction boundary.
        debug_assert!(bucket_ms >= 1, "bucket_ms must be >= 1");

        let runtime = Arc::new(WriterRuntime::from_config(cfg));
        let metrics = Arc::new(MetricWriterMetrics::default());
        let queued_bytes = Arc::new(AtomicU64::new(0));
        let spool = Arc::new(spool::SpoolWriter::new(
            runtime.spool_dir.clone(),
            metrics.clone(),
        ));
        let (shutdown, shutdown_rx) = ShutdownSignal::new();
        let series_lru = Arc::new(Mutex::new(SeriesLru::new(runtime.lru_capacity)));
        let metadata_cache = Arc::new(Mutex::new(MetadataCache::new(
            runtime.metadata_lru_capacity,
        )));

        let samples = Arc::new(buffer::TableBuffer::new());
        let series = Arc::new(buffer::TableBuffer::new());
        let metadata = Arc::new(buffer::TableBuffer::new());
        let hist_samples = Arc::new(buffer::TableBuffer::new());
        let samples_notify = Arc::new(Notify::new());
        let series_notify = Arc::new(Notify::new());
        let metadata_notify = Arc::new(Notify::new());
        let hist_samples_notify = Arc::new(Notify::new());

        // `metric_series`'s success-only LRU promotion (architect plan
        // amendment 1): populated ONLY here, after a confirmed flush —
        // never optimistically at admission. Reconstructing `Arc<str>` from
        // the row's `String` is an allocation, but only on the (rare, by
        // construction) path of a *newly confirmed* registration, not the
        // per-sample hot path.
        let series_lru_for_hook = series_lru.clone();
        let on_series_flush_success: table::FlushSuccessHook<MetricSeriesRow> =
            Arc::new(move |rows: &[MetricSeriesRow]| {
                let mut guard = series_lru_for_hook
                    .lock()
                    .expect("series lru mutex poisoned");
                for row in rows {
                    let key: SeriesKey = (
                        Arc::from(row.metric_name.as_str()),
                        row.fingerprint,
                        row.unix_milli,
                        row.value_type,
                    );
                    guard.insert(key);
                }
            });

        // `metric_metadata`'s success-only last-value promotion (architect
        // plan amendment 1, finding 2): populated ONLY here, after a
        // confirmed flush. The single flush task confirms in admission
        // order (monotone `updated_ns` per name), so this unconditional
        // upsert is safe; the backfill task — the only out-of-order
        // confirmer — never upserts (issue #139: `metadata_healed_hook`
        // is invalidate-only).
        let metadata_cache_for_hook = metadata_cache.clone();
        let on_metadata_flush_success: table::FlushSuccessHook<MetricMetadataRow> =
            Arc::new(move |rows: &[MetricMetadataRow]| {
                let mut guard = metadata_cache_for_hook
                    .lock()
                    .expect("metadata cache mutex poisoned");
                for row in rows {
                    let key: Arc<str> = Arc::from(row.metric_name.as_str());
                    guard.upsert(
                        key,
                        (row.metric_type.clone(), row.help.clone(), row.unit.clone()),
                    );
                }
            });

        // Poisoned-only registration backfill for `metric_series` and
        // `metric_metadata` (issue #139, extending #134's log-family
        // mechanism): a definitely-failed registration flush enqueues its
        // rows for the 5s re-insert cadence. `metric_samples`/
        // `metric_hist_samples` keep `on_flush_poisoned: None` — the
        // structural append-only #9 exclusion.
        let series_backlog = Arc::new(Mutex::new(RegistrationBacklog::<MetricSeriesRow>::new(
            runtime.backfill_max_bytes,
        )));
        let series_backlog_for_hook = series_backlog.clone();
        let series_backfill_metrics = metrics.series_backfill.clone();
        let on_series_flush_poisoned: table::FlushPoisonedHook<MetricSeriesRow> =
            Arc::new(move |rows: &[MetricSeriesRow]| {
                backfill::enqueue_failed(&series_backlog_for_hook, &series_backfill_metrics, rows);
            });

        let metadata_backlog = Arc::new(Mutex::new(RegistrationBacklog::<MetricMetadataRow>::new(
            runtime.backfill_max_bytes,
        )));
        let metadata_backlog_for_hook = metadata_backlog.clone();
        let metadata_backfill_metrics = metrics.metadata_backfill.clone();
        let on_metadata_flush_poisoned: table::FlushPoisonedHook<MetricMetadataRow> =
            Arc::new(move |rows: &[MetricMetadataRow]| {
                backfill::enqueue_failed(
                    &metadata_backlog_for_hook,
                    &metadata_backfill_metrics,
                    rows,
                );
            });

        // Success-only `SeriesLru` promotion on a confirmed HEAL — safe
        // under any eviction interleaving: a pure membership set over the
        // FULL logical identity `(metric_name, fingerprint, bucket,
        // value_type)`, so promoting on heal asserts only "this row is
        // durable" (unconditionally true at that point); there is no
        // value to go stale. Same cold-path `Arc::from` allocation note
        // as the flush-success hook above.
        let series_lru_for_heal = series_lru.clone();
        let on_series_healed: BackfillHealedHook<MetricSeriesRow> =
            Arc::new(move |rows: &[MetricSeriesRow]| {
                let mut guard = series_lru_for_heal
                    .lock()
                    .expect("series lru mutex poisoned");
                for row in rows {
                    let key: SeriesKey = (
                        Arc::from(row.metric_name.as_str()),
                        row.fingerprint,
                        row.unix_milli,
                        row.value_type,
                    );
                    guard.insert(key);
                }
            });

        // Clone the inserter/table Arcs the backfill tasks need before
        // the ctxs below move them.
        let series_inserter_for_backfill = series_inserter.clone();
        let series_table_for_backfill = tables.series.clone();
        let metadata_inserter_for_backfill = metadata_inserter.clone();
        let metadata_table_for_backfill = tables.metadata.clone();

        let samples_ctx = TableContext {
            table: tables.samples,
            buffer: samples.clone(),
            notify: samples_notify.clone(),
            inserter: samples_inserter,
            runtime: runtime.clone(),
            table_metrics: metrics.samples.clone(),
            spool: spool.clone(),
            queued_bytes: queued_bytes.clone(),
            on_flush_success: None,
            on_flush_poisoned: None,
        };
        let series_ctx = TableContext {
            table: tables.series,
            buffer: series.clone(),
            notify: series_notify.clone(),
            inserter: series_inserter,
            runtime: runtime.clone(),
            table_metrics: metrics.series.clone(),
            spool: spool.clone(),
            queued_bytes: queued_bytes.clone(),
            on_flush_success: Some(on_series_flush_success),
            on_flush_poisoned: Some(on_series_flush_poisoned),
        };
        let metadata_ctx = TableContext {
            table: tables.metadata,
            buffer: metadata.clone(),
            notify: metadata_notify.clone(),
            inserter: metadata_inserter,
            runtime: runtime.clone(),
            table_metrics: metrics.metadata.clone(),
            spool: spool.clone(),
            queued_bytes: queued_bytes.clone(),
            on_flush_success: Some(on_metadata_flush_success),
            on_flush_poisoned: Some(on_metadata_flush_poisoned),
        };
        // `metric_hist_samples` (M7-A4, issue #120): no flush-success hook —
        // `metric_series` registration (the only success-gated cache) is
        // driven at admission from BOTH float samples and hist samples, and
        // is promoted via the `metric_series` flush hook above, not this
        // table's flush.
        let hist_samples_ctx = TableContext {
            table: tables.hist_samples,
            buffer: hist_samples.clone(),
            notify: hist_samples_notify.clone(),
            inserter: hist_samples_inserter,
            runtime: runtime.clone(),
            table_metrics: metrics.hist_samples.clone(),
            spool: spool.clone(),
            queued_bytes: queued_bytes.clone(),
            on_flush_success: None,
            on_flush_poisoned: None,
        };

        let samples_task = table::spawn(samples_ctx, shutdown_rx.clone());
        let series_task = table::spawn(series_ctx, shutdown_rx.clone());
        let metadata_task = table::spawn(metadata_ctx, shutdown_rx.clone());
        let hist_samples_task = table::spawn(hist_samples_ctx, shutdown_rx.clone());
        let series_backfill_task = backfill::spawn_backfill(
            series_backlog,
            series_inserter_for_backfill,
            series_table_for_backfill,
            Some(on_series_healed),
            metrics.series_backfill.clone(),
            runtime.clone(),
            shutdown_rx.clone(),
        );
        let metadata_backfill_task = backfill::spawn_backfill(
            metadata_backlog,
            metadata_inserter_for_backfill,
            metadata_table_for_backfill,
            Some(metadata_healed_hook(metadata_cache.clone())),
            metrics.metadata_backfill.clone(),
            runtime.clone(),
            shutdown_rx,
        );

        let shared = Arc::new(Shared {
            samples,
            series,
            metadata,
            hist_samples,
            samples_notify,
            series_notify,
            metadata_notify,
            hist_samples_notify,
            queued_bytes,
            runtime,
            metrics,
            series_lru,
            metadata_cache,
            bucket_ms,
            shutdown,
            shutting_down: AtomicBool::new(false),
            samples_task: Mutex::new(Some(samples_task)),
            series_task: Mutex::new(Some(series_task)),
            metadata_task: Mutex::new(Some(metadata_task)),
            hist_samples_task: Mutex::new(Some(hist_samples_task)),
            series_backfill_task: Mutex::new(Some(series_backfill_task)),
            metadata_backfill_task: Mutex::new(Some(metadata_backfill_task)),
        });

        MetricWriter { shared }
    }

    /// Admits `batch`, appending to the samples/series/metadata buffers
    /// under one atomic byte reservation. `with_waiters` selects sync- vs
    /// async-mode admission, mirroring `LogWriter::admit_batch`.
    fn admit_batch(
        &self,
        batch: ParsedMetrics,
        with_waiters: bool,
    ) -> Result<Vec<oneshot::Receiver<Result<(), WriteError>>>, Backpressure> {
        if self.shared.shutting_down.load(Ordering::Acquire) {
            return Err(Backpressure);
        }

        self.shared
            .metrics
            .collisions_total
            .fetch_add(batch.collisions, Ordering::Relaxed);
        self.shared
            .metrics
            .rejected_total
            .fetch_add(batch.rejected, Ordering::Relaxed);

        // Reserve-before-materialize (mirrors `LogWriter::admit_batch`):
        // estimate bytes and decide which series/metadata are cache misses
        // *before* cloning/canonicalizing anything into the target row
        // shapes.
        let sample_bytes: u64 = batch
            .samples
            .iter()
            .map(MetricSampleRow::est_source_bytes)
            .sum();
        let hist_sample_bytes: u64 = batch
            .hist_samples
            .iter()
            .map(MetricHistSampleRow::est_source_bytes)
            .sum();

        // An exact `(metric_name, fingerprint) -> &SeriesRef` index, built
        // once per admission and consulted per touched bucket (architect
        // plan, "Data flow"). One `SeriesRef` serves whichever of the float
        // and histogram samples reference that `(metric_name, fingerprint)`.
        let series_by_key: HashMap<(&str, u64), &SeriesRef> = batch
            .series
            .iter()
            .map(|s| ((s.metric_name.as_ref(), s.fingerprint), s))
            .collect();

        // Cross-bucket-in-one-request rule (docs/schemas.md §2.1, edge case
        // 4): buckets are derived per-*sample*, not per-series, so a
        // backfilled/straddling request emits one `metric_series` row per
        // touched `(metric_name, fingerprint, bucket, value_type)`. Both
        // float samples (`value_type = 0`) and native-histogram samples
        // (`value_type = 1`, M7-A4 issue #120) drive registration; the
        // `value_type`-extended key means a series carrying both a float and
        // a histogram sample in one bucket registers BOTH rows.
        let mut seen_in_request: HashSet<SeriesKey> = HashSet::new();
        let mut new_series: Vec<(&SeriesRef, i64, u8)> = Vec::new();
        {
            let mut lru = self
                .shared
                .series_lru
                .lock()
                .expect("series lru mutex poisoned");
            let float_keys = batch.samples.iter().map(|s| {
                (
                    &s.metric_name,
                    s.fingerprint,
                    s.unix_milli,
                    VALUE_TYPE_FLOAT,
                )
            });
            let hist_keys = batch.hist_samples.iter().map(|h| {
                (
                    &h.metric_name,
                    h.fingerprint,
                    h.unix_milli,
                    VALUE_TYPE_HISTOGRAM,
                )
            });
            for (metric_name, fingerprint, unix_milli, value_type) in float_keys.chain(hist_keys) {
                let bucket = floor_to_activity_bucket(unix_milli, self.shared.bucket_ms);
                let key: SeriesKey = (metric_name.clone(), fingerprint, bucket, value_type);
                if !seen_in_request.insert(key.clone()) {
                    continue; // already queued by an earlier sample this request
                }
                if lru.contains(&key) {
                    self.shared
                        .metrics
                        .series_lru_hits_total
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                self.shared
                    .metrics
                    .series_lru_misses_total
                    .fetch_add(1, Ordering::Relaxed);
                let Some(series_ref) = series_by_key
                    .get(&(metric_name.as_ref(), fingerprint))
                    .copied()
                else {
                    // The receiver's contract requires a `SeriesRef` for
                    // every distinct series a request's samples touch — the
                    // writer never panics on a caller-side contract
                    // violation, it just cannot register a series it was
                    // never told the labels of. The sample is still
                    // admitted below.
                    continue;
                };
                new_series.push((series_ref, bucket, value_type));
            }
        }
        let series_bytes: u64 = new_series
            .iter()
            .map(|(s, _, _)| MetricSeriesRow::est_source_bytes(s))
            .sum();

        // `metric_metadata`: local-dedup (last occurrence per metric_name
        // wins within one request), then gate on the last-*emitted* value
        // (architect plan amendment 1, finding 2) — emit iff it differs
        // from what `MetadataCache` last confirmed-flushed for this name.
        let mut last_by_name: HashMap<&Arc<str>, &MetricMetadata> = HashMap::new();
        for meta in &batch.metadata {
            last_by_name.insert(&meta.metric_name, meta);
        }
        let mut new_metadata: Vec<&MetricMetadata> = Vec::new();
        {
            let cache = self
                .shared
                .metadata_cache
                .lock()
                .expect("metadata cache mutex poisoned");
            for meta in last_by_name.into_values() {
                let emit = match cache.get(&meta.metric_name) {
                    Some((t, h, u)) => t != &meta.metric_type || h != &meta.help || u != &meta.unit,
                    None => true,
                };
                if emit {
                    new_metadata.push(meta);
                }
            }
        }
        let metadata_bytes: u64 = new_metadata
            .iter()
            .map(|m| MetricMetadataRow::est_source_bytes(m))
            .sum();

        let total_bytes = sample_bytes + series_bytes + metadata_bytes + hist_sample_bytes;

        // Atomic reservation (mirrors `LogWriter::admit_batch`): reserve
        // first, roll back on overflow.
        super::reserve_queued_bytes(
            &self.shared.queued_bytes,
            &self.shared.metrics.backpressure_total,
            total_bytes,
            self.shared.runtime.queue_bytes_limit,
        )?;

        if self.shared.shutting_down.load(Ordering::Acquire) {
            self.shared
                .queued_bytes
                .fetch_sub(total_bytes, Ordering::AcqRel);
            return Err(Backpressure);
        }

        // Reservation secured: only now materialize the target rows.
        let sample_rows: Vec<MetricSampleRow> =
            batch.samples.iter().map(MetricSampleRow::from).collect();
        let series_rows: Vec<MetricSeriesRow> = new_series
            .iter()
            .map(|(s, bucket, value_type)| {
                MetricSeriesRow::from_series_at_bucket(s, *bucket, *value_type)
            })
            .collect();
        let metadata_rows: Vec<MetricMetadataRow> = new_metadata
            .iter()
            .map(|m| MetricMetadataRow::from(*m))
            .collect();
        let hist_sample_rows: Vec<MetricHistSampleRow> = batch
            .hist_samples
            .iter()
            .map(MetricHistSampleRow::from)
            .collect();

        let mut receivers = Vec::new();

        if !sample_rows.is_empty() {
            if with_waiters {
                let (should_notify, rx) = self.shared.samples.append_and_wait(
                    sample_rows,
                    sample_bytes,
                    self.shared.runtime.batch_bytes,
                );
                receivers.push(rx);
                if should_notify {
                    self.shared.samples_notify.notify_one();
                }
            } else if self.shared.samples.append(
                sample_rows,
                sample_bytes,
                self.shared.runtime.batch_bytes,
            ) {
                self.shared.samples_notify.notify_one();
            }
        }

        if !series_rows.is_empty() {
            self.shared
                .metrics
                .series_registrations_total
                .fetch_add(series_rows.len() as u64, Ordering::Relaxed);
            if with_waiters {
                let (should_notify, rx) = self.shared.series.append_and_wait(
                    series_rows,
                    series_bytes,
                    self.shared.runtime.batch_bytes,
                );
                receivers.push(rx);
                if should_notify {
                    self.shared.series_notify.notify_one();
                }
            } else if self.shared.series.append(
                series_rows,
                series_bytes,
                self.shared.runtime.batch_bytes,
            ) {
                self.shared.series_notify.notify_one();
            }
        }

        if !metadata_rows.is_empty() {
            self.shared
                .metrics
                .metadata_upserts_total
                .fetch_add(metadata_rows.len() as u64, Ordering::Relaxed);
            if with_waiters {
                let (should_notify, rx) = self.shared.metadata.append_and_wait(
                    metadata_rows,
                    metadata_bytes,
                    self.shared.runtime.batch_bytes,
                );
                receivers.push(rx);
                if should_notify {
                    self.shared.metadata_notify.notify_one();
                }
            } else if self.shared.metadata.append(
                metadata_rows,
                metadata_bytes,
                self.shared.runtime.batch_bytes,
            ) {
                self.shared.metadata_notify.notify_one();
            }
        }

        if !hist_sample_rows.is_empty() {
            if with_waiters {
                let (should_notify, rx) = self.shared.hist_samples.append_and_wait(
                    hist_sample_rows,
                    hist_sample_bytes,
                    self.shared.runtime.batch_bytes,
                );
                receivers.push(rx);
                if should_notify {
                    self.shared.hist_samples_notify.notify_one();
                }
            } else if self.shared.hist_samples.append(
                hist_sample_rows,
                hist_sample_bytes,
                self.shared.runtime.batch_bytes,
            ) {
                self.shared.hist_samples_notify.notify_one();
            }
        }

        Ok(receivers)
    }

    /// A point-in-time metrics snapshot.
    pub fn metrics(&self) -> MetricWriterMetricsSnapshot {
        self.shared
            .metrics
            .snapshot(self.shared.queued_bytes.load(Ordering::Relaxed))
    }

    /// Graceful shutdown, mirroring [`crate::writer::LogWriter::shutdown`]
    /// generalized to three flush tasks: stops admitting immediately
    /// (subsequent `admit`/`admit_flush` calls return `Backpressure`), then
    /// drains every open/in-flight generation up to `deadline`. Idempotent.
    pub async fn shutdown(&self, deadline: Duration) {
        self.shared.shutting_down.store(true, Ordering::Release);
        self.shared.shutdown.begin(Instant::now() + deadline);

        let samples_task = self
            .shared
            .samples_task
            .lock()
            .expect("task handle mutex poisoned")
            .take();
        let series_task = self
            .shared
            .series_task
            .lock()
            .expect("task handle mutex poisoned")
            .take();
        let metadata_task = self
            .shared
            .metadata_task
            .lock()
            .expect("task handle mutex poisoned")
            .take();
        let hist_samples_task = self
            .shared
            .hist_samples_task
            .lock()
            .expect("task handle mutex poisoned")
            .take();
        let series_backfill_task = self
            .shared
            .series_backfill_task
            .lock()
            .expect("task handle mutex poisoned")
            .take();
        let metadata_backfill_task = self
            .shared
            .metadata_backfill_task
            .lock()
            .expect("task handle mutex poisoned")
            .take();

        if let Some(task) = samples_task
            && let Err(e) = task.await
        {
            warn!(error = %e, table = SAMPLES_TABLE, "flush task panicked during shutdown");
        }
        if let Some(task) = series_task
            && let Err(e) = task.await
        {
            warn!(error = %e, table = SERIES_TABLE, "flush task panicked during shutdown");
        }
        if let Some(task) = metadata_task
            && let Err(e) = task.await
        {
            warn!(error = %e, table = METADATA_TABLE, "flush task panicked during shutdown");
        }
        if let Some(task) = hist_samples_task
            && let Err(e) = task.await
        {
            warn!(error = %e, table = HIST_SAMPLES_TABLE, "flush task panicked during shutdown");
        }
        if let Some(task) = series_backfill_task
            && let Err(e) = task.await
        {
            warn!(
                error = %e,
                table = SERIES_TABLE,
                "registration backfill task panicked during shutdown"
            );
        }
        if let Some(task) = metadata_backfill_task
            && let Err(e) = task.await
        {
            warn!(
                error = %e,
                table = METADATA_TABLE,
                "registration backfill task panicked during shutdown"
            );
        }
    }
}

impl MetricSink for MetricWriter {
    fn admit(&self, batch: ParsedMetrics) -> Result<(), Backpressure> {
        self.admit_batch(batch, false).map(|_| ())
    }

    fn admit_flush(&self, batch: ParsedMetrics) -> Result<FlushWait, Backpressure> {
        let receivers = self.admit_batch(batch, true)?;
        Ok(FlushWait::new(async move {
            super::join_generations(receivers)
                .await
                .map_err(|e| LogsIngestError::FlushFailed(e.to_string()))
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata_row(name: &str, metric_type: &str, updated_ns: i64) -> MetricMetadataRow {
        MetricMetadataRow {
            metric_name: name.to_string(),
            metric_type: metric_type.to_string(),
            help: String::new(),
            unit: String::new(),
            updated_ns,
        }
    }

    /// Issue #139 M7a (plan v3 delta 2) — the eviction-resurrection
    /// regression, pinned against the REAL production hook body
    /// ([`metadata_healed_hook`]) at the component seam (writer-level
    /// capacity is the documented 1M constant, so the interleaving is
    /// forced with `MetadataCache::new(1)`):
    ///
    /// confirm newer B@2 for name X (`upsert`) → force X's eviction by
    /// upserting a second name Y (capacity 1) → the heal of stale A@1
    /// fires through the real hook → `get(X)` MUST be `None` (the
    /// emission decision given `None` is *emit* — B re-emitted on the
    /// next push, stale A never installed).
    ///
    /// Fails under ANY upsert-like reverted hook: with B evicted there is
    /// no stored version to gate on, so both a plain upsert and a
    /// version-gated upsert (whose gate is vacuous after eviction) would
    /// install stale A → `get(X) == Some(A)` → the `None` assertion
    /// fails.
    #[test]
    fn m7a_heal_after_eviction_never_installs_the_stale_descriptor() {
        let cache = Arc::new(Mutex::new(MetadataCache::new(1)));
        let x: Arc<str> = Arc::from("metric_x");
        let y: Arc<str> = Arc::from("metric_y");

        {
            let mut guard = cache.lock().expect("metadata cache mutex poisoned");
            // Newer B@2 confirmed for X (flush-success path).
            guard.upsert(
                x.clone(),
                ("gauge".to_string(), "B".to_string(), String::new()),
            );
            // Capacity-1 pressure: Y's confirmation evicts X.
            guard.upsert(
                y.clone(),
                ("counter".to_string(), String::new(), String::new()),
            );
            assert_eq!(guard.get(&x), None, "precondition: X evicted");
        }

        // The stale A@1 row heals through the REAL production hook.
        let hook = metadata_healed_hook(cache.clone());
        hook(&[metadata_row("metric_x", "counter", 1)]);

        let guard = cache.lock().expect("metadata cache mutex poisoned");
        assert_eq!(
            guard.get(&x),
            None,
            "a heal must never install a descriptor — stale A would suppress a client \
             revert while ReplacingMergeTree(updated_ns) serves B"
        );
        // The admission emission-decision given `None` is *emit*: the
        // next push of B (or anything) for X re-emits a redundant,
        // RMT-collapsed row — the safe direction.
        assert!(guard.get(&y).is_some(), "unrelated entries untouched");
    }

    /// Companion pin: the hook invalidates a RESIDENT entry too (the
    /// heal's own name), never rewrites it — the resident-interleaving
    /// writer-level counterpart is M7b in `tests/metric_writer.rs`.
    #[test]
    fn metadata_healed_hook_invalidates_a_resident_entry_and_never_writes() {
        let cache = Arc::new(Mutex::new(MetadataCache::new(10)));
        let x: Arc<str> = Arc::from("metric_x");
        cache.lock().expect("metadata cache mutex poisoned").upsert(
            x.clone(),
            ("gauge".to_string(), "B".to_string(), String::new()),
        );

        let hook = metadata_healed_hook(cache.clone());
        hook(&[metadata_row("metric_x", "counter", 1)]);

        let guard = cache.lock().expect("metadata cache mutex poisoned");
        assert_eq!(
            guard.get(&x),
            None,
            "resident entry invalidated, not rewritten"
        );
        assert!(guard.is_empty());
    }
}
