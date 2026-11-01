//! `TraceWriter`: one push is one `INSERT` of one block into
//! `trace_landing` (issues #586 and #602), the shape the log and
//! metric writers have. Implements [`crate::ingest::traces::TraceSink`].
//!
//! Eight materialized views maintain the tables traces are read from, and
//! this writer inserts into none of them. The loop, its two fates, the
//! budget, the shutdown boundary, the byte accounting and the spool copy are
//! `writer::landing`'s; what traces decides for itself is in
//! [`crate::writer::trace_landing`].
//!
//! **The acknowledgement is the landing insert's.** A sync push is answered
//! once its block's insert has committed, or with the error that ended it; a
//! claim (issue #494) records the block as its one target, so a client's
//! retry of a push whose insert failed is stored rather than suppressed.

use std::sync::Arc;
use std::time::Duration;

use pulsus_clickhouse::ChClient;
use pulsus_config::WriterConfig;
use pulsus_model::Tenant;
use tokio::sync::oneshot;

use crate::error::LogsIngestError;
use crate::ingest::traces::{ParsedTraceLanding, TraceSink};
use crate::ingest::{AdmitRefusal, FlushWait, PushHeaders};
use crate::writer::config::WriterRuntime;
use crate::writer::drain::DrainBoundary;
use crate::writer::metrics::{TraceWriterMetrics, TraceWriterMetricsSnapshot};
use crate::writer::push_dedup::PushDedup;
use crate::writer::push_dedup::{self, Admission, ClaimTicket};
use crate::writer::rows::TraceLandingRow;
use crate::writer::spool;
use crate::writer::table::BlockInserter;
use crate::writer::trace_landing::{self, TraceLandingPath};
use crate::writer::{AdmitMode, Admitted, Suppressed, note_target};

/// The one table a [`TraceWriter`] inserts into. No `_dist` wrapper in any
/// mode: a push carries many trace ids, so inserting the push itself through
/// one would split one push per shard. The routing happens one step later,
/// on the way out of the per-trace views.
#[derive(Debug, Clone)]
pub struct TraceWriterTables {
    pub landing: Arc<str>,
}

impl TraceWriterTables {
    /// The landing table's name, the same in every mode.
    pub fn traces_default() -> Self {
        TraceWriterTables {
            landing: Arc::from(trace_landing::LANDING_TABLE),
        }
    }

    /// The landing table for a deployment: the bare name for `None`, and
    /// `trace_landing<suffix>` for `Some(suffix)`.
    pub fn landing_for(dist_suffix: Option<&str>) -> Self {
        let _ = dist_suffix;
        Self::traces_default()
    }
}

struct Shared {
    /// The landing path: its queue, its insert workers and its byte counter.
    landing: TraceLandingPath,
    /// Test-only: runs a case's hook from inside this struct's teardown,
    /// after the landing path and its queue's sender is dropped and before
    /// the boundary is (`writer::teardown_probe`). Field order is drop order.
    #[cfg(test)]
    teardown_probe: crate::writer::teardown_probe::TeardownProbe,
    runtime: Arc<WriterRuntime>,
    metrics: Arc<TraceWriterMetrics>,
    /// Issue #494's per-signal push-suppression index. `None` while
    /// `PULSUS_INGEST_DEDUP` is off.
    dedup: Option<Arc<PushDedup>>,
    /// The shutdown boundary: the admission gate, the announced deadline and
    /// every task this writer spawned (`writer::drain`).
    boundary: DrainBoundary,
}

/// Cancels the drain boundary **before** any field is dropped — the landing
/// queue's sender and the boundary's own signal among them — so no insert
/// worker on another thread is authorized an attempt once the writer's
/// teardown has begun ([`DrainBoundary::cancel`]).
impl Drop for Shared {
    fn drop(&mut self) {
        self.boundary.cancel();
    }
}

/// Implements `TraceSink` over one landing table. See the module docs.
pub struct TraceWriter {
    shared: Arc<Shared>,
}

impl TraceWriter {
    /// Production constructor against the landing table.
    pub fn new(client: Arc<ChClient>, cfg: &WriterConfig) -> Self {
        Self::new_with_tables(client, cfg, TraceWriterTables::traces_default())
    }

    /// [`Self::new`], but against `tables`.
    pub fn new_with_tables(
        client: Arc<ChClient>,
        cfg: &WriterConfig,
        tables: TraceWriterTables,
    ) -> Self {
        Self::with_inserters_with_tables(TraceLandingPath::production_inserter(client), cfg, tables)
    }

    /// Test/mock constructor: any landing [`BlockInserter`] — e.g. a
    /// scriptable mock that can fail or hang on demand — against `tables`.
    pub fn with_inserters_with_tables(
        landing_inserter: Arc<dyn BlockInserter<TraceLandingRow>>,
        cfg: &WriterConfig,
        tables: TraceWriterTables,
    ) -> Self {
        Self::with_inserters_and_runtime(landing_inserter, WriterRuntime::from_config(cfg), tables)
    }

    /// [`Self::with_inserters_with_tables`], but against a caller-supplied
    /// [`WriterRuntime`], so a test can point the spool anywhere. No
    /// production call site depends on it.
    #[doc(hidden)]
    pub fn with_inserters_and_runtime(
        landing_inserter: Arc<dyn BlockInserter<TraceLandingRow>>,
        runtime: WriterRuntime,
        tables: TraceWriterTables,
    ) -> Self {
        let runtime = Arc::new(runtime);
        let metrics = Arc::new(TraceWriterMetrics::default());
        let spool = Arc::new(spool::SpoolWriter::new(
            runtime.spool_dir.clone(),
            metrics.clone(),
        ));
        let boundary = DrainBoundary::new();
        let dedup = runtime.ingest_dedup.then(|| {
            PushDedup::new(
                runtime.ingest_dedup_max_bytes,
                runtime.ingest_dedup_window,
                runtime.claim_deadline,
            )
        });
        let landing = TraceLandingPath::new(
            tables.landing,
            landing_inserter,
            runtime.clone(),
            metrics.clone(),
            spool,
            dedup.clone(),
            &boundary,
        );
        TraceWriter {
            shared: Arc::new(Shared {
                landing,
                #[cfg(test)]
                teardown_probe: Default::default(),
                runtime,
                metrics,
                dedup,
                boundary,
            }),
        }
    }

    /// Admits `landing` as one sealed block.
    ///
    /// The order is the suppression index, then the per-push ceilings, then
    /// the byte reservation; any refusal stores nothing. A `Sync` push's block
    /// carries the waiter its answer comes from.
    fn admit_batch(
        &self,
        tenant: &Tenant,
        landing: ParsedTraceLanding,
        mode: AdmitMode,
        push: PushHeaders,
    ) -> Result<Admitted, AdmitRefusal> {
        // The admission gate. The pass is held for the whole of this body,
        // which never awaits, and `shutdown` waits for every outstanding pass
        // before it announces the deadline — so a block admitted here is in
        // the queue before the drain can close it.
        let Some(pass) = self.shared.boundary.enter() else {
            return Err(AdmitRefusal::Backpressure);
        };
        // Every landing row of a push carries the same stamp, so the block
        // lies in one landing partition.
        let received_ms = now_unix_millis();
        // The landing budget's anchor, taken before the claim, because the
        // claim deadline it must settle inside of starts here too.
        let admitted_at = tokio::time::Instant::now();

        // Issue #494's claim, taken before the byte reservation, so a request
        // refused by backpressure leaves no claim behind. **A suppressed
        // trace push sends nothing at all.**
        let mut suppressed: Option<Suppressed> = None;
        let mut guard = match &self.shared.dedup {
            Some(dedup) => {
                let id = push_dedup::trace_identity(&landing, &push, tenant);
                let rows = landing.spans.len() as u64;
                match dedup.admit(id, mode.wait_mode()) {
                    Admission::Admit(guard) => Some(guard),
                    Admission::SuppressedSettled(outcome) => {
                        dedup.count_suppressed(id.declared_retry, rows);
                        suppressed = Some(Suppressed::Settled(outcome));
                        None
                    }
                    Admission::SuppressedPending { guard, rx } => {
                        dedup.count_suppressed(id.declared_retry, rows);
                        suppressed = Some(Suppressed::Pending(guard, rx));
                        None
                    }
                    Admission::KeyReused => return Err(AdmitRefusal::KeyReused),
                    Admission::Shed => return Err(AdmitRefusal::DedupShed),
                    Admission::WaitShed => return Err(AdmitRefusal::DedupWaitShed),
                }
            }
            None => None,
        };

        // The per-push ceilings, decided before anything is queued. Returning
        // here drops the un-sealed guard, which removes the claim. **A valid
        // push with no span makes no block and no insert** and is charged for
        // nothing.
        let landing_rows = landing.total_rows();
        // Issue #738: every row shares the push's one tenant string, whose
        // bytes are charged once, beside the slot each row's `Arc` occupies.
        let landing_bytes = trace_landing::charge_of(&landing) + tenant.as_str().len() as u64;
        if landing_rows > 0 {
            // `spans`, `traces` and `resources` are partitioned by UTC day,
            // so one view's insert produces one part per day in the push and
            // the server throws above `max_partitions_per_insert_block`.
            let days = trace_landing::distinct_span_days(&landing);
            if days > trace_landing::TRACE_LANDING_DAY_LIMIT {
                return Err(AdmitRefusal::PushSpansTooManyDays {
                    days,
                    day_limit: trace_landing::TRACE_LANDING_DAY_LIMIT,
                });
            }
            if landing_rows >= self.shared.runtime.trace_landing_max_rows
                || landing_bytes > self.shared.runtime.batch_bytes
            {
                return Err(AdmitRefusal::PushTooLarge {
                    rows: landing_rows,
                    row_limit: self.shared.runtime.trace_landing_max_rows,
                    bytes: landing_bytes,
                    byte_limit: self.shared.runtime.batch_bytes,
                });
            }
        }

        // The suppressed push stops here, having stored nothing, and is
        // answered what the original caller was told.
        if let Some(suppressed) = suppressed {
            return Ok(Admitted::Suppressed(suppressed));
        }

        self.shared
            .metrics
            .rejected_total
            .fetch_add(landing.rejected, std::sync::atomic::Ordering::Relaxed);

        let mut receivers = Vec::new();
        if landing_rows > 0 {
            self.shared
                .landing
                .reserve(landing_bytes)
                .map_err(AdmitRefusal::from)?;
            let mut claim = ClaimTicket::new(self.shared.dedup.clone(), true).replay_safe();
            if let Some(g) = guard.as_ref() {
                claim.push(g.key());
            }
            note_target(&mut guard, true);
            let waiter = if mode == AdmitMode::Sync {
                let (tx, rx) = oneshot::channel();
                receivers.push(rx);
                Some(tx)
            } else {
                None
            };
            let rows = trace_landing::rows_of(tenant.as_arc(), received_ms, landing);
            debug_assert_eq!(
                rows.len() as u64,
                landing_rows,
                "the rows materialized are the rows the push was charged and \
                 measured for"
            );
            self.shared.landing.queue(
                &pass,
                &self.shared.boundary,
                rows,
                landing_bytes,
                claim,
                waiter,
                admitted_at,
            );
        }

        // Arms the claim: from here a drop no longer removes it, and the
        // target set is closed.
        if let Some(guard) = guard {
            guard.seal();
        }
        Ok(Admitted::Stored(receivers))
    }

    /// A point-in-time metrics snapshot.
    pub fn metrics(&self) -> TraceWriterMetricsSnapshot {
        self.shared.landing.snapshot(
            self.shared
                .dedup
                .as_ref()
                .map(|d| d.snapshot())
                .unwrap_or_default(),
        )
    }

    /// This writer's push-suppression index (issue #494), or `None` while
    /// `PULSUS_INGEST_DEDUP` is off.
    pub fn dedup(&self) -> Option<&Arc<PushDedup>> {
        self.shared.dedup.as_ref()
    }

    /// Re-opens the admission gate after [`Self::shutdown`] has returned, so
    /// the next admission takes a pass and meets the closed queue. Nothing in
    /// the server calls this.
    #[doc(hidden)]
    pub fn reopen_admission_for_test(&self) {
        self.shared.boundary.reopen_for_test();
    }

    /// Graceful shutdown, which is the boundary's whole job
    /// (`writer::drain`): admission closes and the admissions inside it
    /// finish, the deadline is announced, and every task this writer spawned
    /// is awaited. Idempotent.
    pub async fn shutdown(&self, deadline: Duration) {
        self.shared.boundary.shutdown(deadline).await;
    }
}

impl TraceSink for TraceWriter {
    fn admit(
        &self,
        tenant: &Tenant,
        landing: ParsedTraceLanding,
        push: PushHeaders,
    ) -> Result<(), AdmitRefusal> {
        self.admit_batch(tenant, landing, AdmitMode::Async, push)
            .map(|_| ())
    }

    fn admit_flush(
        &self,
        tenant: &Tenant,
        landing: ParsedTraceLanding,
        push: PushHeaders,
    ) -> Result<FlushWait, AdmitRefusal> {
        match self.admit_batch(tenant, landing, AdmitMode::Sync, push)? {
            Admitted::Stored(receivers) => Ok(FlushWait::new(async move {
                super::join_generations(receivers)
                    .await
                    .map_err(|e| LogsIngestError::FlushFailed(e.to_string()))
            })),
            Admitted::Suppressed(suppressed) => Ok(FlushWait::new(suppressed.into_answer())),
        }
    }
}

/// The writer's UTC epoch-millisecond stamp for one push. A clock before the
/// epoch degrades to `0` rather than panicking.
fn now_unix_millis() -> i64 {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **1-U6 (issue #494 part 1).** The bare name without a suffix, the
    /// suffixed wrapper with one.
    #[test]
    fn landing_for_names_the_wrapper_only_when_given_a_suffix() {
        assert_eq!(
            &*TraceWriterTables::landing_for(None).landing,
            "trace_landing"
        );
        assert_eq!(
            &*TraceWriterTables::landing_for(Some("_d")).landing,
            "trace_landing_d"
        );
    }

    /// [`crate::writer::log`]'s case of the same name, for the trace
    /// writer's landing path: B is refused between the landing sender's
    /// teardown and the boundary's.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_queued_trace_block_is_refused_between_the_senders_and_the_boundarys_teardown() {
        use crate::writer::teardown_probe::{
            ParkingInserter, assert_b_filed_never_sent, release_then_wait_for_b, spool_root,
        };
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
        use opentelemetry_proto::tonic::resource::v1::Resource;
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

        const TS: u64 = 1_700_000_000_000_000_000;
        let push = |span_id: u8| {
            let req = ExportTraceServiceRequest {
                resource_spans: vec![ResourceSpans {
                    resource: Some(Resource {
                        attributes: vec![KeyValue {
                            key: "service.name".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("checkout".to_string())),
                            }),
                            key_strindex: 0,
                        }],
                        ..Default::default()
                    }),
                    scope_spans: vec![ScopeSpans {
                        spans: vec![Span {
                            trace_id: vec![0xab; 16],
                            span_id: vec![span_id; 8],
                            name: "op".to_string(),
                            start_time_unix_nano: TS,
                            end_time_unix_nano: TS + 1_000_000,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            };
            crate::parse_trace_landing(&req).expect("the landing decode")
        };

        let inserter = ParkingInserter::new();
        let root = spool_root("trace");
        let mut runtime = WriterRuntime::from_config(&WriterConfig::default());
        runtime.spool_dir = root.clone();
        runtime.trace_landing_inserters = 1;
        let writer = TraceWriter::with_inserters_and_runtime(
            inserter.clone(),
            runtime,
            TraceWriterTables::traces_default(),
        );
        for span_id in [1, 2] {
            let landed = push(span_id);
            writer
                .admit(
                    &Tenant::from_header(None, false).expect("the empty tenant"),
                    landed,
                    PushHeaders::default(),
                )
                .expect("the push is admitted");
            if span_id == 1 {
                // A is in flight before B is queued.
                inserter
                    .entered
                    .acquire()
                    .await
                    .expect("the entered semaphore is never closed")
                    .forget();
            }
        }
        writer.shared.teardown_probe.set(release_then_wait_for_b(
            inserter.clone(),
            root.clone(),
            trace_landing::LANDING_TABLE,
        ));
        drop(writer);

        assert_b_filed_never_sent(&inserter, &root, trace_landing::LANDING_TABLE).await;
        std::fs::remove_dir_all(&root).ok();
    }

    /// **T-W2 (issue #738): a trace push is charged its tenant's bytes
    /// once.** The same push, queued by two writers whose insert parks, once
    /// as the empty tenant and once as a 150-byte tenant: every row shares
    /// one tenant string, so the two landing reservations differ by the
    /// tenant's 150 bytes and nothing else.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_trace_push_is_charged_its_tenants_bytes_once() {
        use crate::writer::teardown_probe::{ParkingInserter, spool_root};
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
        use opentelemetry_proto::tonic::resource::v1::Resource;
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

        const TS: u64 = 1_700_000_000_000_000_000;
        let landing = || {
            let req = ExportTraceServiceRequest {
                resource_spans: vec![ResourceSpans {
                    resource: Some(Resource {
                        attributes: vec![KeyValue {
                            key: "service.name".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("checkout".to_string())),
                            }),
                            key_strindex: 0,
                        }],
                        ..Default::default()
                    }),
                    scope_spans: vec![ScopeSpans {
                        spans: (1..=2u8)
                            .map(|id| Span {
                                trace_id: vec![0xab; 16],
                                span_id: vec![id; 8],
                                name: "op".to_string(),
                                start_time_unix_nano: TS,
                                end_time_unix_nano: TS + 1_000_000,
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            };
            crate::parse_trace_landing(&req).expect("the landing decode")
        };
        let long = "a".repeat(150);
        let mut charged = Vec::new();
        for name in [None, Some(long.as_str())] {
            let inserter = ParkingInserter::new();
            let root = spool_root("trace-tenant-charge");
            let mut runtime = WriterRuntime::from_config(&WriterConfig::default());
            runtime.spool_dir = root.clone();
            runtime.trace_landing_inserters = 1;
            let writer = TraceWriter::with_inserters_and_runtime(
                inserter.clone(),
                runtime,
                TraceWriterTables::traces_default(),
            );
            let value = name.map(|n| axum::http::HeaderValue::from_str(n).expect("a header value"));
            let who = Tenant::from_header(value.as_ref(), false).expect("a valid tenant");
            writer
                .admit(&who, landing(), PushHeaders::default())
                .expect("the push is admitted");
            charged.push(writer.metrics().queue_bytes);
            inserter
                .entered
                .acquire()
                .await
                .expect("the entered semaphore is never closed")
                .forget();
            inserter.release.add_permits(1);
            writer.shutdown(Duration::from_secs(5)).await;
            std::fs::remove_dir_all(&root).ok();
        }
        assert_eq!(
            charged[1].checked_sub(charged[0]),
            Some(150),
            "the 150-byte tenant's push is charged {} where the empty tenant's is {}",
            charged[1],
            charged[0]
        );
    }
}
