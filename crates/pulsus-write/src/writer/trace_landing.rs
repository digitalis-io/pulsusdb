//! The trace landing path: **one push is one `INSERT` of one block into
//! `trace_landing`** (issue #586), beside the old two-table trace writer,
//! which keeps writing `trace_spans` and `trace_attrs_idx` until task 20
//! (#602) because the reads that have not moved still answer from them.
//!
//! Five materialized views maintain `spans`, `resources`, `traces`,
//! `tag_names` and `tag_values` from that one source table, and this path
//! inserts into none of them. The loop, its two fates, the budget, the
//! shutdown boundary, the byte accounting and the spool copy are
//! `writer::landing`'s, whole; what is here is what traces decides for
//! itself.
//!
//! **Four things, and each is here because of what a trace push carries:**
//!
//! * the four landed event shapes a push produces, built from the handler's
//!   [`ParsedTraceLanding`] rather than decoded here — the decode runs in the
//!   handler so a decode-class refusal can leave through each route's own
//!   whole-request error writer;
//! * the per-push UTC-date ceiling, which is traces' own: `spans`, `traces`
//!   and `resources` are partitioned by UTC day, so one view's insert
//!   produces one part per day in the push and the server throws above
//!   `max_partitions_per_insert_block`;
//! * the byte charge, which walks two arrays of tuples — a span's events and
//!   links are client-chosen and unbounded short of the expansion ceiling,
//!   so a charge that priced the vector headers alone would not bound what
//!   the queue holds;
//! * a byte counter of its own, separate from the old path's. One counter
//!   for both would charge a trace push both paths' bytes against one
//!   ceiling, so a push the route accepts today would be refused `429` at an
//!   unchanged setting while the old path is still the one answering the
//!   client.
//!
//! **Nothing here subtracts the landing counter.** The reservation is taken
//! at admission and released exactly once, by whichever ending settles the
//! block, through `writer::landing`'s `LandingBlock::release` — which owns
//! when. Admission reserves the old path's bytes first and the landing
//! path's second, so a refusal between the two rolls back the old path's
//! reservation and never this one, and a granted landing reservation is
//! always followed by a queued block.

// STUB (issue #586, the tests-first commit): `writer::trace` does not call
// this path's helpers yet, so each of them reads as dead.
#![allow(dead_code)]

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use pulsus_clickhouse::{MAX_PARTITIONS_PER_INSERT_BLOCK, QuerySettings};
use tokio::sync::mpsc;

use crate::ingest::traces::ParsedTraceLanding;
use crate::writer::config::WriterRuntime;
use crate::writer::drain::{AdmissionPass, DrainBoundary};
use crate::writer::landing::{
    LandingBlock, LandingContext, LandingFate, MSG_SHUTDOWN_QUEUED, TerminalCause, landing_charge,
    mint_landing_token, settle_block, spawn_landing_worker,
};
use crate::writer::metrics::{TraceLandingSnapshot, TraceWriterMetrics};
use crate::writer::push_dedup::ClaimTicket;
use crate::writer::push_dedup::PushDedup;
use crate::writer::rows::TraceLandingRow;
use crate::writer::spool::SpoolWriter;
use crate::writer::table::{BlockInserter, ChBlockInserter, XorShift64, spawn_dedup_ticker};

/// The one table this path inserts into. The landing table has no
/// `Distributed` wrapper — a push carries many trace ids, so inserting the
/// push itself through one would split one push per shard and one push would
/// stop being one block — so this is the base name in every mode. The
/// routing happens one step later, on the way out of the two per-trace
/// views.
pub(crate) const LANDING_TABLE: &str = "trace_landing";

/// How many distinct UTC dates one push's spans may fall on.
///
/// **The gate and the pin carry the same constant**, so neither can admit
/// what the other refuses: `max_partitions_per_insert_block` is pinned at
/// `MAX_PARTITIONS_PER_INSERT_BLOCK` on every landing insert, and
/// `throw_on_max_partitions_per_insert_block` is `1`, so a push above this
/// many dates is refused by the server after the block was sent. 100 dates
/// is admitted, 101 is refused.
pub const TRACE_LANDING_DAY_LIMIT: u64 = MAX_PARTITIONS_PER_INSERT_BLOCK;

/// The distinct UTC dates the push's spans fall on.
///
/// Read off the landed resource rows rather than recomputed from the spans:
/// every span that passed every check inserts its own `(resource_id, day)`
/// entry, so the day set of those rows is exactly the day set of the landed
/// spans, and the UTC day of a span is computed in one place — the decode's
/// own `Date::start_of_day_utc_datetime_safe`, which is also what rejects a
/// span outside the admitted domain.
pub(crate) fn distinct_span_days(landing: &ParsedTraceLanding) -> u64 {
    landing
        .resources
        .iter()
        .map(|r| r.day)
        .collect::<BTreeSet<u16>>()
        .len() as u64
}

/// What the queue is charged for one push's landed rows, over **all four
/// kinds**, plus what the block itself holds.
///
/// Taken from the decoded batch before any row is built — the
/// reserve-before-materialize rule — and every term is a kind's own owned
/// text beside the row slot each kind occupies, because the row type is the
/// union of four kinds' columns and every row of every kind holds every
/// slot.
pub(crate) fn charge_of(landing: &ParsedTraceLanding) -> u64 {
    let spans: u64 = landing
        .spans
        .iter()
        .map(|s| TraceLandingRow::est_landing_bytes(TraceLandingRow::est_span_bytes(s)))
        .sum();
    let resources: u64 = landing
        .resources
        .iter()
        .map(|r| TraceLandingRow::est_landing_bytes(TraceLandingRow::est_resource_bytes(r)))
        .sum();
    let names: u64 = landing
        .tag_names
        .iter()
        .map(|t| TraceLandingRow::est_landing_bytes(TraceLandingRow::est_tag_name_bytes(t)))
        .sum();
    let values: u64 = landing
        .tag_values
        .iter()
        .map(|t| TraceLandingRow::est_landing_bytes(TraceLandingRow::est_tag_value_bytes(t)))
        .sum();
    landing_charge::<TraceLandingRow>(spans + resources + names + values)
}

/// The push's landed rows, every kind in one vector.
///
/// Nothing depends on their order inside the block: the table's sorting key
/// orders what is stored, and every consumer matches rows by `row_kind`.
/// `received_ms` is taken once per push, so a block lies in one landing
/// partition.
pub(crate) fn rows_of(received_ms: i64, landing: ParsedTraceLanding) -> Vec<TraceLandingRow> {
    let ParsedTraceLanding {
        spans,
        resources,
        tag_names,
        tag_values,
        ..
    } = landing;
    let mut rows: Vec<TraceLandingRow> =
        Vec::with_capacity(spans.len() + resources.len() + tag_names.len() + tag_values.len());
    rows.extend(
        spans
            .into_iter()
            .map(|s| TraceLandingRow::span(received_ms, s)),
    );
    rows.extend(
        resources
            .into_iter()
            .map(|r| TraceLandingRow::resource(received_ms, r)),
    );
    rows.extend(
        tag_names
            .into_iter()
            .map(|t| TraceLandingRow::tag_name(received_ms, t)),
    );
    rows.extend(
        tag_values
            .into_iter()
            .map(|t| TraceLandingRow::tag_value(received_ms, t)),
    );
    rows
}

/// The landing half of the trace writer: its queue, its insert workers, its
/// own byte counter and the token generator.
pub(crate) struct TraceLandingPath {
    /// The landing queue's sender. Bounded by **bytes, not by length**:
    /// [`Self::reserve`] is the only gate, so what the queue holds is
    /// bounded by `PULSUS_INGEST_QUEUE_BYTES`, whatever number of blocks
    /// that comes to.
    tx: mpsc::UnboundedSender<LandingBlock<TraceLandingRow>>,
    ctx: Arc<LandingContext<TraceLandingRow>>,
    /// This path's own reservation counter, not the old path's.
    queued_bytes: Arc<AtomicU64>,
    metrics: Arc<TraceWriterMetrics>,
    /// The token generator. One lock per admitted push: a token minted from
    /// the clock alone would repeat for two pushes inside one millisecond,
    /// and the second would be dropped as a resend of the first.
    token_rng: Mutex<XorShift64>,
}

impl TraceLandingPath {
    /// Builds the path and spawns everything it owns onto `boundary`: the
    /// insert workers and the suppression index's ticker. A task this writer
    /// spawned and nothing awaits can be cancelled by runtime teardown
    /// mid-settlement, which is why every one of them is tracked.
    pub(crate) fn new(
        table: Arc<str>,
        inserter: Arc<dyn BlockInserter<TraceLandingRow>>,
        runtime: Arc<WriterRuntime>,
        metrics: Arc<TraceWriterMetrics>,
        spool: Arc<SpoolWriter>,
        dedup: Option<Arc<PushDedup>>,
        boundary: &DrainBoundary,
    ) -> Self {
        let queued_bytes = Arc::new(AtomicU64::new(0));
        let ctx = Arc::new(LandingContext {
            table,
            inserter,
            runtime: runtime.clone(),
            table_metrics: metrics.landing.clone(),
            queued_bytes: queued_bytes.clone(),
            spool,
            // Nothing is promoted at a trace commit: there is no cache of
            // anything already written anywhere on this path.
            on_commit: None,
            retries: runtime.trace_landing_retries,
            max_rows: runtime.trace_landing_max_rows,
        });

        let (tx, rx) = mpsc::unbounded_channel::<LandingBlock<TraceLandingRow>>();
        // A mutex over the receiver rather than a `Notify` beside a deque,
        // which stores one permit and would leave a second queued block
        // waiting for a later push. A worker holds the lock only while
        // taking a block, so up to `trace_landing_inserters` inserts
        // overlap.
        let rx = Arc::new(tokio::sync::Mutex::new(rx));
        boundary.track_background(
            (0..runtime.trace_landing_inserters.max(1))
                .map(|_| spawn_landing_worker(ctx.clone(), rx.clone(), boundary.watch()))
                .collect::<Vec<_>>(),
        );
        // The suppression index's `tick()` is a task of its own, as it is on
        // both shipped signals: the old trace path's two flush loops drive
        // no index, and `TableContext::dedup` stays `None` for that reason.
        boundary.track_background(
            dedup.map(|d| spawn_dedup_ticker(d, runtime.batch_age, boundary.watch())),
        );

        TraceLandingPath {
            tx,
            ctx,
            queued_bytes,
            metrics,
            token_rng: Mutex::new(XorShift64::seeded()),
        }
    }

    /// The production constructor's inserter: a plain block inserter, because
    /// the landing insert's nineteen pins travel on the block itself
    /// ([`QuerySettings::trace_landing_insert`]) and are sent with it on
    /// every attempt and every resend.
    pub(crate) fn production_inserter(
        client: Arc<pulsus_clickhouse::ChClient>,
    ) -> Arc<dyn BlockInserter<TraceLandingRow>> {
        Arc::new(ChBlockInserter::new(client))
    }

    /// Reserves `bytes` against this path's own counter, counting a
    /// backpressure rejection on overflow. **Nothing gives these bytes
    /// back but `LandingBlock::release`**, so every granted reservation is
    /// followed by a queued block.
    pub(crate) fn reserve(&self, bytes: u64) -> Result<(), crate::ingest::Backpressure> {
        super::reserve_queued_bytes(
            &self.queued_bytes,
            &self.metrics.backpressure_total,
            bytes,
            self.ctx.runtime.queue_bytes_limit,
        )
    }

    /// Seals one block — minting its token, so two byte-identical pushes
    /// carry different ones — and hands it to the queue.
    ///
    /// **The block carries no waiter.** The route's answer stays the old
    /// path's, byte for byte: a landing failure is counted and spooled like
    /// any other block and changes no status code. Task 20 moves the answer
    /// to this path when it deletes the old one.
    ///
    /// A block that reaches here after the queue closed is settled by a task
    /// of this admission's own, through the same ending a worker gives a
    /// block still queued at shutdown.
    pub(crate) fn queue(
        &self,
        pass: &AdmissionPass<'_>,
        boundary: &DrainBoundary,
        rows: Vec<TraceLandingRow>,
        bytes: u64,
        claim: ClaimTicket,
        admitted_at: tokio::time::Instant,
    ) {
        let token = {
            let mut rng = self
                .token_rng
                .lock()
                .expect("trace landing token rng mutex poisoned");
            mint_landing_token(&mut rng)
        };
        let block = LandingBlock::seal(
            rows,
            bytes,
            QuerySettings::trace_landing_insert(&token, self.ctx.max_rows),
            claim,
            None,
            admitted_at,
        );
        if let Err(closed) = self.tx.send(block) {
            let ctx = self.ctx.clone();
            let block = closed.0;
            let mut fate = LandingFate::NeverSent(String::new());
            fate.saw_pre_send(MSG_SHUTDOWN_QUEUED.to_string());
            boundary.spawn_settlement(pass, async move {
                settle_block(&ctx, block, fate, TerminalCause::ShuttingDown).await;
            });
        }
    }

    /// This path's own figures, which no exported series carries.
    pub(crate) fn snapshot(&self) -> TraceLandingSnapshot {
        self.metrics
            .landing_snapshot(self.queued_bytes.load(Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    /// **The landing counter is subtracted in exactly one place, and it is
    /// not in this module** (issue #586): `writer::landing`'s
    /// `LandingBlock::release` owns when a block's reservation goes back,
    /// and it gives it back only once the block is gone. A census over this
    /// module's own source, because the rule is about where the subtraction
    /// may appear rather than about one call's result.
    #[test]
    fn the_trace_landing_reservation_is_released_in_one_place() {
        const SOURCE: &str = include_str!("trace_landing.rs");
        let body = SOURCE
            .split_once("#[cfg(test)]")
            .map(|(before, _)| before)
            .unwrap_or(SOURCE);
        let subtractions: Vec<&str> = body
            .lines()
            .filter(|line| line.contains("fetch_sub") || line.contains("saturating_sub"))
            .collect();
        assert!(
            subtractions.is_empty(),
            "this module must subtract the landing counter nowhere — \
             `LandingBlock::release` is the one place: {subtractions:?}"
        );
    }
}
