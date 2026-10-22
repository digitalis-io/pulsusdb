//! The per-table flush task: `select!{Notify, interval, shutdown}`
//! (architect plan), a bounded pre-send retry policy with
//! exponential-backoff-plus-full-jitter (hand-rolled xorshift — no `rand`
//! dependency), and the poison/uncertain spool classifier. One task per
//! table (not shared): a stalled high-volume `log_samples` insert must
//! not head-of-line-block low-volume `log_streams`, and each table needs
//! independent retry/timer state (architect plan).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use pulsus_clickhouse::{ChClient, ChError, ChRow, QuerySettings};

use crate::writer::drain::DrainWatch;
use crate::writer::push_dedup::PushDedup;

/// Real-vs-mock seam over a columnar block insert (architect plan): the
/// production impl ([`ChBlockInserter`]) wraps
/// `pulsus_clickhouse::ChClient::insert_block`; tests substitute a mock
/// that can fail/hang on demand — no real ClickHouse in unit tests. A
/// hand-rolled boxed-future method (not `async fn`) so the trait stays
/// object-safe: `LogWriter` holds this behind `Arc<dyn BlockInserter<R>>`
/// per table.
pub trait BlockInserter<R>: Send + Sync
where
    R: ChRow,
{
    fn insert<'a>(
        &'a self,
        table: &'a str,
        rows: &'a [R],
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>>;

    /// [`Self::insert`] with `extra` settings on this one insert (issue
    /// #603: the metrics landing block's deduplication token and block-size
    /// pin). The default ignores `extra` and calls [`Self::insert`], so an
    /// implementation that has no per-insert settings — every one but the
    /// production inserter — needs no change.
    fn insert_with<'a>(
        &'a self,
        table: &'a str,
        rows: &'a [R],
        extra: &'a QuerySettings,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        let _ = extra;
        self.insert(table, rows)
    }
}

/// Production [`BlockInserter`]: a thin adapter over
/// `ChClient::insert_block`, generic over every row shape this crate
/// defines (the landing rows and every trace row share one instance).
pub struct ChBlockInserter {
    client: Arc<ChClient>,
    /// Settings added to every insert this inserter sends; empty for every
    /// table but the span table (issue #560).
    extra: QuerySettings,
}

impl ChBlockInserter {
    pub fn new(client: Arc<ChClient>) -> Self {
        Self::with_settings(client, QuerySettings::new())
    }

    /// An inserter whose every insert carries `extra` (issue #560).
    pub fn with_settings(client: Arc<ChClient>, extra: QuerySettings) -> Self {
        ChBlockInserter { client, extra }
    }

    /// The settings one insert sends: this inserter's own, then the call's on
    /// top, so where both name a setting the call's value wins. The metrics
    /// landing token is minted per push and an inserter-wide value could never
    /// stand in for it (issue #603).
    fn merged_settings(&self, extra: &QuerySettings) -> QuerySettings {
        let mut merged = self.extra.clone();
        for (key, value) in extra.entries() {
            merged = merged.set(key, value);
        }
        merged
    }
}

impl<R: ChRow> BlockInserter<R> for ChBlockInserter {
    fn insert<'a>(
        &'a self,
        table: &'a str,
        rows: &'a [R],
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        Box::pin(self.client.insert_block_with(table, rows, &self.extra))
    }

    /// **Overridden, not inherited** (issue #603). The trait default drops
    /// `extra` and calls [`Self::insert`], which sends this inserter's own
    /// settings alone; inheriting it here would silently discard every
    /// per-block setting the metrics landing path supplies — the deduplication
    /// token and pins that make a resend safe, and the block-size ceiling that
    /// keeps one push from becoming two blocks.
    fn insert_with<'a>(
        &'a self,
        table: &'a str,
        rows: &'a [R],
        extra: &'a QuerySettings,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
        Box::pin(async move {
            let merged = self.merged_settings(extra);
            self.client.insert_block_with(table, rows, &merged).await
        })
    }
}

/// Issue #494's suppression index, ticked on its own task rather than off a
/// flush loop (issue #603): the metrics writer has no flush task left to
/// ride, and without a ticker its claims would never age past their deadline
/// and an unsettled claim would answer a waiting caller nothing at all.
/// Ticks every `every` — `PULSUS_BATCH_MS` at the one call site — until the
/// shutdown signal fires, which is also what bounds how stale the age
/// eviction can be.
pub(crate) fn spawn_dedup_ticker(
    dedup: Arc<PushDedup>,
    every: Duration,
    mut watch: DrainWatch,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(every);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = interval.tick() => dedup.tick(),
                // A gone signal — the writer dropped without a graceful
                // shutdown — ends the ticker exactly as a deadline does.
                () = watch.until_announced() => return,
            }
        }
    })
}

/// A cheap, non-cryptographic xorshift64 PRNG for full-jitter retry
/// delays — a dedicated `rand` dependency is unwarranted for "pick a
/// uniform random delay" (lean-deps ethos, architect plan).
pub(crate) struct XorShift64(u64);

impl XorShift64 {
    pub(crate) fn seeded() -> Self {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x9E37_79B9_7F4A_7C15);
        Self::from_seed(seed)
    }

    /// A generator whose whole sequence is `seed`'s, so a case that has to
    /// hold a block in a retry sleep draws the same delay every run — the
    /// landing worker takes one when `WriterRuntime::retry_jitter_seed` is
    /// set. Full jitter permits a zero delay, and a case pinned to a delay
    /// it happened to draw fails on that one.
    pub(crate) fn from_seed(seed: u64) -> Self {
        // A xorshift generator's state must never be zero (it is a fixed
        // point), hence the `| 1`.
        XorShift64(seed | 1)
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

/// Exponential backoff (`base * 2^(attempt-1)`, capped at `max`) with full
/// jitter (`Prometheus`/AWS-style: a uniform random delay in
/// `[0, capped]`, not just added noise around the capped value) — spreads
/// out retries from many concurrently-failing batches instead of having
/// them all retry in lockstep.
pub(crate) fn backoff_delay(
    base: Duration,
    max: Duration,
    attempt: u32,
    rng: &mut XorShift64,
) -> Duration {
    let shift = attempt.saturating_sub(1).min(20);
    let multiplier = 1u32 << shift;
    let capped = base.saturating_mul(multiplier).min(max);
    let millis = capped.as_millis() as u64;
    if millis == 0 {
        return Duration::ZERO;
    }
    Duration::from_millis(rng.next_u64() % (millis + 1))
}

#[cfg(test)]
mod tests {
    use pulsus_model::Fingerprint;

    use super::*;

    /// Issue #603: the suppression index used to be ticked off a flush loop,
    /// and the metrics writer has no flush loop left. Without a ticker an
    /// open claim would never age past its deadline into a tombstone, and a
    /// caller waiting on it would be told nothing at all.
    ///
    /// Two indexes with the same limits, each holding one open claim; only
    /// the first gets a ticker. The second is what shows the ageing came from
    /// the ticker and not from `admit`, which expires the *window* itself.
    #[tokio::test(start_paused = true)]
    async fn the_dedup_ticker_ages_an_open_claim_on_its_own() {
        use crate::ingest::PushHeaders;
        use crate::ingest::metrics::{MetricPoint, ParsedMetrics};
        use crate::writer::push_dedup::{Admission, WaitMode, metric_identity};

        let claim_deadline = Duration::from_millis(200) + Duration::from_secs(120);
        let batch = ParsedMetrics {
            samples: vec![MetricPoint {
                metric_name: Arc::from("m"),
                fingerprint: Fingerprint::from_raw(1),
                unix_milli: 1_000,
                value: 1.0,
            }],
            ..Default::default()
        };

        let ticked = PushDedup::new(16 * 1024 * 1024, Duration::from_secs(300), claim_deadline);
        let unticked = PushDedup::new(16 * 1024 * 1024, Duration::from_secs(300), claim_deadline);
        for index in [&ticked, &unticked] {
            let id = metric_identity(
                &batch,
                &PushHeaders::default(),
                &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            );
            match index.admit(id, WaitMode::None) {
                Admission::Admit(mut guard) => {
                    guard.note_target(true);
                    guard.seal();
                }
                other => panic!("a fresh index admits: {other:?}"),
            }
            assert_eq!(index.snapshot().unknown_total, 0);
        }

        let boundary = crate::writer::drain::DrainBoundary::new();
        let handle =
            spawn_dedup_ticker(ticked.clone(), Duration::from_millis(50), boundary.watch());

        // Past the claim deadline, then let the ticker run.
        tokio::time::advance(claim_deadline + Duration::from_secs(1)).await;
        for _ in 0..64 {
            tokio::task::yield_now().await;
            if ticked.snapshot().unknown_total == 1 {
                break;
            }
        }
        assert_eq!(
            ticked.snapshot().unknown_total,
            1,
            "the ticked index aged its open claim into a tombstone"
        );
        assert_eq!(
            unticked.snapshot().unknown_total,
            0,
            "nothing but the ticker ages a claim: no flush task, no `admit` call"
        );

        // Dropping the boundary closes the signal, which ends the ticker.
        drop(boundary);
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("the ticker returns once the signal is gone")
            .expect("it does not panic");
    }

    #[test]
    fn backoff_delay_never_exceeds_the_cap() {
        let mut rng = XorShift64::seeded();
        for attempt in 1..=20 {
            let delay = backoff_delay(
                Duration::from_millis(100),
                Duration::from_secs(1),
                attempt,
                &mut rng,
            );
            assert!(delay <= Duration::from_secs(1));
        }
    }

    #[test]
    fn backoff_delay_grows_with_attempt_before_the_cap() {
        // Deterministic bound check: the maximum possible jittered delay
        // (the cap itself) strictly increases attempt-over-attempt while
        // still below `max`.
        let base = Duration::from_millis(10);
        let max = Duration::from_secs(10);
        let cap_at = |attempt: u32| {
            let shift = attempt.saturating_sub(1).min(20);
            base.saturating_mul(1u32 << shift).min(max)
        };
        assert!(cap_at(1) < cap_at(2));
        assert!(cap_at(2) < cap_at(3));
    }

    /// The two seeds the landing-drain cases run on, and the delays they fix.
    /// Full jitter draws from `[0, capped]`, **zero included** — a case pinned
    /// to a delay it happened to draw fails on that one (issue #603 code review
    /// round 5, finding 5), so a case that has to hold a block in the retry
    /// sleep seeds the generator through `WriterRuntime::retry_jitter_seed` and
    /// the case that covers the zero draw seeds it to draw zero.
    #[test]
    fn a_seeded_jitter_draw_is_the_same_every_run() {
        let ten = Duration::from_secs(10);

        let mut held = XorShift64::from_seed(1);
        assert_eq!(
            backoff_delay(ten, ten, 1, &mut held),
            Duration::from_millis(1_545),
            "the seed a case uses to park a block in a retry sleep"
        );

        let mut zero_first = XorShift64::from_seed(10_658);
        assert_eq!(
            backoff_delay(ten, ten, 1, &mut zero_first),
            Duration::ZERO,
            "the seed whose first draw is the permitted zero delay"
        );
        assert_eq!(
            backoff_delay(ten, ten, 2, &mut zero_first),
            Duration::from_millis(9_136),
            "and whose next draw parks the block"
        );
    }

    /// A capped delay under a millisecond is zero without drawing at all, so a
    /// zero-length base and cap is a zero delay for every generator state.
    #[test]
    fn a_sub_millisecond_cap_is_a_zero_delay() {
        let mut rng = XorShift64::seeded();
        for attempt in 1..=4 {
            assert_eq!(
                backoff_delay(Duration::ZERO, Duration::ZERO, attempt, &mut rng),
                Duration::ZERO
            );
            assert_eq!(
                backoff_delay(
                    Duration::from_micros(200),
                    Duration::from_micros(900),
                    attempt,
                    &mut rng
                ),
                Duration::ZERO
            );
        }
    }

    #[test]
    fn xorshift_next_u64_is_deterministic_and_varies() {
        let mut rng = XorShift64(42);
        let a = rng.next_u64();
        let b = rng.next_u64();
        assert_ne!(a, b);
        let mut rng2 = XorShift64(42);
        assert_eq!(rng2.next_u64(), a);
    }
}
