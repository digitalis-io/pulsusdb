//! `ChClient`: the crate-agnostic facade over the winning `clickhouse`
//! (HTTP) crate (docs/decisions/0001-clickhouse-client.md).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::{Stream, StreamExt};

use crate::config::{ChConnConfig, ConsistencyConfig};
use crate::error::{ChError, Idempotency};
use crate::pool::{ChPool, PooledConn, ReadBinding, ServerFailure, StatementClass};
use crate::settings::QuerySettings;

/// Row-mapping trait re-exported from the winning crate so downstream
/// crates depend only on `pulsus-clickhouse`, never on `clickhouse` directly.
pub use clickhouse::Row;

/// Bound satisfied by any plain, owned `#[derive(Row, Serialize,
/// Deserialize)]` struct — the shape every `ChClient` method needs for both
/// insert (`Serialize`) and fetch (`DeserializeOwned`). Built on
/// `clickhouse::RowOwned` (`Row<Value<'a> = Self>`) so the compiler can see
/// that an owned row's associated `Value<'a>` is simply itself.
pub trait ChRow:
    clickhouse::RowOwned + serde::Serialize + serde::de::DeserializeOwned + Send + Sync
{
}
impl<T> ChRow for T where
    T: clickhouse::RowOwned + serde::Serialize + serde::de::DeserializeOwned + Send + Sync
{
}

/// A small, bounded number of retry attempts for `Idempotency::Idempotent`
/// statements. Capped and jittered so a flaky server does not turn one
/// caller request into an unbounded retry storm.
const MAX_IDEMPOTENT_RETRIES: u32 = 3;
const RETRY_BASE_DELAY: Duration = Duration::from_millis(100);

/// One `String` column, aliased `s` — the row shape
/// [`ChClient::query_strings`] decodes.
#[derive(crate::Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct StringRow {
    s: String,
}

/// How far one insert attempt had got when it failed — the only thing that
/// decides whether its commit fate is knowable.
///
/// The boundary is the first `write`, not the server's answer: from there the
/// request is open and the vendored client flushes on its own buffer
/// threshold, so nothing here can tell whether bytes left the process. A
/// failure at or after it may have committed; one before it provably did not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InsertPhase {
    /// The pooled connection's checkout and the target's column-metadata
    /// read: none of the block has been sent, whether the phase ended in the
    /// server's own exception or in the client's deadline.
    BeforeSend,
    /// Writing the rows and ending the insert, whether the phase ended in the
    /// server's answer or in the client's deadline.
    MayHaveSent,
}

pub struct ChClient {
    pool: Arc<ChPool>,
    /// The endpoint every read of this client goes to (issue #803): bound
    /// when the client is built, moved only by [`Self::rebind`].
    read_binding: ReadBinding,
    default_timeout: Duration,
    consistency: ConsistencyConfig,
}

impl ChClient {
    /// Connects `cfg.pool_size` connections and validates them with one
    /// `SELECT 1` (see [`ChPool::connect`], which emits a warning to stderr if
    /// `tls_skip_verify` is set: edge case #5 — this relaxes certificate
    /// verification only, it must never look like a silent plaintext
    /// downgrade).
    pub async fn new(cfg: ChConnConfig) -> Result<Self, ChError> {
        cfg.validate()?;
        let default_timeout = cfg.query_timeout;
        // `cfg.consistency` is `Copy`; read it before the pool consumes `cfg`.
        // `cfg.validate()` above already enforced the quorum/deadline
        // invariant against `query_timeout`.
        let consistency = cfg.consistency;
        let pool = Arc::new(ChPool::connect(cfg).await?);
        let read_binding = ReadBinding::new(pool.bind_read());
        Ok(Self {
            pool,
            read_binding,
            default_timeout,
            consistency,
        })
    }

    /// Builds a `ChClient` over an already-connected, shared `ChPool`
    /// (issue #13's read-API wiring): `AppState` holds exactly one
    /// `Arc<ChPool>`, and every per-request `LogQlEngine` borrows it via a
    /// cheap `Arc::clone` rather than opening a second connection pool
    /// (issue #13 architect plan, resolved open question #1).
    pub fn from_shared_pool(pool: Arc<ChPool>, query_timeout: Duration) -> Self {
        Self::from_shared_pool_avoiding(pool, query_timeout, None)
    }

    /// [`Self::from_shared_pool`], its reads bound to any endpoint but
    /// `avoid` while another exists (issue #803): the client of a request
    /// run again after `avoid` failed.
    pub fn from_shared_pool_avoiding(
        pool: Arc<ChPool>,
        query_timeout: Duration,
        avoid: Option<usize>,
    ) -> Self {
        let read_binding = ReadBinding::new(pool.bind_read_avoiding(avoid));
        Self {
            pool,
            read_binding,
            default_timeout: query_timeout,
            consistency: ConsistencyConfig::default(),
        }
    }

    /// The first server failure a read of this client met since it was
    /// bound (issue #803), or `None`.
    pub fn server_failure(&self) -> Option<ServerFailure> {
        self.read_binding.failure()
    }

    /// Binds this client's reads to a new endpoint and clears its failure
    /// (issue #803). Never call it while one of its reads is open.
    pub fn rebind(&self) {
        self.rebind_avoiding(None);
    }

    /// [`Self::rebind`] to any endpoint but `avoid` while another exists
    /// (issue #803): after `avoid` failed.
    pub fn rebind_avoiding(&self, avoid: Option<usize>) {
        self.read_binding
            .rebind(self.pool.bind_read_avoiding(avoid));
    }

    /// Whether an endpoint other than `endpoint` is healthy (issue #803):
    /// the condition for running a failed read request once more.
    pub fn other_healthy(&self, endpoint: usize) -> bool {
        self.pool.other_healthy(endpoint)
    }

    /// Runs one read request on this client's pool (issue #803): see
    /// [`crate::run_read_request`]. For a caller that holds a client and
    /// no pool — the background passes, which rebind this client at the
    /// start of each attempt.
    pub async fn run_read_request<T, E, F, Fut>(
        &self,
        attempt: F,
        failed_after_success: impl FnOnce(ServerFailure) -> E,
    ) -> Result<T, E>
    where
        F: FnMut(Option<usize>) -> Fut,
        Fut: Future<Output = crate::request::Attempt<T, E>>,
    {
        crate::request::run_read_request(&self.pool, attempt, failed_after_success).await
    }

    /// A read lease on this client's bound endpoint: the lease
    /// [`Self::query_stream`] takes.
    pub(crate) async fn lease_read(&self) -> Result<PooledConn<'_>, ChError> {
        self.pool.get_bound(&self.read_binding).await
    }

    /// Installs a [`ConsistencyConfig`] onto a shared-pool client (issue
    /// #114) — the additive builder the `from_shared_pool` read/write
    /// callers use. **Fallible:** validates the quorum/deadline invariant
    /// against the deadline this client was built with (`default_timeout`,
    /// i.e. `query_timeout`), so no construction path can install a quorum
    /// timeout the insert deadline would preempt (or a dangerous zero). The
    /// `ChClient::new` path enforces the same invariant via
    /// [`ChConnConfig::validate`].
    pub fn with_consistency(mut self, c: ConsistencyConfig) -> Result<Self, ChError> {
        c.validate_for_deadline(self.default_timeout)?;
        self.consistency = c;
        Ok(self)
    }

    /// The complete settings the insert path attaches (issue #114): the
    /// server-side deadline (`max_execution_time`), `async_insert = 0`, and
    /// — when quorum is enabled — the quorum trio. Pool-free and pure, so
    /// [`Self::insert_block`]'s exact injected set is unit-testable directly.
    ///
    /// **`async_insert = 0` is pinned, not inherited (issue #376).**
    /// ClickHouse flipped its default `0 → 1` at 26.2
    /// (`system.settings_changes`: "Enable async inserts by default."), and
    /// that default is a loss for this writer rather than a gain: async
    /// insert exists to coalesce many small inserts, and
    /// [`Self::insert_block`] already sends one columnar block per batch, so
    /// all it adds is the adaptive busy timeout (50–200 ms) on the way to a
    /// flush the batch did not need. Measured on 26.3.17.110 across five
    /// `pulsus-write` live suites, 1 warmup discarded and 5 reps taken
    /// interleaved between two otherwise identical servers: default-on
    /// 23.51 / 22.52 / 23.98 / 22.66 / 23.92 s against pinned-off 12.77 /
    /// 12.69 / 13.38 / 13.72 / 14.51 s — **1.76x slower at the median, with
    /// no overlap between the two distributions.** `logs_tail_live` passes
    /// on both legs with no deadline change, so read-your-write is not what
    /// decides it (`wait_for_async_insert` is `1` on both versions anyway).
    /// It is also the setting #114's quorum reasoning and #143's
    /// effectively-once design both assume.
    ///
    /// **Scope of the pin, stated so it is not read as wider than it is.**
    /// It covers [`Self::insert_block`], which is the whole production data
    /// path — `git grep -n "INSERT INTO" -- crates/*/src` finds no other
    /// production insert (the migration bookkeeping it once found has since
    /// been removed). Verified on the connected server rather than argued:
    /// after a `pulsus-write` live run, every `INSERT INTO \`log_samples\``
    /// / \`log_streams\` / \`metric_series\` / … row in
    /// `system.query_log` carries `Settings['async_insert'] = '0'`, while
    /// the rows that do NOT were test-fixture `execute()` seeding and the
    /// migration bookkeeping since removed, neither of which is this path.
    #[cfg(test)]
    fn insert_settings_of(c: &ConsistencyConfig, timeout: Duration) -> QuerySettings {
        Self::insert_settings_with(c, timeout, &QuerySettings::new())
    }

    /// [`Self::insert_settings_of`] plus a caller's `extra` settings, placed
    /// after `async_insert` and before the deadline (issue #560: the span
    /// table's deduplication pins).
    fn insert_settings_with(
        c: &ConsistencyConfig,
        timeout: Duration,
        extra: &QuerySettings,
    ) -> QuerySettings {
        let mut s = c.insert_settings().set("async_insert", 0);
        for (k, v) in extra.entries() {
            s = s.set(k, v);
        }
        s.with_max_execution_time(timeout)
    }

    /// The complete settings the read path attaches (issue #114): the
    /// caller's `base` settings, plus — when enabled —
    /// `select_sequential_consistency`, plus the stream deadline. Pool-free
    /// and pure, so [`Self::query_stream`]'s exact injected set is
    /// unit-testable directly.
    fn read_settings_of(
        c: &ConsistencyConfig,
        base: &QuerySettings,
        timeout: Duration,
    ) -> QuerySettings {
        c.apply_read(base.clone()).with_max_execution_time(timeout)
    }

    /// Columnar block insert into `table`.
    ///
    /// **Never auto-retried.** `metric_samples`/`log_samples` are append-only
    /// `MergeTree` tables; silently retrying a partially-delivered insert
    /// block would duplicate rows and, for tier tables fed by materialized
    /// views, permanently inflate `val_sum`/`val_count`
    /// (docs/schemas.md §2.2) — an irreversible corruption. Nothing at this
    /// layer makes a repeated insert safe: the guard against a *client's*
    /// retry is one layer up, where `pulsus-write` suppresses a
    /// content-identical push inside `PULSUS_INGEST_DEDUP_WINDOW` before it
    /// reaches this call at all (issue #494). On error, the classified
    /// [`ChError`] is returned; the caller owns idempotency (typically:
    /// drop the batch and rely on at-least-once upstream redelivery, per
    /// `pulsus-write`'s policy, out of scope here).
    ///
    /// Bounded by both a server-side `max_execution_time` (on the `INSERT`
    /// statement) and one client-side deadline over the whole
    /// create/write/end sequence, awaited phase by phase against the same
    /// instant so that which phase it fired in is known.
    ///
    /// **What decides the class is the phase, not the error.** An insert's
    /// commit fate is unknown from the moment any of the
    /// block may have left the process: the server may have applied part of it,
    /// and a source table whose derived tables are maintained by materialized
    /// view can have committed its own block before a view on it throws. So
    /// every failure from the write-or-end phase — retryable or not — is
    /// [`ChError::InsertUncertain`], which no caller auto-retries. Only a
    /// failure raised **before** the block was sent, which is the pooled
    /// connection's checkout and the target's column-metadata read, is genuine
    /// pre-commit poison and keeps its precise class: nothing was committed,
    /// so it is not uncertain, merely wrong, and the caller may release
    /// whatever it was holding for a retry.
    ///
    /// Where the phase itself is not knowable it over-reports, and it never
    /// under-reports: a client-side row serialization failure on the first row
    /// is reported unknown although nothing was sent, because the vendored
    /// client buffers and flushes on its own threshold and nothing here can
    /// tell which side of that the failure fell on. A caller told "unknown"
    /// for a block that was never stored loses a retry; a caller told "not
    /// stored" for a block that was stored double-counts it. So an
    /// over-report is not free, and nothing is over-reported whose phase is
    /// knowable — the client's own deadline is classified by the phase it
    /// fired in, not by the worse of the two.
    pub async fn insert_block<R: ChRow>(&self, table: &str, rows: &[R]) -> Result<(), ChError> {
        self.insert_block_with(table, rows, &QuerySettings::new())
            .await
    }

    /// [`Self::insert_block`] with `extra` settings on this one insert
    /// (issue #560).
    pub async fn insert_block_with<R: ChRow>(
        &self,
        table: &str,
        rows: &[R],
        extra: &QuerySettings,
    ) -> Result<(), ChError> {
        self.insert_parts_with(table, &[rows], extra).await
    }

    /// [`Self::insert_block_with`] over `parts`, written in order into one
    /// request as one block (issue #494 part 2: a landing batch is its
    /// members' rows, and no row is copied to join them).
    pub async fn insert_parts_with<R: ChRow>(
        &self,
        table: &str,
        parts: &[&[R]],
        extra: &QuerySettings,
    ) -> Result<(), ChError> {
        let conn = self.pool.get(StatementClass::Write).await?;
        // Issue #114: the whole attached set — the server deadline plus,
        // when quorum is enabled, the quorum trio — is the single testable
        // `insert_settings_of`, applied pair-by-pair (the `Insert` builder
        // has no typed settings helper).
        let settings = Self::insert_settings_with(&self.consistency, self.default_timeout, extra);
        // ONE client-side deadline over the whole create/write/end sequence,
        // awaited in its two phases against the same instant: `timeout_at`
        // shares the deadline, so the two waits cannot add up to more than
        // `default_timeout`, and the phase the deadline fires in is the phase
        // whose wait returned it. A single wait over the whole sequence cannot
        // say that, and classifying a deadline that fired during the metadata
        // read as post-send costs a caller the pre-send retry it was entitled
        // to.
        let deadline = tokio::time::Instant::now() + self.default_timeout;
        let timed_out =
            || ChError::Timeout(format!("insert_block exceeded {:?}", self.default_timeout));

        // Phase one, acquisition: `insert` reads the target's column metadata
        // and returns a builder. None of the block has been sent while this
        // runs, so every ending here — the server's own exception and the
        // deadline alike — is `BeforeSend`.
        let acquire = async {
            let mut insert = conn.client().insert::<R>(table).await?;
            for (k, v) in settings.iter() {
                insert = insert.with_setting(k, v);
            }
            Ok::<_, clickhouse::error::Error>(insert)
        };
        let acquired = match tokio::time::timeout_at(deadline, acquire).await {
            Ok(Ok(insert)) => Ok(insert),
            Ok(Err(e)) => Err((InsertPhase::BeforeSend, ChError::from(e))),
            Err(_elapsed) => Err((InsertPhase::BeforeSend, timed_out())),
        };

        // Phase two, write and end: from the first `write` the request is open
        // and the client flushes on its own buffer threshold, so anything from
        // here on may already be on the wire — every ending is `MayHaveSent`,
        // the deadline included.
        let result = match acquired {
            Err(pair) => Err(pair),
            Ok(mut insert) => {
                let send = async {
                    for row in parts.iter().flat_map(|part| part.iter()) {
                        insert.write(row).await?;
                    }
                    insert.end().await
                };
                match tokio::time::timeout_at(deadline, send).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err((InsertPhase::MayHaveSent, ChError::from(e))),
                    Err(_elapsed) => Err((InsertPhase::MayHaveSent, timed_out())),
                }
            }
        };

        let (phase, err) = match result {
            Ok(()) => return Ok(()),
            Err(pair) => pair,
        };

        // Transport-class failure demotes this endpoint so the next insert
        // steers to a healthy one (no-op for logic errors — the guard lives in
        // `report_transport_failure`).
        conn.report_transport_failure(&err);
        match phase {
            InsertPhase::MayHaveSent => Err(ChError::InsertUncertain(err.to_string())),
            // Provably before the block was sent, so its class is preserved:
            // a retryable one is still retryable, and poison is still poison.
            InsertPhase::BeforeSend => Err(err),
        }
    }

    /// Streaming SELECT. Settings are injected per-statement (never on the
    /// pooled connection itself — edge case #2). The returned stream owns
    /// its pooled-connection lease via RAII ([`ChRowStream`]): the
    /// connection is held for the whole stream lifetime and returned to
    /// the pool when the stream is dropped, whether by full consumption,
    /// early return, or cancellation.
    ///
    /// Also carries an overall stream deadline (`self.default_timeout`,
    /// i.e. `PULSUS_QUERY_TIMEOUT`): see [`ChRowStream`]'s doc comment for
    /// the guarantee this provides.
    pub async fn query_stream<R: ChRow>(
        &self,
        sql: &str,
        s: &QuerySettings,
    ) -> Result<ChRowStream<'_, R>, ChError> {
        let conn = self.lease_read().await?;
        // Issue #114: caller settings + (when enabled)
        // `select_sequential_consistency` + the stream deadline, as the
        // single testable `read_settings_of`.
        let settings = Self::read_settings_of(&self.consistency, s, self.default_timeout);
        let cursor = settings
            .apply_to_query(conn.client().query(sql))
            .fetch::<R>()?;
        Ok(ChRowStream {
            cursor,
            deadline: Box::pin(tokio::time::sleep(self.default_timeout)),
            done: false,
            timeout: self.default_timeout,
            conn,
        })
    }

    /// Reads a statement whose result is one `String` column aliased `s`, as
    /// a `Vec<String>`.
    ///
    /// A convenience over [`Self::query_stream`] for the several callers that
    /// want a list of names and nothing else: without it each one declares
    /// its own one-field row type, which needs the `clickhouse` derive in
    /// scope in that crate.
    pub async fn query_strings(
        &self,
        sql: &str,
        settings: &QuerySettings,
    ) -> Result<Vec<String>, ChError> {
        let mut stream = self.query_stream::<StringRow>(sql, settings).await?;
        let mut out = Vec::new();
        while let Some(row) = stream.next().await {
            out.push(row?.s);
        }
        Ok(out)
    }

    /// DDL / maintenance statement. Settings are injected per-statement.
    ///
    /// **Carries the same `async_insert = 0` pin as the insert path**
    /// ([`Self::insert_settings_of`] explains why we do not want async
    /// inserts at all). It matters here because this method also runs
    /// `INSERT` statements — test fixtures seed rows through it and read
    /// them back, which is read-your-write. (The schema bookkeeping it was
    /// first pinned for has since been removed.) From ClickHouse 26.2 the server
    /// default is `1`, so without this pin those inserts would be buffered
    /// and a read could miss rows that have not landed. Measured
    /// (issue #376): `logs_tail_live`, whose fixtures seed through this
    /// method, failed **3 of 6 runs** against a 26.3 server at the stock
    /// default and passed **6 of 6** against an otherwise identical 26.3
    /// server with `async_insert = 0`. Harmless on the DDL statements that
    /// make up the rest of this method's traffic.
    ///
    /// The wrapper auto-retries a *retryable* [`ChError`] only when
    /// `idem == Idempotency::Idempotent`, up to [`MAX_IDEMPOTENT_RETRIES`]
    /// with capped exponential backoff. `Idempotency::NonIdempotent`
    /// statements (e.g. an `INSERT ... SELECT` backfill, which can
    /// duplicate rows and permanently inflate tier aggregates on replay)
    /// are never retried — the classified error is surfaced immediately.
    /// Statement-level idempotency (e.g. `CREATE TABLE IF NOT EXISTS`) is
    /// the caller's responsibility; this flag governs wrapper-level retry
    /// only.
    pub async fn execute(
        &self,
        sql: &str,
        s: &QuerySettings,
        idem: Idempotency,
    ) -> Result<(), ChError> {
        let settings = s
            .clone()
            .set("async_insert", 0)
            .with_max_execution_time(self.default_timeout);
        let mut attempt = 0u32;
        loop {
            let conn = self.pool.get(StatementClass::Write).await?;
            let q = settings.apply_to_query(conn.client().query(sql));
            let result = tokio::time::timeout(self.default_timeout, q.execute())
                .await
                .map_err(|_| {
                    ChError::Timeout(format!("execute exceeded {:?}", self.default_timeout))
                })
                .and_then(|inner| inner.map_err(ChError::from));

            // Demote this endpoint on a transport-class failure before
            // deciding whether to retry, so the retry's `get()` re-lands on a
            // healthy endpoint (no-op for logic errors — guarded inside).
            if let Err(ref e) = result {
                conn.report_transport_failure(e);
            }

            match result {
                Ok(()) => return Ok(()),
                Err(e)
                    if idem == Idempotency::Idempotent
                        && e.is_retryable()
                        && attempt < MAX_IDEMPOTENT_RETRIES =>
                {
                    attempt += 1;
                    tokio::time::sleep(RETRY_BASE_DELAY * attempt).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Health probe (`SELECT 1`), always idempotent, never retried at this
    /// layer (callers polling health typically re-invoke on their own
    /// cadence).
    pub async fn ping(&self) -> Result<(), ChError> {
        self.pool.ping().await
    }
}

/// A streaming SELECT result. Owns the pooled-connection lease for its
/// entire lifetime (RAII) — dropping the stream, whether from full
/// consumption or early cancellation, releases the lease back to the pool.
///
/// Also enforces an **overall stream deadline**: a single `tokio::time::Sleep`
/// timer, started when the stream is created and polled first on every
/// `poll_next`, bounding the connection lease's total lifetime regardless of
/// how much progress the underlying read is making. This directly addresses
/// the pool-exhaustion risk of a stuck/slow `SELECT` holding its lease
/// forever (issue #3 fix plan, finding 2) — a per-chunk progress timeout
/// would not bound total lease time the way an overall deadline does. The
/// tradeoff: a legitimately long, still-progressing stream is cut at
/// `PULSUS_QUERY_TIMEOUT` with a retryable [`ChError::Timeout`] — this is
/// the intended "hard per-query timeout" semantic, not a bug. Reads are
/// idempotent, so retryable classification is correct here (unlike
/// `insert_block`'s uncertain-fate downgrade).
pub struct ChRowStream<'a, R> {
    cursor: clickhouse::query::RowCursor<R>,
    deadline: Pin<Box<tokio::time::Sleep>>,
    done: bool,
    timeout: Duration,
    conn: PooledConn<'a>,
}

impl<R> ChRowStream<'_, R> {
    /// The server-reported number of bytes read by the query so far,
    /// from ClickHouse's `X-ClickHouse-Summary` (`Option::None` until a
    /// summary frame has been observed). The clickhouse 0.15.1 crate
    /// captures the summary from the **initial** response header, so this
    /// only reflects the FINAL scanned-byte total once the query ran with
    /// `wait_end_of_query=1` and the stream has been fully drained — the
    /// invariant the streams fetch-until-limit paging loop relies on to
    /// keep its cumulative-scan budget accounting sound (issue #90).
    pub fn read_bytes(&self) -> Option<u64> {
        self.cursor
            .summary()
            .and_then(clickhouse::QuerySummary::read_bytes)
    }
}

impl<R> Stream for ChRowStream<'_, R>
where
    R: ChRow,
{
    type Item = Result<R, ChError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // `RowCursor<R>` contains no self-referential fields (a byte buffer,
        // a response handle, and `PhantomData`), so it is `Unpin` and a
        // plain field projection is sound.
        let this = self.get_mut();

        if this.done {
            return Poll::Ready(None);
        }

        // Poll the deadline first: it registers the task waker, so it can
        // interrupt a stalled read even if the cursor itself would never
        // wake the task on its own.
        if this.deadline.as_mut().poll(cx).is_ready() {
            this.done = true;
            return Poll::Ready(Some(Err(ChError::Timeout(format!(
                "query_stream exceeded {:?}",
                this.timeout
            )))));
        }

        let cursor = Pin::new(&mut this.cursor);
        match Stream::poll_next(cursor, cx) {
            Poll::Ready(Some(Ok(row))) => Poll::Ready(Some(Ok(row))),
            Poll::Ready(Some(Err(e))) => {
                this.done = true;
                let err = ChError::from(e);
                // A server failure mid-stream demotes this endpoint and marks
                // the read request (issue #803; no-op for timeouts, decode
                // and logic errors — guarded in `report_read_failure`). The
                // deadline arm above is intentionally excluded: a
                // self-imposed client timeout does not mean the endpoint is
                // unhealthy.
                this.conn.report_read_failure(&err);
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => {
                this.done = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_idempotent_retries_is_bounded() {
        const { assert!(MAX_IDEMPOTENT_RETRIES > 0 && MAX_IDEMPOTENT_RETRIES <= 10) };
    }

    /// AC3 (issue #114): the default (quorum off) insert emits only the
    /// deadline and the pinned `async_insert = 0` (issue #376) — nothing
    /// else.
    #[test]
    fn insert_settings_of_default_emits_the_deadline_and_the_async_insert_pin() {
        let s =
            ChClient::insert_settings_of(&ConsistencyConfig::default(), Duration::from_secs(120));
        assert_eq!(
            s.render_suffix(),
            " SETTINGS async_insert = 0, max_execution_time = 120.000"
        );
    }

    /// Issue #560: the span inserts' two deduplication pins come after
    /// `async_insert` and before the deadline, and nothing else moves.
    #[test]
    fn insert_settings_with_the_span_pins_emits_both_before_the_deadline() {
        let s = ChClient::insert_settings_with(
            &ConsistencyConfig::default(),
            Duration::from_secs(120),
            &QuerySettings::deduplicate_through_views(),
        );
        assert_eq!(
            s.render_suffix(),
            " SETTINGS async_insert = 0, deduplicate_insert = enable, \
             deduplicate_blocks_in_dependent_materialized_views = 1, max_execution_time = 120.000"
        );
    }

    /// The metrics landing insert carries the full setting set, in order:
    /// the two block-deduplication pins, the block's own minted token, and
    /// the seven settings that decide how many blocks the server forms out
    /// of the request and whether it deduplicates them, and the four that
    /// decide what a view does with it, and the five that keep a landing
    /// insert on its shard and stored before it answers (issue #494 part
    /// 1b) — between
    /// `async_insert` and the deadline, with nothing else moving. A path that
    /// sent only some of them would leave a resend storing the block twice,
    /// or one push becoming two blocks.
    #[test]
    fn the_landing_insert_emits_the_full_setting_set_in_order() {
        let s = ChClient::insert_settings_with(
            &ConsistencyConfig::default(),
            Duration::from_secs(120),
            &QuerySettings::landing_insert("t1", 1_048_576),
        );
        assert_eq!(
            s.render_suffix(),
            " SETTINGS async_insert = 0, deduplicate_insert = enable, \
             deduplicate_blocks_in_dependent_materialized_views = 1, \
             insert_deduplication_token = t1, max_insert_block_size = 1048576, \
             max_insert_block_size_bytes = 0, input_format_max_block_size_bytes = 0, \
             min_insert_block_size_rows = 1048576, min_insert_block_size_bytes = 0, \
             input_format_connection_handling = 0, input_format_max_block_wait_ms = 0, \
             materialized_views_ignore_errors = 0, \
             ignore_materialized_views_with_dropped_target_table = 0, \
             min_insert_block_size_rows_for_materialized_views = 0, \
             min_insert_block_size_bytes_for_materialized_views = 0, \
             distributed_foreground_insert = 1, insert_shard_id = 0, \
             skip_unavailable_shards = 0, prefer_localhost_replica = 0, \
             timeout_overflow_mode = throw, \
             max_partitions_per_insert_block = 100, \
             throw_on_max_partitions_per_insert_block = 1, \
             max_execution_time = 120.000"
        );
    }

    /// Issue #376: the writer pins `async_insert = 0` rather than
    /// inheriting the server default, which flipped to `1` at ClickHouse
    /// 26.2. Measured 1.76x slower on this writer's own batches — see
    /// [`ChClient::insert_settings_of`]. Deleting the pin reddens here.
    #[test]
    fn every_insert_pins_async_insert_off_whatever_the_server_default_is() {
        for c in [
            ConsistencyConfig::default(),
            ConsistencyConfig {
                insert_quorum: 2,
                insert_quorum_parallel: true,
                insert_quorum_timeout: Duration::from_secs(5),
                select_sequential_consistency: true,
            },
        ] {
            let s = ChClient::insert_settings_of(&c, Duration::from_secs(120));
            assert_eq!(s.get("async_insert"), Some("0"), "{c:?}");
        }
    }

    /// AC4 (issue #114): the default (sequential consistency off) select
    /// emits ONLY the deadline — byte-for-byte the pre-#114 select.
    #[test]
    fn read_settings_of_default_emits_only_the_deadline() {
        let s = ChClient::read_settings_of(
            &ConsistencyConfig::default(),
            &QuerySettings::new(),
            Duration::from_secs(120),
        );
        assert_eq!(s.render_suffix(), " SETTINGS max_execution_time = 120.000");
    }

    /// AC4a (issue #114): an enabled quorum insert emits the trio (timeout
    /// in ms) alongside the deadline.
    #[test]
    fn insert_settings_of_emits_the_quorum_trio_when_enabled() {
        let c = ConsistencyConfig {
            insert_quorum: 3,
            insert_quorum_parallel: false,
            insert_quorum_timeout: Duration::from_secs(5),
            select_sequential_consistency: false,
        };
        let s = ChClient::insert_settings_of(&c, Duration::from_secs(120));
        assert_eq!(s.get("insert_quorum"), Some("3"));
        assert_eq!(s.get("insert_quorum_parallel"), Some("0"));
        assert_eq!(s.get("insert_quorum_timeout"), Some("5000"));
        assert_eq!(s.get("max_execution_time"), Some("120.000"));
    }

    /// AC4b (issue #114): an enabled sequential-consistency read emits the
    /// setting AND preserves the caller's engine budgets + deadline.
    #[test]
    fn read_settings_of_preserves_caller_settings_and_adds_sequential_consistency() {
        let c = ConsistencyConfig {
            select_sequential_consistency: true,
            ..ConsistencyConfig::default()
        };
        let base = QuerySettings::new().set("max_bytes_to_read", 42u64);
        let s = ChClient::read_settings_of(&c, &base, Duration::from_secs(120));
        assert_eq!(s.get("select_sequential_consistency"), Some("1"));
        assert_eq!(s.get("max_bytes_to_read"), Some("42"));
        assert_eq!(s.get("max_execution_time"), Some("120.000"));
    }

    // ---- issue #803: one read request, one server ----

    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::config::ReadSpread;
    use crate::pool::test_pool;
    use crate::request::{Attempt, run_read_request};

    fn pool3() -> Arc<ChPool> {
        Arc::new(test_pool(
            vec![
                (Some("a"), "http://a:8123"),
                (Some("b"), "http://b:8123"),
                (Some("c"), "http://c:8123"),
            ],
            Some("a"),
            ReadSpread::All,
            8,
        ))
    }

    fn client(pool: &Arc<ChPool>) -> ChClient {
        ChClient::from_shared_pool(Arc::clone(pool), Duration::from_secs(5))
    }

    fn counts(pool: &ChPool) -> Vec<u64> {
        pool.endpoint_selection_counts()
            .into_iter()
            .map(|(_, n)| n)
            .collect()
    }

    fn delta(before: &[u64], after: &[u64]) -> Vec<u64> {
        before.iter().zip(after).map(|(b, a)| a - b).collect()
    }

    /// One read through `client`: the empty URL fails at `Url::parse`
    /// after the lease and before any I/O, so the result is ignored.
    async fn read(client: &ChClient) {
        let _ = client
            .query_stream::<StringRow>("SELECT 1", &QuerySettings::new())
            .await;
    }

    #[tokio::test]
    async fn each_client_binds_its_reads_to_one_endpoint() {
        let pool = pool3();
        let c1 = client(&pool);
        let c2 = client(&pool);
        let before = counts(&pool);
        read(&c1).await;
        read(&c1).await;
        let mid = counts(&pool);
        read(&c2).await;
        read(&c2).await;
        let after = counts(&pool);
        assert_eq!(delta(&before, &mid), vec![2, 0, 0], "client 1");
        assert_eq!(delta(&mid, &after), vec![0, 2, 0], "client 2");
    }

    #[tokio::test]
    async fn insert_is_a_write_whatever_the_binding() {
        let pool = pool3();
        let c1 = client(&pool);
        read(&c1).await;
        // Client 2 is bound to the second endpoint, `b`.
        let c2 = client(&pool);
        let before = counts(&pool);
        for _ in 0..2 {
            let _ = c2.insert_block::<StringRow>("t", &[]).await;
        }
        assert_eq!(delta(&before, &counts(&pool)), vec![2, 0, 0]);
    }

    #[tokio::test]
    async fn execute_is_a_write_whatever_the_binding() {
        let pool = pool3();
        let _c1 = client(&pool);
        let c2 = client(&pool);
        let before = counts(&pool);
        for _ in 0..2 {
            let _ = c2
                .execute(
                    "SELECT 1",
                    &QuerySettings::new(),
                    Idempotency::NonIdempotent,
                )
                .await;
        }
        assert_eq!(delta(&before, &counts(&pool)), vec![2, 0, 0]);
    }

    #[tokio::test]
    async fn ping_is_a_write_whatever_the_binding() {
        let pool = pool3();
        let _c1 = client(&pool);
        let c2 = client(&pool);
        let before = counts(&pool);
        for _ in 0..2 {
            let _ = c2.ping().await;
        }
        assert_eq!(delta(&before, &counts(&pool)), vec![2, 0, 0]);
    }

    #[tokio::test]
    async fn three_concurrent_reads_stay_on_one_server_through_a_failover_and_the_rerun_moves_all_of_them()
     {
        let pool = pool3();
        let calls = AtomicUsize::new(0);
        let seen: Mutex<Vec<Vec<usize>>> = Mutex::new(Vec::new());
        let (pool_ref, calls_ref, seen_ref) = (&pool, &calls, &seen);
        let out = run_read_request(
            &pool,
            move |avoid| async move {
                let n = calls_ref.fetch_add(1, Ordering::SeqCst) + 1;
                let c = ChClient::from_shared_pool_avoiding(
                    Arc::clone(pool_ref),
                    Duration::from_secs(5),
                    avoid,
                );
                let (l1, l2, l3) = tokio::join!(c.lease_read(), c.lease_read(), c.lease_read());
                let (l1, l2, l3) = (
                    l1.expect("lease 1"),
                    l2.expect("lease 2"),
                    l3.expect("lease 3"),
                );
                let mut eps = vec![l1.endpoint(), l2.endpoint(), l3.endpoint()];
                if n == 1 {
                    l2.report_read_failure(&ChError::Io("reset".to_string()));
                    let l4 = c.lease_read().await.expect("lease 4");
                    eps.push(l4.endpoint());
                }
                drop((l1, l2, l3));
                seen_ref.lock().expect("seen").push(eps);
                Attempt {
                    result: Ok::<usize, ChError>(n),
                    failure: c.server_failure(),
                }
            },
            ChError::from,
        )
        .await;
        assert_eq!(out.expect("the second attempt is accepted"), 2);
        let seen = seen.lock().expect("seen").clone();
        assert_eq!(seen.first(), Some(&vec![0, 0, 0, 0]), "attempt 1: {seen:?}");
        assert_eq!(seen.get(1), Some(&vec![2, 2, 2]), "attempt 2: {seen:?}");
        assert_eq!(counts(&pool), vec![4, 0, 3]);
        assert!(!pool.endpoint_health()[0].1, "a demoted");
    }

    #[tokio::test]
    async fn rebind_moves_to_a_healthy_server_and_clears_the_failure() {
        let pool = pool3();
        let c = client(&pool);
        {
            let lease = c.lease_read().await.expect("lease");
            assert_eq!(lease.endpoint(), 0);
            lease.report_read_failure(&ChError::Io("reset".to_string()));
        }
        assert!(c.server_failure().is_some(), "the failure is recorded");
        c.rebind();
        assert_eq!(c.server_failure(), None, "rebind clears the failure");
        let lease = c.lease_read().await.expect("lease");
        assert_eq!(lease.endpoint(), 2);
    }

    // ---- issue #803, code review round 1: a rerun never goes back ----

    fn pool2() -> Arc<ChPool> {
        Arc::new(test_pool(
            vec![(None, "http://a:8123"), (None, "http://b:8123")],
            None,
            ReadSpread::All,
            8,
        ))
    }

    /// R7: the server that failed is promoted again before the rerun, and
    /// another request's bind puts the rotation back on it. The rerun is
    /// bound to the other server all the same, and its answer is used.
    #[tokio::test]
    async fn a_rerun_never_binds_the_server_that_failed_even_once_it_is_promoted() {
        let pool = pool2();
        let seen: Mutex<Vec<usize>> = Mutex::new(Vec::new());
        let calls = AtomicUsize::new(0);
        let (pool_ref, seen_ref, calls_ref) = (&pool, &seen, &calls);
        let out = run_read_request(
            &pool,
            move |avoid| async move {
                let n = calls_ref.fetch_add(1, Ordering::SeqCst) + 1;
                let c = ChClient::from_shared_pool_avoiding(
                    Arc::clone(pool_ref),
                    Duration::from_secs(5),
                    avoid,
                );
                let lease = c.lease_read().await.expect("lease");
                let ep = lease.endpoint();
                seen_ref.lock().expect("seen").push(ep);
                if ep == 0 {
                    // Server `a` fails whenever it is asked.
                    lease.report_read_failure(&ChError::Io("reset".to_string()));
                }
                drop(lease);
                if n == 1 {
                    // Another request binds meanwhile, and `a` is promoted.
                    let _other = client(pool_ref);
                    crate::pool::promote_for_test(pool_ref, 0);
                }
                Attempt {
                    result: Ok::<usize, ChError>(n),
                    failure: c.server_failure(),
                }
            },
            ChError::from,
        )
        .await;
        let seen = seen.lock().expect("seen").clone();
        assert_eq!(seen, vec![0, 1], "the rerun is bound to `b`");
        assert_eq!(out.expect("the rerun on `b` is accepted"), 2);
    }

    /// K8: a client rebound after its server failed is not bound to that
    /// server again, even once it is promoted and the rotation is back on
    /// it — the tail's rebind between polls.
    #[tokio::test]
    async fn rebind_avoiding_skips_the_failed_server_even_once_promoted() {
        let pool = pool2();
        let c = client(&pool);
        {
            let lease = c.lease_read().await.expect("lease");
            assert_eq!(lease.endpoint(), 0);
            lease.report_read_failure(&ChError::Io("reset".to_string()));
        }
        let _other = client(&pool);
        crate::pool::promote_for_test(&pool, 0);
        c.rebind_avoiding(Some(0));
        assert_eq!(c.lease_read().await.expect("lease").endpoint(), 1);
    }
}
