//! `ChClient`: the crate-agnostic facade over the winning `clickhouse`
//! (HTTP) crate (docs/decisions/0001-clickhouse-client.md).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;

use crate::config::{ChConnConfig, ConsistencyConfig};
use crate::error::{ChError, Idempotency};
use crate::pool::{ChPool, PooledConn};
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

pub struct ChClient {
    pool: Arc<ChPool>,
    default_timeout: Duration,
    consistency: ConsistencyConfig,
}

impl ChClient {
    /// Connects `cfg.pool_size` connections and validates them with one
    /// `SELECT 1` (see [`ChPool::connect`]). Emits a warning to stderr if
    /// `tls_skip_verify` is set (edge case #5 — this relaxes certificate
    /// verification only, it must never look like a silent plaintext
    /// downgrade).
    pub async fn new(cfg: ChConnConfig) -> Result<Self, ChError> {
        cfg.validate()?;
        if cfg.tls_skip_verify {
            eprintln!(
                "pulsus-clickhouse: CLICKHOUSE_TLS_SKIP_VERIFY=true — certificate verification \
                 is DISABLED for this ClickHouse connection. The connection is still encrypted \
                 (TLS is not downgraded to plaintext); only the peer certificate is unchecked."
            );
        }
        let default_timeout = cfg.query_timeout;
        // `cfg.consistency` is `Copy`; read it before the pool consumes `cfg`.
        // `cfg.validate()` above already enforced the quorum/deadline
        // invariant against `query_timeout`.
        let consistency = cfg.consistency;
        let pool = ChPool::connect(cfg).await?;
        Ok(Self {
            pool: Arc::new(pool),
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
        Self {
            pool,
            default_timeout: query_timeout,
            consistency: ConsistencyConfig::default(),
        }
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
    /// server-side deadline (`max_execution_time`) plus — when quorum is
    /// enabled — the quorum trio. Pool-free and pure, so
    /// [`Self::insert_block`]'s exact injected set is unit-testable directly.
    fn insert_settings_of(c: &ConsistencyConfig, timeout: Duration) -> QuerySettings {
        c.insert_settings().with_max_execution_time(timeout)
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
    /// `MergeTree` tables that are exact-once only by writer batch atomicity
    /// (docs/schemas.md §8); silently retrying a partially-delivered insert
    /// block would duplicate rows and, for tier tables fed by materialized
    /// views, permanently inflate `val_sum`/`val_count`
    /// (docs/schemas.md §2.2) — an irreversible corruption. On error, the
    /// classified [`ChError`] is returned; the caller owns idempotency
    /// (typically: drop the batch and rely on at-least-once upstream
    /// redelivery, per `pulsus-write`'s policy, out of scope here).
    ///
    /// Bounded by both a server-side `max_execution_time` (on the `INSERT`
    /// statement) and a client-side `tokio::time::timeout` wrapping the
    /// whole create/write/end sequence. Because a timed-out or
    /// network-aborted insert has **unknown commit fate** (the server may
    /// have partially applied it), any retryable-class failure here is
    /// downgraded to the non-retryable [`ChError::InsertUncertain`] — this
    /// is the rule that keeps a caller from ever auto-retrying an insert
    /// whose effect is uncertain. Genuine pre-commit poison (bad SQL,
    /// decode failure) is surfaced unchanged: nothing was committed, so it
    /// is not uncertain, merely wrong.
    pub async fn insert_block<R: ChRow>(&self, table: &str, rows: &[R]) -> Result<(), ChError> {
        let conn = self.pool.get().await?;
        // Issue #114: the whole attached set — the server deadline plus,
        // when quorum is enabled, the quorum trio — is the single testable
        // `insert_settings_of`, applied pair-by-pair (the `Insert` builder
        // has no typed settings helper).
        let settings = Self::insert_settings_of(&self.consistency, self.default_timeout);
        let fut = async {
            let mut insert = conn.client().insert::<R>(table).await?;
            for (k, v) in settings.iter() {
                insert = insert.with_setting(k, v);
            }
            for row in rows {
                insert.write(row).await?;
            }
            insert.end().await
        };
        let result = tokio::time::timeout(self.default_timeout, fut)
            .await
            .map_err(|_| {
                ChError::Timeout(format!("insert_block exceeded {:?}", self.default_timeout))
            })
            .and_then(|inner| inner.map_err(ChError::from));

        match result {
            Ok(()) => Ok(()),
            Err(e) => {
                // Transport-class failure demotes this endpoint so the next
                // insert steers to a healthy one (no-op for logic errors —
                // the guard lives in `report_transport_failure`).
                conn.report_transport_failure(&e);
                // Uncertain-fate downgrade: any retryable failure during an
                // insert must NOT reach a caller as retryable (would
                // duplicate the block on replay).
                if e.is_retryable() {
                    Err(ChError::InsertUncertain(e.to_string()))
                } else {
                    Err(e) // genuine pre-commit poison, surfaced precisely
                }
            }
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
        let conn = self.pool.get().await?;
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

    /// DDL / maintenance statement. Settings are injected per-statement.
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
        let settings = s.clone().with_max_execution_time(self.default_timeout);
        let mut attempt = 0u32;
        loop {
            let conn = self.pool.get().await?;
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
                // A transport-class failure mid-stream demotes this endpoint
                // so the next query fails over (no-op for decode/logic errors
                // — guarded in `report_transport_failure`). The deadline arm
                // above is intentionally excluded: a self-imposed client
                // timeout does not mean the endpoint is unhealthy.
                this.conn.report_transport_failure(&err);
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

    /// AC3 (issue #114): the default (quorum off) insert emits ONLY the
    /// deadline — byte-for-byte the pre-#114 insert.
    #[test]
    fn insert_settings_of_default_emits_only_the_deadline() {
        let s =
            ChClient::insert_settings_of(&ConsistencyConfig::default(), Duration::from_secs(120));
        assert_eq!(s.render_suffix(), " SETTINGS max_execution_time = 120.000");
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
}
