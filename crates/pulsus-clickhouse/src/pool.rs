//! `ChPool`: a fixed-size pool of `pool_size` connections over the winning
//! `clickhouse` (HTTP) crate.
//!
//! The `clickhouse` crate's `Client` already shares one keep-alive HTTP
//! transport across clones (its own internal `hyper` connection pool), and
//! carries no server-side session state between requests (every setting is
//! sent per-request, never `SET` on a persistent session) — see
//! docs/decisions/0001-clickhouse-client.md. `ChPool` therefore adapts the
//! architect's fixed-size, health-checked, RAII-lease pool contract onto
//! that transport: its primary job is bounding **concurrent request count**
//! to `pool_size` (a semaphore), plus a uniform lease API so callers do not
//! need to know which crate ships underneath.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Semaphore, SemaphorePermit};

use crate::config::{ChConnConfig, ChProto};
use crate::error::ChError;
use crate::tls;

/// A connection is pinged before being handed out if it has been idle
/// longer than this.
const STALE_AFTER: Duration = Duration::from_secs(30);

struct Slot {
    client: clickhouse::Client,
    last_checked: Mutex<Instant>,
}

pub struct ChPool {
    slots: Vec<Slot>,
    semaphore: Arc<Semaphore>,
    next_slot: AtomicU64,
    query_timeout: Duration,
}

impl ChPool {
    /// Connects (lazily — the `clickhouse` crate dials on first request) a
    /// pool of `cfg.pool_size` client handles sharing one HTTP transport.
    pub async fn connect(cfg: ChConnConfig) -> Result<Self, ChError> {
        cfg.validate()?;
        let base = build_base_client(&cfg)?;
        let mut slots = Vec::with_capacity(cfg.pool_size);
        for _ in 0..cfg.pool_size {
            slots.push(Slot {
                client: base.clone(),
                last_checked: Mutex::new(Instant::now() - STALE_AFTER),
            });
        }
        let pool = Self {
            semaphore: Arc::new(Semaphore::new(slots.len())),
            slots,
            next_slot: AtomicU64::new(0),
            query_timeout: cfg.query_timeout,
        };
        // Fail fast: a misconfigured server/credentials should surface at
        // startup, not on the first caller request.
        pool.ping().await?;
        Ok(pool)
    }

    /// Checks out a connection, blocking (bounded by the pool's own
    /// `query_timeout`) if all `pool_size` connections are already leased.
    /// Pings the underlying connection first if it has been idle past
    /// [`STALE_AFTER`].
    pub async fn get(&self) -> Result<PooledConn<'_>, ChError> {
        let permit = tokio::time::timeout(self.query_timeout, self.semaphore.acquire())
            .await
            .map_err(|_| ChError::Timeout("pool exhausted: no connection available".to_string()))?
            .expect("semaphore is never closed for the pool's lifetime");

        let idx = (self.next_slot.fetch_add(1, Ordering::Relaxed) as usize) % self.slots.len();
        let slot = &self.slots[idx];

        {
            let mut last_checked = slot.last_checked.lock().await;
            if last_checked.elapsed() > STALE_AFTER {
                ping_client(&slot.client).await?;
                *last_checked = Instant::now();
            }
        }

        Ok(PooledConn {
            client: slot.client.clone(),
            _permit: permit,
        })
    }

    /// Health probe (`SELECT 1`) against one connection.
    pub async fn ping(&self) -> Result<(), ChError> {
        let conn = self.get().await?;
        ping_client(&conn.client).await
    }
}

/// Builds the one base `clickhouse::Client` every pool slot clones from.
/// Routes through the skip-verify TLS builder ([`tls::skip_verify_ch_client`])
/// only for `https` + `tls_skip_verify` (docs/configuration.md §2); every
/// other combination (plain `http`, or verified `https` against public CAs)
/// uses the crate's own default `rustls-tls-webpki-roots` connector.
fn build_base_client(cfg: &ChConnConfig) -> Result<clickhouse::Client, ChError> {
    let mut client = if cfg.proto == ChProto::Https && cfg.tls_skip_verify {
        tls::skip_verify_ch_client()?
    } else {
        clickhouse::Client::default()
    };
    client = client
        .with_url(cfg.base_url())
        .with_database(&cfg.database)
        .with_user(&cfg.user);
    if !cfg.password.is_empty() {
        client = client.with_password(&cfg.password);
    }
    Ok(client)
}

async fn ping_client(client: &clickhouse::Client) -> Result<(), ChError> {
    client
        .query("SELECT 1")
        .execute()
        .await
        .map_err(ChError::from)
}

/// An RAII lease on one of the pool's `pool_size` connections. The
/// underlying permit is released back to the pool when this value is
/// dropped (including on early return / error / cancellation), so a
/// caller can never leak a lease by forgetting to call a `release()`
/// method.
pub struct PooledConn<'a> {
    pub(crate) client: clickhouse::Client,
    _permit: SemaphorePermit<'a>,
}

impl PooledConn<'_> {
    pub(crate) fn client(&self) -> &clickhouse::Client {
        &self.client
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_after_is_positive() {
        assert!(STALE_AFTER > Duration::ZERO);
    }
}
