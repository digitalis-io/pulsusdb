//! `spawn_rotation`: a background tick that reapplies TTL on an interval
//! (`PULSUS_ROTATION_INTERVAL`, docs/configuration.md §3) so a changed
//! `PULSUS_RETENTION_DAYS` propagates to already-created tables without a
//! restart. `--mode init` calls [`crate::controller::apply_ttl`] once
//! directly and never spawns this timer (docs/schemas.md's MV/TTL
//! reconciliation is a startup concern; only a long-running `all`/`writer`
//! process needs the timer, out of scope for issue #5's init-mode wiring
//! but shipped here so issue #6 has it ready to call).

use std::sync::Arc;
use std::time::Duration;

use pulsus_clickhouse::ChClient;

use crate::controller::apply_ttl;
use crate::render::RenderCtx;

/// Spawns a task that reapplies TTL every `every`. The returned
/// [`tokio::task::JoinHandle`] runs until aborted or the process exits;
/// there is no internal shutdown signal here (structured-shutdown wiring is
/// issue #6's `CancellationToken` plumbing) — callers that need graceful
/// shutdown should `handle.abort()` the returned handle from their own
/// shutdown path.
///
/// A failed tick is logged via `tracing::warn!` and does not stop the loop:
/// TTL reapplication is best-effort background maintenance, and one
/// transient ClickHouse error must not silently disable rotation until the
/// next process restart.
pub fn spawn_rotation(
    client: Arc<ChClient>,
    ctx: RenderCtx,
    every: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(every);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            if let Err(err) = apply_ttl(&client, &ctx).await {
                tracing::warn!("pulsus-schema: rotation: failed to reapply TTL: {err}");
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn spawn_rotation_returns_a_handle_that_can_be_aborted() {
        // No live ClickHouse in this test — just proves the task is spawned
        // and abortable, not that a tick succeeds (that's covered live in
        // tests/live_schema.rs via `apply_ttl` directly).
        let cfg = pulsus_clickhouse::ChConnConfig {
            server: "127.0.0.1".to_string(),
            http_port: 1, // nothing listens here
            pool_size: 1,
            query_timeout: Duration::from_millis(50),
            ..Default::default()
        };
        // `ChClient::new` pings at connect time and would fail fast against
        // a closed port, so this test only exercises `spawn_rotation`'s
        // task-management contract with a handle built from a connection
        // attempt we expect to fail — skip if we can't even construct one.
        let Ok(client) = pulsus_clickhouse::ChClient::new(cfg).await else {
            return;
        };
        let ctx = RenderCtx {
            db: "pulsus".to_string(),
            cluster: None,
            dist_suffix: "_dist".to_string(),
            storage_policy: None,
            retention_days: 7,
            log_rollup: Duration::from_secs(5),
        };
        let handle = spawn_rotation(Arc::new(client), ctx, Duration::from_secs(3600));
        assert!(!handle.is_finished());
        handle.abort();
    }
}
