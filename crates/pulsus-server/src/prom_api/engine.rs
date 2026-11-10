//! The one place a metrics handler gets a [`MetricsEngine`] (issue #803).
//!
//! A read request is one engine: its client is bound to one ClickHouse
//! server when the engine is built, and every statement of the request goes
//! there. [`with_engine`] builds the engine, runs the handler's engine calls
//! on it, and — when the server fails during the request — drops that run
//! and everything it built and runs it once more from the start on another
//! healthy server. The handler encodes the response after `with_engine`
//! returns, from the accepted run's result only.
//!
//! `tests/read_request_sites.rs` holds the handlers to this: none of them
//! reaches the pool or builds an engine itself.

use std::future::Future;
use std::sync::{Arc, Mutex};

use pulsus_clickhouse::{Attempt, ChClient, ChError, ChPool, run_read_request};
use pulsus_config::Config;
use pulsus_read::{LabelCache, MetricsEngine, ReadError};

use crate::app::AppState;
use crate::chconfig;

use super::error::ApiError;

/// The serving pool, `503` before it is established.
async fn serving_pool(state: &AppState) -> Result<Arc<ChPool>, ApiError> {
    let pool = {
        let guard = state.pool.read().await;
        guard.clone()
    };
    pool.ok_or(ApiError::Unavailable)
}

/// A [`MetricsEngine`] over the serving pool and the constructed label
/// cache (clone each slot out, never hold a lock across an `.await`) —
/// `503 unavailable` before either exists, matching `/ready` — with the
/// request's `lookback_delta` when it set one.
async fn engine_for(
    state: &AppState,
    lookback_ms: Option<i64>,
    avoid: Option<usize>,
) -> Result<MetricsEngine, ApiError> {
    let pool = serving_pool(state).await?;
    let label_cache = state
        .label_cache
        .get()
        .cloned()
        .ok_or(ApiError::Unavailable)?;
    // Issue #114: the consistency-config invariant is already enforced at
    // config load, so this is unreachable in the shipped binary; a failure maps
    // to the existing 503 "not serving" semantics.
    let engine = metrics_engine(
        pool,
        label_cache,
        &state.config,
        state.eval_gate.clone(),
        Arc::clone(&state.rollup_levels),
        avoid,
    )
    .map(crate::request_id::tag)
    .map_err(|_| ApiError::Unavailable)?;
    Ok(match lookback_ms {
        Some(ms) => engine.with_lookback_ms(ms),
        None => engine,
    })
}

/// Builds a [`MetricsEngine`] over `pool` and the already-constructed
/// `label_cache` — the same `Arc<ChPool>`/`Arc<LabelCache>` `AppState`
/// already holds, so a request never opens a second connection pool
/// (issue #32).
fn metrics_engine(
    pool: Arc<ChPool>,
    label_cache: Arc<LabelCache>,
    config: &Config,
    eval_gate: Arc<pulsus_read::EvalGate>,
    rollup_levels: Arc<pulsus_read::metrics::rollup::LevelSlot>,
    avoid: Option<usize>,
) -> Result<MetricsEngine, ChError> {
    let client = ChClient::from_shared_pool_avoiding(pool, config.query_timeout.0, avoid)
        .with_consistency(chconfig::consistency_from(config))?;
    Ok(
        MetricsEngine::new(client, label_cache, chconfig::metrics_config_from(config))
            .with_eval_gate(eval_gate)
            .with_rollup_levels(rollup_levels),
    )
}

/// Runs one metrics read request: builds the engine (with `lookback_ms`
/// when the request set one), then `run(engine)`, which makes the engine
/// calls and returns their materialised result. When the engine's server
/// fails during the run and another server is healthy, the run and its
/// engine are dropped and both happen once more. The accepted engine is
/// committed and `run`'s result returned.
pub(super) async fn with_engine<T, F, Fut>(
    state: &AppState,
    lookback_ms: Option<i64>,
    run: F,
) -> Result<T, ApiError>
where
    F: FnMut(Arc<MetricsEngine>) -> Fut,
    Fut: Future<Output = Result<T, ApiError>>,
{
    let pool = serving_pool(state).await?;
    // `run` is called once per attempt from a closure that returns a
    // future; the lock only spans the call that makes `run`'s future.
    let run = Mutex::new(run);
    let run = &run;
    run_read_request(
        &pool,
        move |avoid| async move {
            let engine = match engine_for(state, lookback_ms, avoid).await {
                Ok(engine) => Arc::new(engine),
                Err(e) => return Attempt::failed_to_start(e),
            };
            let fut = {
                let mut run = run.lock().unwrap_or_else(|p| p.into_inner());
                (*run)(Arc::clone(&engine))
            };
            let result = fut.await;
            Attempt {
                failure: engine.server_failure(),
                result: result.map(|t| (t, engine)),
            }
        },
        |f| ApiError::Read(ReadError::Clickhouse(ChError::from(f))),
    )
    .await
    .map(|(t, engine)| {
        engine.commit();
        t
    })
}
