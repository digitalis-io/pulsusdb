//! The one place a logs handler gets a [`LogQlEngine`] (issue #803).
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
use pulsus_model::Tenant;
use pulsus_read::{LogQlEngine, ReadError};

use crate::app::AppState;
use crate::chconfig;

use super::error::ApiError;

/// The serving pool, `503` before it is established (mirrors `ops::ready`:
/// the `Option` is cloned out from behind the lock and the guard dropped
/// before anything else).
async fn serving_pool(state: &AppState) -> Result<Arc<ChPool>, ApiError> {
    let pool = {
        let guard = state.pool.read().await;
        guard.clone()
    };
    pool.ok_or(ApiError::PoolUnavailable)
}

/// A [`LogQlEngine`] over the serving pool, its client bound to one server.
async fn engine_for(state: &AppState, tenant: Tenant) -> Result<LogQlEngine, ApiError> {
    let pool = serving_pool(state).await?;
    // Issue #114: the consistency-config invariant is already enforced at
    // config load, so this is unreachable in the real binary; a failure maps
    // to the existing 503 "not serving" semantics.
    logql_engine(pool, &state.config, tenant)
        .map(crate::request_id::tag)
        .map_err(|_| ApiError::PoolUnavailable)
}

/// Builds a [`LogQlEngine`] over `pool` — the same `Arc<ChPool>` `AppState`
/// already holds (`ChClient::from_shared_pool`, issue #13 resolved open
/// question #1), so a `/api/logs/v1` request never opens a second
/// connection pool. The engine reads `tenant`'s rows alone (issue #653).
fn logql_engine(
    pool: Arc<ChPool>,
    config: &Config,
    tenant: Tenant,
) -> Result<LogQlEngine, ChError> {
    let client = ChClient::from_shared_pool(pool, config.query_timeout.0)
        .with_consistency(chconfig::consistency_from(config))?;
    Ok(LogQlEngine::new(
        client,
        chconfig::engine_config_from(config),
        tenant,
    ))
}

/// The live tail's engine: one per connection, rebound by the tail itself
/// between polls when its server fails (`tail.rs`).
pub(super) async fn tail_engine(state: &AppState, tenant: Tenant) -> Result<LogQlEngine, ApiError> {
    engine_for(state, tenant).await
}

/// Runs one logs read request: builds the engine, then `run(engine)`, which
/// makes the engine calls and returns their materialised result. When the
/// engine's server fails during the run and another server is healthy, the
/// run and its engine are dropped and both happen once more. The accepted
/// engine is committed and `run`'s result returned.
pub(super) async fn with_engine<T, F, Fut>(
    state: &AppState,
    tenant: &Tenant,
    run: F,
) -> Result<T, ApiError>
where
    F: FnMut(Arc<LogQlEngine>) -> Fut,
    Fut: Future<Output = Result<T, ApiError>>,
{
    let pool = serving_pool(state).await?;
    // `run` is called once per attempt from a closure that returns a
    // future; the lock only spans the call that makes `run`'s future.
    let run = Mutex::new(run);
    let run = &run;
    run_read_request(
        &pool,
        move || async move {
            let engine = match engine_for(state, tenant.clone()).await {
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
