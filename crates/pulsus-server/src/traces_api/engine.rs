//! The one place a traces handler gets a [`TraceEngine`] (issue #803).
//!
//! A read request is one engine: its client is bound to one ClickHouse
//! server when the engine is built, and every statement of the request goes
//! there. [`with_engine`] builds the engine, runs the handler's engine calls
//! on it, and — when the server fails during the request — drops that run
//! and everything it built and runs it once more from the start on another
//! healthy server. The handler encodes the response after `with_engine`
//! returns, from the accepted run's result only. The accepted engine's
//! `commit` is the one place the service graph's target reaches the
//! process-wide cache.
//!
//! `tests/read_request_sites.rs` holds the handlers to this: none of them
//! reaches the pool or builds an engine itself.

use std::future::Future;
use std::sync::{Arc, Mutex};

use pulsus_clickhouse::{Attempt, ChClient, ChError, ChPool, run_read_request};
use pulsus_config::Config;
use pulsus_model::Tenant;
use pulsus_read::TraceEngine;
use pulsus_read::logql::ReadError;

use crate::app::AppState;
use crate::chconfig;

use super::error::ApiError;

/// The serving pool, `503` before it is established.
async fn serving_pool(state: &AppState) -> Result<Arc<ChPool>, ApiError> {
    let pool = {
        let guard = state.pool.read().await;
        guard.clone()
    };
    pool.ok_or(ApiError::PoolUnavailable)
}

/// A [`TraceEngine`] over the serving pool, its client bound to one server,
/// its statements stamped `<prefix>-<n>` when the request minted a prefix.
async fn engine_for(
    state: &AppState,
    tenant: Tenant,
    statement_prefix: Option<&str>,
    avoid: Option<usize>,
) -> Result<TraceEngine, ApiError> {
    let pool = serving_pool(state).await?;
    // Issue #114: the consistency-config invariant is already enforced at
    // config load, so this is unreachable in the shipped binary; a failure maps
    // to the existing 503 "not serving" semantics.
    let engine = trace_engine(
        pool,
        &state.config,
        state.graph_target.clone(),
        tenant,
        avoid,
    )
    .map(crate::request_id::tag)
    .map_err(|_| ApiError::PoolUnavailable)?;
    Ok(match statement_prefix {
        Some(prefix) => engine.with_statement_prefix(prefix),
        None => engine,
    })
}

/// Builds a [`TraceEngine`] over `pool`, sharing the pool `AppState`
/// already holds, with the process's one service-graph target cache
/// (issue #599).
fn trace_engine(
    pool: Arc<ChPool>,
    config: &Config,
    graph_target: pulsus_read::GraphTargetCache,
    tenant: Tenant,
    avoid: Option<usize>,
) -> Result<TraceEngine, ChError> {
    let client = ChClient::from_shared_pool_avoiding(pool, config.query_timeout.0, avoid)
        .with_consistency(chconfig::consistency_from(config))?;
    Ok(
        TraceEngine::new(client, chconfig::trace_read_config_from(config), tenant)
            .with_indexed_attrs(
                pulsus_read::traces::spans::attr_index::IndexedAttr::from_config(
                    &config.traceql_indexed_attributes,
                ),
            )
            .with_graph_target_cache(graph_target),
    )
}

/// Runs one traces read request: builds the engine, then `run(engine)`,
/// which makes the engine calls and returns their materialised result.
/// When the engine's server fails during the run and another server is
/// healthy, the run and its engine are dropped and both happen once more.
/// The accepted engine is committed and `run`'s result returned.
pub(super) async fn with_engine<T, F, Fut>(
    state: &AppState,
    tenant: &Tenant,
    statement_prefix: Option<&str>,
    run: F,
) -> Result<T, ApiError>
where
    F: FnMut(Arc<TraceEngine>) -> Fut,
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
            let engine = match engine_for(state, tenant.clone(), statement_prefix, avoid).await {
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
