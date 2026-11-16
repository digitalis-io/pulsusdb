//! The one place a profiles handler gets a [`ProfilesEngine`] (issue #821),
//! on the shape of `traces_api/engine.rs`: a read request is one engine,
//! its client bound to one server, run once more on another healthy server
//! when that one fails during the request.
//!
//! `tests/read_request_sites.rs` holds the handlers to this: none of them
//! reaches the pool or builds an engine itself.

use std::future::Future;
use std::sync::{Arc, Mutex};

use pulsus_clickhouse::{Attempt, ChClient, ChError, ChPool, run_read_request};
use pulsus_model::Tenant;
use pulsus_read::profiles::{ProfilesEngine, ProfilesReadConfig, engine::map_read_error};

use crate::app::AppState;
use crate::chconfig;

use super::connect::{Code, ConnectError};

/// The serving pool, `unavailable` before it is established.
async fn serving_pool(state: &AppState) -> Result<Arc<ChPool>, ConnectError> {
    let pool = {
        let guard = state.pool.read().await;
        guard.clone()
    };
    pool.ok_or_else(not_serving)
}

fn not_serving() -> ConnectError {
    ConnectError::new(Code::Unavailable, "the storage connection is not ready")
}

/// A [`ProfilesEngine`] over the serving pool, its client bound to one
/// server, its statements tagged with the request id.
async fn engine_for(
    state: &AppState,
    tenant: Tenant,
    avoid: Option<usize>,
) -> Result<ProfilesEngine, ConnectError> {
    let pool = serving_pool(state).await?;
    let client = ChClient::from_shared_pool_avoiding(pool, state.config.query_timeout.0, avoid)
        .with_consistency(chconfig::consistency_from(&state.config))
        .map_err(|_| not_serving())?;
    let engine = ProfilesEngine::new(
        client,
        ProfilesReadConfig::from_config(&state.config),
        tenant,
    );
    Ok(crate::request_id::tag(engine))
}

/// Runs one profiles read request: builds the engine, then `run(engine)`.
/// When the engine's server fails during the run and another server is
/// healthy, the run and its engine are dropped and both happen once more.
pub(super) async fn with_engine<T, F, Fut>(
    state: &AppState,
    tenant: &Tenant,
    run: F,
) -> Result<T, ConnectError>
where
    F: FnMut(Arc<ProfilesEngine>) -> Fut,
    Fut: Future<Output = Result<T, ConnectError>>,
{
    let pool = serving_pool(state).await?;
    let run = Mutex::new(run);
    let run = &run;
    run_read_request(
        &pool,
        move |avoid| async move {
            let engine = match engine_for(state, tenant.clone(), avoid).await {
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
                result,
            }
        },
        |f| ConnectError::from(map_read_error(ChError::from(f))),
    )
    .await
}
