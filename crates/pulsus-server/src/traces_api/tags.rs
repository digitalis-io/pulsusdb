//! The two §4.3 tag-discovery handlers (issue #58): parse (`params.rs`)
//! → catalog read via `TraceEngine::list_tag_names`/`list_tag_values`
//! (`pulsus-read` — `trace_tag_catalog` ONLY, never spans/attr-index/
//! payloads) → shape the documented JSON (`tags_response.rs`). Thin by
//! design, mirroring `search.rs`.
//!
//! Contract corners (docs/api.md §4.3, adjudicated on issue #58):
//! `start`/`end` are accepted and ignored on both routes (the catalog is
//! time-less); `q=` on the values route is accepted and ignored —
//! results may be a superset of what a narrowing query would return
//! (Tempo's own best-effort semantics; a 400 would break Grafana
//! autocomplete). The values route therefore never parses its query
//! string at all.

use axum::Json;
use axum::extract::{Path, RawQuery, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::app::AppState;

use super::error::ApiError;
use super::handlers::engine_for;
use super::params::{TagLookup, TagScope};
use super::tags_response::{TagNamesAnswer, TagValuesAnswer};
use super::{intrinsics, params, tags_response};

/// `GET /api/traces/v1/tags` — scoped tag-name discovery.
pub(crate) async fn tags(State(state): State<AppState>, RawQuery(raw): RawQuery) -> Response {
    match tags_impl(state, raw.as_deref().unwrap_or("")).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

async fn tags_impl(state: AppState, raw: &str) -> Result<Response, ApiError> {
    // Parse before the pool: `scope=bogus` resolves 400 without
    // ClickHouse — the search surface's parse-before-engine ordering.
    let params = params::parse_tags_params(raw)?;
    // The two static scopes short-circuit before `engine_for`: they issue
    // NO ClickHouse query at all (issue #475), which is what the
    // zero-delta gate in `traces_tags_live.rs` measures.
    let names = match params.scope {
        TagScope::Intrinsic => {
            return Ok(ok_json(tags_response::render_tag_names(
                &TagNamesAnswer::IntrinsicOnly,
            )));
        }
        TagScope::NoTags => {
            return Ok(ok_json(tags_response::render_tag_names(
                &TagNamesAnswer::NoTags,
            )));
        }
        scope => {
            let engine = engine_for(&state).await?;
            engine.list_tag_names(attribute_scope(scope)).await?
        }
    };
    Ok(ok_json(tags_response::render_tag_names(
        &TagNamesAnswer::Catalog {
            names: &names,
            with_intrinsic: matches!(params.scope, TagScope::All),
        },
    )))
}

/// The catalog-read scope for the two scopes that reach the engine.
/// `All` reads every attribute scope (the builder's own `IN` list);
/// `Attribute` confines to one.
pub(super) fn attribute_scope(scope: TagScope) -> Option<&'static str> {
    match scope {
        TagScope::Attribute(s) => Some(s),
        TagScope::All | TagScope::Intrinsic | TagScope::NoTags => None,
    }
}

pub(super) fn ok_json(body: serde_json::Value) -> Response {
    (StatusCode::OK, Json(body)).into_response()
}

/// `GET /api/traces/v1/tag/{tag}/values` — typed value discovery for one
/// key. The query string (`q`/`start`/`end`) is ignored entirely
/// (module doc); the scope comes from the `{tag}` prefix.
pub(crate) async fn tag_values(State(state): State<AppState>, Path(tag): Path<String>) -> Response {
    match tag_values_impl(state, &tag).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

async fn tag_values_impl(state: AppState, raw_tag: &str) -> Result<Response, ApiError> {
    // An intrinsic spelling is answered from the static vocabulary with
    // no ClickHouse query (issue #475) — bypassing the catalog rather
    // than unioning with it is what stops a bare `name` or `status`
    // lookup answering out of a reserved intrinsic scope or a
    // same-named user attribute.
    match params::parse_tag_lookup(raw_tag)? {
        TagLookup::Intrinsic(intrinsic) => Ok(ok_json(tags_response::render_tag_values(
            &TagValuesAnswer::Static(intrinsics::intrinsic_tag_values(intrinsic)),
        ))),
        TagLookup::Attribute { scope, key } => {
            let engine = engine_for(&state).await?;
            let values = engine.list_tag_values(&key, scope.as_deref()).await?;
            Ok(ok_json(tags_response::render_tag_values(
                &TagValuesAnswer::Catalog(&values),
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::error::testutil::error_body;
    use super::*;

    use pulsus_config::Config;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    use crate::app::BuildInfo;
    use crate::ingest::{MetricWriterSink, TraceWriterSink, WriterSink};

    fn test_state() -> AppState {
        AppState {
            pool: Arc::new(RwLock::new(None)),
            config: Arc::new(Config::default()),
            metrics: metrics_exporter_prometheus::PrometheusBuilder::new()
                .build_recorder()
                .handle(),
            build: BuildInfo::from_build_env(),
            writer: Arc::new(WriterSink::new(Arc::new(std::sync::OnceLock::new()))),
            metric_writer: Arc::new(MetricWriterSink::new(Arc::new(std::sync::OnceLock::new()))),
            trace_writer: Arc::new(TraceWriterSink::new(Arc::new(std::sync::OnceLock::new()))),
            label_cache: Arc::new(std::sync::OnceLock::new()),
            eval_gate: Arc::new(pulsus_read::EvalGate::new(
                pulsus_config::Config::default()
                    .reader
                    .query_eval_concurrency,
            )),
            started_at: std::time::SystemTime::now(),
            tail: std::sync::Arc::new(crate::app::TailRuntime::for_tests()),
        }
    }

    // Param failures resolve BEFORE the pool is consulted (no-pool test
    // state); a well-formed request stops at 503, proving parse precedes
    // execution.

    #[tokio::test]
    async fn a_bogus_scope_is_400_before_the_pool() {
        let res = tags(
            State(test_state()),
            RawQuery(Some("scope=bogus".to_string())),
        )
        .await;
        // `error_body` asserts Tempo's container on the way through, so
        // this module is not a hole in the #384 check.
        let (status, body) = error_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("bogus"), "body {body}");
    }

    #[tokio::test]
    async fn a_well_formed_tags_request_without_a_pool_is_503() {
        let res = tags(State(test_state()), RawQuery(None)).await;
        let (status, body) = error_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, "clickhouse pool not yet established");
    }

    #[tokio::test]
    async fn an_empty_tag_key_is_400_before_the_pool() {
        let res = tag_values(State(test_state()), Path("resource.".to_string())).await;
        let (status, _) = error_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn a_well_formed_values_request_without_a_pool_is_503() {
        let res = tag_values(State(test_state()), Path("service.name".to_string())).await;
        let (status, body) = error_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, "clickhouse pool not yet established");
    }
}
