//! `GET /api/traces/v1/search` (issue #57; docs/api.md §4.2): parse
//! params (`params.rs`) → obtain the TraceQL AST (`q` via
//! `pulsus_traceql::parse`, or the legacy params via `legacy.rs` — one
//! validation path either way) → plan (`pulsus_read::plan_search_request`,
//! the search statement's compiler, issue #602 part 2b) → execute
//! (`TraceEngine::search_routed`) → shape the documented JSON
//! (`search_response.rs`). Thin by design: SQL/execution stays in
//! `pulsus-read`.

use axum::Json;
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::app::AppState;

use super::error::ApiError;
use super::handlers::engine_for;
use super::{legacy, params, search_response};

/// `X-Pulsus-Explain: 1` (docs/api.md "Request headers"), issue #492.
///
/// The search route had no explain surface at all: the previous engine's
/// explained search built a `PlanExplain` and was reached only by a test, so the
/// language whose plan changes most was the one a user could not see.
/// The contract is the same as the logs and metrics routes': without the
/// header nothing changes, and with it the response gains one sibling
/// key.
fn wants_explain(headers: &HeaderMap) -> bool {
    headers
        .get("x-pulsus-explain")
        .and_then(|v| v.to_str().ok())
        == Some("1")
}

/// `GET /api/traces/v1/search`.
pub(crate) async fn search(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    match search_impl(state, &headers, raw.as_deref().unwrap_or("")).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

async fn search_impl(
    state: AppState,
    headers: &HeaderMap,
    raw: &str,
) -> Result<Response, ApiError> {
    let params = params::parse_search_params(raw)?;
    // `q` XOR legacy (both present is already a 400 in the parser);
    // neither present compiles to the `{}` time-only search.
    let q_string = match &params.q {
        Some(q) => q.as_str().to_string(),
        None => legacy::compile_legacy(
            params.tags.as_deref(),
            params.min_duration.as_deref(),
            params.max_duration.as_deref(),
        )?,
    };
    let query = pulsus_traceql::parse(&q_string).map_err(ApiError::Query)?;
    // Issue #328: the reference's route-independent semantic validation
    // (`traceql.Validate`), after parse and before planning — exactly
    // where its query-frontend middleware runs it. Only for the
    // user-supplied `q`: the legacy-compiled text is never seen by the
    // reference's middleware (the shadow `query` parameter runs the same
    // pass inside `params::parse_search_params`).
    if params.q.is_some() {
        super::querytext::validate_semantics(&query)?;
    }

    // Plan BEFORE acquiring the pool: planning needs only config-derived
    // table names/budgets, so every 400-class failure (parse, params,
    // plan) resolves without ClickHouse — same discipline as the logs
    // surface's parse-before-engine ordering.
    let read_config = crate::chconfig::trace_read_config_from(&state.config);
    let indexed = pulsus_read::traces::spans::attr_index::IndexedAttr::from_config(
        &state.config.traceql_indexed_attributes,
    );
    let tables = pulsus_read::StatementTables {
        spans: &read_config.spans_table,
        error_spans: &read_config.error_spans_table,
        resources: &read_config.resources_table,
        traces: &read_config.traces_table,
        indexed: &indexed,
    };
    // docs/api.md §4, "Retention": the window starts no earlier than the
    // retention cutoff, for both engines.
    let start_ns = params::floored_start(
        params.start_ns,
        params.end_ns,
        params::retention_floor_ns(
            state.retention_days().ok_or(ApiError::PoolUnavailable)?,
            super::tags::now_unix_nanos(),
        ),
    );
    let search_params = pulsus_read::SearchParams {
        start_ns,
        end_ns: params.end_ns,
        limit: params.limit,
        spss: params.spss,
    };
    let request =
        pulsus_read::plan_search_request(&query, &search_params, &tables, read_config.max_depth)
            .map_err(ApiError::Plan)?;

    let engine = engine_for(&state).await?;
    if !wants_explain(headers) {
        let output = engine.search_routed(&request).await?;
        return Ok((StatusCode::OK, Json(search_response::render(&output))).into_response());
    }
    // One execution that also captures the per-stage SQL — the same
    // single-pass contract the logs route has, never a second run.
    let (output, explain) = engine.search_routed_explained(&request).await?;
    let mut body = search_response::render(&output);
    if let Some(obj) = body.as_object_mut() {
        obj.insert(
            "explain".to_string(),
            search_response::explain_value(&explain),
        );
    }
    Ok((StatusCode::OK, Json(body)).into_response())
}

#[cfg(test)]
mod tests {
    use super::super::error::testutil::error_body;
    use super::*;
    use crate::app::BuildInfo;
    use crate::ingest::{MetricWriterSink, TraceWriterSink, WriterSink};
    use pulsus_config::Config;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    fn test_state() -> AppState {
        AppState {
            pool: Arc::new(RwLock::new(None)),
            retention_days: std::sync::Arc::new(std::sync::OnceLock::from(7)),
            config: Arc::new(Config::default()),
            metrics: metrics_exporter_prometheus::PrometheusBuilder::new()
                .build_recorder()
                .handle(),
            build: BuildInfo::from_build_env(),
            writer: Arc::new(WriterSink::new(Arc::new(std::sync::OnceLock::new()))),
            metric_writer: Arc::new(MetricWriterSink::new(Arc::new(std::sync::OnceLock::new()))),
            trace_writer: Arc::new(TraceWriterSink::new(Arc::new(std::sync::OnceLock::new()))),
            label_cache: Arc::new(std::sync::OnceLock::new()),
            graph_target: pulsus_read::GraphTargetCache::default(),
            eval_gate: Arc::new(pulsus_read::EvalGate::new(
                pulsus_config::Config::default()
                    .reader
                    .query_eval_concurrency,
            )),
            started_at: std::time::SystemTime::now(),
            tail: std::sync::Arc::new(crate::app::TailRuntime::for_tests()),
        }
    }

    /// Every case below is an ERROR response, so it goes through
    /// `error::testutil::error_body` — which asserts Tempo's container
    /// (`text/plain; charset=utf-8`, exactly once, NO `nosniff`) on the
    /// way through. That is how this module is not a hole in the
    /// container check (issue #384).
    async fn run(query: &str) -> (StatusCode, String) {
        let res = search(
            State(test_state()),
            axum::http::HeaderMap::new(),
            RawQuery(Some(query.to_string())),
        )
        .await;
        error_body(res).await
    }

    // Param/parse failures resolve BEFORE the pool is consulted, so the
    // no-pool test state exercises them end to end; a well-formed request
    // stops at 503 (no pool), proving parse precedes execution.

    #[tokio::test]
    async fn malformed_q_is_400_naming_its_byte_offset() {
        let (status, body) = run("q=%7B&start=1&end=2").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("byte 1"), "body {body}");
    }

    #[tokio::test]
    async fn bad_start_is_400() {
        let (status, body) = run("q=%7B%7D&start=abc&end=2").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(!body.contains("byte "), "body {body}");
    }

    #[tokio::test]
    async fn q_plus_legacy_is_400() {
        let (status, _) = run("q=%7B%7D&tags=a%3Db&start=1&end=2").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn malformed_tags_logfmt_is_400() {
        let (status, _) = run("tags=barekey&start=1&end=2").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn a_planner_type_mismatch_is_400() {
        // `{ !-.a }` parses, and the search statement's compiler refuses
        // it: a 400 before the pool is consulted (issue #602 part 2b).
        let (status, body) = run("q=%7B%20%21-.a%20%7D&start=1&end=2").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "body {body}");
        assert_eq!(
            body,
            "type mismatch: an arithmetic expression is not a predicate"
        );
    }

    /// Issue #602 part 2b: the route plans with the search statement's
    /// compiler, which orders strings; the previous planner refused
    /// `name > "x"` with a 400. A planned request without a pool is 503.
    #[tokio::test]
    async fn a_string_ordering_is_planned_by_the_statement() {
        let (status, body) = run("q=%7B%20name%20%3E%20%22x%22%20%7D&start=1&end=2").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "body {body}");
    }

    #[tokio::test]
    async fn a_metrics_stage_on_search_is_400_without_a_byte_offset() {
        // Issue #59 error-shape shift: `q={}|rate()` used to be a
        // positioned NotYetSupported parse error; it now PARSES and
        // fails in the planner — still 400, but with no byte offset (a
        // plan error, not a parse error).
        let (status, body) = run("q=%7B%7D%20%7C%20rate()&start=1&end=2").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(!body.contains("byte "), "body {body}");
        assert!(
            body.contains("metrics"),
            "message must point at the metrics surface, got {body}"
        );
    }

    // --- issue #326: the query-frontend validator's parse step ----------

    /// The reference validates the `query` parameter on the search
    /// pipeline even though its request parser never reads it as TraceQL
    /// (`async_query_validator_middleware.go:49-55` vs
    /// `pkg/api/http.go:180`). PulsusDB served this `200` before; it is a
    /// `400` now, with the same body a malformed `q` produces.
    ///
    /// Reachable over the wire, unlike the issue #284 length cap: a
    /// malformed expression is short, so `http::Uri`'s 65,534-byte
    /// request-target limit (#296) never intercepts it.
    #[tokio::test]
    async fn a_malformed_query_parameter_is_400_naming_its_byte_offset() {
        let (status, body) = run("query=%7B&start=1&end=2").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("byte 1"), "body {body}");
        assert!(
            body.starts_with("invalid TraceQL query: "),
            "the reference's wrapping, got {body}"
        );
    }

    /// The rejection is about admission, not about routing: a `query`
    /// that parses is accepted and the search still runs the time-only
    /// `{}` (503 here only because no pool is configured). Were `query`
    /// wrongly aliased to `q`, this would still be 503 — so the
    /// not-an-alias half stays pinned by
    /// `params::tests::a_malformed_search_query_parameter_is_rejected_though_it_never_runs`,
    /// which asserts `q` is `None`.
    #[tokio::test]
    async fn a_parseable_query_parameter_is_admitted_and_the_search_still_runs() {
        let (status, body) =
            run("query=%7B%20resource.service.name%20%3D%20%22checkout%22%20%7D&start=1&end=2")
                .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "body {body}");
    }

    /// Issue #328 (AC 4, replacing the #326 divergence pin
    /// `a_query_parameter_failing_only_the_reference_semantic_validation_is_accepted`):
    /// the `traceql.Validate` half is now reproduced route-independently,
    /// so ONE expression that parses but fails semantic validation —
    /// `{ span.a =~ "[" }`, an unterminated character class, the
    /// reference's own middleware-test shape — is a `400` with
    /// the reference's `invalid TraceQL query: ` wrapping on BOTH
    /// parameters. The 503 backend-reachability coverage this replaces
    /// survives in `a_parseable_query_parameter_is_admitted_and_the_search_still_runs`
    /// and `a_well_formed_request_without_a_pool_is_503`.
    #[tokio::test]
    async fn a_semantically_invalid_expression_is_rejected_on_both_parameters() {
        // `{ span.a =~ "[" }` — an unterminated character class.
        const EXPR: &str = "%7B%20span.a%20%3D~%20%22%5B%22%20%7D";

        for param in ["q", "query"] {
            let (status, body) = run(&format!("{param}={EXPR}&start=1&end=2")).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{param}: body {body}");
            assert!(
                body.starts_with("invalid TraceQL query: "),
                "{param}: the reference's wrapping, got {body}"
            );
            // The D6 superset: the deleted #326 pin's q-side assertion.
            assert!(
                body.contains("invalid regex"),
                "{param}: the semantic validator's regex rejection, got {body}"
            );
            // A semantic rejection names no byte offset (the reference's
            // Validate errors name none either).
            assert!(!body.contains("byte "), "{param}: body {body}");
        }
    }

    #[tokio::test]
    async fn a_well_formed_request_without_a_pool_is_503() {
        let (status, body) = run("q=%7B%7D&start=1&end=2").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, "clickhouse pool not yet established");
    }

    #[tokio::test]
    async fn a_missing_query_string_entirely_is_400_for_the_missing_range() {
        let res = search(
            State(test_state()),
            axum::http::HeaderMap::new(),
            RawQuery(None),
        )
        .await;
        let (status, _) = error_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    /// `q` as the request sends it: every byte outside the unreserved set
    /// percent-encoded.
    fn pct(s: &str) -> String {
        s.bytes()
            .map(|b| match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                    (b as char).to_string()
                }
                _ => format!("%{b:02X}"),
            })
            .collect()
    }

    /// One corpus case: the input without the single trailing newline the
    /// file format mandates, and its golden.
    fn corpus_case(dir: &str, name: &str) -> (String, String) {
        let base = format!(
            "{}/../pulsus-traceql/tests/corpus/{dir}/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        let raw = std::fs::read_to_string(format!("{base}.traceql")).expect("the input");
        let golden = std::fs::read_to_string(format!("{base}.golden")).expect("the golden");
        (raw.strip_suffix('\n').unwrap_or(&raw).to_string(), golden)
    }

    fn names(dir: &str) -> Vec<String> {
        let path = format!(
            "{}/../pulsus-traceql/tests/corpus/{dir}",
            env!("CARGO_MANIFEST_DIR")
        );
        let mut out: Vec<String> = std::fs::read_dir(path)
            .expect("the corpus directory")
            .filter_map(|e| {
                let p = e.expect("an entry").path();
                (p.extension()? == "traceql")
                    .then(|| p.file_stem()?.to_str().map(str::to_string))?
            })
            .collect();
        out.sort();
        out
    }

    /// Issue #600, `T-C9` (guard): every query of the corpus the route
    /// refuses is a `400` in the reference's container, through the route
    /// itself: the status and the container, as parity with the reference
    /// asks, and a message; the wording is the corpus goldens' to pin, and
    /// each parse case's golden is checked here against the parser so the
    /// case is the one the golden names. `reject/empty` is not a refusal here:
    /// an empty `q` is no query on this route and on the reference's
    /// (`pkg/api/http.go:768-771`), so it plans as `{}` and, without a pool,
    /// answers `503`.
    #[tokio::test]
    async fn t_c9_every_corpus_refusal_is_a_400_in_the_reference_container() {
        let mut refused = 0;
        for dir in ["reject", "unsupported"] {
            for name in names(dir) {
                let (src, golden) = corpus_case(dir, &name);
                let err = pulsus_traceql::parse(&src).expect_err("a refused case");
                assert_eq!(format!("{err:#?}\n"), golden, "{dir}/{name}: the golden");
                let (status, body) = run(&format!("q={}&start=1&end=2", pct(&src))).await;
                if name == "empty" {
                    assert_eq!(
                        status,
                        StatusCode::SERVICE_UNAVAILABLE,
                        "{dir}/{name}: {body}"
                    );
                    continue;
                }
                assert_eq!(status, StatusCode::BAD_REQUEST, "{dir}/{name}: {body}");
                assert!(!body.is_empty(), "{dir}/{name}: a message");
                refused += 1;
            }
        }
        for name in names("validate_reject") {
            let (src, _) = corpus_case("validate_reject", &name);
            let query = pulsus_traceql::parse(&src).expect("a validator case parses");
            assert!(
                pulsus_traceql::validate(&query).is_err(),
                "validate_reject/{name}: refused"
            );
            let (status, body) = run(&format!("q={}&start=1&end=2", pct(&src))).await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "validate_reject/{name}: {body}"
            );
            assert!(!body.is_empty(), "validate_reject/{name}: a message");
            refused += 1;
        }
        for name in ["duration_unitless", "pipeline_spanset_operation"] {
            let (src, _) = corpus_case("accept", name);
            let (status, body) = run(&format!("q={}&start=1&end=2", pct(&src))).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "accept/{name}: {body}");
            assert!(!body.is_empty(), "accept/{name}: a message");
            refused += 1;
        }
        assert_eq!(
            refused, 48,
            "39 of reject/, 3 of unsupported/, 4 of validate_reject/, 2 planned"
        );
    }
}
