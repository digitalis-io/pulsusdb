//! The two §4.3 tag-discovery handlers (issue #58): parse (`params.rs`)
//! → a read via `TraceEngine::list_tag_names`/`list_tag_values`/
//! `list_span_name_values` (`pulsus-read`) → shape the documented JSON
//! (`tags_response.rs`). Thin by design, mirroring `search.rs`.
//!
//! **Which read answers what** (issue #478 changed this; the module used
//! to say "catalog ONLY" and that stopped being true):
//!
//! * tag NAMES — always `trace_tag_catalog`, still time-less.
//! * attribute values with no narrowing `q` — `trace_tag_catalog`, the
//!   same SQL bytes as before.
//! * attribute values with a narrowing `q` — `trace_attrs_idx`
//!   intersected with the matching span set.
//! * `name`/`span:name` — `trace_spans`, always. The catalog holds no
//!   span-`name` row (its MV projects `trace_attrs_idx` alone), so this
//!   is the only place the answer exists. Every OTHER intrinsic still
//!   answers from the static vocabulary with no query at all, and
//!   [`tag_value_source`] is the exhaustive dispatch that decides which.
//!
//! Contract corners (docs/api.md §4.3):
//!
//! * `start`/`end` are accepted and ignored on the NAMES routes (the
//!   catalog has no timestamp column) and BOUND the read on the values
//!   route, defaulting to `reader.traceql_tag_lookback`. A range FAULT —
//!   unparseable, half-supplied, inverted — is a `400` on every route.
//! * `q=` on the values route NARROWS, and a `q` that is well-formed
//!   input and does not parse as TraceQL is tolerated rather than
//!   rejected: the editor sends half-typed text on every keystroke, so a
//!   `400` there would break autocomplete for input the user cannot
//!   avoid sending. Lowering it is total
//!   (`pulsus_read::traces::tag_narrow`), so no `q` can become a status
//!   code at the interpretation layer. Two classes ARE rejected below
//!   that layer by the HTTP transport, both avoidable and both measured:
//!   raw invalid UTF-8 in the request target is `400` (the same bytes
//!   percent-encoded are served `200`), and a `q` past the 64 KiB
//!   request-target bound is refused by the transport with `414` or
//!   `431`. That module's doc carries the measured length boundary and
//!   why the status itself is not pinned.

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

/// Where one intrinsic's values come from (issue #478).
///
/// The dispatch below is an exhaustive `match` with NO wildcard, so a new
/// `Intrinsic` variant fails to compile until someone decides which of
/// these it is. That is the point of the enum: the previous shape — an
/// `if` on one variant — would have silently swept a new intrinsic into
/// the vocabulary answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TagValueSource {
    /// Read from `trace_spans`. Exactly one intrinsic is here.
    Store,
    /// The static closed keyword set, or the empty list for an
    /// open-valued intrinsic. No ClickHouse query at all.
    Vocabulary,
}

/// Which source answers an intrinsic's values.
///
/// `name` moved to the store because the answer exists nowhere else: the
/// catalog has no span-`name` row, and before issue #478 a bare `name`
/// lookup answered from the static vocabulary with an EMPTY list. Every
/// other open-valued intrinsic still answers empty, deliberately — a
/// store read for `duration` or `span:id` would enumerate values that are
/// not a discoverable set.
pub(crate) fn tag_value_source(intrinsic: pulsus_traceql::Intrinsic) -> TagValueSource {
    use pulsus_traceql::Intrinsic as I;
    match intrinsic {
        I::Name => TagValueSource::Store,
        I::Duration
        | I::Status
        | I::Kind
        | I::NestedSetParent
        | I::NestedSetLeft
        | I::NestedSetRight
        | I::StatusMessage
        | I::ChildCount
        | I::SpanId
        | I::ParentId
        | I::TraceId
        | I::TraceDuration
        | I::RootName
        | I::RootServiceName
        | I::InstrumentationName
        | I::InstrumentationVersion
        | I::EventName
        | I::EventTimeSinceStart
        | I::LinkSpanId
        | I::LinkTraceId => TagValueSource::Vocabulary,
    }
}

/// The window the values routes read over when the client supplies none.
pub(super) fn tag_lookback_ns(state: &AppState) -> i64 {
    i64::try_from(state.config.reader.traceql_tag_lookback.0.as_nanos()).unwrap_or(i64::MAX)
}

/// `now` in unix nanoseconds — the same shape `graph.rs` uses for its
/// injected clock.
pub(super) fn now_unix_nanos() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_nanos()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

/// The values a §4.3 request resolves to, before rendering — shared by
/// the native handler and the v2 alias so the two cannot answer the same
/// request from different sources.
pub(super) enum ValuesSource {
    /// A static vocabulary answer; no query was issued.
    Static(&'static [&'static str]),
    /// A store or catalog read.
    Values(pulsus_read::TagValues),
}

impl ValuesSource {
    pub(super) fn answer(&self) -> TagValuesAnswer<'_> {
        match self {
            Self::Static(values) => TagValuesAnswer::Static(values),
            Self::Values(values) => TagValuesAnswer::Catalog(values),
        }
    }
}

/// The shared fetch for the native and v2 values routes: parse the
/// window, then issue exactly ONE read (or none, for a vocabulary
/// answer).
pub(super) async fn values_for(
    state: &AppState,
    raw_tag: &str,
    raw_query: &str,
) -> Result<ValuesSource, ApiError> {
    let lookup = params::parse_tag_lookup(raw_tag)?;
    let params =
        params::parse_tag_values_params(raw_query, now_unix_nanos(), tag_lookback_ns(state))?;
    let req = pulsus_read::TagValuesRequest {
        q: params.q.as_deref(),
        start_ns: params.start_ns,
        end_ns: params.end_ns,
    };
    match lookup {
        TagLookup::Intrinsic(intrinsic) => match tag_value_source(intrinsic) {
            TagValueSource::Vocabulary => Ok(ValuesSource::Static(
                intrinsics::intrinsic_tag_values(intrinsic),
            )),
            TagValueSource::Store => {
                let engine = engine_for(state).await?;
                Ok(ValuesSource::Values(
                    engine.list_span_name_values(req).await?,
                ))
            }
        },
        TagLookup::Attribute { scope, key } => {
            let engine = engine_for(state).await?;
            Ok(ValuesSource::Values(
                engine.list_tag_values(&key, scope.as_deref(), req).await?,
            ))
        }
    }
}

/// `GET /api/traces/v1/tag/{tag}/values` — typed value discovery for one
/// key. The scope comes from the `{tag}` prefix; `q` narrows and
/// `start`/`end` bound (module doc).
pub(crate) async fn tag_values(
    State(state): State<AppState>,
    Path(tag): Path<String>,
    RawQuery(raw): RawQuery,
) -> Response {
    match tag_values_impl(state, &tag, raw.as_deref().unwrap_or("")).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

async fn tag_values_impl(
    state: AppState,
    raw_tag: &str,
    raw_query: &str,
) -> Result<Response, ApiError> {
    let source = values_for(&state, raw_tag, raw_query).await?;
    Ok(ok_json(tags_response::render_tag_values(&source.answer())))
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
        let res = tag_values(
            State(test_state()),
            Path("resource.".to_string()),
            RawQuery(None),
        )
        .await;
        let (status, _) = error_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn a_well_formed_values_request_without_a_pool_is_503() {
        let res = tag_values(
            State(test_state()),
            Path("service.name".to_string()),
            RawQuery(None),
        )
        .await;
        let (status, body) = error_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, "clickhouse pool not yet established");
    }

    // -- issue #478 ----------------------------------------------------

    /// Criterion 6. **Exactly one intrinsic is store-backed, and every
    /// other one still answers from the vocabulary.**
    ///
    /// The domain is `Intrinsic::ALL`, which is GENERATED from the enum's
    /// own token list (`enum_with_all!`), so a variant cannot exist
    /// without being iterated here; and `tag_value_source` is an
    /// exhaustive `match` with no wildcard, so a new variant fails to
    /// compile until someone classifies it. The count is asserted, not
    /// the membership alone, so a SECOND intrinsic quietly acquiring a
    /// store read fails — which a "`name` is `Store`" assertion would
    /// not.
    #[test]
    fn exactly_one_intrinsic_is_store_backed() {
        let store: Vec<_> = pulsus_traceql::Intrinsic::ALL
            .iter()
            .copied()
            .filter(|i| tag_value_source(*i) == TagValueSource::Store)
            .collect();
        assert_eq!(
            store,
            vec![pulsus_traceql::Intrinsic::Name],
            "exactly `name` reads the store; every other intrinsic answers from the vocabulary"
        );
        assert_eq!(
            pulsus_traceql::Intrinsic::ALL.len(),
            21,
            "a new intrinsic must be classified in `tag_value_source` and counted here"
        );
    }

    /// The two closed keyword sets still answer from the vocabulary after
    /// `name` left it — the permutation break (exchanging `Name` and
    /// `Status` in the dispatch) makes this and the test above disagree.
    #[test]
    fn the_closed_keyword_intrinsics_are_vocabulary_backed() {
        for intrinsic in [
            pulsus_traceql::Intrinsic::Status,
            pulsus_traceql::Intrinsic::Kind,
        ] {
            assert_eq!(tag_value_source(intrinsic), TagValueSource::Vocabulary);
            assert!(!intrinsics::intrinsic_tag_values(intrinsic).is_empty());
        }
    }

    /// The configured lookback reaches the handler as nanoseconds.
    #[test]
    fn the_lookback_is_read_from_the_configuration() {
        let state = test_state();
        assert_eq!(tag_lookback_ns(&state), 24 * 3_600 * 1_000_000_000);
    }

    // ---- criterion 12: every range fault rejects on every §4.3 route --

    /// The six mounted §4.3 routes, as they are mounted.
    const SIX_ROUTES: [&str; 6] = [
        "/api/traces/v1/tags",
        "/api/v2/search/tags",
        "/api/search/tags",
        "/api/traces/v1/tag/service.name/values",
        "/api/v2/search/tag/service.name/values",
        "/api/search/tag/service.name/values",
    ];

    /// The seven fault shapes. Three of the four faults have a bound
    /// PERMUTATION and both sides are asserted — a rule that checked only
    /// `start` would pass with `end` unvalidated.
    const SEVEN_FAULTS: [(&str, &str); 7] = [
        ("malformed start", "start=abc&end=1700003600"),
        ("malformed end", "start=1700000000&end=abc"),
        ("half range, start only", "start=1700000000"),
        ("half range, end only", "end=1700003600"),
        ("zero start", "start=0&end=1700003600"),
        ("zero end", "start=1700000000&end=0"),
        ("inverted", "start=1700003600&end=1700000000"),
    ];

    /// The two ACCEPTING shapes, asserted on the same six routes as the
    /// direction-neutral half: a handler that rejected everything would
    /// pass the table above and fail here.
    const TWO_ACCEPTED: [(&str, &str); 2] = [
        ("both bounds zero", "start=0&end=0"),
        ("zero width", "start=1700000000&end=1700000000"),
    ];

    /// Builds the whole mounted router over a state with NO pool, so a
    /// `400` here proves the parse precedes the engine: anything that
    /// reached ClickHouse would be `503` instead.
    fn compat_router() -> axum::Router {
        let config = pulsus_config::Config {
            compat_endpoints: true,
            ..pulsus_config::Config::default()
        };
        crate::app::build_router(test_state(), &config).expect("router builds")
    }

    async fn status_of(router: &axum::Router, uri: String) -> StatusCode {
        use tower::ServiceExt;
        router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(uri)
                    .body(axum::body::Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response")
            .status()
    }

    /// Criterion 12. **7 fault shapes × 6 routes = 42 cells, all `400`,
    /// plus the 2 accepting shapes × 6 routes as the discriminator.**
    ///
    /// The domain is enumerated from two authorities — the fault shapes
    /// the range contract names, and the six routes as mounted — rather
    /// than from the cases that happened to be interesting.
    #[tokio::test]
    async fn every_range_fault_rejects_on_every_mounted_tag_route() {
        let router = compat_router();
        let mut checked = 0usize;
        for path in SIX_ROUTES {
            for (name, query) in SEVEN_FAULTS {
                let status = status_of(&router, format!("{path}?{query}")).await;
                assert_eq!(
                    status,
                    StatusCode::BAD_REQUEST,
                    "{path}?{query} ({name}) must be 400 before the pool"
                );
                checked += 1;
            }
            for (name, query) in TWO_ACCEPTED {
                let status = status_of(&router, format!("{path}?{query}")).await;
                assert_eq!(
                    status,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "{path}?{query} ({name}) must be accepted and reach the (absent) pool"
                );
                checked += 1;
            }
        }
        assert_eq!(checked, 54, "7 faults + 2 accepted, over 6 routes");
    }

    /// An unparseable `q` is NOT a fault: the editor sends half-typed text
    /// on every keystroke, so these reach the pool (503 here) rather than
    /// rejecting. The pair with the table above is what pins the rule
    /// "tolerate what the client cannot avoid sending; reject what
    /// indicates a fault".
    #[tokio::test]
    async fn a_half_typed_q_reaches_the_engine_rather_than_rejecting() {
        let router = compat_router();
        for q in [
            "%7Bspan.http.status_code%3D",
            "garbage",
            "%7B",
            "%7D",
            "%7B.foo%3D%7D",
        ] {
            for path in [
                "/api/traces/v1/tag/service.name/values",
                "/api/v2/search/tag/service.name/values",
                "/api/search/tag/service.name/values",
            ] {
                assert_eq!(
                    status_of(&router, format!("{path}?q={q}")).await,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "{path}?q={q} must not be rejected"
                );
            }
        }
    }
}
