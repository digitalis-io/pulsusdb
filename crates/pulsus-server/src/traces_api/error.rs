//! `/api/traces/v1`'s error envelope: `{"status":"error","errorType",
//! "error","position"?}` (docs/api.md §4.1/§4.2), and the status-code
//! mapping table pinned by the issue #55 plan (v2's error table + v3's
//! `406 not_acceptable`) plus issue #57's search rows. Mirrors
//! `logs_api/error.rs`'s structure. `position` (a byte offset) comes from
//! exactly two families, and both index a string the client supplied on
//! THIS request: a TraceQL parse failure — [`ApiError::Query`] always,
//! and [`ApiError::SearchParam`]/[`ApiError::QueryText`] when the inner is
//! `QueryTextError::Invalid`, the length-cap and semantic rejections
//! carrying none — and a legacy `tags` logfmt failure,
//! [`ApiError::Legacy`], always, whose offset indexes the decoded `tags`
//! value rather than a query expression. The other eleven of the enum's
//! fifteen variants render no `position`, which is why
//! [`read_error_parts`] returns no offset at all; and the §4.2 fetch path
//! raises none of the four that can — it reaches only `Param`,
//! `NotFound`, `NotAcceptable`, `Read`, `Assemble` and `PoolUnavailable`
//! (`handlers::trace_by_id_impl`).
//!
//! Errors are **always** this JSON envelope, never protobuf, regardless of
//! the request's `Accept` header (docs/api.md §4.1) — the mounted-but-
//! absent `404` JSON envelope doubles as the conformance suite's mounting
//! oracle (an unmounted path returns axum's empty `404` instead).

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use pulsus_clickhouse::ChError;
use pulsus_read::logql::ReadError;
use pulsus_traceql::TraceQlError;

use super::assemble::AssembleError;
use super::legacy::LegacyError;
use super::params::{
    GraphParamError, MetricsParamError, SearchParamError, TagPathError, TagsParamError,
    TraceIdError,
};

/// Every failure mode a `/api/traces/v1` handler can return, converted
/// to the documented error envelope by [`IntoResponse`]:
///
/// | variant | HTTP | `errorType` |
/// |---|---|---|
/// | `Param` / `SearchParam` / `MetricsParam` / `GraphParam` / `TagsParam` / `TagPath` | 400 | `bad_data` |
/// | `SearchParam(QueryText(Invalid))` (search `query` parse, carries `position`) | 400 | `bad_data` |
/// | `QueryText(Semantic)` / `SearchParam(QueryText(Semantic))` (issue #328 `traceql.Validate` port, no `position`) | 400 | `bad_data` |
/// | `Plan` (except the point cap) | 400 | `bad_data` |
/// | `Query` (TraceQL parse, carries `position`) | 400 | `bad_data` |
/// | `Legacy` (strict logfmt, carries `position` into `tags`) | 400 | `bad_data` |
/// | `NotFound` | 404 | `not_found` |
/// | `NotAcceptable` | 406 | `not_acceptable` |
/// | `Plan(MetricsPointCap)` (issue #59 static pre-execution rejection) | 422 | `query_too_broad` |
/// | `Read(…)` | see `read_error_parts` below — matched exhaustively (issue #266) |
/// | `PoolUnavailable` | 503 | `unavailable` |
/// | `Assemble(_)` | 500 | `internal` |
#[derive(Debug)]
pub(crate) enum ApiError {
    Param(TraceIdError),
    /// Search request-parameter failures (issue #57).
    SearchParam(SearchParamError),
    /// Metrics request-parameter failures (issue #59, no `position`).
    MetricsParam(MetricsParamError),
    /// Service-graph request-parameter failures (issue #173, no `position`).
    GraphParam(GraphParamError),
    /// `/tags` request-parameter failures (issue #58, no `position`).
    TagsParam(TagsParamError),
    /// `{tag}` path-parameter failures (issue #58, no `position`).
    TagPath(TagPathError),
    /// Legacy `tags` logfmt failures (issue #57).
    Legacy(LegacyError),
    /// TraceQL parse failure — `400 bad_data` with a `position` byte
    /// offset, matching the LogQL parse-error envelope.
    Query(TraceQlError),
    /// Query-text admission failure raised by a HANDLER rather than
    /// parameter parsing (issue #328): the executed expression parsed
    /// but failed the reference's semantic validation
    /// (`querytext::validate_semantics`). `400 bad_data`, the
    /// `invalid TraceQL query: ` wrapping, no `position` for the
    /// semantic variant.
    QueryText(super::querytext::QueryTextError),
    /// Search planning failure (unsupported field / type mismatch).
    Plan(pulsus_read::TracePlanError),
    /// The trace has no stored spans (an empty §4.2 fetch).
    NotFound,
    /// RFC 9110: no served representation is acceptable under the
    /// request's `Accept` header (plan v3 §3).
    NotAcceptable,
    Read(ReadError),
    Assemble(AssembleError),
    /// The ClickHouse pool has not been established yet — same "not yet
    /// serving" contract as `logs_api::error::ApiError::PoolUnavailable`
    /// (mirrors `/ready`'s 503).
    PoolUnavailable,
}

impl From<TraceIdError> for ApiError {
    fn from(e: TraceIdError) -> Self {
        ApiError::Param(e)
    }
}

impl From<SearchParamError> for ApiError {
    fn from(e: SearchParamError) -> Self {
        ApiError::SearchParam(e)
    }
}

impl From<super::querytext::QueryTextError> for ApiError {
    fn from(e: super::querytext::QueryTextError) -> Self {
        ApiError::QueryText(e)
    }
}

impl From<MetricsParamError> for ApiError {
    fn from(e: MetricsParamError) -> Self {
        ApiError::MetricsParam(e)
    }
}

impl From<GraphParamError> for ApiError {
    fn from(e: GraphParamError) -> Self {
        ApiError::GraphParam(e)
    }
}

impl From<LegacyError> for ApiError {
    fn from(e: LegacyError) -> Self {
        ApiError::Legacy(e)
    }
}

impl From<TagsParamError> for ApiError {
    fn from(e: TagsParamError) -> Self {
        ApiError::TagsParam(e)
    }
}

impl From<TagPathError> for ApiError {
    fn from(e: TagPathError) -> Self {
        ApiError::TagPath(e)
    }
}

impl From<ReadError> for ApiError {
    fn from(e: ReadError) -> Self {
        ApiError::Read(e)
    }
}

impl From<AssembleError> for ApiError {
    fn from(e: AssembleError) -> Self {
        ApiError::Assemble(e)
    }
}

#[derive(Serialize)]
struct ErrorEnvelope {
    status: &'static str,
    #[serde(rename = "errorType")]
    error_type: &'static str,
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    position: Option<usize>,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error_type, message, position) = match &self {
            ApiError::Param(e) => (StatusCode::BAD_REQUEST, "bad_data", e.to_string(), None),
            // `position` only when the failure is the query-frontend
            // validator's parse step on the search `query` parameter
            // (issue #326) — the same byte offset `ApiError::Query`
            // reports for a malformed `q`, so a TraceQL parse error looks
            // the same whichever parameter carried it.
            ApiError::SearchParam(e) => (
                StatusCode::BAD_REQUEST,
                "bad_data",
                e.to_string(),
                e.position(),
            ),
            ApiError::MetricsParam(e) => (StatusCode::BAD_REQUEST, "bad_data", e.to_string(), None),
            ApiError::GraphParam(e) => (StatusCode::BAD_REQUEST, "bad_data", e.to_string(), None),
            ApiError::TagsParam(e) => (StatusCode::BAD_REQUEST, "bad_data", e.to_string(), None),
            ApiError::TagPath(e) => (StatusCode::BAD_REQUEST, "bad_data", e.to_string(), None),
            // Strict logfmt errors carry a byte offset into the decoded
            // `tags` value (code review round 1 — documented in
            // docs/api.md §4.2 alongside the TraceQL parse offset).
            ApiError::Legacy(e) => (
                StatusCode::BAD_REQUEST,
                "bad_data",
                e.to_string(),
                Some(e.pos()),
            ),
            ApiError::Query(e) => (
                StatusCode::BAD_REQUEST,
                "bad_data",
                e.to_string(),
                Some(e.span().start),
            ),
            // Issue #328: the semantic-validation rejection carries the
            // same envelope whichever parameter carried the expression
            // (`position` is `None` for the semantic variant — the
            // reference's Validate errors name no offset).
            ApiError::QueryText(e) => (
                StatusCode::BAD_REQUEST,
                "bad_data",
                e.to_string(),
                e.position(),
            ),
            // The metrics point cap is the one plan-time 422 (issue #59
            // adjudication: a static pre-execution rejection in the
            // too-broad family — bounded response, never a silent
            // truncation); every other plan failure stays a 400.
            ApiError::Plan(e @ pulsus_read::TracePlanError::MetricsPointCap { .. }) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "query_too_broad",
                e.to_string(),
                None,
            ),
            ApiError::Plan(e) => (StatusCode::BAD_REQUEST, "bad_data", e.to_string(), None),
            ApiError::NotFound => (
                StatusCode::NOT_FOUND,
                "not_found",
                "trace not found".to_string(),
                None,
            ),
            ApiError::NotAcceptable => (
                StatusCode::NOT_ACCEPTABLE,
                "not_acceptable",
                "no acceptable representation: this endpoint serves application/json and \
                 application/protobuf"
                    .to_string(),
                None,
            ),
            ApiError::Read(e) => {
                let (status, error_type, message) = read_error_parts(e);
                (status, error_type, message, None)
            }
            ApiError::Assemble(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                e.to_string(),
                None,
            ),
            ApiError::PoolUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                "clickhouse pool not yet established".to_string(),
                None,
            ),
        };
        let body = ErrorEnvelope {
            status: "error",
            error_type,
            error: message,
            position,
        };
        (status, Json(body)).into_response()
    }
}

/// The `ReadError` half of the table above, matched **exhaustively**
/// (issue #266) — the shape `logs_api::error::read_error_parts` and
/// `prom_api::error::read_error_parts` have had all along. The catch-all
/// this replaces put every unlisted variant on 500 `internal` by
/// omission, so adding or re-routing a `ReadError` variant was compiler-
/// forced on two of the three query surfaces and silently absorbed by the
/// third; it now fails the build on all three.
///
/// That absorption is a live hazard, not a tidiness one: Grafana's Tempo
/// datasource proxies our status and body through verbatim —
/// `grafana/grafana-tempo-datasource` `pkg/tempo/tempo.go:370,373`
/// @ `3c7375bb541c3acde1deb068ea7ead9ebfdf56b9` (`v13.1.5-11-g3c7375b`)
/// copies the upstream headers, then `rw.WriteHeader(resp.StatusCode)`
/// and `io.Copy(rw, resp.Body)` with no status rewriting. So a rejection
/// that should be 400 arriving as 500 stops being "your query is wrong"
/// and becomes "this datasource is failing": Grafana reports the
/// datasource unhealthy and dependent alert rules go to Error state, over
/// a database that is fine.
///
/// No `ReadError` that can reach this surface carries a `position`, hence
/// the 3-tuple. The traces envelope's offset indexes a client-supplied
/// string this renderer can point at (the TraceQL text, or the legacy
/// `tags` value), and each of the three trace-reachable variants —
/// `QueryTooBroad`, `Clickhouse`, `PipelineInvalid` (enumerated below) —
/// is raised after that text has parsed, so none of them can name an
/// offset into it. [`ReadError::Parse`] does carry a span, and it is by
/// definition a parse failure, but it is a **LogQL** span: an offset into
/// a query text this surface never receives. It is unreachable here (see
/// the reachability paragraph below), and if the call graph ever brings
/// it here its offset still must not be rendered as a traces `position` —
/// pinned by `a_logql_parse_error_renders_no_traces_position`.
///
/// Reachability, checked at the construction sites rather than assumed.
/// `git grep 'ReadError::' -- crates/pulsus-read/src/traces` — the read
/// path ALONE, deliberately NOT `traces_api`, whose renderer tests below
/// construct all fifteen variants on purpose — matches two of that
/// directory's 13 files, `exec.rs` and `search_eval.rs`, and every
/// construction outside their `#[cfg(test)]` modules is `QueryTooBroad`,
/// `Clickhouse` or `PipelineInvalid`. `traces/` names no
/// `LogQlError`/`PromqlError`/`HistogramError` at all (so nothing there
/// can `?`-convert into the `#[from]` variants), and the one
/// `PipelineError` it does touch is mapped to `TracePlanError`
/// (`traces/filter.rs:624`). Of the `pulsus-read` items `traces_api`
/// calls, only `TraceEngine`'s methods and
/// `plan_search`/`plan_trace_metrics` are fallible — `EvalGate::new` and
/// `canonical_double_bits` return no `Result` at all. The claims below are
/// wrong if some future traces path calls a `pulsus-read` entry point
/// outside `traces/` that returns `ReadError` — which is exactly the case
/// the exhaustive match exists to force a decision on.
///
/// Wire effect of issue #266: no pre-existing conformance fixture, live
/// assertion or documented wire expectation changed — every status a
/// client can observe today (the three reachable variants, and every one
/// of `ChError`'s seven inners: `Timeout`'s 504 and the other six's 500)
/// is the one it had under the catch-all, and docs/api.md §4.1-§4.4's
/// error tables are unchanged and still
/// correct. The renderer tests below ARE new: they are fresh pins on
/// statuses the catch-all left unstated, not edits to existing ones.
fn read_error_parts(e: &ReadError) -> (StatusCode, &'static str, String) {
    match e {
        // REACHABLE. The trace scan/result/generator budgets and the
        // `by()` series cap (docs/api.md §4.2/§4.4).
        ReadError::QueryTooBroad(_) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            "query_too_broad",
            e.to_string(),
        ),
        // UNREACHABLE here — in production `NamelessSelectorUnresolvable`
        // is constructed only by the metrics engine (`metrics/exec.rs`,
        // four sites), which no traces handler calls; a `Display` test in
        // `logql/error.rs` and the renderer test below also build it, as
        // tests. `HistogramResultUnsupported` has no
        // production construction site left anywhere in the workspace:
        // M7-A5b's histogram encoders replaced that reject
        // (`metrics/exec.rs:3693`), leaving renderer tests as its only
        // constructors (`prom_api/error.rs`'s and the one below), which
        // build it precisely because nothing else does. Mapped to
        // this surface's bounded-response family, the same class both
        // other renderers give them: a well-formed query the engine
        // declines, never a server fault. `prom_api` spells that class
        // `execution`; this surface (like `logs_api`) has only
        // `query_too_broad` in its documented taxonomy — docs/api.md
        // §4.1's table lists no `execution` type — so the STATUS matches
        // prom_api and the `errorType` stays in the traces vocabulary.
        ReadError::NamelessSelectorUnresolvable { .. } | ReadError::HistogramResultUnsupported => (
            StatusCode::UNPROCESSABLE_ENTITY,
            "query_too_broad",
            e.to_string(),
        ),
        // REACHABLE (issue #335 Stage B: a present NON-boolean operand
        // under `!` — `expression (!.a) expected a boolean`, the eval-time
        // failure the reference reports as well).
        //
        // Issue #240: a LogQL rejection is a client error on every surface
        // that can carry it — `logs_api::error::read_error_parts` and
        // `prom_api::error::read_error_parts` both map it to 400
        // `bad_data`. It WAS unreachable from `traces_api` when this arm
        // was written, and matching it anyway is why the Stage B change
        // landed correctly instead of falling into the 500 catch-all.
        // `Display` is transparent, so `e.to_string()` is the body,
        // unmodified.
        //
        // STATUS DIVERGENCE, deliberate: the reference answers that query
        // 500, because the failure happens mid-scan inside its querier. A
        // malformed query is the client's error, not a server fault, so we
        // keep 400 `bad_data` — consistent with every other surface
        // carrying this variant. The accept-surface matrix scores a
        // reference 500 as INCONCLUSIVE rather than as a rejection, so the
        // scoreboard is unaffected either way. Pinned end to end by
        // `traces_search_live::
        // negation_demands_a_boolean_where_truthiness_tolerates_a_string`.
        ReadError::PipelineInvalid { .. } => (StatusCode::BAD_REQUEST, "bad_data", e.to_string()),
        // UNREACHABLE here, and NOT a malformed request — which is why it
        // is split out of the uniform 400 below. `Cancelled` is raised at
        // the PromQL evaluator's cancel checkpoints (`pulsus-promql`
        // `eval/mod.rs:308,369,432,1577`) once the awaiting request future
        // has already been dropped: a client disconnect, or the
        // server-wide `TimeoutLayer` firing first (`middleware.rs:59`,
        // which answers 408 itself). Nothing about the query was wrong, so
        // 400 `bad_data` would be a false accusation; this takes
        // `prom_api`'s mapping for the same variant, 408 `timeout`.
        // `timeout` is already this surface's documented `errorType`
        // (docs/api.md §4.1-§4.4 spell the ClickHouse read timeout that
        // way), so no type outside the traces taxonomy is invented — only
        // the status is one §4.1's table does not list, and like
        // `prom_api`'s arm this is unreachable in practice: by the time
        // the variant exists, the future that would encode the response is
        // gone. (`prom_api`'s 408 is undocumented for the same reason.)
        ReadError::Promql(pulsus_promql::PromqlError::Cancelled) => {
            (StatusCode::REQUEST_TIMEOUT, "timeout", e.to_string())
        }
        // UNREACHABLE here — no PRODUCTION code under
        // `crates/pulsus-read/src/traces` or `traces_api` constructs any
        // of these (the renderer tests below build seven of them on
        // purpose), and this surface's only fallible route into
        // `pulsus-read` is `TraceEngine` and the plan functions (see
        // above). Where they DO come from, enumerated pattern by pattern:
        // the LogQL planner/pipeline builds six of the eight non-`Promql`
        // patterns — `EmptyMatcherSet`, `ContradictoryMatchers`,
        // `InvalidStep` and `QuerySpanTooLong` in `logql/plan.rs`,
        // `DurationOutOfRange` in `logql/params.rs`, `MetricPipelineError`
        // in `logql/client_agg.rs`. `Parse` has no explicit production
        // construction site anywhere: it arises from the
        // `#[from] LogQlError` conversion (the renderer test below builds
        // one directly, as a test). `PipelineUnsupportedInMetric` has no
        // production construction site left in the workspace either (M6-10
        // replaced that rejection with client aggregation —
        // `logql/plan.rs:1602`); as with `HistogramResultUnsupported`
        // above, the only code that builds it is renderer tests,
        // `logs_api/error.rs`'s and the one below. `Promql` is different
        // in kind: the metrics engine constructs exactly ONE of its ten
        // inners directly, `InvalidRegexMatcher` (`metrics/dispatch.rs`,
        // `metrics/exec.rs` — three sites), and the other nine are raised
        // inside `pulsus-promql` (`eval/`, `plan.rs`, `parser.rs`) and
        // reach `ReadError` through its own `#[from] PromqlError`
        // conversion. TraceQL's own equivalents arrive as
        // `ApiError::Query`/`ApiError::Plan` instead.
        //
        // The eight non-`Promql` patterns take the class BOTH
        // other renderers give them — a malformed or out-of-domain client
        // query is 400 `bad_data`, and it stays 400 if the call graph ever
        // brings one here, rather than becoming a 500 that blames the
        // database for the client's query.
        //
        // `Promql` is the exception to that sentence, and only against
        // `prom_api`. `logs_api` is uniform 400 across every inner (its
        // own `ReadError::Promql(_)` arm), so this surface agrees with it
        // apart from `Cancelled` above. `prom_api` instead splits per
        // inner: `Parse`/`InvalidRegexMatcher` 400 `bad_data` (agreeing
        // with this arm), then `Unsupported`, `BadMatching`,
        // `HistogramBucket`, `LabelSet`, `InvalidParameter`, `ScalarOp`
        // and `ExtendedHistogram` — seven of the ten — 422 `execution`,
        // and `Cancelled` 408. We do not reproduce that split: it is a
        // PromQL-API contract (docs/api.md §3's five-type taxonomy) whose
        // 422 `execution` type §4.1's traces table does not carry, so
        // reproducing it here would invent a traces mapping for a type
        // this endpoint does not document.
        ReadError::Parse(_)
        | ReadError::Promql(_)
        | ReadError::EmptyMatcherSet
        | ReadError::ContradictoryMatchers
        | ReadError::InvalidStep
        | ReadError::DurationOutOfRange { .. }
        | ReadError::QuerySpanTooLong { .. }
        | ReadError::MetricPipelineError { .. }
        | ReadError::PipelineUnsupportedInMetric { .. } => {
            (StatusCode::BAD_REQUEST, "bad_data", e.to_string())
        }
        // UNREACHABLE here — a `metric_hist_samples` row that cannot
        // rebuild a histogram is a metrics-path data-integrity defect, and
        // it is a genuine 500 wherever it occurs (both other renderers
        // agree): the client's request was fine, our stored row was not.
        ReadError::HistogramDecode(_) => {
            (StatusCode::INTERNAL_SERVER_ERROR, "internal", e.to_string())
        }
        // REACHABLE — `ReadError::Clickhouse` is one of the three variants
        // the traces read path builds. The INNERS are enumerated rather
        // than left on a `_` so a new `ChError` variant is a decision here
        // too, not because each is individually reachable: a read never
        // produces `InsertUncertain` (`pulsus-clickhouse`
        // `client.rs:180`, an insert-path downgrade). The mapping is
        // unchanged from the catch-all era — only `Timeout` is special.
        // `Connect` staying 500 (where `prom_api` answers 503
        // `unavailable`) is the pre-existing cross-surface difference this
        // change deliberately does NOT touch: it is live on both the
        // traces and logs surfaces, so re-routing it is a wire change with
        // its own sweep, not a by-product of making the match total.
        ReadError::Clickhouse(ch) => match ch {
            ChError::Timeout(_) => (StatusCode::GATEWAY_TIMEOUT, "timeout", e.to_string()),
            ChError::Connect(_)
            | ChError::Io(_)
            | ChError::Server { .. }
            | ChError::Decode(_)
            | ChError::Config(_)
            | ChError::InsertUncertain(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal", e.to_string())
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn envelope(err: ApiError) -> (StatusCode, serde_json::Value) {
        let res = err.into_response();
        let status = res.status();
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .expect("read body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("valid json");
        (status, json)
    }

    /// Issue #240: the LogQL-class rejection is 400 `bad_data` on this
    /// surface too, with the BARE reason as the whole body (the variant's
    /// `Display` carries no prefix). Deleting the explicit arm no longer
    /// compiles — since issue #266 the match is exhaustive, so the arm
    /// cannot fall back to the 500 `internal` catch-all it would have hit
    /// before; restoring a decorating `#[error]` prefix breaks the
    /// byte-exact `error`-field assertion.
    #[tokio::test]
    async fn read_error_pipeline_invalid_maps_to_400_bad_data() {
        let err = ApiError::Read(ReadError::PipelineInvalid {
            reason: "bad regex: unclosed group".to_string(),
        });
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["status"], "error");
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_none());
        assert_eq!(json["error"], "bad regex: unclosed group");
    }

    /// Issue #240 (AC2's once-only leg): a reason that itself begins
    /// `parse error ` appears in the rendered body exactly once — no
    /// renderer- or variant-level decoration doubles it.
    #[tokio::test]
    async fn read_error_pipeline_invalid_body_is_the_reason_exactly_once() {
        let reason = "parse error : synthetic prefix-collision probe";
        let err = ApiError::Read(ReadError::PipelineInvalid {
            reason: reason.to_string(),
        });
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let body = json["error"].as_str().expect("error body");
        assert_eq!(body, reason);
        assert_eq!(body.matches("parse error ").count(), 1);
    }

    #[tokio::test]
    async fn param_error_maps_to_400_bad_data() {
        let err = ApiError::Param(TraceIdError::InvalidLength("abc".to_string()));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["status"], "error");
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_none());
    }

    #[tokio::test]
    async fn tags_param_error_maps_to_400_bad_data_without_a_position() {
        let err = ApiError::TagsParam(TagsParamError::UnsupportedScope("bogus".to_string()));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["status"], "error");
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_none());
        assert!(
            json["error"].as_str().is_some_and(|m| m.contains("bogus")),
            "message must name the rejected scope, got {json}"
        );
    }

    #[tokio::test]
    async fn tag_path_error_maps_to_400_bad_data_without_a_position() {
        let err = ApiError::TagPath(TagPathError::EmptyKey);
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_none());
    }

    #[tokio::test]
    async fn not_found_maps_to_404_not_found() {
        let (status, json) = envelope(ApiError::NotFound).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(json["errorType"], "not_found");
    }

    #[tokio::test]
    async fn not_acceptable_maps_to_406_not_acceptable() {
        let (status, json) = envelope(ApiError::NotAcceptable).await;
        assert_eq!(status, StatusCode::NOT_ACCEPTABLE);
        assert_eq!(json["errorType"], "not_acceptable");
    }

    #[tokio::test]
    async fn read_clickhouse_timeout_maps_to_504_timeout() {
        let err = ApiError::Read(ReadError::Clickhouse(ChError::Timeout(
            "deadline".to_string(),
        )));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(json["errorType"], "timeout");
    }

    #[tokio::test]
    async fn read_other_clickhouse_error_maps_to_500_internal() {
        let err = ApiError::Read(ReadError::Clickhouse(ChError::Decode(
            "bad row".to_string(),
        )));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(json["errorType"], "internal");
    }

    /// Issue #266: a connect failure keeps the 500 it had under the
    /// catch-all — enumerating `ChError` did not silently adopt
    /// `prom_api`'s 503 `unavailable` for it. Re-routing this one is a
    /// live wire change on two surfaces and is deliberately out of scope.
    #[tokio::test]
    async fn read_clickhouse_connect_error_still_maps_to_500_internal() {
        let err = ApiError::Read(ReadError::Clickhouse(ChError::Connect(
            "refused".to_string(),
        )));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(json["errorType"], "internal");
    }

    /// Issue #266: a LogQL planner/pipeline client-input rejection is 400
    /// `bad_data` here as it is on `logs_api`/`prom_api`, not the 500 the
    /// removed catch-all gave it. All are unreachable from a traces
    /// handler today (the LogQL planner is not on this call graph, and
    /// `PipelineUnsupportedInMetric` has had no production construction
    /// site anywhere since M6-10 — the loop below is one of its two, both
    /// renderer tests) — the arms are the record of the decision, so a
    /// future re-route cannot make one a 500 by omission.
    #[tokio::test]
    async fn a_logql_planner_read_error_maps_to_400_bad_data_not_500() {
        for err in [
            ReadError::EmptyMatcherSet,
            ReadError::ContradictoryMatchers,
            ReadError::InvalidStep,
            ReadError::DurationOutOfRange {
                what: "range",
                value: 0,
                max: i64::MAX,
            },
            ReadError::QuerySpanTooLong {
                value: 1,
                max: pulsus_logql::MAX_QUERY_SPAN_NS,
            },
            ReadError::MetricPipelineError {
                error_type: "JSONParserErr".to_string(),
                series: "{a=\"b\"}".to_string(),
            },
            ReadError::PipelineUnsupportedInMetric {
                construct: "| json".to_string(),
            },
        ] {
            let rendered = format!("{err}");
            let (status, json) = envelope(ApiError::Read(err)).await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "expected 400 for {rendered}, got {json}"
            );
            assert_eq!(json["errorType"], "bad_data", "for {rendered}");
            assert!(json.get("position").is_none(), "for {rendered}");
        }
    }

    /// Issue #266 review round 1: a cancelled PromQL evaluation is a
    /// dropped request future (client disconnect, or the server's own 408
    /// `TimeoutLayer`), never malformed input — 408 `timeout`, the mapping
    /// `prom_api` gives the same variant, NOT the uniform 400 `bad_data`
    /// the other `Promql` inners take here.
    #[tokio::test]
    async fn a_cancelled_promql_evaluation_maps_to_408_timeout_not_400_bad_data() {
        let err = ApiError::Read(ReadError::Promql(pulsus_promql::PromqlError::Cancelled));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
        assert_eq!(json["errorType"], "timeout");
        assert!(json.get("position").is_none(), "body {json}");
    }

    /// Issue #266: every OTHER `Promql` inner stays on the uniform 400
    /// `bad_data` — deliberately not `prom_api`'s 422 `execution` for the
    /// declined-evaluation family, whose `errorType` docs/api.md §4.1's
    /// traces table does not define. `Unsupported` is a 422 on `prom_api`,
    /// so it pins the divergence rather than merely agreeing with it.
    #[tokio::test]
    async fn a_non_cancellation_promql_error_maps_to_400_bad_data() {
        let err = ApiError::Read(ReadError::Promql(pulsus_promql::PromqlError::Unsupported {
            construct: "the @ modifier".to_string(),
        }));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_none());
    }

    /// Issue #266 review round 1: [`ReadError::Parse`] is the one arm that
    /// IS a parse failure and DOES carry a span — a **LogQL** span, into a
    /// query text this surface never receives. It must therefore never
    /// render a traces `position`, which the renderer guarantees
    /// structurally (`read_error_parts` returns a 3-tuple and the call
    /// site supplies `None`); this pins that guarantee against a future
    /// widening.
    #[tokio::test]
    async fn a_logql_parse_error_renders_no_traces_position() {
        let inner = pulsus_logql::parse("{").expect_err("must fail");
        assert!(
            inner.span().end <= "{".len(),
            "the span must be an offset into the LOGQL text, got {:?}",
            inner.span()
        );
        let (status, json) = envelope(ApiError::Read(ReadError::Parse(inner))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_none(), "body {json}");
    }

    /// Issue #266: the two metrics-path "engine declines a well-formed
    /// query" variants take this surface's bounded-response family (the
    /// 422 `prom_api` gives them, spelled with the `errorType` docs/api.md
    /// §4.1 defines for traces), not the removed catch-all's 500. Only
    /// `NamelessSelectorUnresolvable` is raised by the metrics engine
    /// today; `HistogramResultUnsupported` has had no production
    /// construction site since M7-A5b's histogram encoders replaced that
    /// reject — the loop below is one of its two, both renderer tests.
    #[tokio::test]
    async fn a_metrics_engine_decline_maps_to_422_query_too_broad_not_500() {
        for err in [
            ReadError::NamelessSelectorUnresolvable {
                reason: "ColdCache".to_string(),
            },
            ReadError::HistogramResultUnsupported,
        ] {
            let rendered = format!("{err}");
            let (status, json) = envelope(ApiError::Read(err)).await;
            assert_eq!(
                status,
                StatusCode::UNPROCESSABLE_ENTITY,
                "expected 422 for {rendered}, got {json}"
            );
            assert_eq!(json["errorType"], "query_too_broad", "for {rendered}");
        }
    }

    /// Issue #266: a histogram-decode failure is the one newly explicit
    /// variant that genuinely IS a 500 — our stored row is malformed, the
    /// client's request was not. Same verdict on all three renderers.
    #[tokio::test]
    async fn a_histogram_decode_failure_stays_500_internal() {
        let err = ReadError::HistogramDecode(pulsus_model::HistogramError::SchemaOutOfRange(200));
        let (status, json) = envelope(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(json["errorType"], "internal");
    }

    #[tokio::test]
    async fn assemble_unsupported_payload_type_maps_to_500_internal_naming_the_count() {
        let err = ApiError::Assemble(AssembleError::UnsupportedPayloadType { count: 3 });
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(json["errorType"], "internal");
        assert!(
            json["error"].as_str().is_some_and(|m| m.contains("3")),
            "message must name the count, got {json}"
        );
    }

    #[tokio::test]
    async fn pool_unavailable_maps_to_503() {
        let (status, json) = envelope(ApiError::PoolUnavailable).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["errorType"], "unavailable");
    }

    #[tokio::test]
    async fn a_traceql_parse_error_maps_to_400_bad_data_with_a_position() {
        let err = pulsus_traceql::parse("{ ").expect_err("must fail");
        let (status, json) = envelope(ApiError::Query(err)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json["position"].is_u64(), "body {json}");
    }

    #[tokio::test]
    async fn query_too_broad_maps_to_422_query_too_broad() {
        let err = ApiError::Read(ReadError::QueryTooBroad(
            pulsus_read::logql::TooBroadReason::TraceScanBudgetRows { budget_rows: 42 },
        ));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "query_too_broad");
        assert!(json.get("position").is_none());
    }

    #[tokio::test]
    async fn a_logfmt_error_maps_to_400_bad_data_with_its_tags_offset() {
        let err = ApiError::Legacy(LegacyError::UnquotedEquals {
            key: "a".to_string(),
            pos: 3,
        });
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert_eq!(json["position"], 3, "body {json}");
    }

    /// Issue #266 review round 3: the module header used to say
    /// `position` appears only on TraceQL parse errors, while
    /// `ApiError::Legacy` (above) and `ApiError::SearchParam` also emit
    /// one. The corrected header states the rule per inner, so pin that
    /// rule rather than leaving it prose: the offset appears for
    /// `QueryTextError::Invalid` — the validated `query` parameter's
    /// parse failure — and for neither of that enum's other two inners,
    /// whichever of the two `ApiError` variants carries it.
    #[tokio::test]
    async fn a_query_text_position_appears_for_the_parse_inner_only() {
        use super::super::querytext::QueryTextError;

        let invalid = || QueryTextError::Invalid {
            message: "syntax error".to_string(),
            position: 7,
        };
        for err in [
            ApiError::SearchParam(SearchParamError::QueryText(invalid())),
            ApiError::QueryText(invalid()),
        ] {
            let (status, json) = envelope(err).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(json["errorType"], "bad_data");
            assert_eq!(json["position"], 7, "body {json}");
        }

        let offsetless = || {
            [
                QueryTextError::TooLong { len: 9, cap: 8 },
                QueryTextError::Semantic(pulsus_traceql::ValidateError::TypeMismatch {
                    expr: "1 = `a`".to_string(),
                }),
            ]
        };
        let [long_a, sem_a] = offsetless();
        let [long_b, sem_b] = offsetless();
        for err in [
            ApiError::SearchParam(SearchParamError::QueryText(long_a)),
            ApiError::SearchParam(SearchParamError::QueryText(sem_a)),
            ApiError::QueryText(long_b),
            ApiError::QueryText(sem_b),
            // A non-`QueryText` search-parameter failure is about the
            // request, not a place inside an expression.
            ApiError::SearchParam(SearchParamError::ConflictingQuery),
        ] {
            let (status, json) = envelope(err).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(json.get("position").is_none(), "body {json}");
        }
    }

    #[tokio::test]
    async fn a_plan_error_maps_to_400_bad_data_without_a_position() {
        let err = ApiError::Plan(pulsus_read::TracePlanError::TypeMismatch(
            "status supports only = and !=".to_string(),
        ));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_none());
    }

    #[tokio::test]
    async fn a_metrics_param_error_maps_to_400_bad_data_without_a_position() {
        let err = ApiError::MetricsParam(MetricsParamError::InvalidStep("500ms".to_string()));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["errorType"], "bad_data");
        assert!(json.get("position").is_none());
        assert!(
            json["error"].as_str().is_some_and(|m| m.contains("500ms")),
            "message must name the rejected step, got {json}"
        );
    }

    #[tokio::test]
    async fn the_metrics_point_cap_plan_error_maps_to_422_query_too_broad() {
        // Issue #59 adjudication: the one plan-time 422 — never conflated
        // with the 400 bad_data plan family.
        let err = ApiError::Plan(pulsus_read::TracePlanError::MetricsPointCap {
            buckets: 12_000,
            cap: 11_000,
        });
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "query_too_broad");
        assert!(json.get("position").is_none());
        assert!(
            json["error"].as_str().is_some_and(|m| m.contains("12000")),
            "message must name the bucket count, got {json}"
        );
    }

    #[tokio::test]
    async fn the_metrics_set_budget_reason_maps_to_422_query_too_broad() {
        let err = ApiError::Read(ReadError::QueryTooBroad(
            pulsus_read::logql::TooBroadReason::TraceMetricsSetRows {
                max_set_rows: 1_000_000,
            },
        ));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "query_too_broad");
    }

    /// Issue #57 re-audit AC-conformance: the generator-memory reason
    /// carries the same envelope as every other `QueryTooBroad` variant
    /// — no dedicated match arm was needed (`ApiError::Read`'s
    /// `QueryTooBroad(_)` arm already covers it).
    #[tokio::test]
    async fn the_generator_memory_reason_maps_to_422_query_too_broad() {
        let err = ApiError::Read(ReadError::QueryTooBroad(
            pulsus_read::logql::TooBroadReason::TraceGeneratorMemory {
                budget_bytes: 1_048_576,
            },
        ));
        let (status, json) = envelope(err).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(json["errorType"], "query_too_broad");
        assert!(
            json["error"]
                .as_str()
                .is_some_and(|m| m.contains("generator memory")),
            "message must name the reason, got {json}"
        );
    }
}
