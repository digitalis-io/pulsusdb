//! `/api/logs/v1`'s error body: a BARE `text/plain` message (docs/api.md
//! §2.3 "Errors"), and the status-code mapping table pinned by the issue
//! #13 architect plan.
//!
//! Issue #264 replaced the previous `{"status","errorType","error",
//! "position"?}` JSON envelope with the reference's shape. Loki writes
//! every LogQL error its HANDLERS produce — the scope of this module, i.e.
//! every [`ApiError`] variant below — through one function,
//! `pkg/util/server/error.go:46-52 @ v3.7.4`:
//!
//! ```go
//! func WriteError(err error, w http.ResponseWriter) {
//!     status, cerr := ClientHTTPStatusAndError(err)
//!     w.Header().Set("Content-Type", "text/plain; charset=utf-8")
//!     w.Header().Set("X-Content-Type-Options", "nosniff")
//!     w.WriteHeader(status)
//!     fmt.Fprint(w, cerr.Error())
//! }
//! ```
//!
//! `fmt.Fprint` adds no terminator, so there is **no trailing newline**.
//! Every LogQL route reaches it: the query/series/label/stats/volume/
//! patterns/detected family through
//! `pkg/querier/queryrange/serialize.go:67,73,81,89` and
//! `pkg/lokifrontend/frontend/transport/handler.go:141`, and `/tail`'s
//! pre-upgrade rejections through `pkg/querier/tail/http.go:35,42` — all
//! @ v3.7.4.
//!
//! **What that does NOT cover: the routing layer.** Rejections made above
//! the handlers are not `WriteError`'s in the reference either, and they
//! do not match. Measured on `grafana/loki:3.7.4` (2026-08-07): an
//! unrouted path is `404 page not found\n` — LF-terminated plain text via
//! Go's `http.NotFound` — where ours is an empty axum `404`; a wrong
//! method is an empty `405` with no `Allow` there and an empty `405` with
//! `Allow` here. Both pre-date #264 and are unchanged by it; documented in
//! docs/api.md §2.3.
//!
//! Measured to agree, on `grafana/loki:3.7.4`
//! (`sha256:87f0a067673756a3cede1bcbf0c74875f7df9b09fddb53e399d0c576f756cfcc`,
//! 2026-08-07): a parse error (`query={`) on each of `/query`,
//! `/query_range`, `/series`, `/label/{n}/values`, `/index/stats`,
//! `/index/volume`, `/patterns`, `/detected_labels`, `/detected_fields`
//! and `/tail`; plus, on `/query_range`, the param class (missing
//! `query`; unparseable `step`; out-of-range `end`), the
//! pipeline-validation class (`|~ "("`) and the limit class (a
//! 131,081-byte POST-form query). All 18 answered `400` with
//! `Content-Type: text/plain; charset=utf-8` and
//! `X-Content-Type-Options: nosniff`, and **no** response's last byte was
//! `0x0a` (`}`, `d`, `n`, `` ` ``, `e`, `)`); the limit case was
//! `od -c`-checked whole, 51 bytes ending `…(131081 > 131072)`.
//!
//! The `422`, `500` and `504` rows of the table below are NOT
//! container-probed. Every input tried above was classified `400` by
//! `ClientHTTPStatusAndError`, and this suite made no attempt to drive
//! the reference into a genuine `5xx` (it has such paths — see the
//! docs/api.md §2.1 note on its `500` for a vector-matching cardinality
//! violation). The source settles those rows instead, and more strongly
//! than a probe of any one of them could: `WriteError` takes the status
//! as a *value* and then writes the same two headers and the same
//! `fmt.Fprint` body regardless of it, so the container provably cannot
//! vary by status code.
//!
//! Only this surface changed. The three query surfaces have three
//! independent `IntoResponse` impls — this one, `prom_api::error` and
//! `traces_api::error` — so nothing here reaches the other two; and the
//! two left alone were each checked rather than assumed (issue #264
//! determination 2):
//!
//! - `prom_api` keeps its JSON envelope and is CORRECT to. Upstream
//!   Prometheus writes every API error as `application/json` carrying
//!   `{status, errorType, error}` — `respondError`,
//!   `web/api/v1/api.go:2200-2230`, read at
//!   `vendor/github.com/prometheus/prometheus/` @ grafana/loki v3.7.4.
//! - `traces_api` keeps its JSON envelope and is KNOWN DIVERGENT.
//!   Tempo v3.0.2 writes trace query errors as plain text: the querier
//!   calls `http.Error` directly (`modules/querier/http.go:45,52,94,…`)
//!   and the query frontend routes everything through dskit's
//!   `httpgrpc.WriteError` (`modules/frontend/handler.go:156-168`), which
//!   is `http.Error` as well. Changing it is a second wire-shape change
//!   across a second surface and is deliberately NOT done here — #264's
//!   ruling and its measured sweep both name the LogQL surface. Recorded
//!   in docs/api.md §2.3 as an open divergence.
//!
//! Note also that plain text alone is not the whole contract. The
//! reference's two Loki writers agree on BOTH headers — Go's `http.Error`
//! sets `Content-Type` and `nosniff` exactly as `WriteError` does — and
//! disagree only on the trailing byte: this one (`fmt.Fprint`) writes
//! none, `/loki/api/v1/push`'s (`fmt.Fprintln`, issue #374) writes an LF.
//! `tests/support/manifest.rs`'s `PlainTextWriter` splits on that one
//! property and asserts the shared headers for both.
//!
//! The writers themselves are separate: `pulsus-write`'s ingest module has
//! nine response builders and none of them is reachable from `logs_api` —
//! the crates do not call into each other's responders at all. Two of
//! those nine ARE shared across references, though (`rw_error_response`
//! and `rw_backpressure_response` serve both `/api/v1/write` and
//! `/api/v2/spans`), which is a live divergence recorded on
//! `rw_error_response` — not a claim of separation anywhere but here.

use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

use pulsus_clickhouse::ChError;
use pulsus_logql::LogQlError;
use pulsus_read::ReadError;

use super::params::ParamError;

/// Every failure mode a `/api/logs/v1` handler can return, converted to the
/// bare plain-text error body by [`IntoResponse`].
#[derive(Debug)]
pub(crate) enum ApiError {
    Param(ParamError),
    LogQl(LogQlError),
    Read(ReadError),
    /// The ClickHouse pool has not been established yet (mirrors `/ready`'s
    /// 503 — `ops::ready`) — not one of the architect plan's error-table
    /// rows (that table covers parse/plan/execute failures against an
    /// already-live pool), but the same "not yet serving" contract every
    /// other data-plane route needs before a pool exists.
    PoolUnavailable,
    /// Issue #74: every live-tail connection slot
    /// (`reader.tail_max_connections`) is taken — rejected `429` BEFORE
    /// the WebSocket upgrade (docs/api.md §2.4).
    TailBusy,
}

impl From<ParamError> for ApiError {
    fn from(e: ParamError) -> Self {
        ApiError::Param(e)
    }
}

impl From<LogQlError> for ApiError {
    fn from(e: LogQlError) -> Self {
        ApiError::LogQl(e)
    }
}

impl From<ReadError> for ApiError {
    fn from(e: ReadError) -> Self {
        ApiError::Read(e)
    }
}

/// Writes the reference's error response: the message verbatim, no
/// trailing newline, `text/plain; charset=utf-8` + `nosniff`
/// (`pkg/util/server/error.go:46-52 @ v3.7.4` — quoted in this module's
/// header). The `Content-Type` is set explicitly rather than left to
/// `String`'s own `IntoResponse` so the two headers are pinned in one
/// place and read as a unit against the reference.
pub(crate) fn plain_text_error(status: StatusCode, message: String) -> Response {
    (
        status,
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        message,
    )
        .into_response()
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            ApiError::Param(e) => (StatusCode::BAD_REQUEST, e.to_string()),
            ApiError::LogQl(e) => (StatusCode::BAD_REQUEST, e.to_string()),
            ApiError::Read(e) => read_error_parts(e),
            ApiError::PoolUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "clickhouse pool not yet established".to_string(),
            ),
            ApiError::TailBusy => (
                StatusCode::TOO_MANY_REQUESTS,
                "tail connection limit reached (reader.tail_max_connections)".to_string(),
            ),
        };
        plain_text_error(status, message)
    }
}

/// The `ReadError` branch of the architect plan's status-code table.
/// Issue #264 dropped the `errorType` column with the JSON envelope — the
/// status code is the whole machine-readable classification now, exactly
/// as in the reference, whose `ClientHTTPStatusAndError`
/// (`pkg/util/server/error.go:54-131 @ v3.7.4`) also returns only
/// `(status, error)`.
///
/// | source | HTTP |
/// |---|---|
/// | `Parse`/`EmptyMatcherSet`/`ContradictoryMatchers`/`InvalidStep` | 400 |
/// | `QueryTooBroad` | 422 |
/// | `Clickhouse(Timeout)` | 504 |
/// | `Clickhouse(_)` | 500 |
fn read_error_parts(e: &ReadError) -> (StatusCode, String) {
    match e {
        ReadError::Parse(_) => (StatusCode::BAD_REQUEST, e.to_string()),
        // Issue M6-09 plan v3 delta 6: pipeline-validation failures (bad
        // regex / invalid template (#230) / bad parser expression /
        // `unwrap` outside a range aggregation) and the M6-10
        // metric-pipeline deferral are both client-caused query-shape
        // rejections — 400, alongside the other planner validation
        // variants.
        // `MetricPipelineError` (issue M6-10 adjudication #1: a metric
        // line retained a nonempty `__error__` after the full pipeline)
        // is likewise 400 — the status the oracle returns for the same
        // failure, live-probed.
        ReadError::EmptyMatcherSet
        | ReadError::ContradictoryMatchers
        | ReadError::InvalidStep
        // Issue #227: an out-of-domain `[range]`/`step` duration is a client
        // input error, same class as `InvalidStep`.
        | ReadError::DurationOutOfRange { .. }
        // Issue #343: the 5-year query-span cap, the same client-input class.
        | ReadError::QuerySpanTooLong { .. }
        | ReadError::PipelineInvalid { .. }
        | ReadError::MetricPipelineError { .. }
        | ReadError::PipelineUnsupportedInMetric { .. } => {
            (StatusCode::BAD_REQUEST, e.to_string())
        }
        // Issue #85: `NamelessSelectorUnresolvable` is a PromQL-only
        // variant (the LogQL engine never produces it), matched here for
        // exhaustiveness like `ReadError::Promql` below — mapped as a
        // too-broad-class client rejection, mirroring
        // `prom_api::error::read_error_parts`'s own 422 for it.
        // M7-A5a: `HistogramResultUnsupported` is a metrics-only variant
        // (the LogQL engine never produces it), matched here for
        // exhaustiveness — mapped like the other 422 declines, mirroring
        // `prom_api::error::read_error_parts`.
        ReadError::QueryTooBroad(_)
        | ReadError::NamelessSelectorUnresolvable { .. }
        | ReadError::HistogramResultUnsupported => {
            (StatusCode::UNPROCESSABLE_ENTITY, e.to_string())
        }
        // M7-A5a: a histogram-decode failure is a metrics-only data-
        // integrity defect (unreachable from LogQL), 500.
        ReadError::HistogramDecode(_) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        ReadError::Clickhouse(ch) => match ch {
            ChError::Timeout(_) => (StatusCode::GATEWAY_TIMEOUT, e.to_string()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        },
        // Issue #31: every `pulsus_promql::PromqlError` variant (parse,
        // unsupported construct, bad vector matching, histogram_quantile
        // bucket error) is a client-caused failure — mapped uniformly to
        // 400 (unlike `LogQlError`'s span-tracked
        // parser, `PromqlError::Parse` carries only the vendored parser's
        // message text). `pulsus-server` does not yet wire `MetricsEngine`
        // into a route (that is #32), so this arm is unreachable from any
        // request today; #32 may split it into a finer-grained mapping
        // if the PromQL API surface needs one — this keeps `ReadError`
        // matches exhaustive and correct in the meantime.
        ReadError::Promql(_) => (StatusCode::BAD_REQUEST, e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Renders an `ApiError` and returns `(status, body)`, asserting the
    /// reference's two headers on the way through
    /// (`pkg/util/server/error.go:48-49 @ v3.7.4`) so EVERY case below
    /// covers the container, not just the one dedicated header test.
    async fn rendered(err: ApiError) -> (StatusCode, String) {
        let res = err.into_response();
        let status = res.status();
        assert_eq!(
            res.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("text/plain; charset=utf-8"),
        );
        assert_eq!(
            res.headers()
                .get(header::X_CONTENT_TYPE_OPTIONS)
                .and_then(|v| v.to_str().ok()),
            Some("nosniff"),
        );
        // `plain_text_error` sets `Content-Type` explicitly on top of a
        // `String` body, which sets it too. `HeaderMap::get` would return
        // the first of a duplicated pair and hide the second, and so
        // would the conformance suite's header HashMap — count instead.
        assert_eq!(
            res.headers().get_all(header::CONTENT_TYPE).iter().count(),
            1,
            "exactly one Content-Type, never a duplicated pair",
        );
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .expect("read body");
        (
            status,
            String::from_utf8(body.to_vec()).expect("utf-8 body"),
        )
    }

    /// Issue #264: the exact wire container. The body is the message and
    /// nothing else — no JSON, no trailing newline (`fmt.Fprint` in
    /// `pkg/util/server/error.go:51 @ v3.7.4` writes no terminator, and
    /// none of the 18 `grafana/loki:3.7.4` `400`s in this module's header
    /// probe ended in `0x0a`). Asserted on the raw bytes, not on a
    /// trimmed string, so a stray newline fails.
    #[tokio::test]
    async fn an_error_is_the_bare_message_with_no_json_and_no_trailing_newline() {
        let res = ApiError::Read(ReadError::PipelineInvalid {
            reason: "bad regex: unclosed group".to_string(),
        })
        .into_response();
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .expect("read body");
        assert_eq!(body.as_ref(), b"bad regex: unclosed group");
        assert!(!body.ends_with(b"\n"), "body must not end with a newline");
        assert!(
            serde_json::from_slice::<serde_json::Value>(&body).is_err(),
            "the body must not be JSON at all"
        );
    }

    #[tokio::test]
    async fn param_error_maps_to_400() {
        let (status, body) = rendered(ApiError::Param(ParamError::MissingQuery)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, ParamError::MissingQuery.to_string());
    }

    /// Issue #264: a LogQL parse error carried a `position` byte offset in
    /// the old JSON envelope. The reference has no such field — its body
    /// is `err.Error()` and nothing more — so the span survives only
    /// inside the message text where the reference itself puts it.
    #[tokio::test]
    async fn logql_error_maps_to_400_with_no_position_field() {
        let err = LogQlError::EmptySelector {
            span: pulsus_logql::Span { start: 4, end: 6 },
        };
        let expected = err.to_string();
        let (status, body) = rendered(ApiError::LogQl(err)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, expected);
        assert!(!body.contains("position"), "{body}");
    }

    #[tokio::test]
    async fn read_error_empty_matcher_set_maps_to_400() {
        let (status, body) = rendered(ApiError::Read(ReadError::EmptyMatcherSet)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, ReadError::EmptyMatcherSet.to_string());
    }

    #[tokio::test]
    async fn read_error_invalid_step_maps_to_400() {
        let (status, body) = rendered(ApiError::Read(ReadError::InvalidStep)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, ReadError::InvalidStep.to_string());
    }

    /// Issue M6-09 plan v3 delta 6: both pipeline rejection classes map
    /// to 400 (matching the `read_error_empty_matcher_set_maps_to_400`
    /// idiom).
    /// Issue #240: the body is the BARE reason, asserted byte-exactly on
    /// the WHOLE body — the old `contains(…)` assert pinned the
    /// removed PulsusDB-only prefix and is deleted, not weakened.
    #[tokio::test]
    async fn read_error_pipeline_invalid_maps_to_400() {
        let err = ReadError::PipelineInvalid {
            reason: "bad regex: unclosed group".to_string(),
        };
        let (status, body) = rendered(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, "bad regex: unclosed group");
    }

    /// Issue #240 (AC2's once-only leg): a reason that itself begins
    /// `parse error ` renders exactly once — no decoration doubles it.
    #[tokio::test]
    async fn read_error_pipeline_invalid_body_is_the_reason_exactly_once() {
        let reason = "parse error : synthetic prefix-collision probe";
        let err = ReadError::PipelineInvalid {
            reason: reason.to_string(),
        };
        let (status, body) = rendered(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, reason);
        assert_eq!(body.matches("parse error ").count(), 1);
    }

    /// Issue M6-10 adjudication #1: a surviving metric-pipeline
    /// `__error__` maps to 400 (the oracle's status for the same
    /// failure, live-probed) with the oracle's message shape intact.
    #[tokio::test]
    async fn read_error_metric_pipeline_error_maps_to_400() {
        let err = ReadError::MetricPipelineError {
            error_type: "SampleExtractionErr".to_string(),
            series: r#"{__error__="SampleExtractionErr", app="x"}"#.to_string(),
        };
        let (status, body) = rendered(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            body.starts_with("pipeline error: 'SampleExtractionErr' for series:"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn read_error_pipeline_unsupported_in_metric_maps_to_400() {
        let err = ReadError::PipelineUnsupportedInMetric {
            construct: "json".to_string(),
        };
        let (status, body) = rendered(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("json"), "{body}");
    }

    #[tokio::test]
    async fn read_error_query_too_broad_maps_to_422() {
        let err = ReadError::QueryTooBroad(pulsus_read::logql::TooBroadReason::StreamCap {
            count: 1,
            cap: 1,
        });
        let expected = err.to_string();
        let (status, body) = rendered(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body, expected);
    }

    /// Issue #35: the query-text guard's reason rides the existing
    /// `QueryTooBroad(_)` wildcard arm — no mapper change was needed, and
    /// this test proves it.
    #[tokio::test]
    async fn read_error_query_text_bytes_maps_to_422() {
        let err = ReadError::QueryTooBroad(pulsus_read::logql::TooBroadReason::QueryTextBytes {
            rendered_bytes: 9_000_000,
            cap: 8_388_608,
        });
        let (status, _) = rendered(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    }

    /// Issue #272: the walk-admission reason rides the same
    /// `QueryTooBroad(_)` wildcard — no mapper arm changes, and this
    /// proves it.
    #[tokio::test]
    async fn read_error_walk_transient_bytes_maps_to_422() {
        let err =
            ReadError::QueryTooBroad(pulsus_read::logql::TooBroadReason::WalkTransientBytes {
                estimate: 900_000_000,
                cap: 80_000_000,
            });
        let (status, _) = rendered(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn read_error_clickhouse_timeout_maps_to_504() {
        let err = ReadError::Clickhouse(ChError::Timeout("deadline".to_string()));
        let (status, _) = rendered(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    }

    #[tokio::test]
    async fn read_error_clickhouse_other_maps_to_500() {
        let err = ReadError::Clickhouse(ChError::Decode("bad row".to_string()));
        let (status, _) = rendered(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn pool_unavailable_maps_to_503() {
        let (status, body) = rendered(ApiError::PoolUnavailable).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, "clickhouse pool not yet established");
    }

    /// Issue #74: tail slot exhaustion is `429`, naming the knob, in the
    /// plain-text body (#264).
    #[tokio::test]
    async fn tail_busy_maps_to_429() {
        let (status, body) = rendered(ApiError::TailBusy).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert!(body.contains("tail_max_connections"), "{body}");
    }
}
