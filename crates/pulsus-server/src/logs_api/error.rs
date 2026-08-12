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
//! the handlers — axum's own 404/405, and the server-wide `TimeoutLayer`'s
//! 408 — are not written by this module. They diverge from the reference,
//! they pre-date #264, and it neither changed nor covers them
//! (docs/api.md §2.3).
//!
//! The container is pinned by test, not by this comment:
//! `tests::assert_reference_container` asserts both headers and rejects a
//! duplicated `Content-Type`, and the live wire leg is `api_conformance`'s
//! `PlainTextWriter::LogqlWriteError` arm.
//!
//! The container does not vary by status code, which is what lets the
//! `422`/`500`/`504` rows of the table below rest on the same rule:
//! `WriteError` takes the status as a *value* and then writes the same two
//! headers and the same `fmt.Fprint` body regardless of it. That is a
//! property of the source, and a stronger one than probing statuses
//! individually would give — the reference classifies client-caused
//! failures as `400` (`ClientHTTPStatusAndError`), so its `5xx` paths are
//! not reachable by sending a bad request at all.
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
//! - `traces_api` also writes plain text since issue #384, but NOT this
//!   container, and the difference is one header. Tempo's user-facing
//!   `/api/*` query routes are served by its query FRONTEND
//!   (`cmd/tempo/app/modules.go:500-512 @ tempo v3.0.2`), whose 4xx
//!   rejections are `*http.Response` values with a nil `Header` map,
//!   copied out verbatim by `modules/frontend/handler.go:113-116` — so no
//!   `X-Content-Type-Options` and no terminator. (The `http.Error` sites
//!   in `modules/querier/http.go` DO set `nosniff` and DO append an LF,
//!   but they are registered only under `/querier/…`,
//!   `modules.go:438-459` + `pkg/api/http.go:67` — an internal path.) So
//!   `traces_api::error::plain_text_error` is a SEPARATE function that
//!   omits `nosniff`; do not merge the two.
//!
//! Note also that plain text alone is not the whole contract, and the
//! three writers do not split on one axis. Loki's two — this one and
//! `/loki/api/v1/push`'s (issue #374) — agree on BOTH headers (Go's
//! `http.Error` sets `Content-Type` and `nosniff` exactly as `WriteError`
//! does) and disagree only on the trailing byte: `fmt.Fprint` writes
//! none, `fmt.Fprintln` writes an LF. Tempo's frontend agrees with THIS
//! one on the terminator (neither writes one) and differs on `nosniff`.
//! `tests/support/manifest.rs`'s `PlainTextWriter` therefore carries two
//! independent per-writer rules, and its `NosniffRule` is an enum rather
//! than a `bool` so that "absent by rule" cannot be spelled the same way
//! as "no rule at all" (issue #385 retired the latter value; the reason
//! for the enum did not go with it).
//!
//! The writers themselves are separate: every response builder lives in
//! `pulsus-write`'s ingest module or here, and none of the ingest ones is
//! reachable from `logs_api` — the crates do not call into each other's
//! responders at all. Since issue #385 no ingest builder is shared across
//! references either: `/api/v1/write` and `/api/v2/spans` used to share
//! one, which was a live divergence, and each now has its own writer per
//! STAGE (both references `http.Error` their pre-admission rejections and
//! part company after admission).

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
    /// Issue #376, code review finding 5: the path that fell BETWEEN the
    /// hermetic 427 fixture check and the live `SELECT version()` check.
    /// `ReadError::Clickhouse(ChError::Server { .. })` is rendered here
    /// with `e.to_string()`, and before the fix that put the connected
    /// server's exact version in the body a tenant receives. The redaction
    /// lives on `ChError`'s `Display` (one choke point for all three
    /// surfaces); this asserts it from the surface a client actually talks
    /// to, over a REAL 26.3 body.
    #[tokio::test]
    async fn a_raw_clickhouse_server_error_never_puts_the_server_version_in_the_body() {
        let raw = "Code: 241. DB::Exception: Query memory limit exceeded: would use 194.36 \
                   MiB. (MEMORY_LIMIT_EXCEEDED) (version 26.3.17.110 (official build))";
        let err = ReadError::Clickhouse(pulsus_clickhouse::ChError::Server {
            code: 241,
            message: raw.to_string(),
        });
        let (status, body) = rendered(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            !carries_a_server_version_banner(&body),
            "the connected server's version leaked into a client body: {body}"
        );
        // The premise: the fixture really does carry one, so this cannot
        // pass by checking an empty body.
        assert!(carries_a_server_version_banner(raw), "premise");
    }

    /// `true` when `body` carries anything SHAPED like a ClickHouse server
    /// version banner — the literal `version ` followed by `<digits>.<digits>`.
    ///
    /// Issue #376: the assertions below used to name `"version 24.8"`. That
    /// made a claim about *any* server version leaking, checked against
    /// *one* server's spelling — on 26.3 the fragment never appears at all,
    /// so the check would have passed while testing nothing. Matching the
    /// shape instead means the next version bump cannot silently retire it.
    fn carries_a_server_version_banner(body: &str) -> bool {
        let mut rest = body;
        while let Some(i) = rest.find("version ") {
            let tail = &rest[i + "version ".len()..];
            let digits: String = tail.chars().take_while(|c| c.is_ascii_digit()).collect();
            let after = &tail[digits.len()..];
            if !digits.is_empty()
                && after.starts_with('.')
                && after[1..].starts_with(|c: char| c.is_ascii_digit())
            {
                return true;
            }
            rest = &rest[i + "version ".len()..];
        }
        false
    }

    use super::*;

    /// Asserts the reference's error container on a rendered response:
    /// both headers (`pkg/util/server/error.go:48-49 @ v3.7.4`), and that
    /// `Content-Type` appears exactly once.
    ///
    /// The count matters because `plain_text_error` sets `Content-Type`
    /// explicitly on top of a `String` body, which sets it too.
    /// `HeaderMap::get` returns the first of a duplicated pair and hides
    /// the second, and so would the conformance suite's header `HashMap`.
    fn assert_reference_container(res: &Response) {
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
        assert_eq!(
            res.headers().get_all(header::CONTENT_TYPE).iter().count(),
            1,
            "exactly one Content-Type, never a duplicated pair",
        );
    }

    /// Renders an `ApiError` and returns `(status, body)`, checking the
    /// container via [`assert_reference_container`] on the way through.
    async fn rendered(err: ApiError) -> (StatusCode, String) {
        let res = err.into_response();
        let status = res.status();
        assert_reference_container(&res);
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
    /// `pkg/util/server/error.go:51 @ v3.7.4` writes no terminator).
    /// Asserted on the raw bytes, not on a trimmed string, so a stray
    /// newline fails.
    ///
    /// Cannot use [`rendered`], which decodes the body to a `String`; it
    /// calls [`assert_reference_container`] directly instead, so this test
    /// covers the headers too rather than being the one hole in them.
    #[tokio::test]
    async fn an_error_is_the_bare_message_with_no_json_and_no_trailing_newline() {
        let res = ApiError::Read(ReadError::PipelineInvalid {
            reason: "bad regex: unclosed group".to_string(),
        })
        .into_response();
        assert_reference_container(&res);
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

    /// Issue #398 AC L4: a LogQL read that exhausted ClickHouse's memory
    /// answers **422**, not 500 — the same status the too-broad family
    /// already answers, in the reference error container, naming the knob
    /// an operator would raise.
    ///
    /// 422 rather than the reference's 400 is the PRE-EXISTING,
    /// family-wide divergence already recorded under `#236` in
    /// `docs/benchmarks/logs-differential-ledger.md`; this reason joins
    /// that family and opens no new one. (Prometheus v3.13.0 and Tempo
    /// v3.0.2 were both measured at 422 for their own equivalents, so only
    /// Loki differs.)
    #[tokio::test]
    async fn logql_read_memory_maps_to_422() {
        let err = ReadError::QueryTooBroad(pulsus_read::logql::TooBroadReason::LogqlReadMemory {
            budget_bytes: 8_589_934_592,
        });
        let expected = err.to_string();
        let (status, body) = rendered(ApiError::Read(err)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body, expected);
        assert!(
            body.contains("reader.logql_read_max_memory_bytes"),
            "the body must name the knob an operator would raise: {body}"
        );
        assert!(
            body.contains("8589934592"),
            "the body must carry the configured ceiling: {body}"
        );
        // The pre-#398 failure mode: the raw server exception in the body.
        // `body == expected` above already pins the whole rendering; these
        // two say what a regression would look like, and the version half
        // is matched as a SHAPE so it cannot go vacuous (issue #376).
        assert!(
            !body.contains("DB::Exception"),
            "the 422 body must carry only our own message: {body}"
        );
        assert!(
            !carries_a_server_version_banner(&body),
            "a server version banner leaked into the 422 body: {body}"
        );
    }

    /// Issue #398: the sibling surfaces' reasons are variants of the SAME
    /// shared `ReadError`, so this mapper must classify them too rather
    /// than falling into an unreachable-today arm that silently becomes
    /// reachable later (the `NamelessSelectorUnresolvable` precedent
    /// above).
    #[tokio::test]
    async fn the_sibling_surfaces_read_memory_reasons_also_map_to_422() {
        for reason in [
            pulsus_read::logql::TooBroadReason::PromqlReadMemory { budget_bytes: 4096 },
            pulsus_read::logql::TooBroadReason::TraceReadMemory { budget_bytes: 4096 },
        ] {
            let (status, _) = rendered(ApiError::Read(ReadError::QueryTooBroad(reason))).await;
            assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{reason:?}");
        }
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
