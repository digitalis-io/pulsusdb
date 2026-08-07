//! `/api/logs/v1` — the five M1 core LogQL query endpoints (issue #13,
//! docs/api.md §2). Thin handlers (`handlers.rs`): parse params
//! (`params.rs`) → parse LogQL (`pulsus-logql`) → dispatch to `LogQlEngine`
//! (`pulsus-read`) → encode the envelope (`encode.rs`, `error.rs`). All
//! planning/SQL/execution stays in `pulsus-read` — this module only ever
//! talks to it through `LogQlEngine`'s public methods.
//!
//! The `/loki/api/v1/*` M1 query aliases (docs/api.md §8.1) ship **here**:
//! [`mount_log_query_routes`] is the single source of truth for the five
//! routes' method matrix, shared by [`router`] (native) and
//! [`compat_router`] (alias) so the two surfaces cannot drift apart.
//! `compat.rs` only decides *whether* [`compat_router`] gets merged in
//! (flag + mode gating) — it never duplicates the route list itself.
//!
//! Issue #74 (M6-11) adds `/tail` (WebSocket, §2.4) and `/stats` (§2.5)
//! plus their `/loki/api/v1/{tail,index/stats}` aliases. Issue #169 (M7)
//! adds the first drilldown endpoint, `/volume` (§2.6), with its
//! `/loki/api/v1/index/volume` alias. Neither alias suffix is a prefix
//! swap of its native path (`/index/stats` vs `/stats`, `/index/volume`
//! vs `/volume`), so those routes mount explicitly below rather than
//! through [`mount_log_query_routes`]. Issue #170 (M7) adds
//! `/detected_labels` + `/detected_fields` (§2.6) — both aliases ARE pure
//! prefix swaps, mounted via [`mount_detected_routes`] on both surfaces.
//! Issue #171 (M7-C3) adds `/patterns` (§2.6) with its `/loki/api/v1/patterns`
//! alias (also a pure prefix swap, mounted explicitly on both surfaces).

mod detected;
mod encode;
mod error;
mod handlers;
mod params;
mod patterns;
mod stats;
mod tail;
mod volume;

use axum::Router;
use axum::routing::get;

use crate::app::AppState;

/// Mounts the five log-query routes under `prefix` (no trailing slash),
/// e.g. `/api/logs/v1` (native) or `/loki/api/v1` (compat alias, issue #14).
/// Full method matrix, pinned identically for both surfaces: `GET|POST` on
/// `/query_range` and `/query` (issue #13 architect plan amendment 3 §2,
/// ratified by task-manager, reversing amendment 1's M1 GET-only deferral
/// for those two) and `GET|POST` on `/labels` and `/series` (pinned
/// `GET|POST` from amendment 1 onward, per api.md §2.3); `label/{name}/values`
/// is `GET`-only throughout. Any other method on a mounted path is a 405;
/// any method on an unmounted path (alias off, or writer-only mode) is a 404.
fn mount_log_query_routes(router: Router<AppState>, prefix: &str) -> Router<AppState> {
    router
        .route(
            &format!("{prefix}/query_range"),
            get(handlers::query_range).post(handlers::query_range_post),
        )
        .route(
            &format!("{prefix}/query"),
            get(handlers::query).post(handlers::query_post),
        )
        .route(
            &format!("{prefix}/labels"),
            get(handlers::labels_get).post(handlers::labels_post),
        )
        .route(
            &format!("{prefix}/label/{{name}}/values"),
            get(handlers::label_values),
        )
        .route(
            &format!("{prefix}/series"),
            get(handlers::series_get).post(handlers::series_post),
        )
}

/// Mounts the two detected-labels/fields drilldown routes under `prefix`
/// (issue #170, docs/api.md §2.6): `GET|POST` form-encoded on both (the
/// `/labels`/`/series` precedent). Unlike `/index/stats`/`/index/volume`,
/// both `/loki/api/v1` aliases ARE pure prefix swaps, so one helper
/// serves both surfaces — the same cannot-drift-apart rationale as
/// [`mount_log_query_routes`].
fn mount_detected_routes(router: Router<AppState>, prefix: &str) -> Router<AppState> {
    router
        .route(
            &format!("{prefix}/detected_labels"),
            get(detected::detected_labels).post(detected::detected_labels_post),
        )
        .route(
            &format!("{prefix}/detected_fields"),
            get(detected::detected_fields).post(detected::detected_fields_post),
        )
}

/// The native `/api/logs/v1` surface (docs/api.md §2.1-2.6): the five
/// query routes via [`mount_log_query_routes`], the two detected
/// drilldown routes via [`mount_detected_routes`] (issue #170), plus
/// `/tail` (WebSocket, issue #74), `/stats`, and `/volume` (issue #169)
/// mounted explicitly (all `GET`-only).
/// Parses a LogQL query and admits its iterative walks (issue #272).
///
/// **ORDER IS LOAD-BEARING.** #279's 131,072-byte query-text cap lives
/// INSIDE `parse` (`pulsus-logql`'s `limits.rs`, reached from
/// `parser.rs`) and answers 400 with the reference's verbatim
/// message. Admission therefore runs only on input `parse` has ACCEPTED,
/// so its 422 can never pre-empt that 400.
///
/// Every accepted query is `< MAX_QUERY_BYTES` and the walk threshold is
/// above `2 x` that, so the refusal below is **execution-dead** through
/// this function (AC 6 clauses 3b/3c prove it: an exhaustive
/// monotonicity sweep plus a `const` endpoint relation). It is kept, not
/// deleted, because it is the runtime half of the L1-L5 memory argument
/// and because a future caller that does not go through `parse` must
/// still be refused rather than abort.
pub(crate) fn parse_logql(query: &str) -> Result<pulsus_logql::Expr, error::ApiError> {
    let expr = pulsus_logql::parse(query)?;
    pulsus_read::logql::admit_logql_walk(query)
        .map_err(pulsus_read::logql::ReadError::QueryTooBroad)?;
    Ok(expr)
}

pub(crate) fn router() -> Router<AppState> {
    let router = mount_log_query_routes(Router::new(), "/api/logs/v1")
        .route("/api/logs/v1/tail", get(tail::tail))
        .route("/api/logs/v1/stats", get(stats::stats))
        .route("/api/logs/v1/volume", get(volume::volume))
        .route("/api/logs/v1/patterns", get(patterns::patterns));
    mount_detected_routes(router, "/api/logs/v1")
}

/// The `/loki/api/v1/*` compat alias surface (docs/api.md §8.1, issue #14).
/// Same handler fns, same method matrix as [`router`] — responses are
/// byte-identical to native, including `X-Pulsus-Explain` passthrough,
/// because the two surfaces are pure route bindings onto the same handlers.
/// Mounting this router at all is `compat.rs`'s job (flag + Reader-mode
/// gated); this fn just builds the route set.
pub(crate) fn compat_router() -> Router<AppState> {
    let router = mount_log_query_routes(Router::new(), "/loki/api/v1")
        // Issue #74: the M6 aliases. `/index/stats` is deliberately NOT
        // derived from the native `/stats` path — the alias suffix is not
        // a prefix swap (docs/api.md §8.1's M6 row). Issue #169: the M7
        // `/index/volume` alias follows the same irregular-suffix rule.
        .route("/loki/api/v1/tail", get(tail::tail))
        .route("/loki/api/v1/index/stats", get(stats::stats))
        .route("/loki/api/v1/index/volume", get(volume::volume))
        // Issue #171: `/loki/api/v1/patterns` IS a pure prefix swap of the
        // native `/patterns` (docs/api.md §8.1's M7 row), unlike the irregular
        // `/index/stats`/`/index/volume` aliases.
        .route("/loki/api/v1/patterns", get(patterns::patterns));
    mount_detected_routes(router, "/loki/api/v1")
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use pulsus_config::Config;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use tower::ServiceExt;

    use crate::app::BuildInfo;
    use crate::ingest::{MetricWriterSink, TraceWriterSink, WriterSink};

    #[test]
    fn router_constructs_without_panicking() {
        let _ = router();
    }

    #[test]
    fn compat_router_constructs_without_panicking() {
        let _ = compat_router();
    }

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

    /// One routed request against a POOLLESS state; returns status + body
    /// bytes so alias and native responses can be compared byte-for-byte.
    async fn routed(router: Router<AppState>, req: Request<Body>) -> (StatusCode, Vec<u8>) {
        let res = router
            .with_state(test_state())
            .oneshot(req)
            .await
            .expect("router does not fail the request");
        let status = res.status();
        let bytes = to_bytes(res.into_body(), usize::MAX).await.expect("body");
        (status, bytes.to_vec())
    }

    fn get_req(uri: &str) -> Request<Body> {
        Request::builder()
            .uri(uri)
            .body(Body::empty())
            .expect("build request")
    }

    fn form_post(uri: &str, form: String) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from(form))
            .expect("build request")
    }

    /// Issue #279 (AC5): the `/loki/api/v1` alias surface rejects an
    /// over-cap query (exactly 131,072 bytes — `MAX_QUERY_BYTES`, the
    /// reference's `maxInputSize`) identically to the native paths, for
    /// both alias shapes.
    ///
    /// The pure-prefix-swap shape (`query_range`) carries the genuine
    /// over-cap query via POST form body through BOTH routed surfaces.
    /// A GET cannot carry it at all: `http::Uri` caps a URI at 65,534
    /// bytes (`InvalidUri(TooLong)`), under half the 131,072-byte LogQL
    /// cap — so for the GET-only irregular-suffix shape (`/index/stats`
    /// vs `/stats`) the routed probe pins alias/native byte-identity of
    /// the parse-rejection surface (same handler fn, same parse seam),
    /// and the over-cap rejection of that same handler is pinned directly
    /// in `stats.rs`'s
    /// `stats_rejects_an_over_cap_query_400_before_the_pool_check`.
    ///
    /// That GET ceiling is a divergence at a public surface (the
    /// reference serves such GETs) and is recorded as
    /// `get-request-target-uri-bound` in
    /// docs/benchmarks/logs-differential-ledger.md — over a socket it
    /// surfaces as hyper's `414 URI Too Long`, before routing.
    /// Issue #272 AC 6 — the drift guard. Every request path must reach
    /// the parser through [`parse_logql`], so the walk admission cannot
    /// be bypassed by a new handler that calls the parser directly. The
    /// two allowlisted shapes are `parse_selector` (a selector is not a
    /// full query) and the `#[cfg(test)]` probe in `handlers.rs`.
    #[test]
    fn no_request_path_calls_the_logql_parser_directly() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/logs_api");
        let mut offenders: Vec<String> = Vec::new();
        for entry in std::fs::read_dir(&dir).expect("the logs_api source directory") {
            let path = entry.expect("dir entry").path();
            if path.extension().is_none_or(|x| x != "rs") {
                continue;
            }
            let name = path
                .file_name()
                .expect("file name")
                .to_string_lossy()
                .into_owned();
            let src = std::fs::read_to_string(&path).expect("source");
            // `mod.rs` declares the seam itself; the test region of
            // `handlers.rs` carries an allowlisted probe.
            let production = match src.find("\n#[cfg(test)]\nmod tests {") {
                Some(i) => &src[..i],
                None => &src[..],
            };
            for (i, line) in production.lines().enumerate() {
                if line.contains("pulsus_logql::parse(") && name != "mod.rs" {
                    offenders.push(format!("{name}:{}", i + 1));
                }
            }
        }
        assert!(
            offenders.is_empty(),
            "these request paths bypass `parse_logql` (issue #272 memory L5 Leg A): {offenders:?}"
        );
    }

    /// Issue #272 AC 6 clause 3d — the ordering at the wire.
    ///
    /// `parse_logql` parses BEFORE it admits, so #279's 400
    /// wins and our 422 can never pre-empt the reference's answer. The
    /// guard is the MODEL relation, independent of `admit_logql_walk`
    /// (clause 3a owns the implementation half), so the two clauses catch
    /// different failures.
    ///
    /// POST, because the GET target-URI ceiling is an already-ledgered
    /// divergence (`get-request-target-uri-bound`, see the module test
    /// above).
    #[tokio::test]
    async fn an_over_cap_query_answers_the_references_400_not_our_422() {
        let query = "a".repeat(4 * pulsus_logql::MAX_QUERY_BYTES);
        assert!(
            pulsus_read::logql::walk_transient_bound(query.len() as u64)
                > pulsus_read::logql::MAX_LOGQL_WALK_TRANSIENT_BYTES,
            "probe must sit above the walk threshold, or this test cannot observe the ordering"
        );
        let (status, body) = routed(
            router(),
            form_post("/api/logs/v1/query_range", format!("query={query}")),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        // Issue #264: the bare `text/plain` body, byte-exact.
        assert_eq!(body, b"input size too long (524288 > 131072)");
    }

    #[tokio::test]
    async fn alias_surfaces_reject_an_over_cap_query_identically_to_native() {
        // Pure prefix swap, genuine over-cap payload via POST.
        let form = format!(r#"query={{app="{}"}}"#, "a".repeat(131_072 - 8));
        let (native_status, native_body) = routed(
            router(),
            form_post("/api/logs/v1/query_range", form.clone()),
        )
        .await;
        let (alias_status, alias_body) = routed(
            compat_router(),
            form_post("/loki/api/v1/query_range", form.clone()),
        )
        .await;
        assert_eq!(native_status, StatusCode::BAD_REQUEST);
        assert_eq!(alias_status, native_status);
        assert_eq!(
            alias_body, native_body,
            "alias and native over-cap rejection bodies must be byte-identical"
        );
        assert_eq!(alias_body, b"input size too long (131072 > 131072)");

        // Irregular suffix (`/index/stats`), GET-only: the routed halves
        // prove both bindings reach the same parse seam (byte-identical
        // parse rejection); the over-cap behaviour of that handler is
        // pinned in stats.rs (see doc comment).
        let (native_status, native_body) =
            routed(router(), get_req("/api/logs/v1/stats?query=%7B")).await;
        let (alias_status, alias_body) = routed(
            compat_router(),
            get_req("/loki/api/v1/index/stats?query=%7B"),
        )
        .await;
        assert_eq!(native_status, StatusCode::BAD_REQUEST);
        assert_eq!(alias_status, native_status);
        assert_eq!(
            alias_body, native_body,
            "alias and native parse-rejection bodies must be byte-identical"
        );
    }
}
