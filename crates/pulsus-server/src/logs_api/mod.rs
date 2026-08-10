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
/// `GET|POST` from amendment 1 onward, per api.md §2.3). `label/{name}/values`
/// joined them in issue #406 Part B2 — the reference registers it
/// `Methods("GET","POST")` (`pkg/loki/modules.go:687`, `:1365` @ v3.7.4
/// `b318f282`) and answered a form POST `200` where we answered `405`.
/// Any other method on a mounted path is a 405; any method on an unmounted
/// path (alias off, or writer-only mode) is a 404.
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
            get(handlers::label_values).post(handlers::label_values_post),
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

/// The native `/api/logs/v1` surface (docs/api.md §2.1-2.6): the five
/// query routes via [`mount_log_query_routes`], the two detected
/// drilldown routes via [`mount_detected_routes`] (issue #170), plus
/// `/tail` (WebSocket, issue #74), `/stats`, `/volume` (issue #169) and
/// `/patterns` (issue #171) mounted explicitly. Issue #406 Part B2 added
/// `POST` to the last three (the reference registers all of them
/// `Methods("GET","POST")`, `pkg/loki/modules.go:690-692` @ v3.7.4
/// `b318f282`); `/tail` stays `GET`-only, because its upstream `POST`
/// registration is nominal — a POST cannot carry a WebSocket handshake and
/// the reference answers `400`, measured 2026-08-10.
pub(crate) fn router() -> Router<AppState> {
    let router = mount_log_query_routes(Router::new(), "/api/logs/v1")
        .route("/api/logs/v1/tail", get(tail::tail))
        .route(
            "/api/logs/v1/stats",
            get(stats::stats).post(stats::stats_post),
        )
        .route(
            "/api/logs/v1/volume",
            get(volume::volume).post(volume::volume_post),
        )
        .route(
            "/api/logs/v1/patterns",
            get(patterns::patterns).post(patterns::patterns_post),
        );
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
        .route(
            "/loki/api/v1/index/stats",
            get(stats::stats).post(stats::stats_post),
        )
        .route(
            "/loki/api/v1/index/volume",
            get(volume::volume).post(volume::volume_post),
        )
        // Issue #171: `/loki/api/v1/patterns` IS a pure prefix swap of the
        // native `/patterns` (docs/api.md §8.1's M7 row), unlike the irregular
        // `/index/stats`/`/index/volume` aliases.
        .route(
            "/loki/api/v1/patterns",
            get(patterns::patterns).post(patterns::patterns_post),
        );
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

    // -- Issue #391: present-but-empty scalar params are absent ones -----

    /// One row of the empty-vs-absent identity table: a mounted route, a
    /// base query string that does NOT already carry `param` (first-wins
    /// would otherwise mask the empty value), and the scalar parameter
    /// under test.
    #[derive(Debug)]
    struct EmptyParamCase {
        path: &'static str,
        base: &'static str,
        param: &'static str,
    }

    /// Every scalar parameter every mounted `/api/logs/v1` handler reads
    /// through `params::get`, except `/tail`'s `delay_for`
    /// ([`TAIL_ONLY_PARAMS`]). Completeness is a checked claim, not a hand
    /// list — see `the_empty_param_table_covers_every_parameter_the_handlers_read`.
    ///
    /// Selectors are percent-encoded because these are query strings.
    const EMPTY_PARAM_CASES: &[EmptyParamCase] = &[
        // /query_range
        EmptyParamCase {
            path: "/api/logs/v1/query_range",
            base: SELECTOR_QUERY,
            param: "limit",
        },
        EmptyParamCase {
            path: "/api/logs/v1/query_range",
            base: SELECTOR_QUERY,
            param: "direction",
        },
        EmptyParamCase {
            path: "/api/logs/v1/query_range",
            base: SELECTOR_QUERY,
            param: "step",
        },
        EmptyParamCase {
            path: "/api/logs/v1/query_range",
            base: SELECTOR_QUERY,
            param: "start",
        },
        EmptyParamCase {
            path: "/api/logs/v1/query_range",
            base: SELECTOR_QUERY,
            param: "end",
        },
        EmptyParamCase {
            path: "/api/logs/v1/query_range",
            base: SELECTOR_QUERY,
            param: "since",
        },
        EmptyParamCase {
            path: "/api/logs/v1/query_range",
            base: "",
            param: "query",
        },
        // /query
        EmptyParamCase {
            path: "/api/logs/v1/query",
            base: SELECTOR_QUERY,
            param: "limit",
        },
        EmptyParamCase {
            path: "/api/logs/v1/query",
            base: SELECTOR_QUERY,
            param: "direction",
        },
        EmptyParamCase {
            path: "/api/logs/v1/query",
            base: SELECTOR_QUERY,
            param: "time",
        },
        EmptyParamCase {
            path: "/api/logs/v1/query",
            base: "",
            param: "query",
        },
        // /labels
        EmptyParamCase {
            path: "/api/logs/v1/labels",
            base: "",
            param: "start",
        },
        EmptyParamCase {
            path: "/api/logs/v1/labels",
            base: "",
            param: "end",
        },
        EmptyParamCase {
            path: "/api/logs/v1/labels",
            base: "",
            param: "since",
        },
        // /label/{name}/values
        EmptyParamCase {
            path: "/api/logs/v1/label/env/values",
            base: "",
            param: "start",
        },
        EmptyParamCase {
            path: "/api/logs/v1/label/env/values",
            base: "",
            param: "end",
        },
        EmptyParamCase {
            path: "/api/logs/v1/label/env/values",
            base: "",
            param: "since",
        },
        // /series — `match[]` present so `start`/`end` are actually reached.
        EmptyParamCase {
            path: "/api/logs/v1/series",
            base: SELECTOR_MATCH,
            param: "start",
        },
        EmptyParamCase {
            path: "/api/logs/v1/series",
            base: SELECTOR_MATCH,
            param: "end",
        },
        EmptyParamCase {
            path: "/api/logs/v1/series",
            base: SELECTOR_MATCH,
            param: "since",
        },
        // /stats
        EmptyParamCase {
            path: "/api/logs/v1/stats",
            base: "",
            param: "query",
        },
        EmptyParamCase {
            path: "/api/logs/v1/stats",
            base: SELECTOR_QUERY,
            param: "start",
        },
        EmptyParamCase {
            path: "/api/logs/v1/stats",
            base: SELECTOR_QUERY,
            param: "end",
        },
        EmptyParamCase {
            path: "/api/logs/v1/stats",
            base: SELECTOR_QUERY,
            param: "since",
        },
        // /volume
        EmptyParamCase {
            path: "/api/logs/v1/volume",
            base: "",
            param: "query",
        },
        EmptyParamCase {
            path: "/api/logs/v1/volume",
            base: SELECTOR_QUERY,
            param: "limit",
        },
        EmptyParamCase {
            path: "/api/logs/v1/volume",
            base: SELECTOR_QUERY,
            param: "aggregateBy",
        },
        EmptyParamCase {
            path: "/api/logs/v1/volume",
            base: SELECTOR_QUERY,
            param: "targetLabels",
        },
        EmptyParamCase {
            path: "/api/logs/v1/volume",
            base: SELECTOR_QUERY,
            param: "since",
        },
        // /patterns
        EmptyParamCase {
            path: "/api/logs/v1/patterns",
            base: "",
            param: "query",
        },
        EmptyParamCase {
            path: "/api/logs/v1/patterns",
            base: SELECTOR_QUERY,
            param: "step",
        },
        EmptyParamCase {
            path: "/api/logs/v1/patterns",
            base: SELECTOR_QUERY,
            param: "start",
        },
        EmptyParamCase {
            path: "/api/logs/v1/patterns",
            base: SELECTOR_QUERY,
            param: "end",
        },
        EmptyParamCase {
            path: "/api/logs/v1/patterns",
            base: SELECTOR_QUERY,
            param: "since",
        },
        // /detected_labels — `query` is optional here.
        EmptyParamCase {
            path: "/api/logs/v1/detected_labels",
            base: "",
            param: "query",
        },
        EmptyParamCase {
            path: "/api/logs/v1/detected_labels",
            base: "",
            param: "start",
        },
        EmptyParamCase {
            path: "/api/logs/v1/detected_labels",
            base: "",
            param: "end",
        },
        EmptyParamCase {
            path: "/api/logs/v1/detected_labels",
            base: "",
            param: "since",
        },
        // /detected_fields
        EmptyParamCase {
            path: "/api/logs/v1/detected_fields",
            base: "",
            param: "query",
        },
        EmptyParamCase {
            path: "/api/logs/v1/detected_fields",
            base: SELECTOR_QUERY,
            param: "line_limit",
        },
        EmptyParamCase {
            path: "/api/logs/v1/detected_fields",
            base: SELECTOR_QUERY,
            param: "limit",
        },
        EmptyParamCase {
            path: "/api/logs/v1/detected_fields",
            base: SELECTOR_QUERY,
            param: "field_limit",
        },
        EmptyParamCase {
            path: "/api/logs/v1/detected_fields",
            base: SELECTOR_QUERY,
            param: "start",
        },
        EmptyParamCase {
            path: "/api/logs/v1/detected_fields",
            base: SELECTOR_QUERY,
            param: "end",
        },
        EmptyParamCase {
            path: "/api/logs/v1/detected_fields",
            base: SELECTOR_QUERY,
            param: "since",
        },
    ];

    /// `query={env="prod"}` percent-encoded — a bare stream selector, so
    /// it is accepted by every route in the table including `/volume` and
    /// `/patterns`, which reject any pipeline stage.
    const SELECTOR_QUERY: &str = "query=%7Benv%3D%22prod%22%7D";
    /// The same selector as a repeated `match[]`, for `/series`.
    const SELECTOR_MATCH: &str = "match%5B%5D=%7Benv%3D%22prod%22%7D";

    /// The scalar parameters read through `params::get` that
    /// [`EMPTY_PARAM_CASES`] cannot carry, because `/tail` is the only
    /// route that reads them and `/tail` cannot ride the routed table:
    /// axum's `WebSocketUpgrade` extractor rejects a plain `GET` before
    /// the handler body runs, so a routed probe of `/tail` measures
    /// axum's rejection and never our parsing — identically for every
    /// parameter, including one we got wrong. They are gated directly on
    /// `parse_tail_params` instead, by `tail.rs`'s
    /// `empty_tail_params_default_exactly_as_absent_ones`.
    ///
    /// **This list is CHECKED, not maintained by hand.**
    /// `the_empty_param_table_covers_every_parameter_the_handlers_read`
    /// asserts it equals, exactly, the parameters `tail.rs`'s production
    /// region reads through `params::get(` and no other `logs_api`
    /// production region reads. Both halves of that are load-bearing:
    ///
    /// * A name here that `tail.rs` does not read would be a free pass —
    ///   the union below would absorb a parameter genuinely missing from
    ///   [`EMPTY_PARAM_CASES`] and the guard would stay green while
    ///   exactly what it exists to catch happened.
    /// * A name `tail.rs` reads but another handler reads too (`query`,
    ///   `limit`, `start`) must NOT be exempted: a routed row can reach
    ///   it, so it belongs in the table, and listing it here would let
    ///   the union mask its removal from the table in the same way.
    const TAIL_ONLY_PARAMS: &[&str] = &["delay_for"];

    /// The `/api/logs/v1` paths mounted with a `POST` binding
    /// ([`mount_log_query_routes`], [`mount_detected_routes`]) — the rows
    /// of [`EMPTY_PARAM_CASES`] that also run through `form_post`, where
    /// the same pairs arrive as an `application/x-www-form-urlencoded`
    /// body through `handlers::read_form_pairs` rather than as a query
    /// string.
    const POST_PATHS: &[&str] = &[
        "/api/logs/v1/query_range",
        "/api/logs/v1/query",
        "/api/logs/v1/labels",
        "/api/logs/v1/label/env/values",
        "/api/logs/v1/series",
        "/api/logs/v1/detected_labels",
        "/api/logs/v1/detected_fields",
        // Issue #406 Part B2: these four gained `POST` with the same
        // form-decode core, so the #391 identity has to hold on them too.
        "/api/logs/v1/stats",
        "/api/logs/v1/volume",
        "/api/logs/v1/patterns",
    ];

    fn with_empty(base: &str, param: &str) -> String {
        if base.is_empty() {
            format!("{param}=")
        } else {
            format!("{base}&{param}=")
        }
    }

    fn uri(path: &str, query: &str) -> String {
        if query.is_empty() {
            path.to_string()
        } else {
            format!("{path}?{query}")
        }
    }

    /// **Issue #391, the identity — routed, over every scalar parameter.**
    ///
    /// A present-but-empty scalar parameter answers exactly as an absent
    /// one: same status, byte-identical body. That is Go's `r.Form.Get`,
    /// which returns `""` for an absent key and for an empty one alike and
    /// so cannot distinguish them; every parse helper behind it defaults
    /// on `""` (`pkg/loghttp/params.go:152-159` @ v3.7.4 `b318f282`).
    ///
    /// Against the POOLLESS `test_state()`, so a request that gets past
    /// parsing lands on `engine_for`'s `503` — the discriminating rows
    /// (`limit`, `direction`, `step`, `start`, `end`, `time`,
    /// `aggregateBy`) were `400` before this issue and are `503` after,
    /// and the `query` rows keep their `400` but move to the absent
    /// path's body. The non-discriminating rows (`targetLabels`,
    /// `line_limit`, `/detected_fields`' `limit`/`field_limit`) are pins:
    /// they already collapsed, and this stops them regressing.
    #[tokio::test]
    async fn an_empty_scalar_param_answers_exactly_as_an_absent_one() {
        for case in EMPTY_PARAM_CASES {
            let empty = with_empty(case.base, case.param);

            let (absent_status, absent_body) =
                routed(router(), get_req(&uri(case.path, case.base))).await;
            let (empty_status, empty_body) =
                routed(router(), get_req(&uri(case.path, &empty))).await;
            assert_eq!(
                empty_status,
                absent_status,
                "GET {} with `{}=` must answer as the absent parameter does\nempty: {}\nabsent: {}",
                case.path,
                case.param,
                String::from_utf8_lossy(&empty_body),
                String::from_utf8_lossy(&absent_body),
            );
            assert_eq!(
                empty_body,
                absent_body,
                "GET {} with `{}=` must answer BYTE-identically to the absent parameter\nempty: \
                 {}\nabsent: {}",
                case.path,
                case.param,
                String::from_utf8_lossy(&empty_body),
                String::from_utf8_lossy(&absent_body),
            );

            if !POST_PATHS.contains(&case.path) {
                continue;
            }
            let (absent_status, absent_body) =
                routed(router(), form_post(case.path, case.base.to_string())).await;
            let (empty_status, empty_body) =
                routed(router(), form_post(case.path, empty.clone())).await;
            assert_eq!(
                empty_status,
                absent_status,
                "POST {} with `{}=` must answer as the absent parameter does\nempty: {}\nabsent: \
                 {}",
                case.path,
                case.param,
                String::from_utf8_lossy(&empty_body),
                String::from_utf8_lossy(&absent_body),
            );
            assert_eq!(
                empty_body,
                absent_body,
                "POST {} with `{}=` must answer BYTE-identically to the absent parameter\nempty: \
                 {}\nabsent: {}",
                case.path,
                case.param,
                String::from_utf8_lossy(&empty_body),
                String::from_utf8_lossy(&absent_body),
            );
        }
    }

    /// Every string literal passed to `needle` in `src`, taken from the
    /// call's own argument list (bounded at the closing `)`), so a call
    /// shape this scanner cannot read is a PANIC rather than a silent
    /// miss.
    fn param_literals(src: &str, needle: &str) -> std::collections::BTreeSet<String> {
        let mut out = std::collections::BTreeSet::new();
        let mut rest = src;
        while let Some(i) = rest.find(needle) {
            rest = &rest[i + needle.len()..];
            let close = rest
                .find(')')
                .unwrap_or_else(|| panic!("a `{needle}` call must close its argument list"));
            let args = &rest[..close];
            let open_quote = args
                .find('"')
                .unwrap_or_else(|| panic!("a `{needle}` call must name its parameter literally"));
            let after = &args[open_quote + 1..];
            let end_quote = after
                .find('"')
                .unwrap_or_else(|| panic!("a `{needle}` parameter literal must terminate"));
            out.insert(after[..end_quote].to_string());
            rest = &rest[close..];
        }
        out
    }

    /// **Issue #391, the completeness guard.** The identity table above is
    /// only as good as its coverage, so coverage is checked rather than
    /// claimed: every parameter any `logs_api` handler reads through
    /// `params::get` must appear in [`EMPTY_PARAM_CASES`] or in
    /// [`TAIL_ONLY_PARAMS`], and the only parameter read through
    /// `params::get_all` must be `match[]`. A new parameter added to any
    /// handler reddens this test until it is classified into one seam or
    /// the other — which is the point, because the two seams behave
    /// DIFFERENTLY (see
    /// `an_empty_repeated_match_is_a_value_not_an_absence`).
    ///
    /// **The exemption is derived from the same scan, not trusted.**
    /// [`TAIL_ONLY_PARAMS`] is asserted equal to the parameters `tail.rs`
    /// reads and no other production region reads, BEFORE it is unioned
    /// into the classified set. Without that, a stale or speculative name
    /// there would absorb a parameter genuinely missing from
    /// [`EMPTY_PARAM_CASES`] and this test would stay green through
    /// exactly the failure it exists to catch (task-manager ruling on
    /// #391, 2026-08-09).
    ///
    /// **What this test cannot see:** a parameter the reference reads and
    /// we never read at all. `since`, `interval`, `regexp` and `shards`
    /// are read by `pkg/loghttp` @ v3.7.4 on routes we mount and appear
    /// nowhere in our source, so no scan of our source can surface them.
    /// They are absence gaps rather than empty-vs-absent ones, are
    /// enumerated from the reference's handlers in issue #391's plan, and
    /// are filed separately.
    #[test]
    fn the_empty_param_table_covers_every_parameter_the_handlers_read() {
        type Params = std::collections::BTreeSet<String>;
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/logs_api");
        let mut scalar = Params::new();
        let mut repeated = Params::new();
        // Kept per-file so the `/tail` exemption below can be DERIVED from
        // the scan rather than trusted.
        let mut tail_scalar = Params::new();
        let mut non_tail_scalar = Params::new();
        let mut scanned = 0usize;
        for entry in std::fs::read_dir(&dir).expect("the logs_api source directory") {
            let path = entry.expect("dir entry").path();
            if path.extension().is_none_or(|x| x != "rs") {
                continue;
            }
            let is_tail = path.file_name().is_some_and(|n| n == "tail.rs");
            let src = std::fs::read_to_string(&path).expect("source");
            // Production region only — the same `#[cfg(test)]` split
            // `no_request_path_calls_the_logql_parser_directly` uses.
            let production = match src.find("\n#[cfg(test)]\nmod tests {") {
                Some(i) => &src[..i],
                None => &src[..],
            };
            let file_scalar = param_literals(production, "params::get(");
            if is_tail {
                tail_scalar.extend(file_scalar.iter().cloned());
            } else {
                non_tail_scalar.extend(file_scalar.iter().cloned());
            }
            scalar.extend(file_scalar);
            repeated.extend(param_literals(production, "params::get_all("));
            scanned += 1;
        }
        assert!(scanned >= 9, "the scan found only {scanned} source files");
        assert!(
            !tail_scalar.is_empty(),
            "the scan never found tail.rs — the exemption below would be vacuous"
        );

        // The exemption, derived: what `tail.rs` reads that NO routed
        // handler reads. A name `tail.rs` does not read at all would be a
        // free pass; a name another handler also reads can ride the routed
        // table and must not be exempted. Either way the union below would
        // otherwise absorb a parameter missing from EMPTY_PARAM_CASES.
        let derived_tail_only: Params = tail_scalar.difference(&non_tail_scalar).cloned().collect();
        let declared_tail_only: Params = TAIL_ONLY_PARAMS.iter().map(|p| p.to_string()).collect();
        assert_eq!(
            declared_tail_only, derived_tail_only,
            "TAIL_ONLY_PARAMS must be exactly the parameters tail.rs reads through `params::get(` \
             and no routed handler reads — each one gated by tail.rs's \
             `empty_tail_params_default_exactly_as_absent_ones`"
        );

        let mut classified: Params = EMPTY_PARAM_CASES
            .iter()
            .map(|c| c.param.to_string())
            .collect();
        classified.extend(derived_tail_only);
        assert_eq!(
            scalar, classified,
            "every parameter read through `params::get` must be classified: in \
             EMPTY_PARAM_CASES, or in TAIL_ONLY_PARAMS with its own gate in tail.rs"
        );

        // Issue #406 Part A: `/series` now reads BOTH repeated keys the
        // reference reads — `match` and `match[]` (`pkg/loghttp/series.go:23-25`
        // @ v3.7.4 `b318f282`). Neither collapses empty into absent.
        let expected_repeated: Params = ["match".to_string(), "match[]".to_string()]
            .into_iter()
            .collect();
        assert_eq!(
            repeated, expected_repeated,
            "`params::get_all` is the NON-collapsing seam (issue #391); a new repeated parameter \
             needs its own decision, not this table's"
        );
    }

    /// **Issue #391, the pin on the fix SHAPE.** The reference collapses
    /// empty into absent at the SCALAR seam only. Repeated parameters go
    /// through `r.Form[...]` (`pkg/loghttp/params.go:79-81`,
    /// `pkg/loghttp/series.go:23-25` @ v3.7.4 `b318f282`), which keeps
    /// `""` as a value — measured against `grafana/loki:3.7.4` on
    /// 2026-08-09, `/loki/api/v1/series?match[]=` is a `400`
    /// `parse error : syntax error: unexpected $end` while `/series` with
    /// no `match[]` at all is a `200`. Ours is the same `400` from the
    /// same seam with our own parser's prose (the message text is below
    /// the parity bar, owner ruling on #253); the body asserted below is
    /// PulsusDB's, measured 2026-08-09.
    ///
    /// So `params::get_all` deliberately does NOT collapse, and the two
    /// answers must stay different. This test passes both before and
    /// after the change by design: it is not evidence of the fix, it is
    /// the guard that fails if the collapse is over-applied to `get_all`,
    /// which would look like a completion rather than a regression.
    ///
    /// (Issue #406 Part A closed the other half: an ABSENT `match[]` is
    /// now the reference's `200` over every series, which against the
    /// poolless `test_state()` surfaces as the engine `503`. The two
    /// answers still differ, and now differ by status class.)
    #[tokio::test]
    async fn an_empty_repeated_match_is_a_value_not_an_absence() {
        let (empty_status, empty_body) =
            routed(router(), get_req("/api/logs/v1/series?match%5B%5D=")).await;
        let (absent_status, absent_body) = routed(router(), get_req("/api/logs/v1/series")).await;
        assert_eq!(empty_status, StatusCode::BAD_REQUEST);
        assert_eq!(
            absent_status,
            StatusCode::SERVICE_UNAVAILABLE,
            "an absent `match[]` reaches the engine (issue #406 Part A)"
        );
        let empty_text = String::from_utf8_lossy(&empty_body).into_owned();
        assert!(
            !empty_text.contains("missing required parameter"),
            "an EMPTY `match[]` is a value, not an absence, got {empty_text:?}"
        );
        assert_eq!(
            empty_text, "unexpected end of query at byte 0: expected '{'",
            "an empty `match[]` reaches the selector parser"
        );
        assert_ne!(empty_body, absent_body);
    }

    // -- Issue #406 --------------------------------------------------------

    /// **Part A, the routed half.** `/series` with no `match[]` no longer
    /// short-circuits on a missing parameter: it reaches the engine, which
    /// against the poolless `test_state()` is a `503`. Pre-change: `400`
    /// `missing required parameter 'match[]': at least one selector is
    /// required`, container-measured against `grafana/loki:3.7.4` as a
    /// `200` over all three seeded series on 2026-08-10.
    ///
    /// The `{}` and `{ }` spellings ride the same branch (the reference's
    /// lone-`{}` collapse), and both alias surfaces answer identically.
    #[tokio::test]
    async fn series_without_match_answers_every_series_in_the_window() {
        for query in ["", "?match%5B%5D=%7B%7D", "?match%5B%5D=%7B%20%7D"] {
            let (status, body) =
                routed(router(), get_req(&format!("/api/logs/v1/series{query}"))).await;
            let text = String::from_utf8_lossy(&body).into_owned();
            assert_eq!(
                status,
                StatusCode::SERVICE_UNAVAILABLE,
                "GET /series{query} must reach the engine, got {text:?}"
            );
            assert!(
                !text.contains("missing required parameter"),
                "GET /series{query} must not be MissingMatch, got {text:?}"
            );
        }
        // A `{}` alongside a real selector is NOT a collapse — the
        // reference answers `400 0 matchers in group: {}` there, and ours
        // is the same status from our own parser.
        let (status, _body) = routed(
            router(),
            get_req("/api/logs/v1/series?match%5B%5D=%7B%7D&match%5B%5D=%7Bapp%3D%22a%22%7D"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    /// **Part B2.** The four routes that were `GET`-only answer a form
    /// `POST` exactly as the equivalent `GET` — same status, byte-identical
    /// body — on both the native and the `/loki/api/v1` alias surface.
    /// Pre-change: `405` on all eight bindings. `/tail` stays out, on the
    /// measurement that its upstream `POST` registration is nominal (a POST
    /// cannot carry a WebSocket handshake; the reference answers `400`).
    #[tokio::test]
    async fn the_get_only_routes_now_accept_a_form_post() {
        let native = [
            ("/api/logs/v1/stats", SELECTOR_QUERY),
            ("/api/logs/v1/volume", SELECTOR_QUERY),
            ("/api/logs/v1/patterns", SELECTOR_QUERY),
            ("/api/logs/v1/label/env/values", ""),
        ];
        let alias = [
            ("/loki/api/v1/index/stats", SELECTOR_QUERY),
            ("/loki/api/v1/index/volume", SELECTOR_QUERY),
            ("/loki/api/v1/patterns", SELECTOR_QUERY),
            ("/loki/api/v1/label/env/values", ""),
        ];
        for (make, rows) in [
            (router as fn() -> Router<AppState>, native),
            (compat_router as fn() -> Router<AppState>, alias),
        ] {
            for (path, base) in rows {
                let (get_status, get_body) = routed(make(), get_req(&uri(path, base))).await;
                let (post_status, post_body) =
                    routed(make(), form_post(path, base.to_string())).await;
                assert_ne!(
                    post_status,
                    StatusCode::METHOD_NOT_ALLOWED,
                    "POST {path} must be mounted"
                );
                assert_eq!(post_status, get_status, "POST {path} vs GET {path}");
                assert_eq!(
                    post_body, get_body,
                    "POST {path} must answer byte-identically to GET {path}"
                );
            }
        }
        // `/tail` stays GET-only.
        let (status, _body) = routed(
            router(),
            form_post("/api/logs/v1/tail", SELECTOR_QUERY.to_string()),
        )
        .await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    }

    /// **Part B1, the routed table.** A `POST` reads the URL query
    /// alongside the body, in the reference's order: the body's pairs
    /// first, the URL's appended (Go's `ParseForm`). Three rows per POST
    /// route, each one a different failure if the merge is wrong:
    ///
    /// * **URL-only** must equal the GET with the same query string —
    ///   fails on `ff0fb09`, where the URL was dropped entirely.
    /// * **body `limit=` + URL `limit=5`** must equal the parameter being
    ///   absent, NOT `5` — passes today and fails under the plausible
    ///   "fall back to the URL when the body value is empty" spelling.
    ///   Container-measured: the reference serves 100 there, not 5.
    /// * **repeated `match[]` split across the two carriers** must union —
    ///   the reference returns BOTH series.
    #[tokio::test]
    async fn a_post_reads_url_query_parameters_alongside_the_body() {
        for path in POST_PATHS {
            let base = post_base_query(path);
            // (a) URL-only POST == the equivalent GET.
            let (get_status, get_body) = routed(router(), get_req(&uri(path, base))).await;
            let (post_status, post_body) = routed(
                router(),
                form_post(&uri(path, base), String::new()),
            )
            .await;
            assert_eq!(
                post_status,
                get_status,
                "POST {path} with everything in the URL must answer as the GET does: {}",
                String::from_utf8_lossy(&post_body)
            );
            assert_eq!(
                post_body, get_body,
                "POST {path} with everything in the URL must answer byte-identically to GET"
            );

            // (b) an empty body value is the BODY's value and wins, so the
            // URL's `limit=5` is not consulted.
            let with_url_limit = if base.is_empty() {
                format!("{path}?limit=5")
            } else {
                format!("{path}?{base}&limit=5")
            };
            let (empty_status, empty_body) = routed(
                router(),
                form_post(&with_url_limit, format!("{base}&limit=")),
            )
            .await;
            let (absent_status, absent_body) =
                routed(router(), form_post(path, base.to_string())).await;
            assert_eq!(
                empty_status, absent_status,
                "POST {path} body `limit=` must not fall back to the URL's `limit=5`"
            );
            assert_eq!(empty_body, absent_body, "POST {path}");
        }

        // (c) the repeated seam unions across carriers. Both selectors
        // parse, so the request reaches the engine (503, poolless) rather
        // than a parse 400 — and a body-only merge would answer the same,
        // so the discriminating half is the URL-only `match[]` above.
        let (status, body) = routed(
            router(),
            form_post(
                "/api/logs/v1/series?match%5B%5D=%7Bapp%3D%22b%22%7D",
                "match%5B%5D=%7Bapp%3D%22a%22%7D".to_string(),
            ),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "both carriers' selectors must parse: {}",
            String::from_utf8_lossy(&body)
        );
        // A malformed selector arriving ONLY through the URL must still be
        // seen — the direct proof that the URL half is parsed at all.
        let (status, _body) = routed(
            router(),
            form_post(
                "/api/logs/v1/series?match%5B%5D=%7B",
                "match%5B%5D=%7Bapp%3D%22a%22%7D".to_string(),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    /// A minimal valid query string for `path`, for the POST-merge table.
    fn post_base_query(path: &str) -> &'static str {
        if path.ends_with("/series") {
            SELECTOR_MATCH
        } else if path.ends_with("/labels")
            || path.ends_with("/values")
            || path.ends_with("/detected_labels")
        {
            ""
        } else {
            SELECTOR_QUERY
        }
    }

    /// **The `end < start` table, both positives and negatives.** The
    /// reference refuses a reversed window on seven of the nine routes and
    /// deliberately serves it on `/query` and `/patterns` — an exemption
    /// nobody tests is where the next defect hides, so both negatives are
    /// rows here. Container-measured on both stores 2026-08-10 (issue #406
    /// adjacent finding 7, folded in by ruling v2).
    ///
    /// Against the poolless `test_state()` the refusal is a `400` and
    /// everything else is the engine `503`, so the two outcomes are
    /// distinguishable without a database.
    #[tokio::test]
    async fn end_before_start_is_refused_on_exactly_the_reference_routes() {
        // A window whose `end` precedes its `start` by one hour. Both are
        // 19-digit nanosecond literals, so Part D's length rule cannot
        // change what they mean.
        const START: &str = "1786342706000000000";
        const END: &str = "1786339106000000000";
        // (path, base query, refuses?)
        let cases: &[(&str, &str, bool)] = &[
            ("/api/logs/v1/query_range", SELECTOR_QUERY, true),
            ("/api/logs/v1/labels", "", true),
            ("/api/logs/v1/series", SELECTOR_MATCH, true),
            ("/api/logs/v1/stats", SELECTOR_QUERY, true),
            ("/api/logs/v1/volume", SELECTOR_QUERY, true),
            ("/api/logs/v1/detected_labels", "", true),
            ("/api/logs/v1/detected_fields", SELECTOR_QUERY, true),
            // The two the reference deliberately does NOT check.
            // `/query` is an instant query and carries no `start`/`end` at
            // all; the row is here so its absence from the refusal set is
            // asserted rather than assumed.
            ("/api/logs/v1/query", SELECTOR_QUERY, false),
            ("/api/logs/v1/patterns", SELECTOR_QUERY, false),
        ];
        for &(path, base, refuses) in cases {
            let query = if base.is_empty() {
                format!("start={START}&end={END}")
            } else {
                format!("{base}&start={START}&end={END}")
            };
            let (status, body) = routed(router(), get_req(&uri(path, &query))).await;
            let text = String::from_utf8_lossy(&body).into_owned();
            if refuses {
                assert_eq!(
                    status,
                    StatusCode::BAD_REQUEST,
                    "GET {path} with `end` before `start` must be a 400, got {text:?}"
                );
                assert_eq!(
                    text, "invalid time range: 'end' precedes 'start'",
                    "GET {path}"
                );
            } else {
                assert_eq!(
                    status,
                    StatusCode::SERVICE_UNAVAILABLE,
                    "GET {path} is a reference EXEMPTION and must still be served, got {text:?}"
                );
            }
        }

        // `end == start` is served everywhere: every reference call site
        // spells `End.Before(Start)`, despite the message's "or equal".
        let (status, body) = routed(
            router(),
            get_req(&uri(
                "/api/logs/v1/query_range",
                &format!("{SELECTOR_QUERY}&start={START}&end={START}"),
            )),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "`end == start` is served: {}",
            String::from_utf8_lossy(&body)
        );
    }

    /// **Part D, routed.** The ten-digit unix-SECONDS spelling of a window
    /// and its nineteen-digit nanosecond spelling are the same request:
    /// same status, byte-identical body, on every route that reads
    /// `start`/`end`. Pre-change they differed on every one — the seconds
    /// form collapsed to a microsecond window and answered `200` with no
    /// data.
    #[tokio::test]
    async fn a_seconds_window_and_the_same_window_in_nanoseconds_answer_identically() {
        const START_S: &str = "1786342706";
        const END_S: &str = "1786346306";
        const START_NS: &str = "1786342706000000000";
        const END_NS: &str = "1786346306000000000";
        let routes: &[(&str, &str)] = &[
            ("/api/logs/v1/query_range", SELECTOR_QUERY),
            ("/api/logs/v1/labels", ""),
            ("/api/logs/v1/label/env/values", ""),
            ("/api/logs/v1/series", SELECTOR_MATCH),
            ("/api/logs/v1/stats", SELECTOR_QUERY),
            ("/api/logs/v1/volume", SELECTOR_QUERY),
            ("/api/logs/v1/patterns", SELECTOR_QUERY),
            ("/api/logs/v1/detected_labels", ""),
            ("/api/logs/v1/detected_fields", SELECTOR_QUERY),
        ];
        for &(path, base) in routes {
            let build = |start: &str, end: &str| {
                let q = if base.is_empty() {
                    format!("start={start}&end={end}")
                } else {
                    format!("{base}&start={start}&end={end}")
                };
                uri(path, &q)
            };
            let (secs_status, secs_body) =
                routed(router(), get_req(&build(START_S, END_S))).await;
            let (nanos_status, nanos_body) =
                routed(router(), get_req(&build(START_NS, END_NS))).await;
            assert_eq!(
                secs_status,
                nanos_status,
                "GET {path}: seconds and nanoseconds must be the same request\nseconds: {}\nnanos: \
                 {}",
                String::from_utf8_lossy(&secs_body),
                String::from_utf8_lossy(&nanos_body),
            );
            assert_eq!(secs_body, nanos_body, "GET {path}");
        }

        // `/query`'s `time` rides the same parser.
        let (secs_status, secs_body) = routed(
            router(),
            get_req(&uri(
                "/api/logs/v1/query",
                &format!("{SELECTOR_QUERY}&time={START_S}"),
            )),
        )
        .await;
        let (nanos_status, nanos_body) = routed(
            router(),
            get_req(&uri(
                "/api/logs/v1/query",
                &format!("{SELECTOR_QUERY}&time={START_NS}"),
            )),
        )
        .await;
        assert_eq!(secs_status, nanos_status);
        assert_eq!(secs_body, nanos_body);
    }

    /// **Part D's second failure mode, and the worst of the three: an
    /// error describing a query nobody wrote.** A `start` in seconds with
    /// an `end` in nanoseconds used to compute a 1,786,343,164,213,657,294
    /// ns — fifty-six year — span and refuse it with the span cap, naming
    /// a window the user never asked for. The reference serves it. After
    /// Part D the two bounds land on the same scale and the request is
    /// served (poolless: `503`).
    #[tokio::test]
    async fn a_mixed_seconds_and_nanoseconds_window_no_longer_reports_a_fifty_six_year_span() {
        for (start, end) in [
            ("1786342706", "1786346306000000000"),
            ("1786342706000000000", "1786346306"),
        ] {
            let (status, body) = routed(
                router(),
                get_req(&uri(
                    "/api/logs/v1/query_range",
                    &format!("{SELECTOR_QUERY}&start={start}&end={end}"),
                )),
            )
            .await;
            let text = String::from_utf8_lossy(&body).into_owned();
            assert!(
                !text.contains("outside the supported range"),
                "start={start}&end={end} must not invent a span the client never sent: {text:?}"
            );
            assert_eq!(
                status,
                StatusCode::SERVICE_UNAVAILABLE,
                "start={start}&end={end}: {text:?}"
            );
        }
    }

    /// **What Part D masks, pinned as a pair.** A seconds-valued window
    /// used to collapse to microseconds, so the 5-year span cap
    /// (`MAX_QUERY_SPAN_NS`) never fired on it and a six-year request was
    /// a `200`. Without this criterion, the identity test above could pass
    /// on windows that are empty on both sides while the cap stays
    /// unexercised. The two spellings must produce the same `400` **byte
    /// for byte** — same span figure, same message.
    #[tokio::test]
    async fn a_six_year_seconds_window_reaches_the_span_cap() {
        // Six 365-day years, in seconds and in nanoseconds.
        const START_S: i64 = 1_500_000_000;
        const SIX_YEARS_S: i64 = 6 * 365 * 24 * 3_600;
        let end_s = START_S + SIX_YEARS_S;
        let secs = format!("{SELECTOR_QUERY}&start={START_S}&end={end_s}");
        let nanos = format!(
            "{SELECTOR_QUERY}&start={}&end={}",
            START_S * 1_000_000_000,
            end_s * 1_000_000_000
        );
        let (secs_status, secs_body) =
            routed(router(), get_req(&uri("/api/logs/v1/query_range", &secs))).await;
        let (nanos_status, nanos_body) =
            routed(router(), get_req(&uri("/api/logs/v1/query_range", &nanos))).await;
        assert_eq!(
            secs_status,
            StatusCode::BAD_REQUEST,
            "a six-year window in SECONDS must reach the span cap: {}",
            String::from_utf8_lossy(&secs_body)
        );
        assert_eq!(nanos_status, secs_status);
        assert_eq!(
            secs_body, nanos_body,
            "the cap's 400 must be byte-identical in both encodings"
        );
        let text = String::from_utf8_lossy(&secs_body).into_owned();
        assert!(
            text.contains("189216000000000000") && text.contains("157680000000000000"),
            "the cap message names the requested span and the cap: {text:?}"
        );
    }

    /// **Part C, routed.** `since` is read on the nine routes that carry a
    /// `start`/`end` pair and rejected rather than ignored: `since=bogus`
    /// moves from a silent `200` to a `400`. `/query` is the reference's
    /// exemption (it has only `time`) and is asserted as such.
    #[tokio::test]
    async fn since_is_read_on_every_bounded_route_and_ignored_on_the_instant_one() {
        let bounded: &[(&str, &str)] = &[
            ("/api/logs/v1/query_range", SELECTOR_QUERY),
            ("/api/logs/v1/labels", ""),
            ("/api/logs/v1/label/env/values", ""),
            ("/api/logs/v1/series", SELECTOR_MATCH),
            ("/api/logs/v1/stats", SELECTOR_QUERY),
            ("/api/logs/v1/volume", SELECTOR_QUERY),
            ("/api/logs/v1/patterns", SELECTOR_QUERY),
            ("/api/logs/v1/detected_labels", ""),
            ("/api/logs/v1/detected_fields", SELECTOR_QUERY),
        ];
        for &(path, base) in bounded {
            let q = if base.is_empty() {
                "since=bogus".to_string()
            } else {
                format!("{base}&since=bogus")
            };
            let (status, body) = routed(router(), get_req(&uri(path, &q))).await;
            let text = String::from_utf8_lossy(&body).into_owned();
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "GET {path}?since=bogus must be a 400, got {text:?}"
            );
            assert_eq!(
                text, "could not parse 'since' parameter \"bogus\": expected a duration literal",
                "GET {path}"
            );
        }

        // `/query` never reads it (the reference's one exemption).
        let (status, _body) = routed(
            router(),
            get_req(&uri(
                "/api/logs/v1/query",
                &format!("{SELECTOR_QUERY}&since=bogus"),
            )),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
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
