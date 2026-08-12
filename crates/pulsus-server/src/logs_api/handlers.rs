//! The five `/api/logs/v1` handlers (docs/api.md §2): parse params → parse
//! LogQL (`pulsus-logql`) → dispatch to `LogQlEngine` (`pulsus-read`) →
//! encode the envelope (`encode.rs`). Thin by design — all planning/SQL/
//! execution stays in `pulsus-read` (issue #13 architect plan).

use axum::body::{Body, Bytes};
use axum::extract::{FromRequest, Path, RawQuery, Request, State};
use axum::http::{HeaderMap, header};
use axum::response::{IntoResponse, Response};

use pulsus_logql::{Expr, LogExpr};
use pulsus_read::{LogQlEngine, QueryParams, QuerySpec, TimeBounds};

use crate::app::AppState;
use crate::chconfig;

use super::encode;
use super::error::ApiError;
use super::params::{self, ParamError};

/// `X-Pulsus-Explain: 1` (docs/api.md "Request headers"): included on all
/// five endpoints (issue #13 architect plan amendment §4).
fn wants_explain(headers: &HeaderMap) -> bool {
    headers
        .get("x-pulsus-explain")
        .and_then(|v| v.to_str().ok())
        == Some("1")
}

/// Acquires the shared `Arc<ChPool>` from `AppState` (mirrors `ops::ready`'s
/// pattern: clone the `Option` out from behind the lock, drop the guard
/// before doing anything else) and builds a `LogQlEngine` over it —
/// `503 unavailable` before the pool is established, matching `/ready`.
pub(super) async fn engine_for(state: &AppState) -> Result<LogQlEngine, ApiError> {
    let pool = {
        let guard = state.pool.read().await;
        guard.clone()
    };
    let pool = pool.ok_or(ApiError::PoolUnavailable)?;
    // Issue #114: the consistency-config invariant is already enforced at
    // config load, so this is unreachable in the real binary; a failure maps
    // to the existing 503 "not serving" semantics.
    chconfig::logql_engine(pool, &state.config).map_err(|_| ApiError::PoolUnavailable)
}

/// Parses `start`/`end`/`since` (docs/api.md §2.1) and applies the
/// **5-year query-span cap** (issue #343).
///
/// **Defaults, issue #406 Part C, exactly the reference's `determineBounds`
/// (`pkg/loghttp/params.go:91-119` @ grafana/loki v3.7.4 `b318f282`):**
/// `end = now`; `start = min(end, now) - since`; `since = 1h`. `since` is
/// read here and nowhere else, and is ignored entirely when `start` is
/// present. The `min(end, now)` clamp — the reference's `endOrNow` — is
/// what stops a future `end` dragging the default `start` into the future
/// with it, and it applies to requests carrying no `since` at all.
///
/// **THE CAP LIVES HERE BECAUSE THIS IS THE ONLY PLACE EVERY ENDPOINT
/// CARRYING `start`/`end` PASSES THROUGH.** Its first cut sat in
/// `pulsus_read::logql::plan`, which three of these code paths never
/// reach, so a 20-year range walked straight past it.
///
/// **Whether a route reaches `plan()` is a property of the ENGINE METHOD
/// it calls, not of whether it takes a selector.** The nine routes sharing
/// this function, each verified against the engine code and then measured
/// (see below):
///
/// | route | reaches `plan()`? |
/// |---|---|
/// | `query_range` | yes — it IS a plan |
/// | `series` | yes — `series_inner` builds a synthetic `QuerySpec::Range` and plans per selector |
/// | `stats`, `patterns`, `volume`, `detected_fields` | yes — same idiom; each requires a `query` |
/// | `detected_labels` **with** `query` | yes — the `Some(expr)` arm plans |
/// | `detected_labels` **without** `query` | **NO** — the `None` arm derives months and aggregates directly |
/// | `labels`, `label/{name}/values` | **NO** — label discovery resolves names/values without a plan |
///
/// So the last three rows are capped by THIS call and nothing else, and
/// the rest are capped twice. `/series` looks like it should be among them
/// and is not; `/detected_labels` is in both groups depending on one
/// optional parameter.
///
/// **How that table was established, because guessing it from route shape
/// is what produced two wrong versions of this comment:** the call below
/// was deleted and `logs_api_live::nothing_in_a_query_may_span_more_than_five_years`
/// re-run, which sends an over-cap range to every row (both
/// `detected_labels` forms) and COLLECTS the ones still served. It named
/// exactly `labels`, `label/{name}/values` and the unscoped
/// `detected_labels` — the scoped `detected_labels` row, same path, stayed
/// capped, which is what makes the `query`-dependent split a measurement
/// rather than a reading.
///
/// The two endpoints deliberately NOT covered, because neither takes a
/// time RANGE:
/// * `/query` (instant) — a single `time`, no span to bound. Its window is
///   `[time - range, time]`, and `[range]` is capped in the parser.
/// * `/tail` — a live stream with a `start` and no `end`, whose `start` is
///   already raised to the retention floor (`tail.rs`), so its span is
///   bounded by retention rather than by the client.
///
/// `pulsus_read::logql::check_query_span_ns` is the single implementation;
/// `plan` keeps its own call for the library API.
pub(super) fn parse_bounds(pairs: &[(String, String)]) -> Result<(i64, i64), ApiError> {
    let now = params::now_ns();
    let since_ns = params::parse_since(params::get(pairs, "since"))?;
    let end_ns = match params::get(pairs, "end") {
        Some(v) => params::parse_ts(v)?,
        None => now,
    };
    // `endOrNow` (`pkg/loghttp/params.go:105-111` @ v3.7.4 `b318f282`): a
    // future `end` does not push the default `start` into the future with
    // it. Container-measured on the reference 2026-08-10 — `end = now+2h`
    // with no `start` answers from `now - 1h`, not from `end - 1h`; the
    // same request with `since=10m` answers from `now - 10m` and returns
    // nothing against 20-minute-old data.
    let end_or_now = end_ns.min(now);
    let start_ns = match params::get(pairs, "start") {
        Some(v) => params::parse_ts(v)?,
        None => end_or_now.saturating_sub(since_ns),
    };
    pulsus_read::logql::check_query_span_ns(start_ns, end_ns)?;
    Ok((start_ns, end_ns))
}

/// [`parse_bounds`] plus the reference's `end < start` refusal — issue
/// #406, folded in by task-manager ruling v2 (a user swapping two dates in
/// a URL gets a plain error rather than an empty dashboard).
///
/// **The check is PER ROUTE and the exemptions are deliberate.** The
/// reference applies it on `query_range` (`ParseRangeQuery`,
/// `pkg/loghttp/query.go:483-485 @ v3.7.4 b318f282`), on `index/stats`
/// (`ParseIndexStatsQuery`, which IS `ParseRangeQuery`, `:543-547`), on
/// `index/volume` (`:621-623`), on `detected_fields` (`:676-678`), on
/// `detected_labels` (`pkg/loghttp/labels.go:99-101`), and on `labels` and
/// `series` one layer down, where `ValidateQueryTimeRangeLimits`
/// (`pkg/querier/limits/validation.go:91-93`) refuses `through.Before(from)`
/// for `SingleTenantQuerier.Label`/`Series` (`pkg/querier/querier.go:311,388`).
/// `/label/{name}/values` is in the refusal set for the same reason
/// `/labels` is, and by the same code: upstream they are ONE handler —
/// `ParseLabelQuery` sets `Values: ok` from the path variable
/// (`pkg/loghttp/labels.go:70-84`) and `SingleTenantQuerier.Label`
/// validates the range before branching on it
/// (`pkg/querier/querier.go:307-315`). Container-measured 2026-08-10:
/// `400` there, `200`-empty here. (It was left out of issue #406's first
/// route table by oversight and folded in by rulings v3.)
///
/// It does **not** apply on `/query` (instant — no range) or on
/// `/patterns` (`ParsePatternsQuery` never checks). Both negatives are
/// pinned by `mod.rs`'s
/// `end_before_start_is_refused_on_exactly_the_reference_routes`;
/// container-measured on both stores 2026-08-10.
///
/// Strictly BEFORE, not before-or-equal, despite the reference's own
/// message prose: every one of those call sites spells `End.Before(Start)`,
/// so `end == start` is served.
pub(super) fn parse_bounds_ordered(pairs: &[(String, String)]) -> Result<(i64, i64), ApiError> {
    let (start_ns, end_ns) = parse_bounds(pairs)?;
    if end_ns < start_ns {
        return Err(ParamError::EndBeforeStart.into());
    }
    Ok((start_ns, end_ns))
}

/// Form pairs for a POST: the body's pairs FIRST, then the URL query's,
/// appended — Go's `ParseForm` copies `PostForm` into `Form` and then
/// appends `url.ParseQuery(r.URL.RawQuery)` per key, and
/// `NewPrepopulateMiddleware` (`pkg/util/server/middleware.go:12-23` @
/// v3.7.4 `b318f282`) runs it for every logs route, so every handler
/// upstream sees both carriers (issue #406 Part B1).
///
/// **Order is the whole contract.** [`params::get`] returns the FIRST
/// occurrence, so the body wins a scalar collision, and
/// [`params::get_all`] returns BOTH, concatenated, so a repeated
/// parameter unions across the two carriers. Container-measured against
/// `grafana/loki:3.7.4` 2026-08-10: `?limit=7` + body `limit=5` serves 5;
/// `?limit=5` + body `limit=` serves 100 (the DEFAULT, not 5 — which is
/// what rules out the plausible "fall back to the URL when the body value
/// is empty" spelling); `?match[]={app="b"}` + body `match[]={app="a"}`
/// returns both series.
///
/// **`Content-Type` decides whether the BODY is read, never whether the
/// request is SERVED** (issue #406). A POST carrying every parameter in
/// its URL is answered under `application/json`, under `text/plain`, and
/// under no `Content-Type` at all, exactly as the reference answers it —
/// the body is simply invisible. Only a header that cannot be PARSED is a
/// `400`, and that verdict is [`params::form_body_disposition`]'s, which
/// carries the port of Go's `mime.ParseMediaType` and the measurements
/// behind it. Before #406 we refused every non-form type outright, which
/// refused working clients: `application/json` is what plenty of HTTP
/// clients send by default.
///
/// A header value that is not valid UTF-8 cannot be a well-formed media
/// type (Go's grammar admits ASCII token characters only), so it takes the
/// malformed branch rather than being read as absent.
///
/// **The header is examined BEFORE the body is consumed** — the ordering
/// is the contract, not an implementation detail, and it is why this takes
/// [`Body`] rather than `Bytes`. A `Bytes` extractor buffers the entire
/// upload before the handler runs, so a client POSTing a large body it did
/// not need to send pays the whole transfer for a request that is decided
/// by its URL. Measured 2026-08-10 against `grafana/loki:3.7.4`
/// (`b318f282`) with `Content-Length: 100000` and 7 bytes actually sent:
/// the reference answers `200` in under 10 ms under `application/json`,
/// under `text/plain` and under no `Content-Type`, and `400` just as
/// promptly for a malformed one — while PulsusDB, before this change,
/// answered **none of the four** and sat waiting for a body it was going
/// to discard. On the [`params::FormBody::Ignore`] branch the `Body` is
/// dropped unpolled, which is what lets the response go out first.
///
/// **One measured divergence, deliberate, and it is the reference being
/// wasteful rather than us being wrong.** For a form `Content-Type` whose
/// PARAMETERS are malformed (`application/x-www-form-urlencoded; bogus`),
/// Go's `parsePostForm` enters the form branch on the base type, reads and
/// parses the whole body, and only then returns the MIME error it retained
/// (`request.go:1274-1297` — `mime.ParseMediaType` yields a non-empty
/// media type ALONGSIDE `ErrInvalidMediaParameter`, `mediatype.go:164`).
/// Measured: the reference waits for the full body and then answers `400`;
/// we answer the same `400` without reading it. Same status, same body,
/// strictly less transfer — so the divergence is invisible to any client
/// that sends a complete request, and is not ledgered as a behavioural
/// difference. Reading an upload we have already decided to reject is
/// exactly the waste this change exists to remove.
///
/// Returns a ready [`Response`] rather than an `ApiError` because the
/// over-limit rejection on the parse branch is **axum's own** and must
/// stay byte-identical: routing it through [`ApiError`] would re-render it
/// through `plain_text_error`, which adds `X-Content-Type-Options:
/// nosniff` that today's `413` does not carry.
///
/// `pub(super)` (issue #170): the detected_labels/detected_fields POST
/// handlers (`detected.rs`) reuse the same form-decode core.
pub(super) async fn read_form_pairs(
    headers: &HeaderMap,
    raw_query: Option<&str>,
    body: Body,
) -> Result<Vec<(String, String)>, Response> {
    let raw_content_type = headers.get(header::CONTENT_TYPE);
    // An ABSENT header reads as `""`, which is what Go's `Header.Get`
    // returns for one; `parsePostForm` then substitutes
    // `application/octet-stream`. A present-but-not-UTF-8 value is
    // malformed, not absent.
    let content_type = match raw_content_type {
        Some(value) => value.to_str().ok(),
        None => Some(""),
    };
    let Some(content_type) = content_type else {
        let lossy =
            String::from_utf8_lossy(raw_content_type.map(|v| v.as_bytes()).unwrap_or_default())
                .into_owned();
        return Err(ApiError::Param(ParamError::MalformedContentType(lossy)).into_response());
    };
    // Decided from the header alone. Nothing below this line polls `body`
    // unless the answer is `Parse`.
    let disposition = params::form_body_disposition(Some(content_type))
        .map_err(|e| ApiError::from(e).into_response())?;

    let mut pairs = match disposition {
        params::FormBody::Parse => {
            let bytes = read_capped_form_body(body).await?;
            let text = std::str::from_utf8(&bytes)
                .map_err(|_| ApiError::Param(ParamError::InvalidFormBody).into_response())?;
            params::parse_pairs(text)
        }
        // Dropped without a single poll: the upload is never awaited, and
        // the response is written while the client may still be sending.
        params::FormBody::Ignore => {
            drop(body);
            Vec::new()
        }
    };
    pairs.extend(params::parse_pairs(raw_query.unwrap_or("")));
    Ok(pairs)
}

/// Buffers a form body under the same bound, and with the same rejection
/// bytes, that the `Bytes` extractor applied before issue #406 moved the
/// content-type check ahead of the body.
///
/// `Bytes::from_request` is called rather than reimplemented precisely so
/// the `413` stays axum's own — status, `text/plain; charset=utf-8` and
/// the `Failed to buffer the request body: length limit exceeded` text
/// alike (captured from a running server at `60670cc` before the change).
/// The synthetic [`Request`] carries no extensions, so axum applies its
/// 2 MiB `DefaultBodyLimit` default; **no `logs_api` route layers a
/// `DefaultBodyLimit` of its own** (`git grep DefaultBodyLimit` finds only
/// `pulsus-write`'s ingest note), and one added later would have to bound
/// this call itself rather than the extractor.
async fn read_capped_form_body(body: Body) -> Result<Bytes, Response> {
    Bytes::from_request(Request::new(body), &())
        .await
        .map_err(|rejection| rejection.into_response())
}

/// The reference's series groups (`ParseSeriesQuery`,
/// `pkg/loghttp/series.go:23-38` @ v3.7.4 `b318f2829f0ae2094ab3a1e90780450e9e4b03be`),
/// issue #406 Part A: `match` and `match[]` are BOTH read, through the
/// repeated seam (`r.Form[...]`), unioned, sorted and deduped; a set of
/// exactly one element that is `{}` after removing every ASCII space
/// collapses to empty, which is legal and means "every series in the
/// window" (`MatchForSeriesRequest(nil)` returns no error —
/// `pkg/logql/matchers.go:13-26`).
///
/// Container-measured against `grafana/loki:3.7.4`, 2026-08-10: no
/// `match[]` → `200` with all series; `?match[]={}` and `?match[]={ }` →
/// `200` with all series; **`?match[]={}&match[]={app="a"}` → `400`**
/// (`0 matchers in group: {}` — the collapse needs `len == 1`, so the
/// plausible `retain(|g| g != "{}")` spelling would wrongly turn this into
/// a scoped `200`); `?match[]=` → `400`, unchanged, because the repeated
/// seam does not collapse empty into absent (issue #391).
///
/// Space stripping is ASCII space only (`strings.ReplaceAll(matcher, " ",
/// "")`): `{ }` collapses, a tab does not — so no `trim`/`is_whitespace`.
fn series_groups(pairs: &[(String, String)]) -> Vec<&str> {
    let mut groups: Vec<&str> = params::get_all(pairs, "match");
    groups.extend(params::get_all(pairs, "match[]"));
    groups.sort_unstable();
    groups.dedup();
    if groups.len() == 1 && groups[0].replace(' ', "") == "{}" {
        groups.clear();
    }
    groups
}

// ---------------------------------------------------------------------
// GET|POST /api/logs/v1/query_range
// ---------------------------------------------------------------------

pub(crate) async fn query_range(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let pairs = params::parse_pairs(raw.as_deref().unwrap_or(""));
    match query_range_impl(state, &headers, pairs).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

/// `POST /api/logs/v1/query_range`: same param names as GET, as an
/// `application/x-www-form-urlencoded` body (task-manager ratification on
/// issue #13 amendment 3 finding 2 — large queries/long ranges can exceed
/// URL length limits; mainstream Loki-datasource clients POST this
/// endpoint).
pub(crate) async fn query_range_post(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
    body: Body,
) -> Response {
    match read_form_pairs(&headers, raw.as_deref(), body).await {
        Ok(pairs) => match query_range_impl(state, &headers, pairs).await {
            Ok(res) => res,
            Err(e) => e.into_response(),
        },
        Err(response) => response,
    }
}

async fn query_range_impl(
    state: AppState,
    headers: &HeaderMap,
    pairs: Vec<(String, String)>,
) -> Result<Response, ApiError> {
    let query = params::get(&pairs, "query").ok_or(ParamError::MissingQuery)?;
    let expr = super::parse_logql(query)?;
    let (start_ns, end_ns) = parse_bounds_ordered(&pairs)?;
    let step_ns = params::parse_step(params::get(&pairs, "step"), start_ns, end_ns)?;
    // Issue #227: Loki's `(end-start)/step > 11000` resolution limit — a hard
    // 400 at request parsing with Loki's exact message (the engine keeps its
    // `MetricBuckets` guard as a defense-in-depth backstop).
    params::ensure_range_resolution(start_ns, end_ns, step_ns)?;
    let limit = params::parse_limit(params::get(&pairs, "limit"))?;
    let direction = params::parse_direction(params::get(&pairs, "direction"))?;
    let query_params = QueryParams {
        spec: QuerySpec::Range {
            start_ns,
            end_ns,
            step_ns,
        },
        limit,
        direction,
    };

    let engine = engine_for(&state).await?;
    run_query(
        &engine,
        &expr,
        &query_params,
        wants_explain(headers),
        end_ns,
    )
    .await
}

// ---------------------------------------------------------------------
// GET|POST /api/logs/v1/query
// ---------------------------------------------------------------------

pub(crate) async fn query(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let pairs = params::parse_pairs(raw.as_deref().unwrap_or(""));
    match query_impl(state, &headers, pairs).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

/// `POST /api/logs/v1/query`: same param names as GET, form-encoded (same
/// rationale as `query_range_post`).
pub(crate) async fn query_post(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
    body: Body,
) -> Response {
    match read_form_pairs(&headers, raw.as_deref(), body).await {
        Ok(pairs) => match query_impl(state, &headers, pairs).await {
            Ok(res) => res,
            Err(e) => e.into_response(),
        },
        Err(response) => response,
    }
}

async fn query_impl(
    state: AppState,
    headers: &HeaderMap,
    pairs: Vec<(String, String)>,
) -> Result<Response, ApiError> {
    let query = params::get(&pairs, "query").ok_or(ParamError::MissingQuery)?;
    let expr = super::parse_logql(query)?;
    let at_ns = match params::get(&pairs, "time") {
        Some(v) => params::parse_ts(v)?,
        None => params::now_ns(),
    };
    let limit = params::parse_limit(params::get(&pairs, "limit"))?;
    let direction = params::parse_direction(params::get(&pairs, "direction"))?;
    let query_params = QueryParams {
        spec: QuerySpec::Instant { at_ns },
        limit,
        direction,
    };

    let engine = engine_for(&state).await?;
    run_query(&engine, &expr, &query_params, wants_explain(headers), at_ns).await
}

/// Shared success path for `query`/`query_range`: run with or without the
/// explain side channel (single execution either way — see
/// `LogQlEngine::query_explained`'s doc comment), then encode.
async fn run_query(
    engine: &LogQlEngine,
    expr: &Expr,
    query_params: &QueryParams,
    explain: bool,
    at_ns: i64,
) -> Result<Response, ApiError> {
    let preserve_vector_order = preserve_vector_order(&query_params.spec, expr);
    if explain {
        // Issue #277: `explain` lives INSIDE `data`, `warnings` outside
        // it — the two are not siblings, so both must be threaded
        // through and the encoder places each on its own side of
        // `data`'s closing brace.
        let (result, warnings, plan_explain) = engine.query_explained(expr, query_params).await?;
        Ok(encode::query_response_warned(
            result,
            Some(plan_explain),
            at_ns,
            preserve_vector_order,
            &warnings,
        ))
    } else {
        let (result, warnings) = engine.query(expr, query_params).await?;
        Ok(encode::query_response_warned(
            result,
            None,
            at_ns,
            preserve_vector_order,
            &warnings,
        ))
    }
}

/// The encoder's order gate (issue #406 R2). `true` suppresses
/// `encode::query_response`'s default label re-sort so the engine's
/// sequence reaches the client.
///
/// Two conjuncts, and both are load-bearing:
///
/// * the result must be an instant VECTOR. A range query yields a matrix,
///   which has no per-series value order to preserve and keeps its
///   deterministic label sort — the same shape the reference's own gate
///   has, where `Sortable` is consulted only under
///   `GetRangeType(q.params) == InstantType`
///   (`pkg/logql/engine.go:551-570 @ grafana/loki v3.7.4
///   b318f2829f0ae2094ab3a1e90780450e9e4b03be`);
/// * the order must actually come from a `sort`/`sort_desc` — the whole
///   question [`pulsus_read::logql::sorted_order_reaches_the_wire`]
///   answers. Before #406 this asked a ROOT-ONLY predicate, so
///   `label_replace(sort(…), …)`, `sort(…) * 1` and a vector binary
///   operand all had the order the engine had already computed thrown
///   away by the re-sort.
fn preserve_vector_order(spec: &QuerySpec, expr: &Expr) -> bool {
    matches!(spec, QuerySpec::Instant { .. })
        && pulsus_read::logql::sorted_order_reaches_the_wire(expr)
}

// ---------------------------------------------------------------------
// GET|POST /api/logs/v1/labels
// ---------------------------------------------------------------------

pub(crate) async fn labels_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let pairs = params::parse_pairs(raw.as_deref().unwrap_or(""));
    match labels_impl(state, &headers, pairs).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

pub(crate) async fn labels_post(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
    body: Body,
) -> Response {
    match read_form_pairs(&headers, raw.as_deref(), body).await {
        Ok(pairs) => match labels_impl(state, &headers, pairs).await {
            Ok(res) => res,
            Err(e) => e.into_response(),
        },
        Err(response) => response,
    }
}

async fn labels_impl(
    state: AppState,
    headers: &HeaderMap,
    pairs: Vec<(String, String)>,
) -> Result<Response, ApiError> {
    let (start_ns, end_ns) = parse_bounds_ordered(&pairs)?;
    let bounds = TimeBounds { start_ns, end_ns };
    let engine = engine_for(&state).await?;
    if wants_explain(headers) {
        let (names, explain) = engine.label_names_explained(bounds).await?;
        Ok(encode::string_array_response(names, Some(explain)))
    } else {
        let names = engine.label_names(bounds).await?;
        Ok(encode::string_array_response(names, None))
    }
}

// ---------------------------------------------------------------------
// GET|POST /api/logs/v1/label/{name}/values
// ---------------------------------------------------------------------

pub(crate) async fn label_values(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let pairs = params::parse_pairs(raw.as_deref().unwrap_or(""));
    match label_values_impl(state, &name, &headers, pairs).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

/// `POST /api/logs/v1/label/{name}/values` (issue #406 Part B2, folded in
/// by task-manager ruling): the reference registers this route
/// `Methods("GET","POST")` like the other three
/// (`pkg/loki/modules.go:687`, `:1365` @ v3.7.4 `b318f282`) and answers a
/// form POST `200` where we answered `405` — measured 2026-08-10.
///
/// Extractor order matters: `Path` before the `Bytes` body extractor,
/// which must stay last.
pub(crate) async fn label_values_post(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
    body: Body,
) -> Response {
    match read_form_pairs(&headers, raw.as_deref(), body).await {
        Ok(pairs) => match label_values_impl(state, &name, &headers, pairs).await {
            Ok(res) => res,
            Err(e) => e.into_response(),
        },
        Err(response) => response,
    }
}

async fn label_values_impl(
    state: AppState,
    name: &str,
    headers: &HeaderMap,
    pairs: Vec<(String, String)>,
) -> Result<Response, ApiError> {
    let (start_ns, end_ns) = parse_bounds_ordered(&pairs)?;
    let bounds = TimeBounds { start_ns, end_ns };
    let engine = engine_for(&state).await?;
    if wants_explain(headers) {
        let (values, explain) = engine.label_values_explained(name, bounds).await?;
        Ok(encode::string_array_response(values, Some(explain)))
    } else {
        let values = engine.label_values(name, bounds).await?;
        Ok(encode::string_array_response(values, None))
    }
}

// ---------------------------------------------------------------------
// GET|POST /api/logs/v1/series
// ---------------------------------------------------------------------

pub(crate) async fn series_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Response {
    let pairs = params::parse_pairs(raw.as_deref().unwrap_or(""));
    match series_impl(state, &headers, pairs).await {
        Ok(res) => res,
        Err(e) => e.into_response(),
    }
}

pub(crate) async fn series_post(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
    body: Body,
) -> Response {
    match read_form_pairs(&headers, raw.as_deref(), body).await {
        Ok(pairs) => match series_impl(state, &headers, pairs).await {
            Ok(res) => res,
            Err(e) => e.into_response(),
        },
        Err(response) => response,
    }
}

async fn series_impl(
    state: AppState,
    headers: &HeaderMap,
    pairs: Vec<(String, String)>,
) -> Result<Response, ApiError> {
    // Issue #406 Part A: an EMPTY group set is legal and means "every
    // series active in the window" — the reference's
    // `MatchForSeriesRequest(nil)` returns no error
    // (`pkg/logql/matchers.go:13-26 @ v3.7.4 b318f282`), and it is reached
    // both by sending no `match[]` at all and by sending a lone `{}`.
    let groups = series_groups(&pairs);
    let mut selectors = Vec::with_capacity(groups.len());
    for m in groups {
        let selector = pulsus_logql::parse_selector(m)?;
        selectors.push(Expr::Log(LogExpr {
            selector,
            pipeline: Vec::new(),
        }));
    }
    let (start_ns, end_ns) = parse_bounds_ordered(&pairs)?;
    let bounds = TimeBounds { start_ns, end_ns };
    let engine = engine_for(&state).await?;
    if wants_explain(headers) {
        let (data, explain) = engine.series_explained(&selectors, bounds).await?;
        Ok(encode::json_array_response(data, Some(explain)))
    } else {
        let data = engine.series(&selectors, bounds).await?;
        Ok(encode::json_array_response(data, None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::body::to_bytes;
    use axum::http::StatusCode;
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

    /// Issue #406 R2, AC 6: the encoder's order gate reads the WHOLE AST
    /// and fires only at instant. Before this change the predicate was
    /// root-only, so all six of these were `false` in both columns and
    /// the six wrapped sorts reached the client label-sorted — measured
    /// against the digest-pinned reference, 20/20 on both stores.
    #[test]
    fn preserve_vector_order_reads_the_whole_ast_and_only_at_instant() {
        const X: &str = r#"sum by (svc) (count_over_time({app="x"}[5m]))"#;
        let instant = QuerySpec::Instant { at_ns: 1_000 };
        let range = QuerySpec::Range {
            start_ns: 0,
            end_ns: 1_000,
            step_ns: 100,
        };
        for query in [
            format!(r#"label_replace(sort({X}), "tag", "$1", "svc", "(.*)")"#),
            format!(r#"label_replace(sort_desc({X}), "tag", "$1", "svc", "(.*)")"#),
            format!("sort({X}) * 1"),
            format!("sort_desc({X}) * 1"),
            format!("sort({X}) + on(svc) ({X} * 0)"),
            format!("sort_desc({X}) + on(svc) ({X} * 0)"),
        ] {
            let expr = pulsus_logql::parse(&query).expect("parses");
            assert!(preserve_vector_order(&instant, &expr), "instant: {query}");
            assert!(
                !preserve_vector_order(&range, &expr),
                "a range query yields a matrix and keeps its label sort: {query}"
            );
        }
        // The negative the whole design turns on: a vector aggregation
        // over a sort would put our own `HashMap` walk on the wire.
        let nested = pulsus_logql::parse(&format!("sum by (svc) (sort({X}))")).expect("parses");
        assert!(!preserve_vector_order(&instant, &nested));
        assert!(!preserve_vector_order(&range, &nested));
    }

    /// `(status, body text)`. Issue #264: every error on this surface is a
    /// bare `text/plain` body, so tests assert the raw message; success
    /// bodies are still JSON and those cases parse the returned string.
    ///
    /// On any 4xx/5xx this also asserts the reference's error container —
    /// `Content-Type: text/plain; charset=utf-8` + `X-Content-Type-Options:
    /// nosniff`, non-empty body (`pkg/util/server/error.go:48-51 @
    /// v3.7.4`) — so every error case in this module covers the wire
    /// shape, not just its status code.
    async fn status_and_body(res: Response) -> (StatusCode, String) {
        let status = res.status();
        if status.is_client_error() || status.is_server_error() {
            assert_eq!(
                res.headers()
                    .get(axum::http::header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok()),
                Some("text/plain; charset=utf-8"),
            );
            assert_eq!(
                res.headers()
                    .get(axum::http::header::X_CONTENT_TYPE_OPTIONS)
                    .and_then(|v| v.to_str().ok()),
                Some("nosniff"),
            );
        }
        let bytes = to_bytes(res.into_body(), usize::MAX).await.expect("body");
        let text = String::from_utf8(bytes.to_vec()).expect("utf-8 body");
        if status.is_client_error() || status.is_server_error() {
            assert!(!text.is_empty(), "an error body is never empty");
        }
        (status, text)
    }

    #[tokio::test]
    async fn query_range_without_a_pool_is_503_unavailable() {
        let res = query_range(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some(r#"query={app="x"}"#.to_string())),
        )
        .await;
        let (status, _body) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// Issue #279: a syntactically valid selector of exactly 131,072
    /// bytes — `MAX_QUERY_BYTES`, the reference's `maxInputSize`
    /// (grafana/loki v3.7.4 `pkg/logql/syntax/parser.go:42`), one byte
    /// past the longest accepted query. Valid syntax so the only possible
    /// rejection is the cap itself.
    fn oversized_query() -> String {
        format!(r#"{{app="{}"}}"#, "a".repeat(131_072 - 8))
    }

    /// Issue #279: the over-cap rejection — 400 carrying the reference's
    /// verbatim message (its own `len > cap` rendering of a `>=`
    /// comparison) and nothing else (#264: no envelope, no `position`).
    fn assert_query_too_long(status: StatusCode, body: &str) {
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, "input size too long (131072 > 131072)");
    }

    /// Issue #279 (AC4): `/query_range` rejects an over-cap query 400
    /// against a POOLLESS state, while a valid query on the same state is
    /// 503 — proving the parse (and so the cap) precedes the pool check,
    /// so the 400 is the cap and not an artefact.
    #[tokio::test]
    async fn query_range_rejects_an_over_cap_query_400_before_the_pool_check() {
        let res = query_range(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some(format!("query={}", oversized_query()))),
        )
        .await;
        let (status, body) = status_and_body(res).await;
        assert_query_too_long(status, &body);
        // The valid-query half lives in
        // `query_range_without_a_pool_is_503_unavailable` above.
    }

    /// Issue #279 (AC4): `/query` — over-cap 400 vs valid 503, poolless.
    #[tokio::test]
    async fn query_rejects_an_over_cap_query_400_before_the_pool_check() {
        let res = query(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some(format!("query={}", oversized_query()))),
        )
        .await;
        let (status, body) = status_and_body(res).await;
        assert_query_too_long(status, &body);

        let res = query(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some(r#"query={app="x"}"#.to_string())),
        )
        .await;
        let (status, _body) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// Issue #279 (AC4): `/series` — an over-cap `match[]` value is 400
    /// (the cap applies per matcher string, matching the reference's
    /// one-input-at-a-time `ParseMatchers`) vs valid 503, poolless.
    #[tokio::test]
    async fn series_rejects_an_over_cap_match_value_400_before_the_pool_check() {
        let res = series_get(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some(format!("match[]={}", oversized_query()))),
        )
        .await;
        let (status, body) = status_and_body(res).await;
        assert_query_too_long(status, &body);

        let res = series_get(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some(r#"match[]={app="x"}"#.to_string())),
        )
        .await;
        let (status, _body) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// Issue #221: an over-cap **leafless** `vector(n)` range query is guarded
    /// at the engine's materialization boundary (no leaf ever runs, so it
    /// bypasses `ClientAggState::new`'s cap) and returns the same
    /// `QueryTooBroad(MetricBuckets)` a leaf over-cap range query trips —
    /// cap-checked BEFORE allocating any grid, with no DB round-trip. This
    /// drives the REAL over-cap-vector error (`materialize_vector_lit`, the
    /// exact function `run_metric_node` calls for a `VectorLit`) through the
    /// handler's `ReadError` → response conversion (the same
    /// `From<ReadError> for ApiError` + `IntoResponse` path `query_range` uses
    /// via `?`), asserting it surfaces end-to-end as **422**
    /// with the 11000-bucket-cap message — identical to any other over-cap
    /// LogQL range query.
    ///
    /// Driving the `query_range` HTTP entrypoint itself to the engine requires
    /// a live `ChPool` (there is no hermetic `ChPool`/`ChClient`/`LogQlEngine`
    /// constructor — every path pings a real endpoint), and the LogQL handler
    /// deliberately has NO param-layer cap pre-check (plan v4/v5: it would
    /// diverge the leaf error semantics 422→400). So the hermetic ceiling is
    /// this composed proof: the real leafless-vector error + the handler's
    /// real error-mapping. The window matches the `QuerySpec::Range` →
    /// evaluation-window mapping `query_range_impl` builds (`start_ns`/
    /// `end_ns`/`step_ns`), so it is the exact error an over-cap
    /// `query_range` would carry.
    #[tokio::test]
    async fn over_cap_leafless_vector_range_maps_to_422() {
        const S: i64 = 1_000_000_000; // 1s
        // 11_001 step INTERVALS over `[0, 11001s]` at a 1s step > the 11000
        // cap (issue #227 review round 7, finding 1: exactly 11_000
        // intervals — 11_001 grid points — is SERVED, matching the
        // reference's `(end-start)/step > 11000` rule).
        let window = pulsus_read::logql::GridWindow {
            start_ns: 0,
            end_ns: 11_001 * S,
            step_ns: Some(
                pulsus_read::logql::validate_duration_ns(S as u64, "step").expect("valid step"),
            ),
        };
        let err = pulsus_read::logql::materialize_vector_lit(0.0, &window)
            .expect_err("an over-cap vector(n) range query must reject");
        let (status, body) = status_and_body(ApiError::Read(err).into_response()).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(
            body.contains("11000"),
            "over-cap message must name the 11000-bucket cap: {body}"
        );
    }

    /// Issue #221 (AC 9/10): `QueryTooBroad(VariantSubStates)` maps to the
    /// same 422 surface as every other too-broad LogQL
    /// query, via the real `ReadError` → `ApiError` → response path
    /// `query_range` uses.
    ///
    /// **Issue #236 changed how this is driven, not what it asserts.**
    /// The test used to build a 501-variant query, because the DERIVED
    /// backstop was then 500. #236 deleted `AggCaps::series`, moving
    /// `min_field()` onto `MAX_TS_COLLISION_GROUP` = 10 000 — and at
    /// #279's `MAX_QUERY_BYTES` the largest expressible variants query
    /// carries 4 368 variants, so the backstop can no longer be reached
    /// through a parse. The reason is asserted here by CONSTRUCTION
    /// instead, which is what this test was ever really about (the
    /// mapping, not the guard); the guard's own arithmetic and its
    /// unreachability verdict are pinned in
    /// `plan::tests::variants_past_the_derived_backstop_reject_at_plan_time`.
    #[tokio::test]
    async fn variants_past_the_sub_state_backstop_maps_to_422() {
        let err = pulsus_read::logql::ReadError::QueryTooBroad(
            pulsus_read::logql::TooBroadReason::VariantSubStates {
                count: 10_001,
                cap: 10_000,
            },
        );
        let (status, body) = status_and_body(ApiError::Read(err).into_response()).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(
            body.contains("10001 variants, exceeding the 10000-variant cap"),
            "the message names the derived cap: {body}"
        );
    }

    /// Issue #227: at the REQUEST boundary, `(end-start)/step > 11000` is
    /// Loki's HTTP **400** with its exact message (the engine's
    /// `MetricBuckets` 422 above is now only a defense-in-depth backstop).
    #[tokio::test]
    async fn query_range_over_the_11000_resolution_is_400_with_lokis_message() {
        // (0, 11001s] at a 1s step = 11001 intervals > 11000.
        let q = "query=count_over_time(%7Bapp%3D%22a%22%7D%5B5m%5D)\
                 &start=0&end=11001000000000&step=1s";
        let res = query_range(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some(q.to_string())),
        )
        .await;
        let (status, body) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body,
            "exceeded maximum resolution of 11,000 points per time series. Try increasing the \
             value of the step parameter"
        );
    }

    /// Issue #227 review round 7, finding 1: EXACTLY `(end-start)/step ==
    /// 11000` is inside the reference's fence (`> 11000` rejects), so the
    /// request guard must admit it — the request proceeds past parameter
    /// parsing (hermetically that surfaces as the engine-pool 503, never
    /// the resolution 400).
    #[tokio::test]
    async fn query_range_at_exactly_the_11000_resolution_passes_the_request_guard() {
        // (end - start) / step == 11000 exactly (0 → 11000s at a 1s step).
        let q = "query=count_over_time(%7Bapp%3D%22a%22%7D%5B5m%5D)\
                 &start=0&end=11000000000000&step=1s";
        let res = query_range(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some(q.to_string())),
        )
        .await;
        let (status, body) = status_and_body(res).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "an exactly-at-the-limit request must pass the resolution guard \
             (got {body})"
        );
    }

    /// Issue #227 review round 8 (end-to-end): the WIDEST admissible
    /// request at a 1_000_000s step must pass the resolution guard and
    /// proceed past parameter parsing (hermetically the engine-pool 503,
    /// never a resolution 400).
    ///
    /// **Round 8's actual subject — the reference's SATURATING span
    /// subtraction (Go `time.Time.Sub`), which counts 9_223 intervals for
    /// a full-i64 span instead of the true 18_446 — is no longer reachable
    /// from the wire.** This case used to send `start=i64::MIN,
    /// end=i64::MAX`; issue #343's 5-year query-span cap now refuses that
    /// with a 400 before the resolution guard sees it, and no admissible
    /// span can exceed an int64 duration any more, so nothing can
    /// saturate. Said rather than deleted: the saturation behaviour itself
    /// is still pinned where it lives, by
    /// `pulsus_read::logql::window`'s
    /// `grid_resolution_fence_saturates_the_span_like_the_reference`,
    /// which calls `fence_intervals(i64::MIN, i64::MAX, …)` directly and
    /// is untouched by an HTTP-layer cap. What remains here is the
    /// end-to-end half: the widest request a client can send clears
    /// parameter parsing and the resolution guard.
    #[tokio::test]
    async fn query_range_across_the_widest_admissible_span_passes_the_request_guard() {
        let q = format!(
            "query=count_over_time(%7Bapp%3D%22a%22%7D%5B5m%5D)\
             &start={}&end={}&step=1000000s",
            0,
            pulsus_logql::MAX_QUERY_SPAN_NS
        );
        let res = query_range(State(test_state()), HeaderMap::new(), RawQuery(Some(q))).await;
        let (status, body) = status_and_body(res).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "the widest admissible request must pass the resolution \
             guard (got {body})"
        );
    }

    /// Issue #227 review round 10 (the reported case, verbatim):
    /// `query=vector(1)&start=0&end=0&step=3153600000s` — a 100-year step —
    /// fits the reference's positive `time.Duration` and passes its
    /// resolution fence, so the reference serves it. The request must clear
    /// parameter parsing and the resolution guard (hermetically the
    /// engine-pool 503, never a 400); the engine leg of the same case —
    /// validator + grid guard — is pinned by the agreement table above and
    /// the read-crate's `a_100_year_step_the_reference_serves_is_served`.
    #[tokio::test]
    async fn query_range_with_a_100_year_step_passes_the_request_guard() {
        let q = "query=vector(1)&start=0&end=0&step=3153600000s";
        let res = query_range(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some(q.to_string())),
        )
        .await;
        let (status, body) = status_and_body(res).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "a 100-year step the reference serves must pass request parsing \
             (got {body})"
        );
    }

    /// Issue #227 review round 7, finding 1 (extended in round 8 to the
    /// extreme timestamp domain): the HTTP request guard
    /// (`ensure_range_resolution`) and the engine's grid guard
    /// (`ensure_grid_resolution`, driven here through the public
    /// `materialize_vector_lit` — the SAME function `RangeSlideState::new`
    /// funnels through) implement the identical Loki fence — including its
    /// SATURATING span subtraction (Go `time.Time.Sub` clamps at
    /// `maxDuration = 1<<63-1` ns) — so the engine can never 422 a request
    /// the guard admitted, across the WHOLE i64 domain. Enumerates both
    /// sides of the fence: aligned, unaligned, and saturated.
    #[tokio::test]
    async fn engine_grid_guard_agrees_with_the_request_guard_at_every_fence_case() {
        const S: i64 = 1_000_000_000; // 1s step
        const BIG: u64 = 1_000_000_000_000_000; // 1_000_000s step
        let step = S as u64;
        // floor(i64::MAX/11_001): the largest step whose SATURATED
        // full-domain span counts 11_001 intervals (reject); +1ns admits.
        let sat_reject = (i64::MAX / 11_001) as u64;
        // (start, end, step, expect-admitted)
        let cases: &[(i64, i64, u64, bool)] = &[
            (0, 10_999 * S, step, true),         // one interval under
            (0, 11_000 * S, step, true),         // exactly at the fence
            (0, 11_001 * S, step, false),        // one interval over
            (0, 11_000 * S + S / 2, step, true), // step does not divide the span
            (0, 11_001 * S - 1, step, true),     // 1ns under the rejecting interval
            (7, 7 + 11_000 * S, step, true),     // unaligned start, at the fence
            (7, 7 + 11_001 * S, step, false),    // unaligned start, over
            // Round 8: the saturated extreme — the true 2^64-1 ns span
            // counts 18_446 intervals, but the reference's saturated span
            // (i64::MAX ns) counts 9_223 → SERVED.
            (i64::MIN, i64::MAX, BIG, true),
            (i64::MIN, i64::MAX, sat_reject, false), // 11_001 saturated
            (i64::MIN, i64::MAX, sat_reject + 1, true), // 11_000 saturated
            (i64::MIN, i64::MAX, 1, false),          // 1ns step, far over
            (-1, i64::MAX - 1, BIG, true),           // saturation onset, exact
            // Round 10: the widened duration domain — the reference accepts
            // ANY positive int64-ns step. A 100-year step (the reported
            // reject-a-served-request case) and the largest representable
            // step must both pass BOTH guards (the retired `i64::MAX / 4`
            // validator cap panicked this table's engine leg on them).
            (0, 0, 3_153_600_000_000_000_000, true), // 100y (3153600000s)
            (0, 0, i64::MAX as u64, true),           // Go's maximum Duration
            (i64::MIN, i64::MAX, i64::MAX as u64, true), // full domain, max step
        ];
        for &(start_ns, end_ns, step_ns, admitted) in cases {
            let guard_ok = params::ensure_range_resolution(start_ns, end_ns, step_ns).is_ok();
            assert_eq!(
                guard_ok, admitted,
                "request guard disagrees with the reference at \
                 ({start_ns}, {end_ns}, {step_ns})"
            );
            let window = pulsus_read::logql::GridWindow {
                start_ns,
                end_ns,
                step_ns: Some(
                    pulsus_read::logql::validate_duration_ns(step_ns, "step").expect("valid step"),
                ),
            };
            let engine_ok = pulsus_read::logql::materialize_vector_lit(0.0, &window).is_ok();
            assert_eq!(
                engine_ok, guard_ok,
                "engine grid guard disagrees with the request guard at \
                 ({start_ns}, {end_ns}, {step_ns})"
            );
        }
    }

    #[tokio::test]
    async fn query_range_missing_query_param_is_400() {
        let res = query_range(State(test_state()), HeaderMap::new(), RawQuery(None)).await;
        let (status, _body) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn query_range_malformed_logql_is_400_with_the_byte_offset_in_the_message() {
        let res = query_range(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some("query=%7B".to_string())), // "{" — unterminated selector
        )
        .await;
        let (status, body) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("at byte"), "{body}");
    }

    #[tokio::test]
    async fn query_range_limit_above_the_cap_is_400() {
        let res = query_range(
            State(test_state()),
            HeaderMap::new(),
            RawQuery(Some(r#"query={app="x"}&limit=5001"#.to_string())),
        )
        .await;
        let (status, _body) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    /// **Issue #406 inverted this test.** It used to assert that
    /// `application/json` was a `400`; the reference serves it, reading
    /// the request from the URL query and never opening the body. Its old
    /// body was a `400` for TWO reasons at once — the content type, and
    /// (once the body is ignored) a missing `query` — so simply deleting
    /// the header assertion would have left it green against the defect.
    /// Hence the URL query below: it makes the request otherwise VALID,
    /// so the only thing the status can be measuring is the header.
    #[tokio::test]
    async fn query_range_post_serves_a_non_form_content_type_from_the_url() {
        for content_type in ["application/json", "text/plain", "garbage"] {
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, content_type.parse().unwrap());
            let res = query_range_post(
                State(test_state()),
                headers,
                RawQuery(Some(r#"query={app="x"}"#.to_string())),
                // Would be a 400 if it were read: no `query` and a
                // `since` that does not parse.
                Body::from("since=bogus"),
            )
            .await;
            let (status, body) = status_and_body(res).await;
            assert_eq!(
                status,
                StatusCode::SERVICE_UNAVAILABLE,
                "Content-Type {content_type:?} must reach the engine with its body unread \
                 (poolless: 503), not be refused: {body}"
            );
        }
    }

    /// The other half: a `Content-Type` that cannot be PARSED is still a
    /// `400`, on an otherwise valid request, and for the header's own
    /// reason. The reference refuses these too — `mime.ParseMediaType`
    /// errors, `ParseForm` propagates, and the middleware answers `400`
    /// before any handler runs.
    #[tokio::test]
    async fn query_range_post_refuses_a_malformed_content_type() {
        for content_type in ["application/", "application/json/x", ";"] {
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, content_type.parse().unwrap());
            let res = query_range_post(
                State(test_state()),
                headers,
                RawQuery(Some(r#"query={app="x"}"#.to_string())),
                Body::from(""),
            )
            .await;
            let (status, body) = status_and_body(res).await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "Content-Type {content_type:?} must be refused: {body}"
            );
            assert!(
                body.contains("malformed 'Content-Type' header"),
                "Content-Type {content_type:?} must be refused for the HEADER: {body}"
            );
        }
    }

    #[tokio::test]
    async fn query_range_post_without_a_pool_is_503_once_the_form_is_valid() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "application/x-www-form-urlencoded".parse().unwrap(),
        );
        let body = Body::from("query=%7Bapp%3D%22x%22%7D");
        let res = query_range_post(State(test_state()), headers, RawQuery(None), body).await;
        let (status, _body) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn query_post_missing_query_param_is_400() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "application/x-www-form-urlencoded".parse().unwrap(),
        );
        let res = query_post(State(test_state()), headers, RawQuery(None), Body::from("")).await;
        let (status, _body) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn query_post_without_a_pool_is_503_once_the_form_is_valid() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "application/x-www-form-urlencoded".parse().unwrap(),
        );
        let body = Body::from("query=%7Bapp%3D%22x%22%7D");
        let res = query_post(State(test_state()), headers, RawQuery(None), body).await;
        let (status, _body) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// Issue #406 Part A: no `match[]` reaches the engine (`503` against a
    /// poolless state) instead of the old `400 missing required parameter
    /// 'match[]'`. Container-measured on `grafana/loki:3.7.4` 2026-08-10:
    /// `200` with every series in the window.
    #[tokio::test]
    async fn series_without_any_match_param_reaches_the_engine() {
        let res = series_get(State(test_state()), HeaderMap::new(), RawQuery(None)).await;
        let (status, body) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        assert!(!body.contains("missing required parameter"), "{body}");
    }

    /// **Issue #406 Part A's discriminating test**, over the reference's
    /// group collection itself (`ParseSeriesQuery`,
    /// `pkg/loghttp/series.go:23-38` @ v3.7.4 `b318f282`). Every row is
    /// the reference's measured answer.
    ///
    /// Two wrong spellings die here and nowhere else:
    ///
    /// * `groups.retain(|g| g.replace(' ', "") != "{}")` satisfies every
    ///   single-value row and turns the reference's `400` on
    ///   `{}`-plus-a-real-selector into a scoped `200`. The
    ///   `["{}", "{app=\"a\"}"]` row is what fails it — the collapse runs
    ///   only when the deduped set has exactly ONE element.
    /// * collapsing `match[]=` alongside `{}` would undo #391's
    ///   repeated-seam asymmetry: an empty value is a value there, and a
    ///   `400`, while an absent one is a `200`.
    #[test]
    fn series_group_collection_matches_the_reference_rules() {
        let groups = |raw: &str| -> Vec<String> {
            let pairs = params::parse_pairs(raw);
            series_groups(&pairs)
                .into_iter()
                .map(str::to_string)
                .collect()
        };
        // ref: no `match[]` at all -> 200, all series.
        assert!(groups("").is_empty());
        // ref: `?match[]={}` -> 200, all series (the lone-`{}` collapse).
        assert!(groups("match%5B%5D=%7B%7D").is_empty());
        // ref: `?match[]={ }` -> 200 — ASCII spaces are stripped.
        assert!(groups("match%5B%5D=%7B%20%7D").is_empty());
        // A tab is NOT stripped (`strings.ReplaceAll(matcher, " ", "")`).
        assert_eq!(groups("match%5B%5D=%7B%09%7D"), vec!["{\t}".to_string()]);
        // ref: `?match[]={}&match[]={app="a"}` -> 400 `0 matchers in group: {}`.
        // Sorted byte-wise, so `{app="a"}` precedes `{}`.
        assert_eq!(
            groups("match%5B%5D=%7B%7D&match%5B%5D=%7Bapp%3D%22a%22%7D"),
            vec![r#"{app="a"}"#.to_string(), "{}".to_string()]
        );
        // ref: `?match={app="a"}` -> 200, one series (the unbracketed
        // alias, read through the same repeated seam).
        assert_eq!(
            groups("match=%7Bapp%3D%22a%22%7D"),
            vec![r#"{app="a"}"#.to_string()]
        );
        // ref: the two spellings union and DEDUPE.
        assert_eq!(
            groups("match=%7Bapp%3D%22a%22%7D&match%5B%5D=%7Bapp%3D%22a%22%7D"),
            vec![r#"{app="a"}"#.to_string()]
        );
        // ref: `?match[]=X&match[]=X` -> 200, deduped.
        assert_eq!(
            groups("match%5B%5D=%7Ba%7D&match%5B%5D=%7Ba%7D"),
            vec!["{a}".to_string()]
        );
        // ref: `?match[]=` -> 400 parse error. The repeated seam does not
        // collapse empty into absent (#391), so this is ONE group `""`.
        assert_eq!(groups("match%5B%5D="), vec![String::new()]);
        // …and the union is sorted, so `""` sorts first.
        assert_eq!(
            groups("match%5B%5D=&match%5B%5D=%7Ba%7D"),
            vec![String::new(), "{a}".to_string()]
        );
    }

    /// Issue #406 Part C: `since` supplies the `start` default and is
    /// ignored the moment `start` is present (`determineBounds`,
    /// `pkg/loghttp/params.go:91-119` @ v3.7.4 `b318f282`).
    #[test]
    fn bounds_take_since_only_when_start_is_absent() {
        let end_ns = params::now_ns() - 60_000_000_000; // in the past, so no clamp
        let bounds = |raw: &str| parse_bounds(&params::parse_pairs(raw)).expect("bounds");

        let (start, end) = bounds(&format!("end={end_ns}&since=30m"));
        assert_eq!(end, end_ns);
        assert_eq!(start, end_ns - 1_800_000_000_000);

        let (start, _end) = bounds(&format!("end={end_ns}"));
        assert_eq!(start, end_ns - params::DEFAULT_SINCE_NS, "the 1h default");

        let explicit = end_ns - 12_345_000_000_000;
        let (start, _end) = bounds(&format!("start={explicit}&end={end_ns}&since=30m"));
        assert_eq!(
            start, explicit,
            "`since` is ignored when `start` is present"
        );

        let err = parse_bounds(&params::parse_pairs("since=bogus")).expect_err("400");
        assert!(matches!(err, ApiError::Param(ParamError::InvalidSince(_))));
    }

    /// Issue #406 Part C, the `endOrNow` clamp — the one row that changes
    /// behaviour for requests carrying no `since` at all. Measured on
    /// `grafana/loki:3.7.4` 2026-08-10: `end = now + 2h` with no `start`
    /// answers from `now - 1h`, returning all 150 seeded entries; the same
    /// request with `since=10m` answers from `now - 10m` and returns none.
    /// Pre-change PulsusDB defaulted `start` to `end - 1h`, i.e. one hour
    /// into the FUTURE, and answered empty.
    #[test]
    fn a_future_end_does_not_push_the_default_start_into_the_future() {
        let now = params::now_ns();
        let end_ns = now + 7_200_000_000_000; // now + 2h
        let (start, end) =
            parse_bounds(&params::parse_pairs(&format!("end={end_ns}"))).expect("bounds");
        assert_eq!(end, end_ns);
        let expected = now - params::DEFAULT_SINCE_NS;
        assert!(
            (start - expected).abs() < 5_000_000_000,
            "start must be `now - 1h` ({expected}), not `end - 1h` ({}); got {start}",
            end_ns - params::DEFAULT_SINCE_NS
        );
        assert!(
            start < now,
            "the default start must never be in the future; got {start} vs now {now}"
        );
    }

    /// `/series`' half of the same inversion (issue #406). Note that a
    /// bodyless `/series` is no longer a `400` at all — Part A made an
    /// absent `match[]` mean "every series in the window" — so this one
    /// would have gone green on the defect for a second reason too. The
    /// URL selector and the poisoned body keep the two axes apart.
    #[tokio::test]
    async fn series_post_serves_a_non_form_content_type_from_the_url() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        let res = series_post(
            State(test_state()),
            headers,
            RawQuery(Some(r#"match[]={app="x"}"#.to_string())),
            Body::from("since=bogus"),
        )
        .await;
        let (status, body) = status_and_body(res).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "a JSON content type must reach the engine with its body unread: {body}"
        );
    }

    #[tokio::test]
    async fn series_post_without_a_pool_is_503_once_the_form_is_valid() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "application/x-www-form-urlencoded".parse().unwrap(),
        );
        let body = Body::from("match%5B%5D=%7Bapp%3D%22x%22%7D");
        let res = series_post(State(test_state()), headers, RawQuery(None), body).await;
        let (status, _body) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn label_values_without_a_pool_is_503_unavailable() {
        let res = label_values(
            State(test_state()),
            Path("env".to_string()),
            HeaderMap::new(),
            RawQuery(None),
        )
        .await;
        let (status, _body) = status_and_body(res).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn query_instant_missing_query_param_is_400() {
        let res = query(State(test_state()), HeaderMap::new(), RawQuery(None)).await;
        let (status, _body) = status_and_body(res).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}
