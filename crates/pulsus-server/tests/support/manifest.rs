//! The single source of truth for issue #36's exhaustive API conformance
//! matrix: every mounted route, its HTTP method matrix, its mode/flag
//! gate, its documented-vs-planned status, and a representative set of
//! invalid-param cases with the exact `(status, errorType)` (or, for the
//! non-JSON ingest families, the exact protobuf/plain-text error shape)
//! the architect plan pins.
//!
//! Shared by both test binaries via `#[path = "support/manifest.rs"] mod
//! manifest;` (a `tests/` subdirectory, so cargo never builds this file as
//! its own test binary — matches the architect plan's file layout):
//! `route_inventory.rs` compares [`route_manifest`]'s `Mounted` set against
//! a hermetic source-scan of the router modules and checks the docs-gap
//! (no ClickHouse, `ci` job); `api_conformance.rs` expands
//! [`route_manifest`] × case-classes into live HTTP requests against a
//! spawned `pulsusdb` (`schema-it` job).
//!
//! This module is intentionally hand-authored data, not derived from the
//! router at runtime (axum has no route-introspection API, per the
//! architect plan) — `route_inventory.rs`'s source scan is what keeps it
//! from silently drifting out of sync with `src/**`.

#![allow(dead_code)]

// ---------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------

/// An HTTP method as it appears in a `.route(path, get(...).post(...))`
/// chain. Deliberately a closed set matching this codebase's router style
/// (`get`/`post`/`put`/`delete`/`patch`) — the drift guard hard-fails on
/// any other verb it finds in source (see `route_inventory.rs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Method {
    Get,
    Post,
    Put,
    Delete,
    Patch,
}

impl Method {
    pub fn as_str(self) -> &'static str {
        match self {
            Method::Get => "GET",
            Method::Post => "POST",
            Method::Put => "PUT",
            Method::Delete => "DELETE",
            Method::Patch => "PATCH",
        }
    }

    /// Maps a router-chain verb identifier (`"get"`, `"post"`, ...) to its
    /// [`Method`] — the drift guard's method-chain parser uses this;
    /// `None` on anything else (a signal to hard-fail, not to skip).
    pub fn from_chain_ident(ident: &str) -> Option<Method> {
        match ident {
            "get" => Some(Method::Get),
            "post" => Some(Method::Post),
            "put" => Some(Method::Put),
            "delete" => Some(Method::Delete),
            "patch" => Some(Method::Patch),
            _ => None,
        }
    }
}

/// Which handler family a route belongs to — drives which envelope/
/// content-type assertions `api_conformance.rs` applies to its success and
/// error responses (v2/v3/v4 plan deltas: OTLP protobuf, remote-write
/// empty/plain-text, JSON query envelopes are three structurally distinct
/// families, never assumed to share one shape).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Surface {
    /// `/ready`, `/metrics` — always unauthenticated, never gated by mode.
    OpsPublic,
    /// `/config`, `/buildinfo` — authed (when configured), never gated by
    /// mode.
    OpsAuthed,
    /// `/v1/logs`, `/v1/metrics`, `/v1/traces` (OTLP protobuf),
    /// `/api/v1/write` (remote-write) — two structurally distinct envelope
    /// families (three OTLP `Export*ServiceResponse` types + remote-write's
    /// empty-body shape); `api_conformance.rs` dispatches on the concrete
    /// path, not just this variant.
    Ingest,
    /// `/api/logs/v1/*` and its `/loki/api/v1/*` alias — LogQL JSON query
    /// envelope (`{"status","errorType","error","position"?}`).
    LogsQuery,
    /// `GET /api/logs/v1/tail` and its `/loki/api/v1/tail` alias (issue
    /// #74) — a WebSocket route. The generic matrix cannot drive it (a
    /// full upgrade handshake never fits `Connection: close` HTTP), so
    /// it is special-cased at the dispatch site (`assert_tail_route`)
    /// exactly as `Surface::Ingest`/`TracesFetch` are. Mounting oracle,
    /// empirically pinned by
    /// `logs_api/tail.rs::bare_get_without_upgrade_headers_is_the_pinned_rejection_status`:
    /// a bare GET (no upgrade headers) returns axum's `400` plain-text
    /// `WebSocketUpgrade` rejection ("Connection header did not include
    /// 'upgrade'") — an unmounted path returns the EMPTY 404 instead.
    /// `success_status` carries that pinned 400. Param/shape errors are
    /// the LogsQuery JSON envelope, asserted with real upgrade headers
    /// in `assert_tail_route`; slot exhaustion is `429
    /// too_many_requests` (its own dedicated spawn).
    LogsTail,
    /// `GET /api/logs/v1/stats` and its `/loki/api/v1/index/stats` alias
    /// (issue #74, docs/api.md §2.5) — success is the bare
    /// `{"streams","chunks","entries","bytes"}` object (no status/data
    /// envelope); against this suite's empty databases every field is 0
    /// — the mounting oracle. Errors are the LogsQuery JSON envelope
    /// (`position` present exactly on LogQL parse errors).
    LogsStats,
    /// `/api/v1/*` — the Prometheus HTTP API JSON query envelope
    /// (`{"status","errorType","error"}`, no `position`).
    PromApi,
    /// `/api/traces/v1/trace/{traceId}[/json]` (issue #55) — success is an
    /// OTLP `TracesData` body (JSON or protobuf by `Accept`); errors are
    /// always the JSON envelope (`{"status","errorType","error"}`, no
    /// `position`). Against the conformance suite's empty databases the
    /// "success" of a well-formed request is the mounted-but-absent `404
    /// not_found` envelope — which doubles as the mounting oracle (an
    /// unmounted path returns axum's *empty* 404 instead). Asserted by the
    /// dedicated `assert_traces_fetch_route` (the generic matrix's
    /// sibling-404 suffix trick would mutate the trailing `{traceId}` param
    /// into a 400, not an unrouted 404).
    TracesFetch,
    /// `GET /api/traces/v1/search` (issue #57) — success is the
    /// documented docs/api.md §4.2 envelope
    /// (`{"traces":[...],"metrics":{"partial","limit","returned"}}`), not
    /// the `{"status","data"}` query envelope; against this suite's empty
    /// databases a well-formed request returns the empty envelope 200,
    /// which doubles as the mounting oracle. Errors are the JSON envelope
    /// with `position` present exactly on TraceQL parse errors.
    TracesSearch,
    /// `GET /api/traces/v1/metrics/{query_range,query}` (issue #59,
    /// docs/api.md §4.4) — success is the Prometheus query envelope
    /// (`{"status":"success","data":{"resultType","result"}}`), shared
    /// byte-for-byte with `prom_api` via its `encode::query_response`.
    /// Against this suite's empty databases a well-formed request is the
    /// mounting oracle: `query_range` → 200 with `resultType:"matrix"`,
    /// `result:[]`; `query` → 200 with `resultType:"vector"` and exactly
    /// one label-less sample of value `"0"` (a `uniqExact` with no
    /// `GROUP BY` always returns one row). Errors are the JSON envelope
    /// with `position` present exactly on TraceQL parse errors; the
    /// static point-cap rejection is a 422 `query_too_broad` (the
    /// adjudicated bounded-response contract).
    TracesMetrics,
    /// `GET /api/traces/v1/tags` and `/api/traces/v1/tag/{tag}/values`
    /// (issue #58, docs/api.md §4.3) — success is the documented native
    /// envelope (`{"scopes":[...],"truncated":...}` /
    /// `{"tagValues":[...],"truncated":...}`); against this suite's empty
    /// databases a well-formed request returns the empty envelope 200 —
    /// the mounting oracle. The values route's `base_query` carries a
    /// NON-TRIVIAL `q=` on purpose: `q` is adjudicated accept-and-ignore
    /// (superset semantics), so the cell proves a 200, never a 400.
    /// Unlike `TracesFetch`, the middle `{tag}` param with a trailing
    /// static `/values` is compatible with the generic matrix (the
    /// sibling-404 suffix yields a genuinely unrouted path). Errors are
    /// the JSON envelope, never with `position`.
    TracesTags,
    /// The T9 v2 Tempo tag-discovery aliases `/api/v2/search/tags` and
    /// `/api/v2/search/tag/{tag}/values` (issue #61, docs/api.md §8.1) —
    /// reshaping, not pure bindings: the native §4.3 scoped/typed shapes
    /// MINUS the PulsusDB-only top-level `truncated` field (Tempo's v2
    /// wire has no equivalent). Against this suite's empty databases a
    /// well-formed request returns the empty envelope 200
    /// (`{"scopes":[]}` / `{"tagValues":[]}`) with NO `truncated` key —
    /// the mounting oracle. Errors are native-identical (shared param
    /// parsing), never with `position`. The seeded non-empty wire-shape
    /// proof lives in `traces_tags_live.rs`.
    TracesTagsV2,
    /// The T9 v1 Tempo tag-discovery aliases `/api/search/tags` and
    /// `/api/search/tag/{tag}/values` (issue #61, docs/api.md §8.1) —
    /// Tempo's legacy flat shapes: `{"tagNames":[<bare strings>]}` /
    /// `{"tagValues":[<bare strings>]}` (scope, value types, and
    /// `truncated` all projected away server-side). Empty-DB mounting
    /// oracle: the flat empty envelope 200, with neither a `scopes` nor
    /// a `truncated` key. Errors native-identical, never with
    /// `position`. Seeded proof in `traces_tags_live.rs`.
    TracesTagsV1,
    /// `GET /api/echo` (issue #61) — Tempo's constant liveness echo:
    /// `200` with the exact body `echo` (`text/plain; charset=utf-8`,
    /// axum's `&'static str` response), no I/O, no pool. The body text
    /// is the mounting oracle; the route has no error cases.
    Echo,
}

/// Mode/flag gating (issue #36 plan v2 finding 3): mirrors
/// `pulsus_server::modes::mounted`'s subsystem matrix plus the compat
/// flag, so the live matrix can assert exact 404s in writer-only/
/// reader-only spawns and compat-flag-off spawns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gate {
    /// Ops routes — mounted in every mode, flag-independent.
    Always,
    /// Mounted only when the Reader subsystem is mounted (`all`, `reader`).
    ReaderMode,
    /// Mounted only when the Writer subsystem is mounted (`all`, `writer`).
    WriterMode,
    /// Mounted only when `PULSUS_COMPAT_ENDPOINTS=true` **and** the Reader
    /// subsystem is mounted.
    CompatAndReader,
    /// Mounted only when `PULSUS_COMPAT_ENDPOINTS=true` **and** the Writer
    /// subsystem is mounted (issue #77 — the first writer-side compat gate,
    /// `compat.rs::apply_aliases`'s writer branch). Distinct from
    /// [`Gate::CompatAndReader`]: the flag alone never mounts a
    /// `CompatAndWriter` route without the Writer role.
    CompatAndWriter,
}

/// Whether a manifest entry is actually mounted today (drift-guarded,
/// live-tested) or merely documented for a future milestone (excluded from
/// both — plan v1/v4: "Planned entries don't fail the guard").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteStatus {
    Mounted,
    Planned { milestone: &'static str },
}

/// The `/loki/api/v1` §8.1 alias table's prefix — a per-table constant
/// (plan v4 finding 2: pinned explicitly, never derived by stripping a
/// segment off the table's first entry, which breaks for irregular rows
/// like the M7 `/loki/api/v1/index/volume` one).
pub const LOKI_V1: &str = "/loki/api/v1";

/// How the docs-gap check (`route_inventory.rs`) proves a `Mounted`
/// route's path is documented in `docs/api.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocRef {
    /// `path` must appear verbatim somewhere in `docs/api.md` (every
    /// non-§8 route: §1-§7 always spell the full path out, so a plain
    /// substring search is correct and was not among the review findings
    /// — only §8's abbreviated table rows needed special handling).
    Verbatim,
    /// The §8.1 compat-alias table row: documented either as the full
    /// reconstructed path (`LOKI_V1` + `suffix`, the row's first entry) or
    /// as a backtick-quoted bare `suffix` (the row's subsequent shorthand
    /// entries) — scoped to that one row's own line, never a whole-
    /// document search (plan v2/v3 review findings: recurring segments
    /// like `values`/`series` appear in *other* §8 rows too).
    LokiAliasSuffix { suffix: &'static str },
    /// `Planned` surfaces only: excluded from the docs-gap hard-fail
    /// entirely (plan v1/v4: "Planned entries don't fail the guard") — the
    /// M4-M8 compat/receiver/traces/profiles/rules tables use enough
    /// row-specific shorthand (§8.2's Zipkin/Datadog/Elastic/InfluxDB
    /// receivers, §4-§6's per-signal param tables) that reconstructing
    /// every row generically is out of scope for a check whose only AC is
    /// "every **mounted** route is documented" (drift guard) — a `Planned`
    /// row's own doc text is reviewed the ordinary way, by the architect/
    /// reviewer loop that ships its issue, not by this guard.
    Skip,
}

// ---------------------------------------------------------------------
// Case classes (the "invalid-param classes" column of the matrix)
// ---------------------------------------------------------------------

/// One live HTTP request under construction: `api_conformance.rs` seeds
/// `method`/`path` from the owning [`RouteSpec`], then a [`CaseClass`]'s
/// `build` fn mutates whatever it needs (query string, headers, body,
/// even the method, e.g. the ingest routes' single documented `POST`
/// still needs `req.method` settable for symmetry with future multi-
/// method cases).
#[derive(Debug, Clone)]
pub struct Req {
    pub method: &'static str,
    pub path: String,
    /// Raw query string, no leading `?`; empty = none.
    pub query: String,
    pub headers: Vec<(&'static str, String)>,
    pub content_type: Option<&'static str>,
    pub body: Vec<u8>,
}

impl Req {
    pub fn new(method: &'static str, path: impl Into<String>) -> Self {
        Req {
            method,
            path: path.into(),
            query: String::new(),
            headers: Vec::new(),
            content_type: None,
            body: Vec::new(),
        }
    }
}

/// The exact error shape a [`CaseClass`] expects — one variant per
/// response family (plan v3/v4 deltas: OTLP `google.rpc.Status` protobuf,
/// remote-write plain text, and the two JSON query envelopes are never
/// conflated).
#[derive(Debug, Clone, Copy)]
pub enum ExpectedError {
    /// `logs_api`/`prom_api`'s `{"status":"error","errorType",...}`
    /// envelope. `has_position` pins whether `position` is present (only
    /// ever true for `logs_api` LogQL parse errors — `prom_api`'s envelope
    /// never carries the field at all).
    Json {
        error_type: &'static str,
        has_position: bool,
    },
    /// The ingest handlers' hand-rolled `google.rpc.Status { code, message }`
    /// protobuf error body (OTLP `/v1/logs`, `/v1/metrics`, `/v1/traces`).
    Otlp { code: i32 },
    /// `/api/v1/write`'s plain-text error body (`text/plain; charset=utf-8`,
    /// non-empty).
    PlainText,
}

#[derive(Debug, Clone, Copy)]
pub struct CaseClass {
    pub name: &'static str,
    pub build: fn(&mut Req),
    pub expect_status: u16,
    pub expect: ExpectedError,
}

// -- case builders -----------------------------------------------------

/// Minimal percent-encoding for the handful of characters this module's
/// case builders actually embed in a query string (`{`, `}`, `"`, `=`,
/// `~`, space) — not a general-purpose encoder (mirrors the server's own
/// `percent_decode`'s "just enough" scope).
fn enc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn logs_missing_query(req: &mut Req) {
    req.query.clear();
}

fn logs_malformed_logql(req: &mut Req) {
    // "{" — unterminated selector (mirrors `logs_api/handlers.rs`'s own
    // unit test fixture for this exact shape).
    req.query = "query=%7B".to_string();
}

fn logs_limit_over_cap(req: &mut Req) {
    req.query = format!("query={}&limit=5001", enc(r#"{service_name="x"}"#));
}

fn logs_wrong_content_type(req: &mut Req) {
    req.method = "POST";
    req.content_type = Some("application/json");
    req.body = b"{}".to_vec();
}

fn logs_series_missing_match(req: &mut Req) {
    req.query.clear();
}

const LOGS_QUERY_LIKE_CASES: &[CaseClass] = &[
    CaseClass {
        name: "missing_query",
        build: logs_missing_query,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
    CaseClass {
        name: "malformed_logql",
        build: logs_malformed_logql,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: true,
        },
    },
    CaseClass {
        name: "limit_over_cap",
        build: logs_limit_over_cap,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
    CaseClass {
        name: "wrong_content_type",
        build: logs_wrong_content_type,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
];

const LOGS_LABELS_CASES: &[CaseClass] = &[CaseClass {
    name: "wrong_content_type",
    build: logs_wrong_content_type,
    expect_status: 400,
    expect: ExpectedError::Json {
        error_type: "bad_data",
        has_position: false,
    },
}];

// -- logs stats (issue #74, docs/api.md §2.5) ---------------------------

fn logs_stats_metric_query(req: &mut Req) {
    // A metric query has no stream statistics — explicit 400.
    req.query = format!(
        "query={}",
        enc(r#"count_over_time({service_name="checkout"}[1h])"#)
    );
}

fn logs_stats_parser_pipeline(req: &mut Req) {
    // Parsers/formats/label filters have no pushdown aggregation shape —
    // explicit 400, never a silent over-count.
    req.query = format!("query={}", enc(r#"{service_name="checkout"} | logfmt"#));
}

const LOGS_STATS_CASES: &[CaseClass] = &[
    CaseClass {
        name: "missing_query",
        build: logs_missing_query,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
    CaseClass {
        name: "malformed_logql",
        build: logs_malformed_logql,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: true,
        },
    },
    CaseClass {
        name: "metric_query_rejected",
        build: logs_stats_metric_query,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
    CaseClass {
        name: "parser_pipeline_rejected",
        build: logs_stats_parser_pipeline,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
];

const LOGS_SERIES_CASES: &[CaseClass] = &[
    CaseClass {
        name: "missing_match",
        build: logs_series_missing_match,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
    CaseClass {
        name: "wrong_content_type",
        build: logs_wrong_content_type,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
];

fn prom_missing_query(req: &mut Req) {
    req.query.clear();
}

fn prom_malformed_promql(req: &mut Req) {
    req.query = "query=up%7B".to_string();
}

fn prom_missing_start(req: &mut Req) {
    req.query = "query=up&end=100&step=10".to_string();
}

fn prom_points_cap_exceeded(req: &mut Req) {
    // `parse_time` reads a plain integer as unix **seconds**; one point
    // past the 11,000-point cap (mirrors `prom_api/handlers.rs`'s own unit
    // test fixture for this exact shape).
    req.query = "query=up&start=0&end=11000&step=1".to_string();
}

fn prom_wrong_content_type(req: &mut Req) {
    req.method = "POST";
    req.content_type = Some("application/json");
    req.body = b"{}".to_vec();
}

fn prom_series_missing_match(req: &mut Req) {
    req.query.clear();
}

/// `422 execution`: a non-vector-selector `match[]` value — the remaining
/// out-of-subset-construct limitation of the discovery surface
/// (`pulsus_promql::series_selector`'s "match[] selector must be a bare
/// vector selector" -> `PromqlError::Unsupported`, via
/// `prom_api/handlers.rs::parse_match_selectors`), reachable **without**
/// any pool/seed data (fails before `engine_for` is ever called) — stands
/// in for the "over-broad/unsupported selector" case-class the issue body
/// asks for on the metrics surface (see `api_conformance.rs`'s module doc
/// for why the LogQL `query_too_broad` analog is not live-tested here).
///
/// Retargeted from `match[]={__name__=~"up.*"}` by issue #89: regex/
/// negated `__name__` discovery is supported, and its runtime outcome is
/// label-cache-state-dependent (warm -> 200, and since issue #96 a
/// degraded/cold cache -> 200 too, via the bounded `metric_series` probe
/// fallback — only the fan-out-cap breach is a named `422`), which is not a
/// hermetically assertable conformance pin — that behavior is covered by
/// the seeded `prom_api_live.rs` cases and `pulsus-read`'s
/// `live_discovery_fallback.rs` instead. `sum(up)` keeps this case-class
/// deterministic and pool-independent. Note an `or`-matcher selector would
/// NOT do: it is `PromqlError::Parse` -> `400 bad_data`, a different
/// case-class.
fn prom_series_non_selector_unsupported(req: &mut Req) {
    req.query = format!("match%5B%5D={}", enc("sum(up)"));
}

const PROM_QUERY_CASES: &[CaseClass] = &[
    CaseClass {
        name: "missing_query",
        build: prom_missing_query,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
    CaseClass {
        name: "malformed_promql",
        build: prom_malformed_promql,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
    CaseClass {
        name: "wrong_content_type",
        build: prom_wrong_content_type,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
];

const PROM_QUERY_RANGE_CASES: &[CaseClass] = &[
    CaseClass {
        name: "missing_query",
        build: prom_missing_query,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
    CaseClass {
        name: "missing_start",
        build: prom_missing_start,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
    CaseClass {
        name: "points_cap_exceeded",
        build: prom_points_cap_exceeded,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
    CaseClass {
        name: "wrong_content_type",
        build: prom_wrong_content_type,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
];

const PROM_LABELS_CASES: &[CaseClass] = &[
    CaseClass {
        name: "non_selector_unsupported",
        build: prom_series_non_selector_unsupported,
        expect_status: 422,
        expect: ExpectedError::Json {
            error_type: "execution",
            has_position: false,
        },
    },
    CaseClass {
        name: "wrong_content_type",
        build: prom_wrong_content_type,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
];

const PROM_SERIES_CASES: &[CaseClass] = &[
    CaseClass {
        name: "missing_match",
        build: prom_series_missing_match,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
    CaseClass {
        name: "non_selector_unsupported",
        build: prom_series_non_selector_unsupported,
        expect_status: 422,
        expect: ExpectedError::Json {
            error_type: "execution",
            has_position: false,
        },
    },
    CaseClass {
        name: "wrong_content_type",
        build: prom_wrong_content_type,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
];

fn otlp_malformed_body(req: &mut Req) {
    req.body = b"not a protobuf message".to_vec();
}

fn otlp_wrong_content_encoding(req: &mut Req) {
    req.headers.push(("content-encoding", "br".to_string()));
}

// OTLP content negotiation (`Content-Type`) is **not** a `CaseClass` here:
// the two negotiation cells need a signal-specific valid body
// (`ExportLogsServiceRequest` vs `…Metrics…` vs `…Trace…`) which a single
// shared `CaseClass::build` fn can't express (it has no way to know which of
// `/v1/logs`/`/v1/metrics`/`/v1/traces` it is building for). They are modeled
// as direct assertions in `api_conformance.rs::assert_ingest_route` instead.
//
// History: #36 (round 7 adjudication,
// https://github.com/digitalis-io/pulsusdb/issues/36#issuecomment-4978613793)
// pinned the then-reality that the handlers did NOT inspect `Content-Type` —
// a valid protobuf body labeled `application/json` still returned `200` — and
// explicitly DEFERRED the "reject-or-ignore under application/json" question
// to a follow-up. Issue #76 IS that follow-up: `ingest/http.rs` now forks on
// `Content-Type`, so `application/json` selects the OTLP/JSON decode path.
// `assert_ingest_route` was flipped in the same change to pin the new reality —
// (a) valid OTLP/JSON body + `application/json` → success; (b) protobuf body +
// `application/json` → 400/code 3. The `otlp_undecodable_with_correct_content_type`
// case below is unaffected (it sends the documented `application/x-protobuf`).

/// Case-class 6's "undecodable body" cell, with the *correct* documented
/// `Content-Type: application/x-protobuf` header set (unlike
/// `malformed_body`, which sends no header at all) — proves the header
/// alone never saves a genuinely undecodable body.
fn otlp_undecodable_with_correct_content_type(req: &mut Req) {
    req.content_type = Some("application/x-protobuf");
    req.body = b"not a protobuf message".to_vec();
}

const OTLP_INGEST_CASES: &[CaseClass] = &[
    CaseClass {
        name: "malformed_body",
        build: otlp_malformed_body,
        expect_status: 400,
        expect: ExpectedError::Otlp { code: 3 },
    },
    CaseClass {
        name: "unsupported_content_encoding",
        build: otlp_wrong_content_encoding,
        expect_status: 400,
        expect: ExpectedError::Otlp { code: 3 },
    },
    CaseClass {
        name: "undecodable_body_correct_content_type",
        build: otlp_undecodable_with_correct_content_type,
        expect_status: 400,
        expect: ExpectedError::Otlp { code: 3 },
    },
];

/// A valid-protobuf `/v1/traces` body, ~1 MiB on the wire, whose resource ×
/// span fan-out estimates past `otlp_traces::MAX_EXPANDED_BYTES` (each
/// span's stored payload re-carries the whole 1 MiB resource) — the
/// parser's expansion budget must reject it wholesale as the structural
/// 400/code-3 class (issue #54 code-review [high] fix). Traces-specific by
/// construction, hence in [`OTLP_TRACES_INGEST_CASES`] only.
fn otlp_traces_expansion_budget_body(req: &mut Req) {
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
    use prost::Message;

    let resource = Resource {
        attributes: vec![KeyValue {
            key: "big.attr".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue("v".repeat(1024 * 1024))),
            }),
            key_strindex: 0,
        }],
        dropped_attributes_count: 0,
        entity_refs: vec![],
    };
    let span_count = pulsus_write::protocols::otlp_traces::MAX_EXPANDED_BYTES / (1024 * 1024) + 2;
    let body = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(resource),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: (0..span_count)
                    .map(|_| Span {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        name: "conformance-fan-out".to_string(),
                        start_time_unix_nano: 1_700_000_000_000_000_000,
                        end_time_unix_nano: 1_700_000_001_000_000_000,
                        ..Default::default()
                    })
                    .collect(),
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    req.content_type = Some("application/x-protobuf");
    req.body = body.encode_to_vec();
}

/// [`OTLP_INGEST_CASES`] ⊕ the traces-only expansion-budget cell (a
/// `const`-slice concat is not expressible on stable, so the three shared
/// entries are repeated verbatim). Only `/v1/traces` references this list —
/// the logs/metrics parsers have no expansion budget (their outputs are
/// ~1:1 with wire records; see issue #54's implementation notes for the
/// metrics-path follow-up).
const OTLP_TRACES_INGEST_CASES: &[CaseClass] = &[
    CaseClass {
        name: "malformed_body",
        build: otlp_malformed_body,
        expect_status: 400,
        expect: ExpectedError::Otlp { code: 3 },
    },
    CaseClass {
        name: "unsupported_content_encoding",
        build: otlp_wrong_content_encoding,
        expect_status: 400,
        expect: ExpectedError::Otlp { code: 3 },
    },
    CaseClass {
        name: "undecodable_body_correct_content_type",
        build: otlp_undecodable_with_correct_content_type,
        expect_status: 400,
        expect: ExpectedError::Otlp { code: 3 },
    },
    CaseClass {
        name: "expansion_budget_exceeded",
        build: otlp_traces_expansion_budget_body,
        expect_status: 400,
        expect: ExpectedError::Otlp { code: 3 },
    },
];

/// The valid-but-absent 32-hex trace id every `{traceId}` placeholder
/// resolves to (issue #55 verification finding: the old 8-char `abcd1234`
/// placeholder is invalid under the 16-or-32-hex rule and would 400 on the
/// now-mounted route instead of exercising the absent-trace 404). No test
/// database ever ingests this id; the malformed case below mutates it back
/// out of the resolved path.
pub const ABSENT_TRACE_ID: &str = "feedfacefeedfacefeedfacefeedface";

/// Mutates the resolved `{traceId}` into a same-length non-hex id — still
/// one path segment, so it routes to the same handler and must be rejected
/// as `400 bad_data` (not the absent-trace 404).
fn traces_malformed_hex(req: &mut Req) {
    req.path = req
        .path
        .replace(ABSENT_TRACE_ID, "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz");
}

const TRACE_FETCH_CASES: &[CaseClass] = &[CaseClass {
    name: "malformed_hex",
    build: traces_malformed_hex,
    expect_status: 400,
    expect: ExpectedError::Json {
        error_type: "bad_data",
        has_position: false,
    },
}];

// -- traces search (issue #57, docs/api.md §4.2) -----------------------

/// The base query every search case mutates from: match-all `q={}` over
/// a fixed 1h window (both required params present).
pub const TRACES_SEARCH_BASE_QUERY: &str = "q=%7B%7D&start=1700000000&end=1700003600";

fn traces_search_malformed_q(req: &mut Req) {
    // "{" — unterminated spanset: a positioned TraceQL parse error.
    req.query = "q=%7B&start=1700000000&end=1700003600".to_string();
}

fn traces_search_bad_start(req: &mut Req) {
    req.query = "q=%7B%7D&start=notanumber&end=1700003600".to_string();
}

fn traces_search_bad_limit(req: &mut Req) {
    req.query = format!("{TRACES_SEARCH_BASE_QUERY}&limit=abc");
}

fn traces_search_bad_spss(req: &mut Req) {
    req.query = format!("{TRACES_SEARCH_BASE_QUERY}&spss=abc");
}

fn traces_search_malformed_tags(req: &mut Req) {
    // A bare key with no `=` violates the documented logfmt grammar.
    req.query = "tags=barekey&start=1700000000&end=1700003600".to_string();
}

fn traces_search_q_plus_legacy(req: &mut Req) {
    // `q` and the legacy params are mutually exclusive — explicit 400,
    // never silent precedence (task-manager ratification on plan v2).
    req.query = format!("{TRACES_SEARCH_BASE_QUERY}&tags=a%3Db");
}

const TRACES_SEARCH_CASES: &[CaseClass] = &[
    CaseClass {
        name: "malformed_q",
        build: traces_search_malformed_q,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: true,
        },
    },
    CaseClass {
        name: "bad_start",
        build: traces_search_bad_start,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
    CaseClass {
        name: "bad_limit",
        build: traces_search_bad_limit,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
    CaseClass {
        name: "bad_spss",
        build: traces_search_bad_spss,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
    CaseClass {
        name: "malformed_tags_logfmt",
        build: traces_search_malformed_tags,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            // Strict logfmt errors carry a byte offset into the decoded
            // `tags` value (code review round 1).
            has_position: true,
        },
    },
    CaseClass {
        name: "q_plus_legacy_conflict",
        build: traces_search_q_plus_legacy,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
];

// -- traces metrics (issue #59, docs/api.md §4.4) -----------------------

/// The base query every metrics case mutates from: match-all
/// `q={} | rate()` over a fixed, step-ALIGNED 1h window. Deliberately
/// starts at 1700000100, not the other surfaces' 1700000000: the ingest
/// matrix (which runs earlier in the manifest walk) stores a fixture
/// span at exactly t=1700000000, and metrics' outward epoch snap +
/// left-closed `>=` bound (docs/api.md §4.4) would pull a 1700000000
/// start back over it — the empty-envelope mounting oracle needs a
/// window with no stored spans.
pub const TRACES_METRICS_BASE_QUERY: &str =
    "q=%7B%7D%20%7C%20rate()&start=1700000100&end=1700003700&step=60";

fn traces_metrics_malformed_q(req: &mut Req) {
    // "{" — unterminated spanset: a positioned TraceQL parse error.
    req.query = "q=%7B&start=1700000000&end=1700003600".to_string();
}

fn traces_metrics_missing_range(req: &mut Req) {
    req.query = "q=%7B%7D%20%7C%20rate()".to_string();
}

fn traces_metrics_bad_step(req: &mut Req) {
    // Fractional-second steps violate the whole-second contract.
    req.query = "q=%7B%7D%20%7C%20rate()&start=1700000000&end=1700003600&step=500ms".to_string();
}

fn traces_metrics_missing_stage(req: &mut Req) {
    // A plain search query on the metrics surface: parses, but the
    // metrics planner requires exactly one metric stage.
    req.query = "q=%7B%7D&start=1700000000&end=1700003600".to_string();
}

fn traces_metrics_point_cap(req: &mut Req) {
    // 1,000,000 one-second buckets >> MAX_METRICS_POINTS (11,000): the
    // adjudicated static pre-execution 422, never a truncation.
    req.query = "q=%7B%7D%20%7C%20rate()&start=1700000000&end=1701000000&step=1".to_string();
}

const TRACES_METRICS_CASES: &[CaseClass] = &[
    CaseClass {
        name: "malformed_q",
        build: traces_metrics_malformed_q,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: true,
        },
    },
    CaseClass {
        name: "missing_range",
        build: traces_metrics_missing_range,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
    CaseClass {
        name: "bad_step",
        build: traces_metrics_bad_step,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
    CaseClass {
        name: "missing_metric_stage",
        build: traces_metrics_missing_stage,
        expect_status: 400,
        expect: ExpectedError::Json {
            error_type: "bad_data",
            has_position: false,
        },
    },
    CaseClass {
        name: "point_cap_static_422",
        build: traces_metrics_point_cap,
        expect_status: 422,
        expect: ExpectedError::Json {
            error_type: "query_too_broad",
            has_position: false,
        },
    },
];

// -- traces tags (issue #58, docs/api.md §4.3) --------------------------

/// The non-trivial `q=` the values route's `base_query` carries —
/// `{span.x="y"}`, URL-encoded. Accept-and-ignore (adjudication 1 on
/// issue #58): the cell asserts a 200 empty envelope, proving `q` never
/// 400s; the seeded superset-equivalence assertion lives in
/// `traces_tags_live.rs`.
pub const TRACES_TAG_VALUES_BASE_QUERY: &str = "q=%7Bspan.x%3D%22y%22%7D";

fn traces_tags_bogus_scope(req: &mut Req) {
    // `scope` ∈ {resource, span, absent}; anything else is an explicit
    // 400, never silently widened to "all scopes" (adjudication 4).
    req.query = "scope=bogus".to_string();
}

const TRACES_TAGS_CASES: &[CaseClass] = &[CaseClass {
    name: "unsupported_scope",
    build: traces_tags_bogus_scope,
    expect_status: 400,
    expect: ExpectedError::Json {
        error_type: "bad_data",
        has_position: false,
    },
}];

fn traces_tag_values_empty_key(req: &mut Req) {
    // `resource.` — a scope prefix with an empty key: still one path
    // segment, routes to the same handler, rejected as 400.
    req.path = req.path.replace("service.name", "resource.");
    req.query.clear();
}

const TRACES_TAG_VALUES_CASES: &[CaseClass] = &[CaseClass {
    name: "empty_key",
    build: traces_tag_values_empty_key,
    expect_status: 400,
    expect: ExpectedError::Json {
        error_type: "bad_data",
        has_position: false,
    },
}];

fn rw_bad_snappy(req: &mut Req) {
    req.body = b"\xFF\xFF\xFF not snappy".to_vec();
}

const REMOTE_WRITE_CASES: &[CaseClass] = &[CaseClass {
    name: "bad_snappy",
    build: rw_bad_snappy,
    expect_status: 400,
    expect: ExpectedError::PlainText,
}];

// -- Loki push (issue #77, docs/api.md §8.2) ----------------------------
// Oracle-pinned against grafana/loki:3.4.2 (provenance +
// probe matrix: `crates/pulsus-write/tests/fixtures/loki-push/README.md`):
// success is 204 empty both encodings; malformed / undecodable / unsupported
// Content-Type all default to the protobuf path and 400 plain-text. The
// success/async/backpressure/oversize cells are exercised in
// `assert_ingest_route` and the `ingest/http.rs` handler-test matrix; these
// generic cases cover the two 400 plain-text rows the round-2 review demanded
// as distinct: a genuinely undecodable body, and a truly unsupported
// Content-Type (distinct from "JSON body under protobuf negotiation").

/// A raw (non-snappy) body under the default protobuf negotiation — the
/// snappy decode fails (oracle: `snappy: corrupt input` -> 400 plain-text),
/// the Loki analog of `rw_bad_snappy`.
fn loki_bad_snappy(req: &mut Req) {
    req.content_type = Some("application/x-protobuf");
    req.body = b"\xFF\xFF\xFF not snappy".to_vec();
}

/// A genuinely UNSUPPORTED Content-Type (`text/plain`) carrying a
/// non-protobuf body (issue #77 round-2 adjudication item 1): negotiation
/// defaults the unrecognized type to the protobuf path, so the non-snappy
/// body fails decode -> 400 plain-text. Distinct from `loki_bad_snappy`'s
/// documented-protobuf-header malformed-payload row.
fn loki_unsupported_content_type(req: &mut Req) {
    req.content_type = Some("text/plain");
    req.body =
        br#"{"streams":[{"stream":{"a":"b"},"values":[["1700000000000000000","x"]]}]}"#.to_vec();
}

const LOKI_PUSH_CASES: &[CaseClass] = &[
    CaseClass {
        name: "bad_snappy",
        build: loki_bad_snappy,
        expect_status: 400,
        expect: ExpectedError::PlainText,
    },
    CaseClass {
        name: "unsupported_content_type",
        build: loki_unsupported_content_type,
        expect_status: 400,
        expect: ExpectedError::PlainText,
    },
];

// -- Zipkin v2 JSON receiver (issue #75, docs/api.md §8.2) --------------
// Oracle-pinned against openzipkin/zipkin:3 (`POST /api/v2/spans`): success
// is an empty 202 (both sync and async — the Zipkin oracle answers 202
// Accepted regardless of the async header, unlike Loki's 204 or OTLP's 200
// sync); a malformed span array is a whole-request 400 plain-text (Zipkin
// has no partial-success channel), and an unsupported `Content-Encoding` is
// likewise a 400. The success/async cells are exercised in
// `assert_ingest_route`; these generic cases cover the two 400 plain-text
// rows.

/// A body that is not a decodable Zipkin v2 JSON span array — a
/// whole-request `ZipkinDecode` 400 plain-text (oracle: 400 "Expected a
/// JSON_V2 encoded list").
fn zipkin_malformed_json(req: &mut Req) {
    req.content_type = Some("application/json");
    req.body = b"not a json span array".to_vec();
}

/// An unsupported `Content-Encoding` (`br`) — the decompression seam
/// rejects it as a whole-request 400 plain-text, exactly like the other
/// writer-side receivers.
fn zipkin_unsupported_content_encoding(req: &mut Req) {
    req.content_type = Some("application/json");
    req.headers.push(("content-encoding", "br".to_string()));
    req.body = br#"[{"traceId":"0000000000000001","id":"0000000000000002"}]"#.to_vec();
}

const ZIPKIN_CASES: &[CaseClass] = &[
    CaseClass {
        name: "malformed_json",
        build: zipkin_malformed_json,
        expect_status: 400,
        expect: ExpectedError::PlainText,
    },
    CaseClass {
        name: "unsupported_content_encoding",
        build: zipkin_unsupported_content_encoding,
        expect_status: 400,
        expect: ExpectedError::PlainText,
    },
];

// ---------------------------------------------------------------------
// RouteSpec — the manifest itself
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub struct RouteSpec {
    /// The axum route template exactly as `.route(...)` registers it
    /// (e.g. `/api/v1/label/{name}/values`).
    pub path: &'static str,
    pub methods: &'static [Method],
    pub surface: Surface,
    pub gate: Gate,
    pub status: RouteStatus,
    pub doc_ref: DocRef,
    /// The exact documented success status for the first-listed method
    /// against a ready (pool + cache warm, empty-result-set-is-still-
    /// success) server — the "method conformance" case-class's expected
    /// status (plan v2 AC: "no non-404/non-405 placeholders").
    pub success_status: u16,
    /// A valid GET query string (no leading `?`) — or, for a `POST`-only
    /// documented route, a valid `application/x-www-form-urlencoded` body
    /// — reaching `success_status` against an empty-but-live database.
    /// Unused (empty) for `Surface::Ingest`, whose bodies are protobuf/
    /// snappy and built directly in `api_conformance.rs`.
    pub base_query: &'static str,
    pub cases: &'static [CaseClass],
}

static MANIFEST: &[RouteSpec] = &[
    // -- Ops (always mounted, mode/flag independent) --------------------
    RouteSpec {
        path: "/ready",
        methods: &[Method::Get],
        surface: Surface::OpsPublic,
        gate: Gate::Always,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "",
        cases: &[],
    },
    RouteSpec {
        path: "/metrics",
        methods: &[Method::Get],
        surface: Surface::OpsPublic,
        gate: Gate::Always,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "",
        cases: &[],
    },
    RouteSpec {
        path: "/config",
        methods: &[Method::Get],
        surface: Surface::OpsAuthed,
        gate: Gate::Always,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "",
        cases: &[],
    },
    RouteSpec {
        path: "/buildinfo",
        methods: &[Method::Get],
        surface: Surface::OpsAuthed,
        gate: Gate::Always,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "",
        cases: &[],
    },
    // -- Ingest (WriterMode) ---------------------------------------------
    RouteSpec {
        path: "/v1/logs",
        methods: &[Method::Post],
        surface: Surface::Ingest,
        gate: Gate::WriterMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "",
        cases: OTLP_INGEST_CASES,
    },
    RouteSpec {
        path: "/v1/metrics",
        methods: &[Method::Post],
        surface: Surface::Ingest,
        gate: Gate::WriterMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "",
        cases: OTLP_INGEST_CASES,
    },
    RouteSpec {
        path: "/v1/traces",
        methods: &[Method::Post],
        surface: Surface::Ingest,
        gate: Gate::WriterMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "",
        cases: OTLP_TRACES_INGEST_CASES,
    },
    RouteSpec {
        path: "/api/v1/write",
        methods: &[Method::Post],
        surface: Surface::Ingest,
        gate: Gate::WriterMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 204,
        base_query: "",
        cases: REMOTE_WRITE_CASES,
    },
    // -- Logs query, native (ReaderMode) ----------------------------------
    RouteSpec {
        path: "/api/logs/v1/query_range",
        methods: &[Method::Get, Method::Post],
        surface: Surface::LogsQuery,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "query=%7Bservice_name%3D%22checkout%22%7D",
        cases: LOGS_QUERY_LIKE_CASES,
    },
    RouteSpec {
        path: "/api/logs/v1/query",
        methods: &[Method::Get, Method::Post],
        surface: Surface::LogsQuery,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "query=count_over_time%28%7Bservice_name%3D%22checkout%22%7D%5B1h%5D%29",
        cases: LOGS_QUERY_LIKE_CASES,
    },
    RouteSpec {
        path: "/api/logs/v1/labels",
        methods: &[Method::Get, Method::Post],
        surface: Surface::LogsQuery,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "",
        cases: LOGS_LABELS_CASES,
    },
    RouteSpec {
        path: "/api/logs/v1/label/{name}/values",
        methods: &[Method::Get],
        surface: Surface::LogsQuery,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "",
        cases: &[],
    },
    RouteSpec {
        path: "/api/logs/v1/series",
        methods: &[Method::Get, Method::Post],
        surface: Surface::LogsQuery,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "match%5B%5D=%7Bservice_name%3D%22checkout%22%7D",
        cases: LOGS_SERIES_CASES,
    },
    // -- Logs tail + stats, native (ReaderMode, issue #74) ----------------
    // Tail's `success_status` is the empirically-pinned 400 a bare GET
    // (no upgrade headers) receives from axum's `WebSocketUpgrade`
    // extractor — the mounting oracle (`Surface::LogsTail`'s doc
    // comment); the real handshake/error matrix lives in
    // `assert_tail_route` + `logs_tail_live.rs`.
    RouteSpec {
        path: "/api/logs/v1/tail",
        methods: &[Method::Get],
        surface: Surface::LogsTail,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 400,
        base_query: "query=%7Bservice_name%3D%22checkout%22%7D",
        cases: &[],
    },
    RouteSpec {
        path: "/api/logs/v1/stats",
        methods: &[Method::Get],
        surface: Surface::LogsStats,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "query=%7Bservice_name%3D%22checkout%22%7D",
        cases: LOGS_STATS_CASES,
    },
    // -- Logs query, `/loki/api/v1` compat alias (CompatAndReader) -------
    RouteSpec {
        path: "/loki/api/v1/query_range",
        methods: &[Method::Get, Method::Post],
        surface: Surface::LogsQuery,
        gate: Gate::CompatAndReader,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::LokiAliasSuffix {
            suffix: "/query_range",
        },
        success_status: 200,
        base_query: "query=%7Bservice_name%3D%22checkout%22%7D",
        // Review round-1 finding (medium): the alias is a pure route
        // binding onto the native handler (docs/api.md §8.1) — reusing the
        // native route's exact `CaseClass` list (not an empty one) means an
        // alias accidentally wired to the wrong handler still fails this
        // matrix, not just a byte-identity smoke test.
        cases: LOGS_QUERY_LIKE_CASES,
    },
    RouteSpec {
        path: "/loki/api/v1/query",
        methods: &[Method::Get, Method::Post],
        surface: Surface::LogsQuery,
        gate: Gate::CompatAndReader,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::LokiAliasSuffix { suffix: "/query" },
        success_status: 200,
        base_query: "query=count_over_time%28%7Bservice_name%3D%22checkout%22%7D%5B1h%5D%29",
        cases: LOGS_QUERY_LIKE_CASES,
    },
    RouteSpec {
        path: "/loki/api/v1/labels",
        methods: &[Method::Get, Method::Post],
        surface: Surface::LogsQuery,
        gate: Gate::CompatAndReader,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::LokiAliasSuffix { suffix: "/labels" },
        success_status: 200,
        base_query: "",
        cases: LOGS_LABELS_CASES,
    },
    RouteSpec {
        path: "/loki/api/v1/label/{name}/values",
        methods: &[Method::Get],
        surface: Surface::LogsQuery,
        gate: Gate::CompatAndReader,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::LokiAliasSuffix {
            suffix: "/label/{name}/values",
        },
        success_status: 200,
        base_query: "",
        cases: &[],
    },
    RouteSpec {
        path: "/loki/api/v1/series",
        methods: &[Method::Get, Method::Post],
        surface: Surface::LogsQuery,
        gate: Gate::CompatAndReader,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::LokiAliasSuffix { suffix: "/series" },
        success_status: 200,
        base_query: "match%5B%5D=%7Bservice_name%3D%22checkout%22%7D",
        cases: LOGS_SERIES_CASES,
    },
    // -- Logs tail + stats, M6 compat aliases (CompatAndReader, #74) -----
    // `DocRef::Verbatim`, not `LokiAliasSuffix`: the §8.1 M6 row spells
    // both alias paths out in full (and `LokiAliasSuffix` is scoped to
    // the M1 row's line). Note the stats alias is `/index/stats` — NOT a
    // prefix swap of the native `/stats`.
    RouteSpec {
        path: "/loki/api/v1/tail",
        methods: &[Method::Get],
        surface: Surface::LogsTail,
        gate: Gate::CompatAndReader,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 400,
        base_query: "query=%7Bservice_name%3D%22checkout%22%7D",
        cases: &[],
    },
    RouteSpec {
        path: "/loki/api/v1/index/stats",
        methods: &[Method::Get],
        surface: Surface::LogsStats,
        gate: Gate::CompatAndReader,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "query=%7Bservice_name%3D%22checkout%22%7D",
        cases: LOGS_STATS_CASES,
    },
    // -- Loki push receiver, `/loki/api/v1` compat alias (CompatAndWriter,
    //    issue #77) -------------------------------------------------------
    // The first WRITER-side compat surface (docs/api.md §8.2): a foreign-
    // format decoder feeding the existing log-storage path, not a route
    // binding onto a native handler. `success_status` is 204 (empty-body,
    // both encodings — oracle-pinned against grafana/loki:3.4.2), so
    // `assert_ingest_route` treats it in the empty-204 family alongside
    // `/api/v1/write`. `DocRef::Verbatim`: the §8.2 table spells the full
    // path out.
    RouteSpec {
        path: "/loki/api/v1/push",
        methods: &[Method::Post],
        surface: Surface::Ingest,
        gate: Gate::CompatAndWriter,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 204,
        base_query: "",
        cases: LOKI_PUSH_CASES,
    },
    // -- Zipkin v2 JSON receiver, `/api/v2/spans` + `/tempo/spans`
    //    (CompatAndWriter, issue #75) ---------------------------------------
    // The second writer-side compat surface (docs/api.md §8.2): a Zipkin v2
    // JSON decoder adapting each span to OTLP and feeding the existing
    // trace-storage path. Both documented paths bind to the same handler.
    // `success_status` is 202 (empty-body, both sync and async — oracle-
    // pinned against openzipkin/zipkin:3), so `assert_ingest_route` treats
    // them in the empty-body family with 202 sync cells. `DocRef::Verbatim`:
    // the §8.2 table spells both paths out.
    RouteSpec {
        path: "/api/v2/spans",
        methods: &[Method::Post],
        surface: Surface::Ingest,
        gate: Gate::CompatAndWriter,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 202,
        base_query: "",
        cases: ZIPKIN_CASES,
    },
    RouteSpec {
        path: "/tempo/spans",
        methods: &[Method::Post],
        surface: Surface::Ingest,
        gate: Gate::CompatAndWriter,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 202,
        base_query: "",
        cases: ZIPKIN_CASES,
    },
    // -- Prom API (ReaderMode, no compat aliases) ------------------------
    RouteSpec {
        path: "/api/v1/query",
        methods: &[Method::Get, Method::Post],
        surface: Surface::PromApi,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "query=up",
        cases: PROM_QUERY_CASES,
    },
    RouteSpec {
        path: "/api/v1/query_range",
        methods: &[Method::Get, Method::Post],
        surface: Surface::PromApi,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        // Exactly at the 11,000-point cap (`(10999-0)/1 + 1 == 11000`) —
        // proven by `prom_api/handlers.rs`'s own
        // `query_range_at_exactly_the_cap_passes_param_validation`.
        base_query: "query=up&start=0&end=10999&step=1",
        cases: PROM_QUERY_RANGE_CASES,
    },
    RouteSpec {
        path: "/api/v1/labels",
        methods: &[Method::Get, Method::Post],
        surface: Surface::PromApi,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "",
        cases: PROM_LABELS_CASES,
    },
    RouteSpec {
        path: "/api/v1/label/{name}/values",
        methods: &[Method::Get],
        surface: Surface::PromApi,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "",
        cases: &[],
    },
    RouteSpec {
        path: "/api/v1/series",
        methods: &[Method::Get, Method::Post],
        surface: Surface::PromApi,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "match%5B%5D=up",
        cases: PROM_SERIES_CASES,
    },
    RouteSpec {
        path: "/api/v1/metadata",
        methods: &[Method::Get],
        surface: Surface::PromApi,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "",
        cases: &[],
    },
    RouteSpec {
        path: "/api/v1/query_exemplars",
        methods: &[Method::Get, Method::Post],
        surface: Surface::PromApi,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "",
        cases: &[],
    },
    RouteSpec {
        path: "/api/v1/status/buildinfo",
        methods: &[Method::Get],
        surface: Surface::PromApi,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "",
        cases: &[],
    },
    RouteSpec {
        path: "/api/v1/status/config",
        methods: &[Method::Get],
        surface: Surface::PromApi,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "",
        cases: &[],
    },
    RouteSpec {
        path: "/api/v1/status/flags",
        methods: &[Method::Get],
        surface: Surface::PromApi,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "",
        cases: &[],
    },
    RouteSpec {
        path: "/api/v1/status/runtimeinfo",
        methods: &[Method::Get],
        surface: Surface::PromApi,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "",
        cases: &[],
    },
    RouteSpec {
        path: "/api/v1/status/tsdb",
        methods: &[Method::Get],
        surface: Surface::PromApi,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "",
        cases: &[],
    },
    // -- Traces fetch (ReaderMode, issue #55) -----------------------------
    // `success_status` is 404: against this suite's empty databases a
    // well-formed absent-id fetch's documented outcome is the
    // mounted-but-absent `not_found` JSON envelope (`Surface::TracesFetch`'s
    // doc comment — it doubles as the mounting oracle).
    RouteSpec {
        path: "/api/traces/v1/trace/{traceId}",
        methods: &[Method::Get],
        surface: Surface::TracesFetch,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 404,
        base_query: "",
        cases: TRACE_FETCH_CASES,
    },
    RouteSpec {
        path: "/api/traces/v1/trace/{traceId}/json",
        methods: &[Method::Get],
        surface: Surface::TracesFetch,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 404,
        base_query: "",
        cases: TRACE_FETCH_CASES,
    },
    // -- Traces search (ReaderMode, issue #57) ---------------------------
    // `success_status` is 200: against this suite's empty databases a
    // well-formed match-all search returns the documented empty envelope
    // (`{"traces":[],"metrics":{...}}`) — the mounting oracle
    // (`Surface::TracesSearch`'s doc comment).
    RouteSpec {
        path: "/api/traces/v1/search",
        methods: &[Method::Get],
        surface: Surface::TracesSearch,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: TRACES_SEARCH_BASE_QUERY,
        cases: TRACES_SEARCH_CASES,
    },
    // -- Traces tags (ReaderMode, issue #58) ------------------------------
    // `success_status` is 200: against this suite's empty databases both
    // discovery routes return the documented empty envelope — the
    // mounting oracle (`Surface::TracesTags`'s doc comment).
    RouteSpec {
        path: "/api/traces/v1/tags",
        methods: &[Method::Get],
        surface: Surface::TracesTags,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "",
        cases: TRACES_TAGS_CASES,
    },
    RouteSpec {
        path: "/api/traces/v1/tag/{tag}/values",
        methods: &[Method::Get],
        surface: Surface::TracesTags,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: TRACES_TAG_VALUES_BASE_QUERY,
        cases: TRACES_TAG_VALUES_CASES,
    },
    // -- Traces metrics (ReaderMode, issue #59) ---------------------------
    // `success_status` is 200: against this suite's empty databases a
    // well-formed match-all metrics query returns the documented empty
    // Prometheus envelope (range: `result:[]`; instant: one `"0"`
    // sample) — the mounting oracle (`Surface::TracesMetrics`'s doc
    // comment).
    RouteSpec {
        path: "/api/traces/v1/metrics/query_range",
        methods: &[Method::Get],
        surface: Surface::TracesMetrics,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: TRACES_METRICS_BASE_QUERY,
        cases: TRACES_METRICS_CASES,
    },
    RouteSpec {
        path: "/api/traces/v1/metrics/query",
        methods: &[Method::Get],
        surface: Surface::TracesMetrics,
        gate: Gate::ReaderMode,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: TRACES_METRICS_BASE_QUERY,
        cases: TRACES_METRICS_CASES,
    },
    // -- Tempo compat aliases (CompatAndReader, issue #61) ----------------
    // 13 routes, all GET, all mounted iff `PULSUS_COMPAT_ENDPOINTS=true`
    // AND Reader is mounted (the M1 Loki precedent). Eight are pure route
    // bindings onto the native traces handlers — reusing the native
    // surface, `success_status`, `base_query`, and exact `CaseClass` list
    // (the #36 alias pattern: an alias wired to the wrong handler fails
    // this matrix, not just a byte-identity smoke test); byte-identity on
    // seeded data is proven in `traces_api_live.rs`. Four reshape to
    // Tempo's v1 flat / v2 (no `truncated`) tag shapes (own surfaces);
    // `/api/echo` is a constant.
    RouteSpec {
        path: "/api/traces/{traceId}",
        methods: &[Method::Get],
        surface: Surface::TracesFetch,
        gate: Gate::CompatAndReader,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 404,
        base_query: "",
        cases: TRACE_FETCH_CASES,
    },
    RouteSpec {
        path: "/api/traces/{traceId}/json",
        methods: &[Method::Get],
        surface: Surface::TracesFetch,
        gate: Gate::CompatAndReader,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 404,
        base_query: "",
        cases: TRACE_FETCH_CASES,
    },
    RouteSpec {
        path: "/tempo/api/traces/{traceId}",
        methods: &[Method::Get],
        surface: Surface::TracesFetch,
        gate: Gate::CompatAndReader,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 404,
        base_query: "",
        cases: TRACE_FETCH_CASES,
    },
    RouteSpec {
        path: "/api/search",
        methods: &[Method::Get],
        surface: Surface::TracesSearch,
        gate: Gate::CompatAndReader,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: TRACES_SEARCH_BASE_QUERY,
        cases: TRACES_SEARCH_CASES,
    },
    RouteSpec {
        path: "/api/v2/search/tags",
        methods: &[Method::Get],
        surface: Surface::TracesTagsV2,
        gate: Gate::CompatAndReader,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "",
        cases: TRACES_TAGS_CASES,
    },
    RouteSpec {
        path: "/api/v2/search/tag/{tag}/values",
        methods: &[Method::Get],
        surface: Surface::TracesTagsV2,
        gate: Gate::CompatAndReader,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: TRACES_TAG_VALUES_BASE_QUERY,
        cases: TRACES_TAG_VALUES_CASES,
    },
    RouteSpec {
        path: "/api/search/tags",
        methods: &[Method::Get],
        surface: Surface::TracesTagsV1,
        gate: Gate::CompatAndReader,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "",
        cases: TRACES_TAGS_CASES,
    },
    RouteSpec {
        path: "/api/search/tag/{tag}/values",
        methods: &[Method::Get],
        surface: Surface::TracesTagsV1,
        gate: Gate::CompatAndReader,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: TRACES_TAG_VALUES_BASE_QUERY,
        cases: TRACES_TAG_VALUES_CASES,
    },
    RouteSpec {
        path: "/api/echo",
        methods: &[Method::Get],
        surface: Surface::Echo,
        gate: Gate::CompatAndReader,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: "",
        cases: &[],
    },
    RouteSpec {
        path: "/api/metrics/query_range",
        methods: &[Method::Get],
        surface: Surface::TracesMetrics,
        gate: Gate::CompatAndReader,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: TRACES_METRICS_BASE_QUERY,
        cases: TRACES_METRICS_CASES,
    },
    RouteSpec {
        path: "/api/metrics/query",
        methods: &[Method::Get],
        surface: Surface::TracesMetrics,
        gate: Gate::CompatAndReader,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: TRACES_METRICS_BASE_QUERY,
        cases: TRACES_METRICS_CASES,
    },
    RouteSpec {
        path: "/tempo/api/metrics/query_range",
        methods: &[Method::Get],
        surface: Surface::TracesMetrics,
        gate: Gate::CompatAndReader,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: TRACES_METRICS_BASE_QUERY,
        cases: TRACES_METRICS_CASES,
    },
    RouteSpec {
        path: "/tempo/api/metrics/query",
        methods: &[Method::Get],
        surface: Surface::TracesMetrics,
        gate: Gate::CompatAndReader,
        status: RouteStatus::Mounted,
        doc_ref: DocRef::Verbatim,
        success_status: 200,
        base_query: TRACES_METRICS_BASE_QUERY,
        cases: TRACES_METRICS_CASES,
    },
    // -- Planned (documented, not yet mounted) ---------------------------
    // Representative, not exhaustive (deviation, see issue #36 implementation
    // notes): every M4-M8 §4-§8 row is out of scope for a guard whose only
    // AC is "every *mounted* route is documented" — these entries exist for
    // the endpoint inventory's own documentation value and are excluded
    // from both the drift guard (never `Mounted`) and the docs-gap check
    // (`DocRef::Skip`).
    RouteSpec {
        path: "/v1development/profiles",
        methods: &[Method::Post],
        surface: Surface::Ingest,
        gate: Gate::WriterMode,
        status: RouteStatus::Planned { milestone: "M5" },
        doc_ref: DocRef::Skip,
        success_status: 0,
        base_query: "",
        cases: &[],
    },
    RouteSpec {
        path: "/api/profiles/v1/ingest",
        methods: &[Method::Post],
        surface: Surface::Ingest,
        gate: Gate::WriterMode,
        status: RouteStatus::Planned { milestone: "M5" },
        doc_ref: DocRef::Skip,
        success_status: 0,
        base_query: "",
        cases: &[],
    },
    RouteSpec {
        path: "/api/logs/v1/volume",
        methods: &[Method::Get],
        surface: Surface::LogsQuery,
        gate: Gate::ReaderMode,
        status: RouteStatus::Planned { milestone: "M7" },
        doc_ref: DocRef::Skip,
        success_status: 0,
        base_query: "",
        cases: &[],
    },
    RouteSpec {
        path: "/api/logs/v1/detected_labels",
        methods: &[Method::Get],
        surface: Surface::LogsQuery,
        gate: Gate::ReaderMode,
        status: RouteStatus::Planned { milestone: "M7" },
        doc_ref: DocRef::Skip,
        success_status: 0,
        base_query: "",
        cases: &[],
    },
    RouteSpec {
        path: "/api/logs/v1/detected_fields",
        methods: &[Method::Get],
        surface: Surface::LogsQuery,
        gate: Gate::ReaderMode,
        status: RouteStatus::Planned { milestone: "M7" },
        doc_ref: DocRef::Skip,
        success_status: 0,
        base_query: "",
        cases: &[],
    },
    RouteSpec {
        path: "/api/logs/v1/patterns",
        methods: &[Method::Get],
        surface: Surface::LogsQuery,
        gate: Gate::ReaderMode,
        status: RouteStatus::Planned { milestone: "M7" },
        doc_ref: DocRef::Skip,
        success_status: 0,
        base_query: "",
        cases: &[],
    },
    RouteSpec {
        path: "/api/profiles/v1/types",
        methods: &[Method::Get],
        surface: Surface::PromApi,
        gate: Gate::ReaderMode,
        status: RouteStatus::Planned { milestone: "M5" },
        doc_ref: DocRef::Skip,
        success_status: 0,
        base_query: "",
        cases: &[],
    },
    RouteSpec {
        path: "/api/rules/v1/{kind}",
        methods: &[Method::Get, Method::Post],
        surface: Surface::PromApi,
        gate: Gate::ReaderMode,
        status: RouteStatus::Planned { milestone: "M7" },
        doc_ref: DocRef::Skip,
        success_status: 0,
        base_query: "",
        cases: &[],
    },
    RouteSpec {
        path: "/api/v1/rules",
        methods: &[Method::Get],
        surface: Surface::PromApi,
        gate: Gate::ReaderMode,
        status: RouteStatus::Planned { milestone: "M7" },
        doc_ref: DocRef::Skip,
        success_status: 0,
        base_query: "",
        cases: &[],
    },
];

pub fn route_manifest() -> &'static [RouteSpec] {
    MANIFEST
}

// ---------------------------------------------------------------------
// Pinned function bodies (issue #36, task-manager adjudication round 5)
// ---------------------------------------------------------------------

/// Function names this scanner treats as "route-mounting helpers" — a
/// function that is not itself `.merge(`/`.nest(`/`.nest_service(`-based
/// composition, but that a composing function calls to have it register a
/// whole family of routes (`mount_log_query_routes`, docs/api.md §2 — the
/// shared five-route method matrix `logs_api::router`/`compat_router` both
/// delegate to). Pinned explicitly here, by name, rather than derived by
/// a "does this function take a `Router` and register routes" heuristic
/// scan — that heuristic would itself be exactly the kind of semantic
/// inference round 1-4 kept losing to (task-manager adjudication, issue
/// #36 round 5).
pub const ROUTE_MOUNTING_HELPERS: &[&str] = &["mount_log_query_routes"];

/// One pinned function whose *entire body* is exact-pinned — round-5
/// finding: per-call-site pinning (round 4) collapsed distinct call sites
/// with identical text onto one set entry, hiding a second, textually-
/// identical `.merge(...)` added under a different match arm (a real
/// mode-gating change). Pinning the *whole function body* instead makes
/// occurrence count, match-arm placement, and every other control-flow
/// detail part of the pinned text by construction: two merges where there
/// used to be one is a different body, full stop, regardless of whether
/// the two calls read identically in isolation.
///
/// `body` is the function's body text (strictly between its braces),
/// whitespace-normalized (`route_inventory.rs`'s `normalize_whitespace` —
/// a pure reformat never trips the guard, any real content change does).
/// Every non-test function in either scanned tree whose body contains a
/// `.merge(`/`.nest(`/`.nest_service(`/`.fallback(`/
/// `.method_not_allowed_fallback(` token, or a call to a
/// [`ROUTE_MOUNTING_HELPERS`] name, needs an entry here — re-derive
/// (and reconcile [`route_manifest`] alongside it) whenever
/// `every_pinned_function_body_matches_the_snapshot_exactly` reports a
/// drift. Any edit to one of these few load-bearing functions — even one
/// that only adds a second, textually-identical composition call under a
/// different branch — forces this re-derivation; that is the guard doing
/// its job, not a false positive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PinnedFunctionBody {
    /// Workspace-relative path, e.g. `crates/pulsus-server/src/app.rs`.
    pub file: &'static str,
    pub function: &'static str,
    pub body: &'static str,
}

static PINNED_FUNCTION_BODIES: &[PinnedFunctionBody] = &[
    PinnedFunctionBody {
        file: "crates/pulsus-server/src/app.rs",
        function: "build_router",
        body: "let mut authed = ops::ops_authed_router().merge(modes::mount_subsystems(Router::new(), config)); authed = compat::apply_aliases(authed, config); authed = authed.layer(middleware::timeout_layer(config)); if let Some(auth) = middleware::auth_layer(config) {authed = authed.layer(auth);} let router = ops::ops_public_router().merge(authed); let router = router.layer(middleware::trace_layer()).layer(middleware::compression_layer()).layer(middleware::cors_layer(config)?); Ok(router.with_state(state))",
    },
    PinnedFunctionBody {
        file: "crates/pulsus-server/src/modes.rs",
        function: "mount_subsystems",
        body: "let mut router = router; for subsystem in mounted(cfg) {router = match subsystem {Subsystem::Writer => router.merge(writer_router()), Subsystem::Reader => router.merge(reader_router()), Subsystem::Ruler => router.merge(ruler_router()),};} router",
    },
    PinnedFunctionBody {
        file: "crates/pulsus-server/src/compat.rs",
        function: "apply_aliases",
        // Re-pinned for issue #75 (M6-12): the Zipkin v2 JSON receiver's
        // `CompatAndWriter` merge joins the Loki push receiver in the
        // Writer-gated branch. (Previously re-pinned for issue #77's Loki
        // push mount, and #61's Tempo trace-alias merge in the Reader block.)
        body: "if !cfg.compat_endpoints {return router;} let mut router = router; if modes::mounted(cfg).contains(&Subsystem::Reader) {router = router.merge(crate::logs_api::compat_router()); router = router.merge(crate::traces_api::compat_router());} if modes::mounted(cfg).contains(&Subsystem::Writer) {router = router.merge(crate::ingest::loki_push_compat_router()); router = router.merge(crate::ingest::zipkin_compat_router());} router",
    },
    PinnedFunctionBody {
        file: "crates/pulsus-server/src/subsystems.rs",
        function: "reader_router",
        body: "crate::logs_api::router().merge(crate::prom_api::router()).merge(crate::traces_api::router())",
    },
    // Re-pinned for issue #74 (M6-11): tail + stats mount explicitly on
    // both surfaces (the stats alias suffix `/index/stats` is not a
    // prefix swap, so neither route goes through
    // `mount_log_query_routes`).
    PinnedFunctionBody {
        file: "crates/pulsus-server/src/logs_api/mod.rs",
        function: "router",
        body: "mount_log_query_routes(Router::new(), \"/api/logs/v1\").route(\"/api/logs/v1/tail\", get(tail::tail)).route(\"/api/logs/v1/stats\", get(stats::stats))",
    },
    PinnedFunctionBody {
        file: "crates/pulsus-server/src/logs_api/mod.rs",
        function: "compat_router",
        body: "mount_log_query_routes(Router::new(), \"/loki/api/v1\").route(\"/loki/api/v1/tail\", get(tail::tail)).route(\"/loki/api/v1/index/stats\", get(stats::stats))",
    },
    // NOT a router-composition function — this is the Loki-push `PushRequest`'s
    // hand-written `prost::Message::merge` (issue #115 round 2), which routes the
    // raw merge entry point through the aggregate-bounded twin. Its body contains
    // a `bounded.merge(buf)` call, so the textual `.merge(` scan pins it here even
    // though it mounts no routes; re-derive if that decode-bounds body changes.
    // Re-derived for issue #115 round 3: the error-path restoration (assign the
    // twin's streams back on both Ok and Err rather than early-`?`) changed the
    // last three statements of this body.
    PinnedFunctionBody {
        file: "crates/pulsus-write/src/protocols/loki_push.rs",
        function: "merge",
        body: "let mut bounded = BoundedPushRequest {total_entries: self.streams.iter().map(|s| s.entries.len()).sum(), streams: std::mem::take(&mut self.streams),}; let result = bounded.merge(buf); self.streams = bounded.streams; result",
    },
    // NOT a router-composition function — this is the remote-write
    // `WriteRequest`'s hand-written `prost::Message::merge` (issue #115 track 3,
    // finding #62), which routes the raw merge entry point through the
    // aggregate-bounded `BoundedWriteRequest` twin. Its body contains a
    // `bounded.merge(buf)` call, so the textual `.merge(` scan pins it here even
    // though it mounts no routes; re-derive if that decode-bounds body changes.
    // (The `merge_length_delimited` sibling needs no pin: its body's
    // `bounded.merge_length_delimited(buf)` does not contain the `.merge(`
    // scanner token.)
    // Re-derived for issue #140: the inline aggregate re-sum seeding was
    // hoisted into `BoundedWriteRequest::seeded_from` so the histogram
    // span/bucket aggregates are re-summed alongside labels/samples on both
    // raw merge entry points.
    PinnedFunctionBody {
        file: "crates/pulsus-write/src/protocols/remote_write.rs",
        function: "merge",
        body: "let mut bounded = BoundedWriteRequest::seeded_from(std::mem::take(&mut self.timeseries), std::mem::take(&mut self.metadata)); let result = bounded.merge(buf); self.timeseries = bounded.timeseries; self.metadata = bounded.metadata; result",
    },
];

pub fn pinned_function_bodies() -> &'static [PinnedFunctionBody] {
    PINNED_FUNCTION_BODIES
}
