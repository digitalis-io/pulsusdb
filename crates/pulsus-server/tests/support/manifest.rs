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

/// `422 execution`: a `__name__` regex matcher in `match[]` — the
/// documented M2 out-of-subset-construct limitation
/// (`prom_api/handlers.rs::parse_match_selectors`), reachable **without**
/// any pool/seed data (fails before `engine_for` is ever called) — stands
/// in for the "over-broad/unsupported selector" case-class the issue body
/// asks for on the metrics surface (see `api_conformance.rs`'s module doc
/// for why the LogQL `query_too_broad` analog is not live-tested here).
fn prom_name_regex_unsupported(req: &mut Req) {
    req.query = format!("match%5B%5D={}", enc(r#"{__name__=~"up.*"}"#));
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
        name: "name_regex_unsupported",
        build: prom_name_regex_unsupported,
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
        name: "name_regex_unsupported",
        build: prom_name_regex_unsupported,
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

// Case-class 6's "wrong Content-Type" cell (architect plan v1: "wrong
// Content-Type / undecodable body on ingest (400)") is **not** a
// `CaseClass` here (review round-2 finding: round-1's version sent
// undecodable JSON, which is indistinguishable from `malformed_body` —
// it never actually tested what the header does). Verified by reading
// `ingest/http.rs` (`content_encoding`/`decode_request`'s own doc comment:
// "this handler decodes every request body as protobuf unconditionally
// ... never inspects Content-Type") and confirmed live: a **valid**
// `Export{Logs,Metrics}ServiceRequest` protobuf body labeled
// `Content-Type: application/json` still returns the ordinary `200`
// success envelope, byte-identical to the same body sent with the
// documented `application/x-protobuf` header — conformance pins this
// reality (`Content-Type` is not enforced) rather than a 400 that would
// never actually happen. Modeled as a direct success-path assertion in
// `api_conformance.rs::assert_ingest_route` (needs a signal-specific valid
// body — `ExportLogsServiceRequest` vs `ExportMetricsServiceRequest` —
// which a single shared `CaseClass::build` fn can't express, since it has
// no way to know which of `/v1/logs`/`/v1/metrics` it is building for).
//
// Task-manager adjudication, issue #36 round 7
// (https://github.com/digitalis-io/pulsusdb/issues/36#issuecomment-4978613793):
// plan v2's "wrong Content-Type -> 400" case is formally amended — the
// handlers were determined not to enforce Content-Type at all; whether
// ingest SHOULD reject a mismatched Content-Type is a product question
// out of scope for #36 (deferred to a follow-up if desired). These
// pinned-reality assertions stand as-is.

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

fn rw_bad_snappy(req: &mut Req) {
    req.body = b"\xFF\xFF\xFF not snappy".to_vec();
}

const REMOTE_WRITE_CASES: &[CaseClass] = &[CaseClass {
    name: "bad_snappy",
    build: rw_bad_snappy,
    expect_status: 400,
    expect: ExpectedError::PlainText,
}];

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
        path: "/api/logs/v1/tail",
        methods: &[Method::Get],
        surface: Surface::LogsQuery,
        gate: Gate::ReaderMode,
        status: RouteStatus::Planned { milestone: "M6" },
        doc_ref: DocRef::Skip,
        success_status: 0,
        base_query: "",
        cases: &[],
    },
    RouteSpec {
        path: "/api/logs/v1/stats",
        methods: &[Method::Get],
        surface: Surface::LogsQuery,
        gate: Gate::ReaderMode,
        status: RouteStatus::Planned { milestone: "M6" },
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
        path: "/api/traces/v1/search",
        methods: &[Method::Get],
        surface: Surface::PromApi,
        gate: Gate::ReaderMode,
        status: RouteStatus::Planned { milestone: "M4" },
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
        body: "if !cfg.compat_endpoints {return router;} let mut router = router; if modes::mounted(cfg).contains(&Subsystem::Reader) {router = router.merge(crate::logs_api::compat_router());} router",
    },
    PinnedFunctionBody {
        file: "crates/pulsus-server/src/subsystems.rs",
        function: "reader_router",
        body: "crate::logs_api::router().merge(crate::prom_api::router()).merge(crate::traces_api::router())",
    },
    PinnedFunctionBody {
        file: "crates/pulsus-server/src/logs_api/mod.rs",
        function: "router",
        body: "mount_log_query_routes(Router::new(), \"/api/logs/v1\")",
    },
    PinnedFunctionBody {
        file: "crates/pulsus-server/src/logs_api/mod.rs",
        function: "compat_router",
        body: "mount_log_query_routes(Router::new(), \"/loki/api/v1\")",
    },
];

pub fn pinned_function_bodies() -> &'static [PinnedFunctionBody] {
    PINNED_FUNCTION_BODIES
}
