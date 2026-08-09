//! `/api/traces/v1` — the M4 trace-by-ID fetch endpoints (issue #55,
//! docs/api.md §4.1). Thin handlers (`handlers.rs`): parse the hex trace
//! id (`params.rs`) → point-read via `TraceEngine` (`pulsus-read`) →
//! decode/dedup/merge/encode the OTLP `TracesData` (`assemble.rs`) →
//! negotiate the representation (`negotiate.rs`) → render errors as the
//! reference's bare `text/plain` body (`error.rs`, issue #384). All SQL/execution stays in `pulsus-read` — this module
//! only ever talks to it through `TraceEngine`'s public methods; all OTLP
//! shaping stays here so `pulsus-read` stays OTLP-agnostic (task-manager
//! adjudication on issue #55).
//!
//! `/api/traces/v1/search` (issue #57, docs/api.md §4.2) lives here too:
//! `search.rs` (handler), `params.rs` (search params), `legacy.rs`
//! (logfmt → TraceQL), `search_response.rs` (documented JSON shaping) —
//! planning/execution stay in `pulsus-read::traces`.
//!
//! The §4.3 tag-discovery routes (issue #58) follow the same split:
//! `tags.rs` (handlers), `tags_response.rs` (documented JSON shaping +
//! best-effort type inference) — both served exclusively from the
//! Global `trace_tag_catalog` via `TraceEngine`, never by scanning span
//! payloads.
//!
//! The §4.4 TraceQL metrics routes (issue #59) live in `metrics.rs`:
//! thin handlers over `pulsus-read`'s metrics planner/engine, encoding
//! through the shared `prom_api::encode` Prometheus matrix/vector
//! envelope.
//!
//! The §4.5 service-graph route (issue #173, M7-E1) lives in `graph.rs`: a
//! thin handler over `pulsus-read`'s `service_graph` two-level aggregation
//! against the `trace_edges` half-row ledger, shaping the native
//! `{"edges":[...],"truncated":...}` JSON envelope. No Tempo-compat alias.
//!
//! The §8.1 Tempo compat aliases (issue #61, T9) ship here too:
//! [`compat_router`] binds the eight pure aliases onto these same
//! handlers and the reshaping/constant ones onto `compat.rs`;
//! `crate::compat::apply_aliases` decides *whether* it gets merged
//! (`PULSUS_COMPAT_ENDPOINTS` + Reader-mode gating — the M1 Loki
//! precedent, see `logs_api/mod.rs`).

mod assemble;
mod compat;
mod error;
mod graph;
mod handlers;
mod legacy;
mod metrics;
mod metrics_response;
mod negotiate;
mod params;
mod querytext;
mod search;
mod search_response;
mod tags;
mod tags_response;

use axum::Router;
use axum::routing::get;

use crate::app::AppState;

/// The native `/api/traces/v1` surface (docs/api.md §4.1-§4.4):
/// `GET`-only on all routes; the `/json` sibling forces the JSON
/// representation. `.route`-only composition — like `writer_router`, this
/// is not a pinned body in the #36 manifest.
pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/traces/v1/trace/{traceId}", get(handlers::trace_by_id))
        .route(
            "/api/traces/v1/trace/{traceId}/json",
            get(handlers::trace_by_id_json),
        )
        .route("/api/traces/v1/search", get(search::search))
        .route("/api/traces/v1/tags", get(tags::tags))
        .route("/api/traces/v1/tag/{tag}/values", get(tags::tag_values))
        .route(
            "/api/traces/v1/metrics/query_range",
            get(metrics::metrics_query_range),
        )
        .route("/api/traces/v1/metrics/query", get(metrics::metrics_query))
        // Service graph (M7-E1, issue #173): a PulsusDB-native surface with
        // NO Tempo-compat alias (the interop reference has no service-graph
        // HTTP endpoint), so it joins `router()` only, never `compat_router`.
        .route("/api/traces/v1/service_graph", get(graph::service_graph))
}

/// The Tempo-compat alias surface (docs/api.md §8.1, issue #61) — 13
/// `GET` routes, all literal paths (so the #36 drift guard extracts them
/// directly; no mount helper, no pinning needed). Eight are pure route
/// bindings onto the native handlers (byte-identical responses by
/// construction — same fn, same plan); the four tag-discovery aliases
/// reshape to Tempo's v1 flat / v2 (no `truncated`) wire shapes
/// (`compat.rs`); `/api/echo` is a constant. Mounting this router at all
/// is `crate::compat::apply_aliases`'s job (flag + Reader-mode gated);
/// this fn just builds the route set. The alias `/api/traces/{traceId}`
/// coexists with native `/api/traces/v1/...`: the literal `v1` segment
/// wins over the `{traceId}` param (docs/api.md §8.1's routing note).
pub(crate) fn compat_router() -> Router<AppState> {
    Router::new()
        .route("/api/traces/{traceId}", get(handlers::trace_by_id))
        .route(
            "/api/traces/{traceId}/json",
            get(handlers::trace_by_id_json),
        )
        .route("/tempo/api/traces/{traceId}", get(handlers::trace_by_id))
        .route("/api/search", get(search::search))
        .route("/api/search/tags", get(compat::tags_v1))
        .route("/api/search/tag/{tag}/values", get(compat::tag_values_v1))
        .route("/api/v2/search/tags", get(compat::tags_v2))
        .route(
            "/api/v2/search/tag/{tag}/values",
            get(compat::tag_values_v2),
        )
        .route("/api/echo", get(compat::echo))
        .route(
            "/api/metrics/query_range",
            get(metrics::metrics_query_range),
        )
        .route("/api/metrics/query", get(metrics::metrics_query))
        .route(
            "/tempo/api/metrics/query_range",
            get(metrics::metrics_query_range),
        )
        .route("/tempo/api/metrics/query", get(metrics::metrics_query))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn router_constructs_without_panicking() {
        let _ = router();
    }

    /// Guards the alias/native route-overlap edge case (issue #61 plan
    /// risk 1): `/api/traces/{traceId}` vs `/api/traces/v1/trace/...`
    /// must coexist — a matchit conflict would panic at construction.
    #[test]
    fn compat_router_constructs_without_panicking() {
        let _ = compat_router();
    }

    #[test]
    fn native_and_compat_routers_merge_without_conflicts() {
        let _ = router().merge(compat_router());
    }
}
