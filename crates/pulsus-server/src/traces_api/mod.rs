//! `/api/traces/v1` — the M4 trace-by-ID fetch endpoints (issue #55,
//! docs/api.md §4.1). Thin handlers (`handlers.rs`): parse the hex trace
//! id (`params.rs`) → point-read via `TraceEngine` (`pulsus-read`) →
//! decode/dedup/merge/encode the OTLP `TracesData` (`assemble.rs`) →
//! negotiate the representation (`negotiate.rs`) → envelope errors
//! (`error.rs`). All SQL/execution stays in `pulsus-read` — this module
//! only ever talks to it through `TraceEngine`'s public methods; all OTLP
//! shaping stays here so `pulsus-read` stays OTLP-agnostic (task-manager
//! adjudication on issue #55).
//!
//! `/api/traces/v1/search` (issue #57, docs/api.md §4.2) lives here too:
//! `search.rs` (handler), `params.rs` (search params), `legacy.rs`
//! (logfmt → TraceQL), `search_response.rs` (documented JSON shaping) —
//! planning/execution stay in `pulsus-read::traces`.
//!
//! Out of scope here (see the #19 decomposition): tags (T6), TraceQL
//! metrics (T7), and the Tempo compat aliases
//! (`/api/traces/{traceId}[/json]`, T9 — a pure route binding onto these
//! same handlers, docs/api.md §8.1).

mod assemble;
mod error;
mod handlers;
mod legacy;
mod negotiate;
mod params;
mod search;
mod search_response;

use axum::Router;
use axum::routing::get;

use crate::app::AppState;

/// The native `/api/traces/v1` surface (docs/api.md §4.1/§4.2):
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn router_constructs_without_panicking() {
        let _ = router();
    }
}
