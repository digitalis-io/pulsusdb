//! One id per request (issue #587 for the trace fetch routes, issue #614
//! for every route behind authentication).
//!
//! The id is 32 lowercase hex characters, returned in the
//! `X-Pulsus-Query-Id` response header. Every read statement the request
//! issued carries it as `log_comment`, so an operator reads one header and
//! then that request's own rows out of `system.query_log`.
//!
//! [`stamp`] wraps the authenticated router outside the deadline and the
//! authentication layer: it mints the id, runs the request inside
//! [`REQUEST_ID`] and attaches the header to whatever comes back, those
//! layers' own `401`, `408` and `503` included. The engines take the id
//! where they are built ([`tag`]), which is before a tail's upgrade, so a
//! tail's statements carry it too.

use axum::extract::Request;
use axum::http::{HeaderName, HeaderValue};
use axum::middleware::Next;
use axum::response::Response;

/// The response header carrying a request's id.
pub(crate) const QUERY_ID_HEADER: HeaderName = HeaderName::from_static("x-pulsus-query-id");

tokio::task_local! {
    /// The id of the request this task is serving, set by [`stamp`].
    static REQUEST_ID: String;
}

/// The id of the request this task is serving, or `None` outside [`stamp`].
pub(crate) fn current() -> Option<String> {
    REQUEST_ID.try_with(Clone::clone).ok()
}

/// An engine whose statements can carry a `log_comment`.
pub(crate) trait LogComment: Sized {
    fn with_log_comment(self, comment: String) -> Self;
}

impl LogComment for pulsus_read::MetricsEngine {
    fn with_log_comment(self, comment: String) -> Self {
        self.with_query_log_comment(comment)
    }
}

impl LogComment for pulsus_read::LogQlEngine {
    fn with_log_comment(self, comment: String) -> Self {
        self.with_query_log_comment(comment)
    }
}

/// The trace engine numbers its statements `<id>-<n>` and tags each with
/// `log_comment = <id>` (`TraceDispatch::stamped`).
impl LogComment for pulsus_read::TraceEngine {
    fn with_log_comment(self, comment: String) -> Self {
        self.with_statement_prefix(&comment)
    }
}

/// The id of the request this task is serving, or a fresh one outside
/// [`stamp`] (a handler driven directly by a unit test).
pub(crate) fn current_or_new() -> String {
    current().unwrap_or_else(new_statement_prefix)
}

/// `engine`, its statements tagged with the id of the request this task
/// is serving; unchanged outside [`stamp`].
pub(crate) fn tag<E: LogComment>(engine: E) -> E {
    match current() {
        Some(id) => engine.with_log_comment(id),
        None => engine,
    }
}

/// The layer on the authenticated router: one id per request, in scope
/// while the request runs, and in the response header on every status.
pub(crate) async fn stamp(req: Request, next: Next) -> Response {
    let id = new_statement_prefix();
    let mut res = REQUEST_ID.scope(id.clone(), next.run(req)).await;
    let value = HeaderValue::from_str(&id).expect("the id is 32 hex characters");
    res.headers_mut().insert(QUERY_ID_HEADER, value);
    res
}

/// One prefix per request: **32 hex chars** of operating-system
/// randomness, so it is a legal `query_id` and two concurrent requests
/// cannot be given one.
///
/// **`getrandom` rather than `uuid`**, for the reason the workspace's own
/// entry gives: one opaque string per request is the same shape the write
/// path's token mint already declines `uuid` for, and `getrandom` adds no
/// crate to this binary's graph. A failure to read randomness falls back
/// to the monotonic clock plus a process-local counter, which is still
/// unique within a process and is the only thing left when the kernel's
/// own source is unavailable.
fn new_statement_prefix() -> String {
    match (getrandom::u64(), getrandom::u64()) {
        (Ok(hi), Ok(lo)) => format!("{hi:016x}{lo:016x}"),
        _ => {
            static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            format!("{nanos:016x}{n:016x}")
        }
    }
}
