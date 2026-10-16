//! One id per request (issue #614). Test-first stub: the header name only.

use axum::http::HeaderName;

/// The response header carrying a request's id.
#[allow(dead_code)]
pub(crate) const QUERY_ID_HEADER: HeaderName = HeaderName::from_static("x-pulsus-query-id");
