//! The traces read path's single route to ClickHouse (issue #509).
//!
//! # The invariant
//!
//! **[`TraceDispatch`] owns the only [`ChClient`] the traces read path
//! can reach, and its field is private to this module.** Every query the
//! engine executes goes through [`TraceDispatch::query_stream`], which
//! doubles the query text's `?` bytes and applies the query-text
//! admission guard before the driver sees anything. `traces::exec` cannot
//! name the client, so a read added there tomorrow cannot skip either
//! step: `self.dispatch.client.query_stream(...)` is an `E0616`.
//!
//! # Why the boundary exists rather than one more escape call
//!
//! The driver we vendor reads a bare `?` in query TEXT as an unbound bind
//! placeholder (`vendor/clickhouse/src/sql/mod.rs:47-70` — `SqlBuilder::new`
//! scans with `rest.find('?')`; `??` collapses back to one literal `?`, a
//! lone `?` becomes `Part::Arg`, and `?fields` becomes `Part::Fields`, and
//! `finish()` at `:107-131` fails an unbound `Part::Arg` with "invalid
//! SQL: unbound query argument"). Our SQL is always fully rendered text
//! with no bind arguments, so every `?` in it is literal and must be
//! doubled.
//!
//! LogQL and the metrics dispatch each have one function every read
//! passes through, which is why neither was affected. The traces engine
//! had no such function: 27 execution sites kept the rule by hand and 3
//! did not, and one of those three — the unnarrowed tag-values catalog
//! read — carries a user-supplied attribute key. An OTLP attribute named
//! `http.target?raw` is an ordinary thing to record; our own tag-names
//! route advertised it, and the values request built from that answer was
//! a `500`. Two further shapes were WORSE than an error: an even run of
//! `?` collapsed in the driver so ClickHouse was asked for a different
//! key, and `?fields` was substituted with the row's column list — both
//! answered `200` with an empty list.
//!
//! A rule kept by 27 call sites is broken by the 28th. This module is the
//! place the rule now lives.

use pulsus_clickhouse::{ChClient, ChError, ChRow, ChRowStream, QuerySettings};

use crate::logql::error::ReadError;
use crate::logql::exec::escape_query_placeholders;

/// The traces read path's ClickHouse handle. Construct one per
/// [`super::exec::TraceEngine`]; the wrapped client is never handed out.
///
/// No `Debug`: [`ChClient`] has none, and `TraceEngine` — the only thing
/// that holds one of these — has none either.
pub(super) struct TraceDispatch {
    client: ChClient,
}

impl TraceDispatch {
    pub(super) fn new(client: ChClient) -> Self {
        Self { client }
    }

    /// Executes one already-rendered SQL statement and returns its row
    /// stream.
    ///
    /// Three things happen here and nowhere else on this path:
    ///
    /// 1. every literal `?` is doubled, so the driver's placeholder
    ///    scanner puts the byte back verbatim;
    /// 2. [`crate::querytext::ensure_query_text_fits`] runs against the
    ///    FINAL text — the escaped form is never shorter than the input,
    ///    so the guard can only be tighter here than it was at the old
    ///    per-site positions;
    /// 3. the caller's own `map_err` classifies the `ChError`, so the
    ///    generator/metrics/read error taxonomies stay distinct (issue
    ///    #57 re-audit) despite the single choke point.
    ///
    /// The escaped buffer is local and that is sound:
    /// [`ChRowStream`] holds the pooled connection
    /// (`crates/pulsus-clickhouse/src/client.rs:343-349`), never the SQL
    /// text, so the returned stream borrows `self` and not `sql`.
    pub(super) async fn query_stream<'a, R, F>(
        &'a self,
        sql: &str,
        settings: &QuerySettings,
        map_err: F,
    ) -> Result<ChRowStream<'a, R>, ReadError>
    where
        R: ChRow,
        F: FnOnce(ChError) -> ReadError,
    {
        let sql = escape_query_placeholders(sql);
        crate::querytext::ensure_query_text_fits(&sql).map_err(ReadError::QueryTooBroad)?;
        self.client
            .query_stream::<R>(&sql, settings)
            .await
            .map_err(map_err)
    }
}

#[cfg(test)]
mod tests {
    /// Issue #509 criterion 5: the escaping happens HERE and only here.
    ///
    /// The choke point itself is enforced by the compiler — `client` is
    /// private to this module, so a read in `exec.rs` cannot reach the
    /// driver another way. What the compiler cannot see is a site that
    /// escapes a SECOND time before handing its text over: the text is
    /// then doubled twice, the driver collapses `????` to `??`, and the
    /// query matches nothing with no error at all. This test is that
    /// check, as a source-text property of the two files.
    ///
    /// **Its scope is literally these two files.** It says nothing about
    /// a module added to `traces/` later; module privacy is what covers
    /// that, and module privacy is an argument about the compiler rather
    /// than a test.
    #[test]
    fn no_hand_escaping_left_in_exec() {
        let exec = include_str!("exec.rs");
        assert_eq!(
            exec.matches("escape_query_placeholders").count(),
            0,
            "traces/exec.rs must not escape query text by hand — the dispatcher does it once, \
             and a second application silently matches nothing"
        );
        // The needle is assembled from two pieces so that THIS line does
        // not itself contain the sequence it searches for — written out
        // whole, the literal below would be a second occurrence and the
        // count would be 2 no matter what the code did.
        let dispatch = include_str!("dispatch.rs");
        assert_eq!(
            dispatch
                .matches(concat!("escape_query_placeholders", "("))
                .count(),
            1,
            "traces/dispatch.rs must apply the escape exactly once"
        );
    }
}
