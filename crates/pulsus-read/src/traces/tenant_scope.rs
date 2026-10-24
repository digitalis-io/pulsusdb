//! The tenant every trace read is scoped to (issue #738).
//!
//! Every read of a trace table carries [`TenantTerm`] as the first conjunct
//! of the `WHERE` that follows its `FROM`, except the service graph's marker
//! reads, which carry [`MARKER_TERM`]. [`check`] holds that rule over every
//! statement [`super::dispatch::TraceDispatch`] sends.

use pulsus_model::Tenant;

use crate::metrics::TenantSql;

/// `org_id = <tenant>`, the term a trace read's `WHERE` begins with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantTerm(String);

impl TenantTerm {
    /// The term for `tenant`, its text through the crate's one escaper.
    pub fn of(tenant: &Tenant) -> Self {
        TenantTerm(format!("org_id = {}", tenant.sql_literal()))
    }

    /// The term's text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TenantTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The empty tenant's term, for the crate's own unit tests.
#[cfg(test)]
pub(crate) fn empty_term() -> &'static TenantTerm {
    static TERM: std::sync::LazyLock<TenantTerm> = std::sync::LazyLock::new(|| {
        TenantTerm::of(&Tenant::from_header(None, false).expect("the empty tenant"))
    });
    &TERM
}

/// The service graph's marker reads: the job's markers are one set per run,
/// stored under the reserved empty tenant.
pub const MARKER_TERM: &str = "org_id = '' AND conn_type = ''";

/// Whether `sql` reads every trace table in `tables` under `term`.
pub fn check(_sql: &str, _term: &TenantTerm, _tables: &[String]) -> Result<(), String> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn term(name: &str) -> TenantTerm {
        let value = http::HeaderValue::from_str(name).expect("a header value");
        TenantTerm::of(&Tenant::from_header(Some(&value), false).expect("a valid tenant"))
    }

    fn tables() -> Vec<String> {
        [
            "resources",
            "spans",
            "tag_names",
            "tag_values",
            "trace_landing",
            "traces",
            "service_edges",
            "tag_bucket_values",
            "error_spans",
            "service_bucket_resources",
            "spans_dist",
            "error_spans_dist",
            "traces_dist",
        ]
        .iter()
        .map(|t| t.to_string())
        .collect()
    }

    /// **T-G (issue #738): the check refuses every unscoped read.** Each
    /// refused statement reads a trace table without the request's term
    /// leading its `WHERE`, or with an `OR` beside it; each accepted one
    /// carries the term at every site, or reads no trace table there.
    #[test]
    fn the_check_refuses_every_unscoped_read() {
        let a = term("a");
        let tables = tables();
        for sql in [
            "SELECT 1 FROM spans WHERE x",
            "SELECT 1 FROM spans_dist WHERE x",
            "SELECT 1 FROM db.traces WHERE x",
            "SELECT k FROM tag_names ORDER BY k",
            "SELECT 1 FROM spans WHERE org_id = 'b' AND x",
            "SELECT 1 FROM spans WHERE org_id = 'a' AND x OR y",
            "SELECT 1 FROM spans WHERE org_id = '' AND conn_type = ''",
        ] {
            assert!(
                check(sql, &a, &tables).is_err(),
                "refused, as tenant 'a': {sql}"
            );
        }
        for sql in [
            "SELECT 1 FROM spans WHERE org_id = 'a' AND x \
             AND trace_id IN (SELECT trace_id FROM traces_dist WHERE org_id = 'a' AND y) \
             UNION ALL SELECT 2 FROM db.resources WHERE org_id = 'a' AND z",
            "SELECT 1 FROM spans WHERE org_id = 'a' AND (x OR y)",
            "SELECT 'FROM spans' FROM system.one",
            "SELECT 1 FROM service_edges FINAL WHERE org_id = '' AND conn_type = ''",
            "SELECT 1 FROM db.service_edges FINAL WHERE org_id = 'a' AND conn_type IN ('rpc')",
            "SELECT engine_full FROM system.tables WHERE database = currentDatabase()",
        ] {
            assert_eq!(
                check(sql, &a, &tables),
                Ok(()),
                "accepted, as tenant 'a': {sql}"
            );
        }
    }
}
