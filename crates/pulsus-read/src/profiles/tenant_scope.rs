//! Issue #820: every read of a profile table leads its `WHERE` with the
//! tenant term — the traces check, called over the profile tables.

use crate::traces::tenant_scope::TenantTerm;

/// The profile tables this part's schema holds (#813), and #830's
/// `profile_settings`, which has no wrapper. P19 appends its own.
pub const PROFILE_TABLES: [&str; 7] = [
    "profile_landing",
    "profile_samples",
    "profile_entries",
    "profile_series",
    "profile_stacks",
    "profile_locations",
    "profile_settings",
];

/// Tables that carry a `<dist_suffix>` wrapper (`schema/schema.sql`).
const WRAPPED: [&str; 4] = [
    "profile_landing",
    "profile_samples",
    "profile_entries",
    "profile_series",
];

/// The names `check` scopes: the seven, and the four wrappers under
/// `dist_suffix` when it is not empty.
pub fn tables(dist_suffix: &str) -> Vec<String> {
    let mut out: Vec<String> = PROFILE_TABLES.iter().map(|t| t.to_string()).collect();
    if !dist_suffix.is_empty() {
        out.extend(WRAPPED.iter().map(|t| format!("{t}{dist_suffix}")));
    }
    out
}

/// Whether every read of a profile table in `sql`, subqueries included, leads
/// its `WHERE` with `term` — the traces check over the profile tables.
pub fn check(sql: &str, term: &TenantTerm, tables: &[String]) -> Result<(), String> {
    crate::traces::tenant_scope::check(sql, term, tables)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulsus_model::Tenant;

    /// S1's expected text (`selector.rs`).
    const SERIES: &str = "SELECT fingerprint
FROM profile_series
WHERE org_id = 't1' AND day BETWEEN '2026-11-15' AND '2026-11-15'
  AND bitAnd(hours, multiIf(day = '2026-11-15' AND day = '2026-11-15', 6144, day = '2026-11-15', 16775168, day = '2026-11-15', 8191, 16777215)) != 0
  AND service = 'a'
  AND JSONExtractString(labels, 'env') != 'p'
  AND match(JSONExtractString(labels, 'x'), '(?-s)^(?:y.*)$')
  AND type_id = 'process_cpu:cpu:nanoseconds:cpu:nanoseconds'
  AND 0 * match('', '(?-s)^(?:y.*)$') = 0";

    fn t1() -> TenantTerm {
        let value = http::HeaderValue::from_static("t1");
        TenantTerm::of(&Tenant::from_header(Some(&value), false).expect("a valid tenant"))
    }

    /// T1: a profile table read without the tenant's term leading its
    /// `WHERE` is refused, naming that read — a nested one, a wrapper, and
    /// one under another tenant.
    #[test]
    fn t1_a_profile_read_without_the_tenant_is_refused() {
        let tables = tables("_dist");
        for (sql, names) in [
            (
                "SELECT 1 FROM profile_samples WHERE org_id = 't1' AND stack_id IN \
                 (SELECT stack_id FROM profile_stacks WHERE stack_id = 1)",
                "FROM profile_stacks",
            ),
            (
                "SELECT 1 FROM profile_series_dist WHERE day = today()",
                "FROM profile_series_dist",
            ),
            (
                "SELECT 1 FROM profile_locations WHERE org_id = 't2' AND x",
                "FROM profile_locations",
            ),
        ] {
            match check(sql, &t1(), &tables) {
                Err(e) => assert!(e.contains(names), "{sql}\n{e}"),
                Ok(()) => panic!("accepted: {sql}"),
            }
        }
    }

    /// T2: the shape every later route sends — a sample read over a
    /// wrapper, SERIES nested in it and a stack read beside it — passes.
    #[test]
    fn t2_the_route_shape_passes() {
        let sql = format!(
            "SELECT 1 FROM profile_samples_dist WHERE org_id = 't1' AND fingerprint IN ({SERIES}) \
             AND stack_id IN (SELECT stack_id FROM profile_stacks WHERE org_id = 't1' AND x)"
        );
        assert_eq!(check(&sql, &t1(), &tables("_dist")), Ok(()));
    }

    /// T3 (issue #831): `profile_adhoc` and its wrapper are scoped.
    #[test]
    fn t3_the_adhoc_table_is_scoped() {
        let tables = tables("_dist");
        for sql in [
            "SELECT 1 FROM profile_adhoc WHERE row_kind = 1",
            "SELECT 1 FROM profile_adhoc_dist WHERE row_kind = 1",
        ] {
            assert!(check(sql, &t1(), &tables).is_err(), "accepted: {sql}");
        }
        assert_eq!(
            check(
                "SELECT 1 FROM profile_adhoc_dist WHERE org_id = 't1' AND row_kind = 1",
                &t1(),
                &tables
            ),
            Ok(())
        );
    }
}
