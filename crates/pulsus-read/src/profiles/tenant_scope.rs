//! Issue #820: every read of a profile table leads its `WHERE` with the
//! tenant term — the traces check, called over the profile tables.

use crate::traces::tenant_scope::TenantTerm;

/// The profile tables this part's schema holds (#813). P18 and P19 append theirs.
pub const PROFILE_TABLES: [&str; 6] = [
    "profile_landing",
    "profile_samples",
    "profile_entries",
    "profile_series",
    "profile_stacks",
    "profile_locations",
];

/// The names `check` scopes: the six, and the four wrappers under
/// `dist_suffix` when it is not empty.
pub fn tables(dist_suffix: &str) -> Vec<String> {
    let _ = dist_suffix;
    Vec::new()
}

/// Whether every read of a profile table in `sql`, subqueries included, leads
/// its `WHERE` with `term` — the traces check over the profile tables.
pub fn check(sql: &str, term: &TenantTerm, tables: &[String]) -> Result<(), String> {
    let _ = (sql, term, tables);
    Err("not implemented".to_string())
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
}
