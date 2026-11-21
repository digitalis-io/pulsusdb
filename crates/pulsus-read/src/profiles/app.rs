//! Stub (tests first). Issue #830: the exploration app's settings and its profile stats — the
//! three statements (plan §3) and their rows. Settings `Get` is S13-R, a
//! `Set` or a `Delete` is S13-W, one `INSERT … SELECT` whose guard refuses
//! a write older than the stored one with ClickHouse's `throwIf`, and the
//! stats are S11', read from the series table's days and hours.
//!
//! The version is computed in SQL: `modified_at` in the high 64 bits, the
//! hash of the value and the tombstone flag in the low, so the newest write
//! wins and two writes of one time resolve the same way on every server.

use pulsus_clickhouse::Row;
use pulsus_model::Tenant;
use serde::{Deserialize, Serialize};

use crate::traces::tenant_scope::TenantTerm;

/// The settings table (`schema/schema.sql`).
pub const SETTINGS_TABLE: &str = "profile_settings";

/// The guard's message, the reference's `errOldSetting` text.
pub const NEWER_WRITTEN: &str = "newer update already written";

/// One stored setting, as S13-R answers it.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct SettingRow {
    pub name: String,
    pub value: String,
    pub modified_at: i64,
}

/// S11''s one row.
#[derive(Debug, Clone, Default, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct StatsRow {
    pub ingested: bool,
    pub oldest_ms: i64,
    pub newest_ms: i64,
}

/// What a settings write did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Written {
    /// The row was inserted.
    Stored,
    /// A newer write is stored; nothing was inserted.
    Refused,
}

/// S13-R: the tenant's settings, each its newest value, tombstones left
/// out, in name order.
pub fn settings_get_sql(tenant: &Tenant) -> String {
    let _ = tenant;
    String::new()
}

/// S13-W: one row of `name` at `modified_at`, a tombstone when `deleted`,
/// inserted only when no write of `name` newer than `modified_at` is stored.
pub fn settings_write_sql(
    tenant: &Tenant,
    name: &str,
    value: &str,
    modified_at: i64,
    deleted: bool,
) -> String {
    let _ = (tenant, name, value, modified_at, deleted);
    String::new()
}

/// S11': whether the tenant holds a series, the start of the UTC hour of
/// its oldest profile and the last millisecond of the hour of its newest,
/// read from `series_table`.
pub fn stats_sql(tenant: &Tenant, series_table: &str) -> String {
    let _ = (tenant, series_table);
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t1() -> Tenant {
        let value = http::HeaderValue::from_static("t1");
        Tenant::from_header(Some(&value), false).expect("a valid tenant")
    }

    fn checked(sql: &str, suffix: &str) {
        let tables = crate::profiles::tenant_scope::tables(suffix);
        assert!(
            tables.iter().any(|t| t == SETTINGS_TABLE),
            "{SETTINGS_TABLE} is not a checked table: {tables:?}"
        );
        assert_eq!(
            crate::profiles::tenant_scope::check(sql, &TenantTerm::of(&t1()), &tables),
            Ok(()),
            "{sql}"
        );
    }

    /// A1: plan §3's S13-R, exactly; the tenant check passes.
    #[test]
    fn a1_get_sql() {
        let sql = settings_get_sql(&t1());
        assert_eq!(
            sql,
            "SELECT name, argMax(value, version) AS value, max(modified_at) AS modified_at
FROM profile_settings
WHERE org_id = 't1'
GROUP BY name
HAVING argMax(deleted, version) = 0
ORDER BY name"
        );
        checked(&sql, "");
    }

    /// A2: plan §3's S13-W, a value escaped in the row and in the hash, and
    /// the tombstone's flag in both; the tenant check passes.
    #[test]
    fn a2_write_sql() {
        let sql = settings_write_sql(&t1(), "a", "it's", 10, false);
        assert_eq!(
            sql,
            "INSERT INTO profile_settings (org_id, name, value, modified_at, deleted, version)
SELECT 't1', 'a', 'it\\'s', toInt64(10), toUInt8(0),
       bitShiftLeft(toUInt128(toInt64(10)), 64) + cityHash64('it\\'s', toUInt8(0))
WHERE throwIf((SELECT max(modified_at) FROM profile_settings WHERE org_id = 't1' AND name = 'a') > 10,
              'newer update already written') = 0"
        );
        checked(&sql, "");
        let tomb = settings_write_sql(&t1(), "a", "", 7, true);
        assert_eq!(
            tomb,
            "INSERT INTO profile_settings (org_id, name, value, modified_at, deleted, version)
SELECT 't1', 'a', '', toInt64(7), toUInt8(1),
       bitShiftLeft(toUInt128(toInt64(7)), 64) + cityHash64('', toUInt8(1))
WHERE throwIf((SELECT max(modified_at) FROM profile_settings WHERE org_id = 't1' AND name = 'a') > 7,
              'newer update already written') = 0"
        );
        checked(&tomb, "");
    }

    /// A3: plan §3's S11' over the series wrapper; the tenant check passes.
    #[test]
    fn a3_stats_sql() {
        let sql = stats_sql(&t1(), "profile_series_dist");
        assert_eq!(
            sql,
            "SELECT count() > 0 AS ingested,
       if(ingested, toInt64(min(toUInt32(day) * 24 + arrayMin(bitPositionsToArray(hours)))) * 3600000, 0) AS oldest_ms,
       if(ingested, (toInt64(max(toUInt32(day) * 24 + arrayMax(bitPositionsToArray(hours)))) + 1) * 3600000 - 1, 0) AS newest_ms
FROM profile_series_dist
WHERE org_id = 't1'"
        );
        checked(&sql, "_dist");
    }
}
