//! DDL rendering: explicit `{{token}}` string substitution (never a
//! template-engine dependency — docs/schemas.md's DDL is byte-authoritative
//! and the parameter set is small and fixed) plus a structural engine-swap
//! pass for clustered deployments. Double-brace tokens never collide with
//! ClickHouse's own single-brace `{shard}`/`{replica}` macros, which must
//! survive verbatim into `Replicated*` engine arguments.

use std::time::Duration;

/// The config-derived context every DDL block renders against. Re-exported
/// from `pulsus-schema` as `SchemaParams` — the same struct doubles as the
/// public `run_init`/`reconcile` parameter (derived from `Config` once, in
/// `pulsus-server`) and the internal rendering context, so there is exactly
/// one config-shaped struct in this crate rather than two kept in sync by
/// hand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderCtx {
    /// `CLICKHOUSE_DB` / `clickhouse.database` (docs/configuration.md §2).
    pub db: String,
    /// `PULSUS_CLUSTER` (docs/configuration.md §4). `None` = single-node:
    /// plain `MergeTree`-family engines, no `_dist` wrappers, no `ON
    /// CLUSTER`. `Some(name)` = clustered: `Replicated*` engines, `_dist`
    /// wrappers, `ON CLUSTER '<name>'` on every DDL statement.
    pub cluster: Option<String>,
    /// `PULSUS_DIST_SUFFIX` (docs/configuration.md §4, default `_dist`).
    pub dist_suffix: String,
    /// `PULSUS_STORAGE_POLICY` (docs/configuration.md §3). Injected as a
    /// `storage_policy` table SETTING when set.
    pub storage_policy: Option<String>,
    /// `PULSUS_RETENTION_DAYS` (docs/configuration.md §3, default 7).
    pub retention_days: u32,
    /// `PULSUS_LOG_ROLLUP_RESOLUTION` (docs/configuration.md §3, default
    /// 5s). Sets both the `log_metrics_<res>` table-name suffix and the
    /// bucket-floor expression in its materialized view.
    pub log_rollup: Duration,
}

/// Table families that must shard byte-identically (docs/schemas.md §7):
/// every raw/series/index/tier table in a family carries the exact same
/// `Distributed(...)` sharding expression, or a series' rollups silently
/// land on a different shard than its samples.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Family {
    Metrics,
    Logs,
    Traces,
}

impl Family {
    /// The single source of truth for a family's sharding expression
    /// (docs/schemas.md §7). Every `_dist` wrapper in a family renders this
    /// exact string — never a per-table copy.
    pub const fn sharding_expr(self) -> &'static str {
        match self {
            Family::Metrics => "cityHash64(metric_name, fingerprint)",
            Family::Logs => "fingerprint",
            Family::Traces => "cityHash64(trace_id)",
        }
    }
}

/// Renders the human-readable rollup-resolution suffix used in
/// `log_metrics_<res>` (docs/schemas.md §3.1), e.g. `5s`, `500ms`, `2m`.
/// Whole units are preferred over sub-units so the default (`5s`) matches
/// the documented table name exactly; a duration that isn't a whole number
/// of any unit falls back to milliseconds.
pub fn rollup_suffix(d: Duration) -> String {
    let nanos = d.as_nanos();
    if nanos == 0 {
        return "0ms".to_string();
    }
    let millis = d.as_millis();
    if millis > 0 && nanos.is_multiple_of(1_000_000) {
        if millis.is_multiple_of(60_000) {
            return format!("{}m", millis / 60_000);
        }
        if millis.is_multiple_of(1_000) {
            return format!("{}s", millis / 1_000);
        }
        return format!("{millis}ms");
    }
    format!("{nanos}ns")
}

/// Applies every `{{token}}` substitution defined by [`RenderCtx`]. Never
/// touches ClickHouse's own single-brace `{shard}`/`{replica}` macros — they
/// are simply absent from double-brace matching.
pub(crate) fn substitute_tokens(tmpl: &str, ctx: &RenderCtx) -> String {
    substitute_tokens_with(tmpl, ctx, &ctx.retention_days.to_string())
}

/// Same as [`substitute_tokens`], but `{{retention_days}}` is substituted
/// with `retention_repr` rather than `ctx.retention_days` — the seam
/// [`identity_ddl`] uses to exclude the mutable retention value from
/// migration identity (issue #5 fix plan F1) while every other token still
/// renders normally.
fn substitute_tokens_with(tmpl: &str, ctx: &RenderCtx, retention_repr: &str) -> String {
    let on_cluster = match &ctx.cluster {
        Some(name) => format!(" ON CLUSTER '{}'", escape_literal(name)),
        None => String::new(),
    };
    let cluster_name = ctx.cluster.clone().unwrap_or_default();
    let log_rollup_ns = ctx.log_rollup.as_nanos().to_string();

    tmpl.replace("{{db}}", &ctx.db)
        .replace("{{on_cluster}}", &on_cluster)
        .replace("{{cluster}}", &cluster_name)
        .replace("{{dist_suffix}}", &ctx.dist_suffix)
        .replace("{{retention_days}}", retention_repr)
        .replace("{{log_rollup_suffix}}", &rollup_suffix(ctx.log_rollup))
        .replace("{{log_rollup_ns}}", &log_rollup_ns)
}

/// Escapes a single-quoted SQL string literal. Config-derived, not
/// adversarial input (operator-supplied cluster/db names), but cheap
/// insurance against a stray `'` producing invalid DDL rather than a clear
/// syntax error.
fn escape_literal(s: &str) -> String {
    s.replace('\'', "''")
}

/// Renders a table/view *name* template (may contain `{{log_rollup_suffix}}`
/// but never `{{on_cluster}}`) against `ctx`. Used both to compute the
/// fully-resolved name for bookkeeping/zoo-path purposes and as an input to
/// [`render`].
pub fn render_name(name_tmpl: &str, ctx: &RenderCtx) -> String {
    substitute_tokens(name_tmpl, ctx)
}

/// Renders one DDL template end to end: token substitution, then (clustered
/// mode only) the `MergeTree`-family → `Replicated*` engine swap, then
/// (always) storage-policy `SETTINGS` injection. `global` selects the zoo
/// path scope for the clustered engine swap (issue #5 fix plan F2): `true`
/// for the shard-less, cluster-wide bookkeeping/catalog replica set
/// (`Migration.replication == Replication::Global`), `false` for the normal
/// per-shard data tables. Ignored outside clustered mode and by statements
/// with no `ENGINE = ` clause (views, `CREATE DATABASE`).
pub fn render(tmpl: &str, resolved_table_name: &str, ctx: &RenderCtx, global: bool) -> String {
    let mut s = substitute_tokens(tmpl, ctx);
    if ctx.cluster.is_some() {
        s = swap_engine(&s, resolved_table_name, ctx, global);
    }
    inject_storage_policy(&s, ctx.storage_policy.as_deref())
}

/// Renders DDL for **migration-identity** purposes only: checksummed by
/// `apply_migration`, never executed. Mutable operational config —
/// `{{retention_days}}` (replaced with a fixed sentinel, never the real
/// value) and `storage_policy` (`inject_storage_policy` is skipped entirely)
/// — is excluded from the identity, so changing `PULSUS_RETENTION_DAYS` or
/// `PULSUS_STORAGE_POLICY` after first init does not trip `MigrationDrift`
/// (issue #5 fix plan F1). Everything else — db, cluster clause, engine
/// family + `Replicated` swap (incl. `global`'s zoo-path scope), sharding
/// expressions, columns/CODECs/order/partition — is identity-bearing, so a
/// genuine structural/template change still drifts hard.
pub fn identity_ddl(
    tmpl: &str,
    resolved_table_name: &str,
    ctx: &RenderCtx,
    global: bool,
) -> String {
    let mut s = substitute_tokens_with(tmpl, ctx, RETENTION_IDENTITY_SENTINEL);
    if ctx.cluster.is_some() {
        s = swap_engine(&s, resolved_table_name, ctx, global);
    }
    s
}

/// Placeholder substituted for `{{retention_days}}` in [`identity_ddl`].
/// Deliberately not a valid ClickHouse numeral (identity DDL is checksummed
/// only, never executed) so it can never be mistaken for — or collide
/// with — a real rendered retention value.
const RETENTION_IDENTITY_SENTINEL: &str = "__PULSUS_IDENTITY_RETENTION_DAYS__";

/// The explicit zoo path convention (task-manager resolution #2,
/// docs/schemas.md §7): `/clickhouse/tables/{shard}/<db>.<table>` +
/// `{replica}` for per-shard replica sets, or
/// `/clickhouse/tables/all/<db>.<table>` for the shard-less, cluster-wide
/// replica set that catalog/bookkeeping tables join (`global = true`, issue
/// #5 fix plan F2; docs/schemas.md §7 — requires `{replica}` macros unique
/// across the whole cluster, not merely per shard). `{shard}`/`{replica}`
/// are ClickHouse server macros, left verbatim; `<db>` is already-rendered
/// (never a `{{db}}` token, so a second substitution pass can't
/// double-render it).
fn zoo_path(db: &str, table: &str, global: bool) -> String {
    let shard_slot = if global { "all" } else { "{shard}" };
    format!("'/clickhouse/tables/{shard_slot}/{db}.{table}', '{{replica}}'")
}

/// Swaps a rendered CREATE statement's base engine for its `Replicated*`
/// counterpart, in place, textually locating the `ENGINE = <Name>[(...)]`
/// clause. Unknown/view-less engines (anything not matched below) are left
/// untouched — DDL statements with no `ENGINE = ` clause (`CREATE DATABASE`,
/// `CREATE MATERIALIZED VIEW ... TO ...`) simply pass through unchanged.
fn swap_engine(ddl: &str, table: &str, ctx: &RenderCtx, global: bool) -> String {
    const MARKER: &str = "ENGINE = ";
    let Some(pos) = ddl.find(MARKER) else {
        return ddl.to_string();
    };
    let after = &ddl[pos + MARKER.len()..];
    let name_end = after.find(['(', '\n', ' ', ';']).unwrap_or(after.len());
    let engine_name = &after[..name_end];
    let rest = &after[name_end..];

    let (args, tail) = if let Some(body) = rest.strip_prefix('(') {
        match body.find(')') {
            Some(close) => (Some(&body[..close]), &body[close + 1..]),
            None => (None, rest), // malformed; leave untouched below
        }
    } else {
        (None, rest)
    };

    let zoo = zoo_path(&ctx.db, table, global);
    let new_engine = match engine_name {
        "MergeTree" => format!("ReplicatedMergeTree({zoo})"),
        "ReplacingMergeTree" => match args.map(str::trim).filter(|a| !a.is_empty()) {
            Some(version_col) => format!("ReplicatedReplacingMergeTree({zoo}, {version_col})"),
            None => format!("ReplicatedReplacingMergeTree({zoo})"),
        },
        "AggregatingMergeTree" => format!("ReplicatedAggregatingMergeTree({zoo})"),
        _ => return ddl.to_string(), // not a MergeTree-family engine (or malformed args above)
    };

    format!("{}{}{}{}", &ddl[..pos], MARKER, new_engine, tail)
}

/// Appends (or extends an existing) `storage_policy` table SETTING, only for
/// statements that carry a `MergeTree`-family `ENGINE = ` clause (covers
/// both plain and `Replicated*` forms, since the latter still contains the
/// substring `MergeTree`). Views, `CREATE DATABASE`, and `Distributed`
/// wrappers (`Distributed` does not accept `storage_policy`) are left
/// unchanged.
fn inject_storage_policy(ddl: &str, storage_policy: Option<&str>) -> String {
    let Some(policy) = storage_policy else {
        return ddl.to_string();
    };
    if !ddl.contains("MergeTree") {
        return ddl.to_string();
    }
    let setting = format!("storage_policy = '{}'", escape_literal(policy));
    if let Some(pos) = ddl.rfind("SETTINGS ") {
        let split = pos + "SETTINGS ".len();
        format!("{}{}, {}", &ddl[..split], setting, &ddl[split..])
    } else {
        let trimmed = ddl.trim_end();
        let body = trimmed.strip_suffix(';').unwrap_or(trimmed);
        format!("{body}\nSETTINGS {setting};\n")
    }
}

/// Renders the `_dist` `Distributed` wrapper template for `table` in
/// `family` (docs/schemas.md §7). Every family table's wrapper is built from
/// this one function, so [`Family::sharding_expr`] is the single source of
/// truth invariant holds structurally, not by convention.
pub fn dist_ddl_template(table: &str, family: Family) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {{{{db}}}}.{table}{{{{dist_suffix}}}}{{{{on_cluster}}}} AS {{{{db}}}}.{table}\n\
         ENGINE = Distributed('{{{{cluster}}}}', {{{{db}}}}, {table}, {expr});\n",
        expr = family.sharding_expr(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> RenderCtx {
        RenderCtx {
            db: "pulsus".to_string(),
            cluster: None,
            dist_suffix: "_dist".to_string(),
            storage_policy: None,
            retention_days: 7,
            log_rollup: Duration::from_secs(5),
        }
    }

    #[test]
    fn substitute_tokens_replaces_db_and_leaves_ch_macros_verbatim() {
        let tmpl = "CREATE TABLE {{db}}.x{{on_cluster}} ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{{db}}.x', '{replica}')";
        let out = substitute_tokens(tmpl, &ctx());
        assert!(out.contains("pulsus.x"));
        assert!(out.contains("{shard}"));
        assert!(out.contains("{replica}"));
        assert!(!out.contains("{{"));
    }

    #[test]
    fn on_cluster_is_empty_when_no_cluster_configured() {
        let out = substitute_tokens("t{{on_cluster}}", &ctx());
        assert_eq!(out, "t");
    }

    #[test]
    fn on_cluster_renders_quoted_cluster_name() {
        let mut c = ctx();
        c.cluster = Some("prod".to_string());
        let out = substitute_tokens("t{{on_cluster}}", &c);
        assert_eq!(out, "t ON CLUSTER 'prod'");
    }

    #[test]
    fn rollup_suffix_prefers_whole_seconds() {
        assert_eq!(rollup_suffix(Duration::from_secs(5)), "5s");
    }

    #[test]
    fn rollup_suffix_prefers_whole_minutes_over_seconds() {
        assert_eq!(rollup_suffix(Duration::from_secs(120)), "2m");
    }

    #[test]
    fn rollup_suffix_falls_back_to_millis() {
        assert_eq!(rollup_suffix(Duration::from_millis(500)), "500ms");
    }

    #[test]
    fn rollup_suffix_falls_back_to_nanos_for_sub_millisecond() {
        assert_eq!(rollup_suffix(Duration::from_nanos(123)), "123ns");
    }

    #[test]
    fn swap_engine_rewrites_plain_mergetree_with_zoo_path() {
        let tmpl = "CREATE TABLE {{db}}.metric_samples{{on_cluster}} (x UInt8) ENGINE = MergeTree\nORDER BY x;";
        let mut c = ctx();
        c.cluster = Some("prod".to_string());
        let out = render(tmpl, "metric_samples", &c, false);
        assert!(out.contains(
            "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/pulsus.metric_samples', '{replica}')"
        ));
        assert!(out.contains("ORDER BY x;"));
        assert!(out.contains("ON CLUSTER 'prod'"));
    }

    #[test]
    fn swap_engine_uses_the_shard_less_all_zoo_path_when_global() {
        let tmpl = "CREATE TABLE {{db}}.schema_migrations{{on_cluster}} (x UInt8) ENGINE = ReplacingMergeTree(x)\nORDER BY x;";
        let mut c = ctx();
        c.cluster = Some("prod".to_string());
        let out = render(tmpl, "schema_migrations", &c, true);
        assert!(out.contains(
            "ReplicatedReplacingMergeTree('/clickhouse/tables/all/pulsus.schema_migrations', '{replica}', x)"
        ));
        assert!(!out.contains("{shard}"));
    }

    #[test]
    fn swap_engine_rewrites_replacing_mergetree_with_version_column() {
        let tmpl = "CREATE TABLE {{db}}.log_streams{{on_cluster}} (x UInt8) ENGINE = ReplacingMergeTree(updated_ns)\nORDER BY x;";
        let mut c = ctx();
        c.cluster = Some("prod".to_string());
        let out = render(tmpl, "log_streams", &c, false);
        assert!(out.contains(
            "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/pulsus.log_streams', '{replica}', updated_ns)"
        ));
    }

    #[test]
    fn swap_engine_rewrites_bare_replacing_mergetree_without_version_column() {
        let tmpl = "CREATE TABLE {{db}}.log_streams_idx{{on_cluster}} (x UInt8) ENGINE = ReplacingMergeTree\nORDER BY x;";
        let mut c = ctx();
        c.cluster = Some("prod".to_string());
        let out = render(tmpl, "log_streams_idx", &c, false);
        assert!(out.contains(
            "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/pulsus.log_streams_idx', '{replica}')"
        ));
        assert!(!out.contains("ReplicatedReplacingMergeTree(...,"));
    }

    #[test]
    fn swap_engine_rewrites_aggregating_mergetree() {
        let tmpl = "CREATE TABLE {{db}}.log_metrics_5s{{on_cluster}} (x UInt8) ENGINE = AggregatingMergeTree\nORDER BY x;";
        let mut c = ctx();
        c.cluster = Some("prod".to_string());
        let out = render(tmpl, "log_metrics_5s", &c, false);
        assert!(out.contains(
            "ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/pulsus.log_metrics_5s', '{replica}')"
        ));
    }

    #[test]
    fn engine_is_not_swapped_in_single_node_mode() {
        let tmpl = "CREATE TABLE {{db}}.metric_samples{{on_cluster}} (x UInt8) ENGINE = MergeTree\nORDER BY x;";
        let out = render(tmpl, "metric_samples", &ctx(), false);
        assert!(out.contains("ENGINE = MergeTree"));
        assert!(!out.contains("Replicated"));
    }

    #[test]
    fn storage_policy_appends_to_an_existing_settings_clause() {
        let mut c = ctx();
        c.storage_policy = Some("hot_cold".to_string());
        let tmpl = "CREATE TABLE {{db}}.metric_samples{{on_cluster}} (x UInt8) ENGINE = MergeTree\nORDER BY x\nSETTINGS ttl_only_drop_parts = 1;";
        let out = render(tmpl, "metric_samples", &c, false);
        assert!(out.contains("SETTINGS storage_policy = 'hot_cold', ttl_only_drop_parts = 1;"));
    }

    #[test]
    fn storage_policy_adds_a_new_settings_clause_when_absent() {
        let mut c = ctx();
        c.storage_policy = Some("hot_cold".to_string());
        let tmpl = "CREATE TABLE {{db}}.metric_series{{on_cluster}} (x UInt8) ENGINE = MergeTree\nORDER BY x;";
        let out = render(tmpl, "metric_series", &c, false);
        assert!(out.contains("SETTINGS storage_policy = 'hot_cold';"));
    }

    #[test]
    fn storage_policy_is_not_injected_into_non_mergetree_statements() {
        let mut c = ctx();
        c.storage_policy = Some("hot_cold".to_string());
        let out = render(
            "CREATE DATABASE IF NOT EXISTS {{db}}{{on_cluster}};",
            "",
            &c,
            false,
        );
        assert!(!out.contains("storage_policy"));
    }

    #[test]
    fn identity_ddl_excludes_retention_days_from_the_checksum_surface() {
        let tmpl = "CREATE TABLE {{db}}.metric_samples{{on_cluster}} (x UInt8) ENGINE = MergeTree\n\
                    ORDER BY x\nTTL x + INTERVAL {{retention_days}} DAY DELETE;";
        let mut c7 = ctx();
        c7.retention_days = 7;
        let mut c30 = ctx();
        c30.retention_days = 30;
        let identity7 = identity_ddl(tmpl, "metric_samples", &c7, false);
        let identity30 = identity_ddl(tmpl, "metric_samples", &c30, false);
        assert_eq!(
            identity7, identity30,
            "changing retention_days must not change migration identity"
        );
        assert!(!identity7.contains('7'));
        assert!(!identity7.contains("30"));
    }

    #[test]
    fn identity_ddl_excludes_storage_policy_from_the_checksum_surface() {
        let tmpl = "CREATE TABLE {{db}}.metric_series{{on_cluster}} (x UInt8) ENGINE = MergeTree\nORDER BY x;";
        let mut plain = ctx();
        let mut policied = ctx();
        policied.storage_policy = Some("hot_cold".to_string());
        plain.storage_policy = None;
        let identity_plain = identity_ddl(tmpl, "metric_series", &plain, false);
        let identity_policied = identity_ddl(tmpl, "metric_series", &policied, false);
        assert_eq!(
            identity_plain, identity_policied,
            "changing storage_policy must not change migration identity"
        );
        assert!(!identity_policied.contains("storage_policy"));
    }

    #[test]
    fn identity_ddl_still_reflects_structural_changes() {
        let mut c = ctx();
        c.cluster = Some("prod".to_string());
        let tmpl_a =
            "CREATE TABLE {{db}}.x{{on_cluster}} (x UInt8) ENGINE = MergeTree\nORDER BY x;";
        let tmpl_b = "CREATE TABLE {{db}}.x{{on_cluster}} (x UInt8, y UInt8) ENGINE = MergeTree\nORDER BY x;";
        assert_ne!(
            identity_ddl(tmpl_a, "x", &c, false),
            identity_ddl(tmpl_b, "x", &c, false),
            "a genuine column change must still change migration identity"
        );
    }

    #[test]
    fn dist_ddl_template_uses_the_family_sharding_expr() {
        let tmpl = dist_ddl_template("metric_samples", Family::Metrics);
        let out = render(&tmpl, "metric_samples", &ctx(), false);
        assert!(out.contains("cityHash64(metric_name, fingerprint)"));
        assert!(out.contains("pulsus.metric_samples_dist"));
        assert!(out.contains("Distributed('', pulsus, metric_samples,"));
    }

    #[test]
    fn dist_ddl_template_uses_the_traces_family_sharding_expr() {
        let tmpl = dist_ddl_template("trace_spans", Family::Traces);
        let out = render(&tmpl, "trace_spans", &ctx(), false);
        assert!(out.contains("pulsus.trace_spans_dist"));
        assert!(out.contains("Distributed('', pulsus, trace_spans, cityHash64(trace_id))"));
    }

    #[test]
    fn family_sharding_expressions_are_distinct_and_stable() {
        assert_eq!(
            Family::Metrics.sharding_expr(),
            "cityHash64(metric_name, fingerprint)"
        );
        assert_eq!(Family::Logs.sharding_expr(), "fingerprint");
        assert_eq!(Family::Traces.sharding_expr(), "cityHash64(trace_id)");
    }
}
