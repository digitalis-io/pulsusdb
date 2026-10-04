//! Token substitution for `schema/schema.sql`: explicit `{{token}}` string
//! replacement, sixteen names and nothing else.
//!
//! **Double braces, always.** ClickHouse's own `{shard}` and `{replica}`
//! macros are single-brace and must survive verbatim into `Replicated*`
//! engine arguments; a substitution expression that matched a single brace
//! pair would eat them, and the server accepts the result without complaint
//! — every shard then joins one replica set. Double-brace tokens cannot
//! match them.

use std::time::Duration;

/// Config-derived rendering context, and the parameter the test toolkit's
/// `run_init` takes. The same struct doubles as both so there is exactly one
/// config-shaped struct in this crate rather than two kept in sync by hand.
pub type SchemaParams = RenderCtx;

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
    /// `PULSUS_METRICS_LANDING_RETENTION_HOURS` (issue #603): the metrics
    /// landing table's delete-TTL, in hours.
    pub metrics_landing_retention_hours: u32,
    /// `PULSUS_METRICS_DEDUP_WINDOW` (issue #603): the block-deduplication
    /// window the landing table and the four derived metric tables carry.
    pub metrics_dedup_window: u64,
    /// `PULSUS_LOG_LANDING_RETENTION_HOURS` (issue #603): the logs landing
    /// table's delete-TTL, in hours.
    pub log_landing_retention_hours: u32,
    /// `PULSUS_LOG_DEDUP_WINDOW` (issue #603): the block-deduplication window
    /// the logs landing table and the five derived logs tables carry.
    pub log_dedup_window: u64,
    /// `PULSUS_TRACE_LANDING_RETENTION_HOURS` (issues #584 to #586): the
    /// traces landing table's delete-TTL, in hours.
    pub trace_landing_retention_hours: u32,
    /// `PULSUS_TRACE_DEDUP_WINDOW` (issues #584 to #586): the
    /// block-deduplication window the traces landing table and the five
    /// derived trace tables carry.
    pub trace_dedup_window: u64,
}

impl RenderCtx {
    /// A context for a test: `db` as given, every other field at the value
    /// `pulsus_config`'s own default carries.
    ///
    /// **Production does not use this.** `chconfig::schema_params_from` keeps
    /// its exhaustive literal, so a field added here later still cannot be
    /// silently defaulted where a deployment would notice. This exists because
    /// every new field otherwise edits one literal per test file — thirty-odd
    /// of them for the two fields above.
    pub fn for_tests(db: &str) -> Self {
        RenderCtx {
            db: db.to_string(),
            cluster: None,
            dist_suffix: "_dist".to_string(),
            storage_policy: None,
            retention_days: 7,
            log_rollup: Duration::from_secs(5),
            metrics_landing_retention_hours: 6,
            metrics_dedup_window: 10_000,
            log_landing_retention_hours: 6,
            log_dedup_window: 10_000,
            trace_landing_retention_hours: 6,
            trace_dedup_window: 10_000,
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

/// Applies every `{{token}}` substitution the file uses. Never touches
/// ClickHouse's own single-brace `{shard}`/`{replica}` macros — they are
/// simply absent from double-brace matching.
///
/// `schema.sh` carries the same list as sixteen `sed` expressions;
/// `the_script_renders_exactly_what_this_crate_renders` holds the two
/// together.
pub(crate) fn substitute_tokens(tmpl: &str, ctx: &RenderCtx) -> String {
    let on_cluster = match &ctx.cluster {
        Some(name) => format!(" ON CLUSTER '{}'", escape_literal(name)),
        None => String::new(),
    };
    let cluster_name = ctx.cluster.clone().unwrap_or_default();
    let log_rollup_ns = ctx.log_rollup.as_nanos().to_string();
    let storage_policy = match &ctx.storage_policy {
        Some(policy) => format!(", storage_policy = '{}'", escape_literal(policy)),
        None => String::new(),
    };

    tmpl.replace("{{db}}", &ctx.db)
        .replace("{{on_cluster}}", &on_cluster)
        .replace("{{cluster}}", &cluster_name)
        .replace("{{dist_suffix}}", &ctx.dist_suffix)
        .replace("{{route_suffix}}", route_suffix(ctx))
        .replace("{{retention_days}}", &ctx.retention_days.to_string())
        .replace("{{log_rollup_suffix}}", &rollup_suffix(ctx.log_rollup))
        .replace("{{log_rollup_ns}}", &log_rollup_ns)
        .replace(
            "{{metrics_landing_retention_hours}}",
            &ctx.metrics_landing_retention_hours.to_string(),
        )
        .replace(
            "{{log_landing_retention_hours}}",
            &ctx.log_landing_retention_hours.to_string(),
        )
        .replace(
            "{{trace_landing_retention_hours}}",
            &ctx.trace_landing_retention_hours.to_string(),
        )
        .replace(
            "{{metrics_dedup_window}}",
            &ctx.metrics_dedup_window.to_string(),
        )
        .replace("{{log_dedup_window}}", &ctx.log_dedup_window.to_string())
        .replace(
            "{{trace_dedup_window}}",
            &ctx.trace_dedup_window.to_string(),
        )
        .replace(
            "{{dedup_window_seconds}}",
            &crate::checks::DEDUP_WINDOW_SECONDS.to_string(),
        )
        .replace("{{storage_policy}}", &storage_policy)
}

/// The suffix a materialized view's `TO` clause carries when its target is
/// the **routing** table on a cluster and the local table on a single node.
///
/// **Rendered from the same field that renders the engine**, which is the
/// rule `{{on_cluster}}` and `{{dedup_window_setting}}` already follow. It
/// cannot be `{{dist_suffix}}`: that token renders the configured suffix
/// unconditionally, and every template using it today is
/// [`crate::catalog::Ddl::StaticClusterOnly`] or [`crate::catalog::Ddl::Dist`]
/// and so is never rendered without a cluster. A view is rendered in both
/// modes, so it needs the conditional form.
///
/// Two of the five trace views carry it: a trace's spans arrive from as many
/// senders as there are services in it, and the whole trace read design rests
/// on a trace being whole on one shard.
pub(crate) fn route_suffix(ctx: &RenderCtx) -> &str {
    match ctx.cluster {
        Some(_) => &ctx.dist_suffix,
        None => "",
    }
}

/// Escapes a single-quoted SQL string literal. Config-derived, not
/// adversarial input (operator-supplied cluster/db names), but cheap
/// insurance against a stray `'` producing invalid DDL rather than a clear
/// syntax error.
fn escape_literal(s: &str) -> String {
    s.replace('\'', "''")
}

/// Renders a table/view *name* template (may contain
/// `{{log_rollup_suffix}}`, never `{{on_cluster}}`).
pub fn render_name(name_tmpl: &str, ctx: &RenderCtx) -> String {
    substitute_tokens(name_tmpl, ctx)
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
            metrics_landing_retention_hours: 6,
            metrics_dedup_window: 10_000,
            log_landing_retention_hours: 6,
            log_dedup_window: 10_000,
            trace_landing_retention_hours: 6,
            trace_dedup_window: 10_000,
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

    /// The seconds window renders the one constant that carries it, so the
    /// relation a case asserts and the statement text a deployment receives
    /// cannot disagree.
    #[test]
    fn the_seconds_window_token_renders_the_constant() {
        assert_eq!(
            substitute_tokens("SETTING x = {{dedup_window_seconds}};", &ctx()),
            format!("SETTING x = {};", crate::checks::DEDUP_WINDOW_SECONDS)
        );
    }

    /// The two logs landing tokens render the context's own values, so a
    /// statement hard-coding either passes at the default and fails here.
    #[test]
    fn the_log_landing_tokens_render_the_configured_values() {
        let ctx = RenderCtx {
            log_landing_retention_hours: 24,
            log_dedup_window: 5_000,
            ..ctx()
        };
        assert_eq!(
            substitute_tokens("{{log_landing_retention_hours}}/{{log_dedup_window}}", &ctx),
            "24/5000"
        );
    }

    /// The two trace landing tokens render the context's own values, so a
    /// statement hard-coding either passes at the default and fails here.
    #[test]
    fn the_trace_landing_tokens_render_the_configured_values() {
        let ctx = RenderCtx {
            trace_landing_retention_hours: 24,
            trace_dedup_window: 5_000,
            ..ctx()
        };
        assert_eq!(
            substitute_tokens(
                "{{trace_landing_retention_hours}}/{{trace_dedup_window}}",
                &ctx
            ),
            "24/5000"
        );
    }

    /// `{{route_suffix}}` follows the cluster, not the suffix: it renders
    /// the configured suffix on a cluster and the empty string without one,
    /// where `{{dist_suffix}}` renders the suffix in both modes.
    #[test]
    fn the_route_suffix_token_follows_the_cluster() {
        const TMPL: &str = "TO {{db}}.spans{{route_suffix}}";
        assert_eq!(substitute_tokens(TMPL, &ctx()), "TO pulsus.spans");
        let clustered = RenderCtx {
            cluster: Some("prod".to_string()),
            ..ctx()
        };
        assert_eq!(substitute_tokens(TMPL, &clustered), "TO pulsus.spans_dist");
        let renamed = RenderCtx {
            cluster: Some("prod".to_string()),
            dist_suffix: "_routed".to_string(),
            ..ctx()
        };
        assert_eq!(substitute_tokens(TMPL, &renamed), "TO pulsus.spans_routed");
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
}
