//! `pulsusdb rebuild-metrics`: replays a window of `metric_landing` into one
//! of the seven tables the materialized views maintain (issues #603, #623,
//! #635); `pulsusdb rebuild-logs` does the same over `log_landing` and the
//! five log tables (issue #653).
//!
//! **Why it exists.** A view never reconciles against its source — it reacts
//! to new inserts. So if a target ends up wrong (a bad view definition, a
//! detached view, a transformation defect, operator error), the landing table
//! is the only thing it can be rebuilt from, and only for as long as that
//! data is kept: `PULSUS_METRICS_LANDING_RETENTION_HOURS` is the replay
//! window.
//!
//! **It is an operator tool, run by hand.** Absent the subcommand the binary
//! behaves exactly as before. `tests/rebuild_live.rs` runs it as the binary
//! against a live ClickHouse (issue #623).
//!
//! The projection is read out of the view's own rendered statement
//! (`pulsus_schema::mv_projection`), so the replay applies the same
//! projection the view applies to a new insert and the two cannot drift.

use std::process::ExitCode;

use clap::Args;
use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings};
use pulsus_config::Config;
use pulsus_model::Tenant;
use pulsus_read::logql::escape::ch_string;
use pulsus_read::metrics::TenantSql;

use crate::chconfig::{conn_config_from, schema_params_from};

/// The seven targets, each with the materialized view that maintains it and,
/// where a replay must drop partitions first, the expression they are keyed
/// on.
///
/// Five tolerate a replay without dropping. `metric_metadata` is a
/// replacing table keyed on `metric_name`, so a replayed row is the same row
/// or loses to a newer one; `metric_labels` and `metric_series` aggregate,
/// so a replayed row folds into the one stored — `min` and `max` of the same
/// instants, an OR of the same hours (issue #623). The two sample tables are
/// append-only, so a replay that does not drop first stores every row twice
/// and inflates every counting query.
const TARGETS: &[(&str, &str, Option<&str>)] = &[
    (
        "metric_samples",
        "metric_samples_mv",
        Some("toDate(fromUnixTimestamp64Milli(unix_milli))"),
    ),
    (
        "metric_hist_samples",
        "metric_hist_samples_mv",
        Some("toDate(fromUnixTimestamp64Milli(unix_milli))"),
    ),
    // Partitioned by day, but no need to drop (issue #623): a replayed row
    // ORs its hours into the day's mask, which already holds them, so a
    // replay adds nothing twice. Dropping would lose what the landing table
    // no longer holds, and a day could not be restored from its retention.
    ("metric_series", "metric_series_mv", None),
    // No partition key, and no need to drop: the engine collapses on
    // `metric_name`.
    ("metric_metadata", "metric_metadata_mv", None),
    // No partition key, and no need to drop: the engine folds on
    // `(metric_name, fingerprint)`, a series' one lookup row.
    ("metric_labels", "metric_labels_mv", None),
    // No partition key, and no need to drop (issue #635): the engine
    // replaces on the whole row, `(key, value, fingerprint)` and `(key,
    // value)`, so a replayed row merges into the one stored.
    ("metric_label_index", "metric_label_index_mv", None),
    ("metric_label_values", "metric_label_values_mv", None),
];

#[derive(Args, Debug)]
pub(crate) struct RebuildMetrics {
    /// Which table to rebuild: `metric_samples`, `metric_series`,
    /// `metric_metadata`, `metric_labels`, `metric_hist_samples`,
    /// `metric_label_index` or `metric_label_values`.
    #[arg(long)]
    target: String,

    /// Replay landed rows stamped at or after this instant, RFC3339.
    #[arg(long)]
    from: String,

    /// Replay landed rows stamped strictly before this instant, RFC3339.
    #[arg(long)]
    to: String,

    /// Drop the target partitions the replayed rows fall in first. This
    /// deletes **every** row in those partitions, every tenant's, including
    /// data the landing table no longer holds. Without it the two sample
    /// tables are refused, because a replay would store their rows twice.
    /// With `--tenant` no partition is dropped: that tenant's rows on those
    /// days are deleted instead, and no other tenant's.
    #[arg(long)]
    drop_target_partitions: bool,

    /// Replay one tenant's rows alone (issue #635): the landed rows whose
    /// `org_id` is this `X-Scope-OrgID` value, `""` for the single-tenant
    /// deployment. Without it, every tenant's rows are replayed.
    #[arg(long)]
    tenant: Option<String>,
}

pub(crate) async fn run(config: &Config, args: RebuildMetrics) -> ExitCode {
    match rebuild(config, args).await {
        Ok(msg) => {
            println!("pulsusdb: {msg}");
            ExitCode::SUCCESS
        }
        Err(msg) => {
            eprintln!("pulsusdb: {msg}");
            ExitCode::FAILURE
        }
    }
}

async fn rebuild(config: &Config, args: RebuildMetrics) -> Result<String, String> {
    let (target, mv_name, partition_expr) = TARGETS
        .iter()
        .find(|(name, _, _)| *name == args.target)
        .ok_or_else(|| {
            format!(
                "unknown --target {:?}; one of {}",
                args.target,
                TARGETS
                    .iter()
                    .map(|(n, _, _)| *n)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;

    if partition_expr.is_some() && !args.drop_target_partitions {
        return Err(format!(
            "{target} is append-only: a replay without --drop-target-partitions would store \
             every replayed row a second time. Pass --drop-target-partitions, which first \
             deletes EVERY row of every tenant's in the partitions the replayed rows fall in — \
             including data the landing table no longer holds — or with --tenant, that \
             tenant's rows on those days"
        ));
    }

    // Issue #635 part 4: the tenant, validated by the header's own rule.
    let tenant = tenant_arg(args.tenant.as_deref())?;

    let from_ms = parse_rfc3339_millis(&args.from).map_err(|e| format!("--from: {e}"))?;
    let to_ms = parse_rfc3339_millis(&args.to).map_err(|e| format!("--to: {e}"))?;
    if to_ms <= from_ms {
        return Err("--to must be after --from".to_string());
    }

    let ctx = schema_params_from(config);
    let projection = pulsus_schema::mv_projection(mv_name, &ctx)
        .ok_or_else(|| format!("no materialized view named {mv_name} in the catalogue"))?;
    let window = format!(" AND received_ms >= {from_ms} AND received_ms < {to_ms}");
    let scope = tenant
        .as_ref()
        .map(|t| format!(" AND org_id = {}", t.sql_literal()))
        .unwrap_or_default();
    let select = format!("{projection}{window}{scope}");

    let client = ChClient::new(conn_config_from(config))
        .await
        .map_err(|e| e.to_string())?;
    apply_replay(
        &client,
        &ctx.db,
        target,
        *partition_expr,
        &select,
        tenant.as_ref(),
    )
    .await?;

    Ok(format!(
        "rebuilt {target} from the landed rows stamped in [{}, {})",
        args.from, args.to
    ))
}

/// The `--tenant` argument, validated by the header's own rule (issue #635
/// part 4): `None` replays every tenant, `""` the single-tenant deployment.
fn tenant_arg(text: Option<&str>) -> Result<Option<Tenant>, String> {
    let Some(text) = text else {
        return Ok(None);
    };
    let value = axum::http::HeaderValue::from_str(text)
        .map_err(|_| format!("--tenant {text:?}: invalid X-Scope-OrgID"))?;
    let tenant = Tenant::from_header((!text.is_empty()).then_some(&value), false)
        .map_err(|e| format!("--tenant {text:?}: {e}"))?;
    Ok(Some(tenant))
}

/// What a rebuild does to `target` once its replayed `select` is built,
/// shared by `rebuild-metrics` and `rebuild-logs` (issue #653): with a
/// partition expression and a tenant, that tenant's rows on the replayed
/// days are deleted; with a partition expression and no tenant, the
/// partitions the replayed rows fall in are dropped; then the rows are
/// inserted.
async fn apply_replay(
    client: &ChClient,
    db: &str,
    target: &str,
    partition_expr: Option<&str>,
    select: &str,
    tenant: Option<&Tenant>,
) -> Result<(), String> {
    if let (Some(expr), Some(tenant)) = (partition_expr, tenant) {
        // One tenant: delete its rows on the replayed days, and no other
        // tenant's; no partition is dropped.
        let days = distinct_days(client, select, expr).await?;
        if !days.is_empty() {
            let sql = format!(
                "DELETE FROM {db}.{target} WHERE org_id = {} AND {expr} IN ({}) \
                 SETTINGS lightweight_deletes_sync = 2",
                tenant.sql_literal(),
                days.iter()
                    .map(|d| ch_string(d))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            client
                .execute(&sql, &QuerySettings::new(), Idempotency::NonIdempotent)
                .await
                .map_err(|e| format!("deleting the tenant's rows: {e}"))?;
        }
        tracing::info!(
            target = %target,
            days = days.len(),
            "deleted the tenant's rows on the days the replayed rows fall in"
        );
    } else if let Some(expr) = partition_expr {
        let partitions = distinct_partitions(client, select, expr).await?;
        for partition in &partitions {
            let sql = format!(
                "ALTER TABLE {db}.{target} DROP PARTITION ID '{partition}' SETTINGS mutations_sync = 2"
            );
            client
                .execute(&sql, &QuerySettings::new(), Idempotency::NonIdempotent)
                .await
                .map_err(|e| format!("dropping partition {partition}: {e}"))?;
        }
        tracing::info!(
            target = %target,
            dropped = partitions.len(),
            "dropped the target partitions the replayed rows fall in"
        );
    }

    let insert = format!("INSERT INTO {db}.{target} {select}");
    client
        .execute(&insert, &QuerySettings::new(), Idempotency::NonIdempotent)
        .await
        .map_err(|e| format!("replaying into {target}: {e}"))?;

    Ok(())
}

/// The partition IDs the replayed rows fall in, read by applying the target's
/// own partition expression to the rows the replay would insert.
///
/// `partitionID`, not `toString` (issue #623): `DROP PARTITION ID` takes the
/// ID, and a `Date` partition's ID is `20261006` where its text is
/// `2026-10-06`. Dropping by the text names no stored partition, drops
/// nothing and succeeds, so the replay stored every row a second time.
async fn distinct_partitions(
    client: &ChClient,
    select: &str,
    partition_expr: &str,
) -> Result<Vec<String>, String> {
    let sql =
        format!("SELECT DISTINCT partitionID({partition_expr}) AS s FROM ({select}) ORDER BY s");
    client
        .query_strings(&sql, &QuerySettings::new())
        .await
        .map_err(|e| format!("reading the partitions to drop: {e}"))
}

/// The days the replayed rows fall in, as the text of the target's own
/// partition expression (issue #635 part 4): what a tenant's rebuild
/// deletes that tenant's rows on.
async fn distinct_days(
    client: &ChClient,
    select: &str,
    partition_expr: &str,
) -> Result<Vec<String>, String> {
    let sql = format!("SELECT DISTINCT toString({partition_expr}) AS s FROM ({select}) ORDER BY s");
    client
        .query_strings(&sql, &QuerySettings::new())
        .await
        .map_err(|e| format!("reading the days to delete: {e}"))
}

/// The five log targets (issue #653), each with the view that maintains it
/// and, where a replay must remove rows first, the target's own partition
/// expression. Names are templates, rendered with the deployment's rollup
/// resolution before `--target` is matched.
///
/// `log_samples` is append-only and `log_metrics_<res>` and `log_patterns`
/// sum on merge, so a replay over rows still stored would count them twice.
/// `log_streams` replaces on `updated_ns` and `log_streams_idx` on the whole
/// row, so a replayed row folds into the one stored.
const LOG_TARGETS: &[(&str, &str, Option<&str>)] = &[
    (
        "log_samples",
        "log_samples_mv",
        Some("toDate(fromUnixTimestamp64Nano(timestamp_ns))"),
    ),
    (
        "log_metrics_{{log_rollup_suffix}}",
        "log_metrics_{{log_rollup_suffix}}_mv",
        Some("toDate(fromUnixTimestamp64Nano(bucket_ns))"),
    ),
    (
        "log_patterns",
        "log_patterns_mv",
        Some("toDate(fromUnixTimestamp64Nano(bucket_ns))"),
    ),
    ("log_streams", "log_streams_mv", None),
    ("log_streams_idx", "log_streams_idx_mv", None),
];

/// `pulsusdb rebuild-logs`: replays a window of `log_landing` into one of
/// the five tables the log materialized views maintain (issue #653), as
/// `rebuild-metrics` does for the metric tables.
#[derive(Args, Debug)]
pub(crate) struct RebuildLogs {
    /// Which table to rebuild: `log_samples`, `log_metrics_<res>` (for
    /// example `log_metrics_5s`), `log_patterns`, `log_streams` or
    /// `log_streams_idx`.
    #[arg(long)]
    target: String,

    /// Replay landed rows stamped at or after this instant, RFC3339.
    #[arg(long)]
    from: String,

    /// Replay landed rows stamped strictly before this instant, RFC3339.
    #[arg(long)]
    to: String,

    /// Drop the target partitions the replayed rows fall in first. This
    /// deletes **every** row in those partitions, every tenant's, including
    /// data the landing table no longer holds
    /// (`PULSUS_LOG_LANDING_RETENTION_HOURS` bounds the replay). Without it
    /// `log_samples`, `log_metrics_<res>` and `log_patterns` are refused,
    /// because a replay would count their rows twice. With `--tenant` no
    /// partition is dropped: that tenant's rows on those days are deleted
    /// instead, and no other tenant's.
    #[arg(long)]
    drop_target_partitions: bool,

    /// Replay one tenant's rows alone: the landed rows whose `org_id` is
    /// this `X-Scope-OrgID` value, `""` for the single-tenant deployment.
    /// Without it, every tenant's rows are replayed.
    #[arg(long)]
    tenant: Option<String>,
}

pub(crate) async fn run_logs(config: &Config, args: RebuildLogs) -> ExitCode {
    match rebuild_logs(config, args).await {
        Ok(msg) => {
            println!("pulsusdb: {msg}");
            ExitCode::SUCCESS
        }
        Err(msg) => {
            eprintln!("pulsusdb: {msg}");
            ExitCode::FAILURE
        }
    }
}

async fn rebuild_logs(config: &Config, args: RebuildLogs) -> Result<String, String> {
    let ctx = schema_params_from(config);
    let names: Vec<(String, &str, Option<&str>)> = LOG_TARGETS
        .iter()
        .map(|(name, mv, expr)| (pulsus_schema::render_name(name, &ctx), *mv, *expr))
        .collect();
    let (target, mv_name, partition_expr) = names
        .iter()
        .find(|(name, _, _)| *name == args.target)
        .ok_or_else(|| {
            format!(
                "unknown --target {:?}; one of {}",
                args.target,
                names
                    .iter()
                    .map(|(n, _, _)| n.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;

    if partition_expr.is_some() && !args.drop_target_partitions {
        return Err(format!(
            "{target} counts a replayed row a second time: a replay without \
             --drop-target-partitions would store every replayed row twice. Pass \
             --drop-target-partitions, which first deletes EVERY row of every tenant's in the \
             partitions the replayed rows fall in — including data the landing table no longer \
             holds — or with --tenant, that tenant's rows on those days"
        ));
    }

    let tenant = tenant_arg(args.tenant.as_deref())?;
    let from_ms = parse_rfc3339_millis(&args.from).map_err(|e| format!("--from: {e}"))?;
    let to_ms = parse_rfc3339_millis(&args.to).map_err(|e| format!("--to: {e}"))?;
    if to_ms <= from_ms {
        return Err("--to must be after --from".to_string());
    }

    let projection = pulsus_schema::mv_projection(mv_name, &ctx)
        .ok_or_else(|| format!("no materialized view named {mv_name} in the catalogue"))?;
    let select = log_replay_select(&projection, &ctx.db, from_ms, to_ms, tenant.as_ref())?;

    let client = ChClient::new(conn_config_from(config))
        .await
        .map_err(|e| e.to_string())?;
    apply_replay(
        &client,
        &ctx.db,
        target,
        *partition_expr,
        &select,
        tenant.as_ref(),
    )
    .await?;

    Ok(format!(
        "rebuilt {target} from the landed rows stamped in [{}, {})",
        args.from, args.to
    ))
}

/// The statement a log rebuild replays (issue #653): the view's projection
/// with its one `FROM <db>.log_landing` replaced by a bounded source — the
/// window, and with `tenant`, that tenant's rows alone.
///
/// `rebuild-metrics` appends its window to the projection instead, which is
/// invalid for `log_metrics_<res>_mv`, whose projection ends in `GROUP BY`.
/// A projection naming the landing table other than once is refused, so a
/// view rewritten to read it twice cannot replay one read unbounded.
pub(crate) fn log_replay_select(
    projection: &str,
    db: &str,
    from_ms: i64,
    to_ms: i64,
    tenant: Option<&Tenant>,
) -> Result<String, String> {
    let from = format!("FROM {db}.log_landing");
    let sites: Vec<usize> = projection
        .match_indices(&from)
        .map(|(i, _)| i)
        .filter(|i| {
            projection[i + from.len()..]
                .chars()
                .next()
                .is_none_or(char::is_whitespace)
        })
        .collect();
    let [at] = sites[..] else {
        return Err(format!(
            "the view's projection reads {db}.log_landing {} times, not once",
            sites.len()
        ));
    };
    let scope = tenant
        .map(|t| format!(" AND org_id = {}", t.sql_literal()))
        .unwrap_or_default();
    Ok(format!(
        "{}FROM (SELECT * FROM {db}.log_landing WHERE received_ms >= {from_ms} AND \
         received_ms < {to_ms}{scope}) AS log_landing{}",
        &projection[..at],
        &projection[at + from.len()..]
    ))
}

/// `pulsusdb rebuild-traces`: replays a window of `trace_landing` into
/// **all five** tables the trace materialized views maintain (issue #586).
///
/// Two of [`RebuildMetrics`]'s arguments are not taken. There is no
/// `--target`, because one run replays all five; and no
/// `--drop-target-partitions`, because no partition is dropped — every trace
/// target is idempotent under the key it collapses on, so a replay changes no
/// answer.
///
/// **What it promises** is `pulsus_schema::replay_trace_window`'s, whole: a
/// run may leave a partition partly written and may miss a row committed
/// behind it, and a re-run converges.
#[derive(Args, Debug)]
pub(crate) struct RebuildTraces {
    /// Replay landed rows stamped at or after this instant, RFC3339.
    #[arg(long)]
    from: String,

    /// Replay landed rows stamped strictly before this instant, RFC3339.
    #[arg(long)]
    to: String,
}

pub(crate) async fn run_traces(config: &Config, args: RebuildTraces) -> ExitCode {
    match rebuild_traces(config, args).await {
        Ok(msg) => {
            println!("pulsusdb: {msg}");
            ExitCode::SUCCESS
        }
        Err(msg) => {
            eprintln!("pulsusdb: {msg}");
            ExitCode::FAILURE
        }
    }
}

async fn rebuild_traces(config: &Config, args: RebuildTraces) -> Result<String, String> {
    let from_ms = parse_rfc3339_millis(&args.from).map_err(|e| format!("--from: {e}"))?;
    let to_ms = parse_rfc3339_millis(&args.to).map_err(|e| format!("--to: {e}"))?;
    if to_ms <= from_ms {
        return Err("--to must be after --from".to_string());
    }

    let ctx = schema_params_from(config);
    let client = ChClient::new(conn_config_from(config))
        .await
        .map_err(|e| e.to_string())?;
    let report = pulsus_schema::replay_trace_window(
        &client,
        &ctx,
        from_ms,
        to_ms,
        config.writer.trace_landing_max_rows,
    )
    .await
    .map_err(|e| format!("replaying the trace landing window: {e}"))?;

    Ok(format!(
        "replayed {} statements over the landed rows stamped in [{}, {})",
        report.statements.len(),
        args.from,
        args.to
    ))
}

/// An RFC3339 instant as epoch milliseconds, the unit `received_ms` carries.
fn parse_rfc3339_millis(text: &str) -> Result<i64, String> {
    chrono::DateTime::parse_from_rfc3339(text)
        .map(|t| t.timestamp_millis())
        .map_err(|e| format!("{text:?} is not an RFC3339 instant: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **T-R5 (issue #653): each log replay reads one tenant's window.** For
    /// the rendered projection of each of the five log views, the replayed
    /// statement reads `log_landing` once, through the bounded subquery: the
    /// window and, with a tenant, that tenant alone. A projection that
    /// names the landing table twice or never is refused.
    #[test]
    fn each_log_replay_reads_one_tenants_window() {
        let ctx = pulsus_schema::RenderCtx::for_tests("db");
        let tenant = Tenant::from_header(
            Some(&axum::http::HeaderValue::from_static("tenant-q")),
            false,
        )
        .expect("a valid tenant");
        for view in [
            "log_samples_mv",
            "log_metrics_{{log_rollup_suffix}}_mv",
            "log_patterns_mv",
            "log_streams_mv",
            "log_streams_idx_mv",
        ] {
            let projection = pulsus_schema::mv_projection(view, &ctx)
                .unwrap_or_else(|| panic!("{view} is in the catalogue"));
            for (who, scope) in [(Some(&tenant), " AND org_id = 'tenant-q'"), (None, "")] {
                let sql = log_replay_select(&projection, "db", 1000, 2000, who)
                    .unwrap_or_else(|e| panic!("{view}: {e}"));
                let source = format!(
                    "FROM (SELECT * FROM db.log_landing WHERE received_ms >= 1000 AND \
                     received_ms < 2000{scope}) AS log_landing"
                );
                assert_eq!(
                    sql.matches(&source).count(),
                    1,
                    "{view}, tenant {who:?}: the bounded source once in\n{sql}"
                );
                assert!(
                    !sql.replacen(&source, "", 1).contains("log_landing"),
                    "{view}, tenant {who:?}: no other read of log_landing in\n{sql}"
                );
            }
            let twice = format!("{projection} UNION ALL {projection}");
            assert!(
                log_replay_select(&twice, "db", 1000, 2000, Some(&tenant)).is_err(),
                "{view}: a projection naming the landing table twice"
            );
            let never = projection.replace("FROM db.log_landing", "FROM db.elsewhere");
            assert!(
                log_replay_select(&never, "db", 1000, 2000, Some(&tenant)).is_err(),
                "{view}: a projection never naming the landing table"
            );
        }
    }
}
