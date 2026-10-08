//! `pulsusdb rebuild-metrics`: replays a window of `metric_landing` into one
//! of the seven tables the materialized views maintain (issues #603, #623,
//! #635).
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
    let tenant = match &args.tenant {
        None => None,
        Some(text) => {
            let value = axum::http::HeaderValue::from_str(text)
                .map_err(|_| format!("--tenant {text:?}: invalid X-Scope-OrgID"))?;
            let tenant = Tenant::from_header((!text.is_empty()).then_some(&value), false)
                .map_err(|e| format!("--tenant {text:?}: {e}"))?;
            Some(tenant)
        }
    };

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
    let db = &ctx.db;

    if let (Some(expr), Some(tenant)) = (partition_expr, &tenant) {
        // One tenant: delete its rows on the replayed days, and no other
        // tenant's; no partition is dropped.
        let days = distinct_days(&client, &select, expr).await?;
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
        let partitions = distinct_partitions(&client, &select, expr).await?;
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

    Ok(format!(
        "rebuilt {target} from the landed rows stamped in [{}, {})",
        args.from, args.to
    ))
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
