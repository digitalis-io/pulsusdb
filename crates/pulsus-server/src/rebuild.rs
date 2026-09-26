//! `pulsusdb rebuild-metrics`: replays a window of `metric_landing` into one
//! of the four tables the materialized views maintain (issue #603).
//!
//! **Why it exists.** A view never reconciles against its source — it reacts
//! to new inserts. So if a target ends up wrong (a bad view definition, a
//! detached view, a transformation defect, operator error), the landing table
//! is the only thing it can be rebuilt from, and only for as long as that
//! data is kept: `PULSUS_METRICS_LANDING_RETENTION_HOURS` is the replay
//! window.
//!
//! **It is an operator tool, run by hand.** Absent the subcommand the binary
//! behaves exactly as before. It is deliberately without tests: there is no
//! environment here that would exercise it faithfully.
//!
//! The projection is read out of the view's own rendered statement
//! (`pulsus_schema::mv_projection`), so the replay applies the same
//! projection the view applies to a new insert and the two cannot drift.

use std::process::ExitCode;

use clap::Args;
use pulsus_clickhouse::{ChClient, Idempotency, QuerySettings};
use pulsus_config::Config;

use crate::chconfig::{conn_config_from, schema_params_from};

/// The four targets, each with the materialized view that maintains it, the
/// expression its partitions are keyed on, and whether it tolerates a replay
/// without dropping those partitions first.
///
/// `metric_metadata` does: it is a `ReplacingMergeTree(updated_ns)` keyed on
/// `metric_name`, so a replayed row either loses to a newer descriptor or is
/// the same row. The other three are append-only, so a replay that does not
/// drop first stores every row twice and inflates every counting query.
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
    (
        "metric_series",
        "metric_series_mv",
        Some("toYYYYMM(fromUnixTimestamp64Milli(unix_milli))"),
    ),
    // No partition key, and no need to drop: the engine collapses on
    // `metric_name`.
    ("metric_metadata", "metric_metadata_mv", None),
];

#[derive(Args, Debug)]
pub(crate) struct RebuildMetrics {
    /// Which table to rebuild: `metric_samples`, `metric_series`,
    /// `metric_metadata` or `metric_hist_samples`.
    #[arg(long)]
    target: String,

    /// Replay landed rows stamped at or after this instant, RFC3339.
    #[arg(long)]
    from: String,

    /// Replay landed rows stamped strictly before this instant, RFC3339.
    #[arg(long)]
    to: String,

    /// Drop the target partitions the replayed rows fall in first. This
    /// deletes **every** row in those partitions, including data the landing
    /// table no longer holds. Without it the three append-only targets are
    /// refused, because a replay would store their rows twice.
    #[arg(long)]
    drop_target_partitions: bool,
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
             deletes EVERY row in the partitions the replayed rows fall in — including data \
             the landing table no longer holds"
        ));
    }

    let from_ms = parse_rfc3339_millis(&args.from).map_err(|e| format!("--from: {e}"))?;
    let to_ms = parse_rfc3339_millis(&args.to).map_err(|e| format!("--to: {e}"))?;
    if to_ms <= from_ms {
        return Err("--to must be after --from".to_string());
    }

    let ctx = schema_params_from(config);
    let projection = pulsus_schema::mv_projection(mv_name, &ctx)
        .ok_or_else(|| format!("no materialized view named {mv_name} in the catalogue"))?;
    let window = format!(" AND received_ms >= {from_ms} AND received_ms < {to_ms}");
    let select = format!("{projection}{window}");

    let client = ChClient::new(conn_config_from(config))
        .await
        .map_err(|e| e.to_string())?;
    let db = &ctx.db;

    if let Some(expr) = partition_expr {
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

/// The partition ids the replayed rows fall in, read by applying the target's
/// own partition expression to the rows the replay would insert.
async fn distinct_partitions(
    client: &ChClient,
    select: &str,
    partition_expr: &str,
) -> Result<Vec<String>, String> {
    let sql = format!("SELECT DISTINCT toString({partition_expr}) AS s FROM ({select}) ORDER BY s");
    client
        .query_strings(&sql, &QuerySettings::new())
        .await
        .map_err(|e| format!("reading the partitions to drop: {e}"))
}

/// An RFC3339 instant as epoch milliseconds, the unit `received_ms` carries.
fn parse_rfc3339_millis(text: &str) -> Result<i64, String> {
    chrono::DateTime::parse_from_rfc3339(text)
        .map(|t| t.timestamp_millis())
        .map_err(|e| format!("{text:?} is not an RFC3339 instant: {e}"))
}
