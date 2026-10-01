//! Replaying a window of `trace_landing` into the five tables its
//! materialized views maintain (issue #586), which is what
//! `pulsusdb rebuild-traces` runs.
//!
//! **Why it exists.** A view never reconciles against its source — it reacts
//! to new inserts. So if a target ends up wrong (a bad view definition, a
//! detached view, a transformation defect, operator error), the landing
//! table is the only thing it can be rebuilt from, and only for as long as
//! that data is kept: `PULSUS_TRACE_LANDING_RETENTION_HOURS` is the replay
//! window.
//!
//! **Here rather than in the command, because two crates run it.** The
//! command is `pulsus-server`'s; the cases that check what it does live
//! beside the landing round-trips in `pulsus-write`'s own test suite, which
//! cannot reach a binary in another crate. So the engine is a function and
//! the command is a wrapper around it.
//!
//! The projection of each of the five views is read out of that view's own
//! rendered statement ([`crate::mv_projection`]), so the replay applies the
//! projection the view applies to a new insert and the two cannot drift.

use pulsus_clickhouse::{ChClient, ChError, Idempotency, QuerySettings};

use crate::render::{self, RenderCtx};

/// The five targets, each with the materialized view that maintains it and
/// the expression its partitions are keyed on **as the view's own projection
/// emits it**.
///
/// Every one replays, and none needs its partitions dropped first: `spans`,
/// `resources`, `tag_names` and `tag_values` collapse on their full
/// `ReplacingMergeTree` keys, and `traces` aggregates with `min`, `max` and
/// a set union, all idempotent. That is where this differs from the metrics
/// command, which refuses a replay into its three append-only targets unless
/// the caller also asks for their partitions to be dropped.
///
/// `None` is an unpartitioned target: one statement for the whole window.
#[allow(dead_code)]
const TARGETS: &[(&str, &str, Option<&str>)] = &[
    (
        "spans",
        "spans_mv",
        Some("toDate(fromUnixTimestamp64Nano(start_ns))"),
    ),
    ("traces", "traces_mv", Some("day")),
    ("resources", "resources_mv", Some("day")),
    ("tag_names", "tag_names_mv", None),
    ("tag_values", "tag_values_mv", None),
];

/// What one `rebuild-traces` run did: every statement it executed, in the
/// order it executed them.
///
/// **The statements are the report, because the run's own outcome does not
/// carry what it replayed.** `written_rows` counts what a projection
/// emitted rather than what the target ends up holding — the four
/// `ReplacingMergeTree` targets collapse and `traces` aggregates — so the
/// shape of the run is what a caller can check, and the convergence is what
/// the targets themselves give.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReplayReport {
    /// Every statement the run executed: the enumerating `SELECT DISTINCT`
    /// of each partitioned target, then one `INSERT INTO` per (target,
    /// target partition).
    pub statements: Vec<String>,
}

impl ReplayReport {
    /// The `INSERT INTO` statements alone.
    pub fn inserts(&self) -> Vec<&str> {
        self.statements
            .iter()
            .map(String::as_str)
            .filter(|s| s.starts_with("INSERT INTO"))
            .collect()
    }
}

/// Replays the landed rows stamped in `[from_ms, to_ms)` into all five
/// targets, **one target partition per statement**.
///
/// `max_rows` is `PULSUS_TRACE_LANDING_MAX_ROWS`, the value both pinned row
/// limits carry on every statement.
///
/// **What this promises, in full.** A run may leave a partition partly
/// written, and may miss a row committed into a partition it had already
/// processed. **A re-run converges**, on the key each target collapses on.
/// That is the whole guarantee: nothing here is atomic at any granularity,
/// and nothing detects the need to re-run.
pub async fn replay_trace_window(
    client: &ChClient,
    ctx: &RenderCtx,
    from_ms: i64,
    to_ms: i64,
    max_rows: u64,
) -> Result<ReplayReport, ChError> {
    // STUB (issue #586, the tests-first commit).
    let _ = (client, ctx, from_ms, to_ms, max_rows);
    Ok(ReplayReport::default())
}

/// The suffix the two per-trace targets take: a replay routes exactly as the
/// view does, because it issues the view's own projection and that view's
/// target is the routing table on a cluster.
#[allow(dead_code)]
fn target_suffix(ctx: &RenderCtx, target: &str) -> String {
    if matches!(target, "spans" | "traces") {
        render::route_suffix(ctx).to_string()
    } else {
        String::new()
    }
}

/// A per-statement deduplication token.
///
/// **Minted per statement**, which keeps each statement's identity
/// independent and costs nothing. It is not a UUID: the token is an opaque
/// string the server hashes into a block id, so the statement's own identity
/// plus one clock reading per run is enough to make it unique per statement
/// and stable inside one.
#[allow(dead_code)]
fn replay_token(run: u128, target: &str, partition: &str) -> String {
    format!("rebuild-traces-{run:032x}-{target}-{partition}")
}

/// Issues one statement, recording it.
#[allow(dead_code)]
async fn execute_recorded(
    client: &ChClient,
    report: &mut ReplayReport,
    sql: String,
    settings: &QuerySettings,
) -> Result<(), ChError> {
    report.statements.push(sql.clone());
    client
        .execute(&sql, settings, Idempotency::NonIdempotent)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The five targets are the five views' targets and nothing else, and
    /// each names a view the catalogue holds (issue #586).
    #[test]
    fn every_replay_target_names_a_view_in_the_catalogue() {
        let ctx = RenderCtx::for_tests("pulsus");
        let names: Vec<&str> = TARGETS.iter().map(|(t, _, _)| *t).collect();
        assert_eq!(
            names,
            vec!["spans", "traces", "resources", "tag_names", "tag_values"],
            "all five targets replay"
        );
        for (target, mv, _) in TARGETS {
            let projection = crate::mv_projection(mv, &ctx)
                .unwrap_or_else(|| panic!("no view named {mv} for target {target}"));
            assert!(
                projection.contains("FROM pulsus.trace_landing"),
                "{mv}'s projection must read the landing table: {projection}"
            );
        }
    }
}
