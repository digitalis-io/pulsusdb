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

use crate::controller::mv_projection;
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
    let db = &ctx.db;
    let run = run_id(run_stamp());
    let mut report = ReplayReport::default();

    for (target, mv, partition_expr) in TARGETS {
        let projection = mv_projection(mv, ctx).ok_or_else(|| {
            ChError::Config(format!("no materialized view named {mv} in the catalogue"))
        })?;
        // The window goes **inside** the projection's own landing-table
        // predicate rather than after it: the per-trace view's projection
        // ends in a `GROUP BY` inside a subquery, so appending a conjunct to
        // the whole statement would not parse.
        let select = windowed(&projection, from_ms, to_ms)?;
        let table = format!("{db}.{target}{}", target_suffix(ctx, target));

        match partition_expr {
            None => {
                // An unpartitioned target: one statement for the window.
                let sql = format!("INSERT INTO {table} {select}");
                let token = replay_token(run, target, "all");
                let settings = QuerySettings::trace_landing_insert(&token, max_rows);
                execute_recorded(client, &mut report, sql, &settings).await?;
            }
            Some(expr) => {
                // **One statement per target partition**, so every block a
                // statement emits carries rows of exactly one partition —
                // which is at or below every positive value
                // `max_partitions_per_insert_block` can take.
                let partitions =
                    distinct_partitions(client, &mut report, &select, expr, run, target, max_rows)
                        .await?;
                for partition in &partitions {
                    let sql = format!(
                        "INSERT INTO {table} SELECT * FROM ({select}) WHERE {expr} = {}",
                        quote_literal(partition)
                    );
                    let token = replay_token(run, target, partition);
                    let settings = QuerySettings::trace_landing_insert(&token, max_rows);
                    execute_recorded(client, &mut report, sql, &settings).await?;
                }
            }
        }
    }

    Ok(report)
}

/// The partition values the replayed rows fall in, read by applying the
/// target's own partition expression to the rows the replay would insert.
///
/// **It carries the same settings every other statement does**, from the one
/// constructor: three of those pins — the distinct, sort and result overflow
/// modes — bind on this read alone, so a version that passed
/// `QuerySettings::new()` here would leave the partition list a profile could
/// shorten.
///
/// It **races**: a landing row committed between this read and the inserts
/// below adds a partition this list never saw, and that row is missed by this
/// run. A re-run recovers it, which is the whole of what the repair promises.
#[allow(clippy::too_many_arguments)]
async fn distinct_partitions(
    client: &ChClient,
    report: &mut ReplayReport,
    select: &str,
    partition_expr: &str,
    run: u128,
    target: &str,
    max_rows: u64,
) -> Result<Vec<String>, ChError> {
    let sql = format!("SELECT DISTINCT toString({partition_expr}) AS s FROM ({select}) ORDER BY s");
    let token = replay_token(run, target, "partitions");
    let settings = QuerySettings::trace_landing_insert(&token, max_rows);
    report.statements.push(sql.clone());
    client.query_strings(&sql, &settings).await
}

/// Puts the window inside the projection's own `WHERE row_kind = <n>`, which
/// is the landing-table scan's predicate and the only `WHERE` any of the five
/// projections carries.
fn windowed(projection: &str, from_ms: i64, to_ms: i64) -> Result<String, ChError> {
    let marker = "WHERE row_kind = ";
    let at = projection.find(marker).ok_or_else(|| {
        ChError::Config(format!(
            "a trace view's projection must scan the landing table under a \
             row_kind predicate: {projection}"
        ))
    })?;
    // One digit: the four discriminating values are 0 to 3.
    let after = at + marker.len() + 1;
    let mut out = String::with_capacity(projection.len() + 64);
    out.push_str(&projection[..after]);
    out.push_str(&format!(
        " AND received_ms >= {from_ms} AND received_ms < {to_ms}"
    ));
    out.push_str(&projection[after..]);
    Ok(out)
}

/// A single-quoted SQL string literal. The values are partition keys the
/// server itself rendered with `toString`, not caller input, but a stray
/// quote should give a clear refusal rather than invalid SQL.
fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// One clock reading per run, the input [`run_id`] mixes.
fn run_stamp() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

/// One identifier per run, which is what makes a token unique per statement
/// across runs.
///
/// **Not the clock reading itself.** Two runs that read the clock in the
/// same nanosecond — two processes on a cluster, or one clock stepped back
/// — would mint one identifier, hand one target partition the same token
/// twice, and the server, which hashes the token into the block id instead
/// of hashing the block, would discard the second block as a duplicate. The
/// replay would drop those rows in silence, which is the one thing it
/// promises not to do.
///
/// 128 bits from two `std::collections::hash_map::RandomState`s, which are
/// documented to be built from random keys, over a stream that also carries
/// `stamp`, the process id and a count of this process's runs — so two runs
/// differ even if one of those three repeats. Hand-rolled rather than taken
/// from a dependency, for the reason `writer::landing`'s own token mint
/// gives: one opaque string per run is the whole requirement.
fn run_id(stamp: u128) -> u128 {
    use std::collections::hash_map::RandomState;
    use std::hash::BuildHasher;
    use std::sync::atomic::{AtomicU64, Ordering};

    static RUNS: AtomicU64 = AtomicU64::new(0);
    let seed = (
        stamp,
        std::process::id(),
        RUNS.fetch_add(1, Ordering::Relaxed),
    );
    let half = |state: RandomState| state.hash_one(seed);
    // Two states, because one `Hasher` yields 64 bits: each is its own
    // keyed function of the same stream, and the two keys differ.
    (u128::from(half(RandomState::new())) << 64) | u128::from(half(RandomState::new()))
}

/// The suffix the two per-trace targets take: a replay routes exactly as the
/// view does, because it issues the view's own projection and that view's
/// target is the routing table on a cluster. A replay run on one node
/// therefore places rows on the same shards the original push did.
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
/// plus [`run_id`]'s identifier is enough to make it unique per statement and
/// stable inside one run.
///
/// Two statements over different partitions could not collide however the
/// token were chosen — the server's own block identity already carries the
/// chunk's source block number and the partition id — so this is not what
/// keeps them apart.
fn replay_token(run: u128, target: &str, partition: &str) -> String {
    format!("rebuild-traces-{run:032x}-{target}-{partition}")
}

/// Issues one statement, recording it.
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

    /// **Two runs that read the same clock are still two runs.** Wall-clock
    /// nanoseconds are not unique across concurrent processes or across a
    /// clock that steps back, and two runs that minted one identifier would
    /// hand one target partition the same token twice: the server hashes the
    /// token into the block id and discards the second block as a duplicate,
    /// so the second run's rows would be dropped in silence and the repair
    /// would not converge.
    ///
    /// The clock reading is forced equal, which is the whole of what the
    /// case needs: it asserts the identifier is not a function of it.
    #[test]
    fn two_run_identifiers_from_one_clock_reading_differ() {
        let stamp = 1_760_000_000_000_000_000u128;
        let ids: Vec<u128> = (0..64).map(|_| run_id(stamp)).collect();
        for (i, a) in ids.iter().enumerate() {
            for b in &ids[i + 1..] {
                assert_ne!(
                    a, b,
                    "two runs reading the clock in the same nanosecond must \
                     still mint different identifiers"
                );
            }
        }
    }

    /// And the token carries the identifier, so two runs over one target
    /// partition carry two tokens.
    #[test]
    fn two_runs_give_one_target_partition_two_tokens() {
        let stamp = 1_760_000_000_000_000_000u128;
        assert_ne!(
            replay_token(run_id(stamp), "spans", "2026-10-02"),
            replay_token(run_id(stamp), "spans", "2026-10-02"),
            "the token is what the server deduplicates the block on"
        );
    }

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
