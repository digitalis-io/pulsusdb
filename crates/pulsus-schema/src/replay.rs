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
    let run = run_id()?;
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

/// Sixty-four bits from the operating system's own random source, drawn
/// once per run.
///
/// **A failed draw refuses the run.** There is no fallback, because every
/// fallback available here is a value with no collision bound, and a run
/// whose identifier has no bound can hand one target partition a token it
/// has already used — which the server answers by discarding the block.
/// Refusing is the outcome that loses nothing.
fn run_nonce() -> Result<u64, ChError> {
    getrandom::u64().map_err(|e| {
        ChError::Config(format!(
            "a trace replay needs 64 bits from the operating system's random \
             source to identify the run, and the draw failed ({e}); without \
             it the run's deduplication tokens carry no collision bound, and \
             a token this server has already seen is answered by discarding \
             the block"
        ))
    })
}

/// How many runs this process has started before this one. `fetch_add`
/// hands each caller a value no other caller gets, so two runs of one
/// process hold two values — a fact about the counter, not a probability.
fn next_run_counter() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};

    static RUNS: AtomicU64 = AtomicU64::new(0);
    RUNS.fetch_add(1, Ordering::Relaxed)
}

/// The nonce in the high half, the counter in the low half, **neither
/// altered**. Concatenated rather than mixed: a hash of the two would
/// destroy the counter's exactness and put nothing in its place, because
/// nothing promises how a hasher's output is distributed.
fn compose_run_id(nonce: u64, counter: u64) -> u128 {
    (u128::from(nonce) << 64) | u128::from(counter)
}

/// One identifier per run, which is what makes a token unique per statement
/// across runs: a nonce from the operating system above this process's run
/// counter, [`compose_run_id`].
///
/// **Why a run needs its own identifier.** Two runs that minted one
/// identifier would hand one target partition the same token twice, and the
/// server hashes the token into the block id instead of hashing the block,
/// so it would discard the second block as a duplicate. The replay would
/// drop those rows in silence, which is the one thing it promises not to do.
///
/// **The bound, in full.**
///
/// * *Two runs of one process* hold different counters, and the counter is
///   the low half unaltered, so their identifiers differ. Not a
///   probability. It holds for a process's first `2^64` runs.
/// * *Two runs in different processes* — across a restart, across a
///   cluster, or either side of a `fork`, since the nonce is drawn per run
///   rather than per process — collide only if both halves match, so at
///   most if the nonces match. Over `k` runs that is at most
///   `k(k-1)/2 · 2^-64` by the birthday bound: below `3 · 10^-8` at
///   `k = 10^6`.
///
/// **What the second rests on.** `getrandom`'s own contract: the buffer is
/// filled from the system's cryptographically secure random source — the
/// `getrandom(2)` system call on Linux — and any failure, a partial read
/// included, is an error rather than a fill. Nothing here rests on a hash
/// function's output distribution, which is what the previous construction
/// rested on and what nothing promises. A draw that fails refuses the run
/// ([`run_nonce`]), so there is no path on which an identifier is minted
/// without that contract.
fn run_id() -> Result<u128, ChError> {
    Ok(compose_run_id(run_nonce()?, next_run_counter()))
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

    /// **The counter enters the identifier unchanged.** That is what the
    /// construction rests on: the low half is this process's run counter
    /// verbatim and the high half is the run's nonce, so two runs of one
    /// process hold different counters and therefore different
    /// identifiers. A construction that mixed the two — a hash over both,
    /// say — would replace that with a collision probability no hash used
    /// here promises a bound on.
    ///
    /// Both boundary halves are included, because a construction that
    /// masked or shifted by the wrong width passes on small values.
    #[test]
    fn the_identifier_is_a_nonce_above_the_run_counter_unchanged() {
        for (nonce, counter) in [
            (0u64, 0u64),
            (0, 1),
            (1, 0),
            (0x0123_4567_89ab_cdef, 0xfedc_ba98_7654_3210),
            (u64::MAX, u64::MAX),
        ] {
            let id = compose_run_id(nonce, counter);
            assert_eq!(
                (id >> 64) as u64,
                nonce,
                "the high half is the nonce ({nonce:#018x} over {counter:#018x})"
            );
            assert_eq!(
                id as u64, counter,
                "the low half is the counter ({nonce:#018x} over {counter:#018x})"
            );
        }
        assert_eq!(
            compose_run_id(0x0123_4567_89ab_cdef, 7),
            0x0123_4567_89ab_cdef_0000_0000_0000_0007u128,
            "the two halves sit side by side, nothing mixed"
        );
    }

    /// **Two runs of one process never take one counter value**, whatever
    /// else runs between them: each caller is handed a value no other
    /// caller gets, and a later caller's value is larger. That is a fact
    /// about the counter rather than a probability, and it is the whole of
    /// the within-process bound.
    #[test]
    fn the_run_counter_hands_out_each_value_once_and_only_rises() {
        let taken: Vec<u64> = (0..1_000).map(|_| next_run_counter()).collect();
        let mut seen = taken.clone();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(
            seen.len(),
            taken.len(),
            "every value the counter hands out is its own"
        );
        for pair in taken.windows(2) {
            assert!(
                pair[1] > pair[0],
                "the counter only rises: {} then {}",
                pair[0],
                pair[1]
            );
        }
    }

    /// **The nonce is the operating system's, drawn per run.** Two draws
    /// returning one value is a 2^-64 event, so a repeat here is the draw
    /// not being a draw.
    #[test]
    fn two_run_nonces_differ() {
        let a = run_nonce().expect("the operating system's random source answers");
        let b = run_nonce().expect("the operating system's random source answers");
        assert_ne!(a, b, "each run draws its own nonce");
    }

    /// **Sixty-four runs mint sixty-four identifiers, no two alike, and no
    /// two alike in either half.** The nonce is drawn per run rather than
    /// once per process, so a process that forks between two runs does not
    /// hand the child the parent's nonce; the counter rises, so the low
    /// halves differ as well.
    ///
    /// Two runs that minted one identifier would hand one target partition
    /// the same token twice, and the server hashes the token into the block
    /// id rather than hashing the block, so it would discard the second
    /// block as a duplicate — the replay would drop those rows in silence,
    /// which is the one thing it promises not to do.
    ///
    /// **Sixty-four mints and every pair of them, not one pair.** A pair of
    /// assertions over one pair of identifiers says nothing about the
    /// sequence: an implementation that cycles between two values satisfies
    /// every one of them, because the two values differ in both halves.
    /// Code review round three wrote that implementation and the five cases
    /// here passed. Uniqueness over a run of mints is the property, so the
    /// case has to mint a run of them.
    #[test]
    fn sixty_four_runs_mint_sixty_four_identifiers_differing_in_both_halves() {
        let ids: Vec<u128> = (0..64).map(|_| run_id().expect("an identifier")).collect();
        for (i, a) in ids.iter().enumerate() {
            for (j, b) in ids.iter().enumerate().skip(i + 1) {
                assert_ne!(a, b, "runs {i} and {j} minted one identifier {a:#034x}");
                assert_ne!(
                    a >> 64,
                    b >> 64,
                    "runs {i} and {j} drew one nonce {:#018x}",
                    (a >> 64) as u64
                );
                assert_ne!(
                    *a as u64, *b as u64,
                    "runs {i} and {j} took one counter value {:#018x}",
                    *a as u64
                );
            }
        }
        for (i, pair) in ids.windows(2).enumerate() {
            assert!(
                (pair[1] as u64) > (pair[0] as u64),
                "the later run carries the larger counter: run {i} {:#018x} \
                 then run {} {:#018x}",
                pair[0] as u64,
                i + 1,
                pair[1] as u64
            );
        }
    }

    /// And the token carries the identifier, so two runs over one target
    /// partition carry two tokens.
    #[test]
    fn two_runs_give_one_target_partition_two_tokens() {
        assert_ne!(
            replay_token(run_id().expect("an identifier"), "spans", "2026-10-02"),
            replay_token(run_id().expect("an identifier"), "spans", "2026-10-02"),
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
