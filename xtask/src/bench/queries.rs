//! The four canonical LogQL read shapes (issue #16 architect plan): the
//! three issue query shapes plus the §9-mandated label/series discovery
//! shape. Each runs the **product planner's own generated SQL**
//! (`pulsus_read::logql::plan`/`sql` — never hand-written benchmark SQL):
//! a warmup pass, then `reps` timed runs, each stage of the read tagged
//! with a unique `query_id` so its `system.query_log` row is unambiguously
//! correlated. Per-stage evidence
//! (`read_rows`/`read_bytes`/`ProfileEvents['SelectedMarks']`) is summed
//! across every stage one logical read actually executes (stage 1
//! resolution + stage 2 hydration + stage 3 samples/metric read — the
//! same round trips `pulsus_read::logql::exec::LogQlEngine` makes for a
//! real request), because that sum is the real cost of answering the
//! query, not any single stage in isolation.
//!
//! `--dist` mode additionally applies the clustered-reader settings block
//! (docs/schemas.md §7) to every stage (including `EXPLAIN` capture — issue
//! #16 CODE review round 2 [medium] finding), and reads every shard's own
//! `system.query_log` via `clusterAllReplicas` (a node's `system.query_log`
//! is local; there is no automatic cluster-wide aggregation) correlated by
//! `initial_query_id`.
//!
//! **Evidence-schema note (issue #16 CODE review round 2 [high] finding).**
//! Under `prefer_localhost_replica = 1` (part of the clustered-reader
//! settings block), the initiator does not dispatch a separate sub-query to
//! its *own* shard — it reads that shard in-process. ClickHouse still logs
//! that local read, but as part of the initiator's own `is_initial_query =
//! 1` row, not as an `is_initial_query = 0` remote-shard row. [`ShardEvidence`]
//! labels that row `role = "coordinator-local"`; every other participating
//! shard's row is `role = "remote"`. **Totals-overlap caveat:** the
//! `coordinator-local` row's `read_rows`/`read_bytes`/etc. are the *same*
//! row [`read_query_log`] reads for a stage's top-level
//! `QueryEvidence`/`RunOnce` totals — those top-level totals are the
//! initiator's own local reads, **not** a cluster-wide sum (ClickHouse does
//! not write a separate "grand total" row anywhere); a cluster-wide total
//! is `coordinator-local.read_rows + Σ remote.read_rows`, computed by
//! summing the per-stage table this module emits, never read off a single
//! row.
//!
//! **Expected-roster model (issue #16 CODE review round 3 [high] finding).**
//! `optimize_skip_unused_shards = 1` (part of the clustered-reader block)
//! legitimately prunes a `fingerprint IN (...)`-predicated stage
//! (`hydration`/`samples`/`rollup_range`) to only the shards owning one of
//! those fingerprints — a `system.query_log` row that never existed because
//! its shard was correctly pruned is otherwise indistinguishable from a row
//! that *should* exist but was lost (a genuine evidence-capture bug). This
//! module closes that ambiguity by computing the **expected** participating
//! shard set client-side, the same way ClickHouse's Distributed engine
//! would: `fingerprint % total_weight` against a cumulative-weight
//! slot→shard map built from `system.clusters` (docs/schemas.md §7's
//! sharding key is the bare `fingerprint` column, no hash wrapper), then
//! asserts the **observed** participating set (shards that did nonzero
//! storage work) is *exactly* `expected` — neither a missing owner (FAIL:
//! a lost row) nor an unexpected participant (FAIL: a pruning/mapping
//! violation). `resolution` and `discovery` have no `fingerprint` predicate
//! to prune by, so their expected set is unconditionally the full cluster.
//! Every shard in the cluster is always represented in a stage's
//! [`StageEvidence::shards`] — participating shards carry their real
//! `system.query_log` row; shards correctly excluded by pruning carry a
//! synthesized `role = "expected-pruned"` entry with `pruned_reason`
//! spelling out the `fp % total_weight` derivation. The full roster is
//! therefore always **accounted for** (participating + expected-pruned),
//! even though it is not always **participating**.

use std::time::Instant;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChRow, QuerySettings, Row};
use pulsus_read::logql::sql::{self, TimeWindow};
use pulsus_read::logql::{Direction, Plan, PlanCtx, QueryParams, QuerySpec, plan};

use super::dataset::DatasetSummary;
use super::query_log::{
    QueryLogTotals, flush_logs, flush_logs_before_shard_read, read_query_log, tagged_settings,
};

/// One shard's evidence for a `--dist` run (empty in single-node mode) —
/// **every** shard in the cluster gets an entry per stage, whether it
/// participated or was legitimately pruned (the module doc comment's
/// expected-roster model). `role` is `"coordinator-local"` (the
/// initiator's own `is_initial_query = 1` row — its in-process local-shard
/// read under `prefer_localhost_replica = 1`), `"remote"`
/// (`is_initial_query = 0`, a participating non-initiator shard), or
/// `"expected-pruned"` (a shard `optimize_skip_unused_shards` correctly
/// excluded — `read_rows`/`read_bytes`/`selected_marks` are `0` and
/// `pruned_reason` explains the `fp % total_weight` derivation). See the
/// module doc comment's evidence-schema note for the totals-overlap
/// caveat: the `coordinator-local` row is the *same* row a stage's
/// top-level `read_rows`/etc. come from, so summing this vec's
/// participating rows double-counts it against those top-level fields —
/// this vec is per-shard attribution, not an additional total.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ShardEvidence {
    pub shard_num: u32,
    pub hostname: String,
    pub role: String,
    pub read_rows: u64,
    pub read_bytes: u64,
    pub selected_marks: u64,
    pub pruned_reason: Option<String>,
}

/// Per-stage `--dist` evidence: one entry per logical stage a shape's read
/// actually executes (`resolution`/`hydration`/`samples`/`rollup_range`/
/// `discovery`) — issue #16 CODE review [high] finding. A shape's
/// shard-locality claim depends on *every* stage running shard-locally,
/// not just the terminal one; capturing only the terminal stage cannot
/// substantiate a claim about stage-1 resolution or stage-2 hydration.
#[derive(Debug, Clone, serde::Serialize)]
pub struct StageEvidence {
    pub stage: String,
    pub shards: Vec<ShardEvidence>,
    pub explain_pipeline: Vec<String>,
}

/// One query shape's full evidence: wall-clock percentiles over `reps`
/// warm runs, `system.query_log`-derived cost (summed across every stage
/// the read executes), the terminal stage's `EXPLAIN indexes = 1`
/// capture, and — in `--dist` mode — every stage's own shard/pipeline
/// evidence.
#[derive(Debug, Clone, serde::Serialize)]
pub struct QueryEvidence {
    pub name: String,
    pub sql: String,
    pub wall_ms_p50: f64,
    pub wall_ms_p95: f64,
    pub wall_ms_p99: f64,
    pub returned_rows: u64,
    pub read_rows: u64,
    pub read_bytes: u64,
    pub selected_marks: u64,
    pub total_marks: u64,
    pub memory_usage: u64,
    pub query_duration_ms: u64,
    pub explain_indexes: Vec<String>,
    pub stage_evidence: Vec<StageEvidence>,
}

fn percentile(sorted_ms: &[f64], p: f64) -> f64 {
    if sorted_ms.is_empty() {
        return 0.0;
    }
    let idx = (((sorted_ms.len() - 1) as f64) * p).round() as usize;
    sorted_ms[idx.min(sorted_ms.len() - 1)]
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct FingerprintRow {
    fingerprint: u64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct StreamMetaRow {
    fingerprint: u64,
    service: String,
    #[allow(dead_code)]
    labels: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SampleRow {
    #[allow(dead_code)]
    fingerprint: u64,
    #[allow(dead_code)]
    timestamp_ns: i64,
    #[allow(dead_code)]
    body: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct MetricRow {
    #[allow(dead_code)]
    fingerprint: u64,
    #[allow(dead_code)]
    step: i64,
    #[allow(dead_code)]
    n: u64,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct LabelNameRow {
    #[allow(dead_code)]
    name: String,
}

/// One shard's raw `system.query_log` row for a stage, before it is
/// resolved to a `shard_num` and classified against the expected roster.
#[derive(Debug, Clone)]
struct RawShardRow {
    hostname: String,
    is_initial_query: u8,
    read_rows: u64,
    read_bytes: u64,
    selected_marks: u64,
}

/// Reads every shard's own correlated `system.query_log` row for
/// `initial_query_id` — an initial query's own `initial_query_id` equals
/// its `query_id`, so this matches **both** the initiator's own
/// `is_initial_query = 1` row (its coordinator-local read; see the module
/// doc comment's evidence-schema note) and every remote shard's
/// `is_initial_query = 0` sub-query row. A node's `system.query_log` is
/// local only — no automatic cluster-wide aggregation — so this fans out
/// via `clusterAllReplicas`.
async fn read_stage_query_log_rows(
    client: &ChClient,
    cluster: &str,
    initial_query_id: &str,
) -> anyhow::Result<Vec<RawShardRow>> {
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct ShardRow {
        hostname: String,
        is_initial_query: u8,
        read_rows: u64,
        read_bytes: u64,
        selected_marks: u64,
    }
    let sql = format!(
        "SELECT hostName() AS hostname, is_initial_query, read_rows, read_bytes, \
         ProfileEvents['SelectedMarks'] AS selected_marks \
         FROM clusterAllReplicas('{cluster}', system.query_log) \
         WHERE initial_query_id = '{initial_query_id}' AND type = 'QueryFinish'"
    );
    let mut stream = client
        .query_stream::<ShardRow>(&sql, &QuerySettings::new())
        .await?;
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        let row = row?;
        out.push(RawShardRow {
            hostname: row.hostname,
            is_initial_query: row.is_initial_query,
            read_rows: row.read_rows,
            read_bytes: row.read_bytes,
            selected_marks: row.selected_marks,
        });
    }
    Ok(out)
}

/// Cluster shard topology needed to compute a fingerprint-scoped stage's
/// expected owning shard set (issue #16 CODE review round 3 [high]
/// finding): `system.clusters`' shard/weight roster and `system.macros`'
/// per-node `{shard}` value, mapping every replica hostname to its shard
/// number.
#[derive(Debug, Clone)]
pub(crate) struct ClusterTopology {
    /// `slots[i]` is the `shard_num` owning cumulative-weight slot `i` of
    /// `[0, total_weight)` — a general cumulative-weight slot assignment,
    /// not a bare `%` over shard count, so it stays correct if shard
    /// weights ever diverge from equal (this fixture's are equal:
    /// `ci/bench-cluster/*/macros.xml` sets no `<weight>`).
    slots: Vec<u32>,
    total_weight: u64,
    /// Authoritative shard_num set, from `system.clusters` alone.
    /// [`ClusterTopology::all_shards`] returns this field directly, never
    /// something derived from `shard_to_hostname`'s keys (issue #16 CODE
    /// review round 4 [medium] finding: `shard_to_hostname` is validated
    /// to be a bijection onto this set at load time by
    /// [`build_shard_maps`], but keeping `all_shards()`'s source of truth
    /// pinned to `system.clusters` — not the macro-derived map — means a
    /// future change to that validation can't silently make `all_shards()`
    /// track a wrong or partial roster).
    shards: std::collections::BTreeSet<u32>,
    hostname_to_shard: std::collections::HashMap<String, u32>,
    shard_to_hostname: std::collections::HashMap<u32, String>,
}

impl ClusterTopology {
    /// ClickHouse's Distributed engine shard selection for a
    /// non-negative-integer sharding-key expression (our bare
    /// `fingerprint` column, docs/schemas.md §7 — no hash wrapper):
    /// `slots[value % total_weight]`.
    pub(crate) fn shard_for_fingerprint(&self, fingerprint: u64) -> u32 {
        let slot = (fingerprint % self.total_weight) as usize;
        self.slots[slot]
    }

    pub(crate) fn all_shards(&self) -> std::collections::BTreeSet<u32> {
        self.shards.clone()
    }

    /// The cumulative-weight modulus of the sharding-slot map — exposed
    /// for `traces-read`'s `cityHash64(trace_id) % total_weight` pruning
    /// derivation strings (issue #57 AC4; same slot model, hash-valued
    /// key).
    pub(crate) fn total_weight(&self) -> u64 {
        self.total_weight
    }

    /// Resolves a `system.query_log` row's hostname to its shard_num —
    /// exposed for `traces-read`'s per-shard evidence attribution
    /// (issue #57 AC4).
    pub(crate) fn shard_of_hostname(&self, hostname: &str) -> Option<u32> {
        self.hostname_to_shard.get(hostname).copied()
    }
}

/// Validates that `macro_rows` (`(hostname, {shard} macro value)` pairs)
/// forms an exact bijection onto `authoritative_shards` (the shard_num set
/// read from `system.clusters`) and, if so, returns the two lookup maps —
/// issue #16 CODE review round 4 [medium] finding: a bad, duplicate, or
/// out-of-roster `{shard}` macro value must fail loudly *here*, not
/// silently produce a wrong `hostname_to_shard`/`shard_to_hostname` map
/// that a later per-stage `observed == expected` roster check could then
/// pass against for the wrong reason. Pure (no I/O), so the
/// missing/duplicate/out-of-roster cases are unit-testable without a live
/// cluster.
fn build_shard_maps(
    cluster: &str,
    authoritative_shards: &std::collections::BTreeSet<u32>,
    macro_rows: &[(String, u32)],
) -> anyhow::Result<(
    std::collections::HashMap<String, u32>,
    std::collections::HashMap<u32, String>,
)> {
    let mut hostname_to_shard = std::collections::HashMap::new();
    let mut shard_to_hostname: std::collections::HashMap<u32, String> =
        std::collections::HashMap::new();
    for (hostname, shard_num) in macro_rows {
        anyhow::ensure!(
            authoritative_shards.contains(shard_num),
            "cluster {cluster:?}: node {hostname:?} reports {{shard}} = {shard_num}, which is \
             not one of system.clusters' shard_nums {authoritative_shards:?} — out-of-roster \
             {{shard}} macro value"
        );
        if let Some(existing_hostname) = shard_to_hostname.get(shard_num) {
            anyhow::bail!(
                "cluster {cluster:?}: shard_num {shard_num} is claimed by both {existing_hostname:?} \
                 and {hostname:?} — duplicate {{shard}} macro value"
            );
        }
        hostname_to_shard.insert(hostname.clone(), *shard_num);
        shard_to_hostname.insert(*shard_num, hostname.clone());
    }
    let observed_shards: std::collections::BTreeSet<u32> =
        shard_to_hostname.keys().copied().collect();
    anyhow::ensure!(
        &observed_shards == authoritative_shards,
        "cluster {cluster:?}: the {{shard}} macro map covers {observed_shards:?}, missing {:?} \
         from system.clusters' shard_nums {authoritative_shards:?} — every shard must report its \
         own {{shard}} macro",
        authoritative_shards
            .difference(&observed_shards)
            .collect::<Vec<_>>()
    );
    Ok((hostname_to_shard, shard_to_hostname))
}

/// Loads [`ClusterTopology`] for `cluster` — called once per `--dist` run,
/// not per stage (the topology does not change mid-run).
pub(crate) async fn load_cluster_topology(
    client: &ChClient,
    cluster: &str,
) -> anyhow::Result<ClusterTopology> {
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct ClusterRow {
        shard_num: u32,
        shard_weight: u64,
    }
    let sql = format!(
        "SELECT DISTINCT shard_num, toUInt64(shard_weight) AS shard_weight FROM system.clusters \
         WHERE cluster = '{cluster}' ORDER BY shard_num"
    );
    let mut stream = client
        .query_stream::<ClusterRow>(&sql, &QuerySettings::new())
        .await?;
    let mut rows = Vec::new();
    while let Some(row) = stream.next().await {
        rows.push(row?);
    }
    anyhow::ensure!(
        !rows.is_empty(),
        "cluster {cluster:?} has no shards in system.clusters — check --cluster"
    );

    let mut slots = Vec::new();
    for row in &rows {
        anyhow::ensure!(
            row.shard_weight > 0,
            "cluster {cluster:?} shard {} has a zero shard_weight — cannot compute a sharding \
             slot map",
            row.shard_num
        );
        for _ in 0..row.shard_weight {
            slots.push(row.shard_num);
        }
    }
    let total_weight = slots.len() as u64;
    let authoritative_shards: std::collections::BTreeSet<u32> =
        rows.iter().map(|row| row.shard_num).collect();

    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct MacroRow {
        hostname: String,
        shard_num: u32,
    }
    let macro_sql = format!(
        "SELECT hostName() AS hostname, toUInt32(substitution) AS shard_num \
         FROM clusterAllReplicas('{cluster}', system.macros) WHERE macro = 'shard'"
    );
    let mut macro_stream = client
        .query_stream::<MacroRow>(&macro_sql, &QuerySettings::new())
        .await?;
    let mut macro_rows = Vec::new();
    while let Some(row) = macro_stream.next().await {
        let row = row?;
        macro_rows.push((row.hostname, row.shard_num));
    }
    anyhow::ensure!(
        !macro_rows.is_empty(),
        "cluster {cluster:?} has no {{shard}} macro visible via system.macros — check every \
         node's macros.xml"
    );

    let (hostname_to_shard, shard_to_hostname) =
        build_shard_maps(cluster, &authoritative_shards, &macro_rows)?;

    Ok(ClusterTopology {
        slots,
        total_weight,
        shards: authoritative_shards,
        hostname_to_shard,
        shard_to_hostname,
    })
}

#[cfg(test)]
mod cluster_topology_tests {
    use super::build_shard_maps;
    use std::collections::BTreeSet;

    fn shards(nums: &[u32]) -> BTreeSet<u32> {
        nums.iter().copied().collect()
    }

    fn rows(pairs: &[(&str, u32)]) -> Vec<(String, u32)> {
        pairs.iter().map(|(h, s)| (h.to_string(), *s)).collect()
    }

    #[test]
    fn build_shard_maps_accepts_an_exact_bijection() {
        let authoritative = shards(&[1, 2, 3, 4]);
        let macro_rows = rows(&[("a", 1), ("b", 2), ("c", 3), ("d", 4)]);
        let (hostname_to_shard, shard_to_hostname) =
            build_shard_maps("c", &authoritative, &macro_rows).expect("bijection is valid");
        assert_eq!(hostname_to_shard.len(), 4);
        assert_eq!(shard_to_hostname.len(), 4);
        assert_eq!(shard_to_hostname.get(&3), Some(&"c".to_string()));
    }

    #[test]
    fn build_shard_maps_rejects_a_missing_shard() {
        let authoritative = shards(&[1, 2, 3, 4]);
        // shard 4 never reports its {shard} macro.
        let macro_rows = rows(&[("a", 1), ("b", 2), ("c", 3)]);
        let err = build_shard_maps("c", &authoritative, &macro_rows).unwrap_err();
        assert!(err.to_string().contains("missing"), "{err}");
    }

    #[test]
    fn build_shard_maps_rejects_a_duplicate_shard_num() {
        let authoritative = shards(&[1, 2, 3, 4]);
        // Two different hostnames both claim shard 2 — shard 4 never
        // reports, but the duplicate is detected first (fail fast).
        let macro_rows = rows(&[("a", 1), ("b", 2), ("c", 2), ("d", 3)]);
        let err = build_shard_maps("c", &authoritative, &macro_rows).unwrap_err();
        assert!(err.to_string().contains("duplicate"), "{err}");
    }

    #[test]
    fn build_shard_maps_rejects_an_out_of_roster_shard_num() {
        let authoritative = shards(&[1, 2, 3, 4]);
        // shard 5 is not a shard_num system.clusters knows about.
        let macro_rows = rows(&[("a", 1), ("b", 2), ("c", 3), ("d", 5)]);
        let err = build_shard_maps("c", &authoritative, &macro_rows).unwrap_err();
        assert!(err.to_string().contains("out-of-roster"), "{err}");
    }
}

/// Captures `EXPLAIN {kind} {sql}`'s output lines, tagged `query_id` and
/// run under the clustered-reader settings block when `dist` (issue #16
/// CODE review round 2 [medium] finding: `EXPLAIN PIPELINE` under plain
/// settings does not reflect the same `optimize_distributed_group_by_
/// sharding_key`/`prefer_localhost_replica` settings the actual reads use,
/// and can show a different pipeline shape — a coordinator re-merge step
/// vs. shard-local finalization — which is exactly the evidence being
/// captured). `EXPLAIN indexes = 1` is threaded through for consistency;
/// **verified live it is not byte-identical to the plain-settings
/// single-node capture** — under `prefer_localhost_replica = 1` it
/// reflects the *coordinator's own local shard's* index-pruning view, so
/// a literal `fingerprint IN (...)` list with (say) 10 elements can show
/// a narrower "N-element set" in the `PrimaryKey` `Condition` line when
/// only a subset of those values are relevant to that one shard's local
/// parts. This is not data loss — the query's actual SQL text and
/// returned-row counts are unaffected, verified against the single-node
/// baseline — it is itself further evidence of shard-local index
/// confinement, so it is recorded as observed rather than asserted
/// unchanged.
async fn explain_lines(
    client: &ChClient,
    kind: &str,
    sql: &str,
    dist: bool,
    query_id: &str,
) -> anyhow::Result<Vec<String>> {
    if sql.is_empty() {
        return Ok(Vec::new());
    }
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct ExplainRow {
        explain: String,
    }
    let full = format!("EXPLAIN {kind} {sql}").replace('?', "??");
    let settings = reader_settings(dist, query_id);
    let mut stream = client.query_stream::<ExplainRow>(&full, &settings).await?;
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row?.explain);
    }
    Ok(out)
}

/// Which shard set a stage's read is expected to touch — computed
/// client-side from the sharding key so a lost `system.query_log` row can
/// be told apart from `optimize_skip_unused_shards` legitimately pruning a
/// shard (issue #16 CODE review round 3 [high] finding; see the module
/// doc comment's expected-roster model).
#[derive(Debug, Clone)]
enum StageRoster {
    /// `resolution` (label-index `(key,val)` scan) and `discovery` (full
    /// `log_streams_idx` scan): no `fingerprint` predicate to prune by, so
    /// every shard in the cluster is expected to participate.
    Full,
    /// `hydration`/`samples`/`rollup_range`: a `fingerprint IN (...)`
    /// predicate — the expected participants are exactly the shards
    /// owning one or more of these fingerprints under
    /// [`ClusterTopology::shard_for_fingerprint`].
    Fingerprints(Vec<u64>),
}

/// One executed stage of a shape's read: its logical name
/// (`resolution`/`hydration`/`samples`/`rollup_range`/`discovery`), the
/// unique `query_id` its `system.query_log`/`EXPLAIN` rows are tagged
/// with, its SQL text, and its expected shard [`StageRoster`].
#[derive(Debug, Clone)]
struct StageRef {
    stage: &'static str,
    query_id: String,
    sql: String,
    roster: StageRoster,
}

/// Builds the `expected-pruned` derivation string for `shard_num` — the
/// exact `fp % total_weight` reasoning `optimize_skip_unused_shards` uses
/// to exclude it, so a reviewer can verify the pruning by hand rather than
/// take the harness's word for it.
fn pruned_reason(roster: &StageRoster, topology: &ClusterTopology, shard_num: u32) -> String {
    match roster {
        // Never reached: a `Full` roster's `expected` set is every shard,
        // so no shard is ever pruned from it. Kept for exhaustiveness.
        StageRoster::Full => {
            format!("shard {shard_num} unexpectedly excluded from a Full-roster stage")
        }
        StageRoster::Fingerprints(fingerprints) => {
            let slots: Vec<u64> = fingerprints
                .iter()
                .map(|fp| fp % topology.total_weight)
                .collect();
            let owning_shards: std::collections::BTreeSet<u32> = slots
                .iter()
                .map(|slot| topology.slots[*slot as usize])
                .collect();
            format!(
                "optimize_skip_unused_shards pruned shard {shard_num}: none of the {} queried \
                 fingerprints map to it (fingerprint % total_weight={} over slots {:?} resolves \
                 to owning shards {:?} — shard {shard_num} is not among them)",
                fingerprints.len(),
                topology.total_weight,
                slots,
                owning_shards
            )
        }
    }
}

/// Captures one stage's `--dist` evidence against its **computed expected
/// shard roster** (issue #16 CODE review round 3 [high] finding — see the
/// module doc comment's expected-roster model):
/// 1. Reads every shard's own `system.query_log` row
///    ([`read_stage_query_log_rows`]) and resolves each to a `shard_num`
///    via `topology.hostname_to_shard`.
/// 2. Computes `expected` from `stage.roster`.
/// 3. Computes `observed` as the shards that did nonzero storage work
///    (`read_rows > 0 || selected_marks > 0` — a row that exists but did
///    zero work, e.g. a pruned coordinator's own orchestration-only row,
///    does not count as participating).
/// 4. Asserts `observed == expected` **exactly**: a shard in `expected`
///    but missing from `observed` is a lost `system.query_log` row (FAIL —
///    indistinguishable from data loss any other way); a shard in
///    `observed` but not `expected` is a pruning/sharding-mapping
///    violation (FAIL).
/// 5. Emits one [`ShardEvidence`] **per shard in the cluster**: expected
///    shards get their real row (`role` = `coordinator-local`/`remote`);
///    non-expected shards get a synthesized `role = "expected-pruned"`
///    entry (`read_rows = 0`, `pruned_reason` carrying the derivation) —
///    so the full roster is always *accounted for*, whether participating
///    or provably, derivedly pruned.
///
/// Also asserts (unconditionally, regardless of pruning): exactly one
/// `coordinator-local` row, never more distinct shard rows than the
/// cluster has shards, a nonempty `EXPLAIN PIPELINE`, and the
/// coordinator/remote balance sanity guard (a coordinator row
/// disproportionately larger than any remote row would mean a
/// cluster-wide total row was captured instead of the coordinator's own
/// local-only read).
async fn capture_stage_evidence(
    client: &ChClient,
    cluster: &str,
    topology: &ClusterTopology,
    stage: &StageRef,
) -> anyhow::Result<StageEvidence> {
    let raw_rows = read_stage_query_log_rows(client, cluster, &stage.query_id).await?;
    let explain_pipeline = explain_lines(
        client,
        "PIPELINE",
        &stage.sql,
        true,
        &format!("{}-explain-pipeline", stage.query_id),
    )
    .await?;
    anyhow::ensure!(
        !explain_pipeline.is_empty(),
        "stage {:?} (query_id={}) yielded an empty EXPLAIN PIPELINE in --dist mode; SQL:\n{}",
        stage.stage,
        stage.query_id,
        stage.sql
    );

    let mut by_shard: std::collections::BTreeMap<u32, RawShardRow> =
        std::collections::BTreeMap::new();
    for row in raw_rows {
        let shard_num = *topology
            .hostname_to_shard
            .get(&row.hostname)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "stage {:?} (query_id={}): system.query_log row from hostname {:?} has no \
                 corresponding {{shard}} macro in the cluster topology",
                    stage.stage,
                    stage.query_id,
                    row.hostname
                )
            })?;
        by_shard.insert(shard_num, row);
    }

    let expected: std::collections::BTreeSet<u32> = match &stage.roster {
        StageRoster::Full => topology.all_shards(),
        StageRoster::Fingerprints(fingerprints) => fingerprints
            .iter()
            .map(|fp| topology.shard_for_fingerprint(*fp))
            .collect(),
    };
    let observed: std::collections::BTreeSet<u32> = by_shard
        .iter()
        .filter(|(_, row)| row.read_rows > 0 || row.selected_marks > 0)
        .map(|(shard, _)| *shard)
        .collect();
    anyhow::ensure!(
        observed == expected,
        "stage {:?} (query_id={}): observed participating shards {:?} != expected {:?} — \
         missing {:?} (a lost system.query_log row, indistinguishable from data loss any other \
         way) or unexpected {:?} (a pruning/sharding-mapping violation); SQL:\n{}",
        stage.stage,
        stage.query_id,
        observed,
        expected,
        expected.difference(&observed).collect::<Vec<_>>(),
        observed.difference(&expected).collect::<Vec<_>>(),
        stage.sql
    );

    let coordinator_shards: Vec<u32> = by_shard
        .iter()
        .filter(|(_, row)| row.is_initial_query == 1)
        .map(|(shard, _)| *shard)
        .collect();
    anyhow::ensure!(
        coordinator_shards.len() == 1,
        "stage {:?} (query_id={}) yielded {} coordinator-local (is_initial_query = 1) rows, \
         expected exactly 1; SQL:\n{}",
        stage.stage,
        stage.query_id,
        coordinator_shards.len(),
        stage.sql
    );
    let cluster_size = topology.all_shards().len();
    anyhow::ensure!(
        by_shard.len() <= cluster_size,
        "stage {:?} (query_id={}) yielded {} distinct shard rows, more than the cluster's \
         {cluster_size} shards — duplicate/phantom shard-evidence row; SQL:\n{}",
        stage.stage,
        stage.query_id,
        by_shard.len(),
        stage.sql
    );

    // Coordinator/remote balance sanity guard: the coordinator-local row
    // must be the same order of magnitude as an individual remote
    // shard's read_rows (balanced `fingerprint` co-sharding), never
    // inflated to roughly `cluster_size` times a remote shard's value —
    // that shape would mean this query accidentally captured a
    // cluster-wide total row instead of the coordinator's own
    // local-only MergeTree read (the exact hazard the module doc
    // comment's totals-overlap caveat warns about).
    let coord_shard = coordinator_shards[0];
    if let Some(coord_row) = by_shard.get(&coord_shard) {
        let max_remote = by_shard
            .iter()
            .filter(|(shard, _)| **shard != coord_shard)
            .map(|(_, row)| row.read_rows)
            .max()
            .unwrap_or(0);
        anyhow::ensure!(
            max_remote == 0
                || coord_row.read_rows <= max_remote.saturating_mul(cluster_size as u64),
            "stage {:?}: coordinator-local read_rows ({}) is disproportionately larger than the \
             largest remote shard's ({max_remote}) — looks like a cluster-wide total row was \
             captured instead of the coordinator's own local read",
            stage.stage,
            coord_row.read_rows
        );
    }

    // Full-roster assembly: every shard in the cluster gets an entry,
    // participating (real row) or expected-pruned (synthesized, derived).
    let mut shards = Vec::with_capacity(cluster_size);
    for shard_num in topology.all_shards() {
        if let Some(row) = by_shard.get(&shard_num) {
            let role = if row.is_initial_query == 1 {
                "coordinator-local"
            } else {
                "remote"
            };
            shards.push(ShardEvidence {
                shard_num,
                hostname: row.hostname.clone(),
                role: role.to_string(),
                read_rows: row.read_rows,
                read_bytes: row.read_bytes,
                selected_marks: row.selected_marks,
                pruned_reason: None,
            });
        } else {
            shards.push(ShardEvidence {
                shard_num,
                hostname: topology
                    .shard_to_hostname
                    .get(&shard_num)
                    .cloned()
                    .unwrap_or_default(),
                role: "expected-pruned".to_string(),
                read_rows: 0,
                read_bytes: 0,
                selected_marks: 0,
                pruned_reason: Some(pruned_reason(&stage.roster, topology, shard_num)),
            });
        }
    }

    Ok(StageEvidence {
        stage: stage.stage.to_string(),
        shards,
        explain_pipeline,
    })
}

/// Captures `--dist` evidence for every stage in `stages`, in order —
/// shared by every shape runner ([`run_shape`], `run_metric_shape`,
/// `run_discovery_shape`) so the expected-roster discipline lives in one
/// place.
async fn capture_all_stage_evidence(
    client: &ChClient,
    cluster: &str,
    topology: &ClusterTopology,
    stages: &[StageRef],
) -> anyhow::Result<Vec<StageEvidence>> {
    let mut out = Vec::with_capacity(stages.len());
    for stage in stages {
        out.push(capture_stage_evidence(client, cluster, topology, stage).await?);
    }
    Ok(out)
}

/// Total marks the corpus's local table holds — the denominator for the
/// `selected_marks`/`total_marks` skip-index ratio. `cluster: Some(_)`
/// sums every shard's own `system.parts` via `clusterAllReplicas`: in
/// `--dist` mode the corpus is spread across every shard's own local
/// table (docs/schemas.md §7's `fingerprint` co-sharding), so a
/// single-node `system.parts` read would only see its own shard's marks
/// and understate the denominator.
async fn total_marks(
    client: &ChClient,
    db: &str,
    table: &str,
    cluster: Option<&str>,
) -> anyhow::Result<u64> {
    let base_table = table.trim_end_matches("_dist");
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct MarksRow {
        marks: u64,
    }
    let source = match cluster {
        Some(cluster) => format!("clusterAllReplicas('{cluster}', system.parts)"),
        None => "system.parts".to_string(),
    };
    let sql = format!(
        "SELECT sum(marks) AS marks FROM {source} WHERE database = '{db}' \
         AND table = '{base_table}' AND active"
    );
    let mut stream = client
        .query_stream::<MarksRow>(&sql, &QuerySettings::new())
        .await?;
    Ok(match stream.next().await {
        Some(row) => row?.marks,
        None => 0,
    })
}

/// Table names for [`PlanCtx`] / per-stage SQL builders: bare (`--dist`
/// off) or `_dist`-suffixed (`--dist` on) — never database-qualified,
/// matching `pulsus_read::logql::sql`'s own convention (the connection's
/// default database resolves unqualified names).
struct Tables {
    streams_idx: String,
    streams: String,
    samples: String,
    rollup: String,
}

impl Tables {
    fn new(dist: bool) -> Self {
        let suffix = if dist { "_dist" } else { "" };
        Tables {
            streams_idx: format!("log_streams_idx{suffix}"),
            streams: format!("log_streams{suffix}"),
            samples: format!("log_samples{suffix}"),
            rollup: format!("log_metrics_5s{suffix}"),
        }
    }

    fn plan_ctx<'a>(&'a self, db: &'a str) -> PlanCtx<'a> {
        PlanCtx {
            db,
            streams_idx: &self.streams_idx,
            streams: &self.streams,
            samples: &self.samples,
            rollup_table: &self.rollup,
            rollup_res_ns: 5_000_000_000,
            scan_budget_bytes: 200 * 1024 * 1024 * 1024,
            max_streams: 1_000_000,
        }
    }
}

/// `SETTINGS log_comment` run-tag every query in this scenario carries
/// (issue #35 architect plan) — a fixed constant rather than a
/// threaded-through parameter, since every [`reader_settings`] call site in
/// this file is already scoped to the single `logs-read` scenario (`bench
/// metrics-labels`/`bench logs-hydration` tag their own settings via their
/// own modules' calls into [`super::query_log::tagged_settings`]). Purely
/// additive: `log_comment` is an HTTP request setting, never inlined SQL,
/// and does not touch `read_rows`/`read_bytes`/`SelectedMarks`, so
/// `query_log_gates.rs` and the committed `logs-read-ci.json`/
/// `logs-read-dist.json` numbers are unchanged — it only makes this
/// scenario's `system.query_log` rows independently greppable/correlatable
/// by `log_comment` alongside `query_id`.
const LOG_COMMENT: &str = "pulsus-bench:logs-read";

/// Settings applied to every timed query: the clustered-reader block
/// (docs/schemas.md §7) in `--dist` mode, plain otherwise, plus the
/// per-query `query_id` tag every stage carries and the scenario-wide
/// [`LOG_COMMENT`] tag (routed through the shared
/// [`super::query_log::tagged_settings`] so every scenario's `log_comment`
/// application lives in one place). `skip_unavailable_shards = false`
/// (unlike the product's own configurable `PULSUS_SKIP_UNAVAILABLE_SHARDS`
/// default) — this harness exists to *capture evidence*, and a
/// verification/benchmark run should fail loudly on genuine shard
/// unavailability rather than silently tolerate it and record evidence for
/// a degraded read as if it were normal.
fn reader_settings(dist: bool, query_id: &str) -> QuerySettings {
    let base = if dist {
        QuerySettings::clustered_reader(false)
    } else {
        QuerySettings::new()
    };
    tagged_settings(base, query_id, Some(LOG_COMMENT))
}

async fn resolve_fingerprints(
    client: &ChClient,
    stage1_sql: &str,
    query_id: &str,
    dist: bool,
) -> anyhow::Result<Vec<u64>> {
    let settings = reader_settings(dist, query_id);
    let mut stream = client
        .query_stream::<FingerprintRow>(stage1_sql, &settings)
        .await?;
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row?.fingerprint);
    }
    Ok(out)
}

async fn hydrate(
    client: &ChClient,
    stage2_sql: &str,
    query_id: &str,
    dist: bool,
) -> anyhow::Result<Vec<StreamMetaRow>> {
    let settings = reader_settings(dist, query_id);
    let mut stream = client
        .query_stream::<StreamMetaRow>(stage2_sql, &settings)
        .await?;
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row?);
    }
    Ok(out)
}

/// One rep's outcome: the row count the terminal stage returned, the
/// summed `system.query_log` evidence across every stage executed, and
/// every stage actually run (used for the `EXPLAIN`/`--dist` per-stage
/// capture — those want the *real* predicate values a rep actually ran,
/// not a synthetic placeholder). `stages.last()` is the terminal stage.
struct RunOnce {
    returned_rows: u64,
    totals: QueryLogTotals,
    stages: Vec<StageRef>,
}

impl RunOnce {
    /// The terminal (final, result-returning) stage — always present:
    /// every code path that constructs a `RunOnce` pushes at least one
    /// stage before returning.
    fn terminal(&self) -> &StageRef {
        self.stages
            .last()
            .expect("RunOnce always carries at least one stage")
    }
}

/// Flushes `system.query_log` once, then sums every `ids` row's evidence.
/// **Must run after every stage of a rep has finished executing** — a
/// per-stage `SYSTEM FLUSH LOGS` immediately after that single stage would
/// work too, but batching one flush per rep (not one per stage) keeps the
/// harness from tripling ClickHouse's log-flush traffic for no benefit:
/// `system.query_log`'s `QueryFinish` row is only written to the
/// queryable table on `SYSTEM FLUSH LOGS` (or ClickHouse's own periodic
/// flush interval), so reading it before flushing silently returns
/// nothing — the bug this ordering exists to avoid.
async fn sum_query_log(client: &ChClient, ids: &[String]) -> anyhow::Result<QueryLogTotals> {
    flush_logs(client).await?;
    let mut totals = QueryLogTotals::default();
    for id in ids {
        totals += read_query_log(client, id).await?;
    }
    Ok(totals)
}

/// Executes one streams-shape read (stage 1 -> stage 2 -> stage 3, the
/// same round trips `LogQlEngine::run_streams_inner` makes) tagged
/// `base_id`. Every stage runs to completion **before** any
/// `system.query_log` read ([`sum_query_log`]'s ordering requirement).
async fn run_streams_once(
    client: &ChClient,
    db: &str,
    tables: &Tables,
    query: &str,
    params: &QueryParams,
    base_id: &str,
    dist: bool,
) -> anyhow::Result<RunOnce> {
    let expr = pulsus_logql::parse(query)?;
    let sp = match plan(&expr, params, &tables.plan_ctx(db))? {
        Plan::Streams(sp) => sp,
        Plan::Metric(_) => anyhow::bail!("expected a Streams plan for {query}"),
    };

    let s1_id = format!("{base_id}-s1");
    let fingerprints = resolve_fingerprints(client, &sp.stage1_sql, &s1_id, dist).await?;
    let mut stages = vec![StageRef {
        stage: "resolution",
        query_id: s1_id.clone(),
        sql: sp.stage1_sql.clone(),
        roster: StageRoster::Full,
    }];
    if fingerprints.is_empty() {
        let totals = sum_query_log(client, std::slice::from_ref(&s1_id)).await?;
        return Ok(RunOnce {
            returned_rows: 0,
            totals,
            stages,
        });
    }

    let s2_id = format!("{base_id}-s2");
    let stage2_sql = sql::stage2(&tables.streams, &fingerprints);
    let meta = hydrate(client, &stage2_sql, &s2_id, dist).await?;
    stages.push(StageRef {
        stage: "hydration",
        query_id: s2_id.clone(),
        sql: stage2_sql,
        roster: StageRoster::Fingerprints(fingerprints.clone()),
    });

    let mut services: Vec<&str> = meta.iter().map(|m| m.service.as_str()).collect();
    services.sort_unstable();
    services.dedup();
    let escaped: Vec<String> = services
        .into_iter()
        .map(pulsus_read::logql::escape::ch_string)
        .collect();

    let s3_id = format!("{base_id}-s3");
    let sql3 = sql::stage3(
        &tables.samples,
        &escaped,
        &fingerprints,
        TimeWindow {
            start_ns: sp.start_ns,
            end_ns: sp.end_ns,
        },
        &sp.line_filters,
        sp.direction,
        sp.limit,
    );
    let settings3 = reader_settings(dist, &s3_id);
    let returned = {
        let mut stream = client.query_stream::<SampleRow>(&sql3, &settings3).await?;
        let mut n = 0u64;
        while let Some(row) = stream.next().await {
            row?;
            n += 1;
        }
        n
    };
    stages.push(StageRef {
        stage: "samples",
        query_id: s3_id.clone(),
        sql: sql3,
        roster: StageRoster::Fingerprints(fingerprints.clone()),
    });

    let totals = sum_query_log(client, &[s1_id, s2_id, s3_id]).await?;

    Ok(RunOnce {
        returned_rows: returned,
        totals,
        stages,
    })
}

/// `reps`/`dist`/`cluster`/`topology` grouped into one parameter (clippy's
/// argument-count lint, same rationale as `pulsus_read::logql::sql::
/// TimeWindow`): every canonical-shape runner needs all four, and they
/// always travel together. `topology` is `None` when `!dist` (unused in
/// that mode) — loaded once per `--dist` run by [`load_cluster_topology`],
/// not per stage.
#[derive(Debug, Clone, Copy)]
pub struct RunConfig<'a> {
    pub reps: usize,
    pub dist: bool,
    pub cluster: &'a str,
    pub topology: Option<&'a ClusterTopology>,
}

/// Runs one query shape: a warmup pass (discarded), then `cfg.reps` timed
/// runs. Wall p50/p95/p99 come from every timed rep; `system.query_log`
/// evidence, `EXPLAIN`, and `--dist` shard evidence come from the first
/// timed rep only (the corpus is static, so a deterministic query's
/// server-side cost does not vary rep to rep — only wall time does, from
/// cache warmth/scheduling noise).
async fn run_shape(
    client: &ChClient,
    db: &str,
    tables: &Tables,
    name: &str,
    query: &str,
    params: &QueryParams,
    cfg: RunConfig<'_>,
) -> anyhow::Result<QueryEvidence> {
    let base_id = format!("bench-{name}-{}", std::process::id());
    run_streams_once(
        client,
        db,
        tables,
        query,
        params,
        &format!("{base_id}-warmup"),
        cfg.dist,
    )
    .await?;

    let mut wall_ms: Vec<f64> = Vec::with_capacity(cfg.reps);
    let mut first: Option<RunOnce> = None;
    for rep in 0..cfg.reps {
        let id = format!("{base_id}-r{rep}");
        let t0 = Instant::now();
        let outcome = run_streams_once(client, db, tables, query, params, &id, cfg.dist).await?;
        wall_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
        if first.is_none() {
            first = Some(outcome);
        }
    }
    wall_ms.sort_by(|a, b| a.partial_cmp(b).expect("wall-clock ms are finite"));
    let first = first.expect("reps > 0");

    if cfg.dist {
        flush_logs_before_shard_read(client, cfg.cluster).await?;
    } else {
        flush_logs(client).await?;
    }
    let terminal_sql = first.terminal().sql.clone();
    let terminal_query_id = first.terminal().query_id.clone();
    let explain_indexes = explain_lines(
        client,
        "indexes = 1",
        &terminal_sql,
        cfg.dist,
        &format!("{terminal_query_id}-explain-indexes"),
    )
    .await?;
    let stage_evidence = if cfg.dist {
        capture_all_stage_evidence(
            client,
            cfg.cluster,
            cfg.topology
                .expect("--dist implies a loaded ClusterTopology"),
            &first.stages,
        )
        .await?
    } else {
        Vec::new()
    };
    let total = total_marks(client, db, &tables.samples, cfg.dist.then_some(cfg.cluster)).await?;

    Ok(QueryEvidence {
        name: name.to_string(),
        sql: terminal_sql,
        wall_ms_p50: percentile(&wall_ms, 0.50),
        wall_ms_p95: percentile(&wall_ms, 0.95),
        wall_ms_p99: percentile(&wall_ms, 0.99),
        returned_rows: first.returned_rows,
        read_rows: first.totals.read_rows,
        read_bytes: first.totals.read_bytes,
        selected_marks: first.totals.selected_marks,
        total_marks: total,
        memory_usage: first.totals.memory_usage,
        query_duration_ms: first.totals.query_duration_ms,
        explain_indexes,
        stage_evidence,
    })
}

/// Runs all four canonical shapes (the three issue query shapes plus the
/// §9-mandated discovery shape) against `dataset`'s canonical stream.
pub async fn run_all(
    client: &ChClient,
    db: &str,
    dataset: &DatasetSummary,
    reps: usize,
    dist: bool,
    cluster: &str,
) -> anyhow::Result<Vec<QueryEvidence>> {
    let tables = Tables::new(dist);
    let topology = if dist {
        Some(load_cluster_topology(client, cluster).await?)
    } else {
        None
    };
    let cfg = RunConfig {
        reps,
        dist,
        cluster,
        topology: topology.as_ref(),
    };
    let mut out = Vec::new();

    // 1. Label-scoped stream read, 6h, limit 100.
    let window6h = QueryParams {
        spec: QuerySpec::Range {
            start_ns: (dataset.end_ns - 6 * 3_600_000_000_000).max(dataset.start_ns),
            end_ns: dataset.end_ns,
            step_ns: 60_000_000_000,
        },
        limit: 100,
        direction: Direction::Backward,
    };
    out.push(
        run_shape(
            client,
            db,
            &tables,
            "label_scoped_stream_read_6h",
            &format!(
                r#"{{service_name="{}",env="{}"}}"#,
                dataset.canonical_service, dataset.canonical_env
            ),
            &window6h,
            cfg,
        )
        .await?,
    );

    // 2. Body substring search, one service, 24h.
    let window24h = QueryParams {
        spec: QuerySpec::Range {
            start_ns: (dataset.end_ns - 24 * 3_600_000_000_000).max(dataset.start_ns),
            end_ns: dataset.end_ns,
            step_ns: 3_600_000_000_000,
        },
        limit: 1_000,
        direction: Direction::Backward,
    };
    out.push(
        run_shape(
            client,
            db,
            &tables,
            "body_search_24h",
            &format!(
                r#"{{service_name="{}"}} |= "{}""#,
                dataset.canonical_service,
                super::dataset::NEEDLE
            ),
            &window24h,
            cfg,
        )
        .await?,
    );

    // 3. Label/series discovery, 7d (§9-mandated add) — a distinct code
    // path (no stage 2/3, no fingerprint list), captured directly.
    out.push(run_discovery_shape(client, db, &tables, dataset, cfg).await?);

    // 4. Count/rate over the corpus window (rollup-served; nearest analog
    // to a §9 row — none names this shape exactly, recorded regardless).
    out.push(run_metric_shape(client, db, &tables, dataset, cfg).await?);

    Ok(out)
}

async fn run_discovery_shape(
    client: &ChClient,
    db: &str,
    tables: &Tables,
    dataset: &DatasetSummary,
    cfg: RunConfig<'_>,
) -> anyhow::Result<QueryEvidence> {
    let months = month_literals(dataset.start_ns, dataset.end_ns);
    let sql = sql::label_names(&tables.streams_idx, &months);
    let base_id = format!("bench-label_series_discovery_7d-{}", std::process::id());

    execute_discard::<LabelNameRow>(client, &sql, &format!("{base_id}-warmup"), cfg.dist).await?;

    let mut wall_ms = Vec::with_capacity(cfg.reps);
    let mut returned = 0u64;
    let mut first_id = String::new();
    for rep in 0..cfg.reps {
        let id = format!("{base_id}-r{rep}");
        let t0 = Instant::now();
        returned = execute_discard::<LabelNameRow>(client, &sql, &id, cfg.dist).await?;
        wall_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
        if first_id.is_empty() {
            first_id = id;
        }
    }
    wall_ms.sort_by(|a, b| a.partial_cmp(b).expect("finite"));

    if cfg.dist {
        flush_logs_before_shard_read(client, cfg.cluster).await?;
    } else {
        flush_logs(client).await?;
    }
    let totals = read_query_log(client, &first_id).await?;
    let explain_indexes = explain_lines(
        client,
        "indexes = 1",
        &sql,
        cfg.dist,
        &format!("{first_id}-explain-indexes"),
    )
    .await?;
    let total = total_marks(
        client,
        db,
        &tables.streams_idx,
        cfg.dist.then_some(cfg.cluster),
    )
    .await?;
    let stage_evidence = if cfg.dist {
        let stage = StageRef {
            stage: "discovery",
            query_id: first_id,
            sql: sql.clone(),
            roster: StageRoster::Full,
        };
        vec![
            capture_stage_evidence(
                client,
                cfg.cluster,
                cfg.topology
                    .expect("--dist implies a loaded ClusterTopology"),
                &stage,
            )
            .await?,
        ]
    } else {
        Vec::new()
    };

    Ok(QueryEvidence {
        name: "label_series_discovery_7d".to_string(),
        sql,
        wall_ms_p50: percentile(&wall_ms, 0.50),
        wall_ms_p95: percentile(&wall_ms, 0.95),
        wall_ms_p99: percentile(&wall_ms, 0.99),
        returned_rows: returned,
        read_rows: totals.read_rows,
        read_bytes: totals.read_bytes,
        selected_marks: totals.selected_marks,
        total_marks: total,
        memory_usage: totals.memory_usage,
        query_duration_ms: totals.query_duration_ms,
        explain_indexes,
        stage_evidence,
    })
}

/// Runs `sql` tagged `query_id` and discards every row (the harness only
/// needs the returned-row count, not the payload). `dist` applies the
/// clustered-reader settings block (`reader_settings`) — issue #16 CODE
/// review [medium] finding: this is the terminal stage for both the
/// discovery shape and the metric shape, so running it under plain
/// settings in `--dist` mode would capture evidence for a query the
/// product's own distributed reader never actually issues.
async fn execute_discard<R: ChRow>(
    client: &ChClient,
    sql: &str,
    query_id: &str,
    dist: bool,
) -> anyhow::Result<u64> {
    let settings = reader_settings(dist, query_id);
    let mut stream = client.query_stream::<R>(sql, &settings).await?;
    let mut n = 0u64;
    while let Some(row) = stream.next().await {
        row?;
        n += 1;
    }
    Ok(n)
}

async fn run_metric_shape(
    client: &ChClient,
    db: &str,
    tables: &Tables,
    dataset: &DatasetSummary,
    cfg: RunConfig<'_>,
) -> anyhow::Result<QueryEvidence> {
    let query = format!(
        r#"sum by(service_name)(count_over_time({{env="{}"}}[5m]))"#,
        dataset.canonical_env
    );
    let params = QueryParams {
        spec: QuerySpec::Range {
            start_ns: dataset.start_ns,
            end_ns: dataset.end_ns,
            step_ns: 300_000_000_000,
        },
        limit: 1_000,
        direction: Direction::Backward,
    };
    let base_id = format!("bench-count_rate_rollup-{}", std::process::id());

    async fn run_once(
        client: &ChClient,
        tables: &Tables,
        db: &str,
        query: &str,
        params: &QueryParams,
        query_id: &str,
        dist: bool,
    ) -> anyhow::Result<RunOnce> {
        let expr = pulsus_logql::parse(query)?;
        let mp = match plan(&expr, params, &tables.plan_ctx(db))? {
            Plan::Metric(mp) => mp,
            Plan::Streams(_) => anyhow::bail!("expected a Metric plan for {query}"),
        };

        let s1_id = format!("{query_id}-s1");
        let fingerprints = resolve_fingerprints(client, &mp.stage1_sql, &s1_id, dist).await?;
        let mut stages = vec![StageRef {
            stage: "resolution",
            query_id: s1_id.clone(),
            sql: mp.stage1_sql.clone(),
            roster: StageRoster::Full,
        }];
        if fingerprints.is_empty() {
            let totals = sum_query_log(client, std::slice::from_ref(&s1_id)).await?;
            return Ok(RunOnce {
                returned_rows: 0,
                totals,
                stages,
            });
        }

        let s2_id = format!("{query_id}-s2");
        let stage2_sql = sql::stage2(&mp.streams_table, &fingerprints);
        let meta = hydrate(client, &stage2_sql, &s2_id, dist).await?;
        stages.push(StageRef {
            stage: "hydration",
            query_id: s2_id.clone(),
            sql: stage2_sql,
            roster: StageRoster::Fingerprints(fingerprints.clone()),
        });

        let services: Vec<String> = if mp.rollup {
            Vec::new()
        } else {
            let mut s: Vec<&str> = meta.iter().map(|m| m.service.as_str()).collect();
            s.sort_unstable();
            s.dedup();
            s.into_iter()
                .map(pulsus_read::logql::escape::ch_string)
                .collect()
        };
        let source = sql::MetricSource {
            table: &mp.table,
            bucket_col: mp.bucket_col,
            agg_expr: mp.agg_expr,
        };
        let s3_id = format!("{query_id}-s3");
        let sql3 = sql::metric_range(
            source,
            &services,
            &fingerprints,
            TimeWindow {
                start_ns: mp.start_ns,
                end_ns: mp.end_ns,
            },
            mp.step_ns.unwrap_or(300_000_000_000),
            &mp.extra_predicates,
        );
        let returned = execute_discard::<MetricRow>(client, &sql3, &s3_id, dist).await?;
        stages.push(StageRef {
            stage: "rollup_range",
            query_id: s3_id.clone(),
            sql: sql3,
            roster: StageRoster::Fingerprints(fingerprints.clone()),
        });
        let totals = sum_query_log(client, &[s1_id, s2_id, s3_id]).await?;
        Ok(RunOnce {
            returned_rows: returned,
            totals,
            stages,
        })
    }

    run_once(
        client,
        tables,
        db,
        &query,
        &params,
        &format!("{base_id}-warmup"),
        cfg.dist,
    )
    .await?;

    let mut wall_ms = Vec::with_capacity(cfg.reps);
    let mut first: Option<RunOnce> = None;
    for rep in 0..cfg.reps {
        let id = format!("{base_id}-r{rep}");
        let t0 = Instant::now();
        let outcome = run_once(client, tables, db, &query, &params, &id, cfg.dist).await?;
        wall_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
        if first.is_none() {
            first = Some(outcome);
        }
    }
    wall_ms.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
    let first = first.expect("reps > 0");

    if cfg.dist {
        flush_logs_before_shard_read(client, cfg.cluster).await?;
    } else {
        flush_logs(client).await?;
    }
    let terminal_sql = first.terminal().sql.clone();
    let terminal_query_id = first.terminal().query_id.clone();
    let explain_indexes = explain_lines(
        client,
        "indexes = 1",
        &terminal_sql,
        cfg.dist,
        &format!("{terminal_query_id}-explain-indexes"),
    )
    .await?;
    let stage_evidence = if cfg.dist {
        capture_all_stage_evidence(
            client,
            cfg.cluster,
            cfg.topology
                .expect("--dist implies a loaded ClusterTopology"),
            &first.stages,
        )
        .await?
    } else {
        Vec::new()
    };
    // Which physical table the terminal stage read from depends on
    // rollup-vs-raw routing, decided fresh here since `RunOnce` doesn't
    // carry it — cheap (pure, no I/O) and avoids widening `RunOnce` for a
    // single caller's need.
    let expr = pulsus_logql::parse(&query)?;
    let mp = match plan(&expr, &params, &tables.plan_ctx(db))? {
        Plan::Metric(mp) => mp,
        Plan::Streams(_) => anyhow::bail!("expected a Metric plan for {query}"),
    };
    let target_table = if mp.rollup {
        &tables.rollup
    } else {
        &tables.samples
    };
    let total = total_marks(client, db, target_table, cfg.dist.then_some(cfg.cluster)).await?;

    Ok(QueryEvidence {
        name: "count_rate_rollup_over_corpus_window".to_string(),
        sql: terminal_sql,
        wall_ms_p50: percentile(&wall_ms, 0.50),
        wall_ms_p95: percentile(&wall_ms, 0.95),
        wall_ms_p99: percentile(&wall_ms, 0.99),
        returned_rows: first.returned_rows,
        read_rows: first.totals.read_rows,
        read_bytes: first.totals.read_bytes,
        selected_marks: first.totals.selected_marks,
        total_marks: total,
        memory_usage: first.totals.memory_usage,
        query_duration_ms: first.totals.query_duration_ms,
        explain_indexes,
        stage_evidence,
    })
}

/// Month literals (`'YYYY-MM-01'`) overlapping `[start_ns, end_ns]` — a
/// small local reimplementation of `pulsus_read::logql::plan::
/// months_overlapping` (`pub(crate)` there, not reachable from this
/// crate): same Howard Hinnant civil-calendar algorithm `pulsus_model::
/// Date` is built on.
pub(crate) fn month_literals(start_ns: i64, end_ns: i64) -> Vec<String> {
    const NANOS_PER_DAY: i64 = 86_400_000_000_000;
    let day_of = |ns: i64| ns.div_euclid(NANOS_PER_DAY);
    let ymd = |days: i64| {
        let z = days + 719_468;
        let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
        let doe = z - era * 146_097;
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let y = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        (if m <= 2 { y + 1 } else { y }, m)
    };
    let (mut y, mut m) = ymd(day_of(start_ns));
    let (end_y, end_m) = ymd(day_of(end_ns.max(start_ns)));
    let mut out = Vec::new();
    loop {
        out.push(format!("'{y:04}-{m:02}-01'"));
        if (y, m) == (end_y, end_m) {
            break;
        }
        if m == 12 {
            y += 1;
            m = 1;
        } else {
            m += 1;
        }
    }
    out
}
