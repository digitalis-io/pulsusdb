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
//! `--dist` mode reads through the `_dist` tables with the settings every
//! product logs read sends ([`reader_settings`]), and reads every shard's own
//! `system.query_log` via `clusterAllReplicas` (a node's `system.query_log`
//! is local; there is no automatic cluster-wide aggregation) correlated by
//! `initial_query_id`.
//!
//! **Every logs stage reaches every shard** (issue #603). A logs row sits on
//! the shard that took the push, not the one the sharding key names, so the
//! product's reads prune no shard and neither does this harness. A shard
//! counts as reached when it logged exactly one `QueryFinish` row for the
//! stage's `initial_query_id`; the work it did is reported, not used to
//! decide this, because a reached shard with no matching block reads 0
//! marks.
//!
//! **Totals.** The initiator's `read_rows` is the cluster total (measured on
//! 26.8: 200,002 = 100,001 + 100,001), while its `selected_marks` is its own
//! (13). So a stage's top-level totals and its `coordinator` row's
//! `read_rows` count every shard's rows; the `remote` rows are each one
//! shard's.

use std::time::Instant;

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChRow, QuerySettings, Row};
use pulsus_read::logql::predicate::{CheckedLiteral, MonthLiteral, month_literal};
use pulsus_read::logql::sql::{self, TimeWindow};
use pulsus_read::logql::{Direction, Plan, PlanCtx, QueryParams, QuerySpec, plan};

use super::dataset::DatasetSummary;
use pulsus_model::{Fingerprint, FpLiteral};

/// Mints raw identities read back from the database into the literal form
/// the product's builders take (issue #498).
fn sql_literals(fps: &[u128]) -> Vec<FpLiteral> {
    fps.iter()
        .map(|v| Fingerprint::from_raw(*v).sql_literal())
        .collect()
}

use super::query_log::{
    QueryLogTotals, flush_logs, flush_logs_before_shard_read, read_query_log, tagged_settings,
};

/// One shard's evidence for a `--dist` run (empty in single-node mode): one
/// entry per shard per stage, from that shard's own `system.query_log` row.
/// `role` is `"coordinator"` (the initiator's `is_initial_query = 1` row,
/// whose `read_rows` is the cluster total — see the module doc comment) or
/// `"remote"`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ShardEvidence {
    pub shard_num: u32,
    pub hostname: String,
    pub role: String,
    pub read_rows: u64,
    pub read_bytes: u64,
    pub selected_marks: u64,
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
    /// The raw 128-bit identity, decoded as the integer rather than as a
    /// `Fingerprint` (issue #498): this scenario models the `Distributed`
    /// sharding expression, which is `cityHash64(fingerprint) %
    /// total_weight` over the stored value's bytes. The seal is about
    /// rendering, and every statement this file builds still goes through
    /// the mint below.
    fingerprint: u128,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct StreamMetaRow {
    fingerprint: u128,
    service: String,
    #[allow(dead_code)]
    labels: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct SampleRow {
    #[allow(dead_code)]
    fingerprint: u128,
    #[allow(dead_code)]
    timestamp_ns: i64,
    #[allow(dead_code)]
    body: String,
    // Must mirror `pulsus_read::logql::sql::stage3`'s SELECT (fingerprint,
    // timestamp_ns, body, structured_metadata) — the column added by #97.
    #[allow(dead_code)]
    structured_metadata: String,
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct MetricRow {
    #[allow(dead_code)]
    fingerprint: u128,
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
/// resolved to a `shard_num`.
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
/// `is_initial_query = 1` row and every remote shard's
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

/// The cluster's shard roster (`system.clusters`) and every replica
/// hostname's shard (`system.macros`' `{shard}` value).
#[derive(Debug, Clone)]
pub(crate) struct ClusterTopology {
    /// Authoritative shard_num set, from `system.clusters` alone.
    shards: std::collections::BTreeSet<u32>,
    hostname_to_shard: std::collections::HashMap<String, u32>,
}

impl ClusterTopology {
    pub(crate) fn all_shards(&self) -> std::collections::BTreeSet<u32> {
        self.shards.clone()
    }
}

/// Maps every hostname to its `{shard}` macro value, checked against
/// `authoritative_shards` (the shard_num set read from `system.clusters`):
/// a value outside that set fails, and so does a shard no host reports.
/// Several hosts may report one shard — they are its replicas. Pure (no
/// I/O), so the cases are unit-testable without a live cluster.
fn build_shard_maps(
    cluster: &str,
    authoritative_shards: &std::collections::BTreeSet<u32>,
    macro_rows: &[(String, u32)],
) -> anyhow::Result<std::collections::HashMap<String, u32>> {
    let mut hostname_to_shard = std::collections::HashMap::new();
    for (hostname, shard_num) in macro_rows {
        anyhow::ensure!(
            authoritative_shards.contains(shard_num),
            "cluster {cluster:?}: node {hostname:?} reports {{shard}} = {shard_num}, which is \
             not one of system.clusters' shard_nums {authoritative_shards:?} — out-of-roster \
             {{shard}} macro value"
        );
        hostname_to_shard.insert(hostname.clone(), *shard_num);
    }
    let observed_shards: std::collections::BTreeSet<u32> =
        hostname_to_shard.values().copied().collect();
    anyhow::ensure!(
        &observed_shards == authoritative_shards,
        "cluster {cluster:?}: the {{shard}} macro map covers {observed_shards:?}, missing {:?} \
         from system.clusters' shard_nums {authoritative_shards:?} — every shard must report its \
         own {{shard}} macro",
        authoritative_shards
            .difference(&observed_shards)
            .collect::<Vec<_>>()
    );
    Ok(hostname_to_shard)
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
    }
    let sql = format!(
        "SELECT DISTINCT shard_num FROM system.clusters WHERE cluster = '{cluster}' \
         ORDER BY shard_num"
    );
    let mut stream = client
        .query_stream::<ClusterRow>(&sql, &QuerySettings::new())
        .await?;
    let mut authoritative_shards = std::collections::BTreeSet::new();
    while let Some(row) = stream.next().await {
        authoritative_shards.insert(row?.shard_num);
    }
    anyhow::ensure!(
        !authoritative_shards.is_empty(),
        "cluster {cluster:?} has no shards in system.clusters — check --cluster"
    );

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

    let hostname_to_shard = build_shard_maps(cluster, &authoritative_shards, &macro_rows)?;
    Ok(ClusterTopology {
        shards: authoritative_shards,
        hostname_to_shard,
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
        let hostname_to_shard =
            build_shard_maps("c", &authoritative, &macro_rows).expect("bijection is valid");
        assert_eq!(hostname_to_shard.len(), 4);
        assert_eq!(hostname_to_shard.get("c"), Some(&3));
    }

    #[test]
    fn build_shard_maps_rejects_a_missing_shard() {
        let authoritative = shards(&[1, 2, 3, 4]);
        // shard 4 never reports its {shard} macro.
        let macro_rows = rows(&[("a", 1), ("b", 2), ("c", 3)]);
        let err = build_shard_maps("c", &authoritative, &macro_rows).unwrap_err();
        assert!(err.to_string().contains("missing"), "{err}");
    }

    /// X3: two replicas of one shard both map to it.
    #[test]
    fn build_shard_maps_accepts_two_replicas_of_one_shard() {
        let authoritative = shards(&[1, 2]);
        let macro_rows = rows(&[("a", 1), ("b", 1), ("c", 2)]);
        let got = build_shard_maps("c", &authoritative, &macro_rows);
        assert!(got.is_ok(), "{got:?}");
        let hostname_to_shard = got.expect("checked above");
        let want: std::collections::HashMap<String, u32> = [("a", 1), ("b", 1), ("c", 2)]
            .iter()
            .map(|(h, s)| (h.to_string(), *s))
            .collect();
        assert_eq!(hostname_to_shard, want);
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
/// run under the same [`reader_settings`] the stage's read used, so the
/// pipeline captured is the one that read ran.
async fn explain_lines(
    client: &ChClient,
    kind: &str,
    sql: &str,
    dist: bool,
    query_id: &str,
    product: ProductMode,
) -> anyhow::Result<Vec<String>> {
    if sql.is_empty() {
        return Ok(Vec::new());
    }
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct ExplainRow {
        explain: String,
    }
    let full = format!("EXPLAIN {kind} {sql}").replace('?', "??");
    let settings = reader_settings(dist, query_id, product);
    let mut stream = client.query_stream::<ExplainRow>(&full, &settings).await?;
    let mut out = Vec::new();
    while let Some(row) = stream.next().await {
        out.push(row?.explain);
    }
    Ok(out)
}

/// One executed stage of a shape's read: its logical name
/// (`resolution`/`hydration`/`samples`/`rollup_range`/`discovery`), the
/// unique `query_id` its `system.query_log`/`EXPLAIN` rows are tagged
/// with, and its SQL text.
#[derive(Debug, Clone)]
struct StageRef {
    stage: &'static str,
    query_id: String,
    sql: String,
    /// Whether this stage's SQL nests a distributed table inside an `IN
    /// (subquery)` — decides whether its `EXPLAIN PIPELINE` re-issue needs
    /// `distributed_product_mode='local'`. **No stage does since issue
    /// #603**: discovery was the one that did, and it is two flat statements
    /// now.
    product: ProductMode,
}

/// Every shard in `all_shards` logged exactly one `QueryFinish` row for the
/// stage, and exactly one row is the initiator's.
fn participation_verdict(
    stage: &str,
    query_id: &str,
    all_shards: &std::collections::BTreeSet<u32>,
    rows: &[(u32, RawShardRow)],
) -> anyhow::Result<()> {
    let mut per_shard: std::collections::BTreeMap<u32, usize> = std::collections::BTreeMap::new();
    for (shard, _) in rows {
        *per_shard.entry(*shard).or_default() += 1;
    }
    let missing: Vec<u32> = all_shards
        .iter()
        .filter(|shard| !per_shard.contains_key(shard))
        .copied()
        .collect();
    anyhow::ensure!(
        missing.is_empty(),
        "stage {stage:?} (query_id={query_id}): shards {missing:?} logged no QueryFinish row — \
         the read did not reach them (shard pruning on, or a shard skipped) or the row was lost"
    );
    let dup: Vec<u32> = per_shard
        .iter()
        .filter(|(_, n)| **n > 1)
        .map(|(shard, _)| *shard)
        .collect();
    anyhow::ensure!(
        dup.is_empty(),
        "stage {stage:?} (query_id={query_id}): shards {dup:?} logged more than one QueryFinish row"
    );
    let n = rows
        .iter()
        .filter(|(_, row)| row.is_initial_query == 1)
        .count();
    anyhow::ensure!(
        n == 1,
        "stage {stage:?} (query_id={query_id}): {n} initiator rows, expected exactly 1"
    );
    Ok(())
}

/// Captures one stage's `--dist` evidence: every shard's own
/// `system.query_log` row ([`read_stage_query_log_rows`]), resolved to its
/// `shard_num` through `topology.hostname_to_shard`, checked by
/// [`participation_verdict`], plus a nonempty `EXPLAIN PIPELINE`. Emits one
/// [`ShardEvidence`] per row.
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
        stage.product,
    )
    .await?;
    anyhow::ensure!(
        !explain_pipeline.is_empty(),
        "stage {:?} (query_id={}) yielded an empty EXPLAIN PIPELINE in --dist mode; SQL:\n{}",
        stage.stage,
        stage.query_id,
        stage.sql
    );

    let mut by_shard: Vec<(u32, RawShardRow)> = Vec::with_capacity(raw_rows.len());
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
        by_shard.push((shard_num, row));
    }
    by_shard.sort_by_key(|(shard, _)| *shard);
    participation_verdict(
        stage.stage,
        &stage.query_id,
        &topology.all_shards(),
        &by_shard,
    )
    .map_err(|e| anyhow::anyhow!("{e}; SQL:\n{}", stage.sql))?;

    let shards = by_shard
        .into_iter()
        .map(|(shard_num, row)| ShardEvidence {
            shard_num,
            role: if row.is_initial_query == 1 {
                "coordinator"
            } else {
                "remote"
            }
            .to_string(),
            hostname: row.hostname,
            read_rows: row.read_rows,
            read_bytes: row.read_bytes,
            selected_marks: row.selected_marks,
        })
        .collect();

    Ok(StageEvidence {
        stage: stage.stage.to_string(),
        shards,
        explain_pipeline,
    })
}

/// Captures `--dist` evidence for every stage in `stages`, in order —
/// shared by every shape runner ([`run_shape`], `run_metric_shape`,
/// `run_discovery_shape`) so the participation check lives in one place.
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
            tenant: Box::leak(Box::new(
                pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            )),
            db,
            streams_idx: &self.streams_idx,
            streams: &self.streams,
            samples: &self.samples,
            rollup_table: &self.rollup,
            rollup_res_ns: 5_000_000_000,
            scan_budget_bytes: 200 * 1024 * 1024 * 1024,
            max_streams: 1_000_000,
            pipeline_scan_factor: 10,
        }
    }
}

/// `SETTINGS log_comment` run-tag every query in this scenario carries
/// (issue #35 architect plan) — a fixed constant rather than a
/// threaded-through parameter, since every [`reader_settings`] call site in
/// this file is already scoped to the single `logs-read` scenario (`bench
/// logs-hydration` tags its own settings via its own module's calls into
/// [`super::query_log::tagged_settings`]). Purely
/// additive: `log_comment` is an HTTP request setting, never inlined SQL,
/// and does not touch `read_rows`/`read_bytes`/`SelectedMarks`, so
/// `query_log_gates.rs` and the committed `logs-read-ci.json`/
/// `logs-read-dist.json` numbers are unchanged — it only makes this
/// scenario's `system.query_log` rows independently greppable/correlatable
/// by `log_comment` alongside `query_id`.
const LOG_COMMENT: &str = "pulsus-bench:logs-read";

/// Whether a bench query nests a distributed table inside an `IN
/// (subquery)`. **Since issue #603 no bench query does.** The `/labels`
/// discovery shape was the one that did — issue #399's activity semi-join —
/// and it is now two statements, the activity scan and an index scan carrying
/// its result as a literal list, exactly as production issues them. So every
/// shape is `Flat` and none carries `distributed_product_mode`.
///
/// The variant is kept rather than the enum collapsed, because the setting a
/// `Nested` shape would need is a decision this harness should still be able
/// to express if a bench query ever nests one again.
#[derive(Debug, Clone, Copy)]
enum ProductMode {
    Flat,
    #[allow(dead_code)]
    Nested,
}

/// What every product logs read sends, at the product's defaults.
fn bench_base_settings() -> QuerySettings {
    let reader = pulsus_config::Config::default().reader;
    pulsus_read::logql::read_query_settings(
        reader.logql_scan_budget_bytes.0,
        reader.logql_read_max_memory_bytes,
        false,
    )
}

/// Settings applied to every timed query, in both modes:
/// [`bench_base_settings`], so the bench measures the shipped route — shard
/// pruning off, and a lost shard fails the run rather than being skipped —
/// plus the per-query `query_id` tag every stage carries and the
/// scenario-wide [`LOG_COMMENT`] tag (routed through the shared
/// [`super::query_log::tagged_settings`]). A `Nested` shape adds
/// `distributed_product_mode = 'local'` under `--dist`.
fn reader_settings(dist: bool, query_id: &str, product: ProductMode) -> QuerySettings {
    let base = match product {
        ProductMode::Nested if dist => {
            bench_base_settings().set("distributed_product_mode", "local")
        }
        _ => bench_base_settings(),
    };
    tagged_settings(base, query_id, Some(LOG_COMMENT))
}

async fn resolve_fingerprints(
    client: &ChClient,
    stage1_sql: &str,
    query_id: &str,
    dist: bool,
) -> anyhow::Result<Vec<u128>> {
    let settings = reader_settings(dist, query_id, ProductMode::Flat);
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
    let settings = reader_settings(dist, query_id, ProductMode::Flat);
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
        Plan::Metric(_) | Plan::MetricBinary(_) => {
            anyhow::bail!("expected a Streams plan for {query}")
        }
    };

    let s1_id = format!("{base_id}-s1");
    let fingerprints = resolve_fingerprints(client, &sp.stage1_sql, &s1_id, dist).await?;
    let mut stages = vec![StageRef {
        stage: "resolution",
        query_id: s1_id.clone(),
        sql: sp.stage1_sql.clone(),
        product: ProductMode::Flat,
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
    let stage2_sql = sql::stage2(
        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        &tables.streams,
        &sql_literals(&fingerprints),
    );
    let meta = hydrate(client, &stage2_sql, &s2_id, dist).await?;
    stages.push(StageRef {
        stage: "hydration",
        query_id: s2_id.clone(),
        sql: stage2_sql,
        product: ProductMode::Flat,
    });

    let mut services: Vec<&str> = meta.iter().map(|m| m.service.as_str()).collect();
    services.sort_unstable();
    services.dedup();
    let escaped: Vec<CheckedLiteral> = services
        .into_iter()
        .map(pulsus_read::logql::predicate::literal)
        .collect();

    let s3_id = format!("{base_id}-s3");
    let sql3 = sql::stage3(
        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        &tables.samples,
        &escaped,
        &sql_literals(&fingerprints),
        TimeWindow {
            start_ns: sp.start_ns,
            end_ns: sp.end_ns,
        },
        &sp.line_filters,
        sp.direction,
        sp.scan_limit,
    );
    let settings3 = reader_settings(dist, &s3_id, ProductMode::Flat);
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
        product: ProductMode::Flat,
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
        ProductMode::Flat,
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

    // 2b. The same search as a whole word, read from the body's word index
    // (issue #757). `NEEDLE` is a whole word in every body that carries it.
    out.push(
        run_shape(
            client,
            db,
            &tables,
            "word_search_24h",
            &format!(
                r#"{{service_name="{}"}} |= word("{}")"#,
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
    let base_id = format!("bench-label_series_discovery_7d-{}", std::process::id());

    // Issue #399: `/labels` is window-scoped by the streams active in the
    // request's range, so the bench must issue what production issues — the
    // corpus window is the discovery window here, as it always was.
    //
    // **Issue #603: that is now two statements, not one.** The activity scan
    // runs first and its result is rendered into the index scan as a literal
    // list, so neither statement nests a distributed subquery and the index
    // scan is captured `Flat`. The bench issues the UNSCOPED form — no
    // `query=` — which is the shape this evidence has always measured.
    let activity_sql = sql::active_fingerprints(
        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        &tables.rollup,
        None,
        TimeWindow {
            start_ns: dataset.start_ns,
            end_ns: dataset.end_ns,
        },
        5_000_000_000,
    );
    let active = resolve_fingerprints(
        client,
        &activity_sql,
        &format!("{base_id}-activity"),
        cfg.dist,
    )
    .await?;
    let fps: Vec<pulsus_model::FpLiteral> = active
        .into_iter()
        .map(|fp| pulsus_model::Fingerprint::from_raw(fp).sql_literal())
        .collect();
    let sql = sql::label_names(
        &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
        &tables.streams_idx,
        &months,
        &fps,
    );

    execute_discard::<LabelNameRow>(
        client,
        &sql,
        &format!("{base_id}-warmup"),
        cfg.dist,
        ProductMode::Flat,
    )
    .await?;

    let mut wall_ms = Vec::with_capacity(cfg.reps);
    let mut returned = 0u64;
    let mut first_id = String::new();
    for rep in 0..cfg.reps {
        let id = format!("{base_id}-r{rep}");
        let t0 = Instant::now();
        returned =
            execute_discard::<LabelNameRow>(client, &sql, &id, cfg.dist, ProductMode::Flat).await?;
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
        ProductMode::Flat,
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
            product: ProductMode::Flat,
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
/// needs the returned-row count, not the payload), under the same
/// [`reader_settings`] as every other stage, so the terminal stage of the
/// discovery and metric shapes is the read the product issues.
async fn execute_discard<R: ChRow>(
    client: &ChClient,
    sql: &str,
    query_id: &str,
    dist: bool,
    product: ProductMode,
) -> anyhow::Result<u64> {
    let settings = reader_settings(dist, query_id, product);
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
            Plan::Streams(_) | Plan::MetricBinary(_) => {
                anyhow::bail!("expected a Metric plan for {query}")
            }
        };

        let s1_id = format!("{query_id}-s1");
        let fingerprints = resolve_fingerprints(client, &mp.stage1_sql, &s1_id, dist).await?;
        let mut stages = vec![StageRef {
            stage: "resolution",
            query_id: s1_id.clone(),
            sql: mp.stage1_sql.clone(),
            product: ProductMode::Flat,
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
        let stage2_sql = sql::stage2(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            &mp.streams_table,
            &sql_literals(&fingerprints),
        );
        let meta = hydrate(client, &stage2_sql, &s2_id, dist).await?;
        stages.push(StageRef {
            stage: "hydration",
            query_id: s2_id.clone(),
            sql: stage2_sql,
            product: ProductMode::Flat,
        });

        let services: Vec<CheckedLiteral> = if mp.rollup {
            Vec::new()
        } else {
            let mut s: Vec<&str> = meta.iter().map(|m| m.service.as_str()).collect();
            s.sort_unstable();
            s.dedup();
            s.into_iter()
                .map(pulsus_read::logql::predicate::literal)
                .collect()
        };
        let source = sql::MetricSource::new(
            &mp.table,
            mp.source_shape()
                .expect("plan::metric_plan writes both columns out of MetricShape"),
        );
        let s3_id = format!("{query_id}-s3");
        let sql3 = sql::metric_range(
            &pulsus_model::Tenant::from_header(None, false).expect("the empty tenant"),
            source,
            &services,
            &sql_literals(&fingerprints),
            TimeWindow {
                start_ns: mp.start_ns,
                end_ns: mp.end_ns,
            },
            mp.scan_lower,
            // Issue #227 made `MetricPlan::step_ns` a `ValidatedDuration`.
            // This bench stage still measures the step-bucketed rollup SQL
            // shape directly (the committed evidence JSON is byte-frozen);
            // the LogQL RANGE read path itself no longer emits it — see the
            // note on this scenario.
            mp.step_ns.map(|d| d.as_u64()).unwrap_or(300_000_000_000),
            &mp.extra_predicates,
        );
        let returned =
            execute_discard::<MetricRow>(client, &sql3, &s3_id, dist, ProductMode::Flat).await?;
        stages.push(StageRef {
            stage: "rollup_range",
            query_id: s3_id.clone(),
            sql: sql3,
            product: ProductMode::Flat,
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
        ProductMode::Flat,
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
        Plan::Streams(_) | Plan::MetricBinary(_) => {
            anyhow::bail!("expected a Metric plan for {query}")
        }
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
pub(crate) fn month_literals(start_ns: i64, end_ns: i64) -> Vec<MonthLiteral> {
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
        out.push(month_literal(y, u32::try_from(m).expect("month 1..=12")));
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

#[cfg(test)]
mod participation_tests {
    use super::{ProductMode, RawShardRow, participation_verdict, reader_settings};
    use std::collections::BTreeSet;

    fn row(shard: u32, initial: u8, read_rows: u64, marks: u64) -> (u32, RawShardRow) {
        (
            shard,
            RawShardRow {
                hostname: format!("host-{shard}"),
                is_initial_query: initial,
                read_rows,
                read_bytes: read_rows * 10,
                selected_marks: marks,
            },
        )
    }

    fn shards(nums: &[u32]) -> BTreeSet<u32> {
        nums.iter().copied().collect()
    }

    /// The error's text, or nothing when the verdict passed.
    fn err_text(got: &anyhow::Result<()>) -> String {
        match got {
            Ok(()) => String::new(),
            Err(e) => e.to_string(),
        }
    }

    /// X2a: a shard that was asked and read nothing — no matching block —
    /// counts as reached. This is the CI failure.
    #[test]
    fn a_reached_shard_that_read_nothing_counts_as_reached() {
        let rows = [row(1, 1, 2000, 1), row(2, 0, 0, 0)];
        let got = participation_verdict("resolution", "q", &shards(&[1, 2]), &rows);
        assert!(got.is_ok(), "{got:?}");
    }

    /// X2b: a shard with no row fails, by name.
    #[test]
    fn a_shard_with_no_row_fails_by_name() {
        let rows = [row(1, 1, 2000, 1), row(2, 0, 100, 1)];
        let got = participation_verdict("resolution", "q", &shards(&[1, 2, 3]), &rows);
        assert!(err_text(&got).contains("[3]"), "{got:?}");
    }

    /// X2c: a shard with two rows fails.
    #[test]
    fn a_shard_with_two_rows_fails() {
        let rows = [row(1, 1, 2000, 1), row(2, 0, 100, 1), row(2, 0, 100, 1)];
        let got = participation_verdict("resolution", "q", &shards(&[1, 2]), &rows);
        assert!(err_text(&got).contains("more than one"), "{got:?}");
    }

    /// X2d: zero or two initiator rows fail.
    #[test]
    fn zero_or_two_initiator_rows_fail() {
        for rows in [
            [row(1, 0, 2000, 1), row(2, 0, 100, 1)],
            [row(1, 1, 2000, 1), row(2, 1, 100, 1)],
        ] {
            let got = participation_verdict("resolution", "q", &shards(&[1, 2]), &rows);
            assert!(err_text(&got).contains("initiator rows"), "{got:?}");
        }
    }

    /// X1: the bench's reads send what every product logs read sends, at the
    /// product's defaults, in both modes — entry for entry, apart from the two
    /// tags.
    #[test]
    fn the_bench_reads_send_the_products_settings() {
        let reader = pulsus_config::Config::default().reader;
        let want: Vec<(String, String)> = pulsus_read::logql::read_query_settings(
            reader.logql_scan_budget_bytes.0,
            reader.logql_read_max_memory_bytes,
            false,
        )
        .entries()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        for dist in [false, true] {
            let got: Vec<(String, String)> = reader_settings(dist, "q", ProductMode::Flat)
                .entries()
                .filter(|(k, _)| *k != "query_id" && *k != "log_comment")
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            assert_eq!(got, want, "dist {dist}");
        }
    }
}
