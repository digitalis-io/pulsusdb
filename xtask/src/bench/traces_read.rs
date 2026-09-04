//! `bench traces-read` (issue #57 AC4) — the M4 traces read-path
//! shard-locality evidence harness, on the 2-shard
//! `ci/clickhouse-cluster` fixture (the `schema-it-cluster` topology).
//!
//! Follows the issue #16 evidence model exactly (see
//! `queries.rs`'s module docs): every stage the two-phase TraceQL search
//! executes — plus the trace-by-ID point read — runs the **product
//! planner's own generated SQL** (`pulsus_read::traces`), tagged with a
//! unique `query_id` under the docs/schemas.md §7 clustered-reader
//! settings, and is then verdicted against a **client-computed expected
//! shard roster**: `cityHash64(trace_id) % total_weight` over the
//! cumulative-weight slot map from `system.clusters` (the exact selection
//! the Distributed engine performs — the sharding key here is
//! `cityHash64(trace_id)`, so the client carries its own CityHash64
//! v1.0.2 implementation, cross-checked against the live server for
//! every id it derives a roster from). Per-shard `system.query_log` rows
//! are read via `clusterAllReplicas` after a cluster-wide flush;
//! `is_initial_query = 1` is the coordinator's own local-shard read and
//! `= 0` a remote sub-query row (the #16 semantics — a filter keeping
//! only remote rows would silently miss the coordinator's shard).
//!
//! **Verdicts are hard errors** (the CI gate): a shard in the expected
//! roster doing zero work is a lost `system.query_log` row; a shard
//! outside it doing work is a pruning/sharding violation. Shards
//! correctly pruned by `optimize_skip_unused_shards` are emitted as
//! `expected-pruned` evidence entries carrying the
//! `cityHash64 % total_weight` derivation so a reviewer can verify the
//! pruning by hand.
//!
//! Scenario-local evidence structs only — the shared [`super::query_log::
//! QueryLogTotals`] shape is untouched (the frozen-artifact rule).
//!
//! ```text
//! podman-compose -f ci/clickhouse-cluster/compose.yaml up -d   # or plain podman + static IPs
//! cargo run -p xtask -- bench traces-read \
//!     --http-url http://127.0.0.1:18123 --database pulsus_traces_bench \
//!     --cluster pulsus_test_cluster \
//!     --out docs/benchmarks/data/traces-read-cluster-ci.json
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChConnConfig, ChProto, ChRow, Idempotency, QuerySettings, Row};
use pulsus_read::traces::rows::{
    CandidateRow, HydrationRow, MembershipRow, RootRow, StoredSpanRow, TypedStrValueRow,
};
use pulsus_read::traces::search_plan::{SearchCtx, SearchParams, plan_search};
use pulsus_read::traces::sql::point_read_sql;
use pulsus_read::{SearchPlan, SpanFilterCtx, TraceEngine, TraceReadConfig};
use pulsus_schema::{RenderCtx, run_init};

use super::queries::{ClusterTopology, load_cluster_topology};
use super::query_log::{flush_logs_before_shard_read, tagged_settings};
use super::{BenchArgs, parse_http_url};

/// Corpus size: single-span traces spread over [`WINDOW_NS`], 1-in-50
/// (`2%`) tagged `service = 'checkout'` + `http.status_code = 500` — big
/// enough that both shards hold matches for every full-roster stage,
/// small enough for the CI leg (topology mechanics are scale-invariant,
/// docs/schemas.md §7).
const TRACES: u64 = 4_000;
const CHECKOUT_EVERY: u64 = 50;
const WINDOW_NS: i64 = 2 * 3_600 * 1_000_000_000;

/// ClickHouse's `cityHash64` for a 16-byte `FixedString(16)` value —
/// CityHash v1.0.2's `HashLen0to16` `len > 8` branch (ClickHouse pins
/// v1.0.2; Google later changed the algorithm). Verified against live
/// 24.8 vectors in the unit tests below AND cross-checked against the
/// running server for every id a roster is derived from
/// ([`cross_check_hashes`]) — a drifting implementation fails loudly, it
/// can never silently mis-derive a roster.
fn city_hash_64_16(bytes: &[u8; 16]) -> u64 {
    const K_MUL: u64 = 0x9ddf_ea08_eb38_2d69;
    fn rotate_by_at_least_1(val: u64, shift: u32) -> u64 {
        val.rotate_right(shift)
    }
    fn hash_128_to_64(u: u64, v: u64) -> u64 {
        let mut a = (u ^ v).wrapping_mul(K_MUL);
        a ^= a >> 47;
        let mut b = (v ^ a).wrapping_mul(K_MUL);
        b ^= b >> 47;
        b.wrapping_mul(K_MUL)
    }
    let a = u64::from_le_bytes(bytes[0..8].try_into().expect("8 bytes"));
    let b = u64::from_le_bytes(bytes[8..16].try_into().expect("8 bytes"));
    hash_128_to_64(a, rotate_by_at_least_1(b.wrapping_add(16), 16)) ^ b
}

/// The corpus's trace ids are the big-endian bytes of the trace number
/// (matching the server-side `unhex(leftPad(lower(hex(number)), 32, '0'))`
/// seed expression).
fn trace_id_of(n: u64) -> [u8; 16] {
    (n as u128).to_be_bytes()
}

fn hex32(id: &[u8; 16]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

// ---------------------------------------------------------------------
// Scenario-local evidence structs (QueryLogTotals untouched).
// ---------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize)]
pub struct TraceShardEvidence {
    pub shard_num: u32,
    /// `"coordinator-local"` (`is_initial_query = 1`), `"remote"`
    /// (`is_initial_query = 0`), or `"expected-pruned"` (derivedly
    /// excluded by `optimize_skip_unused_shards`).
    pub role: String,
    pub read_rows: u64,
    pub read_bytes: u64,
    pub selected_marks: u64,
    pub memory_usage: u64,
    pub query_duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pruned_reason: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TraceStageEvidence {
    pub stage: &'static str,
    pub sql: String,
    pub wall_ms: f64,
    pub returned_rows: u64,
    pub expected_shards: Vec<u32>,
    pub shards: Vec<TraceShardEvidence>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TracesReadReport {
    pub cluster: String,
    pub database: String,
    pub traces: u64,
    pub checkout_traces: u64,
    pub total_weight: u64,
    /// End-to-end `TraceEngine::search` over the `_dist` tables
    /// (distributed config): wall time + returned/partial, correctness
    /// asserted before the report is written.
    pub search_wall_ms: f64,
    pub search_returned: u32,
    pub search_partial: bool,
    pub stages: Vec<TraceStageEvidence>,
}

/// Which shard set a stage is expected to touch.
enum Roster {
    /// No `trace_id` predicate — every shard participates (generators).
    Full,
    /// `trace_id`-keyed reads — exactly the shards owning one of these
    /// ids under `cityHash64(trace_id) % total_weight`.
    TraceIds(Vec<[u8; 16]>),
}

struct StageSpec {
    stage: &'static str,
    sql: String,
    roster: Roster,
    /// The row type this stage's SQL is decoded into.
    ///
    /// **It lives on the spec, beside the SQL, on purpose.** RowBinary
    /// decoding is positional, so a projection and its decoder are one
    /// fact; when they were two — the SQL built here and the decoder
    /// chosen by a `match` on `stage` further down — issue #510 widened
    /// `membership_sql`'s fused projection from three columns to four,
    /// moved production's call sites, and left this one behind. The
    /// mismatch is a hard decode error, so it could not be silent, but no
    /// local suite runs this bench and only the clustered CI leg saw it.
    decoder: Decoder,
}

/// Which row struct a stage's projection decodes into.
///
/// [`Self::columns`] is the decoder's own `COLUMN_NAMES`, so
/// `the_evidence_stages_project_exactly_what_they_decode` can compare it
/// to the SQL without a second list to keep in step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Decoder {
    StoredSpan,
    Candidate,
    Hydration,
    /// The membership read with NO fused value — two columns.
    Membership,
    /// The membership read with the matched value AND its stored type
    /// fused in (issues #479, #510) — four columns.
    TypedStrValue,
    Root,
}

impl Decoder {
    #[cfg_attr(not(test), allow(dead_code))]
    fn columns(self) -> &'static [&'static str] {
        match self {
            Decoder::StoredSpan => StoredSpanRow::COLUMN_NAMES,
            Decoder::Candidate => CandidateRow::COLUMN_NAMES,
            Decoder::Hydration => HydrationRow::COLUMN_NAMES,
            Decoder::Membership => MembershipRow::COLUMN_NAMES,
            Decoder::TypedStrValue => TypedStrValueRow::COLUMN_NAMES,
            Decoder::Root => RootRow::COLUMN_NAMES,
        }
    }
}

/// The two REAL plans the evidence stages read their SQL from, built by
/// the product planner against the `_dist` tables.
///
/// Pure, so the hermetic projection check builds the same plans the run
/// does rather than a second set written beside it.
fn evidence_plans(base: i64, now: i64) -> anyhow::Result<(SearchPlan, SearchPlan)> {
    let ctx = SearchCtx {
        filter: SpanFilterCtx {
            spans_table: "trace_spans_dist",
            attrs_table: "trace_attrs_idx_dist",
        },
        max_candidates: 100_000,
        // The `reader.traceql_max_series` default (issue #185): the shared
        // `by()` cardinality cap, matching the production sites.
        max_series: 1_000,
        distributed: true,
    };
    let params = SearchParams {
        start_ns: base,
        end_ns: now,
        limit: 20,
        spss: 3,
    };
    let service_query = pulsus_traceql_parse(
        r#"{ resource.service.name = "checkout" && span.http.status_code >= 500 }"#,
    )?;
    let plan: SearchPlan = plan_search(&service_query, &params, &ctx)
        .map_err(|e| anyhow::anyhow!("plan_search failed: {e}"))?;
    anyhow::ensure!(
        plan.generator_sqls.len() == 1 && plan.probes_len() == 1,
        "the evidence query must plan one generator + one membership probe"
    );
    let attr_query = pulsus_traceql_parse("{ span.http.status_code >= 500 }")?;
    let attr_plan = plan_search(&attr_query, &params, &ctx)
        .map_err(|e| anyhow::anyhow!("plan_search failed: {e}"))?;
    Ok((plan, attr_plan))
}

/// The six evidence stages: each one's SQL and the row type it decodes
/// into, from the SAME record.
///
/// Pure — no client, no cluster — so
/// `the_evidence_stages_project_exactly_what_they_decode` can build the
/// real list and check every projection against its real decoder without
/// a two-shard ClickHouse.
fn evidence_stages(
    plan: &SearchPlan,
    attr_plan: &SearchPlan,
    batch: &[[u8; 16]],
    target: [u8; 16],
) -> Vec<StageSpec> {
    vec![
        StageSpec {
            stage: "trace_by_id",
            sql: point_read_sql("trace_spans_dist", &hex32(&target)),
            roster: Roster::TraceIds(vec![target]),
            decoder: Decoder::StoredSpan,
        },
        StageSpec {
            stage: "phase1_generator_service",
            sql: plan.generator_sqls[0].clone(),
            roster: Roster::Full,
            decoder: Decoder::Candidate,
        },
        StageSpec {
            stage: "phase1_generator_attr",
            sql: attr_plan.generator_sqls[0].clone(),
            roster: Roster::Full,
            decoder: Decoder::Candidate,
        },
        StageSpec {
            stage: "phase2_hydration",
            sql: plan.hydration_sql_for(batch),
            roster: Roster::TraceIds(batch.to_vec()),
            decoder: Decoder::Hydration,
        },
        StageSpec {
            stage: "phase2_membership",
            sql: plan.membership_sql_for(0, batch),
            roster: Roster::TraceIds(batch.to_vec()),
            // The membership read has TWO shapes: when a projection needs
            // the probe's matched value, `membership_sql` fuses the
            // byte-capped `val AS v` (issue #479) AND its `val_type AS t`
            // (issue #510) into the SAME statement and the row is FOUR
            // columns; otherwise it is two. Production dispatches on
            // exactly this accessor
            // (`traces/exec.rs::load_batch_attrs`, which decodes the fused
            // arm as `TypedStrValueRow`), and this stage measures the read
            // production issues — so it mirrors that dispatch rather than
            // forcing the projection off, which would measure a statement
            // no query ever sends. The evidence query's probe is
            // `span.http.status_code >= 500`, a range predicate, so this
            // run takes the fused arm.
            decoder: if plan.probe_fuses_value(0) {
                Decoder::TypedStrValue
            } else {
                Decoder::Membership
            },
        },
        StageSpec {
            stage: "root_hydration",
            sql: plan.root_sql_for(&[target]),
            roster: Roster::TraceIds(vec![target]),
            decoder: Decoder::Root,
        },
    ]
}

fn reader_settings(query_id: &str) -> QuerySettings {
    tagged_settings(QuerySettings::clustered_reader(false), query_id, None)
}

/// Drains one stage's SELECT to completion under the tagged clustered
/// settings, returning the row count (the driver's literal-`?` doubling
/// applied, mirroring the engine's own execution boundary).
async fn drain<R: ChRow>(
    client: &ChClient,
    sql: &str,
    settings: &QuerySettings,
) -> anyhow::Result<u64> {
    let sql = sql.replace('?', "??");
    let mut stream = client.query_stream::<R>(&sql, settings).await?;
    let mut rows = 0u64;
    while let Some(row) = stream.next().await {
        row?;
        rows += 1;
    }
    Ok(rows)
}

#[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
struct ShardLogRow {
    hostname: String,
    is_initial_query: u8,
    read_rows: u64,
    read_bytes: u64,
    selected_marks: u64,
    memory_usage: u64,
    query_duration_ms: u64,
}

/// Reads every shard's own `system.query_log` rows for
/// `initial_query_id` via `clusterAllReplicas` (an initial query's
/// `initial_query_id` equals its `query_id`, so this matches both the
/// coordinator's `is_initial_query = 1` row and every remote sub-query
/// row), aggregated per shard.
async fn read_shard_rows(
    client: &ChClient,
    cluster: &str,
    topology: &ClusterTopology,
    initial_query_id: &str,
) -> anyhow::Result<BTreeMap<u32, ShardLogRow>> {
    let sql = format!(
        "SELECT hostName() AS hostname, is_initial_query, read_rows, read_bytes, \
         ProfileEvents['SelectedMarks'] AS selected_marks, memory_usage, query_duration_ms \
         FROM clusterAllReplicas('{cluster}', system.query_log) \
         WHERE initial_query_id = '{initial_query_id}' AND type = 'QueryFinish'"
    );
    let mut stream = client
        .query_stream::<ShardLogRow>(&sql, &QuerySettings::new())
        .await?;
    let mut by_shard: BTreeMap<u32, ShardLogRow> = BTreeMap::new();
    while let Some(row) = stream.next().await {
        let row = row?;
        let shard_num = topology.shard_of_hostname(&row.hostname).ok_or_else(|| {
            anyhow::anyhow!(
                "query_log row from hostname {:?} has no {{shard}} macro in the topology",
                row.hostname
            )
        })?;
        by_shard
            .entry(shard_num)
            .and_modify(|existing| {
                existing.is_initial_query = existing.is_initial_query.max(row.is_initial_query);
                existing.read_rows += row.read_rows;
                existing.read_bytes += row.read_bytes;
                existing.selected_marks += row.selected_marks;
                existing.memory_usage = existing.memory_usage.max(row.memory_usage);
                existing.query_duration_ms = existing.query_duration_ms.max(row.query_duration_ms);
            })
            .or_insert(row);
    }
    Ok(by_shard)
}

/// The `expected-pruned` derivation string — the exact
/// `cityHash64(trace_id) % total_weight` reasoning
/// `optimize_skip_unused_shards` uses, spelled out per queried id.
fn pruned_reason(ids: &[[u8; 16]], topology: &ClusterTopology, shard_num: u32) -> String {
    let derivations: Vec<String> = ids
        .iter()
        .map(|id| {
            let hash = city_hash_64_16(id);
            let slot = hash % topology.total_weight();
            format!(
                "cityHash64({}) = {hash}, % {} = {slot} -> shard {}",
                hex32(id),
                topology.total_weight(),
                topology.shard_for_fingerprint(hash)
            )
        })
        .collect();
    format!(
        "optimize_skip_unused_shards pruned shard {shard_num}: none of the queried trace ids \
         map to it ({})",
        derivations.join("; ")
    )
}

/// Executes one stage and verdicts its per-shard evidence against the
/// client-computed expected roster (hard errors — the CI gate).
async fn capture_stage(
    client: &ChClient,
    cluster: &str,
    topology: &ClusterTopology,
    spec: StageSpec,
    wall_ms: f64,
    returned_rows: u64,
    query_id: &str,
) -> anyhow::Result<TraceStageEvidence> {
    flush_logs_before_shard_read(client, cluster).await?;
    let by_shard = read_shard_rows(client, cluster, topology, query_id).await?;

    let expected: BTreeSet<u32> = match &spec.roster {
        Roster::Full => topology.all_shards(),
        Roster::TraceIds(ids) => ids
            .iter()
            .map(|id| topology.shard_for_fingerprint(city_hash_64_16(id)))
            .collect(),
    };
    let observed: BTreeSet<u32> = by_shard
        .iter()
        .filter(|(_, row)| row.read_rows > 0 || row.selected_marks > 0)
        .map(|(shard, _)| *shard)
        .collect();
    anyhow::ensure!(
        observed == expected,
        "stage {:?} (query_id={query_id}): observed participating shards {observed:?} != \
         expected {expected:?} — missing shards are lost system.query_log rows, unexpected \
         shards are pruning/sharding violations; SQL:\n{}",
        spec.stage,
        spec.sql
    );
    let coordinator_rows = by_shard
        .values()
        .filter(|row| row.is_initial_query == 1)
        .count();
    anyhow::ensure!(
        coordinator_rows == 1,
        "stage {:?} (query_id={query_id}): {coordinator_rows} coordinator-local \
         (is_initial_query = 1) rows, expected exactly 1",
        spec.stage
    );

    let mut shards = Vec::new();
    for shard_num in topology.all_shards() {
        match by_shard.get(&shard_num) {
            Some(row) if expected.contains(&shard_num) => shards.push(TraceShardEvidence {
                shard_num,
                role: if row.is_initial_query == 1 {
                    "coordinator-local".to_string()
                } else {
                    "remote".to_string()
                },
                read_rows: row.read_rows,
                read_bytes: row.read_bytes,
                selected_marks: row.selected_marks,
                memory_usage: row.memory_usage,
                query_duration_ms: row.query_duration_ms,
                pruned_reason: None,
            }),
            _ => {
                let reason = match &spec.roster {
                    Roster::Full => anyhow::bail!(
                        "stage {:?}: shard {shard_num} missing from a Full-roster stage \
                         (unreachable after the observed == expected verdict)",
                        spec.stage
                    ),
                    Roster::TraceIds(ids) => pruned_reason(ids, topology, shard_num),
                };
                shards.push(TraceShardEvidence {
                    shard_num,
                    role: "expected-pruned".to_string(),
                    read_rows: 0,
                    read_bytes: 0,
                    selected_marks: 0,
                    memory_usage: 0,
                    query_duration_ms: 0,
                    pruned_reason: Some(reason),
                });
            }
        }
    }

    Ok(TraceStageEvidence {
        stage: spec.stage,
        sql: spec.sql,
        wall_ms,
        returned_rows,
        expected_shards: expected.into_iter().collect(),
        shards,
    })
}

/// Cross-checks the client-side CityHash64 against the live server for
/// every id a roster is derived from — a hash drift fails loudly here,
/// never as a mysteriously wrong roster.
async fn cross_check_hashes(client: &ChClient, ids: &[[u8; 16]]) -> anyhow::Result<()> {
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct HashRow {
        h: u64,
    }
    for id in ids {
        let hexid = hex32(id);
        let sql = format!("SELECT cityHash64(unhex('{hexid}')) AS h");
        let mut stream = client
            .query_stream::<HashRow>(&sql, &QuerySettings::new())
            .await?;
        let server = stream
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("no cityHash64 row for {hexid}"))??
            .h;
        let client_side = city_hash_64_16(id);
        anyhow::ensure!(
            server == client_side,
            "cityHash64 drift for {hexid}: client {client_side} != server {server}"
        );
    }
    Ok(())
}

async fn exec(client: &ChClient, sql: &str) -> anyhow::Result<()> {
    client
        .execute(sql, &QuerySettings::new(), Idempotency::Idempotent)
        .await?;
    Ok(())
}

async fn count_of(client: &ChClient, sql: &str) -> anyhow::Result<u64> {
    #[derive(Row, serde::Serialize, serde::Deserialize, Debug, Clone)]
    struct CountRow {
        n: u64,
    }
    let mut stream = client
        .query_stream::<CountRow>(sql, &QuerySettings::new())
        .await?;
    match stream.next().await {
        Some(row) => Ok(row?.n),
        None => Ok(0),
    }
}

fn now_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("fits i64")
}

/// Seeds the corpus **through the `_dist` wrappers** (the Distributed
/// engine performs the same `cityHash64(trace_id) % total_weight`
/// placement the rosters derive), then polls until fully visible
/// (Distributed inserts are asynchronous — no fixed sleeps).
async fn seed_corpus(client: &ChClient, db: &str, base_ns: i64) -> anyhow::Result<()> {
    let spread = WINDOW_NS / TRACES as i64;
    exec(
        client,
        &format!(
            "INSERT INTO {db}.trace_spans_dist \
             (trace_id, span_id, parent_id, name, service, timestamp_ns, duration_ns, \
              status_code, kind, payload_type, payload) \
             SELECT \
               toFixedString(unhex(leftPad(lower(hex(number)), 32, '0')), 16), \
               toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8), \
               toFixedString(unhex('0000000000000000'), 8), \
               'op', \
               if(number % {CHECKOUT_EVERY} = 0, 'checkout', concat('svc-', toString(number % 8))), \
               {base_ns} + toInt64(number) * {spread}, \
               1000000, if(number % {CHECKOUT_EVERY} = 0, 2, 0), 1, 1, 'p' \
             FROM numbers({TRACES})"
        ),
    )
    .await?;
    exec(
        client,
        &format!(
            "INSERT INTO {db}.trace_attrs_idx_dist \
             (date, key, val, scope, val_num, timestamp_ns, trace_id, span_id, duration_ns) \
             SELECT \
               toDate(fromUnixTimestamp64Nano({base_ns} + toInt64(number) * {spread})), \
               'http.status_code', \
               if(number % {CHECKOUT_EVERY} = 0, '500', '200'), 'span', \
               if(number % {CHECKOUT_EVERY} = 0, 500.0, 200.0), \
               {base_ns} + toInt64(number) * {spread}, \
               toFixedString(unhex(leftPad(lower(hex(number)), 32, '0')), 16), \
               toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8), \
               1000000 \
             FROM numbers({TRACES})"
        ),
    )
    .await?;
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let spans = count_of(
            client,
            &format!("SELECT count() AS n FROM {db}.trace_spans_dist"),
        )
        .await?;
        let attrs = count_of(
            client,
            &format!("SELECT count() AS n FROM {db}.trace_attrs_idx_dist"),
        )
        .await?;
        if spans == TRACES && attrs == TRACES {
            return Ok(());
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "corpus never became fully visible through the _dist wrappers \
             (spans {spans}/{TRACES}, attrs {attrs}/{TRACES})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Picks the first `want` checkout-trace ids whose owners cover BOTH
/// shards (so the batched stages genuinely exercise a multi-shard
/// roster), preferring an even split.
fn pick_batch(topology: &ClusterTopology, want: usize) -> Vec<[u8; 16]> {
    let mut by_shard: BTreeMap<u32, Vec<[u8; 16]>> = BTreeMap::new();
    for n in (0..TRACES).step_by(CHECKOUT_EVERY as usize) {
        let id = trace_id_of(n);
        by_shard
            .entry(topology.shard_for_fingerprint(city_hash_64_16(&id)))
            .or_default()
            .push(id);
    }
    let mut batch = Vec::new();
    'outer: while batch.len() < want {
        let before = batch.len();
        for ids in by_shard.values_mut() {
            if let Some(id) = ids.pop() {
                batch.push(id);
                if batch.len() == want {
                    break 'outer;
                }
            }
        }
        assert!(
            batch.len() > before,
            "ran out of checkout trace ids before filling the evidence batch"
        );
    }
    batch.sort();
    batch
}

pub async fn run(args: BenchArgs) -> anyhow::Result<()> {
    let (server, http_port) = parse_http_url(&args.http_url)?;
    let admin_cfg = ChConnConfig {
        server,
        http_port,
        database: "default".to_string(),
        user: args.user.clone(),
        password: args.password.clone(),
        proto: ChProto::Http,
        pool_size: 4,
        query_timeout: Duration::from_secs(600),
        ..ChConnConfig::default()
    };
    let admin = ChClient::new(admin_cfg.clone()).await?;
    // `ON CLUSTER … SYNC`: a coordinator-local drop would leave shard 2's
    // database (and its Keeper replica paths) behind, and an async drop
    // races Keeper metadata cleanup on re-create
    // (`REPLICA_ALREADY_EXISTS` — the `live_cluster.rs`-documented race).
    exec(
        &admin,
        &format!(
            "DROP DATABASE IF EXISTS {} ON CLUSTER {} SYNC",
            args.database, args.cluster
        ),
    )
    .await?;

    eprintln!(
        "=== initializing clustered schema (db={}, cluster={}) ===",
        args.database, args.cluster
    );
    run_init(
        &admin,
        &RenderCtx {
            db: args.database.clone(),
            cluster: Some(args.cluster.clone()),
            dist_suffix: "_dist".to_string(),
            storage_policy: None,
            retention_days: 7,
            log_rollup: Duration::from_secs(5),
        },
    )
    .await?;

    let mut data_cfg = admin_cfg.clone();
    data_cfg.database = args.database.clone();
    let client = ChClient::new(data_cfg.clone()).await?;

    let now = now_ns();
    let base = now - WINDOW_NS;
    eprintln!("=== seeding {TRACES} traces through the _dist wrappers ===");
    seed_corpus(&client, &args.database, base).await?;

    let topology = load_cluster_topology(&client, &args.cluster).await?;

    // The batched-stage candidate set: 4 checkout traces spanning both
    // shards; the point-read/root targets are single ids.
    let batch = pick_batch(&topology, 4);
    let target = batch[0];
    let mut roster_ids = batch.clone();
    roster_ids.push(target);
    cross_check_hashes(&client, &roster_ids).await?;
    {
        let owners: BTreeSet<u32> = batch
            .iter()
            .map(|id| topology.shard_for_fingerprint(city_hash_64_16(id)))
            .collect();
        anyhow::ensure!(
            owners == topology.all_shards(),
            "the evidence batch must span every shard (got {owners:?})"
        );
    }

    // The REAL plans, from the product planner, against the _dist tables.
    let (plan, attr_plan) = evidence_plans(base, now)?;

    let stages = evidence_stages(&plan, &attr_plan, &batch, target);

    eprintln!("=== running {} evidence stages ===", stages.len());
    // Per-run nonce: `system.query_log` outlives databases, so a
    // deterministic query_id would aggregate rows across runs (verified
    // live — the evidence read matches on `initial_query_id`).
    let run_nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    let mut evidence = Vec::new();
    for (i, spec) in stages.into_iter().enumerate() {
        let query_id = format!("pulsus-traces-read-{run_nonce}-{i}-{}", spec.stage);
        let settings = reader_settings(&query_id);
        let started = Instant::now();
        // The decoder comes from the SPEC, beside the SQL it decodes —
        // never from a second `match` on the stage name, which is how the
        // membership read came to be built four-column and decoded
        // three-column.
        let returned_rows = match spec.decoder {
            Decoder::StoredSpan => drain::<StoredSpanRow>(&client, &spec.sql, &settings).await?,
            Decoder::Candidate => drain::<CandidateRow>(&client, &spec.sql, &settings).await?,
            Decoder::Hydration => drain::<HydrationRow>(&client, &spec.sql, &settings).await?,
            Decoder::Membership => drain::<MembershipRow>(&client, &spec.sql, &settings).await?,
            Decoder::TypedStrValue => {
                drain::<TypedStrValueRow>(&client, &spec.sql, &settings).await?
            }
            Decoder::Root => drain::<RootRow>(&client, &spec.sql, &settings).await?,
        };
        let wall_ms = started.elapsed().as_secs_f64() * 1_000.0;
        anyhow::ensure!(
            returned_rows > 0,
            "stage {:?} returned zero rows — the evidence query matched nothing",
            spec.stage
        );
        let stage_evidence = capture_stage(
            &client,
            &args.cluster,
            &topology,
            spec,
            wall_ms,
            returned_rows,
            &query_id,
        )
        .await?;
        eprintln!(
            "{:>26}: wall={:.1}ms returned={} expected_shards={:?} roles={:?}",
            stage_evidence.stage,
            stage_evidence.wall_ms,
            stage_evidence.returned_rows,
            stage_evidence.expected_shards,
            stage_evidence
                .shards
                .iter()
                .map(|s| format!("{}:{}", s.shard_num, s.role))
                .collect::<Vec<_>>()
        );
        evidence.push(stage_evidence);
    }

    // End-to-end product search over the _dist tables: correctness
    // asserted (the top `limit` checkout traces, complete result) plus a
    // recorded wall time (never gated).
    eprintln!("=== running the end-to-end TraceEngine::search ===");
    let engine = TraceEngine::new(
        ChClient::new(data_cfg).await?,
        TraceReadConfig {
            // Issue #398: compilation-forced only — the struct gained a
            // required field. Set to the production default so the bench's
            // queries are unchanged apart from a ceiling they never
            // approach; no measured quantity can move.
            read_max_memory_bytes: 8 * 1024 * 1024 * 1024,
            spans_table: "trace_spans_dist".to_string(),
            attrs_table: "trace_attrs_idx_dist".to_string(),
            // Never `_dist`: the tag catalog is a Global table with no
            // wrapper (unused by this bench's search-only path anyway).
            // `_dist` like the other Traces-family tables (unused by this
            // bench's search-only path).
            edges_table: "trace_edges_dist".to_string(),
            max_candidates: 100_000,
            scan_budget_rows: 50_000_000,
            max_series: 1_000,
            generator_max_memory_bytes: 536_870_912,
            distributed: true,
            skip_unavailable_shards: false,
        },
    );
    let started = Instant::now();
    let output = engine
        .search(&plan)
        .await
        .map_err(|e| anyhow::anyhow!("end-to-end search failed: {e}"))?;
    let search_wall_ms = started.elapsed().as_secs_f64() * 1_000.0;
    anyhow::ensure!(
        output.returned == 20 && !output.partial,
        "end-to-end search must return the full page of checkout traces (returned {}, \
         partial {})",
        output.returned,
        output.partial
    );
    for trace in &output.traces {
        let n = u128::from_be_bytes(trace.trace_id) as u64;
        anyhow::ensure!(
            n.is_multiple_of(CHECKOUT_EVERY) && trace.root.service == "checkout",
            "end-to-end search returned a non-checkout trace {}",
            hex32(&trace.trace_id)
        );
    }

    let report = TracesReadReport {
        cluster: args.cluster.clone(),
        database: args.database.clone(),
        traces: TRACES,
        checkout_traces: TRACES / CHECKOUT_EVERY,
        total_weight: topology.total_weight(),
        search_wall_ms,
        search_returned: output.returned,
        search_partial: output.partial,
        stages: evidence,
    };
    eprintln!(
        "end-to-end search: wall={search_wall_ms:.1}ms returned={} partial={}",
        report.search_returned, report.search_partial
    );

    if let Some(out) = &args.out {
        std::fs::write(out, serde_json::to_string_pretty(&report)?)?;
        eprintln!("wrote {out}");
    }
    if let Some(report_out) = &args.report_out {
        std::fs::write(report_out, render_markdown(&report))?;
        eprintln!("wrote {report_out}");
    }
    eprintln!("traces-read: all shard-locality verdicts passed");
    Ok(())
}

/// `pulsus_traceql::parse` with anyhow error context (xtask depends on
/// the parser only here).
fn pulsus_traceql_parse(q: &str) -> anyhow::Result<pulsus_traceql::Query> {
    pulsus_traceql::parse(q).map_err(|e| anyhow::anyhow!("parse {q:?}: {e}"))
}

/// The markdown table body `docs/benchmarks/m4-traces-read-path.md`
/// embeds.
fn render_markdown(report: &TracesReadReport) -> String {
    let mut out = String::new();
    out.push_str(
        "| Stage | Expected shards | Shard | Role | read_rows | read_bytes | selected_marks |\n\
         |---|---|---|---|---|---|---|\n",
    );
    for stage in &report.stages {
        for shard in &stage.shards {
            out.push_str(&format!(
                "| `{}` | {:?} | {} | {} | {} | {} | {} |\n",
                stage.stage,
                stage.expected_shards,
                shard.shard_num,
                shard.role,
                shard.read_rows,
                shard.read_bytes,
                shard.selected_marks
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reference vectors captured from a live ClickHouse 24.8
    /// (`SELECT cityHash64(unhex('…'))`) — the client-side CityHash64
    /// v1.0.2 must match byte-for-byte (the run additionally cross-checks
    /// every roster id against the live server).
    #[test]
    fn city_hash_64_16_matches_live_clickhouse_vectors() {
        for (hexid, want) in [
            ("000102030405060708090a0b0c0d0e0f", 3436483321192130403u64),
            ("00000000000000000000000000000001", 3747971208336161882),
            ("ffffffffffffffffffffffffffffffff", 11156510505809607899),
            ("4bf92f3577b34da6a3ce929d0e0e4736", 9892910747646051082),
        ] {
            let mut id = [0u8; 16];
            for (i, byte) in id.iter_mut().enumerate() {
                *byte = u8::from_str_radix(&hexid[i * 2..i * 2 + 2], 16).expect("hex");
            }
            assert_eq!(city_hash_64_16(&id), want, "vector {hexid}");
        }
    }

    /// Every evidence stage projects exactly the columns its decoder
    /// expects, by name and in order.
    ///
    /// **This is the check that was missing.** Issue #510 widened
    /// `membership_sql`'s fused projection from `trace_id, span_id, v` to
    /// `trace_id, span_id, v, t`, moved the three production call sites in
    /// `traces/exec.rs`, and left this bench's decoder at the
    /// three-column row. RowBinary is positional, so the run died with
    /// `schema mismatch: database schema has 4 columns, but the struct
    /// definition has 3 fields` — but only in the clustered CI leg,
    /// because nothing local runs this bench.
    ///
    /// It needs no cluster and no client: [`evidence_stages`] is pure, and
    /// the expected names are the decoder's own `COLUMN_NAMES` rather
    /// than a second list.
    ///
    /// It is also the check that EVERY real projection parses under
    /// [`projection_names`]'s accepted grammar: that reader refuses what
    /// it cannot read, so a builder emitting a construct outside the
    /// grammar reddens here rather than being read as something
    /// plausible.
    ///
    /// *RED when:* a shared SQL builder's projection changes and this
    /// consumer is not moved with it. Measured, three ways:
    ///
    /// * the exact CI defect, a `StrValueRow` decoder reintroduced on the
    ///   fused arm — `left ["trace_id", "span_id", "v", "t"]` against
    ///   `right ["trace_id", "span_id", "v"]`, the same two lists the
    ///   server's decode error named;
    /// * `phase2_hydration` given the root row's decoder — 12 columns
    ///   against 7;
    /// * the two arms of the membership dispatch swapped — 4 against 2,
    ///   so the check is not specific to one direction.
    #[test]
    fn the_evidence_stages_project_exactly_what_they_decode() {
        let base = 1_700_000_000_000_000_000i64;
        let now = base + WINDOW_NS;
        let (plan, attr_plan) = evidence_plans(base, now).expect("the evidence queries plan");
        let batch = [trace_id_of(1), trace_id_of(2)];
        let stages = evidence_stages(&plan, &attr_plan, &batch, batch[0]);
        assert_eq!(stages.len(), 6, "every stage must be checked");
        for spec in &stages {
            assert_eq!(
                projection_names(&spec.sql),
                spec.decoder.columns(),
                "stage {:?} projects columns its decoder ({:?}) does not expect.\nSQL:\n{}",
                spec.stage,
                spec.decoder,
                spec.sql
            );
        }
        // The membership stage takes the FUSED arm for this query, which
        // is the arm issue #510 widened. Pinned so a planner change that
        // silently stopped fusing would not turn this test into a check
        // of the two-column shape.
        let membership = stages
            .iter()
            .find(|s| s.stage == "phase2_membership")
            .expect("the membership stage");
        assert_eq!(membership.decoder, Decoder::TypedStrValue);
        assert_eq!(
            membership.decoder.columns(),
            ["trace_id", "span_id", "v", "t"]
        );
    }

    /// The outermost `SELECT` list's output names, in order — read by
    /// ACCEPTING a narrow grammar and refusing everything else.
    ///
    /// **The grammar is the definition, not a description of what happens
    /// to pass:**
    ///
    /// ```text
    /// projection := item ("," item)*
    /// item       := expr ["AS" ident]
    /// expr       := term (cmp term)*
    /// term       := call | ident | integer
    /// call       := ident "(" [expr ("," expr)*] ")"
    /// ident      := [A-Za-z_][A-Za-z0-9_]*
    /// integer    := [0-9]+
    /// cmp        := "<=" | ">=" | "!=" | "<" | ">" | "="
    /// ```
    ///
    /// An item that is a bare `ident` is named by that identifier; any
    /// other item MUST carry an `AS ident`, because guessing the name the
    /// server would give an unaliased expression is the mis-read this
    /// check exists to prevent. (Measured: `SELECT max(number), 1 AS z`
    /// is named `max(number)`, `z` by the server.)
    ///
    /// **Why an accept-list and not a longer list of refusals.** Three
    /// review rounds found three fresh holes in a reader that tried to
    /// handle every projection and refuse the bad shapes it knew about:
    /// parentheses inside string literals, then square-bracket array
    /// literals, backslash-escaped quotes, and quoted aliases. Each was a
    /// PLAUSIBLE WRONG ANSWER, and a plausible wrong answer is
    /// indistinguishable from a pass. The set of shapes a reader must
    /// handle is unbounded; the set these five builders emit is not. So
    /// the alphabet is closed — identifiers, integers, `(`, `)`, `,` and
    /// the comparison operators, nothing else — and every other character
    /// is refused BY NAME. Square brackets, backslashes, quotes and
    /// backticks need no special handling: they are simply not in the
    /// alphabet, and neither is the next construct nobody has thought of.
    ///
    /// A projection that grows a new construct fails loudly and somebody
    /// looks. That is the point.
    fn projection_names(sql: &str) -> Vec<String> {
        let head = sql
            .strip_prefix("SELECT DISTINCT ")
            .or_else(|| sql.strip_prefix("SELECT "))
            .unwrap_or_else(|| panic!("a stage's SQL must start with SELECT:\n{sql}"));
        let tokens = tokenize_projection(head, sql);
        parse_projection(&tokens, sql)
    }

    /// One token of the closed alphabet.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Tok {
        Ident(String),
        Int,
        Open,
        Close,
        Comma,
        Cmp,
    }

    /// Lexes the projection up to the first `FROM` at paren depth 0.
    ///
    /// Refuses, naming what it saw, on any character outside the closed
    /// alphabet, on an unbalanced `)`, and on running out of input without
    /// a top-level `FROM`.
    fn tokenize_projection(head: &str, sql: &str) -> Vec<Tok> {
        let chars: Vec<char> = head.chars().collect();
        let mut out: Vec<Tok> = Vec::new();
        let mut depth = 0i32;
        let mut i = 0usize;
        while i < chars.len() {
            let c = chars[i];
            if c.is_whitespace() {
                i += 1;
                continue;
            }
            if c.is_ascii_alphabetic() || c == '_' {
                let from = i;
                while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let word: String = chars[from..i].iter().collect();
                if word == "FROM" && depth == 0 {
                    return out;
                }
                out.push(Tok::Ident(word));
                continue;
            }
            if c.is_ascii_digit() {
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
                out.push(Tok::Int);
                continue;
            }
            match c {
                '(' => {
                    depth += 1;
                    out.push(Tok::Open);
                    i += 1;
                }
                ')' => {
                    depth -= 1;
                    assert!(depth >= 0, "unbalanced `)` in the projection of:\n{sql}");
                    out.push(Tok::Close);
                    i += 1;
                }
                ',' => {
                    out.push(Tok::Comma);
                    i += 1;
                }
                '<' | '>' | '!' | '=' => {
                    if chars.get(i + 1) == Some(&'=') {
                        i += 2;
                    } else {
                        assert!(
                            c != '!',
                            "unexpected character {c:?} in the projection of:\n{sql}"
                        );
                        i += 1;
                    }
                    out.push(Tok::Cmp);
                }
                other => panic!(
                    "unexpected character {other:?} in the projection of:\n{sql}\n\
                     The accepted alphabet is identifiers, integers, `(`, `)`, `,` and the \
                     comparison operators. A projection outside that shape is REFUSED rather \
                     than read, because a plausible wrong answer here is indistinguishable \
                     from a pass — extend the grammar deliberately if a builder needs it."
                ),
            }
        }
        panic!("no top-level FROM in the projection of:\n{sql}")
    }

    /// `projection := item ("," item)*`, consumed to the end.
    fn parse_projection(tokens: &[Tok], sql: &str) -> Vec<String> {
        let mut at = 0usize;
        let mut names = Vec::new();
        loop {
            names.push(parse_item(tokens, &mut at, sql));
            match tokens.get(at) {
                Some(Tok::Comma) => at += 1,
                None => break,
                Some(other) => {
                    panic!("expected `,` or the end of the projection, found {other:?}, in:\n{sql}")
                }
            }
        }
        names
    }

    /// `item := expr ["AS" ident]`. A bare identifier names itself;
    /// anything else must be aliased.
    fn parse_item(tokens: &[Tok], at: &mut usize, sql: &str) -> String {
        let from = *at;
        parse_expr(tokens, at, sql);
        let bare = *at == from + 1 && matches!(tokens[from], Tok::Ident(_));
        if tokens.get(*at) == Some(&Tok::Ident("AS".to_string())) {
            *at += 1;
            let Some(Tok::Ident(alias)) = tokens.get(*at) else {
                panic!(
                    "`AS` must be followed by a plain identifier, found {:?}, in:\n{sql}",
                    tokens.get(*at)
                );
            };
            assert!(alias != "AS", "`AS AS` in the projection of:\n{sql}");
            *at += 1;
            return alias.clone();
        }
        assert!(
            bare,
            "an unaliased projection item that is not a bare column is REFUSED: the server \
             names it from its own rendering of the expression, which this check will not \
             guess. Add `AS <name>`, in:\n{sql}"
        );
        let Tok::Ident(name) = &tokens[from] else {
            unreachable!("bare implies Ident")
        };
        name.clone()
    }

    /// `expr := term (cmp term)*`
    fn parse_expr(tokens: &[Tok], at: &mut usize, sql: &str) {
        parse_term(tokens, at, sql);
        while tokens.get(*at) == Some(&Tok::Cmp) {
            *at += 1;
            parse_term(tokens, at, sql);
        }
    }

    /// `term := call | ident | integer`
    fn parse_term(tokens: &[Tok], at: &mut usize, sql: &str) {
        match tokens.get(*at) {
            Some(Tok::Int) => *at += 1,
            Some(Tok::Ident(word)) => {
                assert!(
                    word != "AS",
                    "`AS` where an expression was expected, in:\n{sql}"
                );
                *at += 1;
                if tokens.get(*at) == Some(&Tok::Open) {
                    *at += 1;
                    if tokens.get(*at) != Some(&Tok::Close) {
                        loop {
                            parse_expr(tokens, at, sql);
                            match tokens.get(*at) {
                                Some(Tok::Comma) => *at += 1,
                                _ => break,
                            }
                        }
                    }
                    assert!(
                        tokens.get(*at) == Some(&Tok::Close),
                        "unclosed call in the projection of:\n{sql}"
                    );
                    *at += 1;
                }
            }
            other => panic!("expected an expression, found {other:?}, in:\n{sql}"),
        }
    }

    /// The accepted grammar reads what the five builders emit, and
    /// REFUSES every construct outside it — including three shapes that
    /// three separate review rounds found the previous, handle-everything
    /// reader mis-reading.
    ///
    /// Every "server" column below is ground truth from ClickHouse 26.3
    /// (`FORMAT TSVWithNames`, header row), not read from source.
    ///
    /// *RED when:* the alphabet is widened without the grammar being
    /// widened with it — measured, accepting `[` as an ordinary character
    /// makes `SELECT [1, 2] AS arr, 3 AS z` read as `["[1", "arr", "z"]`
    /// where the server answers `arr, z`.
    #[test]
    fn the_projection_reader_accepts_one_narrow_grammar_and_refuses_the_rest() {
        // Accepted: exactly the constructs the five builders emit.
        let accepted: [(&str, &[&str]); 6] = [
            // bare columns
            ("SELECT a, b FROM t", &["a", "b"]),
            // an aggregate with an alias — the generator stage's shape
            (
                "SELECT trace_id, max(timestamp_ns) AS bound_ts FROM t",
                &["trace_id", "bound_ts"],
            ),
            // nested calls carrying their own commas, and a comparison
            // inside an argument — the byte-cap expression's shape
            (
                "SELECT trace_id, if(length(val) <= 8192, val, substringUTF8(val, 1, 2048)) AS v, \
                 val_type AS t FROM x",
                &["trace_id", "v", "t"],
            ),
            // server ground truth: plus(1, multiply(2, 3)) AS a, 4 AS b -> a, b
            (
                "SELECT plus(1, multiply(2, 3)) AS a, 4 AS b FROM t",
                &["a", "b"],
            ),
            // a call with no arguments
            ("SELECT now() AS n, b FROM t", &["n", "b"]),
            // SELECT DISTINCT
            ("SELECT DISTINCT a, b FROM t", &["a", "b"]),
        ];
        for (sql, want) in accepted {
            assert_eq!(projection_names(sql), want, "reading {sql:?}");
        }

        // Refused, each with the reason named. The first three are the
        // round-4 findings; the next is round 3's; the rest are shapes
        // this reader will not model.
        let refused: [(&str, &str, &str); 11] = [
            // (sql, what the SERVER answers, the refusal's needle)
            ("SELECT [1, 2] AS arr, 3 AS z FROM t", "arr, z", "'['"),
            (
                "SELECT 'fake \\' actual' AS actual, 2 AS z FROM t",
                "actual, z",
                "'\\''",
            ),
            (
                "SELECT 1 AS `left AS right`, 2 AS z FROM t",
                "left AS right, z",
                "'`'",
            ),
            (
                "SELECT '(' AS extra, ')' AS x, 1 AS y FROM t",
                "extra, x, y",
                "'\\''",
            ),
            ("SELECT CAST(a, 'UInt64') AS n, b FROM t", "n, b", "'\\''"),
            (
                "SELECT (SELECT max(x) FROM u) AS m, b FROM t",
                "m, b",
                "expected an expression",
            ),
            ("SELECT max(x), 1 AS z FROM t", "max(x), z", "unaliased"),
            ("SELECT 'a AS x FROM t", "(a parse error)", "'\\''"),
            ("SELECT a) AS x FROM t", "(a parse error)", "unbalanced"),
            ("SELECT a -- , b\n AS x FROM t", "x", "'-'"),
            ("SELECT a, b", "(a parse error)", "no top-level FROM"),
        ];
        for (sql, server, needle) in refused {
            let err = std::panic::catch_unwind(|| projection_names(sql)).expect_err(&format!(
                "{sql:?} must be refused, not read (server: {server})"
            ));
            let msg = err
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| err.downcast_ref::<&str>().map(|s| (*s).to_string()))
                .unwrap_or_default();
            assert!(
                msg.contains(needle),
                "{sql:?} must be refused naming {needle:?}; got {msg:?}"
            );
        }
    }

    #[test]
    fn trace_id_of_matches_the_server_side_leftpad_hex_expression() {
        // hex(255) = 'ff' → leftPad 32 → big-endian u128 bytes.
        let id = trace_id_of(255);
        assert_eq!(hex32(&id), "000000000000000000000000000000ff");
    }
}
