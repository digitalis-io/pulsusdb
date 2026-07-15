# M4: Traces read-path shard-locality evidence (2-shard cluster)

> **This report establishes Tier-1 evidence for the traces rows of
> `docs/schemas.md` §7** — trace-by-ID single-shard confinement and
> shard-local two-phase TraceQL search — on the 2-shard
> `ci/clickhouse-cluster` fixture. Latency at Tier-2 scale remains
> separately tracked ([#25](https://github.com/digitalis-io/pulsusdb/issues/25));
> nothing here is gated on wall-clock time.

Issue: [#57](https://github.com/digitalis-io/pulsusdb/issues/57) (AC4).
Evidence model: `docs/schemas.md` §9's two-tier model, following the
issue #16 logs graduation methodology (per-stage, exact-shard-roster,
coordinator-inclusive).

## What this proves, and what it doesn't

- **Proves (Tier 1):** on a real 2-shard cluster, every stage the
  two-phase TraceQL search executes — Phase-1 candidate generators,
  Phase-2 batch hydration and attribute-membership reads, winners' root
  hydration — plus the §4.2 trace-by-ID point read, runs **shard-local**:
  each participating shard's own `system.query_log` row reads no more
  than its local table share, and `trace_id`-keyed stages prune to
  **exactly** the client-derived owning-shard set
  (`cityHash64(trace_id) % total_weight`), with excluded shards proven
  absent, not assumed. The end-to-end `TraceEngine::search` over the
  `_dist` tables returns the correct, complete result set under the
  same settings.
- **Does not prove:** index/projection pruning ratios at this corpus
  scale (single-granule parts — that is the single-node AC2 gate's job,
  `crates/pulsus-read/tests/traces_search_explain.rs`, on a ≥100k-span
  corpus), or the §9 latency targets (Tier 2, #25). Wall times below are
  recorded for context only.

## Harness

`cargo xtask bench traces-read` (`xtask/src/bench/traces_read.rs`), on the
2-shard + Keeper `ci/clickhouse-cluster` fixture (the `schema-it-cluster`
topology; the CI leg runs it on every push — verdicts are hard errors):

- Seeds 4,000 single-span traces (2% `service = 'checkout'`, each with a
  `http.status_code` attr row) **through the `_dist` wrappers**, so the
  Distributed engine performs the same `cityHash64(trace_id)` placement
  the rosters are derived from, then polls `_dist` counts until fully
  visible (no fixed sleeps).
- Runs the **product planner's own generated SQL**
  (`pulsus_read::traces::plan_search` / `point_read_sql`) for every
  stage, tagged with a unique `query_id`, under the §7 clustered-reader
  settings.
- **Expected-roster model (the #16 pattern):** `trace_id`-keyed stages'
  expected shard set is computed client-side — CityHash64 v1.0.2
  (ClickHouse's pinned variant, carried in the harness and cross-checked
  against the live server for every roster id; a drift fails the run) of
  each queried id, `% total_weight` over the cumulative-weight slot map
  from `system.clusters`, hostnames resolved to shard numbers via each
  node's `{shard}` macro. Generator stages have no `trace_id` predicate
  and expect the full roster.
- Reads every shard's own `system.query_log` rows via
  `clusterAllReplicas` after a cluster-wide `SYSTEM FLUSH LOGS`,
  matching on `initial_query_id`. **`is_initial_query` semantics (the
  #16 finding):** under `prefer_localhost_replica = 1` the coordinator's
  own local-shard read is part of its `is_initial_query = 1` row — a
  remote-rows-only filter would silently miss it. Verdicts: the observed
  participating set (shards with nonzero `read_rows`/`selected_marks`)
  must equal the expected roster **exactly** — a missing shard is a lost
  `query_log` row, an extra shard is a pruning/sharding violation — and
  exactly one coordinator row must exist per stage.
- **Totals-overlap caveat (per #16):** the coordinator's
  `is_initial_query = 1` row accumulates the profile counters remote
  shards report, so its `read_rows` is the cluster total for the query;
  its own local share is `total − Σ(remote rows)`. Participation and
  locality are judged from each shard's own row.

## Evidence (recorded run: 2 shards, weight 1 each, corpus 4,000 traces / 80 checkout)

Raw JSON: [`data/traces-read-cluster-ci.json`](data/traces-read-cluster-ci.json)
(scenario-local schema; the shared bench structs are untouched).

| Stage | Expected shards | Shard | Role | read_rows | read_bytes | selected_marks |
|---|---|---|---|---|---|---|
| `trace_by_id` | [1] | 1 | coordinator-local | 1994 | 69830 | 1 |
| `trace_by_id` | [1] | 2 | expected-pruned | 0 | 0 | 0 |
| `phase1_generator_service` | [1, 2] | 1 | coordinator-local | 4000 | 100016 | 1 |
| `phase1_generator_service` | [1, 2] | 2 | remote | 2006 | 50158 | 1 |
| `phase1_generator_attr` | [1, 2] | 1 | coordinator-local | 4000 | 148016 | 1 |
| `phase1_generator_attr` | [1, 2] | 2 | remote | 2006 | 74230 | 1 |
| `phase2_hydration` | [1, 2] | 1 | coordinator-local | 4000 | 172818 | 1 |
| `phase2_hydration` | [1, 2] | 2 | remote | 2006 | 86694 | 1 |
| `phase2_membership` | [1, 2] | 1 | coordinator-local | 4000 | 180032 | 1 |
| `phase2_membership` | [1, 2] | 2 | remote | 2006 | 90286 | 1 |
| `root_hydration` | [1] | 1 | coordinator-local | 1994 | 83788 | 1 |
| `root_hydration` | [1] | 2 | expected-pruned | 0 | 0 | 0 |

Reading the table (shard 1 holds 1,994 of the 4,000 rows per table,
shard 2 holds 2,006):

- **`trace_by_id` / `root_hydration` hit one shard.** The queried trace
  id's owning shard is 1 (`cityHash64 % 2` derivation recorded in the
  JSON's `pruned_reason`); shard 2 did zero work — an
  `optimize_skip_unused_shards` prune proven by derivation, not assumed.
  The coordinator-local `read_rows = 1994` is its whole local table:
  this CI corpus is a single granule per part, so granule-level pruning
  is out of frame here (see the single-node AC2 gate); the claim proven
  is **which shard does the work**.
- **Both generators fan out to both shards, each reading only its local
  rows** (shard 2's own row: 2,006 = exactly its local table; the
  coordinator row's 4,000 is the cluster total per the totals-overlap
  caveat, i.e. its local share is 1,994). Only `(trace_id, bound_ts)`
  tuples cross the network.
- **Batched Phase-2 reads are shard-local**: the 4-trace evidence batch
  deliberately spans both shards, and each shard's row reads only local
  data; no cross-shard join, no coordinator-side set materialization.
- **End-to-end** `TraceEngine::search` (distributed config, `_dist`
  tables) returned the correct complete page — 20/20 checkout traces,
  `partial = false` — in ~40 ms wall (recorded, not gated).

## Scope of the bounded-consumption claim (issue #57 round-5 adjudication)

The v7 bounded-consumption claim is about the **PulsusDB engine** (the
Rust side): what a generator *ships* is bounded to the `cap + 1` probe
rows (the with/without-LIMIT transfer differential in the single-node
AC2 gate — 5,001 vs 119,999 `result_rows` on the dense common-value
prefix), and what the engine *retains* is bounded by the request's
256 MiB charge-before-allocate counter. ClickHouse's **server-side**
generator aggregation memory is a separate quantity: it scales with the
matching prefix, not with the cap (measured ~21 MB at the 120k-row dense
`(key, val)` prefix on the AC2 fixture, essentially identical with and
without the LIMIT — the `GROUP BY trace_id` top-K must visit every
prefix row for correctness). That server-side memory is bounded by
prefix confinement plus the per-query server budgets, and its
remediation (a read-bounded common-value generator shape) is tracked in
[#63](https://github.com/digitalis-io/pulsusdb/issues/63). The AC2 gate
records server memory; it never gates on it.

## Caveats

- **The trace-by-ID single-shard row is proven under the §7
  reader-issued settings** (`optimize_skip_unused_shards = 1` et al.,
  which this harness applies — and which the search engine injects in
  clustered mode). The *fetch handler's* `TraceEngine::fetch_by_id` does
  not yet inject the clustered-reader settings itself; wiring them
  through the fetch path is a noted follow-up (`docs/schemas.md` §7).
- 2-shard topology: shard-locality is topology mechanics and
  scale-invariant (§7's graduation argument); corpus scale here is
  CI-sized by design.

## Reproduce

```text
podman-compose -f ci/clickhouse-cluster/compose.yaml up -d
# rootless local alternative: plain `podman network create` +
# `podman run --ip 172.28.0.1x` with the same mounts/ports as compose.yaml
cargo run -p xtask -- bench traces-read \
    --http-url http://127.0.0.1:18123 --database pulsus_traces_bench_ci \
    --cluster pulsus_test_cluster \
    --out docs/benchmarks/data/traces-read-cluster-ci.json
```
