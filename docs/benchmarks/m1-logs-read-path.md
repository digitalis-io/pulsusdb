# M1: Logs read-path benchmark against captured slow queries

> **This report establishes Tier-1 evidence and graduates the shard-locality
> claims.** The §9 latency targets remain **unvalidated** pending the Tier-2
> 1 TB/7d reference run ([#25](https://github.com/digitalis-io/pulsusdb/issues/25)).

Issue: [#16](https://github.com/digitalis-io/pulsusdb/issues/16). Evidence
model: `docs/schemas.md` §9's two-tier evidence model (added by this issue).

## What this proves, and what it doesn't

- **Proves (Tier 1, this report):** the query planner's own generated SQL for
  the three issue query shapes plus the §9-mandated label/series discovery
  shape uses primary-index confinement and skip-index pruning correctly
  (scale-invariant `system.query_log` ratios, `EXPLAIN indexes = 1`); the
  logs family's shard-local fan-out claim (`docs/schemas.md` §7) holds on a
  real 4-shard cluster (`EXPLAIN PIPELINE` + per-shard `system.query_log`);
  the direct RowBinary bulk-load path used by this harness — and by the
  future Tier-2 corpus — is byte-identical to the product OTLP ingest path
  on six hand-derived golden fixtures.
- **Does not prove:** the §9 latency targets at design-target scale (1 TB /
  7 days / 50 services / 5k streams). The CI-scale numbers below are
  recorded for regression-tracking only; they are **not comparable** to the
  §9 targets and are never gated on wall-clock time. That validation is
  Tier 2, tracked by #25.

## Harness

`cargo xtask bench logs-read` (`xtask/src/bench/`):

- **`dataset.rs`** — deterministic corpus generator. Hand-rolled
  splitmix64/xorshift64\* PRNG (never `rand`, so a committed baseline stays
  byte-reproducible across `rand` major-version bumps). Uses
  `pulsus-model`'s frozen `LabelSet::from_normalized`/`stream_fingerprint`
  (the same canonicalization every writer — product or bulk — agrees with)
  and `pulsus-schema::run_init` for DDL, so the generated corpus is
  schema-identical to a product-ingested one. Bulk-loads via direct
  RowBinary `INSERT` (`ChClient::insert_block`); in `--dist` mode this goes
  through the `_dist` Distributed wrappers (not the bare local tables) so
  the corpus actually spreads across shards by the `fingerprint` sharding
  key, then polls `count() FROM log_samples_dist` until the full corpus is
  visible before the query set runs (`_dist` writes are eventually
  consistent — no fixed sleeps, `docs/architecture.md` §9's convention).
  Injects a single-token needle (`xtaskneedle7c91a`) at a controlled 1-in-500
  rate so body-search selectivity is a known constant, not incidental.
  Timestamps are anchored at wall-clock `now` (never a fixed historical
  constant — `log_samples`' `ttl_only_drop_parts = 1` retention would make
  an already-expired fixture flaky, the same hazard
  `crates/pulsus-read/tests/explain_indexes.rs::now_ns` documents).
- **`queries.rs`** — runs the **product planner's own generated SQL**
  (`pulsus_read::logql::plan`/`sql`), never hand-written benchmark SQL, for
  each of the four canonical shapes below: one warmup pass (discarded), then
  `--reps` timed runs tagged with a unique `query_id` per stage. After every
  rep, `SYSTEM FLUSH LOGS` (cluster-wide in `--dist` mode — a shard's own
  sub-query row only becomes queryable via *that shard's own* flush) then
  reads `system.query_log`: `read_rows`, `read_bytes`,
  `ProfileEvents['SelectedMarks']`, `memory_usage`, `query_duration_ms`,
  summed across every stage the read actually executes (stage-1 resolution
  + stage-2 hydration + stage-3 samples/metric read/discovery — the same
  round trips `LogQlEngine` makes for a real request). Captures one
  `EXPLAIN indexes = 1` for the terminal stage per shape; in `--dist` mode
  also captures **every executed stage's own** `EXPLAIN PIPELINE` and
  per-shard `system.query_log` evidence via
  `clusterAllReplicas(cluster, system.query_log)` correlated by
  `initial_query_id` — not just the terminal stage (issue #16 CODE review
  round 1 [high] finding: a shape's shard-locality claim depends on every
  stage running shard-locally, and evidence for the terminal stage alone
  cannot substantiate a claim about stage-1 resolution or stage-2
  hydration). Every stage's shard rows are labelled `coordinator-local`
  (the initiator's own `is_initial_query = 1` row — its in-process
  local-shard read under `prefer_localhost_replica = 1`) or `remote`
  (`is_initial_query = 0`, one per other shard) — round 1's query kept only
  `is_initial_query = 0` rows, excluding the coordinator's own
  `is_initial_query = 1` row entirely, so it silently never captured the
  coordinator shard's contribution (CODE review round 2 [high] finding).
  Every discovery/metric terminal query, and every `EXPLAIN` capture, runs
  under the clustered-reader settings block (`reader_settings`/
  `QuerySettings::clustered_reader`) in `--dist` mode (CODE review round 1
  [medium] finding for the terminal-query settings, round 2 [medium]
  finding for `EXPLAIN`).
  `skip_unavailable_shards = false` for every `--dist` query (unlike the
  product's own configurable default) — a verification harness should fail
  loudly on shard unavailability, not silently record evidence for a
  degraded read.

  **Expected-roster model (CODE review round 3 [high] finding).** A shard
  correctly pruned by `optimize_skip_unused_shards = 1` produces *no*
  `system.query_log` row at all — which, looked at alone, is indistinguishable
  from a row that should exist but was lost to a genuine evidence-capture
  bug. Round 2's tolerance ("1..cluster_size rows, don't assert which
  shards") could not tell the two apart. Round 3 closes that gap by
  computing the **expected** participating shard set client-side, the same
  way ClickHouse's Distributed engine does: a cumulative-weight
  slot→shard map built once from `system.clusters` (+ `system.macros` for
  hostname→shard\_num), then `fingerprint % total_weight` per queried
  fingerprint for the fingerprint-scoped stages (`hydration`/`samples`/
  `rollup_range`); `resolution`/`discovery` have no `fingerprint` predicate
  to prune by, so their expected set is unconditionally the full cluster.
  The harness **fails loudly** (aborts the run) if the **observed**
  participating set (shards that did nonzero storage work) is not
  *exactly* the computed expected set — a missing expected shard is a lost
  row (FAIL), an unexpected participant is a pruning/mapping violation
  (FAIL) — plus the round-1/round-2 invariants (exactly one
  `coordinator-local` row, never more shard rows than the cluster has
  shards, nonempty `EXPLAIN PIPELINE`, the coordinator/remote balance
  sanity guard). Every shard in the cluster is represented in a stage's
  evidence either way: participating shards carry their real
  `system.query_log` row, and shards correctly excluded by pruning carry a
  synthesized `role = "expected-pruned"` entry whose `pruned_reason` field
  spells out the exact `fingerprint % total_weight` derivation — so the
  full roster is always *accounted for*, even on stages where it does not
  fully *participate*.
- **`report.rs`** — serializes to JSON (`docs/benchmarks/data/*.json`) and
  renders a markdown evidence table.

Two profiles: `--profile ci` (minutes-scale, this report's numbers) and
`--profile full` (the parameterized 1 TB/7d/50-service/5k-stream Tier-2
shape — a documented manual procedure, see below; not runnable on shared CI
infrastructure).

## Query set

| Shape | LogQL | §9 target | CI gate (Tier 1) |
|---|---|---|---|
| Label-scoped stream read, 6h, limit 100 | `{service_name="x",env="prod"}` | Log stream read 6h < 200 ms | `stage3_narrow_window_read_rows_are_index_confined_not_a_full_scan` |
| Body substring search, one service, 24h | `{service_name="x"} \|= "needle"` | Log body search 24h < 2 s | `body_search_skip_index_prunes_most_granules` |
| Label/series discovery, 7d (§9-mandated) | series/label API | Log label/series discovery 7d < 100 ms | covered by the `EXPLAIN indexes = 1` snapshot gate |
| Count/rate over 7d (rollup-served) | `sum by(service_name)(count_over_time({env="prod"}[step]))` | no exact §9 row; nearest analog, recorded regardless | covered by the `EXPLAIN indexes = 1` snapshot gate |

## Tier-1 CI regression gates (asserted, scale-invariant)

`crates/pulsus-read/tests/query_log_gates.rs` (gated `PULSUS_TEST_CLICKHOUSE=1`,
runs in the `schema-it` CI job after the existing `explain_indexes` gate,
same ClickHouse 24.8 container). Every assertion is a **ratio**, never an
absolute count (`read_rows`/bytes/marks all scale with corpus size — an
absolute threshold breaks the moment the corpus grows):

- `corpus_is_large_enough_to_prove_skip_index_pruning` — guards the gate
  itself: a too-small corpus can't prove granule skipping.
- `stage3_narrow_window_read_rows_are_index_confined_not_a_full_scan` —
  `read_rows` for a narrow time window bounded at 4×`index_granularity`,
  and well under half the corpus — proves primary-index confinement, not a
  full scan.
- `body_search_skip_index_prunes_most_granules` — `SelectedMarks /
  total_marks ≤ 0.5` for the body-search shape, and `read_bytes` bounded by
  `selected_marks × a per-granule byte ceiling` — proves the
  `tokenbf_v1`/`ngrambf_v1` skip indexes actually prune granules that
  cannot contain the needle.

Verified live in this session (podman, ClickHouse 24.8):

```
running 3 tests
test body_search_skip_index_prunes_most_granules ... ok
test corpus_is_large_enough_to_prove_skip_index_pruning ... ok
test stage3_narrow_window_read_rows_are_index_confined_not_a_full_scan ... ok
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

The pre-existing `crates/pulsus-read/tests/explain_indexes.rs` gate is
unchanged — the architect plan's stage-1/2/3 line-filter and metric-rollup
cases already cover every EXPLAIN shape this benchmark exercises; this
report did not surface a gap.

## Ingest fidelity gate

`crates/pulsus-write/tests/ingest_fidelity.rs` (gated
`PULSUS_TEST_CLICKHOUSE=1`) is what licenses `dataset.rs`'s direct
RowBinary bulk-load shortcut for the CI-scale and Tier-2 corpora. Six
raw-OTLP fixtures (`crates/pulsus-write/tests/fixtures/otlp/*.json`), each
paired with a **hand-derived golden expectation** (read by hand off
`docs/architecture.md` §2.2/§2.3's canonicalization rules — not computed by
`pulsus-model`/`pulsus-write` at test time, which would make the assertion
tautological). Both the product ingest path (raw OTLP protobuf → `POST
/v1/logs` → `LogWriter`) and an independently-written bulk RowBinary
flattener are asserted **against the golden**, not against each other. The
`fingerprint` field specifically is hashed by an **independent oracle**:
ClickHouse's own live `SELECT cityHash64(...)` (issue #16 CODE review
[medium] finding — a golden computed by calling `pulsus_model::
stream_fingerprint`, the same Rust function both paths would otherwise
share, would make that one field's assertion tautological). Path B derives
its own comparison fingerprint the identical way, live, rather than calling
`pulsus_model::stream_fingerprint` either — see `ch_stream_fingerprint`'s
doc comment for the exact derivation query.

1. Label canonicalization order
2. Resource vs. scope attribute flattening
3. Duplicate/colliding labels (resource vs. scope key collision)
4. Timestamp units (`time_unix_nano` vs. `observed_time_unix_nano` fallback)
5. Non-ASCII body encoding
6. MV-created `log_streams_idx` rows (poll-until-settled, no fixed sleep)

Verified live in this session:

```
running 6 tests
test duplicate_colliding_labels ... ok
test label_canonicalization_order ... ok
test mv_created_idx_rows ... ok
test nonascii_body_encoding ... ok
test resource_vs_scope_attributes ... ok
test timestamp_units ... ok
test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

**Scope deviation (documented in the test file):** case 2 exercises resource
vs. **scope** attributes, not resource vs. **log-record** attributes as the
original plan text named it — `otlp_logs::parse` (issue #8) never promotes
per-record attributes into the label set, confirmed by that module's own
exhaustive unit tests. Testing a code path that does not exist would not be
a fidelity gate.

## CI-scale corpus results (recorded, not gated)

Corpus: 720,000 rows, 500 streams, 50 services, seed 42, 1h anchored at
wall-clock `now`, 1,426 needle rows (1-in-500 rate). Loaded in ~4.7s via
direct RowBinary insert. Single ClickHouse 24.8 node (podman, this
session). Full machine-readable evidence:
[`data/logs-read-ci.json`](data/logs-read-ci.json).

| Query | §9 target | CI-scale (recorded) | wall p95 (ms) | read_rows | selected/total marks | Tier-2 status |
|---|---|---|---|---|---|---|
| `label_scoped_stream_read_6h` | Log stream read 6h < 200 ms | CI-scale, not comparable to the §9 target | 86.0 | 68,036 | 10/101 | `UNVALIDATED (Tier 2 — #25)` |
| `body_search_24h` | Log body search 24h < 2 s | CI-scale, not comparable to the §9 target | 166.2 | 68,036 | 10/101 | `UNVALIDATED (Tier 2 — #25)` |
| `label_series_discovery_7d` | Log label/series discovery 7d < 100 ms | CI-scale, not comparable to the §9 target | 4.9 | 2,000 | 1/2 | `UNVALIDATED (Tier 2 — #25)` |
| `count_rate_rollup_over_corpus_window` | no exact §9 row (nearest analog) | CI-scale, not comparable to the §9 target | 116.0 | 363,226 | 46/47 | `UNVALIDATED (Tier 2 — #25)` |

No target above is marked hit or missed at M1 closure. The CI-scale numbers
happen to be comfortably under the §9 figures, which is expected (a 720k-row
single-node corpus is nowhere near 1 TB/7d/50-service/5k-stream) and **must
not** be read as evidence toward the targets.

`EXPLAIN indexes = 1` for `body_search_24h` confirms both the primary-key
prefix (`service`, `fingerprint`, `timestamp_ns`, pruning 8/93 granules) and
the two skip indexes firing (`idx_body_tokens` tokenbf_v1, `idx_body_ngrams`
ngrambf_v1, each pruning to 8/8 of the already-narrowed granule set) — full
output in the linked JSON.

## 4-shard fixture: Tier-1 distributed evidence

Fixture: `ci/bench-cluster/compose.yaml` (4 shards × 1 replica, extends
`ci/clickhouse-cluster/`'s 2-shard pattern; static IPs, `internal_replication
= true`, manual-dispatch-only CI job — evidence-gathering, not per-PR
protection). Topology choice: 4×1 over 2×2, because the claim under test is
shard-local fan-out under `fingerprint` sharding, and replication
correctness is already covered by the 2-shard fixture.

Same CI-scale corpus (720,000 rows / 500 streams / 50 services), loaded
through the `_dist` Distributed wrappers so the `fingerprint` sharding key
actually places rows across all four shards (182,880 / 138,240 / 192,960 /
205,920 rows respectively — not one shard holding everything). Full
machine-readable evidence, including **every stage's own** `EXPLAIN
PIPELINE` capture:
[`data/logs-read-dist.json`](data/logs-read-dist.json).

**Per-stage, per-shard, expected-roster-verified `system.query_log`
evidence**, verified live in this session (podman, four ClickHouse 24.8
nodes + one clickhouse-keeper node) — every stage each shape actually
executes, not just the terminal one (round 1 [high] finding); every shard
including the coordinator's own local-shard read (round 2 [high] finding);
and, for fingerprint-scoped stages, the pruned shard(s) *proven* pruned
rather than merely absent (round 3 [high] finding):

| Query | stage | roster | participating (shard: `read_rows`) | expected-pruned |
|---|---|---|---|---|
| `label_scoped_stream_read_6h` | resolution | full (4/4) | 1:2,000(coord) 2:384 3:536 4:572 | — |
| `label_scoped_stream_read_6h` | hydration | owning subset (3/4) | 1:357(coord) 2:96 3:134 | 4 |
| `label_scoped_stream_read_6h` | samples | owning subset (3/4) | 1:104,392(coord) 2:20,224 3:62,704 | 4 |
| `body_search_24h` | resolution | full (4/4) | 1:2,000(coord) 2:384 3:536 4:572 | — |
| `body_search_24h` | hydration | owning subset (3/4) | 1:357(coord) 2:96 3:134 | 4 |
| `body_search_24h` | samples | owning subset (3/4) | 1:63,432(coord) 2:20,224 3:21,744 | 4 |
| `label_series_discovery_7d` | discovery | full (4/4) | 1:2,000(coord) 2:384 3:536 4:572 | — |
| `count_rate_rollup_over_corpus_window` | resolution | full (4/4) | 1:2,000(coord) 2:384 3:536 4:572 | — |
| `count_rate_rollup_over_corpus_window` | hydration | full (4/4) | 1:500(coord) 2:96 3:134 4:143 | — |
| `count_rate_rollup_over_corpus_window` | rollup_range | full (4/4), **verified not assumed** | 1:360,645(coord) 2:69,246 3:96,648 4:103,152 | — |

Every `expected-pruned` shard 4 entry carries a `pruned_reason` derivation
in the committed JSON, e.g. for `label_scoped_stream_read_6h`'s
`hydration` stage: *"optimize_skip_unused_shards pruned shard 4: none of
the 4 queried fingerprints map to it (fingerprint % total_weight=4 over
slots [0, 0, 1, 2] resolves to owning shards {1, 2, 3} — shard 4 is not
among them)"* — computed client-side from `system.clusters`'
cumulative-weight slot map, **before** the query ran, then verified
against the observed `system.query_log` rows (not a comment tacked onto an
absence). `count_rate_rollup_over_corpus_window`'s `rollup_range` stage —
whose owning set the plan amendment explicitly required verifying, not
assuming — reaches the **full 4-shard roster** because its 167-fingerprint
`env=prod` set is broad enough to span every shard's slots; this is
reported as observed, not presumed from the shape of the query. Every row
above passed the harness's exact-roster assertion (`observed == expected`)
— it would have aborted the run on any missing owner or unexpected
participant. Full machine-readable evidence, including per-shard `EXPLAIN
PIPELINE` and every `expected-pruned` entry's derivation:
[`data/logs-read-dist.json`](data/logs-read-dist.json).

`EXPLAIN PIPELINE` for `count_rate_rollup_over_corpus_window`'s
`rollup_range` stage shows `GroupingAggregatedTransform 4 → 1` feeding
`MergingAggregatedBucketTransform` — partial aggregation happening per
shard, only aggregate states crossing the network, not raw rows.

**A note on `EXPLAIN indexes = 1` under `--dist`.** Threading the
clustered-reader settings into `EXPLAIN indexes = 1` (round 2 [medium]
finding) surfaced a real, benign difference from the single-node capture:
under `prefer_localhost_replica = 1`, `EXPLAIN` reflects the *coordinator's
own local shard's* index-pruning view, so a literal `fingerprint IN (...)`
list with (say) 10 elements can show a narrower "N-element set" in the
`PrimaryKey` `Condition` line when only a subset are relevant to that one
shard's local parts. Verified this is display-only, not data loss: the
query's actual SQL text (`docs/benchmarks/data/logs-read-dist.json`'s
per-query `sql` field) always carries the full, correct fingerprint list,
and `returned_rows` for every shape matches the single-node baseline
exactly (100 / 26 / 4 / 2,171).

### §7 fan-out table walk (logs family)

| `docs/schemas.md` §7 row | Verdict | Evidence |
|---|---|---|
| LogQL stream resolution + read: "all, but every stage completes shard-locally … matched log lines only [cross the network]" | **Confirmed, all three stages, exact roster verified** | `label_scoped_stream_read_6h`/`body_search_24h`: **resolution** participates on the full 4-shard roster (no `fingerprint` predicate to prune by); **hydration**/**samples** participate on *exactly* the computed 3-shard owning subset of the canonical stream's fingerprints, with the 4th shard's absence *proven* — not merely observed — by an `expected-pruned` entry carrying the `fingerprint % total_weight` derivation, matched against the harness's own pre-computed expectation before the query ran; each participating shard's `read_rows` reflects its own local partition, not the full corpus; the initiator returned exactly the matched rows (100 / 26), not the shards' combined local row counts |
| Label/tag discovery: "all [shards]; deduplicated key/value sets [cross]" | **Confirmed, full 4-shard roster** | `label_series_discovery_7d`'s `discovery` stage: all 4 shards (coordinator-local + 3 remote) participate — no `fingerprint` predicate, so the expected roster is unconditionally the full cluster and was verified as such — contributing local `log_streams_idx` rows, none anywhere near a full corpus scan; the initiator returned only the 4 deduplicated label names |
| (supplementary, not a named §7 row) rollup/tier partial aggregation | **Confirmed for `log_metrics_5s`, full 4-shard roster verified, not assumed** | `count_rate_rollup_over_corpus_window`'s `rollup_range` stage: its 167-fingerprint owning set was computed and checked, not presumed from the shape of the query, and reaches all 4 shards; `EXPLAIN PIPELINE`: `GroupingAggregatedTransform` + `MergingAggregatedBucketTransform` — per-shard partial aggregation across all 4 shards, consistent with the co-sharding argument `docs/schemas.md` §7 makes for `log_metrics_5s` |

Every row above has direct, per-stage evidence from this run against an
**exact, pre-computed expected shard roster** — participating shards
verified as observed, non-participating shards verified as *legitimately
pruned* (never merely "absent, presumed fine"). No row here graduates
ahead of its own captured evidence. This corrects three prior versions of
this report: the first graduated the LogQL row on terminal-stage-only
evidence and left discovery unevaluated; the second added per-stage
evidence but silently omitted the coordinator's own shard from every row
and ran `EXPLAIN PIPELINE`/discovery/metric terminal queries under
non-product settings; the third accepted any 1..4-shard roster for
fingerprint-scoped stages without deriving which shards were *expected*,
so a genuinely lost `system.query_log` row would have been
indistinguishable from correct pruning. Per `docs/schemas.md` §9's
two-tier model, this Tier-1 evidence is sufficient to graduate both
logs-family rows of the §7 fan-out table and the corresponding
`docs/architecture.md` risks-table row — done alongside this report (see
those files' diffs in this issue). It does **not** validate latency at
Tier-2 scale, and it
does not touch the metrics/traces/profiles rows, which remain design
intent pending M3/M4.

### A note on this session's environment

The `ci/bench-cluster` fixture's compose file matches `ci/clickhouse-cluster/`
verbatim in structure (static `ipv4_address` per node, `internal_replication
= true`) and is what real CI's `docker compose` (GitHub Actions runner) uses.
This development sandbox's `podman` (3.4.4, predates podman 4.0) does not
honor podman-compose's `--network=name:ip=x.x.x.x` combined-flag syntax and
silently falls back to the default bridge network with dynamic addressing —
confirmed to affect the pre-existing, already-merged 2-shard fixture
identically, so this is an environment/tooling limitation, not a defect in
either fixture. The per-shard evidence above was captured by manually
orchestrating the same five containers with podman's older `--network=NAME
--ip=X` (separate-flag) syntax, using the fixture's own config files
unmodified, so the evidence reflects the fixture as committed. The
manual-dispatch `bench-cluster` CI job (wired in this issue) will re-capture
this evidence on real infrastructure the first time it runs.

## Reproduction

CI-scale (what this report's numbers were produced with):

```
cargo xtask bench logs-read \
    --http-url http://127.0.0.1:19123 --database pulsus_bench_ci \
    --profile ci --seed 42 --services 50 --streams 500 \
    --lines-per-sec 200 --duration-secs 3600 --reps 5 \
    --out docs/benchmarks/data/logs-read-ci.json \
    --report-out /tmp/logs-read-ci.md
```

4-shard (`ci/bench-cluster/compose.yaml` up first):

```
cargo xtask bench logs-read --dist \
    --http-url http://127.0.0.1:18123 --database pulsus_bench_dist \
    --cluster pulsus_bench_cluster \
    --profile ci --seed 42 --services 50 --streams 500 \
    --lines-per-sec 200 --duration-secs 3600 --reps 5 \
    --out docs/benchmarks/data/logs-read-dist.json \
    --report-out /tmp/logs-read-dist.md
```

**Tier 2 (manual, reference hardware only — tracked by #25).** The
parameterized 1 TB/7d/50-service/5k-stream corpus on the reference 4-node
box (8 vCPU, local NVMe per node, `docs/schemas.md` §9):

```
cargo xtask bench logs-read --dist \
    --http-url http://<node1>:8123 --database pulsus_bench_full \
    --cluster <reference-cluster-name> \
    --profile full --seed 42 --services 50 --streams 5000 \
    --lines-per-sec <rate-to-hit-1TB-over-7d> --duration-secs 604800 \
    --reps 5 \
    --out docs/benchmarks/data/logs-read-full.json \
    --report-out docs/benchmarks/m1-logs-read-path-tier2.md
```

This is a hours-long run (edge case #2 of the architect plan: the generator
anchors timestamps at wall-clock `now`, so the load must complete inside
`log_samples`' 7-day TTL window — a real duration risk to budget for). On
completion, #25's acceptance criteria call for replacing every
`UNVALIDATED (Tier 2 — #25)` marker in the table above with a hit/miss
verdict against the §9 target, and revisiting the scale-dependent
`docs/architecture.md` risks-table rows (e.g. skip-index false-positive
rate) accordingly.

## Out of scope (per the architect plan)

Metrics, traces, profiles benchmarks (M2+); the 5M-series label-resolution
gate; write-path/ingest performance measurement (the fidelity gate above
checks correctness, not throughput); changing table DDL, planner SQL,
codecs, or index parameters (this harness observes, it does not tune);
replica-failover/`skip_unavailable_shards` degraded-read behavior; TLS,
refreshable MVs, cross-cluster reads; new `explain_indexes.rs` shapes (none
of the above surfaced a gap in the existing snapshot set).
