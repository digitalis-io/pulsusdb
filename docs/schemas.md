# PulsusDB ClickHouse Schemas

The authoritative storage design. Every table is specified with its full DDL, the queries it exists to serve, and the read path those queries take — including the SQL PulsusDB generates. [architecture.md](architecture.md) summarizes these decisions; this document is the reference the schema controller implements.

**The optimization target is read latency**: dashboard panels, log searches, and trace lookups must be index-served, bounded, and shard-local wherever possible. Ingestion adapts to the schema, never the reverse.

---

## 1. Why not the first-generation layout

ClickHouse observability layers of the first generation typically share one shape: a single generic samples table for logs *and* metrics, a label inverted index consulted before every read, JSON labels reconstructed at query time, and distributed tables sharded for write convenience. That shape has well-understood failure modes, which this design treats as requirements to engineer away:

| # | Failure mode in first-generation schemas | PulsusDB design response |
|---|------------------------------------------|--------------------------|
| 1 | Every label query is a **two-stage lookup** (label index → fingerprint set → samples); high-cardinality selectors create huge intermediate fingerprint sets | Metrics: label resolution moves to an in-process cache; SQL receives a bounded, sorted `fingerprint IN` list, or a JOIN fallback past a threshold. Logs/traces: intersections run *inside* the index table as a single `GROUP BY ... HAVING` pass, and the planner starts from the most selective matcher with hard caps |
| 2 | Label index distributed tables sharded by **`rand()`** — every label lookup broadcasts to all shards and nothing joins shard-locally | All index tables are **co-sharded with their data tables** (same sharding key). Intersections, joins, and per-series aggregation execute shard-locally; only reduced results cross the network |
| 3 | **No log-body text index** — `|= "substring"` scans every log line in range | `tokenbf_v1` + `ngrambf_v1` skip indexes on the body column; line filters compile to `hasToken`-style predicates that skip non-matching granules |
| 4 | Samples **ordering key is configurable** — a wrong choice silently destroys either per-series reads or time scans | Ordering keys are **fixed** per table and chosen from the dominant query shape. No deployment-time ordering knobs |
| 5 | Trace payload table ordered only for **trace-ID fetch**; service + time search depends entirely on the attribute index | `trace_spans` carries a **projection** physically ordered by `(service, timestamp_ns)` — both access patterns are primary-index reads on the same table |
| 6 | **Per-day series metadata** — a 30-day query touches 30 index partitions and re-reads the same series 30 times | Series and label-index tables partition **monthly**; a 30-day query touches 1–2 partitions and each series appears once or twice |
| 7 | **Regex and negative matchers** fall off the `(key, val)` index | Metrics: matched in-process against cached labels (regex is a RAM problem, not a scan problem). Logs/traces: regex/negative matchers evaluated over the *values of one key* (a single index prefix range), never over raw data |
| 8 | **Generated SQL quality** ignored — no `PREWHERE`, no rollup routing, repeated index scans, coordinator-only aggregation | The planner is specified alongside the schema (§ per signal): time and low-cardinality predicates ride in `PREWHERE`, rollups are routed automatically, every intersection is a single pass, aggregation is pushed to shards |
| 9 | **One generic data model** for metrics and logs despite opposite query patterns | Four signals, four schemas. Nothing is shared but conventions |

Fixes proposed elsewhere that this design deliberately **rejects**, and why:

- **`tokenbf_v1` on the labels JSON column** — duplicates the label index's job with false positives and storage cost, and doesn't help the second-stage sample read. Bloom indexes go on the log *body*, where there is no better structure.
- **`Map(String, String)` label columns** — measurably slower to extract from than JSON strings in ClickHouse, and PulsusDB rarely extracts labels in SQL at all (labels resolve in-process or from a hydration read).
- **Materializing arbitrary user labels onto every sample row** — repeats low-cardinality values billions of times and ties the schema to today's queries. The single exception is `service` (§3.2): OpenTelemetry guarantees it exists, it has ideal clustering properties, and the planner can *always* derive it — so it earns a place in the ordering key. No other label gets one.
- **`minmax` skip indexes on unclustered string columns** — near-zero granule skipping unless data is physically clustered by that column; where we need that clustering we buy it explicitly (ordering key or projection).
- **`ReplacingMergeTree` for sample data** — merge-time dedup forces `FINAL` or wrong results. Sample tables are plain `MergeTree`; only metadata tables use `ReplacingMergeTree`, and their read shapes (`LIMIT 1 BY`, `GROUP BY`) are duplicate-tolerant by construction.

Conventions used below: `<db>` defaults to `pulsus`; in clustered mode every table becomes `Replicated*` with a `_dist` Distributed wrapper (§7); `retention` clauses show defaults (`PULSUS_RETENTION_DAYS = 7`). Label keys follow the canonical label model ([architecture.md §2.3](architecture.md)): log/metric label keys are normalized at ingest (`service.name` → `service_name`, before fingerprinting); trace attribute keys are stored verbatim; the promoted physical column is named `service` on the logs, traces, and profiles tables (metrics deliberately have none — reads there are `metric_name` + `fingerprint` driven). **Every DDL block and generated-SQL example in this document is executable and is run against a fresh ClickHouse in CI from M0 onward** — an unrunnable snippet is a build failure, not a docs bug. Latency figures in §9 are targets to validate, not guarantees.

---

## 2. Metrics

**Query shapes served:** instant/range PromQL over one metric with label selectors (dominant); label/series discovery; long-range dashboards (30d+); high-frequency `count by` meta-queries.

The schema's PromQL obligation is **fetch shapes only**: full PromQL evaluation (all functions, operators, subqueries) happens in the engine against the columns below — never in ClickHouse SQL — so language coverage is independent of the schema ([architecture.md §5.1](architecture.md)). One planned extension: **native histogram samples get dedicated storage in M7** (a histogram-typed samples table or serialized sparse-histogram column, designed in that milestone); until then OTLP exponential histograms flatten to classic `_bucket`/`_sum`/`_count` series at ingest.

### 2.1 Tables

```sql
CREATE TABLE metric_samples (
    metric_name  LowCardinality(String),
    fingerprint  UInt64   CODEC(Delta(8), ZSTD(1)),
    unix_milli   Int64    CODEC(DoubleDelta, ZSTD(1)),
    value        Float64  CODEC(Gorilla, ZSTD(1))
) ENGINE = MergeTree
PARTITION BY toDate(fromUnixTimestamp64Milli(unix_milli))
ORDER BY (metric_name, fingerprint, unix_milli)
TTL toDateTime(fromUnixTimestamp64Milli(unix_milli)) + INTERVAL 7 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;
```

- **`metric_name` leads the key.** PromQL queries name their metric; the primary index immediately confines the read to that metric's granules. `LowCardinality` makes the leading column nearly free to store and filter.
- **`fingerprint` second** clusters each series contiguously within its metric → per-series reads (every PromQL evaluation) are sequential scans of a few granules.
- **Daily partitions** on the raw table: retention drops whole partitions (`ttl_only_drop_parts`), and time predicates prune partitions before the index is even consulted.
- **No string data.** The fetch hot path moves only `(UInt64, Int64, Float64)` columns.
- **Resolution-agnostic.** Sample timestamps are stored **verbatim at millisecond precision** — never quantized, bucketed, or aligned. PulsusDB assumes nothing about the source scrape/export interval: 1s, 15s, 5m, or irregular push cadences all land as-is, per-series intervals may differ and drift, and the PromQL engine derives actual intervals from the data (as Prometheus does for extrapolation and staleness) rather than from configuration. Rollup tiers (§2.2) are *optional derived data* at operator-chosen resolutions; they never constrain or replace what raw ingestion accepts.

```sql
CREATE TABLE metric_series (
    metric_name  LowCardinality(String),
    fingerprint  UInt64  CODEC(Delta(8), ZSTD(1)),
    unix_milli   Int64   CODEC(Delta(8), ZSTD(1)),   -- hour-bucketed "last active"
    labels       String  CODEC(ZSTD(5))              -- canonical JSON, sorted keys
) ENGINE = MergeTree
PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(unix_milli))
ORDER BY (metric_name, fingerprint, unix_milli);
```

- Written once per series per **activity bucket** (`PULSUS_SERIES_ACTIVITY_BUCKET`, default `1h`): the writer floors `unix_milli` to the bucket and skips known `(metric_name, fingerprint, bucket)` triples via an in-process LRU — **metric-name-scoped**, not `(fingerprint, bucket)` alone: `metric_fingerprint` (§2.1's fingerprint function) excludes `__name__`, so two differently-named metrics sharing the same label set share a fingerprint, and a name-less key would let one metric's registration false-hit-suppress the other's `metric_series` row. Natural duplicates collapse at read time with `LIMIT 1 BY metric_name, fingerprint` — **no `ReplacingMergeTree`, no `FINAL`**.
- **Size the activity bucket to cardinality.** Rows/month ≈ active series × (30d ÷ bucket). At 5M continuously active series, hourly buckets produce ~3.6B metadata rows/month; a `1d` bucket produces ~150M — the recommended setting at multi-million-series scale. Coarser buckets are always *logically safe*: the bucket-floored read bounds (§2.1 lookup SQL, rendered from the same config constant the writer uses) can over-include series adjacent to the query window — they match no samples — but can never miss one. They are not computationally free, though: a 10-minute historical query against a `1d` bucket drags that whole day's series for the metric through label matching. Bucket size is therefore part of the label-resolution benchmark below, not just a storage knob. **This is not just an internal read-path detail:** it is the documented, deliberate contract for the discovery endpoints built on this table (`/api/v1/series`, `/labels`, `/label/{name}/values` for historical windows, docs/api.md §3.3) — their result set is the bucket-granularity active set, a bounded superset of Prometheus's exact-sample-window set (never a subset — over-inclusion is bounded by activity-bucket size, and it is never a false empty).

#### Label resolution at scale — the strategy ladder

At the design-target cardinality (millions of active series), **label resolution — not sample reads — is the metrics path's primary risk**. The sample table stays strong for any bounded fingerprint set; the question is what produces that set. Three paths, benchmarked separately against the 5M-series scale corpus:

1. **Cache matcher (hot path):** in-process evaluation over the active window. Scaling concern: the refresh sweep (`LIMIT 1 BY` over millions of rows every `PULSUS_CACHE_TTL`) — if the M2 benchmark shows it unsustainable, the planned evolution is **incremental refresh**: sweep only activity buckets newer than the last refresh, merging into the resident map (the schema already supports this; it is a reader change only).
2. **SQL fallback (historical windows, selectors past `PULSUS_CACHE_MAX_SERIES`):** the §2.1 lookup — `JSONExtractString` matching over `metric_series`, scoped by `metric_name` and activity bucket. This is exactly the first-generation pain shape, *bounded by metric scope*; it is affordable when a metric's cardinality is modest and potentially dominant when one metric carries millions of series. Broad selectors at scale will hit this path routinely — it is not assumed rare.
3. **Optional inverted label index (`metric_series_idx`) — spec'd now, created only on benchmark evidence:**

   ```sql
   CREATE TABLE metric_series_idx (
       bucket       Int64,                    -- same activity bucket as metric_series
       metric_name  LowCardinality(String),
       key          LowCardinality(String),
       val          String,
       fingerprint  UInt64
   ) ENGINE = ReplacingMergeTree
   PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(bucket))
   ORDER BY (metric_name, key, val, bucket, fingerprint);
   -- populated by MV over metric_series (ARRAY JOIN over label pairs)
   -- co-sharded with the metrics family: cityHash64(metric_name, fingerprint)
   ```

   Resolution uses the same single-pass shape as logs — equality matchers as `(metric_name, key, val)` prefix reads intersected via `GROUP BY fingerprint HAVING`, regex matchers over one key's value range — but **metric-scoped** (the prefix always starts with `metric_name`, unlike a global gin) and co-sharded so fingerprint sets are shard-local to their samples. Real write/storage cost: one row per label pair per series per bucket.

**Decision gate (M2/M3):** all three paths are benchmarked on the scale corpus — cache matcher latency + refresh cost, SQL-fallback latency across metric cardinalities (10k / 500k / 5M series per metric), and a prototype `metric_series_idx`. The index ships only if the SQL fallback misses the §9 latency targets on realistic broad selectors; the gate's outcome (either way) is recorded with the benchmark evidence.
- Feeds the reader's **label cache**: `fingerprint → labels` + `metric_name → [fingerprints]`, refreshed every `PULSUS_CACHE_TTL` over the active window (`PULSUS_CACHE_WINDOW`, default 24h). Matchers — including regex and negative matchers — evaluate against this map in-process (finding #7).
- **The cache is time-scoped**: it may answer only queries whose data window lies inside the cache window. A series alive last week but silent today is absent from the cache, so answering a historical query from it would return false empties. Older ranges resolve directly from this table with **hour-bucket-aware bounds** — `unix_milli` is bucketed, so the lower bound must be floored to the hour (a series emitting at 10:35 has a 10:00 row; a 10:30–10:40 query with a raw `>= 10:30` bound would miss it) and the upper bound must exclude series first seen after the query window:

  ```sql
  SELECT fingerprint, labels
  FROM metric_series
  WHERE metric_name = {name}
    AND unix_milli >= intDiv({data_start}, {bucket_ms}) * {bucket_ms}   -- {bucket_ms} rendered from
    AND unix_milli <= intDiv({data_end},   {bucket_ms}) * {bucket_ms}   -- PULSUS_SERIES_ACTIVITY_BUCKET (default 3600000)
  ORDER BY unix_milli DESC
  LIMIT 1 BY metric_name, fingerprint
  ```

  The `metric_name`-first ordering makes this a metric-scoped scan of 1–2 monthly partitions. Correctness tests cover sub-hour historical windows and series appearing only after the query end.

```sql
CREATE TABLE metric_metadata (
    metric_name  LowCardinality(String),
    metric_type  LowCardinality(String),   -- counter | gauge | histogram | summary
    help         String,
    unit         String,
    updated_ns   Int64
) ENGINE = ReplacingMergeTree(updated_ns)
ORDER BY metric_name;
```

`metric_type` also drives the planner: counter functions on rollup tiers are only legal because the type is known. **`updated_ns` is the `ReplacingMergeTree` version column** (issue #26 fix, mirroring `log_streams`' `ReplacingMergeTree(updated_ns)`): every non-key column here (`metric_type`/`help`/`unit`) sits outside `ORDER BY metric_name`, so without a version column a merge's latest-wins outcome would be nondeterministic — unacceptable given `metric_type` drives planner correctness. The writer emits a new row (receiver-injected `now_ns`) only when the incoming `(metric_type, help, unit)` tuple differs from the last value it durably emitted for that `metric_name` (a bounded last-value cache, success-only promoted on a confirmed flush) — idempotent on repeats, and a type change that later reverts (A→B→A) re-emits on the second A rather than being suppressed by a static once-only registration.

**`metric_metadata.metric_name` is keyed by the BASE family name, never a derived-series name** (issue #27 architect plan, task-manager-pinned docs contract). A receiver that flattens one metric descriptor into several physical series — a histogram's `<name>_bucket`/`<name>_sum`/`<name>_count`, an exponential histogram's identical shape, or a summary's quantile series plus `<name>_sum`/`<name>_count` — registers exactly **one** `metric_metadata` row for `<name>` itself, typed `histogram`/`summary`, never one row per suffixed series. **Any consumer resolving a metric family's type must strip a trailing `_bucket`, `_sum`, or `_count` suffix (and, for a Summary's quantile series, no suffix at all — the quantile series shares the base name verbatim, distinguished only by its `quantile` label) before looking the family up in `metric_metadata`.** This is the contract issue #30 (label cache)/#31/#32 (PromQL planner, counter-function legality, rollup eligibility) implement against — not tribal knowledge. A lookup that fails to strip suffixes will find no metadata row for `<name>_bucket`/`<name>_sum`/`<name>_count` at all (they were never registered under those names) and must not misinterpret that absence as "unknown metric".

### 2.2 Downsampling tiers

Downsampling happens **entirely inside ClickHouse** with classic insert-triggered materialized views — no external driver, no scheduled jobs. One table per tier (`metric_samples_5m`, `metric_samples_1h`); monthly partitions, long TTLs:

```sql
CREATE TABLE metric_samples_5m (
    metric_name   LowCardinality(String),
    fingerprint   UInt64                                 CODEC(Delta(8), ZSTD(1)),
    ts            DateTime                               CODEC(DoubleDelta, ZSTD(1)),
    val_min       SimpleAggregateFunction(min, Float64)  CODEC(Gorilla, ZSTD(1)),
    val_max       SimpleAggregateFunction(max, Float64)  CODEC(Gorilla, ZSTD(1)),
    val_sum       SimpleAggregateFunction(sum, Float64)  CODEC(Gorilla, ZSTD(1)),
    val_sum_sq    SimpleAggregateFunction(sum, Float64)  CODEC(Gorilla, ZSTD(1)),
    val_count     SimpleAggregateFunction(sum, UInt64)   CODEC(T64, ZSTD(1)),
    first_time    SimpleAggregateFunction(min, Int64)    CODEC(DoubleDelta, ZSTD(1)),
    last_time     SimpleAggregateFunction(max, Int64)    CODEC(DoubleDelta, ZSTD(1)),
    first_value   AggregateFunction(argMin, Float64, Int64),
    last_value    AggregateFunction(argMax, Float64, Int64)
) ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (metric_name, fingerprint, ts)
TTL ts + INTERVAL 90 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW metric_samples_5m_mv TO metric_samples_5m AS
SELECT metric_name, fingerprint,
       toStartOfInterval(fromUnixTimestamp64Milli(unix_milli), INTERVAL 300 SECOND) AS ts,
       min(value) AS val_min, max(value) AS val_max, sum(value) AS val_sum,
       sum(value * value) AS val_sum_sq, count() AS val_count,
       min(unix_milli) AS first_time, max(unix_milli) AS last_time,
       argMinState(value, unix_milli) AS first_value,
       argMaxState(value, unix_milli) AS last_value
FROM metric_samples
GROUP BY metric_name, fingerprint, ts;
-- metric_samples_1h_mv: identical shape, INTERVAL 3600, also reading metric_samples
```

- **Insert-triggered, additive, real-time.** Every aggregate above is a mergeable state, so per-block partial aggregates from any insert order converge under `AggregatingMergeTree` merges — correctness needs no windowing, no refresh schedule, and no "wait for compaction" lag. Tiers are populated to within one insert batch of `now()`, so a tier can serve the *entire* time range of a query. Both tier MVs read the raw insert block directly (no MV-on-MV chaining).
- **Counter resets are handled at query time, from bucket boundaries — and this is an approximation whenever a reset falls inside a bucket.** A per-sample reset-corrected delta cannot be computed inside an incremental MV (an insert block doesn't reliably contain each sample's predecessor). `rate`/`increase` fetch per-bucket `(first_time, last_time, first_value, last_value)` and the engine reconstructs increase over the bucket sequence: intra-bucket `last − first` (a drop marks a reset → contribute `last`), plus boundary deltas against the previous bucket's `last_value` with the same rule — O(buckets) work. **Accuracy caveat, stated precisely:** *any* reset inside a bucket loses information. `100,150,10,40` reconstructs 40 where the true increase is 90; worse, `100,150,10,140` reconstructs 40 where the truth is 190 and no reset is even detectable from boundaries. This is why counter functions **prefer raw samples wherever raw exists** (§2.3 tier policy) and why tier-served counter segments are always flagged approximate. The M3 accuracy report must cover single-reset-in-bucket and undetectable-reset cases, not just multiple resets.
- **Gauge pushdown is a specific function list, not "all of them":** `avg/min/max/sum/count_over_time` from `val_sum/val_min/val_max/val_count`, `stddev/stdvar_over_time` from `val_sum_sq`, `last_over_time` from `last_value`, `present_over_time` from `val_count > 0`. Functions needing sample positions or full distributions (`quantile_over_time`, `mad_over_time`, `changes`, `resets`, ...) route to raw. Tiered gauge results are **bucket-aligned**: a window whose edge falls inside a bucket includes that whole bucket — exact only when windows align with bucket boundaries, otherwise a defined approximation (flagged via `X-Pulsus-Explain`).
- **Late and duplicate data.** Aggregate-state merging makes insert *order* irrelevant, but not insert *multiplicity*: a replayed remote-write batch inflates `val_sum`/`val_count` in tiers permanently, and late samples mutate buckets that earlier queries already read. Policy: the raw read path dedups `(fingerprint, timestamp)` at query time (last write wins), tiers cannot — this is a documented tier-accuracy caveat, measured in M3 with deliberate duplicate/late-data injection. Writer batch atomicity keeps the common path duplicate-free.
- The schema controller only issues DDL: it creates tier tables + MVs, recreates an MV when its config checksum changes, and offers a one-shot chunked `INSERT ... SELECT` backfill when a tier is first enabled on pre-existing data. Nothing runs on a timer. Note the write-cost consequence: every raw insert block is aggregated twice (once per tier MV); the M3 benchmark measures insert throughput and part counts with tiers on and off.

### 2.3 Read paths (generated SQL)

**`rate(http_requests_total{job="api", status=~"5.."}[5m])`, 24h window, 60s step.** The label cache resolves both matchers (regex included) in-process → sorted fingerprints. One fetch:

```sql
SELECT fingerprint, unix_milli, value
FROM metric_samples
PREWHERE metric_name = 'http_requests_total'
WHERE unix_milli >  {start - 300000 - lookback}
  AND unix_milli <= {end}
  AND fingerprint IN (101, 205, 990, ...)
ORDER BY fingerprint, unix_milli
```

Partition pruning (daily) → primary-index pruning (metric, then fingerprints) → sequential per-series reads. Evaluation (extrapolation, resets, staleness) happens in the engine, series-first. Fingerprint lists ≥ 500 split into parallel chunk fetches; selectors matching more than `PULSUS_CACHE_MAX_SERIES` fall back to:

```sql
... AND fingerprint IN (
    SELECT fingerprint FROM metric_series
    WHERE metric_name = 'http_requests_total'
      AND JSONExtractString(labels, 'job') = 'api'
      AND match(JSONExtractString(labels, 'status'), '^(?:5..)$')
)
```

**Clustered honesty:** on a clustered deployment this fallback fetch reads `_dist` names throughout — `metric_samples_dist`, and the nested subquery's `metric_series_dist` — and additionally injects `distributed_product_mode = 'local'`, rewriting that nested subquery to each shard's **local** `metric_series` table (exact under `metric_samples`/`metric_series`'s shared `cityHash64(metric_name, fingerprint)` co-sharding, §7; the same rewrite already applied to the traces metrics semi-join). Without it, ClickHouse's default `distributed_product_mode = 'deny'` rejects the nested `_dist`-inside-`_dist` shape as a double-distributed `IN` (`DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED`).

**Same query over 30 days, step 1h.** Tier eligibility requires `tier.resolution ≤ step` *and* `tier.resolution ≤ the range-vector window` (a 5m-window `rate` can never be answered from 1h buckets, whatever the step). Routing then follows `PULSUS_TIER_POLICY`:

- **`exact` (default):** raw samples are used wherever raw still exists — tiers serve only the range beyond raw retention. With 7-day raw retention, the plan is a two-segment `UNION ALL`: `metric_samples_1h` for `[30d ago, 7d ago)` (approximate, flagged), raw `metric_samples` for `[7d ago, now]` (exact, including edge extrapolation and staleness on real samples). **Exactness is per step, not per query:** a step is raw-exact only when its full evaluation window (range-vector window plus lookback) is covered by raw samples; steps whose window straddles the tier/raw boundary draw on approximate buckets and are flagged tier-approximate in `X-Pulsus-Explain` like any tier-served step.
- **`fast`:** any eligible range is served from the tier — one table, one scan, bucket-aligned approximation across the whole range.

```sql
SELECT fingerprint, toUnixTimestamp(ts) * 1000 AS bucket_ts,
       min(first_time) AS ft, max(last_time) AS lt,
       finalizeAggregation(argMinMergeState(first_value)) AS fv,
       finalizeAggregation(argMaxMergeState(last_value))  AS lv
FROM metric_samples_1h
WHERE metric_name = 'http_requests_total'
  AND ts >= {30d ago} AND ts < {7d ago}
  AND fingerprint IN (101, 205, 990, ...)
GROUP BY fingerprint, ts
ORDER BY fingerprint, ts
```

The engine computes reset-adjusted increases over the bucket sequence (§2.2), splices the raw segment's exact evaluation, and applies the sliding window per step. Any response containing a tier-served segment is flagged approximate when `X-Pulsus-Explain` is set.

**`count by (job) (up)`** — answered entirely from the label cache, zero ClickHouse queries, **when the evaluation window lies inside the cache window**; historical evaluations resolve through `metric_series` like any other query.

### 2.4 Native histogram samples

The M7 extension foreshadowed in §2 lands as a **separate, dedicated samples table** — `metric_hist_samples` — storing Prometheus native (sparse) histograms in their integer wire form. Float samples in `metric_samples` (§2.1) are untouched: the float fetch hot path, its EXPLAIN gate, and its migration checksum cannot regress. The two tables share the same identity, ordering, partitioning, TTL, and — in clustered mode — sharding key, so a series' float and histogram samples always co-reside (see co-sharding note below).

```sql
CREATE TABLE metric_hist_samples (
    metric_name        LowCardinality(String),
    fingerprint        UInt64   CODEC(Delta(8), ZSTD(1)),
    unix_milli         Int64    CODEC(DoubleDelta, ZSTD(1)),
    schema             Int8     CODEC(ZSTD(1)),   -- exponential schema (−4..8); −53 = NHCB
    zero_threshold     Float64  CODEC(Gorilla, ZSTD(1)),
    zero_count         UInt64   CODEC(T64, ZSTD(1)),
    count              UInt64   CODEC(T64, ZSTD(1)),
    sum                Float64  CODEC(Gorilla, ZSTD(1)),
    pos_span_offsets   Array(Int32)   CODEC(ZSTD(1)),
    pos_span_lengths   Array(UInt32)  CODEC(ZSTD(1)),
    pos_bucket_deltas  Array(Int64)   CODEC(ZSTD(1)),
    neg_span_offsets   Array(Int32)   CODEC(ZSTD(1)),
    neg_span_lengths   Array(UInt32)  CODEC(ZSTD(1)),
    neg_bucket_deltas  Array(Int64)   CODEC(ZSTD(1)),
    custom_values      Array(Float64) CODEC(ZSTD(1))
) ENGINE = MergeTree
PARTITION BY toDate(fromUnixTimestamp64Milli(unix_milli))
ORDER BY (metric_name, fingerprint, unix_milli)
TTL toDateTime(fromUnixTimestamp64Milli(unix_milli)) + INTERVAL 7 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;
```

- **Identity and access shape are byte-identical to `metric_samples`** (§2.1): `metric_name` leads the key, `fingerprint` clusters each series, `unix_milli` orders within it — same PK/ordering key `(metric_name, fingerprint, unix_milli)`, same daily partitioning, same `ttl_only_drop_parts` retention. Per-series reads are the same sequential granule scans; the codecs on `fingerprint`/`unix_milli` match §2.1 exactly. Timestamps are stored **verbatim at millisecond precision** (§2.1's resolution-agnostic rule).
- **Sparse wire form, lossless for both schemas.** The `schema`, `zero_threshold`, `zero_count`, `count`, `sum` scalars plus the positive/negative span-and-delta arrays are the integer sparse-histogram encoding (`Array(Int32)`/`Array(UInt32)` span offsets/lengths, `Array(Int64)` delta-encoded bucket counts). This is lossless for the standard exponential schema (−4..8) and for NHCB (schema −53, which populates `custom_values` and leaves the negative/zero fields empty). Each array carries `CODEC(ZSTD(1))` (§8's "everything wrapped in `ZSTD(1)` minimum").
- **Co-sharded with floats.** In clustered mode `metric_hist_samples` reuses the Metrics family sharding key `cityHash64(metric_name, fingerprint)` (§7) — the byte-identical expression `metric_samples`, its tiers, and `metric_series` use. Its `_dist` wrapper is `CREATE TABLE metric_hist_samples_dist AS metric_hist_samples ENGINE = Distributed('{cluster}', pulsus, metric_hist_samples, cityHash64(metric_name, fingerprint))`. Consequence: a series' float samples, histogram samples, and `metric_series` metadata land on the **same shard**, so the read path's co-load of both sample types for one series stays shard-local.

**Counter-reset hint (issue #125).** `metric_hist_samples` gains one additive column — never a mutation of the frozen id-23 `CREATE` (the `value_type` precedent below); the `_dist` wrapper gains the cluster-gated twin (migrations 27/28):

```sql
ALTER TABLE metric_hist_samples ADD COLUMN IF NOT EXISTS counter_reset_hint UInt8 DEFAULT 0;
```

`counter_reset_hint` stores the Prometheus per-sample counter-reset hint byte: `0` = unknown, `1` = counter reset, `2` = not a counter reset, `3` = gauge histogram. Pre-#125 rows read back `0` (the `DEFAULT`) — semantically exact (unknown), no data migration. The read path decodes it into the query-time histogram, where it drives the PromQL not-counter/not-gauge/reset-collision annotations and the reset-detection shortcuts. **Ingest writes `0` today:** OTLP exponential-histogram points carry no monotonicity flag and delta temporality is rejected at the ingest seam, so `3` (gauge) is unproducible until a gauge-capable ingest surface lands (issue #140). The column is fixed-width `UInt8` appended to the existing hist SELECT list — same table, same PK/ordering, same granule pruning, no extra round-trips.

**Per-series value type on `metric_series`.** `metric_series` (§2.1) gains a discriminator column, added by additive `ALTER` (the §3.1 `structured_metadata` precedent — never a mutation of the frozen initial `CREATE`, so fresh and upgraded deployments converge byte-identically):

```sql
ALTER TABLE metric_series ADD COLUMN IF NOT EXISTS value_type UInt8 DEFAULT 0;
```

`value_type` is the per-series float/histogram discriminator: `0 = float`, `1 = histogram`. Pre-M7 rows read back `0` (the `DEFAULT`), so no data migration is required.

**Writer contract (M7-A4).** `value_type` is a *per-row* discriminator on `metric_series`, and it is part of the writer's registration key `(metric_name, fingerprint, activity-bucket, value_type)`. Registration is driven from **both** float samples (`value_type = 0`) and native-histogram samples (`value_type = 1`), so a series that carries both a float and a histogram sample in one activity bucket registers **two** `metric_series` rows — a "mixed" series is the `groupBitOr(bitShiftLeft(1, value_type))` rollup over those rows (`3` = mixed), computed at read time, never stored. Within a single ingest request the writer never emits a float and a native histogram at the same `(metric_name, fingerprint, unix_milli)` — the histogram wins and the colliding float is dropped. Across independent requests both a `metric_samples` and a `metric_hist_samples` row may coexist at one key by design; that is the read path's deterministic-merge concern (it dual-reads both tables and does **not** consult `value_type` for routing). See [ADR 0005](decisions/0005-native-histogram-storage.md).

---

## 3. Logs

**Query shapes served:** stream-selector reads with time bounds (dominant); line-filter search (`|=`, `|~`); LogQL metric queries (`rate`, `count_over_time`); label discovery; live tail.

### 3.1 Tables

```sql
CREATE TABLE log_streams (
    month        Date,                          -- toStartOfMonth(first write in month)
    fingerprint  UInt64,
    service      LowCardinality(String),        -- resource service.name ('' if absent)
    labels       String  CODEC(ZSTD(5)),        -- canonical JSON, sorted keys
    updated_ns   Int64
) ENGINE = ReplacingMergeTree(updated_ns)
PARTITION BY month
ORDER BY fingerprint;

CREATE TABLE log_streams_idx (
    month        Date,
    key          LowCardinality(String),
    val          String,
    fingerprint  UInt64
) ENGINE = ReplacingMergeTree
PARTITION BY month
ORDER BY (key, val, fingerprint);
-- populated by MV over log_streams:
--   ARRAY JOIN JSONExtractKeysAndValues(labels, 'String')
```

```sql
CREATE TABLE log_samples (
    service       LowCardinality(String),
    fingerprint   UInt64,
    timestamp_ns  Int64   CODEC(DoubleDelta, ZSTD(1)),
    severity      Int8    DEFAULT 0,             -- OTel SeverityNumber (0 = unset)
    body          String  CODEC(ZSTD(1)),
    structured_metadata String DEFAULT '',        -- per-entry Loki structured metadata (issue #97); added by additive ALTER, see note below
    INDEX idx_body_tokens body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1,
    INDEX idx_body_ngrams body TYPE ngrambf_v1(4, 32768, 3, 0) GRANULARITY 1,
    INDEX idx_severity severity TYPE minmax GRANULARITY 4
) ENGINE = MergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(timestamp_ns))
ORDER BY (service, fingerprint, timestamp_ns)
TTL toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL 7 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;
```

```sql
-- Rollup resolution is configuration, not schema: PULSUS_LOG_ROLLUP_RESOLUTION
-- (default 5s) sets the bucket size; the table is named for it (log_metrics_5s
-- by default) and the MV bucket expression is rendered from it.
CREATE TABLE log_metrics_5s (
    fingerprint  UInt64,
    bucket_ns    Int64,                          -- intDiv(timestamp_ns, {res_ns}) * {res_ns}
    count        SimpleAggregateFunction(sum, UInt64),
    bytes        SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(bucket_ns))
ORDER BY (fingerprint, bucket_ns);
-- populated by MV over log_samples
```

Raw log timestamps in `log_samples` are stored verbatim at nanosecond precision — the rollup is derived, and only eligible when the query step is a multiple of the configured rollup resolution; otherwise the planner counts raw rows.

- **`service` leads the samples ordering key.** This is the one label promoted to a physical column (populated from resource `service.name`; user-visible as the `service_name` label per the canonical label model), and it earns it three ways: (a) OpenTelemetry guarantees `service.name` on every resource (the collector defaults it to `unknown_service`), so it is never missing; (b) it is the natural clustering dimension — a service's streams sit contiguously, so service-scoped searches (the human default) read a compact range instead of granules scattered across all tenants of the table; (c) **the planner can always supply it**: stream resolution returns full label sets, so even a query that never mentions `service` gets `service IN (...)` injected from the resolved streams, keeping the primary index engaged. No other label is materialized — finding #9's counter-argument (row width, schema coupling) applies to everything else.
- **`(service, fingerprint, timestamp_ns)` is fixed** (finding #4). Per-stream time reads are sequential; multi-stream reads within one service are near-sequential.
- **Body skip indexes** (finding #3): the token bloom serves word-boundary terms, the 4-gram bloom serves substrings and anchored regex literals. The planner extracts index-friendly prefilters from every line filter and applies the exact predicate afterward — granule skipping plus correctness.
- **Monthly stream/index partitions** (finding #6): one row per stream per month; a 30-day label query touches ≤ 2 partitions.
- **`structured_metadata` is per-entry, not per-stream** (issue #97): Loki push carries optional per-entry structured metadata (protobuf `EntryAdapter.structuredMetadata` or a JSON `values` third element), stored here as a canonical sorted-key JSON String — the `log_streams.labels` representation, not `Map(String,String)` (§1 rejects Map for label-shaped data). Empty string = none (pre-#97 rows, and OTLP-logs rows without an instrumentation scope). As of #109, an OTLP-logs row's `InstrumentationScope` is stored here rather than as stream labels, matching grafana/loki 3.4.2 (which places scope identity in structured metadata, not indexed labels): the scope name/version under keys `scope_name`/`scope_version` (each emitted only when non-empty), and each other scope attribute under its own canonicalized key (`canonicalize_label_key`: each Unicode scalar outside `[a-zA-Z0-9_]` becomes a single `_`, per character — so consecutive disallowed characters yield consecutive underscores, e.g. `scope.attr.foo` → `scope_attr_foo`, `a..b` → `a__b`, `team` unchanged), with no `scope_` prefix. Empty attribute values are kept; only name/version are empty-suppressed. Keys are resolved to unique values by last-write-wins in wire order with identity applied last: on a key collision, scope identity wins over any attribute, and among attributes the last in wire order wins. It is added by **additive ALTER** (migration ids 21/22) rather than by mutating the frozen initial `CREATE`, so upgraded and fresh deployments converge byte-identically; a fresh DB runs `CREATE` (no column) then `ADD COLUMN IF NOT EXISTS`. Structured metadata never enters `stream_fingerprint` (a stream pushed with vs. without it fingerprints identically); on the read path it fans into the response stream label set alongside the base labels (grafana/loki 3.4.2 default, `categorize_labels` off), so an entry carrying distinct metadata forms its own result stream and a `| key="value"` pipeline filter selects on it.

### 3.2 Read paths (generated SQL)

**`{service_name="checkout", env="prod"} |= "connection refused"`, last 6h, limit 100.**

Stage 1 — stream resolution, a *single pass* over the index (finding #1): each `(key, val)` pair is a primary-prefix range read; the intersection happens inside the scan:

```sql
SELECT fingerprint
FROM log_streams_idx
WHERE month = '2026-07-01'
  AND ((key = 'service_name' AND val = 'checkout') OR (key = 'env' AND val = 'prod'))
GROUP BY fingerprint
HAVING uniqExact(key, val) = 2
```

Regex/negative matchers (finding #7) resolve within one key's index prefix, e.g. `env=~"prod|staging"` becomes `key = 'env' AND match(val, ...)` — a scan over the distinct *values of that key*, never over samples. The planner orders matchers by selectivity (cheap `count()` probes on index prefixes) and aborts with "query too broad" past `PULSUS_LOGQL_SCAN_BUDGET_BYTES`.

Stage 2 — hydration (needed for response labels anyway): `SELECT fingerprint, service, labels FROM log_streams WHERE fingerprint IN (...)` → also yields the `service` set for stage 3.

Stage 3 — samples, primary-index + skip-index served:

```sql
SELECT fingerprint, timestamp_ns, body
FROM log_samples
PREWHERE service = 'checkout'
WHERE fingerprint IN (18374..., 99120...)
  AND timestamp_ns >  {now - 6h} AND timestamp_ns <= {now}
  AND hasToken(body, 'connection') AND hasToken(body, 'refused')   -- skip-index prefilter
  AND position(body, 'connection refused') > 0                      -- exact predicate
ORDER BY timestamp_ns DESC
LIMIT 100
```

**`sum by (service_name) (rate({env="prod"}[5m]))`** — no body access, so it never touches `log_samples`:

```sql
SELECT fingerprint, intDiv(bucket_ns, 300000000000) * 300000000000 AS step, sum(count) AS n
FROM log_metrics_5s
WHERE fingerprint IN (...) AND bucket_ns > {start} AND bucket_ns <= {end}
GROUP BY fingerprint, step
```

The engine maps fingerprints to `service` from stage 2 and finishes the `sum by`.

**Live tail** polls stage 3's shape with a monotonic `timestamp_ns >` cursor; line-filter pushdown identical.

**Query-text admission (issue #35).** Every read-path query — LogQL, PromQL/metrics, and TraceQL — carries `max_query_size = 8 MiB` as a per-request session setting: ClickHouse's own SQL-text parse-buffer cap defaults to 262,144 bytes, well under the literal `fingerprint IN (...)` list a stage2/stage3 read renders at the documented 100k-stream cap (~2.2 MiB). Because `services`/line-filter text and metrics fan-out width are not bounded by any single constant, a rendered-SQL admission guard rejects any query text at or past the 8 MiB cap *before* dispatch as a clean `422 query_too_broad`, rather than letting an oversized request fail with an opaque ClickHouse parse error — the guaranteed-admitted envelope (100k worst-case fingerprints + a generous services/line-filter margin) fits comfortably inside it. Residual: the guaranteed-admitted envelope arithmetic assumes the shipped caps — should the stream cap or metrics cache/fanout caps ever become operator-configurable, the envelope must be re-derived against `MAX_QUERY_TEXT_BYTES`; scale considerations route to #25.

---

## 4. Traces

**Query shapes served:** trace-by-ID fetch (latency-critical); TraceQL search = attributes + intrinsics + time (human-facing); tag discovery; TraceQL metrics aggregations.

### 4.1 Tables

```sql
CREATE TABLE trace_spans (
    trace_id      FixedString(16),
    span_id       FixedString(8),
    parent_id     FixedString(8),
    name          LowCardinality(String),
    service       LowCardinality(String),
    timestamp_ns  Int64  CODEC(DoubleDelta, ZSTD(1)),
    duration_ns   Int64  CODEC(T64, ZSTD(1)),
    status_code   Int8,
    kind          Int8,
    payload_type  Int8,                          -- 1 = OTLP protobuf, 2 = Zipkin JSON
    payload       String CODEC(ZSTD(3)),
    INDEX idx_duration duration_ns TYPE minmax GRANULARITY 4,
    PROJECTION service_time (
        SELECT * ORDER BY (service, timestamp_ns)
    )
) ENGINE = MergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(timestamp_ns))
ORDER BY (trace_id, timestamp_ns)
TTL toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL 7 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;
```

- **One table, two physical orders** (finding #5). The base order makes trace-by-ID a point read; the `service_time` projection is a full physically re-sorted copy that ClickHouse's optimizer selects automatically for service + time predicates. The write amplification (~2× for this table) is an explicit trade for the two latency-critical read shapes. The `idx_duration` minmax works *within* the projection because slow spans cluster weakly by time — it prunes granules for `duration > X` searches; it is deliberately **not** relied on in the base order (finding: minmax on unclustered data is useless — here the projection provides the clustering context).

```sql
CREATE TABLE trace_attrs_idx (
    date          Date,
    key           LowCardinality(String),
    val           String,
    scope         LowCardinality(String),        -- 'resource' | 'span'
    val_num       Nullable(Float64),             -- populated when val parses numeric
    timestamp_ns  Int64,
    trace_id      FixedString(16),
    span_id       FixedString(8),
    duration_ns   Int64
) ENGINE = ReplacingMergeTree
PARTITION BY date
ORDER BY (key, val, scope, timestamp_ns, trace_id, span_id)
TTL toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL 7 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;

CREATE TABLE trace_tag_catalog (
    scope  LowCardinality(String),
    key    LowCardinality(String),
    val    String
) ENGINE = ReplacingMergeTree
ORDER BY (scope, key, val);
-- populated by MV over trace_attrs_idx (SELECT scope, key, val); grows with distinct (scope, key, val) —
-- no MV-side cardinality bound is enforced today (future work); the §4.3 response caps + truncated flag
-- bound what the API returns, not what the catalog stores or a scan reads
```

- **`scope` discriminates the attribute's origin** (`'resource'` vs `'span'`), so identical verbatim `(key, val)` pairs at different scopes stay separable — scoped TraceQL (`resource.foo` vs `span.foo`) would otherwise be incorrect. In **`trace_attrs_idx`** it sits **after** `(key, val)` in the ordering key: the proven `(key, val)` prefix pruning is preserved, a scoped query fixes `scope` by equality for near-free post-prefix time pruning, and an unscoped legacy tag search still prunes on the bare `(key, val)` prefix. Scope attributes are **not** indexed (M4 TraceQL exposes `resource.`/`span.` selectors plus the unscoped `.attr` form); they remain fully preserved in the span payload.
- **`trace_tag_catalog` orders differently — `(scope, key, val)`, scope FIRST** (its DDL above; do not conflate with `trace_attrs_idx`'s `(key, val, scope, …)`): the catalog serves scope-shaped tag discovery, so a scoped tag-names read prunes on the `(scope)` primary-key prefix and a scoped values read on `(scope, key)`; **unscoped** discovery (no scope, or a bare-key values lookup, which has no `(scope)` prefix to prune on) is contractually a full — small — catalog **scan** of distinct `(scope, key, val)` tuples (ClickHouse's granule exclusion may still skip granules opportunistically on a bare-key lookup, but that is layout-dependent, never a guarantee). That scan is bounded by the reader's Layer-1 read budget (`max_rows_to_read` = `reader.traceql_scan_budget_rows`, `read_overflow_mode = 'throw'` — the same setting the TraceQL search path applies): a catalog large enough that an unscoped/bare-key read would exceed it aborts with `422 query_too_broad` rather than running unbounded. The §4.3 response caps (`TAG_NAMES_MAX`/`TAG_VALUES_MAX`) bound only what a *successful* request returns, not what a scan reads. Tempo's `/api/v2/search/tags` (T6) is scope-aware.
- **`timestamp_ns` after the `(key, val, scope)` prefix**: TraceQL searches are always time-bounded, so within each `(key, val)` (or `(key, val, scope)`) prefix the time predicate prunes granules — a 3h search over a busy attribute reads 3h of index, not 7 days (compare finding #5's index, which ordered trace/span IDs before time).
- **`val_num`** gives numeric comparisons (`span.http.status_code >= 500`) a typed column. Scope this honestly: `val_num` is not in the primary key, so a range predicate scans *all values of that key* in the time range and filters — acceptable for low-cardinality numeric attributes (status codes, retry counts), **not** a general strategy for high-cardinality numerics (sizes, user-defined measurements). Duration, status, kind, name, and service are physical span columns precisely so the common numeric intrinsics never rely on this index. If benchmarks show real workloads need fast range predicates on high-cardinality numeric attributes, the design adds a dedicated numeric index ordered `(key, timestamp_ns, val_num, ...)` — benchmark-gated, not speculative.
- Tag APIs read only `trace_tag_catalog` — discovery never scans span payloads.
- **Admitted trace timestamp domain and runtime TTL (issue #131).** Ingest admits a span only if its UTC day lies in `[1970-01-01, 2106-02-06]` (days `0..=49_709`, `pulsus_model::Date::start_of_day_utc_datetime_safe`); a span outside that domain is rejected (OTLP partial success; Zipkin whole-request 400). Two wrap mechanisms motivate the gate: `PARTITION BY toDate(...)` evaluates in the 16-bit `Date` domain and wraps for days past 2149-06-06, and the delete-TTL evaluates the row timestamp in the 32-bit `DateTime` domain and wraps for instants past 2106-02-07T06:28:15Z (u32-seconds maximum, `4294967295`); day `49_710` (2106-02-07) is excluded because only part of it is u32-representable. The CREATE-time TTL shown above is superseded at runtime: `apply_ttl` re-issues `ALTER TABLE ... MODIFY TTL toDateTime(least(intDiv(timestamp_ns, 1000000000) + retention_days * 86400, 4294967295)) DELETE` on both trace tables at init and on every rotation tick, so for a stored row with epoch-seconds `s = floor(timestamp_ns / 1e9)` the operative expiry is `expiry(s) = min(s + retention_days * 86400, 4294967295)` — i.e. `min(configured_expiry, 2106-02-07T06:28:15Z)`. If `s + retention_days * 86400 <= 4294967295`, the expiry equals the configured instant, bit-identical to the pre-#131 expression; otherwise the expiry is `4294967295`, the actual retention is `4294967295 - s`, and the shortfall vs the configured value is `s + retention_days * 86400 - 4294967295`, which grows without bound as `retention_days` grows. For the enforced range `retention_days >= 1` (config validation rejects `< 1`, `crates/pulsus-config/src/validate.rs:132-134`), a row at the last admitted day (`49_709`, `s = 4_294_943_999`) has actual retention capped at `4_294_967_295 - 4_294_943_999 = 23_296 s ≈ 0.27 days (~6.5 hours)`. For every enforced `retention_days >= 1`, the saturating form strictly dominates the pre-#131 expression: pre-#131, a row with `s + retention_days * 86400 > 4294967295` wrapped to a ~1970-epoch expiry and its part became drop-eligible immediately or near-immediately after insert (`ttl_only_drop_parts = 1`); under the saturating form the same row becomes drop-eligible no earlier than 2106-02-07T06:28:15Z. The admission cutoff is deliberately not coupled to `retention_days`: retention is runtime-ALTERed after rows are stored (a changed `PULSUS_RETENTION_DAYS` re-ALTERs existing tables on the next rotation tick) and has no upper bound, so no admission-time gate can honor a retention value that did not exist when the row was admitted.

### 4.2 Read paths (generated SQL)

**Trace by ID** — pure primary-index point read (plus partition pruning when a time hint is present):

```sql
SELECT trace_id, span_id, parent_id, payload_type, kind, payload
FROM trace_spans
WHERE trace_id = unhex('4bf92f3577b34da6a3ce929d0e0e4736')
```

`kind` is projected (issue #75) solely as the trace-by-ID assembler's `(span_id, kind)` de-duplication key — the response renders `kind` from each winner's decoded OTLP payload, never from this column. This keeps a Zipkin shared span's SERVER and CLIENT sides (identical `(trace_id, span_id)`, different `kind`) as two distinct spans on retrieval, while remaining a genuine no-op for OTLP (span ids are unique per trace) and still de-duplicating identical at-least-once replays (same `(span_id, kind)` + bytes).

**TraceQL search is two-phase** (issue #57): Phase 1 produces a bounded, recency-ranked candidate trace-id set from indexed sources (false positives are harmless — Phase 2 filters; false negatives exist only past the cap and are reported via the response's `partial` flag); Phase 2 hydrates candidates in small batches and evaluates the full query **exactly** in the engine.

**Phase 1 — per-generator bounded ranked queries.** Each leaf comparison compiles to a generator over its natural indexed source; every generator is its own index-served top-K query (never a `UNION ALL` — the `GROUP BY` stays confined to one leaf's pruned prefix):

```sql
SELECT trace_id, max(timestamp_ns) AS bound_ts
FROM <its indexed source>
WHERE <leaf predicate + date/time pruning>
GROUP BY trace_id
ORDER BY bound_ts DESC, trace_id ASC
LIMIT {PULSUS_TRACEQL_MAX_CANDIDATES + 1}
```

The generator classes, their prefixes, and their honest costs:

| Leaf class | Source / prefix | Cost profile |
|---|---|---|
| attr `=` string/bool | `trace_attrs_idx` `(key, val[, scope])` prefix + date/time pruning | index-served |
| attr numeric (`val_num <op> N`) | `trace_attrs_idx` **key-only** `(key)` prefix scan + filter | scans all of the key's in-window values (the §4.1 `val_num` honesty note) |
| attr regex `=~` (anchored `^(?:…)$`) | `trace_attrs_idx` **key-only** `(key)` prefix scan + `match(val, …)` | same key-only scan |
| `resource.service.name =` | `trace_spans` `service_time` projection PREWHERE + time | index-served |
| `resource.service.name =~` | its own `trace_attrs_idx` row (`key='service.name' AND scope='resource'`) | key-only scan |
| `duration <op>` | `trace_spans` + `idx_duration` minmax within the projection | granule-pruned |
| `name`/`status`/`kind` | `trace_spans` time-window scan + predicate | no selective index — window-bounded, budget-limited |
| `!=` / `!~` / `{}` match-all | the time-range generator (`trace_spans` over the window) | complete superset; absence is not indexable |

Within one `{...}` filter, an `&&` needs only its statically most selective conjunct's generator set (matches are a subset of any conjunct's); an `||` needs both sides' sets. Cross-spanset `{A} op {B}` takes the superset union of both operands' generators for **both** `&&` and `||` — exactness is Phase 2's job, never a lossy trace-id reduction. Selectivity is the fixed leaf-class priority above (byte-deterministic, never a runtime probe). Every generator (indexed and fallback alike) carries the reader scan budget (`PULSUS_TRACEQL_SCAN_BUDGET_ROWS` as `max_rows_to_read` + throw): a query too broad to bound fails loud with `422 query_too_broad` — it is never silently slow and never quietly incomplete.

The engine merges the per-generator `(trace_id, bound_ts)` tuples in Rust — an explicit `max(bound_ts)` per trace, ranked `(bound_ts DESC, trace_id ASC)`. `bound_ts` (the newest *leaf-matching* span's timestamp) is an upper bound on the trace's final public sort key (the max timestamp of its *exactly-matched* spans, a subset), which licenses the early termination below.

**Phase 2 — streaming batched exact evaluation.** Candidates are consumed newest-bound-first in batches of `BATCH_TRACES` (32). Per batch: spans hydrate by primary key (`WHERE trace_id IN (batch) AND <time>`, `LIMIT {MAX_SPANS_PER_TRACE + 1} BY trace_id` — `MAX_SPANS_PER_TRACE` = 10,000; the `+1` probe distinguishes exactly-at-cap from overflow, and an overflowing trace is evaluated on its truncated span set and the response marked `partial`), deduped by `span_id` (at-least-once replays, no `FINAL`); each distinct attribute condition runs one `SELECT DISTINCT trace_id, span_id` membership read restricted to the batch, time/date-pruned within its prefix. The engine then evaluates the full boolean tree per span (physical leaves on hydrated columns; an attribute leaf is membership in its read; `!=`/`!~` match a span iff **no** index row for it satisfies the positive predicate — absent-key spans match), applies the cross-spanset algebra with matched-span membership preserved (`{A} && {B}` = trace-level intersection, spanset = union of matched spans; `||` = union), evaluates the pipeline (`count`/`sum`/`avg`/`min`/`max` over the matched spans, attribute aggregates via a batched `val_num` read; `select()` projects response fields only), and pushes survivors into a `limit`-size heap of **response summaries only** — never hydrated spans or payloads. Consumption stops when the heap is full and the next candidate's `bound_ts` is strictly below the k-th held sort key (no unseen candidate can enter the top-K), at exhaustion, or at the `PULSUS_TRACEQL_MAX_CANDIDATES` consumption ceiling. Winners get one trace-wide root hydration (a `trace_id` PK read with **no** time predicate — the true root may predate the search window; root = `parent_id` all-zero, else timestamp-earliest).

**Ordering and partiality contracts** (public, docs/api.md §4.2): `traces[]` is ordered by the max timestamp of each trace's exactly-matched spans, descending, `trace_id` ascending as the tiebreak. `partial = true` whenever ANY bound engaged before natural exhaustion: (a) a generator returned `cap + 1` rows, (b) the consumption ceiling was reached with a lookahead candidate present, (c) a per-trace span overflow occurred. Budget breaches (scan rows, read/result bytes, or the engine's 256 MiB retention counter — §7) are hard `422`s, never partial results.

The worked example `{ resource.service.name = "checkout" && span.http.status_code >= 500 && duration > 2s }` (last 3h, limit 20) therefore runs: one Phase-1 generator — the service-equality projection read above (`PREWHERE service = 'checkout'`, the conjunction's most selective leaf) — then per batch one `key = 'http.status_code' AND val_num >= 500 AND scope = 'span'` membership read (date + time pruned within the key prefix), with `duration_ns > 2000000000` evaluated on the hydrated physical column; the byte-frozen SQL lives in `crates/pulsus-read/tests/golden/traces_search/`.

**TraceQL metrics** (`{...} | rate()` / `| count_over_time()`, issue #59) — one fully-pushed-down, time-bucketed conditional aggregation per request (never the two-phase candidate model). For `{ resource.service.name = "checkout" && span.http.status_code >= 500 && duration > 2s } | rate()` at step 60s:

```sql
SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), INTERVAL 60000 MILLISECOND)) AS t,
       uniqExact(trace_id, span_id) AS n
FROM trace_spans
PREWHERE service = 'checkout'
WHERE timestamp_ns >= {S} AND timestamp_ns < {E}
  AND ((trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx
       WHERE date >= toDate({S}) AND date <= toDate({E - 1ns})
         AND timestamp_ns >= {S} AND timestamp_ns < {E}
         AND key = 'http.status_code' AND val_num >= 500 AND scope = 'span') AND duration_ns > 2000000000)
GROUP BY t
ORDER BY t ASC
```

- **Counting is `uniqExact(trace_id, span_id)`** — the T5 logical-span identity, so at-least-once replays never inflate a bucket (no `FINAL`). `rate` divides the deduped count by the step **client-side at the encode boundary** (the instant `/query` form drops the `GROUP BY` and divides by the snapped window width); `count_over_time` ships the count as-is — the SQL body is byte-identical for both functions.
- **Snapped, left-closed buckets:** `{S} = ⌊start/step⌋·step`, `{E} = ⌈end/step⌉·step` (epoch-aligned, outward), the time filter is left-closed/right-open — every emitted bucket `[b, b + step)` is full-width, so the rate denominator is always the full step. `toUnixTimestamp64Milli(...)` pins the bucket column to a deterministic `Int64` epoch-milliseconds wire type (covers pre-1970/post-2106 buckets that a `UInt32` epoch-seconds column would wrap — issue #59 re-audit). The bucketing interval is rendered in **milliseconds** (`INTERVAL {step*1000} MILLISECOND`), not seconds: ClickHouse 24.8's `toStartOfInterval` downgrades a `DateTime64` argument to a 32-bit `DateTime` for whole-second-and-larger interval units, silently clamping pre-1970/post-2106 instants (and then rejecting `toUnixTimestamp64Milli`'s `DateTime64` argument outright); the millisecond-unit form keeps `DateTime64(3)` precision and range end to end.
- **Access paths:** a root-AND-spine `resource.service.name =` conjunct (never one inside/under an `||`) hoists to `PREWHERE` and selects the `service_time` projection; every attribute leaf is an index-served `(trace_id, span_id) [NOT] IN` semi-join confined to its `(key[, val][, scope])` prefix plus daily-partition/time pruning (`NOT IN` implements the ratified absent-key negation rule); physical leaves render inline on `trace_spans` columns.
- **Bounded state:** every metrics query carries the trace read budgets (scan rows/bytes, result bytes, throw) **plus** the semi-join IN-set limits — `max_rows_in_set` (1,000,000) / `max_bytes_in_set` (64 MiB) with `set_overflow_mode = 'throw'` → `422 query_too_broad` via its own dedicated reason, never an unbounded in-memory set. The bucket count itself is capped statically at plan time (docs/api.md §4.4).
- **Clustered honesty:** the reader additionally injects `distributed_product_mode = 'local'`, rewriting the semi-join subquery to the **local** shard's `trace_attrs_idx` (exact under the `cityHash64(trace_id)` co-sharding, and it kills the `_dist`-inside-`_dist` double-distributed path). The time-bucket `GROUP BY` is **not** shard-local — buckets exist on every shard and the coordinator merges per-bucket partial states, bounded by the point cap × shard count (scale evidence routes to #25).

---

## 5. Profiles

**Query shapes served:** flamegraph merge over `(profile type, service, selector, time range)`; profile-value time series; diff between two ranges.

```sql
CREATE TABLE profile_samples (
    type_id        LowCardinality(String),        -- e.g. process_cpu:cpu:nanoseconds:cpu:nanoseconds
    service        LowCardinality(String),
    fingerprint    UInt64,
    timestamp_ns   Int64  CODEC(DoubleDelta, ZSTD(1)),
    duration_ns    Int64,
    payload_type   Int8,
    payload        String CODEC(ZSTD(3)),         -- original pprof
    tree           Array(Tuple(UInt64, UInt64, Int64, Int64)) CODEC(ZSTD(3)),
                   -- (node_id, parent_id, self_value, total_value)
    functions      Array(Tuple(UInt64, String)) CODEC(ZSTD(3))
                   -- (node_id, function name)
) ENGINE = MergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(timestamp_ns))
ORDER BY (type_id, service, timestamp_ns)
TTL toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL 7 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;

CREATE TABLE profile_series (
    month        Date,
    fingerprint  UInt64,
    type_id      LowCardinality(String),
    service      LowCardinality(String),
    labels       String CODEC(ZSTD(5)),
    updated_ns   Int64
) ENGINE = ReplacingMergeTree(updated_ns)
PARTITION BY month
ORDER BY fingerprint;

CREATE TABLE profile_series_idx (
    month        Date,
    key          LowCardinality(String),
    val          String,
    fingerprint  UInt64
) ENGINE = ReplacingMergeTree
PARTITION BY month
ORDER BY (key, val, fingerprint);
```

- The dominant read (`merge flamegraph for type T, service S, last hour`) is a pure primary-prefix scan of `profile_samples` — no index round trip at all; the series index is consulted only when the selector uses labels beyond type/service.
- **Trees are precomputed at ingest** (from pprof or OTLP profiles): the engine merges compact `(node, parent, self, total)` arrays instead of re-parsing pprof, which is what makes `render` latency independent of original profile size.

Read path — flamegraph for `process_cpu:...{service_name="checkout"}`, last hour:

```sql
SELECT tree, functions
FROM profile_samples
PREWHERE type_id = 'process_cpu:cpu:nanoseconds:cpu:nanoseconds' AND service = 'checkout'
WHERE timestamp_ns > {now - 1h} AND timestamp_ns <= {now}
```

---

## 6. Rules & bookkeeping

```sql
CREATE TABLE rules (
    namespace   String,
    group_name  String,
    kind        LowCardinality(String),   -- logs | metrics
    config      String,                   -- YAML rule group
    updated_at  DateTime64(3),
    is_valid    UInt8
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (namespace, group_name, kind);

CREATE TABLE schema_migrations (
    id           UInt32,
    checksum     String,
    applied_at   DateTime
) ENGINE = ReplacingMergeTree(applied_at)
ORDER BY id;

CREATE TABLE mv_checksums (
    mv_name     String,
    checksum    String,
    updated_at  DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY mv_name;
```

**Migration amendment policy:** the migration catalog (`pulsus-schema`'s `catalog.rs`, recorded per-id in `schema_migrations`) is append-only from the first tagged release onward. In-place amendment of an already-listed migration was permitted only pre-release (no tagged release, no persistent deployments, CI databases created fresh per run); the trace-index scope amendment (issue #54) was the last such amendment window. A local database created before a pre-release amendment must be dropped and re-reconciled — the per-id checksum drift guard refuses to touch the stale tables.

---

## 7. Distributed layout

Enabled by `PULSUS_CLUSTER`. Every table becomes `ReplicatedMergeTree`-family with a Distributed wrapper. **Sharding keys are chosen so that reads join and aggregate shard-locally** (finding #2):

| Table | Sharding key | Why |
|-------|--------------|-----|
| `metric_samples`, `metric_samples_5m/_1h`, `metric_series` | `cityHash64(metric_name, fingerprint)` | the metric fingerprint **excludes `__name__`**, so every metric sharing a target's label set shares one fingerprint — sharding by fingerprint alone would pile all of a target's metrics onto one shard (skew). The true series identity is `(metric_name, fingerprint)`, and the shard key matches it: a series still lives whole on one shard, per-series evaluation and tier `GROUP BY` stay shard-local, and same-labelset metrics spread across the cluster |
| `log_samples`, `log_streams`, `log_streams_idx`, `log_metrics_5s` | `fingerprint` | index and data **co-shard**: the stream-resolution `GROUP BY fingerprint HAVING ...` runs per shard on complete groups, hydration joins locally, and each shard's stage-3 read is against its own streams |
| `trace_spans`, `trace_attrs_idx` | `cityHash64(trace_id)` | a trace is whole on one shard; span-level intersections and trace assembly are shard-local |
| `profile_samples`, `profile_series`, `profile_series_idx` | `fingerprint` | same co-sharding argument as logs |
| `rules`, catalogs, bookkeeping | (replicated to all shards via a shard-less replication path — one cluster-wide replica set, no Distributed writes) | tiny, read-everywhere; **prerequisite: `{replica}` macros must be unique across the whole cluster**, not merely within a shard |

Fan-out analysis for the canonical operations:

| Operation | Shards doing work | What crosses the network |
|-----------|-------------------|--------------------------|
| Trace by ID | **1** (`optimize_skip_unused_shards` prunes by sharding key) | one trace |
| PromQL selector fetch | all (each holds a disjoint series subset) | only matched series' samples, already time-cut |
| PromQL gauge-on-tier | all, **partial aggregation per shard** | per-step aggregate states, not samples |
| LogQL stream resolution + read | all, but every stage completes shard-locally | matched log lines only |
| TraceQL search | all; intersections shard-local (a trace's index rows and spans co-reside) | top-K candidates per shard |
| Label/tag discovery | all | deduplicated key/value sets |

Every local table gets a Distributed wrapper of this shape (the schema controller renders one per table from the sharding-key column above):

```sql
CREATE TABLE log_samples_dist AS log_samples
ENGINE = Distributed('{cluster}', pulsus, log_samples, fingerprint);

CREATE TABLE metric_samples_dist AS metric_samples
ENGINE = Distributed('{cluster}', pulsus, metric_samples, cityHash64(metric_name, fingerprint));
```

Two invariants the schema controller enforces, because co-location silently breaks without them: **every table in a signal family uses the byte-identical sharding expression** (raw, tiers, series/index tables alike — a divergence would put a series' rollups on a different shard than its samples), and **all inserts either go through the `_dist` wrappers or compute the same expression client-side** — the writer never freelances shard placement. Cluster configs use `internal_replication = true` (the underlying tables are `ReplicatedMergeTree`; the Distributed layer must write each block to one replica and let replication fan it out, or rows duplicate).

Reader-issued settings in clustered mode: `optimize_skip_unused_shards = 1`, `optimize_distributed_group_by_sharding_key = 1` (so `GROUP BY fingerprint` shapes skip the coordinator re-aggregation the co-sharding makes unnecessary), `distributed_aggregation_memory_efficient = 1`, `prefer_localhost_replica = 1`, and `skip_unavailable_shards` per `PULSUS_SKIP_UNAVAILABLE_SHARDS`. Where a coordinator-built `IN (...)` list would be large, the planner switches to the JOIN/subquery form so the filter executes as a remote-local subquery rather than shipping the set twice. There is **no** `rand()` sharding anywhere in the schema.

**TraceQL search reader-settings contract** (issue #57). Precondition: `trace_spans` and `trace_attrs_idx` co-shard on the byte-identical `cityHash64(trace_id)` expression, so a trace's spans and index rows always co-reside — every Phase-1 `GROUP BY trace_id` completes on whole groups per shard, and every Phase-2 `trace_id IN (batch)` read (batches are ≤ 32 explicit ids, never a large coordinator-built set) prunes to the owning shards under `optimize_skip_unused_shards`. Every search query — generators and hydration/membership batches alike — additionally carries server-side budgets with throw semantics: `max_rows_to_read = PULSUS_TRACEQL_SCAN_BUDGET_ROWS` + `read_overflow_mode = 'throw'` (non-indexable generators are budget-limited → `422 query_too_broad`, never silently slow), `max_bytes_to_read` + the same throw mode, `max_result_bytes` + `result_overflow_mode = 'throw'`, and `max_block_size = TRACE_SEARCH_MAX_BLOCK_ROWS` (4096 rows). Enforcement of the byte ceilings is **block-granular**, but (issue #57 re-audit) the transient is now HARD-bounded, not merely accepted-and-documented: every string value the search response returns (`name`/`service`, and `select()`-projected attribute values) is truncated at the SOURCE with a hard **byte** ceiling — `if(length(col) <= 8192, col, substringUTF8(col, 1, 2048)) AS col` (`TRACE_STR_COL_CAP` = 8192 bytes; the fallback branch cuts at 2048 UTF-8 code points, each ≤ 4 bytes, so it too never exceeds the byte ceiling) — so the driver's one transiently-buffered result block is bounded at ≤ `TRACE_SEARCH_MAX_BLOCK_ROWS` rows × (2 × `TRACE_STR_COL_CAP` string bytes + fixed-width columns) ≈ ≤ ~67 MB, never a-priori row-unbounded. **Live-verified on 24.8:** the result-side budget (`max_result_bytes` + `result_overflow_mode = 'throw'`) does not throw on **unwrapped passthrough columns** in streamed `SELECT` shapes; the source-truncation projection above makes its accounting **effective** on the hydration/root/value reads — a **deliberate hardening**. Layer 1 (64 MiB `max_result_bytes` per query) is therefore the practical **per-batch** byte bound on the search's Phase-2 reads, firing server-side before the driver materializes anything; Layer 2 — the engine's request-scoped 256 MiB retention counter (charged per row/entry as results stream) — remains the binding bound on **cross-batch retained accumulation** (merge tuples, membership sets, heap-held response summaries, root summaries), which survives each batch's charge release and which no per-query server setting can see. A breach of either layer is a `422`, never an OOM. The engine's bounded-consumption guarantee is Rust-side (bounded generator transfer + the retention counter); ClickHouse's own generator-aggregation memory is additionally bounded (issue #57 re-audit, sub-problem B) by a dedicated generator-only ceiling — `max_memory_usage = PULSUS_TRACEQL_GENERATOR_MAX_MEMORY_BYTES` (512 MiB default) + `max_bytes_before_external_group_by = 0` (throw-not-spill) — so a dense common-value prefix's `GROUP BY trace_id` aggregation state is hard-bounded too: a breach is server code 241 (`MEMORY_LIMIT_EXCEEDED`) → `422 query_too_broad`, never an OOM (read-cost, as opposed to memory, is bounded by prefix confinement + the per-query server budgets; a read-bounded common-value generator SQL shape is tracked in issue #63). The "TraceQL search" and "Trace by ID" fan-out rows above are confirmed by Tier-1 per-stage/per-shard evidence on the 2-shard fixture (`docs/benchmarks/m4-traces-read-path.md` — coordinator-inclusive `system.query_log` rows verdicted against a client-computed `cityHash64(trace_id) % total_weight` roster, the same methodology that graduated the logs family); one caveat noted there: the trace-by-ID single-shard confinement is proven under the §7 reader-issued settings, which the search engine injects in clustered mode but the fetch handler does not yet — wiring them through the fetch path is a follow-up.

**Status: logs family graduated (M1, issue #16) and the traces read-path rows confirmed (M4, issue #57 — `docs/benchmarks/m4-traces-read-path.md`, 2-shard fixture, same methodology, wired into `schema-it-cluster` as hard verdicts); metrics/profiles remain design intent, not observed behavior.** Co-sharding is necessary but not sufficient — Distributed plans can still merge at the initiator or ship large sets if the generated SQL doesn't cooperate, which is why graduation requires *per-stage, exact-shard-roster* evidence, not just a terminal-query spot check (three rounds of CODE review on issue #16 caught successively narrower gaps: the first draft graduated on terminal-stage-only evidence with the discovery row uncovered; the second added per-stage evidence but silently excluded the coordinator's own shard from every row — under `prefer_localhost_replica = 1` the initiator's local-shard read is logged as its own `is_initial_query = 1` row, not a separate `is_initial_query = 0` sub-query row, and a filter that keeps only `is_initial_query = 0` misses it entirely; the third accepted any shard count for fingerprint-pruned stages without deriving which shards were *expected*, so a genuinely lost `system.query_log` row was indistinguishable from correct `optimize_skip_unused_shards` pruning). The `log_samples`/`log_streams`/`log_streams_idx`/`log_metrics_5s` sharding row above, and both logs-relevant rows of the Fan-out analysis table ("LogQL stream resolution + read" and "Label/tag discovery"), are confirmed by Tier-1 evidence (docs/schemas.md §9's two-tier model): per-shard `system.query_log` + `EXPLAIN PIPELINE`, captured separately for **every** stage each shape executes (resolution, hydration, samples/rollup read, and the discovery query in its own right) and **every** shard including the coordinator's own, verified against a **client-computed expected shard roster** (a cumulative-weight slot→shard map from `system.clusters`/`system.macros`, `fingerprint % total_weight` per queried fingerprint for pruned stages) on the 4-shard fixture (`docs/benchmarks/m1-logs-read-path.md`) — showing stage-1 stream resolution executing shard-locally (reaching the full 4-shard roster), hydration/samples joining and reading shard-locally (narrowing to *exactly* the computed owning subset for narrow fingerprint predicates, with the excluded shard's absence proven, not assumed), and the label/tag discovery query itself fanning out across the full roster with only deduplicated results crossing the network. This is topology mechanics a 4-shard cluster demonstrates at any corpus scale. **Latency at Tier-2 scale (1 TB/7d) is separately tracked and unvalidated (issue #25)**; it does not gate this graduation, which is about which node does the work, not how fast. The traces rows ("Trace by ID" and "TraceQL search") were confirmed the same way in M4 — per-stage, coordinator-inclusive, roster-verdicted evidence on the 2-shard fixture (`docs/benchmarks/m4-traces-read-path.md`, `cityHash64(trace_id) % total_weight` client-side derivation, run as hard CI verdicts by `cargo xtask bench traces-read`), with the trace-by-ID caveat noted there (proven under the §7 reader-issued settings; the fetch handler does not yet inject them). The metrics/profiles rows above remain design intent until the M3/M5 multi-shard benchmarks confirm them the same way; those snapshots then join the CI regression set.

---

## 8. Cross-cutting defaults

| Concern | Decision |
|---------|----------|
| `index_granularity` | 8192 default everywhere; revisit per-table only with benchmark evidence |
| `PREWHERE` | planner always places the most selective low-cardinality predicate (`metric_name`, `service`, `type_id`) in `PREWHERE`; time predicates in `WHERE` (partitions already prune them) |
| Partitioning | **daily** for raw sample/span tables (short TTL, whole-part drops); **monthly** for series/index/tier tables (long-lived, low-churn) |
| TTL | `ttl_only_drop_parts = 1` on all raw tables; per-tier retention on rollups; `PULSUS_STORAGE_POLICY` for hot/cold volumes |
| Dedup strategy | metadata: `ReplacingMergeTree` + duplicate-tolerant reads (`LIMIT 1 BY`, `GROUP BY`); samples: append-only `MergeTree`, exact-once by writer batch atomicity |
| Codecs | timestamps `DoubleDelta`, gauge-like floats `Gorilla`, counters/ids `Delta`/`T64`, payloads/labels `ZSTD(3..5)`, everything wrapped in `ZSTD(1)` minimum |
| Minimum ClickHouse | 24.8 LTS (projections + modern TTL/`SimpleAggregateFunction` behavior; all MVs are classic incremental — no refreshable-MV or scheduler dependency) |

---

## 9. Validation plan

The schemas are accepted only with benchmark evidence, produced by the M-milestone e2e harness on a reference 4-node cluster (8 vCPU, local NVMe per node) and a single-node baseline:

**Datasets.** Metrics, two tiers: an accuracy corpus of 10k series (counter/gauge/histogram mix) for differential testing, and a **scale corpus of 5M active series (churning to ~20M distinct over 30 days) — the design-target cardinality**, exercising label-cache memory and refresh, `metric_series` volume, selector resolution past the cache cap, and shard balance under `cityHash64(metric_name, fingerprint)`. The scale corpus deliberately includes skewed metrics (one metric at ~2M series, mid-cardinality metrics at ~500k, a long tail at ≤10k) so the **three-path label-resolution benchmark** (§2.1 strategy ladder: cache matcher / SQL fallback / prototype inverted index) measures each path where it is weakest — that benchmark is the M2/M3 decision gate for `metric_series_idx`. Both use 30 days of **mixed source resolutions** — 1s, 15s, 60s, 5m, plus deliberately irregular/jittered push cadences — because PulsusDB assumes no scrape interval and the engine's interval-derived semantics (extrapolation, staleness) must be validated across all of them. Logs: 1 TB over 7 days across 50 services / 5k streams. Traces: 100M spans over 7 days. Profiles: 1M profiles over 7 days.

**Latency targets (warm cache, p95) — validated by a Tier-2 reference run (see two-tier model below):**

| Query | Target |
|-------|--------|
| PromQL instant, one metric, ≤100 series | < 50 ms |
| PromQL range 24h/60s incl. `rate` + `sum by` | < 150 ms |
| PromQL range 30d/1h (tier-served) | < 1 s |
| Log label/series discovery, 7d | < 100 ms |
| Log stream read 6h, limit 100 | < 200 ms |
| Log body search, one service, 24h | < 2 s |
| Trace by ID | < 50 ms |
| TraceQL search 3h (attrs + duration) | < 500 ms |
| Flamegraph merge, one service, 1h | < 1 s |

**Two-tier evidence model.** Read-path acceptance is proven in two tiers. **Tier 1 (per-milestone, CI)** is scale-invariant: `EXPLAIN indexes=1` snapshots plus `system.query_log` *ratios* (`read_rows`/returned, `SelectedMarks`/`total_marks`, `read_bytes`/selected-marks) on a deterministic CI-scale corpus, and — for distributed claims — per-shard `query_log` + `EXPLAIN PIPELINE` on the 4-shard fixture. Tier 1 catches index-pruning and fan-out regressions and is sufficient to **upgrade or revise the §7 fan-out table and the architecture.md risks-table shard-locality rows**, because shard-local execution is topology mechanics (which node aggregates, what crosses the network) that a 4-shard cluster demonstrates at any scale. **Tier 2 (reference cluster)** is the 1 TB/7d / 50-service / 5k-stream run on the reference 4-node box (8 vCPU, local NVMe) that validates the **latency targets above**. Until a Tier-2 run lands, the latency figures in the table above are **unvalidated targets**; a report may claim Tier-1 evidence and shard-locality graduation without claiming the latency numbers. The Tier-2 run is tracked by a follow-up issue (#25) and its numbers are appended to the report when reference hardware is available. The logs family closed Tier 1 in M1 (issue #16); see `docs/benchmarks/m1-logs-read-path.md`.

**Regression harness.** Every planner/schema PR runs the query set against fixed datasets; `system.query_log` metrics (`read_rows`, `read_bytes`, `SelectedMarks`, memory) are recorded so index-pruning regressions are caught by CI, not by users. `EXPLAIN indexes = 1` output for the canonical queries is snapshot-tested — a query silently losing its primary-index prefix or skip-index usage fails the build. All DDL blocks in this document are rendered and executed against a fresh ClickHouse in CI from M0.

**Tier accuracy suite (M3).** Differential tests comparing raw vs tier evaluation over: deliberately misaligned query windows (start/end inside buckets), the current partially-filled bucket, reset-heavy counters including single-reset-in-bucket and undetectable-reset (`100,150,10,140`) shapes, and injected duplicate/late samples. The report quantifies error per function and window shape; `exact`-policy results must be bit-identical to Prometheus, `fast`-policy error bounds get documented numbers.

**Storage amplification (M5).** Profile rows carry tree/function arrays plus the original payload; the M5 run measures bytes/profile at high frequency with shared symbol tables. If amplification is unacceptable, the fallback design (payload, function dictionary, and compact tree samples in separate tables) replaces it behind the same read path.
