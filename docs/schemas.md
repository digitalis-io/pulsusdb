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
- **Size the activity bucket to cardinality.** Rows/month ≈ active series × (30d ÷ bucket). At 5M continuously active series, hourly buckets produce ~3.6B metadata rows/month; a `1d` bucket produces ~150M — the recommended setting at multi-million-series scale. Coarser buckets are always *logically safe*: the bucket-floored read bounds (§2.1 lookup SQL, rendered from the same config constant the writer uses) can over-include series adjacent to the query window — they match no samples — but can never miss one. They are not computationally free, though: a 10-minute historical query against a `1d` bucket drags that whole day's series for the metric through label matching. Bucket size is therefore part of the label-resolution benchmark below, not just a storage knob.

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
    val_num       Nullable(Float64),             -- populated when val parses numeric
    timestamp_ns  Int64,
    trace_id      FixedString(16),
    span_id       FixedString(8),
    duration_ns   Int64
) ENGINE = ReplacingMergeTree
PARTITION BY date
ORDER BY (key, val, timestamp_ns, trace_id, span_id);

CREATE TABLE trace_tag_catalog (
    key  LowCardinality(String),
    val  String
) ENGINE = ReplacingMergeTree
ORDER BY (key, val);
-- populated by MV over trace_attrs_idx; bounded per key by the writer
```

- **`timestamp_ns` third in the index key**: TraceQL searches are always time-bounded, so within each `(key, val)` prefix the time predicate prunes granules — a 3h search over a busy attribute reads 3h of index, not 7 days (compare finding #5's index, which ordered trace/span IDs before time).
- **`val_num`** gives numeric comparisons (`span.http.status_code >= 500`) a typed column. Scope this honestly: `val_num` is not in the primary key, so a range predicate scans *all values of that key* in the time range and filters — acceptable for low-cardinality numeric attributes (status codes, retry counts), **not** a general strategy for high-cardinality numerics (sizes, user-defined measurements). Duration, status, kind, name, and service are physical span columns precisely so the common numeric intrinsics never rely on this index. If benchmarks show real workloads need fast range predicates on high-cardinality numeric attributes, the design adds a dedicated numeric index ordered `(key, timestamp_ns, val_num, ...)` — benchmark-gated, not speculative.
- Tag APIs read only `trace_tag_catalog` — discovery never scans span payloads.

### 4.2 Read paths (generated SQL)

**Trace by ID** — pure primary-index point read (plus partition pruning when a time hint is present):

```sql
SELECT trace_id, span_id, parent_id, payload_type, payload
FROM trace_spans
WHERE trace_id = unhex('4bf92f3577b34da6a3ce929d0e0e4736')
```

**`{ resource.service.name = "checkout" && span.http.status_code >= 500 && duration > 2s }`, last 3h, limit 20:**

Stage 1 — intrinsics via the projection (service + time + duration are all physical columns):

```sql
SELECT trace_id, span_id, timestamp_ns, duration_ns
FROM trace_spans
PREWHERE service = 'checkout'
WHERE timestamp_ns > {now - 3h} AND timestamp_ns <= {now}
  AND duration_ns > 2000000000
```

Stage 2 — attribute conditions on the index, time-pruned within the key prefix:

```sql
SELECT trace_id, span_id
FROM trace_attrs_idx
WHERE date >= today() - 1
  AND key = 'http.status_code' AND val_num >= 500
  AND timestamp_ns > {now - 3h} AND timestamp_ns <= {now}
```

The planner intersects stages (in SQL via `INNER JOIN` on `(trace_id, span_id)` when both sides are large, in the engine when one side is small), caps candidates at `PULSUS_TRACEQL_MAX_CANDIDATES` (top-K by recency), and hydrates the winning traces by primary key. Multi-condition attribute queries use the same single-pass `GROUP BY (trace_id, span_id) HAVING uniqExact(key) = n` shape as logs.

**TraceQL metrics** (`{...} | rate()`) — fully pushed down:

```sql
SELECT toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns), INTERVAL 60 SECOND) AS t,
       count() / 60 AS rate
FROM trace_spans
PREWHERE service = 'checkout'
WHERE timestamp_ns > {start} AND timestamp_ns <= {end} AND duration_ns > 2000000000
GROUP BY t ORDER BY t
```

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

**Status: logs family graduated (M1, issue #16); metrics/traces/profiles remain design intent, not observed behavior.** Co-sharding is necessary but not sufficient — Distributed plans can still merge at the initiator or ship large sets if the generated SQL doesn't cooperate, which is why graduation requires *per-stage, exact-shard-roster* evidence, not just a terminal-query spot check (three rounds of CODE review on issue #16 caught successively narrower gaps: the first draft graduated on terminal-stage-only evidence with the discovery row uncovered; the second added per-stage evidence but silently excluded the coordinator's own shard from every row — under `prefer_localhost_replica = 1` the initiator's local-shard read is logged as its own `is_initial_query = 1` row, not a separate `is_initial_query = 0` sub-query row, and a filter that keeps only `is_initial_query = 0` misses it entirely; the third accepted any shard count for fingerprint-pruned stages without deriving which shards were *expected*, so a genuinely lost `system.query_log` row was indistinguishable from correct `optimize_skip_unused_shards` pruning). The `log_samples`/`log_streams`/`log_streams_idx`/`log_metrics_5s` sharding row above, and both logs-relevant rows of the Fan-out analysis table ("LogQL stream resolution + read" and "Label/tag discovery"), are confirmed by Tier-1 evidence (docs/schemas.md §9's two-tier model): per-shard `system.query_log` + `EXPLAIN PIPELINE`, captured separately for **every** stage each shape executes (resolution, hydration, samples/rollup read, and the discovery query in its own right) and **every** shard including the coordinator's own, verified against a **client-computed expected shard roster** (a cumulative-weight slot→shard map from `system.clusters`/`system.macros`, `fingerprint % total_weight` per queried fingerprint for pruned stages) on the 4-shard fixture (`docs/benchmarks/m1-logs-read-path.md`) — showing stage-1 stream resolution executing shard-locally (reaching the full 4-shard roster), hydration/samples joining and reading shard-locally (narrowing to *exactly* the computed owning subset for narrow fingerprint predicates, with the excluded shard's absence proven, not assumed), and the label/tag discovery query itself fanning out across the full roster with only deduplicated results crossing the network. This is topology mechanics a 4-shard cluster demonstrates at any corpus scale. **Latency at Tier-2 scale (1 TB/7d) is separately tracked and unvalidated (issue #25)**; it does not gate this graduation, which is about which node does the work, not how fast. The metrics/traces/profiles rows above remain design intent until the M3/M4 multi-shard benchmarks confirm them the same way; those snapshots then join the CI regression set.

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
