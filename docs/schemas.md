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
| 3 | **No log-body text index** — `|= "substring"` scans every log line in range | `tokenbf_v1` + `ngrambf_v1` skip indexes on the body column; line filters compile to `body LIKE '%…%'` / `match(body, …)` predicates that skip non-matching granules |
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

Conventions used below: `<db>` defaults to `pulsus`; in clustered mode every table becomes `Replicated*`, and every per-shard table but `log_landing`, `metric_landing` and `trace_landing` also gets a `_dist` Distributed wrapper — those three and the cluster-wide tables get none (§7 lists all eight); `retention` clauses show defaults (`PULSUS_RETENTION_DAYS = 7`). Label keys follow the canonical label model ([architecture.md §2.3](architecture.md)): log label keys are normalized at ingest (`service.name` → `service_name`, before fingerprinting); trace attribute keys are stored verbatim; **OTLP metric names and label keys follow Prometheus v3.13.0's OTLP receiver instead** (issue #461) — the metric name gains its unit and type suffixes, attribute keys are sanitized with the reference's `key`/`key_` prefix rule and collisions merge with `;`, resource attributes become `job`/`instance` plus a `target_info` series rather than per-series labels, and the strategy is selectable with `PULSUS_OTLP_TRANSLATION_STRATEGY` ([configuration.md §5](configuration.md)); remote-write names and labels arrive already translated and are stored verbatim; the promoted physical column is named `service` on the logs, traces, and profiles tables (metrics deliberately have none — reads there are `metric_name` + `fingerprint` driven). **Every DDL block in this document mirrors `schema/schema.sql`, which is applied to a fresh ClickHouse in CI** (`crates/pulsus-schema/tests/live_schema.rs`) — an unapplyable table definition is a build failure, not a docs bug. **The generated-SQL examples below are a different matter and no suite executes one**: several carry `{placeholders}` and could not run as written. Each is instead bound to the code that renders it where one exists — §2.3's grouped instant read is asserted byte for byte against its builder by `the_grouped_statement_in_schemas_md_is_the_one_the_builder_renders`, and §4.2's shapes by the TraceQL SQL suites' doc-consistency tests. Latency figures in §9 are targets to validate, not guarantees.

---

## 2. Metrics

**Query shapes served:** instant/range PromQL over one metric with label selectors (dominant); label/series discovery; long-range dashboards (30d+); high-frequency `count by` meta-queries.

The schema's PromQL obligation is **fetch shapes, plus one reduction**: full PromQL evaluation — all functions, operators and subqueries — happens in the engine against the columns below, with a single enumerated exception. `min`, `max`, `count` and `group` over a plain instant selector are reduced in ClickHouse SQL (§2.3, issue #549), because each is exactly reproducible in a statement with no runtime condition, and so are `sum`, `avg`, `count`, `min` and `max` over `rate`, `irate` or `increase` of a plain range selector (issue #579), at any node of the query. Everything else evaluates in the engine, so language coverage is still independent of the schema ([architecture.md §5.1](architecture.md)). One planned extension: **native histogram samples get dedicated storage in M7** (a histogram-typed samples table or serialized sparse-histogram column, designed in that milestone); until then OTLP exponential histograms flatten to classic `_bucket`/`_sum`/`_count` series at ingest.

### 2.1 Tables

```sql
CREATE TABLE metric_samples (
    fingerprint  UInt128   CODEC(ZSTD(1)),   -- the series ID
    unix_milli   Int64    CODEC(DoubleDelta, ZSTD(1)),
    value        Float64  CODEC(Gorilla, ZSTD(1))
) ENGINE = MergeTree
PARTITION BY toDate(fromUnixTimestamp64Milli(unix_milli))
ORDER BY (fingerprint, unix_milli)
TTL toDateTime(fromUnixTimestamp64Milli(unix_milli)) + INTERVAL 7 DAY DELETE
SETTINGS ttl_only_drop_parts = 1,
         primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 1;
```

- **The series ID leads the key** (issue #623). `fingerprint` is the series ID: the top 32 bits of `cityHash64(metric_name)`, then the low 96 bits of the 128-bit hash of `metric_name ++ 0xFF ++` the label buffer (`pulsus_model::series_fingerprint`). One label set under two names is two series, and a metric's series share the name prefix, so they sort together as they did when `metric_name` led the key: reading one metric's IDs reads one key range. Measured: 1,000 IDs of one of 200 metrics, a day merged into one part, read 81,920 sample rows; a uniform hash puts them in about 763 granules, 6.25 million rows. Two names sharing a prefix share their granules; identity is unaffected. Among 100,000 names about one pair is expected to share a 32-bit prefix (298 pairs at 24 bits); two names sharing a prefix each read the other's samples in that range, and no series is merged (issue #635).
- **Sample tables are read only with an exact ID list and a time range.** Every flexible match — the metric name, regexes, label matchers — runs on the lookup table below, which yields the IDs.
- **Each series is contiguous** → per-series reads (every PromQL evaluation) are sequential scans of a few granules.
- **Daily partitions** on the raw table: retention drops whole partitions (`ttl_only_drop_parts`), and time predicates prune partitions before the index is even consulted.
- **No string data.** The fetch hot path moves only `(UInt64, Int64, Float64)` columns.
- **The whole key stays in the in-memory index** (issue #623). At the engine's default ratio of `0.9`, a part where the leading column is nearly unique per granule drops the later key columns from the index it keeps in memory, and a one-series read stops pruning on them: measured on a mixed-volume demo part at ratio 0.912, one series for a day selected 91 marks at the default and 2 with the setting at `1`, same answer. `metric_hist_samples` carries the same setting.
- **Resolution-agnostic.** Sample timestamps are stored **verbatim at millisecond precision** — never quantized, bucketed, or aligned. PulsusDB assumes nothing about the source scrape/export interval: 1s, 15s, 5m, or irregular push cadences all land as-is, per-series intervals may differ and drift, and the PromQL engine derives actual intervals from the data (as Prometheus does for extrapolation and staleness) rather than from configuration. Rollup tiers (§2.2) are *optional derived data* at operator-chosen resolutions; they never constrain or replace what raw ingestion accepts.
- **Admitted metric timestamp domain and runtime TTL (issue #137, mirroring #131).** Ingest admits a metric data point only if its UTC day lies in `[1970-01-01, 2106-02-06]` (days `0..=49_709`, `pulsus_model::Date::start_of_day_utc_ms_datetime_safe`); a data point outside that domain is rejected (OTLP metrics: per-point partial success; remote write: per-sample drop counted in `rejected_total`). Two wrap mechanisms motivate the gate: `PARTITION BY toDate(...)` evaluates in the 16-bit `Date` domain and wraps for days past 2149-06-06, and the delete-TTL evaluates the row timestamp in the 32-bit `DateTime` domain and wraps for instants past 2106-02-07T06:28:15Z (u32-seconds maximum, `4294967295`); day `49_710` (2106-02-07) is excluded because only part of it is u32-representable. The CREATE-time TTL shown above is superseded at runtime: `apply_ttl` re-issues `ALTER TABLE ... MODIFY TTL toDateTime(least(intDiv(unix_milli, 1000) + retention_days * 86400, 4294967295)) DELETE` on `metric_samples` and `metric_hist_samples` (§2.4) at init and on every rotation tick, so for a stored row with epoch-seconds `s = intDiv(unix_milli, 1000)` the operative expiry is `expiry(s) = min(s + retention_days * 86400, 4294967295)` — i.e. `min(configured_expiry, 2106-02-07T06:28:15Z)`. If `s + retention_days * 86400 <= 4294967295`, the expiry equals the configured instant, bit-identical to the pre-#137 expression; otherwise the expiry is `4294967295`, the actual retention is `4294967295 - s`, and the shortfall vs the configured value is `s + retention_days * 86400 - 4294967295`, which grows without bound as `retention_days` grows. For the enforced range `retention_days >= 1` (config validation rejects `< 1`, `crates/pulsus-config/src/validate.rs:285-287`), a row at the last admitted day (`49_709`, `s = 4_294_943_999`) has actual retention capped at `4_294_967_295 - 4_294_943_999 = 23_296 s ≈ 0.27 days (~6.5 hours)`. For every enforced `retention_days >= 1`, the saturating form strictly dominates the pre-#137 expression: pre-#137, a row with `s + retention_days * 86400 > 4294967295` wrapped to a ~1970-epoch expiry and its part became drop-eligible immediately or near-immediately after insert (`ttl_only_drop_parts = 1`); under the saturating form the same row becomes drop-eligible no earlier than 2106-02-07T06:28:15Z. The admission cutoff is deliberately not coupled to `retention_days`: retention is runtime-ALTERed after rows are stored (a changed `PULSUS_RETENTION_DAYS` re-ALTERs existing tables on the next rotation tick) and has no upper bound, so no admission-time gate can honor a retention value that did not exist when the row was admitted.

```sql
CREATE TABLE metric_labels (                       -- the lookup
    metric_name  LowCardinality(String),
    fingerprint  UInt128  CODEC(Delta(8), ZSTD(1)),
    labels       String  CODEC(ZSTD(5)),              -- canonical JSON, sorted keys, no __name__
    first_seen   SimpleAggregateFunction(min, Int64) CODEC(ZSTD(1)),
    last_seen    SimpleAggregateFunction(max, Int64) CODEC(ZSTD(1))
) ENGINE = AggregatingMergeTree
ORDER BY (metric_name, fingerprint);

CREATE TABLE metric_series (                       -- the activity
    day          Date,
    fingerprint  UInt128  CODEC(ZSTD(1)),
    metric_name  LowCardinality(String),
    hours        SimpleAggregateFunction(groupBitOr, UInt32)   -- bit h: samples in hour h, UTC
) ENGINE = AggregatingMergeTree
PARTITION BY day
ORDER BY fingerprint
TTL toDateTime(least((toUInt64(toUInt16(day)) + 1 + 7) * 86400, 4294967295))
SETTINGS ttl_only_drop_parts = 1;
```

- **The lookup answers every matcher; the activity answers the window** (issue #623). One kind-2 landing row feeds both views: `metric_labels_mv` writes the series' lookup row and `metric_series_mv` its activity row. `metric_labels` holds **one row per series**, keyed `(metric_name, fingerprint)`: a name equality, or a regex with a literal prefix, is a key range; other name regexes run once per distinct name; label matchers read the rows the name leaves, by `JSONExtractString(labels, '<key>')` (absent is `''`). `metric_series` holds one row per series per UTC day, `hours` a 24-bit mask of the hours the series had samples in, OR'd together on merge. No read joins the two: a read takes the IDs the lookup selects and keeps those the activity table finds in the window.
- **First and last seen.** The lookup view sets both `first_seen` and `last_seen` to the kind-2 row's `unix_milli`, the hour the writer registered the series in; merges keep the `min` and the `max`. A read takes `min(first_seen)` and `max(last_seen)` by fingerprint, exact across unmerged parts and shards. No read uses them yet.
- **Activity expires with the samples; lookup rows are kept.** `metric_series` keeps a day until its last sample has expired (`day + 1 + retention`), capped as the sample tables are. `metric_labels` has no TTL: the label sets are kept on disk permanently, by the owner's decision.
- **Exact to the hour, no `FINAL`.** The writer registers a series once per hour it has samples in (`pulsus_model::ACTIVITY_BUCKET_MS`, fixed), skipping known `(metric_name, fingerprint, hour)` triples through an in-process LRU. A read bounds `day` by the window's first and last UTC day and tests each row's `hours` against that day's hours of the window, so an unmerged row is tested on its own and a merge changes no answer. Measured: 300 random windows of up to six days over 1,000 series active in a random tenth of 168 hours answered exactly the hourly form's sets (`activity_is_exact_to_the_hour`). The discovery endpoints built on this table (`/api/v1/series`, `/labels`, `/label/{name}/values`, docs/api.md §3.3) therefore answer the series active in the window's hours: a bounded superset of the reference's exact-sample-window set, never a subset and never a false empty.

#### The four statements

`up{job="api", status=~"5.."}` over 2026-09-07 22:30 to 23:30 UTC. The window renders as `day BETWEEN d0 AND d1` and the mask `multiIf(day = d0 AND day = d1, bits(h0, h1), day = d0, bits(h0, 23), day = d1, bits(0, h1), 16777215)`, `bits(a, b) = (1 << (b + 1)) - (1 << a)`; across two days the first branch cannot hold and renders `0`. A regex matcher adds one `0 * match('', <pattern>) = 0` line to the activity read: the compile probe (§2.3).

Statement 1, series IDs — the fallback's `IN` set and the `info()` cardinality probe:

```sql
SELECT fingerprint
FROM metric_series
WHERE day BETWEEN '2026-09-07' AND '2026-09-07'
  AND 0 * match('', '(?-s)^(?:5..)$') = 0
  AND bitAnd(hours, multiIf(day = '2026-09-07' AND day = '2026-09-07', 12582912, day = '2026-09-07', 12582912, day = '2026-09-07', 16777215, 16777215)) != 0
  AND fingerprint IN (
    SELECT fingerprint
    FROM metric_labels
    WHERE metric_name = 'up'
      AND JSONExtractString(labels, 'job') = 'api'
      AND match(JSONExtractString(labels, 'status'), '(?-s)^(?:5..)$')
  )
```

Statement 2, series with their labels — discovery, the label-cache sweep (no matcher, the cache window) and the fan-out's discovery (scoped to the cache's names and IDs):

```sql
SELECT fingerprint, any(name) AS metric_name, any(label_text) AS labels
FROM (
  SELECT fingerprint, metric_name AS name, labels AS label_text
  FROM metric_labels
  WHERE metric_name = 'up'
    AND JSONExtractString(labels, 'job') = 'api'
    AND match(JSONExtractString(labels, 'status'), '(?-s)^(?:5..)$')
    AND fingerprint IN (
      <statement 1's FROM … WHERE …>
    )
)
GROUP BY fingerprint
ORDER BY metric_name, fingerprint
```

Statement 3, names — `/label/__name__/values`, and with name matchers and a `LIMIT` the degraded fan-out's names probe. With no matcher it reads the activity table alone (#472):

```sql
SELECT DISTINCT metric_name
FROM metric_series
WHERE day BETWEEN '2026-09-07' AND '2026-09-07'
  AND bitAnd(hours, multiIf(…)) != 0
ORDER BY metric_name
```

Statement 4, labels for IDs the request window already bounds — the fallback fetch's label hydration, over the IDs that returned samples:

```sql
SELECT fingerprint, any(name) AS metric_name, any(label_text) AS labels
FROM (
  SELECT fingerprint, metric_name AS name, labels AS label_text
  FROM metric_labels
  WHERE metric_name IN ('up', 'down')
    AND fingerprint IN (toUInt128('101'), toUInt128('205'))
)
GROUP BY fingerprint
ORDER BY metric_name, fingerprint
```

`any` collapses the copies an unmerged lookup holds; a series with no lookup row of its own is absent, never returned with empty labels.

- Feeds the reader's **label cache**: `fingerprint → labels` + `metric_name → [fingerprints]`, refreshed every `PULSUS_CACHE_TTL` over the active window (`PULSUS_CACHE_WINDOW`, default 24h). Matchers — including regex and negative matchers — evaluate against this map in-process (finding #7).
- **The cache is time-scoped**: it may answer only queries whose data window lies inside the cache window. A series alive last week but silent today is absent from the cache, so answering a historical query from it would return false empties. Older ranges resolve directly from these tables through the statements above, whose window is exact to the hour: a series emitting at 10:35 has hour 10 in its day's mask, so a 10:30–10:40 query finds it, and a series first seen after the window has no bit in it. Correctness tests cover sub-hour historical windows and series appearing only after the query end.

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

`metric_type` also drives the planner: counter functions on rollup tiers are only legal because the type is known. **`updated_ns` is the `ReplacingMergeTree` version column** (issue #26 fix, mirroring `log_streams`' `ReplacingMergeTree(updated_ns)`): every non-key column here (`metric_type`/`help`/`unit`) sits outside `ORDER BY metric_name`, so without a version column a merge's latest-wins outcome would be nondeterministic — unacceptable given `metric_type` drives planner correctness. The writer emits a new row (receiver-injected `now_ns`) only when the incoming `(metric_type, help, unit)` tuple differs from the last value it durably emitted for that `metric_name`, or the hour has turned since it did (a bounded last-value cache, promoted only when the block commits; issue #623) — idempotent on repeats, and a type change that later reverts (A→B→A) re-emits on the second A rather than being suppressed by a static once-only registration. The hourly resend bounds how long one writer's descriptor can stand over another writer's different one; on a single-host demo every push re-sending every descriptor came to 345,362 rows an hour, 39% of all landed rows.

**`metric_metadata.metric_name` is keyed by the BASE family name, never a derived-series name** (issue #27 architect plan, task-manager-pinned docs contract). A receiver that flattens one metric descriptor into several physical series — a histogram's `<name>_bucket`/`<name>_sum`/`<name>_count`, an exponential histogram's identical shape, or a summary's quantile series plus `<name>_sum`/`<name>_count` — registers exactly **one** `metric_metadata` row for `<name>` itself, typed `histogram`/`summary`, never one row per suffixed series. **Any consumer resolving a metric family's type must strip a trailing `_bucket`, `_sum`, or `_count` suffix (and, for a Summary's quantile series, no suffix at all — the quantile series shares the base name verbatim, distinguished only by its `quantile` label) before looking the family up in `metric_metadata`.** This is the contract issue #30 (label cache)/#31/#32 (PromQL planner, counter-function legality, rollup eligibility) implement against — not tribal knowledge. A lookup that fails to strip suffixes will find no metadata row for `<name>_bucket`/`<name>_sum`/`<name>_count` at all (they were never registered under those names) and must not misinterpret that absence as "unknown metric".

### 2.2 Downsampling tiers

Downsampling happens **entirely inside ClickHouse** with classic insert-triggered materialized views — no external driver, no scheduled jobs. One table per tier (`metric_samples_5m`, `metric_samples_1h`); monthly partitions, long TTLs:

```sql
CREATE TABLE metric_samples_5m (
    fingerprint   UInt128                                 CODEC(Delta(8), ZSTD(1)),
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
ORDER BY (fingerprint, ts)
TTL ts + INTERVAL 90 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW metric_samples_5m_mv TO metric_samples_5m AS
SELECT fingerprint,
       toStartOfInterval(fromUnixTimestamp64Milli(unix_milli), INTERVAL 300 SECOND) AS ts,
       min(value) AS val_min, max(value) AS val_max, sum(value) AS val_sum,
       sum(value * value) AS val_sum_sq, count() AS val_count,
       min(unix_milli) AS first_time, max(unix_milli) AS last_time,
       argMinState(value, unix_milli) AS first_value,
       argMaxState(value, unix_milli) AS last_value
FROM metric_samples
GROUP BY fingerprint, ts;
-- metric_samples_1h_mv: identical shape, INTERVAL 3600, also reading metric_samples
```

- **Insert-triggered, additive, real-time.** Every aggregate above is a mergeable state, so per-block partial aggregates from any insert order converge under `AggregatingMergeTree` merges — correctness needs no windowing, no refresh schedule, and no "wait for compaction" lag. Tiers are populated to within one insert batch of `now()`, so a tier can serve the *entire* time range of a query. Both tier MVs read the raw insert block directly (no MV-on-MV chaining).
- **Counter resets are handled at query time, from bucket boundaries — and this is an approximation whenever a reset falls inside a bucket.** A per-sample reset-corrected delta cannot be computed inside an incremental MV (an insert block doesn't reliably contain each sample's predecessor). `rate`/`increase` fetch per-bucket `(first_time, last_time, first_value, last_value)` and the engine reconstructs increase over the bucket sequence: intra-bucket `last − first` (a drop marks a reset → contribute `last`), plus boundary deltas against the previous bucket's `last_value` with the same rule — O(buckets) work. **Accuracy caveat, stated precisely:** *any* reset inside a bucket loses information. `100,150,10,40` reconstructs 40 where the true increase is 90; worse, `100,150,10,140` reconstructs 40 where the truth is 190 and no reset is even detectable from boundaries. This is why counter functions **prefer raw samples wherever raw exists** (§2.3 tier policy) and why tier-served counter segments are always flagged approximate. The M3 accuracy report must cover single-reset-in-bucket and undetectable-reset cases, not just multiple resets.
- **Gauge pushdown is a specific function list, not "all of them":** `avg/min/max/sum/count_over_time` from `val_sum/val_min/val_max/val_count`, `stddev/stdvar_over_time` from `val_sum_sq`, `last_over_time` from `last_value`, `present_over_time` from `val_count > 0`. Functions needing sample positions or full distributions (`quantile_over_time`, `mad_over_time`, `changes`, `resets`, ...) route to raw. Tiered gauge results are **bucket-aligned**: a window whose edge falls inside a bucket includes that whole bucket — exact only when windows align with bucket boundaries, otherwise a defined approximation (flagged via `X-Pulsus-Explain`).
- **Late and duplicate data.** Aggregate-state merging makes insert *order* irrelevant, but not insert *multiplicity*: a replayed remote-write batch inflates `val_sum`/`val_count` in tiers permanently, and late samples mutate buckets that earlier queries already read. Policy: at one `(series, millisecond)` the raw read path answers a histogram if any histogram is stored there and no float survives, else each distinct float value-bit identity in arrival order; tiers cannot do even that — a documented tier-accuracy caveat, measured in M3 with deliberate duplicate/late-data injection. Two different values stored at one `(series, millisecond)` are both returned: the read path does not resolve a contradiction the stored columns cannot settle. Ahead of the read path, a content-identical push that reaches the same writer inside `PULSUS_INGEST_DEDUP_WINDOW` stores nothing at all (issue #494), so a client retrying after a network timeout no longer inflates anything; a retry that reaches a different writer process still does.
- The schema controller only issues DDL: it creates tier tables + MVs, recreates an MV when its config checksum changes, and offers a one-shot chunked `INSERT ... SELECT` backfill when a tier is first enabled on pre-existing data. Nothing runs on a timer. Note the write-cost consequence: every raw insert block is aggregated twice (once per tier MV); the M3 benchmark measures insert throughput and part counts with tiers on and off.

### 2.3 Read paths (generated SQL)

**`rate(http_requests_total{job="api", status=~"5.."}[5m])`, 24h window, 60s step.** The label cache resolves both matchers (regex included) in-process → sorted fingerprints. One fetch:

```sql
SELECT fingerprint, unix_milli, value
FROM metric_samples
WHERE unix_milli >  {start - 300000 - lookback}
  AND unix_milli <= {end}
  AND fingerprint IN (toUInt128('101'), toUInt128('205'), toUInt128('990'), ...)
ORDER BY fingerprint, unix_milli
```

**Each selector reads the two sample tables in two concurrent statements** (issue #623): the float read above and the same selection over `metric_hist_samples`, sent together, merged in the reader with a histogram winning a key both tables hold. The second statement costs server time, not latency. A statement over a table holding none of the metric's rows still costs about 3 ms; the concurrent pair overlaps it, and one statement over both tables (`UNION ALL`) pays it in series — measured on a single-host demo, one float series: the pair answered in 6.69 ms, the one statement in 10.11 ms (median of 40, one client). Skipping the statement a series' cached value kinds say is empty is not done: a sample of the other kind with an old timestamp, ingested after the cache's last refresh, would be left out.

Partition pruning (daily) → primary-index pruning (the IDs, which one metric's share a prefix of) → sequential per-series reads. Evaluation (extrapolation, resets, staleness) happens in the engine, series-first — **for every query but the four below**. Fingerprint lists ≥ 500 split into parallel chunk fetches; selectors matching more than `PULSUS_CACHE_MAX_SERIES` fall back to:

```sql
... AND fingerprint IN (
SELECT fingerprint
FROM metric_series
WHERE day BETWEEN {first day} AND {last day}
  AND 0 * match('', '(?-s)^(?:5..)$') = 0
  AND bitAnd(hours, {the window's hours of each day}) != 0
  AND fingerprint IN (
    SELECT fingerprint
    FROM metric_labels
    WHERE metric_name = 'http_requests_total'
      AND JSONExtractString(labels, 'job') = 'api'
      AND match(JSONExtractString(labels, 'status'), '(?-s)^(?:5..)$')
  )
  )
```

— statement 1 of §2.1; the samples that return are hydrated with statement 4, over their own IDs.

**Two details in that regex predicate.** ClickHouse's `match()` compiles with RE2's `dot_nl` option set, so `.` matches a newline there and does not in RE2 — and therefore not in the reference, which compiles matchers with Go's `regexp`. Every pattern this path renders is prefixed with RE2's own `(?-s)` flag group to restore the reference reading; a `(?s)` the user wrote still overrides it. And because ClickHouse compiles a pattern only when it evaluates `match()` on a row, a selector naming a metric with **no rows in the window** would never reach RE2 at all and an invalid pattern would answer an empty `200` instead of the reference's `400`. The activity read therefore carries one `0 * match('', <pattern>) = 0` line per regex matcher: ClickHouse folds the constant during query analysis and rejects an uncompilable pattern before reading a part (issue #315). The patterns themselves run on the lookup rows, inside the activity read's `IN` set, so no activity row evaluates one. A matcher set with no regex renders no probe at all.

**Clustered honesty:** on a clustered deployment this fallback fetch reads `_dist` names throughout — `metric_samples_dist`, and the nested subqueries' `metric_series_dist` and `metric_labels_dist` — and additionally injects `distributed_product_mode = 'local'`, rewriting those nested subqueries to each shard's **local** tables (the same rewrite already applied to the traces metrics semi-join). It is exact because one kind-2 landing row writes a series' activity row and its lookup row on the node that received the push, beside that push's samples. Every series read that filters on labels or returns them carries the same setting, the label-cache sweep included. Without it, ClickHouse's default `distributed_product_mode = 'deny'` rejects the nested `_dist`-inside-`_dist` shape as a double-distributed `IN` (`DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED`).

**`max by (status) (http_requests_total)`, one hour, 15 s step — the grouped instant read (issue #549).** `min`, `max`, `count` and `group` over a **plain** instant selector — no range, no `offset`, no `@`, no subquery context, one concrete metric name — do not take the fetch above. They compile into ONE statement per fingerprint chunk, which returns the answer already reduced:

```sql
WITH 1782907200000 AS grid_start, 15000 AS grid_step, 241 AS grid_n, 300000 AS lookback,
     [toUInt128('101'), toUInt128('205'), toUInt128('990')] AS fps,
     CAST([0, 1, 0], 'Array(UInt32)') AS gids
SELECT gid, min(gi) AS gi_start, max(gi) AS gi_end, any(agg) AS agg, any(flags) AS flags
FROM (
  SELECT gid, gi, agg, flags,
    sum(is_new) OVER (PARTITION BY gid ORDER BY gi ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run
  FROM (
    SELECT gid, gi, agg, flags,
      toUInt8(gi != lagInFrame(gi) OVER w + 1
              OR reinterpretAsUInt64(agg) != reinterpretAsUInt64(lagInFrame(agg) OVER w)
              OR flags != lagInFrame(flags) OVER w) AS is_new
    FROM (
      SELECT gid, gi,
        if(countIf(NOT is_hist AND NOT isNaN(v)) = 0, argMaxIf(v, fingerprint, NOT is_hist),
           maxIf(v, NOT is_hist AND NOT isNaN(v))) AS agg,
        toUInt8(if(countIf(NOT is_hist) > 0, 1, 0) + if(countIf(is_hist) > 0, 2, 0)) AS flags
      FROM (
        SELECT gid, fingerprint, v, is_hist,
          arrayJoin(range(
            toUInt32(least(toInt64(grid_n),
              if(ts <= grid_start, 0, intDiv(ts - grid_start + grid_step - 1, grid_step)))),
            toUInt32(least(toInt64(grid_n),
              if(cover_end <= grid_start, 0, intDiv(cover_end - grid_start + grid_step - 1, grid_step))))
          )) AS gi
        FROM (
          SELECT transform(fingerprint, fps, gids, CAST(0, 'UInt32')) AS gid, fingerprint,
            ts, v, is_hist, stale,
            least(leadInFrame(ts, 1, toInt64(1782910800000) + lookback + 1) OVER (
                    PARTITION BY fingerprint ORDER BY ts, is_hist
                    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING),
                  ts + lookback) AS cover_end
          FROM (
            SELECT fingerprint, unix_milli AS ts, value AS v, CAST(0, 'UInt8') AS is_hist,
                   reinterpretAsUInt64(value) = 9218868437227405314 AS stale
            FROM metric_samples
            WHERE unix_milli > 1782906900000 AND unix_milli <= 1782910800000 AND fingerprint IN fps
            UNION ALL
            SELECT fingerprint, unix_milli AS ts, CAST(0, 'Float64') AS v, CAST(1, 'UInt8') AS is_hist,
                   reinterpretAsUInt64(sum) = 9218868437227405314 AS stale
            FROM metric_hist_samples
            WHERE unix_milli > 1782906900000 AND unix_milli <= 1782910800000 AND fingerprint IN fps
          )
        )
        WHERE NOT stale
      )
      GROUP BY gi, gid
    )
    WINDOW w AS (PARTITION BY gid ORDER BY gi ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
  )
)
GROUP BY gid, run
ORDER BY gid, gi_start
```

**How to read it, bottom up.** The union gives one row per stored sample from both channels with a stale flag; `leadInFrame` gives each sample a `cover_end`, the first millisecond it stops being its series' most recent one — the next sample's timestamp, or `ts + lookback`; `arrayJoin(range(...))` turns that interval into the grid indices the sample covers; `GROUP BY gi, gid` reduces; and the two window functions collapse consecutive grid points carrying the same answer into one **run** row. A stale sample still occupies its interval — it blocks the earlier sample — and is then dropped by `WHERE NOT stale`, which is the reference's rule: a stale marker makes the series absent rather than falling back to an older sample.

**Three things that are not obvious.** The group key never enters the SQL: `gid` is assigned in our process from the label sets the resolver already returned, by the same function the evaluator's aggregation uses, and reaches the statement as the `fps`/`gids` arrays — so `by`, `without` and bare render byte-identical text outside the `gids` array. The extremum is written out rather than delegated to ClickHouse's own `max`, which answers `nan` over `[NaN, 1, 3]` and `3` over `[1, NaN, 3]`; the guarded expression answers `3` on every ordering and a NaN only when no member is a number, and the NaN it answers is the payload a member carried. And `flags` bit 0 says the group had a float member — a `min`/`max` group with none is dropped, which is the reference's not-seen rule — while `count` and `group` carry no `flags` at all, because they count a histogram member rather than ignoring it.

**What it costs and what it buys.** The database does more work and this process does much less. On 400 series in 4 groups over an hour at a 15 s step, the statement returns 964 rows where the fetch above returns 96,400, and the bytes the coordinator sent this client went from 691,011 to 9,389 — measured, `ProfileEvents['NetworkSendBytes']` plus the statement text, median of three. **The threshold is a heuristic, and the row count it stands in for is a transition count.** A run ends when the group's answer CHANGES, so what the statement returns depends on how often that happens — not on how many series feed the group. Measured on two corpora with the same 100 series in the same 1 group: 39 rows where the count is flat through the middle, 199 rows where it changes at nearly every grid point. The transition count is not knowable before the statement runs, so the compiler uses what it can compute from the resolver's answer alone and declines below `series >= 2 * groups` — as many groups as series is the shape with no grouping work to do, and that is a proxy for the shape rather than a bound on rows: measured on a declined corpus of three constant series in one group each, the push returns 3 rows against the raw read's 723 under every one of the four operations. Bytes are not bounded in general — the run row is wider than a sample row — and on a 100-series expiry corpus the pushed read moved 5,433 bytes against the raw read's 2,754. Rows ARE bounded: each sample opens its coverage at one grid index and closes it at another, so `pushed_rows <= 2 * raw_rows`.

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

**`count by (job) (up)`** — **the cache-only answer was withdrawn** (issue #33; [architecture.md §5.1](architecture.md)): the label cache records activity at bucket granularity and cannot tell "had a sample inside the 5-minute staleness lookback" from "active somewhere in an up-to-24h-old bucket", and it returned 69 series where the reference correctly returned 57. `count`/`group` read samples like every other aggregation. What issue #549 changed is the SHAPE of that read, not the rule: over a plain instant selector it is one statement per fingerprint chunk that applies the lookback in SQL (§2.3), so the answer is still computed from real samples.

### 2.4 Native histogram samples

The M7 extension foreshadowed in §2 lands as a **separate, dedicated samples table** — `metric_hist_samples` — storing Prometheus native (sparse) histograms in their integer wire form. Float samples in `metric_samples` (§2.1) are untouched: the float fetch hot path, its EXPLAIN gate, and its migration checksum cannot regress. The two tables share the same identity, ordering, partitioning, TTL, and — in clustered mode — sharding key, so a series' float and histogram samples always co-reside (see co-sharding note below).

```sql
CREATE TABLE metric_hist_samples (
    fingerprint        UInt128   CODEC(ZSTD(1)),
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
ORDER BY (fingerprint, unix_milli)
TTL toDateTime(fromUnixTimestamp64Milli(unix_milli)) + INTERVAL 7 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;
```

- **Identity and access shape are byte-identical to `metric_samples`** (§2.1): the series ID leads the key and clusters each series, `unix_milli` orders within it — same PK/ordering key `(fingerprint, unix_milli)`, same daily partitioning, same `ttl_only_drop_parts` retention. Per-series reads are the same sequential granule scans; the codecs on `fingerprint`/`unix_milli` match §2.1 exactly. Timestamps are stored **verbatim at millisecond precision** (§2.1's resolution-agnostic rule).
- **Runtime TTL and admitted timestamp domain (issue #137).** The CREATE-time TTL above is superseded at runtime exactly as §2.1's: as of #137, `apply_ttl` includes `metric_hist_samples` (previously the table was absent from the runtime ALTER list, so a `PULSUS_RETENTION_DAYS` change did not propagate to it — #137 closes that gap too) and re-issues the same saturating `MODIFY TTL toDateTime(least(intDiv(unix_milli, 1000) + retention_days * 86400, 4294967295)) DELETE` at init and on every rotation tick, with §2.1's expiry formula and saturation semantics. Admission on every hist ingest path is gated by §2.1's domain (days `0..=49_709`).
- **Sparse wire form, lossless for both schemas.** The `schema`, `zero_threshold`, `zero_count`, `count`, `sum` scalars plus the positive/negative span-and-delta arrays are the integer sparse-histogram encoding (`Array(Int32)`/`Array(UInt32)` span offsets/lengths, `Array(Int64)` delta-encoded bucket counts). This is lossless for the standard exponential schema (−4..8) and for NHCB (schema −53, which populates `custom_values` and leaves the negative/zero fields empty). Each array carries `CODEC(ZSTD(1))` (§8's "everything wrapped in `ZSTD(1)` minimum").
- **Co-sharded with floats.** In clustered mode `metric_hist_samples` reuses the Metrics family sharding key `cityHash64(fingerprint)` (§7) — the byte-identical expression `metric_samples`, its tiers, and `metric_series` use. Its `_dist` wrapper is `CREATE TABLE metric_hist_samples_dist AS metric_hist_samples ENGINE = Distributed('{cluster}', pulsus, metric_hist_samples, cityHash64(fingerprint))`. Consequence: a series' float samples, histogram samples, and `metric_series` metadata land on the **same shard**, so the read path's co-load of both sample types for one series stays shard-local.

**Counter-reset hint (issue #125).** `metric_hist_samples` gains one additive column — never a mutation of the frozen id-23 `CREATE`; the `_dist` wrapper gains the cluster-gated twin (migrations 27/28):

```sql
ALTER TABLE metric_hist_samples ADD COLUMN IF NOT EXISTS counter_reset_hint UInt8 DEFAULT 0;
```

`counter_reset_hint` stores the Prometheus per-sample counter-reset hint byte: `0` = unknown, `1` = counter reset, `2` = not a counter reset, `3` = gauge histogram. Pre-#125 rows read back `0` (the `DEFAULT`) — semantically exact (unknown), no data migration. The read path decodes it into the query-time histogram, where it drives the PromQL not-counter/not-gauge/reset-collision annotations and the reset-detection shortcuts. **Ingest writes `0` today:** OTLP exponential-histogram points carry no monotonicity flag and delta temporality is rejected at the ingest seam, so `3` (gauge) is unproducible until a gauge-capable ingest surface lands (issue #140). The column is fixed-width `UInt8` appended to the existing hist SELECT list — same table, same PK/ordering, same granule pruning, no extra round-trips.

**Per-series value type.** The kind-2 landing row carries `value_type`, the per-series float/histogram discriminator (`0 = float`, `1 = histogram`). Since issue #623 no stored table keeps it: `metric_series` became the day activity, and no reader read the column.

**Writer contract (M7-A4).** `value_type` is a *per-row* discriminator on the kind-2 landing row, and it is part of the writer's registration key `(metric_name, fingerprint, activity-bucket, value_type)`. Registration is driven from **both** float samples (`value_type = 0`) and native-histogram samples (`value_type = 1`), so a series that carries both a float and a histogram sample in one activity bucket lands **two** kind-2 rows — a "mixed" series is the `groupBitOr(bitShiftLeft(1, value_type))` rollup over those rows (`3` = mixed), never stored. Within a single ingest request the writer never emits a float and a native histogram at the same `(metric_name, fingerprint, unix_milli)` — the histogram wins and the colliding float is dropped. Across independent requests both a `metric_samples` and a `metric_hist_samples` row may coexist at one key by design. **The read path reads both tables, in two concurrent statements, and does *not* consult `value_type` for routing** (#112), and the reason is durability rather than merge determinism: the views over `metric_landing` are not a transaction ([ingest-one-source-table.md](ingest-one-source-table.md) §9, D1; the 300-trial measurement of that shape — one throwing view and three healthy siblings over one source table — is §4.1, "What a failing view leaves behind"), so `metric_hist_samples_mv` could commit for a block while `metric_series_mv` did not, leaving a stored histogram sample whose `value_type = 1` row never existed. The landing insert now fails when a view does not write (issue #623) and the writer's resend writes what is missing, but the read still does not route on a type: a series' samples are whatever the two tables hold. Under the dual read that sample is still returned once the series resolves from any other row; under a type mask the registration rows that did survive mask to `1`, `metric_hist_samples` is never read for that fingerprint, and the sample stays invisible for as long as it is retained. Coexistence itself is not what a mask breaks — a mixed series masks to `3` and reads both tables — the mask's own completeness is. See [ADR 0005](decisions/0005-native-histogram-storage.md) §(c), where the superseded type-mask design is marked as such.

---

## 3. Logs

**Query shapes served:** stream-selector reads with time bounds (dominant); line-filter search (`|=`, `|~`); LogQL metric queries (`rate`, `count_over_time`); label discovery; live tail.

### 3.1 Tables

**One push is one insert into `log_landing`, and five materialized views fan its rows out** (issue #603; the pattern, its guarantee and its costs are [ingest-one-source-table.md](ingest-one-source-table.md)). The four tables below are written only by those views — the writer names none of them — so their DDL is what a read sees, and the landing table's is what a write sends.

```sql
-- The one table a logs push inserts into. `kind` discriminates the three
-- landed row shapes: 0 a log line, 1 a stream registration, 2 a pre-
-- aggregated pattern row. A column a kind does not use is left at its
-- type's zero value, which is why the kind-1 and kind-2 rows are cheap.
-- `event_id` is filled by the server (the writer's row type omits it), so
-- a resent block cannot reuse an identity the first send minted.
-- There is NO `_dist` wrapper, in either mode: routing a push through a
-- Distributed table would split it into one insert per shard and would
-- return before the shards held the rows, either of which ends "one push
-- is one block" (§7).
CREATE TABLE log_landing (
    event_id             UUID DEFAULT generateUUIDv7(),
    received_ms          Int64  CODEC(DoubleDelta, ZSTD(1)),  -- wall clock at admission
    kind                 UInt8  CODEC(ZSTD(1)),               -- 0 line, 1 stream, 2 pattern
    service              LowCardinality(String),              -- kinds 0, 1
    fingerprint          UInt128  CODEC(Delta(8), ZSTD(1)),   -- every kind
    timestamp_ns         Int64  CODEC(DoubleDelta, ZSTD(1)),  -- kind 0 the line's time; kind 2 the bucket floor
    severity             Int8  CODEC(ZSTD(1)),                -- kind 0
    body                 String  CODEC(ZSTD(1)),              -- kind 0
    structured_metadata  String  CODEC(ZSTD(1)),              -- kind 0
    month                Date  CODEC(ZSTD(1)),                -- kind 1
    labels               String  CODEC(ZSTD(5)),              -- kind 1, canonical JSON
    updated_ns           Int64  CODEC(DoubleDelta, ZSTD(1)),  -- kind 1
    pattern              String  CODEC(ZSTD(1)),              -- kind 2
    pattern_count        UInt64  CODEC(T64, ZSTD(1))          -- kind 2
) ENGINE = MergeTree
PARTITION BY toStartOfHour(fromUnixTimestamp64Milli(received_ms))
ORDER BY (kind, service, fingerprint, timestamp_ns)
SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600;
-- `apply_ttl` issues at init and on every rotation tick:
--   ALTER TABLE log_landing MODIFY TTL
--     toDateTime(intDiv(received_ms, 1000)) + INTERVAL {retention_hours} HOUR DELETE
--   ALTER TABLE log_landing MODIFY SETTING
--     non_replicated_deduplication_window = {window}   -- replicated_… on a cluster
--   ALTER TABLE log_landing MODIFY SETTING
--     replicated_deduplication_window_seconds = 3600   -- cluster only
```

**The five views.** Three kinds, five views: `log_streams_idx_mv` is an `ARRAY JOIN` over the very same kind-1 row `log_streams_mv` takes, rather than a kind of its own, because a kind of its own would make the writer land the `labels` blob twice per stream. It is correct whether the server applies its `WHERE` before or after the `ARRAY JOIN` — a kind-0 or kind-2 row carries `labels = ''`, `JSONExtractKeysAndValues('', 'String')` is the empty array, and an `ARRAY JOIN` over an empty array emits no row.

```sql
CREATE MATERIALIZED VIEW log_samples_mv TO log_samples AS
SELECT service AS service, fingerprint AS fingerprint,
       timestamp_ns AS timestamp_ns, severity AS severity, body AS body,
       structured_metadata AS structured_metadata
FROM log_landing WHERE kind = 0;

CREATE MATERIALIZED VIEW log_streams_mv TO log_streams AS
SELECT month AS month, fingerprint AS fingerprint, service AS service,
       labels AS labels, updated_ns AS updated_ns
FROM log_landing WHERE kind = 1;

CREATE MATERIALIZED VIEW log_streams_idx_mv TO log_streams_idx AS
SELECT month, kv.1 AS key, kv.2 AS val, fingerprint
FROM log_landing
ARRAY JOIN JSONExtractKeysAndValues(labels, 'String') AS kv
WHERE kind = 1;

-- {res_ns} is PULSUS_LOG_ROLLUP_RESOLUTION; the view and its target are
-- named for it (log_metrics_5s_mv by default).
CREATE MATERIALIZED VIEW log_metrics_5s_mv TO log_metrics_5s AS
SELECT fingerprint AS fingerprint,
       intDiv(timestamp_ns, {res_ns}) * {res_ns} AS bucket_ns,
       count() AS count,
       sum(length(body)) AS bytes
FROM log_landing WHERE kind = 0
GROUP BY fingerprint, bucket_ns;

-- Pattern rows are pre-aggregated in Rust before they land, so this view
-- copies one landed row per (fingerprint, bucket, template) through and
-- aliases `pattern_count` back to the target's `count`. `timestamp_ns`
-- carries the bucket floor on a kind-2 row.
CREATE MATERIALIZED VIEW log_patterns_mv TO log_patterns AS
SELECT fingerprint AS fingerprint, timestamp_ns AS bucket_ns,
       pattern AS pattern, pattern_count AS count
FROM log_landing WHERE kind = 2;
```

**Neither the rollup view nor the index view is second-level any more.** Both used to read a target table (`log_samples`, `log_streams`); both now read `log_landing`, so exactly one view fires per landed row per target. For `log_metrics_5s` that is load-bearing rather than tidy: its `count`/`bytes` are `SimpleAggregateFunction(sum, UInt64)`, and a second-level view firing as well as a first-level one would double every count.

```sql
CREATE TABLE log_streams (
    month        Date,                          -- toStartOfMonth(first write in month)
    fingerprint  UInt128,
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
    fingerprint  UInt128
) ENGINE = ReplacingMergeTree
PARTITION BY month
ORDER BY (key, val, fingerprint);
-- populated by log_streams_idx_mv over log_landing's kind-1 rows:
--   ARRAY JOIN JSONExtractKeysAndValues(labels, 'String')
```

```sql
CREATE TABLE log_samples (
    service       LowCardinality(String),
    fingerprint   UInt128,
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
    fingerprint  UInt128,
    bucket_ns    Int64,                          -- intDiv(timestamp_ns, {res_ns}) * {res_ns}
    count        SimpleAggregateFunction(sum, UInt64),
    bytes        SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(bucket_ns))
ORDER BY (fingerprint, bucket_ns);
-- populated by log_metrics_5s_mv over log_landing's kind-0 rows
```

```sql
-- Log patterns (M7-C3, issue #171): ingest-extracted log templates, batch-pre-
-- aggregated per (fingerprint, bucket_ns, pattern) by the WRITER (extraction
-- is Rust, not SQL, and the aggregate still forms at admission; the rows
-- then reach this table through log_patterns_mv off the landing table —
-- one landed kind-2 row per aggregate, never one per line). The fixed 10s ingest
-- bucket is a code constant (patterns::PATTERN_BUCKET_NS). `count` is a
-- mergeable SimpleAggregateFunction(sum) — the template identity is a pure
-- function of the line, so counts sum correctly across batches, shards,
-- replicas, and retries. Kill-switch: PULSUS_LOG_PATTERNS (default true).
CREATE TABLE log_patterns (
    fingerprint  UInt128,
    bucket_ns    Int64,                          -- intDiv(timestamp_ns, 10s) * 10s
    pattern      String  CODEC(ZSTD(1)),
    count        SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(bucket_ns))
ORDER BY (fingerprint, bucket_ns, pattern)
TTL toDateTime(fromUnixTimestamp64Nano(bucket_ns)) + INTERVAL 7 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;
```

Raw log timestamps in `log_samples` are stored verbatim at nanosecond precision — the rollup is derived, and only eligible when the query step is a multiple of the configured rollup resolution; otherwise the planner counts raw rows.

- **`log_patterns` primary-key order is `(fingerprint, bucket_ns, pattern)`** (issue #171, `bucket_ns` **before** `pattern`): a `/api/logs/v1/patterns` read is a `fingerprint IN (...)` + bounded `bucket_ns` window, so putting `bucket_ns` second prunes at the PK level inside each fingerprint's key range, not only via daily partitions. The template is a **deterministic, stateless** token-class rendering of the line body (digit/length classification, `key=value`/`key:value` awareness, 1 KiB prefix / 64-token / 512-byte caps, whitespace runs collapse) — NOT a drain-style online clusterer, whose order-dependent per-stream mutable state would emit different templates on different shards/replicas/retries and break both the mergeable `sum` and idempotent re-inserts. Templates are normalized (whitespace-collapsed), documented as "not round-trip matchable". **Count semantics** are exact on the clean ingest path and **best-effort approximate under ingest-failure re-sends**, at parity with `log_metrics` (§2.2's tier caveat): the writer never auto-replays a block that could have committed (an `InsertUncertain` batch is spooled audit-only, never re-inserted), so the only over-count vector is a client-level re-send after a 5xx/timeout ack (steady-state zero), and **there is no under-count vector left from a failing flush** (issue #603): the pattern rows ride the same landed block as the lines they were extracted from, so there is no separate patterns insert to fail, and a push whose block does not land lands no lines either. What remains is the fan-out residual every target of `log_landing` shares — a view that throws while the server processes the insert leaves it unrecorded which targets kept the block's rows (§4.1, "What a failing view leaves behind"). A single request that emits more than 10 000 distinct templates (pathological — the extraction caps make templates low-cardinality by construction) drops the excess from pattern accounting only (log lines untouched, counted via the writer's `patterns_dropped_total`), an under-count event folded into the same approximate semantics; the next batch resumes discovery.

- **`service` leads the samples ordering key.** This is the one label promoted to a physical column (populated from resource `service.name`; user-visible as the `service_name` label per the canonical label model), and it earns it three ways: (a) OpenTelemetry guarantees `service.name` on every resource (the collector defaults it to `unknown_service`), so it is never missing; (b) it is the natural clustering dimension — a service's streams sit contiguously, so service-scoped searches (the human default) read a compact range instead of granules scattered across all tenants of the table; (c) **the planner can always supply it**: stream resolution returns full label sets, so even a query that never mentions `service` gets `service IN (...)` injected from the resolved streams, keeping the primary index engaged. No other label is materialized — finding #9's counter-argument (row width, schema coupling) applies to everything else.
- **`(service, fingerprint, timestamp_ns)` is fixed** (finding #4). Per-stream time reads are sequential; multi-stream reads within one service are near-sequential.
- **Body skip indexes** (finding #3): the token bloom serves word-boundary terms, the 4-gram bloom serves substrings and anchored regex literals. The planner mints **one** predicate per line filter — `body LIKE '%needle%'` for `|=`/`!=`, `match(body, …)` for `|~`/`!~` — and lets ClickHouse's own index analysis decide which index each engages. It mints **no `hasToken` prefilter of its own**: `hasToken()` is an exact whole-token membership test, so ANDing one onto a substring search dropped matching lines, kept excluded ones and failed outright on a needle containing `_` (issue #450). Measured on 26.3.17.110 over 10M rows / 1.28 GiB of bodies, needle `wxyze73b62205bc78c841234`: `body LIKE` reads **0.97M rows**, `position(body, …) > 0` reads all **10.0M** (`ngrambf_v1` serves `LIKE` and does not serve `position`), and the old wrong-but-fast `hasToken` conjunction read 8,192. Residual: a needle shorter than the ngram order `n = 4` engages neither index and reads the full selector/time-bounded range (still PK-bounded, never the table) — scale behaviour of short needles is issue #25's.
- **Monthly stream/index partitions** (finding #6): one row per stream per month; a 30-day label query touches ≤ 2 partitions.
- **`structured_metadata` is per-entry, not per-stream** (issue #97): Loki push carries optional per-entry structured metadata (protobuf `EntryAdapter.structuredMetadata` or a JSON `values` third element), stored here as a canonical sorted-key JSON String — the `log_streams.labels` representation, not `Map(String,String)` (§1 rejects Map for label-shaped data). Empty string = none (pre-#97 rows, and OTLP-logs rows without an instrumentation scope). As of #109, an OTLP-logs row's `InstrumentationScope` is stored here rather than as stream labels, matching grafana/loki 3.4.2 (which places scope identity in structured metadata, not indexed labels): the scope name/version under keys `scope_name`/`scope_version` (each emitted only when non-empty), and each other scope attribute under its stored name (`log_label_name`, the reference's label namer, issue #507: a run of characters outside `[A-Za-z0-9]` becomes one `_`, a name of four or more bytes that starts and ends with `__` keeps those affixes, and a leading digit gains `key_` — e.g. `scope.attr.foo` → `scope_attr_foo`, `a..b` → `a_b`, `9s` → `key_9s`, `team` unchanged), with no `scope_` prefix. The whole ordered list — every attribute, then `scope_name`/`scope_version` — is then resolved by `pulsus_model::resolve_structured_metadata`, the one seam every structured-metadata producer funnels through (issues #259, #381). That function **is** Prometheus' `labels.Builder` as Loki's distributor drives it (`pkg/distributor/distributor.go:697-722` @ v3.7.4), and its rule is two-tier rather than positional: *a pair that was `Set` — renamed, or carrying `utf8.RuneError` — beats a pair that was not, wherever either sits in wire order; among pairs `Set` onto one name the last wins; among pairs that were never `Set` the reference keeps them all as duplicate labels*, which this key-unique column collapses keeping the last (what a JSON consumer observes of the reference's duplicate-keyed object). It is **not** last-write-wins in wire order — that cannot explain why both orders of `{a.b="x", a_b="keep"}` store `a_b="x"`. An **empty attribute value is a `Del`, not a kept pair**, and the interaction between the two is the rule copied verbatim from `pulsus_model::resolve_structured_metadata`'s primitive 7 (`tests/copied_rule.rs` asserts the copy): <!-- copied-rule:del-vs-set:start -->**`del` drops BASE entries only, so a `Set` outranks it.** An empty value deletes every pair stored under its name that the builder did not `Set`, and a rename or a U+FFFD rewrite re-adds the name in either wire order, because `add` is emitted whether or not `del` holds that name.<!-- copied-rule:del-vs-set:end --> Identity fields are inside that rule, not beside it: an attribute `scope_name=""` takes a plain `scope_name` with it, and does **not** take one carrying U+FFFD — measured, `{scope_name="", scope_name="N\ufffd"}` stores `scope_name="N "` while the control `{scope_name="", scope_name="N"}` stores nothing. A value containing U+FFFD has every occurrence rewritten to a SPACE, and that rewrite is itself the `Set`. Name and version are additionally suppressed at their own append site when empty (#108). On THIS path the keys reaching the builder are already `log_label_name` fixed points (`log_label_name` is idempotent and both identity names are fixed points), so the rename branch cannot fire and `add` carries the U+FFFD rewrites and nothing else. **When no value carries U+FFFD, `add` is empty** and the rule degenerates to the by-name delete plus keep-last — which is why appending identity last makes it win a non-empty collision with an attribute. When one does, that pair is `Set` and the two-tier rule above is what decides. That is the reference's own shape at the same place: its OTLP translation runs `LabelNamer.Build` over every attribute key before the distributor sees it (`pkg/loghttp/push/otlp.go:602-614` @ v3.7.4), whereas the push transport hands the builder raw names. docs/api.md §8.2 rows 5-6 carry the measured cases, and its residuals are in `docs/benchmarks/logs-differential-ledger.md` under `structured-metadata-collision-resolution`. It is added by **additive ALTER** (migration ids 21/22) rather than by mutating the frozen initial `CREATE`, so upgraded and fresh deployments converge byte-identically; a fresh DB runs `CREATE` (no column) then `ADD COLUMN IF NOT EXISTS`. Structured metadata never enters `stream_fingerprint` (a stream pushed with vs. without it fingerprints identically); on the read path it fans into the response stream label set alongside the base labels (grafana/loki 3.4.2 default, `categorize_labels` off), so an entry carrying distinct metadata forms its own result stream and a `| key="value"` pipeline filter selects on it.
- **Admitted log timestamp domain and runtime TTL (issue #137, mirroring #131).** Ingest admits a log record only if its UTC day lies in `[1970-01-01, 2106-02-06]` (days `0..=49_709`, `pulsus_model::Date::start_of_day_utc_datetime_safe`); a record outside that domain is rejected (OTLP logs: per-record partial success; Loki push: whole-request 400 — Loki is all-or-nothing). The same two wrap mechanisms as §2.1 motivate the gate: the 16-bit `Date` partition key wraps for days past 2149-06-06, and the delete-TTL evaluates the row timestamp in the 32-bit `DateTime` domain and wraps for instants past 2106-02-07T06:28:15Z (u32-seconds maximum, `4294967295`); day `49_710` (2106-02-07) is excluded because only part of it is u32-representable. The CREATE-time TTL shown above is superseded at runtime: `apply_ttl` re-issues `ALTER TABLE ... MODIFY TTL toDateTime(least(intDiv(timestamp_ns, 1000000000) + retention_days * 86400, 4294967295)) DELETE` on `log_samples` at init and on every rotation tick, so for a stored row with epoch-seconds `s = floor(timestamp_ns / 1e9)` the operative expiry is `expiry(s) = min(s + retention_days * 86400, 4294967295)`, with exactly §2.1's saturation semantics: a clamped row's shortfall vs the configured value is `s + retention_days * 86400 - 4294967295` (growing without bound as `retention_days` grows); for the enforced `retention_days >= 1` (`crates/pulsus-config/src/validate.rs:285-287`) a row at the last admitted day has actual retention capped at `23_296 s ≈ 0.27 days (~6.5 hours)`; for every enforced `retention_days >= 1` the saturating form strictly dominates the pre-#137 wrapping expression; and the admission cutoff is deliberately not coupled to `retention_days` (§2.1's rationale).

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

Regex/negative matchers (finding #7) resolve within one key's index prefix, e.g. `env=~"prod|staging"` becomes `key = 'env' AND match(val, ...)` — a scan over the distinct *values of that key*, never over samples. The planner orders matchers by selectivity (cheap `count()` probes on index prefixes) and aborts with "query too broad" past `PULSUS_LOGQL_SCAN_BUDGET_BYTES` or past the per-query memory ceiling `PULSUS_LOGQL_READ_MAX_MEMORY_BYTES` (issue #398: `max_memory_usage` + `max_bytes_before_external_group_by=0`, throw-not-spill, on every LogQL read — a memory breach is the same `422` as a byte-budget breach, never a `500`; the extracted-field group key read is the one read whose own memory breach is not returned: that query runs again on the raw-scan read under the same ceiling; a timeout of the extracted-field group key read is returned as a timeout, as for any read). An unwrapped read over JSON groups by the classes and label values that reach the answer; if that read breaches the memory ceiling, the query runs on the raw-scan read, whose breach is the `422`.

Stage 2 — hydration (needed for response labels anyway): `SELECT fingerprint, service, labels FROM log_streams WHERE fingerprint IN (...)` → also yields the `service` set for stage 3.

Stage 3 — samples, primary-index + skip-index served:

```sql
SELECT fingerprint, timestamp_ns, body
FROM log_samples
PREWHERE service = 'checkout'
WHERE fingerprint IN (18374..., 99120...)
  AND timestamp_ns >  {now - 6h} AND timestamp_ns <= {now}
  AND body LIKE '%connection refused%'   -- exact, and ngrambf_v1-prunable
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

**Structured metadata on the metric path (issue #249).** A LogQL metric
query merges each entry's per-entry structured metadata into that sample's
label set, exactly as the streams path does and exactly where the reference
does — both of Loki's sample extractors add it as their FIRST act, before
any pipeline stage runs
(`pkg/logql/log/metrics_extraction.go:102-104` and `:202-205 @ v3.7.4`),
and the `NoopStage` short-circuit sits after that `Add`, so even a query
with no stages merges. So `sum by (trace_id) (count_over_time(...))` groups
by metadata, a label filter filters on it, and `line_format`/`label_format`/
`unwrap`/`drop`/`keep` read it. A fingerprint is therefore no longer an
output-series identity on the metric path: one stream yields one series per
distinct metadata combination.

Three consequences worth stating, because none is visible from the query
text:

- **`structured_metadata` is predicated for an equality or inequality over
  a metadata name, and projected for everything else** (issue #544). The
  raw scans add it to the `SELECT` list; `| trace_id="…"` and
  `| trace_id!="…"` additionally compile into the statement's `WHERE`, and
  the request `LIMIT` compiles with them, which is what turns the sample
  read from a page loop into one statement. **Two things put it back on the
  route it takes today, with the same answer: rendered fragments over
  `MAX_METADATA_FRAGMENT_BYTES` (2 MiB), and a selected stream carrying
  both `k` and `k` without its `_extracted` suffix as labels** — the double
  collision, where a renamed metadata pair overwrites the stream label of
  that name, so the three name-resolution arms do not describe it.
  The regular-expression and numeric forms stay client-side, each for a
  reason docs/query-to-sql.md states.
  Two of the three reasons the earlier rule gave survive and are still
  true: the column is an opaque canonical-JSON `String` with `DEFAULT ''`
  and no index, so the predicate prunes no granule — `EXPLAIN indexes=1`
  over 3,000,000 rows lists `MinMax`, `Partition`, `PrimaryKey` and no
  `Skip` section at all — and the merge does rename a colliding key to
  `<k>_extracted` before any filter would see it. What does not survive is
  the conclusion drawn from them: the value was never pruning. It is the
  statement count and the bytes on the metered hop, and the rename is
  reproduced by the predicate's three name-resolution arms — except in the
  double collision above, which those three arms do not cover and which
  therefore does not lower — rather than being a reason it cannot exist. The `ORDER BY` clauses are unchanged, so
  `optimize_read_in_order` is intact.
- **`absent_over_time` does not read the column at all.** It is the one
  reducer whose label set is provably metadata-independent
  (`syntax/extractor.go:46-47` forces `noLabels = true`, and
  `labels.go:667-668` then returns `EmptyLabelsResult`), so it keeps the
  lean projection rather than paying for a column it cannot use on an
  unbounded scan.
- **A mixed-shape selection loses the zero-allocation slider, and it costs
  about 2.1x.** The per-fingerprint streaming slider is kept only where a
  fingerprint's base label set is provably unreachable by merging any
  co-selected stream's metadata; the test is a sound over-approximation
  (minimum label count, and no `_extracted` key), so a selection whose
  streams have DIFFERENT label counts withholds the slider from the longer
  ones — sometimes correctly, sometimes not.

  Measured on this machine, `count_over_time({app="a"}[5m])` over 20 000
  metadata-free rows, step 10s, release build, five interleaved rounds of
  11 reps each against the pre-#249 tree, reported as the median of the
  per-round medians:

  | selection | vs pre-#249 |
  |---|---|
  | single-shape (slider kept on both) | **1.19x** |
  | mixed-shape, shorter stream's keys a proper SUBSET of the longer's | **2.13x** |
  | mixed-shape, NOT a subset | **2.14x** |
  | the instant path | **1.07x** |

  Reproduce with `cargo test -p pulsus-read --release --test
  logql_slider_withholding_timings -- --ignored --nocapture`, running the
  same generator in a worktree at the pre-#249 commit for the baseline.

  Three things that table does not say on its own:

  - **The two mixed-shape rows are not the same kind of cost.** Where the
    shorter stream's key set is a proper subset of the longer's, merging its
    metadata can genuinely produce the longer's label set, so withholding
    the slider is REQUIRED for correctness and no tightening could recover
    it. Where it is not a subset, the withholding is the over-approximation
    — recoverable in principle by an exact subset test, which is a follow-up
    on issue #249 rather than work in hand. That follow-up is now worth
    about 1.2x rather than the ~7x it would have been worth before this
    optimisation, and the number is recorded there so the case is not
    re-derived from the old figure.
  - **1.19x on the single-shape row is a measured ceiling, not "no cost".**
    An earlier revision of this path cost 1.61x; hoisting the slider-safety
    probe to a per-fingerprint memo and eliding the route comparison where
    the pipeline provably cannot change the label set removed most of it.
    The residual is roughly 9 ns/row and is **unattributed**: the one
    ablation that was run excludes the ordering pre-scan
    (`run_client_agg_rows_folded`, ~4 ns/row on both trees) and excludes
    nothing else. The wider `MetricScanRow` (40 -> 64 bytes) and the
    per-row `structured_metadata.is_empty()` load are CANDIDATES, not
    causes; deciding between them needs a matched-layout ablation on the
    baseline tree and a branch ablation that pins the load, neither of
    which has been run.
  - **The mixed-shape path allocates nothing per row.** Removing the
    per-row allocations is what took the two mixed-shape rows from ~7.3x
    and ~8.4x to ~2.1x — 80% (subset) and 82% (non-subset) of the measured
    regression. The remaining ~20% / ~18% is likewise unattributed: the
    ablation that excluded the routing work excludes only that. The
    zero-per-row property is gated as a scale-invariant identity rather
    than a rounded average — the same fixture at twice the row density over
    the same span costs the same total (measured: 47 allocations at both
    20 000 and 40 000 rows), so a per-row term of even one allocation would
    miss by 20 000 (`logql_pipeline_alloc.rs`). Wall time is never
    asserted in CI (§9 Tier-1).

**`GET /api/logs/v1/patterns`** (M7-C3, issue #171) — stage-1 fingerprint resolution (selector only; line filters are rejected — templates are precomputed, bodies are gone), then ONE pushed-down aggregate over `log_patterns` with no hydration (the response carries no labels), top-1000 by total count:

```sql
SELECT pattern, sum(cnt) AS total,
       arraySort(x -> x.1, groupArray((ts_ns, cnt))) AS samples
FROM (
    SELECT pattern, intDiv(bucket_ns, {step_ns}) * {step_ns} AS ts_ns, sum(count) AS cnt
    FROM log_patterns
    WHERE fingerprint IN (...) AND bucket_ns >= {start} AND bucket_ns < {end}
    GROUP BY pattern, ts_ns
)
GROUP BY pattern
ORDER BY total DESC, pattern ASC
LIMIT 1000
```

`fingerprint IN` engages the `(fingerprint, bucket_ns, pattern)` primary-key prefix (granule pruning), daily partitions prune the window, and the aggregation + top-K + LIMIT all execute in ClickHouse — the client decodes ≤ 1000 already-assembled series. `step` is floored to the 10s ingest bucket; the `(end-start)/step` grid is capped at 11,000 (else 400), and the response is the Loki-interop envelope (docs/api.md §2.6).

**Live tail** polls stage 3's shape with a monotonic `timestamp_ns >` cursor; line-filter pushdown identical.

**Where LogQL SQL fragments come from, and what that guarantees (issue #286).** Every ClickHouse `match(...)` a LogQL read can render is written in ONE leaf module, `crates/pulsus-read/src/logql/predicate.rs`, and the builders above take its output as a type rather than as text: predicate fragments arrive as `CheckedFragment`, string literals as `CheckedLiteral`, month partition literals as `MonthLiteral`, and a metric read's bucket/aggregate columns as a four-inhabitant `MetricShape`. Each newtype's field carries no visibility modifier and the module is a flat leaf, so no other module can build one — a hand-written `format!("match(body, {})", ch_string(pat))` is still a perfectly legal `String`, but it no longer compiles at a builder. The guarantee bought is #240's: an uncompilable user regex is a `400` at plan time, never a ClickHouse `500` mid-query.

The limits are as important as the claim, and are recorded verbatim in that module's doc: the property is closed at the type boundary and open at the one unwrap point per type (`as_sql()`); the file itself is the trust base, so a mint added *inside* it is a failing census (`tests/logqltest_provenance.rs` check H), not a compile error; a sibling macro *invoked* in the leaf expands inside it and reaches the private field, which is a disclosed residual with a committed fixture; `unsafe` is outside the boundary entirely; and the six **table-name** parameters deliberately stay `&str`, with no enforced property, because the only candidate mechanisms were a wire change (backtick-quoting via `ch_ident`) or sealing `PlanCtx`, and a `Checked`-shaped name on an unchecked value is worse than an honest `&str`. `tests/logqltest_provenance.rs` check G inventories every `match(` spelling across `crates/*/src` — an inventory and drift detector, explicitly **not** a gate.

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
    -- Narrowed by migrations 44/45/46 (issue #555), not part of the frozen
    -- CREATE: the projection above is dropped and re-added over the 14
    -- non-payload columns. The final list cannot be printed inside this
    -- block -- four of its columns (shared, status_message, scope_name,
    -- scope_version) arrive by later ALTERs and are not declared here, so
    -- the block would stop parsing. It is printed under the table instead.
    -- Added by migrations 47/48 (issue #555), not part of the frozen
    -- CREATE: PROJECTION name_time, the same 14 columns ORDER BY
    -- (name, timestamp_ns). Also printed under the table.
    -- Added by migrations 42/43 (issue #478), not part of the frozen
    -- CREATE: PROJECTION span_name_day (
    --     SELECT toDate(fromUnixTimestamp64Nano(timestamp_ns)) AS d, name, count()
    --     GROUP BY d, name
    -- )
) ENGINE = MergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(timestamp_ns))
ORDER BY (trace_id, timestamp_ns)
TTL toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL 7 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;
```

**The two re-sorted projections as they finally stand** (issue #555). They are declared inside
`trace_spans`' own `CREATE` in `schema/schema.sql`, and are printed separately here because their
column list includes `shared`, `status_message`, `scope_name` and `scope_version`, so the
list cannot appear inside a `CREATE` that does not declare them — ClickHouse answers
`Code: 47 UNKNOWN_IDENTIFIER`.

```sql
ALTER TABLE trace_spans DROP PROJECTION IF EXISTS service_time;

ALTER TABLE trace_spans ADD PROJECTION IF NOT EXISTS service_time (
    SELECT duration_ns, kind, name, parent_id, payload_type, scope_name,
           scope_version, service, shared, span_id, status_code,
           status_message, timestamp_ns, trace_id
    ORDER BY (service, timestamp_ns)
);
ALTER TABLE trace_spans MATERIALIZE PROJECTION service_time;

ALTER TABLE trace_spans ADD PROJECTION IF NOT EXISTS name_time (
    SELECT duration_ns, kind, name, parent_id, payload_type, scope_name,
           scope_version, service, shared, span_id, status_code,
           status_message, timestamp_ns, trace_id
    ORDER BY (name, timestamp_ns)
);
ALTER TABLE trace_spans MATERIALIZE PROJECTION name_time;
```

**The span's own attribute arrays** (issue #556). Five aligned arrays plus the constraint that
keeps them aligned, declared inside `trace_spans`' own `CREATE` in `schema/schema.sql`. They are
printed separately here for the same reason as the projections: the constraint's `CHECK` names
five columns the frozen `CREATE` above does
not declare, so moving it inside the block stops it parsing —
`Code: 47 ... Missing columns: 'attr_num' 'attr_key' ... (UNKNOWN_IDENTIFIER)`.

Each `ADD COLUMN` also has a cluster-only `_dist` twin (ids 50, 52, 54, 56, 58) that repeats it
against `trace_spans_dist`, because the wrapper is created from a `CREATE ... AS` that does not
inherit the base table's `ALTER`s. The constraint has **no** twin: a `Distributed` table refuses
one (`Code: 48 ... Alter of type 'ADD_CONSTRAINT' is not supported by storage Distributed`), and
does not need one — a misaligned row sent through the wrapper is refused by the base table's
constraint.

```sql
ALTER TABLE trace_spans
ADD COLUMN IF NOT EXISTS attr_key Array(LowCardinality(String));
ALTER TABLE trace_spans
ADD COLUMN IF NOT EXISTS attr_scope Array(LowCardinality(String));
ALTER TABLE trace_spans
ADD COLUMN IF NOT EXISTS attr_val Array(String);
ALTER TABLE trace_spans
ADD COLUMN IF NOT EXISTS attr_type Array(LowCardinality(String));
ALTER TABLE trace_spans
ADD COLUMN IF NOT EXISTS attr_num Array(Nullable(Float64));
ALTER TABLE trace_spans
ADD CONSTRAINT IF NOT EXISTS attr_arrays_aligned CHECK
length(attr_key) = length(attr_scope)
AND length(attr_key) = length(attr_val)
AND length(attr_key) = length(attr_type)
AND length(attr_key) = length(attr_num);
```

- **One table, four physical orders** (finding #5, extended by issues #478 and #555). The base order made trace-by-ID a point read **until issue #587**, which moved the fetch off this table entirely — §4.2's Read-paths entry has what it reads now; the order still serves the search path's `trace_id IN (batch)` hydration; the `service_time` projection is a physically re-sorted copy of the 14 non-payload columns that ClickHouse's optimizer selects automatically for service + time predicates; the `name_time` projection (migrations 47/48) is the same 14 columns sorted `(name, timestamp_ns)`, which is what gives a span-name search a sorted path — neither the base order nor `service_time` leads with `name`; and the `span_name_day` **aggregate** projection (migrations 42/43, `SELECT toDate(fromUnixTimestamp64Nano(timestamp_ns)) AS d, name, count() GROUP BY d, name`) holds one row per `(UTC day, span name)` so the §4.3 Span Name dropdown reads the distinct names instead of the spans. `name_time` does not replace it: that one is sorted by `name` but still holds a row per span, where the aggregate holds one per `(UTC day, span name)`. It is selected by the DAY expression the table is partitioned by — the same predicate `tags_sql::span_name_values_sql` emits — and a `timestamp_ns` predicate defeats it, which is why that builder carries no sub-day bound. Being an aggregate projection it is tiny (one row per distinct day-and-name pair); the three projections' write amplification is an explicit trade for the read shapes. **Neither re-sorted copy stores the `payload`** (migrations 44-48, issue #555): the only statement that selects it is the §4.2 trace-by-ID point read, which filters on `trace_id` — the base table's first sort key — so the optimizer never reaches a projection for it. What dropping that second copy costs and saves, on a named corpus with the settings it was taken at, is docs/traceql-schema-migration.md §4-§5; no figure is repeated here, because a number away from its instrument cannot be checked. The `idx_duration` minmax works *within* the projection because slow spans cluster weakly by time — it prunes granules for `duration > X` searches; it is deliberately **not** relied on in the base order (finding: minmax on unclustered data is useless — here the projection provides the clustering context).

```sql
CREATE TABLE trace_attrs_idx (
    date          Date,
    key           LowCardinality(String),
    val           String,
    scope         LowCardinality(String),        -- 'resource' | 'span' | 'instrumentation' | 'event' | 'event:intrinsic' | 'link' | 'link:intrinsic'
    val_num       Nullable(Float64),             -- populated when val parses numeric
    timestamp_ns  Int64,
    trace_id      FixedString(16),
    span_id       FixedString(8),
    duration_ns   Int64,
    val_type      LowCardinality(String) DEFAULT '' -- 'string' | 'int' | 'float' | 'bool' (migration 39)
) ENGINE = ReplacingMergeTree
PARTITION BY date
ORDER BY (key, val, scope, timestamp_ns, trace_id, span_id)
TTL toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL 7 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;

CREATE TABLE trace_tag_catalog (
    scope     LowCardinality(String),
    key       LowCardinality(String),
    val       String,
    val_type  LowCardinality(String)   -- no DEFAULT; migration 41 (see below)
) ENGINE = ReplacingMergeTree
ORDER BY (scope, key, val, val_type);   -- PRIMARY KEY stays (scope, key, val)
-- populated by MV over trace_attrs_idx (SELECT scope, key, val, val_type); grows with distinct
-- (scope, key, val, val_type) — no MV-side cardinality bound is enforced today (future work); the §4.3
-- response caps + truncated flag bound what the API returns, not what the catalog stores or a scan reads
```

- **`scope` discriminates the attribute's origin** (`'resource'`, `'span'`, `'instrumentation'`, `'event'`, `'link'`, or the reserved `'event:intrinsic'` / `'link:intrinsic'`), so identical verbatim `(key, val)` pairs at different scopes stay separable — scoped TraceQL (`resource.foo` vs `span.foo` vs `instrumentation.foo` vs `event.foo` vs `link.foo`) would otherwise be incorrect. In **`trace_attrs_idx`** it sits **after** `(key, val)` in the ordering key: the proven `(key, val)` prefix pruning is preserved, a scoped query fixes `scope` by equality for near-free post-prefix time pruning, and an unscoped legacy tag search still prunes on the bare `(key, val)` prefix. Instrumentation-scope (`InstrumentationScope`) attributes **are** indexed under `scope='instrumentation'` (issue #192, superseding the earlier M4 decision that dropped them), so the `instrumentation.<key>` selector resolves index-served exactly like `resource.`/`span.`; they remain fully preserved in the span payload as well. **Span events (issue #192 PR-B)** are indexed the same way: each event's attributes ride `scope='event'` (verbatim keys, so `event.<key>` resolves), while each event's `name`/`timeSinceStart` intrinsics ride a **dedicated `scope='event:intrinsic'`** (reserved keys `name` / `timeSinceStart`, the latter's ns delta in `val_num`) that the writer emits ONLY from intrinsic code — a hard namespace partition, so no sender-supplied event attribute (even one literally keyed `name`, reachable via `event."name"`) can collide with the `event:name` intrinsic. **Span links (issue #192 PR-C)** mirror events exactly: each link's attributes ride `scope='link'` (so `link.<key>` resolves), while each link's `spanID`/`traceID` intrinsics ride a **dedicated `scope='link:intrinsic'`** (reserved keys `spanID` / `traceID`, `val` = lowercase hex of the referenced id bytes) — the same hard partition. Events and links remain fully preserved in the span payload as well.
- **`trace_tag_catalog` orders differently — `(scope, key, val)`, scope FIRST** (its DDL above; do not conflate with `trace_attrs_idx`'s `(key, val, scope, …)`): the catalog serves scope-shaped tag discovery, so a scoped tag-names read prunes on the `(scope)` primary-key prefix and a scoped values read on `(scope, key)`; **unscoped** discovery (no scope, or a bare-key values lookup) carries `WHERE scope IN (…)` over the five ATTRIBUTE scopes (issue #475), which is still the leading primary-key column — so it prunes the two writer-reserved intrinsic scopes (`event:intrinsic`, `link:intrinsic`) away and reads only the attribute half, rather than scanning every distinct `(scope, key, val)` tuple. Within that half it is a scan: there is no narrower prefix to prune on (ClickHouse's granule exclusion may still skip granules opportunistically on a bare-key lookup, but that is layout-dependent, never a guarantee). That scan is bounded by the reader's Layer-1 read budget (`max_rows_to_read` = `reader.traceql_scan_budget_rows`, `read_overflow_mode = 'throw'` — the same setting the TraceQL search path applies): a catalog large enough that an unscoped/bare-key read would exceed it aborts with `422 query_too_broad` rather than running unbounded. The §4.3 response caps (`TAG_NAMES_MAX`/`TAG_VALUES_MAX`) bound only what a *successful* request returns, not what a scan reads. Tempo's `/api/v2/search/tags` (T6) is scope-aware.
- **`timestamp_ns` after the `(key, val, scope)` prefix**: TraceQL searches are always time-bounded, so within each `(key, val)` (or `(key, val, scope)`) prefix the time predicate prunes granules — a 3h search over a busy attribute reads 3h of index, not 7 days (compare finding #5's index, which ordered trace/span IDs before time).
- **`val_num`** gives numeric comparisons (`span.http.status_code >= 500`) a typed column. Scope this honestly: `val_num` is not in the primary key, so a range predicate scans *all values of that key* in the time range and filters — acceptable for low-cardinality numeric attributes (status codes, retry counts), **not** a general strategy for high-cardinality numerics (sizes, user-defined measurements). Duration, status, kind, name, and service are physical span columns precisely so the common numeric intrinsics never rely on this index. If benchmarks show real workloads need fast range predicates on high-cardinality numeric attributes, the design adds a dedicated numeric index ordered `(key, timestamp_ns, val_num, ...)` — benchmark-gated, not speculative.
- **`val_type` — the attribute's stored OTLP type (issue #476, migrations 39/40/41).** One of `string`, `int`, `float`, `bool`, written at ingest from the OTLP `AnyValue` kind (`pulsus_write::ingest::traces::AttrValueType`) and projected into the catalog by `trace_tag_catalog_mv`. It exists because the §4.3 tag-values wire `type` used to be **inferred from `val`'s text**, so a string attribute whose value read as a number was reported `int` — and a client that quotes only `string`-typed values then built a query matching nothing. **The type is not derivable from what else is stored:** `val` is the rendered text and `val_num` is a parse of that text, so the string `"1.5"` and the double `1.5` are byte-identical in both. Array, kvlist and bytes values render to a string and are therefore `string` — the column states the type of what was stored.
  - **On `trace_attrs_idx`** (migration 39, `_dist` twin 40) it is `DEFAULT ''` and stays **out** of the sorting key: the index's `(key, val, scope, …)` prefix and every prune that rides it are unchanged. **Nothing on the SEARCH path reads the column since issue #558** — the kind a search response renders comes from `trace_spans.attr_type`, at the element the value came from. The default matters for a fixture rather than for a request: an `INSERT` into the index that names no `val_type` stores `''` while the span row beside it stores one of the four spellings, and the two stores then disagree. `pulsus_testkit::assert_stores_agree` is what every live search fixture that writes BOTH stores checks that with — two corpora are excluded by construction and say so at the fixture: one writes the index alone so granule selection over it is observable, the other writes a million index rows against sixty-four span rows to measure the generator's memory ceiling.

    Two consequences of `val_type` (and `val_num`) sitting outside this
    table's ordering key, both measured on ClickHouse 26.3.29.7:

      system.columns is_in_sorting_key    key val scope timestamp_ns trace_id span_id -> 1
                                          val_type val_num                            -> 0

      merges stopped, two identical INSERTs   raw 2   FINAL 1
        a duplicate index row is VISIBLE until a merge, which is what the
        replay corpora reproduce on purpose

      two rows differing ONLY in val_type, then OPTIMIZE ... FINAL
        1 row left, val_type = the LATER insert's
        the index cannot hold two kinds for one element; the survivor is
        decided by insertion order, the same rule the `trace_tag_catalog`
        bullet below avoids by putting `val_type` IN its key
  - **On `trace_tag_catalog`** (migration 41) it is **in** the sorting key and carries **no default**. In the key because the type is per VALUE, not per key — one key can hold a string `'8080'` and an int `8080`, whose `val` bytes are identical, and without the extension the `ReplacingMergeTree` collapses the pair with the survivor decided by insertion order. No default because ClickHouse rejects a defaulted column entering a sorting key in the same `ALTER`, and rejects a standalone `MODIFY ORDER BY` afterwards; the single-statement, no-default form is the only accepted one. `MODIFY ORDER BY` **appends**, so `PRIMARY KEY` stays `(scope, key, val)` and the prefix prune that serves every tag-values read is untouched (gated by `crates/pulsus-read/tests/traces_tags_explain.rs`, which asserts both keys and the prune relations).
  - **Rows written before migration 41 read back `''`** and are reported as `string` on the wire. That is not a recovery — nothing stored can distinguish them — and it is what those rows already reported for non-numeric text. `trace_tag_catalog` has no TTL, so they do not age out; the branch is dead once `SELECT count() FROM trace_tag_catalog WHERE val_type = ''` returns `0` everywhere, which needs a catalog rebuild-or-clear mechanism that does not exist yet.
- Tag **names** read only `trace_tag_catalog` — name discovery never scans span payloads — and the intrinsic vocabulary (the `intrinsic` scope, the closed `status`/`kind` value sets) is served from the TraceQL grammar with no read at all (issue #475). **Tag values read one of three places** (issue #478): the catalog, for an attribute key with no narrowing `q`, byte-identically to before; `trace_attrs_idx` intersected with the matching span set, for an attribute key narrowed by `q`; and `trace_spans` for `name`/`span:name`, which the catalog cannot answer at all because `trace_tag_catalog_mv` projects `trace_attrs_idx` alone and holds no span-`name` row.
- **`trace_spans.shared` (issue #173, additive migration).** A `shared UInt8 DEFAULT 0` column is added to `trace_spans` by an additive `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` (never a mutation of the frozen CREATE above; pre-#173 rows read back `0`). It is `1` iff the span carried the `zipkin.shared = "true"` attribute at OTLP parse time — the exact wire contract the Zipkin receiver emits (`str_kv("zipkin.shared", "true")`), documented so an OTLP-native sender may set it too. The attribute itself still flows to `trace_attrs_idx` unchanged; the column exists only so the service-graph edge MV below can identify a Zipkin shared span (whose SERVER side is stored under the *client's* `span_id`) and key it by its own id rather than its inherited `parent_id`.
- **`trace_spans.status_message` (issue #184, migrations 35/36).** A `status_message String DEFAULT ''` column added by the same additive-`ALTER` pattern (id 35 on the base table, id 36 the cluster-gated `_dist` copy; the frozen CREATE above is never mutated, pre-#184 rows read back `''`). It stores the OTLP `Status.message` verbatim — previously dropped at parse time — so the `statusMessage` / `span:statusMessage` TraceQL intrinsic is queryable as a physical span column (a bounded time-window span scan in Phase 1, exact hydrated-column evaluation in Phase 2, byte-capped on read like `name`/`service`).
- **`trace_spans.attr_key` / `attr_scope` / `attr_val` / `attr_type` / `attr_num` (issue #556, migrations 49-59).** Five **aligned** arrays printed above, added by the same additive-`ALTER` pattern (odd ids on the base table, even ids the cluster-gated `_dist` copies; the frozen CREATE is never mutated, rows written before them read back five EMPTY arrays). Element `i` of each array describes one attribute of that span: its verbatim key, its `scope` discriminator (the same seven spellings `trace_attrs_idx.scope` carries), its rendered value, its declared OTLP kind, and its numeric value or NULL. They carry exactly the attributes that also become `trace_attrs_idx` rows for that span, **in the same order** — the parser derives them from the same records rather than recomputing them, so the number is decided by the whole of (scope, key, value) and not by the value text: a link's `spanID` of `0000000000000001` parses as `1.0` and is stored NULL in both places. Migration 59's `attr_arrays_aligned` CHECK refuses an `INSERT` whose five lengths are not equal (`Code: 469 VIOLATED_CONSTRAINT`); a row naming no array column at all is accepted, because `0 = 0 = 0 = 0 = 0`. A `CHECK` does **not** see an `ALTER TABLE ... UPDATE` mutation, so any later backfill must check alignment itself. **Every phase-2 attribute read comes off these columns** (issues #557 and #558): the batch hydration statement carries one projected SLOT per attribute condition and per projected field — `arrayFirstIndex` over `(attr_key, attr_scope)` locates the element the span's attribute resolves to, and the condition's value test, the rendered value, its numeric reading and its stored kind are all taken at that one element. The event and link VALUE SETS come off the same arrays, expanded with `arrayJoin` over the rows the reader retained. `trace_attrs_idx` serves phase 1 — the candidate generator — and nothing else on the search route. The `timeSinceStart` intrinsic's `attr_num` reproduces `trace_attrs_idx.val_num` bit for bit, including its `i64 as f64` rounding above 2^53.
- **`trace_spans.scope_name` / `trace_spans.scope_version` (issue #192, migrations 37/38).** Two `LowCardinality(String) DEFAULT ''` columns added by the same additive-`ALTER` pattern (id 37 on the base table, id 38 the cluster-gated `_dist` copy; the frozen CREATE above is never mutated, pre-#192 rows read back `''`). `LowCardinality` (unlike `status_message`'s plain `String`) matches the sibling `name`/`service` columns — instrumentation library name/version are genuinely low-cardinality. They store the OTLP `InstrumentationScope.name`/`version` verbatim so the `instrumentation:name` / `instrumentation:version` TraceQL intrinsics are queryable as physical span columns (bounded time-window span scan in Phase 1, exact hydrated-column evaluation in Phase 2, byte-capped on read like `name`/`service`), and so `compare()` has a per-span source (§4.2).

**Service-graph edge ledger (issue #173, M7-E1).** `trace_edges` is a **ReplacingMergeTree half-row ledger**: one narrow row per edge-relevant span (a CLIENT/PRODUCER or a SERVER/CONSUMER span), with its own plain `timestamp_ns` — no `SimpleAggregateFunction` anywhere. The directed `client → server` edge is assembled at **query time** (§4.2), so pair completion is a pure function of the stored half-row multiset, never of background-merge progress.

```sql
CREATE TABLE trace_edges (
    date          Date,
    side          UInt8,                         -- 0 = client half (kind 3|4), 1 = server half (kind 2|5)
    trace_id      FixedString(16),
    span_id       FixedString(8),
    pair_id       FixedString(8),                -- the edge's CLIENT-side span id (the join key)
    conn_type     LowCardinality(String),        -- 'rpc' | 'messaging', from the emitting span's own kind
    timestamp_ns  Int64  CODEC(DoubleDelta, ZSTD(1)),
    service       LowCardinality(String),
    duration_ns   Int64  CODEC(T64, ZSTD(1)),
    failed        UInt8
) ENGINE = ReplacingMergeTree
PARTITION BY date
ORDER BY (side, trace_id, span_id)
TTL toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL 7 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;
-- populated by trace_edges_mv over trace_spans (a pure per-row projection, kind-filtered); no MV-side
-- GROUP BY/join/state. The CREATE-time TTL is superseded at runtime by apply_ttl's saturating form
-- (the trace-table pattern above): toDateTime(least(intDiv(timestamp_ns, 1000000000) + retention_days*86400, 4294967295)) DELETE
```

```sql
CREATE MATERIALIZED VIEW trace_edges_mv TO trace_edges AS
SELECT
    toDate(fromUnixTimestamp64Nano(timestamp_ns)) AS date,
    toUInt8(kind IN (2, 5)) AS side,
    trace_id,
    span_id,
    if(kind IN (3, 4) OR shared = 1, span_id, parent_id) AS pair_id,
    if(kind IN (2, 3), 'rpc', 'messaging') AS conn_type,
    timestamp_ns,
    service,
    duration_ns,
    toUInt8(status_code = 2) AS failed
FROM trace_spans
WHERE kind IN (3, 4)
   OR (kind IN (2, 5) AND (shared = 1 OR parent_id != toFixedString(unhex('0000000000000000'), 8)));
```

- **`side` leads the ORDER BY** so each per-half read subquery (§4.2) prunes on the PrimaryKey `side` prefix — a second, independently-gateable prune besides the daily-partition MinMax prune. Because `side` is in the `ReplacingMergeTree` dedup key, a Zipkin shared span (issue #75: SERVER side stored under the client's `span_id`) never collapses its two halves.
- **`pair_id` is the edge's CLIENT-side span id** — the single-valued join key. A client/producer half and a *shared* server half key by their own `span_id` (Zipkin's shared model: both RPC sides carry the same id); a non-shared server half keys by `parent_id`. Edge identity is thus the SERVER-side span (`(trace_id, span_id)`, one row per edge), which preserves client fan-out: a client parenting N servers yields N edges, none `max()`-collapsed across siblings.
- **`conn_type` is derived from the emitting span's own kind** (`kind ∈ {2,3}` → `'rpc'`, `{4,5}` → `'messaging'`), and the read join (§4.2) requires `c.conn_type = s.conn_type` — so only CLIENT(3)→SERVER(2) and PRODUCER(4)→CONSUMER(5) can pair; the four cross-kind combinations are structurally rejected.
- **Root non-shared server halves are excluded** (`parent_id` all-zero → no client twin possible); a shared server half is admitted even with a zero parent, since its pair key is its own id.
- **Replay idempotence is read-time dedup, not ledger-exact.** Byte-identical at-least-once redelivery (the ingest contract) is fully absorbed — the read's per-side `GROUP BY` computes identical `any(...)`/`max(...)` whether zero, some, or all duplicates were physically merged. A *mutated* re-send of the same `(side, trace_id, span_id)` makes `any()` merge-order-sensitive (documented residual, unchanged in kind from the metrics path).
- **Zipkin shared-span limitation.** The correct handling above (key a shared server half by its own id) depends on the `shared` column; a shared server half whose `zipkin.shared` marker was lost cannot be distinguished from an ordinary server half and would key by its inherited `parent_id`. No historical backfill: the edge MV sees only post-deploy inserts (a one-shot operator `INSERT ... SELECT trace_edges_mv-body FROM trace_spans` recipe reconstructs history from retained spans if needed).

**The two derived trace tables (#560).** `trace_recent` holds one row per (five-minute bucket, trace) and `trace_error_spans` one row per span with `status_code = 2`, both written by views over `trace_spans`, so the empty search `{}` and `{ status = error }` stop scanning the span table (§4.2). The writer still sends one `INSERT` into `trace_spans` and one into `trace_attrs_idx`; both new tables are derived.

```sql
CREATE TABLE trace_recent (
    date      Date,
    bucket    UInt32,                          -- intDiv(timestamp_ns, 300000000000): 288 per UTC day
    trace_id  FixedString(16),
    ts_max    SimpleAggregateFunction(max, Int64)  CODEC(T64, ZSTD(1)),
    ts_min    SimpleAggregateFunction(min, Int64)  CODEC(T64, ZSTD(1))
) ENGINE = AggregatingMergeTree
PARTITION BY date
ORDER BY (bucket, trace_id)
TTL toDateTime(fromUnixTimestamp64Nano(ts_max)) + INTERVAL 7 DAY DELETE
SETTINGS ttl_only_drop_parts = 1, non_replicated_deduplication_window = 10000;

CREATE TABLE trace_error_spans (
    date          Date,
    trace_id      FixedString(16),
    span_id       FixedString(8),
    timestamp_ns  Int64  CODEC(DoubleDelta, ZSTD(1)),
    duration_ns   Int64  CODEC(T64, ZSTD(1)),
    service       LowCardinality(String),
    name          LowCardinality(String),
    kind          Int8
) ENGINE = ReplacingMergeTree
PARTITION BY date
ORDER BY (timestamp_ns, trace_id, span_id)
TTL toDateTime(fromUnixTimestamp64Nano(timestamp_ns)) + INTERVAL 7 DAY DELETE
SETTINGS ttl_only_drop_parts = 1, non_replicated_deduplication_window = 10000;
-- Both CREATE-time TTLs are superseded at runtime by apply_ttl's saturating form:
--   trace_recent       toDateTime(least(intDiv(ts_max, 1000000000) + retention_days*86400, 4294967295)) DELETE
--   trace_error_spans  toDateTime(least(intDiv(timestamp_ns, 1000000000) + retention_days*86400, 4294967295)) DELETE
```

```sql
CREATE MATERIALIZED VIEW trace_recent_mv TO trace_recent AS
SELECT toDate(fromUnixTimestamp64Nano(timestamp_ns))  AS date,
       toUInt32(intDiv(timestamp_ns, 300000000000))   AS bucket,
       trace_id,
       max(timestamp_ns)                              AS ts_max,
       min(timestamp_ns)                              AS ts_min
FROM trace_spans
GROUP BY date, bucket, trace_id;

CREATE MATERIALIZED VIEW trace_error_spans_mv TO trace_error_spans AS
SELECT toDate(fromUnixTimestamp64Nano(timestamp_ns)) AS date,
       trace_id, span_id, timestamp_ns, duration_ns, service, name, kind
FROM trace_spans
WHERE status_code = 2;
```

- **`ts_min` is what makes the recency read correct.** The read bounds each row by `ts_max >= start AND ts_min <= end - 1` — the window's first included nanosecond and its last (requirement R9). Without `ts_min` every trace whose spans lie wholly after `end` in the window's last bucket is a candidate, and those rank above every genuine one: the empty search returns nothing once they reach the candidate ceiling, which at the mean bucket tail is about 667 traces per second (`docs/traceql-schema-migration.md` §3.6). There is no `ts_max <= end` bound: a trace with a span in the window and a later span in the same bucket is an answer.
- **`date` is a function of `bucket`.** A UTC day is 288 buckets exactly, so no bucket straddles midnight; `bucket` leads the sort key, so a time predicate prunes granules rather than whole day partitions. The bucket width is the reader's `RECENT_BUCKET_NS` (`crates/pulsus-read/src/traces/window_sql.rs`), bound to the view's literal by a test.
- **`trace_recent`'s TTL reads `ts_max`, not `date`:** a `date` TTL would expire the whole partition at midnight of `date + N`, under-retaining a span written at 23:59 by almost a day.
- **A repeated identical span block leaves both tables' physical `count()` unchanged**, immediately, with no merge and no `FINAL`. A view's insert into its target carries a block id derived from the source block (`deduplicate_blocks_in_dependent_materialized_views = 1`), and a target with its own `non_replicated_deduplication_window` recognises the repeat and drops it — the single-node `trace_spans` itself has no window and stores the block twice, as before. The span inserter pins `deduplicate_insert = enable` and `deduplicate_blocks_in_dependent_materialized_views = 1` on every insert into `trace_spans`/`trace_spans_dist` (`crates/pulsus-write/src/writer/trace.rs`, `span_insert_settings`), so this does not depend on the server profile; no other insert carries the pins. A replay whose rows arrive in another order is a different block and is written again; its rows collapse at merge, because both engines are idempotent under a duplicate row, and they change no answer. The window is 10,000 blocks per table.
- **Neither read uses `FINAL`.** A trace whose spans for one bucket arrive in several inserts has several unmerged rows for one key; the read's `GROUP BY trace_id` finds it through the row of the block that holds its in-window span. An unmerged table can hold a false candidate, never lose a true one.

**What a failing view leaves behind (#560).** Three views now fire on every `trace_spans` insert — `trace_edges_mv`, `trace_recent_mv` and `trace_error_spans_mv` — and this is the first change at which a view's failure means a missing answer, the empty search returning nothing for that block, rather than a missing index row. The insert fails: the caller receives `HTTP 500` with `Code: 395`, and is not told which tables kept the block's rows. The design record measured that outcome over 300 trials of one throwing view and three healthy sibling views over one source table (`docs/traceql-schema-migration.md` §6.3): the throwing view's own target held nothing in 300 of 300; the source rows were present in 297 of 300 and absent in 3; the three healthy siblings committed 28, 25 and 32 times out of 300, and all three together 7 times against about 0.25 if they were independent. Nothing else held on every trial. The writer passes `on_flush_poisoned: None` for `trace_spans`, so nothing replays the block, and no machinery is built for this: a client that retries after the `500` re-runs the views, and the two tables' collapse rules absorb any rows written twice; a client that gives up leaves the block's derived rows partly written, with no record of which.

- **Admitted trace timestamp domain and runtime TTL (issue #131).** Ingest admits a span only if its UTC day lies in `[1970-01-01, 2106-02-06]` (days `0..=49_709`, `pulsus_model::Date::start_of_day_utc_datetime_safe`); a span outside that domain is rejected (OTLP partial success; Zipkin whole-request 400). Two wrap mechanisms motivate the gate: `PARTITION BY toDate(...)` evaluates in the 16-bit `Date` domain and wraps for days past 2149-06-06, and the delete-TTL evaluates the row timestamp in the 32-bit `DateTime` domain and wraps for instants past 2106-02-07T06:28:15Z (u32-seconds maximum, `4294967295`); day `49_710` (2106-02-07) is excluded because only part of it is u32-representable. The CREATE-time TTL shown above is superseded at runtime: `apply_ttl` re-issues `ALTER TABLE ... MODIFY TTL toDateTime(least(intDiv(timestamp_ns, 1000000000) + retention_days * 86400, 4294967295)) DELETE` on both trace tables at init and on every rotation tick, so for a stored row with epoch-seconds `s = floor(timestamp_ns / 1e9)` the operative expiry is `expiry(s) = min(s + retention_days * 86400, 4294967295)` — i.e. `min(configured_expiry, 2106-02-07T06:28:15Z)`. If `s + retention_days * 86400 <= 4294967295`, the expiry equals the configured instant, bit-identical to the pre-#131 expression; otherwise the expiry is `4294967295`, the actual retention is `4294967295 - s`, and the shortfall vs the configured value is `s + retention_days * 86400 - 4294967295`, which grows without bound as `retention_days` grows. For the enforced range `retention_days >= 1` (config validation rejects `< 1`, `crates/pulsus-config/src/validate.rs:285-287`), a row at the last admitted day (`49_709`, `s = 4_294_943_999`) has actual retention capped at `4_294_967_295 - 4_294_943_999 = 23_296 s ≈ 0.27 days (~6.5 hours)`. For every enforced `retention_days >= 1`, the saturating form strictly dominates the pre-#131 expression: pre-#131, a row with `s + retention_days * 86400 > 4294967295` wrapped to a ~1970-epoch expiry and its part became drop-eligible immediately or near-immediately after insert (`ttl_only_drop_parts = 1`); under the saturating form the same row becomes drop-eligible no earlier than 2106-02-07T06:28:15Z. The admission cutoff is deliberately not coupled to `retention_days`: retention is runtime-ALTERed after rows are stored (a changed `PULSUS_RETENTION_DAYS` re-ALTERs existing tables on the next rotation tick) and has no upper bound, so no admission-time gate can honor a retention value that did not exist when the row was admitted.

### 4.2 Read paths (generated SQL)

**Trace by ID** — since issue #587 the fetch reads the TraceQL design's own three tables (`spans`, `traces`, `resources`) and no longer reads this one. One statement answers an indexed trace and a second answers two cases, each named with its count in `docs/TraceQL/server-implementation.md` §3.5:

```text
statement 1, always, first   the per-trace row by `trace_id` — its own whole sort
                             key — for the trace's stored five-minute bucket set
                             and its extent; then the spans by
                             `(bucket, trace_id)`, the first two columns of the
                             span table's sort key, against the buckets the
                             trace actually occupies; then the distinct
                             resources those spans reference, by the whole
                             `(service, resource_id)` key inside the extent's
                             day range. One row out: four scalars and two arrays
statement 1w, only when      the same answer with NO bucket condition, over the
the stored bucket set is at   trace's own date partitions, because an incomplete
its 4,096-element cap        key set would EXCLUDE rows
statement 2, only when the   the spans over the REQUEST's window instead of the
per-trace table has not      trace's extent, with the same bucket pushdown
indexed the trace and the
request supplied a window
```

The full text of all three is `docs/TraceQL/sql-schema.md` §5.4, which is where they are byte-frozen. **No production statement selects the per-span payload column after this change** — the two write-path round-trip suites that still decode it filter on `trace_id`, the base table's first sort key.

`kind` is projected (issue #75) as the fetch assembler's `(span_id, kind)` de-duplication key **and** as the rendered value: since issue #587 the response renders `kind` **from the column**, as the protocol's own signed `Int32`, because there is no decoded payload to take it from. Its reason for existing is unchanged — it keeps a Zipkin shared span's SERVER and CLIENT sides (identical `(trace_id, span_id)`, different `kind`) as two distinct spans on retrieval, while remaining a genuine no-op for OTLP (span ids are unique per trace) and still de-duplicating identical at-least-once replays.

**TraceQL search is two-phase** (issue #57): Phase 1 produces a bounded, recency-ranked candidate trace-id set from indexed sources (false positives are harmless — Phase 2 filters; false negatives exist only past the cap and are reported by the response OMITTING `metrics.completedJobs`); Phase 2 hydrates candidates in small batches and evaluates the full query **exactly** in the engine.

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
| `name`/`status`/`kind`, except `status = error` | `trace_spans` time-window scan + predicate | no selective index — window-bounded, budget-limited |
| `status = error` (#560) | `trace_error_spans` date + time prune, no predicate — the view's `WHERE status_code = 2` is the predicate | granule-pruned on the `(timestamp_ns, …)` sort key; one row per error span |
| `trace:id =` (issue #184) | `trace_spans` `trace_id = unhex('…')` — the `ORDER BY (trace_id, timestamp_ns)` **PK prefix** | index-served (Tier-1 EXPLAIN-gated) |
| `statusMessage` / `span:id` / `span:parentID` (issue #184) | `trace_spans` time-window scan + predicate (`status_message`, `lower(hex(span_id/parent_id))`) | no selective index — window-bounded, budget-limited |
| `instrumentation:name` / `instrumentation:version` (issue #192) | `trace_spans` time-window scan + predicate (`scope_name` / `scope_version`) | no selective index — window-bounded, budget-limited |
| `event.<key>` attr (issue #192 PR-B) | `trace_attrs_idx` `(key, val, scope='event')` prefix (`=`/`=~`) or key-only `(key)` scan | index-served exactly like any attribute |
| `event:name =` (issue #192 PR-B) | `trace_attrs_idx` `(key='name', val, scope='event:intrinsic')` prefix | index-served (AttrEq) |
| an event/link intrinsic compared against another FIELD, e.g. `{ .a = event:name }` (issues #351, #558) | the time-range generator, plus one phase-2 statement expanding `trace_spans`'s own arrays over the rows the reader retained | one row per value; bounded by `PULSUS_TRACEQL_EVENT_SET_MAX_VALUES` **before** the expansion, from per-span counts the batch hydration statement already returned |
| `event:timeSinceStart <op>` (issue #192 PR-B) | `trace_attrs_idx` **key-only** `(key='timeSinceStart')` scan + `val_num <op> ns` under `scope='event:intrinsic'` | key-only scan (numeric filter) |
| `link.<key>` attr (issue #192 PR-C) | `trace_attrs_idx` `(key, val, scope='link')` prefix (`=`/`=~`) or key-only `(key)` scan | index-served exactly like any attribute |
| `link:spanID =` / `link:traceID =` (issue #192 PR-C) | `trace_attrs_idx` `(key='spanID'\|'traceID', val, scope='link:intrinsic')` prefix (`val` = lowercase hex) | index-served (AttrEq) |
| trace-level intrinsics — `traceDuration` / `rootName` / `rootServiceName` / `span:childCount` (issue #184) | the time-range generator | no candidates of their own (a windowed root scan would MISS out-of-window roots); exact via the trace-wide co-loads below — sole-predicate scale routed to #25 |
| `!=` / `!~` / `{}` match-all | the time-range generator (`trace_recent` over the window, #560) | complete superset; absence is not indexable. Granule-pruned on the leading `bucket` column; the same candidates as a `trace_spans` scan whenever the window's ends lie in different buckets |

**The two derived-table generators (#560).** The time-range generator and `{ status = error }` read the tables §4.1 derives from `trace_spans`, and neither statement carries an `AND (<predicate>)` line — the table is the predicate. For the window `[1700000000000000000, 1700010800000000000)` at `PULSUS_TRACEQL_MAX_CANDIDATES = 100000`:

```sql
-- the time-range generator
SELECT trace_id, toInt64(max(ts_max)) AS bound_ts
FROM trace_recent
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')
  AND bucket >= 5666666 AND bucket <= 5666702
  AND ts_max >= 1700000000000000000 AND ts_min <= 1700010799999999999
GROUP BY trace_id
ORDER BY bound_ts DESC, trace_id ASC
LIMIT 100001

-- { status = error }
SELECT trace_id, max(timestamp_ns) AS bound_ts
FROM trace_error_spans
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')
  AND timestamp_ns >= 1700000000000000000 AND timestamp_ns < 1700010800000000000
GROUP BY trace_id
ORDER BY bound_ts DESC, trace_id ASC
LIMIT 100001
```

- **The bucket clause** is `floor(start / 300 s)` to `floor(last included ns / 300 s)`, rendered signed with no clamp; a pre-epoch window renders negative literals, which a `UInt32` column compares correctly.
- **The candidate set is the span scan's exactly** whenever the window's ends lie in different buckets — every window of 300 s or more. Inside one bucket it is a superset by one shape: a trace with a span before `start` and a span after `end` in that bucket and none between. Phase 2 hydrates it, finds no in-window span and drops it; at the candidate ceiling the response is marked partial, never wrong.
- **`bound_ts` on `trace_recent` is the trace's newest span in its overlapping buckets**, `>=` the newest in-window span. It is still an upper bound on the public sort key, so threshold termination stops later, never earlier; the response order does not move. The cast is there because `max` over a `SimpleAggregateFunction(max, Int64)` column keeps the wrapper.
- **Measured on a 2,000,000-span corpus** (12 spans per trace, `docs/benchmarks/issue560-two-table-reads.sh`): `{}` over three hours reads 167,277 rows against 2,000,000 and 22 marks against 248; over 300 s, 8,192 rows against 1,185,089. `{ status = error }` reads 20,000 rows against 2,000,000. The recency read reads more bytes than the span scan only below 1.58 spans per trace.
- **No aggregate is pushed onto either table**: `aggregate_having_sql` accepts only `trace_spans` and `trace_attrs_idx` as sources.

Within one `{...}` filter, an `&&` needs only its statically most selective conjunct's generator set (matches are a subset of any conjunct's); an `||` needs both sides' sets. Cross-spanset `{A} op {B}` takes the superset union of both operands' generators for **both** `&&` and `||` — exactness is Phase 2's job, never a lossy trace-id reduction. Selectivity is the fixed leaf-class priority above (byte-deterministic, never a runtime probe). Every generator (indexed and fallback alike) carries the reader scan budget (`PULSUS_TRACEQL_SCAN_BUDGET_ROWS` as `max_rows_to_read` + throw): a query too broad to bound fails loud with `422 query_too_broad` — it is never silently slow and never quietly incomplete. Issue #398 adds the per-query memory ceiling `PULSUS_TRACEQL_READ_MAX_MEMORY_BYTES` (`max_memory_usage` + `max_bytes_before_external_group_by=0`, throw-not-spill) to **every** trace read at all three settings origins — search, the independent catalog-discovery root, and the §4.2 trace-by-id point read, which carried no settings at all before — so a memory breach on any of them is a `422`, not a `500`. Phase-1 generators layer their own tighter `PULSUS_TRACEQL_GENERATOR_MAX_MEMORY_BYTES` on top and keep their own reason.

The engine merges the per-generator `(trace_id, bound_ts)` tuples in Rust — an explicit `max(bound_ts)` per trace, ranked `(bound_ts DESC, trace_id ASC)`. `bound_ts` (the newest *leaf-matching* span's timestamp) is an upper bound on the trace's final public sort key (the max timestamp of its *exactly-matched* spans, a subset), which licenses the early termination below.

**Phase 2 — streaming batched exact evaluation.** Candidates are consumed newest-bound-first in batches of `BATCH_TRACES` (32). Per batch: spans hydrate by primary key (`WHERE trace_id IN (batch) AND <time>`, `LIMIT {MAX_SPANS_PER_TRACE + 1} BY trace_id` — `MAX_SPANS_PER_TRACE` = 10,000; the `+1` probe distinguishes exactly-at-cap from overflow, and an overflowing trace is evaluated on its truncated span set and the response then omits `metrics.completedJobs`), deduped by `span_id` (at-least-once replays, no `FINAL`). **The deduplication keeps the FIRST row per `span_id` in the hydration read's `(trace_id, timestamp_ns, span_id)` order**, and the selector is then applied to that survivor — so two rows sharing a `(trace_id, span_id)` and disagreeing on a projected column are resolved to one of them, and where their timestamps are equal the choice is merge-order dependent. That resolution is the reason a pushed spanset aggregate is bounded by the six (aggregate, operator) cells it is (issue #492 part 5): the statement aggregates the generator's ROWS and the evaluator its deduplicated, selector-matched spans, so the generator's rows are always a superset and `count()`/`max` read HIGH while `min` reads LOW; each distinct attribute condition is **one predicate column on that same hydration statement** (issue #557) — `arrayFirstIndex` over the span row's `(attr_key, attr_scope)` arrays locates the element the span's attribute resolves to, and the condition's value test is applied to that element — so an attribute condition costs no statement of its own. The engine then evaluates the full boolean tree per span (physical leaves on hydrated columns; an attribute leaf is membership in the set that column filled; `!=`/`!~` match a span iff it is NOT in that set — absent-key spans match, and over the multi-valued `event`/`link` scopes the set is any-element, so the negation is all-match), applies the cross-spanset algebra with matched-span membership preserved (`{A} && {B}` = trace-level intersection, spanset = union of matched spans; `||` = union; the structural relations `>`/`>>`/`<`/`<<`/`~` — issues #172/#183 — compute the child/descendant/parent/ancestor/sibling result over the hydrated spans' `parent_id` graph in O(spans) per trace, budget-charged, cycle-guarded; the **plain** form keeps the **RHS-only** matched set, the **negated** form (`!>` …) keeps the RHS spans NOT satisfying the relation — with an empty LHS the whole RHS set — and the **union** form (`&>` …) keeps both participating sides; field-vs-field comparison `{ .a = .b }` — issue #183 — resolves both operands per span and compares engine-side, its Phase-1 pruning being the attribute operand's key-existence `(key)` scan), evaluates the pipeline (`count`/`sum`/`avg`/`min`/`max` over the matched spans, attribute aggregates via a batched `val_num` read; `select()` projects response fields only), and pushes survivors into a `limit`-size heap of **response summaries only** — never hydrated spans or payloads. Consumption stops when the heap is full and the next candidate's `bound_ts` is strictly below the k-th held sort key (no unseen candidate can enter the top-K), at exhaustion, or at the `PULSUS_TRACEQL_MAX_CANDIDATES` consumption ceiling. Winners get one trace-wide root hydration (a `trace_id` PK read with **no** time predicate — the true root may predate the search window; root = `parent_id` all-zero, else timestamp-earliest).

**Trace-level intrinsics evaluate via trace-wide co-loads** (issue #184 — `traceDuration`/`trace:duration`, `rootName`/`trace:rootName`, `rootServiceName`/`trace:rootService`, `span:childCount`). These are whole-trace values, so evaluating them over the window-bounded hydrated spans would truncate window-spanning traces; instead, Phase 2 issues up to two additional per-batch reads over the candidate `trace_id` set — **deliberately window-free `trace_id IN` PK reads** (the winners' root-hydration contract generalized to the filter phase), making the evaluated values full-trace-exact regardless of the search window or the per-trace hydration cap, with zero write amplification and no rollup table. The trace-context read (`GROUP BY trace_id`): `min(timestamp_ns)`, `max(timestamp_ns + duration_ns)` (→ `traceDuration`), and `argMin(name/service, (toUInt8(parent_id != <zero>), timestamp_ns, span_id))` — the exact `pick_roots` selection order (a true zero-parent root beats any non-root, then earliest `(timestamp_ns, span_id)`), value-projected through the SAME byte-cap expression as the displayed-root read, so the evaluated root strings and the response's root summary can never disagree, capped or not, rooted or root-less. The child-count read: `count(DISTINCT span_id) GROUP BY trace_id, parent_id` (replay-deduped; an absent parent key means 0 children). Both reads are issued only when the plan references the corresponding intrinsics — every other query pays nothing; `trace:id` needs neither (the candidate's id is compared engine-side against its lowercase-hex rendering, and its Phase-1 generator is the `trace_id` PK prefix). A query filtering **only** on a trace-level intrinsic degrades to the same time-range superset class as `{}` today (scale characterization → #25).

**Ordering and partiality contracts** (public, docs/api.md §4.2): `traces[]` is ordered by the max timestamp of each trace's exactly-matched spans, descending, `trace_id` ascending as the tiebreak. Partiality is signalled by omission: the response OMITS `metrics.completedJobs`, leaving `{"totalJobs":1}`, whenever ANY bound engaged before natural exhaustion: (a) a generator returned `cap + 1` rows — and since issue #492 part 4 a generator whose statement carries a pushed spanset aggregate returns a SUPERSET of the traces that pass it (part 5: the aggregate is computed over the generator's rows and the evaluator's is computed over its deduplicated, selector-matched spans, so the statement admits traces phase 2 then drops), so the cap is charged against that superset rather than against the traces the selector matched, and the same query can now be complete where it was partial, (b) the consumption ceiling was reached with a lookahead candidate present, (c) a per-trace span overflow occurred. The engine's own flag is still called `partial`; it has carried no wire field of that name since #464 (docs/api.md §4.2). Budget breaches (scan rows, read/result bytes, or the engine's 256 MiB retention counter — §7) are hard `422`s, never partial results.

**Structural operators need no SQL of their own** (issue #172 design spike, ratified; extended to all 15 forms in #183): a structural relation is a per-trace set computation over data Phase 2 must fetch anyway — the hydration read already selects `parent_id`, and both operands' filters need the spans regardless — so `{A} > {B}` / `>>` / `<` / `<<` / `~` and their negated (`!>` …) and union (`&>` …) modifiers all plan as the **superset union of both operands' generators, byte-identical to the equivalent `{A} && {B}` plan** (pinned by an SQL-identity test across all 5 base ops × 3 modifiers), and the parent-id walk runs engine-side in Phase 2: one adjacency pass per trace, O(spans), bounded by `MAX_SPANS_PER_TRACE`, every intermediate charge-before-allocate against the request budget. The one genuinely new #183 read path, field-vs-field comparison (`{ .a = .b }`), prunes Phase-1 on the LHS attribute's **key-existence `(key)` scan** (an index-served superset — a matching span must possess the key) rather than a bare time-range fallback; both operands' `val`/`val_num` reads hydrate per candidate span for the engine-side compare. A recursive CTE was rejected (server-side unbounded graph recursion, cannot express the attr-membership legs without duplicating the Phase-2 evaluator in SQL, no clean mapping onto the layered budget contract) and a `trace_spans` self-join was rejected (handles only `>`, and both operands still need Phase-2 exactness — read amplification without removing any work). Because the SQL stage set is unchanged, the `cityHash64(trace_id)` co-sharding (§7) keeps structural search shard-local with the already-verified evidence, and the scan-budget contract threads through untouched.

The worked example `{ resource.service.name = "checkout" && span.http.status_code >= 500 && duration > 2s }` (last 3h, limit 20) therefore runs: one Phase-1 generator — the service-equality projection read above (`PREWHERE service = 'checkout'`, the conjunction's most selective leaf) — then per batch ONE hydration statement carrying `arrayFirstIndex((k, s) -> k = 'http.status_code' AND s = 'span', attr_key, attr_scope) AS pi0` and the predicate column `[(pi0 != 0) AND ifNull(attr_num[pi0] >= 500, 0)] AS attr_slot`, with `duration_ns > 2000000000` evaluated on the hydrated physical column; the byte-frozen SQL lives in `crates/pulsus-read/tests/golden/traces_search/`.

**TraceQL metrics** (`{...} | rate()` / `| count_over_time()`, issue #59) — one fully-pushed-down, time-bucketed conditional aggregation per request (never the two-phase candidate model). For `{ resource.service.name = "checkout" && span.http.status_code >= 500 && duration > 2s } | rate()` at step 60s:

```sql
SELECT toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL 60000000000 NANOSECOND)) + 60000 AS t,
       uniqExact(trace_id, span_id) AS n
FROM trace_spans
PREWHERE service = 'checkout'
WHERE timestamp_ns >= {S - step + 1ns} AND timestamp_ns < {E + 1ns}
  AND ((trace_id, span_id) IN (SELECT trace_id, span_id FROM trace_attrs_idx
       WHERE date >= toDate({S}) AND date <= toDate({E - 1ns})
         AND timestamp_ns >= {S} AND timestamp_ns < {E}
         AND key = 'http.status_code' AND val_num >= 500 AND scope = 'span') AND duration_ns > 2000000000)
GROUP BY t
ORDER BY t ASC
```

- **Counting is `uniqExact(trace_id, span_id)`** — the T5 logical-span identity, so at-least-once replays never inflate a bucket (no `FINAL`). `rate` divides the deduped count by the step **client-side at the encode boundary** (the instant `/query` form drops the `GROUP BY` and divides by the snapped window width); `count_over_time` ships the count as-is — the SQL body is byte-identical for both functions.
- **Snapped window, RIGHT-CLOSED range buckets:** `{S} = ⌊start/step⌋·step`, `{E} = ⌈end/step⌉·step` (epoch-aligned, outward). The **range** form labels a bucket by its RIGHT edge — label `L` covers the instants `(L − step, L]`, so an instant landing exactly on a grid point belongs to THAT point — and its window is correspondingly `(S − step, E]`, one whole step wider on the left (the extra leading bucket the emitted grid begins with) and inclusive of `E`. Over integer nanoseconds that is exactly the left-closed/right-open `[S − step + 1, E + 1)` the SQL renders, so the time and date clauses are unchanged in shape. The `- 1` inside `toStartOfInterval` and the `+ step` after it are what turn a floor into a ceiling; deleting either makes the boundary go right and moves every sample one step. **The range label's interval is rendered in NANOSECONDS** (`INTERVAL {step_ms * 1000000} NANOSECOND`), unlike the instant form's: measured on 26.3.17.110, `toStartOfInterval(DateTime64(9), INTERVAL n MILLISECOND)` converts to the interval's unit by ROUNDING before it floors, which erases a one-nanosecond shift and degenerates the ceiling back into `left_edge + step`. The **instant** form keeps the plain left-closed/right-open `[S, E)`. Every emitted bucket is full-width, so the rate denominator is always the full step. `toUnixTimestamp64Milli(...)` pins the bucket column to a deterministic `Int64` epoch-milliseconds wire type (covers pre-1970/post-2106 buckets that a `UInt32` epoch-seconds column would wrap — issue #59 re-audit). The bucketing interval is rendered in **milliseconds** (`INTERVAL {step_ms} MILLISECOND`, the unit the step is carried in end to end), not seconds: ClickHouse 26.3.17.110's `toStartOfInterval` downgrades a `DateTime64` argument to a 32-bit `DateTime` for whole-second-and-larger interval units (re-measured on the version move — issue #376), silently clamping pre-1970/post-2106 instants (and then rejecting `toUnixTimestamp64Milli`'s `DateTime64` argument outright); the millisecond-unit form keeps `DateTime64(3)` precision and range end to end.
- **Access paths:** a root-AND-spine `resource.service.name =` conjunct (never one inside/under an `||`) hoists to `PREWHERE` and selects the `service_time` projection; every attribute leaf is a **locate-then-test predicate on the span row's own arrays** (issue #559) — a `WITH` item `arrayFirstIndex((k, s) -> k = … AND s = …, attr_key, attr_scope) AS <alias>` locates the element the span's attribute resolves to and the `WHERE` clause tests that element, with `NOT (…)` around the positive test implementing the ratified absent-key negation rule; physical leaves render inline on `trace_spans` columns. Two consequences worth stating rather than leaving to be discovered. **A metrics filter now answers a span that repeats a key exactly as a search does** — the index carries no element ordinal, so the old `(trace_id, span_id) [NOT] IN (SELECT … FROM trace_attrs_idx …)` semi-join matched a span when ANY of its attribute rows matched, and the two routes disagreed. **And a filter carrying an attribute condition loses the `service_time` projection**, because that projection holds 14 named columns and not the attribute arrays: measured on a 2,000,000-span corpus — 8 attributes per span, 50 services with `checkout` 1 in 50, `http.status_code = 500` on 1 span in 100, ClickHouse 26.3.29.7 at `max_block_size = 65409`, `use_query_condition_cache = 0`, `optimize_move_to_prewhere = 1`, rebuilt by `docs/benchmarks/issue559-metrics-filter-bytes.sh`'s corpus rules — `{ resource.service.name = "checkout" } | rate()` reads `ReadFromMergeTree (service_time)` at `Granules: 7/245`, 57,344 rows and 1,892,563 bytes, while `{ resource.service.name = "checkout" && span.http.status_code >= 500 } | rate()` reads `ReadFromMergeTree (trace_spans)` at `245/245`, 2,000,000 rows and 290,003,920 bytes, with `query_log.projections` empty. **Granule, row and byte counts are layout-specific** — they hold for that corpus and those settings; what the guarding test asserts is the identity of the table read (`docs/benchmarks/traces-differential-ledger.md`, `traceql-attribute-resolves-to-one-element`).
- **Bounded state:** every metrics query carries the trace read budgets (scan rows/bytes, result bytes, throw) **plus** the IN-set limits — `max_rows_in_set` (1,000,000) / `max_bytes_in_set` (64 MiB) with `set_overflow_mode = 'throw'` → `422 query_too_broad` via its own dedicated reason, never an unbounded in-memory set. Since issue #559 the metrics FILTER builds no such set, so on that path the binding limit is `max_rows_to_read = reader.traceql_scan_budget_rows`; the set limits still bind the narrowed tag-values read, which carries these same settings and keeps its own `IN`. The constants and the 422 wording do not move. The bucket count itself is capped statically at plan time (docs/api.md §4.4).
- **Clustered:** the reader additionally injects `distributed_product_mode = 'local'`, rewriting an `IN (SELECT … FROM <table>_dist …)` subquery to the **local** shard's table (exact under the `cityHash64(trace_id)` co-sharding, and it kills the `_dist`-inside-`_dist` double-distributed path). After issue #559 the readers on this route that depend on it are `compare()`'s attribute enumeration and the narrowed tag-values read; the filter does not, because it sends no subquery. The time-bucket `GROUP BY` is **not** shard-local — buckets exist on every shard and the coordinator merges per-bucket partial states, bounded by the point cap × shard count (scale evidence routes to #25).
- **Value aggregations, grouping, quantiles, histograms, exemplars (issue #182).** `sum`/`min`/`max`/`avg_over_time(duration)` nest a per-`(t, [group], trace_id, span_id)` dedup inner query (`any(duration_ns)`) then aggregate the outer `toFloat64(sum|min|max|avg(val))`, so replays never inflate `sum`/`avg` (`min`/`max`/`count` are replay-idempotent by construction); the engine scales ns→seconds at the encode boundary. `by(resource.service.name)` adds the physical `service` column to the `SELECT`/`GROUP BY` (`… AS g0`) and runs a **distinct-by-key** cap probe (`SELECT count() FROM (SELECT <keys> … GROUP BY <keys> LIMIT cap+1)`, bounded by `reader.traceql_max_series`, default 1000) **before** the main query — a `cap+1` result is a static `422 query_too_broad`, bucket-count-independent by construction. `quantile_over_time` pushes `CAST(quantilesTDigest(q…)(val) AS Array(Float64))` (the #173 TDigest precedent) — a **deliberate, ledgered divergence** from the reference's bucket-walk percentile (`2026-08-05-traceql-quantile-over-time-tdigest`, docs/api.md §4.4.1); `histogram_over_time` pushes the reference's `Log2Bucketize` as `SELECT t, toUInt64(roundToExp2(val - 1)) * 2 AS bucket, count() AS n … WHERE val >= 2 GROUP BY t, bucket` over the SAME dedup inner query — one row per OCCUPIED `(t, bucket)`, a plain tally, never cumulative, with no bucket ladder (issue #252). Series are framed **ascending by bucket** — a ledgered divergence from the reference's lexicographic-on-`%g` order (`2026-08-05-traceql-histogram-series-order`, docs/api.md §4.4.1); ORDER only, with label values, tallies, counts and membership identical (the label TEXT differs for `2^10`..`2^13` ns, a separate recorded rendering difference with no ordering consequence). The `val >= 2` guard sits on the OUTER query, after the dedup: it reproduces the reference's sub-2ns drop and keeps the inner subquery byte-identical to the value aggregations', so PREWHERE hoisting, `service_time` projection selection and `trace_attrs_idx` granule pruning are untouched. `toUInt64` before the doubling is load-bearing — a duration above `2^62` buckets to `2^63`, which the signed form wraps to a negative `__bucket` label. Rows per step are bounded by the bit width of `Int64` — 63 buckets are reachable (`2^1`..`2^63`), gated against a static ceiling of 64 — so the bucket axis cannot breach `traceql_max_series`; throughput at 1 TB → #25. Exemplars are collected for every range shape by default: a bounded `groupArraySample(K, seed)(tuple(trace_id, timestamp_ns))` per bucket, framed into `trace:id` exemplars whose value is read at the bucket's right-closed label. `K` is the resolved TOTAL budget spread over the grid (at least 1), and the collected list is thinned to that total engine-side — the SQL cannot bound a total, because `groupArraySample`'s own `k` is per group. The extra statement is skipped outright when no framed sample is non-zero, which is the common shape for a sparse panel. `with(exemplars=…)` and the `exemplars` request parameter are the two inputs, hint first (docs/api.md §4.4); `topk`/`bottomk` reduce the (probe-capped) series set client-side per timestamp. `compare({selection})` builds an attribute cross-tab — a replay-deduped base (`GROUP BY t, trace_id, span_id`) `arrayJoin`ed over the `name`/`kind`/`status`/`resource.service.name`/`statusMessage`/`instrumentation:name`/`instrumentation:version` intrinsics and joined to the `DISTINCT (scope.key, val)` index attributes, counting `countIf(is_sel = 0)` (baseline complement) and `countIf(is_sel)` (selection) per `(t, key, value)`, plus per-key totals — bounded by the same `traceql_max_series` distinct-`(key, value)` cap probe. `statusMessage` comes from the per-span `status_message` column; every span has a `""`-or-value (no absent case), and an empty value is emitted verbatim as a DISTINCT `""` value — Tempo v3.0.2 parity (issue #185/#189: no `arrayFilter` fold-to-nil). `instrumentation:name`/`instrumentation:version` source the per-span `scope_name`/`scope_version` columns the same way (issue #192): real per-value baseline/selection counts, a scopeless span contributing the distinct `""` value. `rootName`/`rootServiceName` are resolved by a **window-free** per-trace roots read `LEFT JOIN`ed into the intrinsics branch (`argMin(name/service, root ordering)` over `trace_spans WHERE trace_id IN (SELECT DISTINCT trace_id FROM base)`, no date/time predicate) — byte-identical root selection to the §Phase-2 trace-context co-load, so compare()'s roots can never disagree with search's; it is trace-wide-exact yet bounded by the same `max_rows_in_set`/`max_rows_to_read` throw budgets (scale → issue #25). A trailing `… > 5` result comparison post-filters samples client-side. These endpoints emit the **Tempo-native `{series, metrics}` body** (docs/api.md §4.4), not the Prometheus matrix/vector envelope.

**Service graph** (`GET /api/traces/v1/service_graph`, issue #173) — one fully-pushed-down two-level aggregation over the `trace_edges` half-row ledger (§4.1), both aggregation levels in ClickHouse:

```sql
SELECT
    c.service AS client,
    s.service AS server,
    s.conn_type AS conn_type,
    count() AS calls,
    countIf(greatest(s.failed, c.failed) = 1) AS failed,
    CAST(quantilesTDigest(0.5, 0.95, 0.99)(s.duration_ns) AS Array(Float64)) AS quantiles_ns
FROM
(
    SELECT trace_id, span_id, any(pair_id) AS pair_id, any(conn_type) AS conn_type,
           any(service) AS service, max(duration_ns) AS duration_ns, max(failed) AS failed
    FROM trace_edges
    WHERE side = 1 AND date >= toDate({S}) AND date <= toDate({E - 1ns})
      AND timestamp_ns >= {S} AND timestamp_ns < {E}
    GROUP BY trace_id, span_id
) AS s
INNER JOIN
(
    SELECT trace_id, pair_id, any(conn_type) AS conn_type,
           any(service) AS service, max(failed) AS failed
    FROM trace_edges
    WHERE side = 0 AND date >= toDate({S}) AND date <= toDate({E - 1ns})
      AND timestamp_ns >= {S} AND timestamp_ns < {E}
    GROUP BY trace_id, pair_id
) AS c
ON c.trace_id = s.trace_id AND c.pair_id = s.pair_id AND c.conn_type = s.conn_type
GROUP BY client, server, conn_type
ORDER BY calls DESC, client ASC, server ASC
LIMIT {SERVICE_GRAPH_MAX_EDGES + 1}
```

- **Determinism (merge-invariant).** Every half-row carries its own plain `timestamp_ns`, evaluated per stored row, so window membership of each half is merge-invariant; the per-side `GROUP BY` performs exactly the `ReplacingMergeTree` collapse at read time, so the *edge set* and its replay-deduped `calls`/`failed` counts are a pure function of the deduped in-window half-rows — **byte-identical before and after `OPTIMIZE TABLE trace_edges FINAL`**. The `quantilesTDigest` latency quantiles are the one exception: TDigest is an approximate, merge-order-sensitive estimator, so a merge can shift a quantile slightly — they are stable only within a tolerance band (the live gate bounds the drift at ±5% + 1ns), never asserted byte-equal. An edge is reported iff BOTH halves' own timestamps fall in `[S, E)` (the normative window rule, docs/api.md §4.5); a window edge with one half only is dropped by the `INNER JOIN`.
- **Pruning (perf mandate).** Each half-scan prunes on the daily-partition `date` MinMax (the Tier-1 EXPLAIN gate) **and** the leading-`side` PrimaryKey prefix. The scan is over the payload-free ledger (~2 narrow rows per RPC edge instance vs full spans). Counting is `count()` over the deduped, `pair_id`-joined edge instances — never a bare count on the raw ledger.
- **Quantiles wire type.** `quantilesTDigest` over `Int64` is `CAST` to `Array(Float64)` so the wire type is pinned independent of the server's internal default (which is `Array(Float32)` on 26.3.17.110, re-measured on the version move — issue #376) — the whole edge-row decode path carries no f32 (`GraphEdgeRow.quantiles_ns: Vec<f64>`, `[p50, p95, p99]`).
- **Bounded response, bounded state.** `max_rows_to_read = reader.traceql_scan_budget_rows` (throw) bounds the join's scan + hash-table cost → `422 query_too_broad`; `LIMIT SERVICE_GRAPH_MAX_EDGES + 1` bounds the returned edge set (the extra row flips a non-silent `truncated` flag). The join hash table is bounded by the in-window deduped client halves.
- **Clustered.** The SQL names the `_dist` ledger on both sides and the reader injects `distributed_product_mode = 'local'` (the ratified §4.2/§4.4 semi-join pattern): halves co-shard on `cityHash64(trace_id)`, so every joinable pair is shard-local, the join executes per shard, and the initiator merges only per-`(client, server, conn_type)` partial states (TDigest states merge), bounded by distinct service-pair labels, not by edge instances (scale evidence routes to #25).

---

### 4.3 The landing table and the five tables the TraceQL reads query

Issues #584 to #586. **One push is one `INSERT` of one block into
`trace_landing`**, and the five tables below are each one materialized view
away from it; the writer names none of them. The shape, the sealed block, the
insert loop and its two fates, the budget and the byte accounting are
`docs/ingest-one-source-table.md`'s and are not restated here.

The six tables are **additional**: the reads that have not moved still answer
from `trace_spans` and `trace_attrs_idx` (§4.1, §4.2), so a trace push
performs three inserts — that path's two and this one. **No equivalence
between the two stores is claimed, required or tested.**

```sql
CREATE TABLE trace_landing (
    event_id        UUID DEFAULT generateUUIDv7(),
    received_ms     Int64  CODEC(DoubleDelta, ZSTD(1)),
    row_kind        UInt8  CODEC(ZSTD(1)),
    trace_id        FixedString(16)  CODEC(ZSTD(1)),
    span_id         FixedString(8)  CODEC(ZSTD(1)),
    parent_span_id  FixedString(8)  CODEC(ZSTD(1)),
    start_ns        Int64  CODEC(Delta, ZSTD(1)),
    duration_ns     Int64  CODEC(T64, ZSTD(1)),
    resource_id     UInt128  CODEC(ZSTD(1)),
    name            LowCardinality(String)  CODEC(ZSTD(1)),
    kind            Int32  CODEC(ZSTD(1)),
    status_code     Int32  CODEC(ZSTD(1)),
    status_message  String  CODEC(ZSTD(1)),
    trace_state     String  CODEC(ZSTD(1)),
    flags           UInt32  CODEC(ZSTD(1)),
    scope_name      LowCardinality(String)  CODEC(ZSTD(1)),
    scope_version   LowCardinality(String)  CODEC(ZSTD(1)),
    scope_attrs     JSON  CODEC(ZSTD(1)),
    events          Array(Tuple(time_ns UInt64, name LowCardinality(String), attrs JSON, attrs_other String, dropped_attrs UInt32))  CODEC(ZSTD(1)),
    dropped_events  UInt32  CODEC(ZSTD(1)),
    links           Array(Tuple(trace_id String, span_id String, trace_state String, flags UInt32, attrs JSON, attrs_other String, dropped_attrs UInt32))  CODEC(ZSTD(1)),
    dropped_links   UInt32  CODEC(ZSTD(1)),
    service         LowCardinality(String)  CODEC(ZSTD(1)),
    attrs           JSON  CODEC(ZSTD(1)),
    attrs_other     String  CODEC(ZSTD(1)),
    dropped_attrs   UInt32  CODEC(ZSTD(1)),
    day             Date  CODEC(ZSTD(1)),
    schema_url      String  CODEC(ZSTD(1)),
    tag_scope       LowCardinality(String)  CODEC(ZSTD(1)),
    tag_key         String  CODEC(ZSTD(1)),
    tag_value       String  CODEC(ZSTD(1)),
    tag_type        LowCardinality(String)  CODEC(ZSTD(1)),
    scope_schema_url    String               CODEC(ZSTD(1)),
    scope_dropped_attrs UInt32               CODEC(ZSTD(1)),
    scope_attrs_other   String               CODEC(ZSTD(1)),
    end_ns              UInt64               CODEC(Delta, ZSTD(1)),
    entity_refs         String               CODEC(ZSTD(1)),
    service_type        LowCardinality(String)  CODEC(ZSTD(1))
) ENGINE = MergeTree
PARTITION BY toStartOfHour(fromUnixTimestamp64Milli(received_ms))
ORDER BY (row_kind, trace_id, start_ns, span_id, kind, tag_key, tag_value)
SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, async_insert = 0;

CREATE TABLE spans (
    trace_id        FixedString(16)          CODEC(ZSTD(1)),
    span_id         FixedString(8)           CODEC(ZSTD(1)),
    parent_span_id  FixedString(8)           CODEC(ZSTD(1)),
    start_ns        Int64                    CODEC(Delta, ZSTD(1)),
    duration_ns     Int64                    CODEC(T64, ZSTD(1)),
    service         LowCardinality(String)   CODEC(ZSTD(1)),
    resource_id     UInt128                  CODEC(ZSTD(1)),
    name            LowCardinality(String)   CODEC(ZSTD(1)),
    kind            Int32                    CODEC(ZSTD(1)),
    status_code     Int32                    CODEC(ZSTD(1)),
    status_message  String                   CODEC(ZSTD(1)),
    trace_state     String                   CODEC(ZSTD(1)),
    flags           UInt32                   CODEC(ZSTD(1)),
    scope_name      LowCardinality(String)   CODEC(ZSTD(1)),
    scope_version   LowCardinality(String)   CODEC(ZSTD(1)),
    scope_attrs     JSON                     CODEC(ZSTD(1)),
    attrs           JSON                     CODEC(ZSTD(1)),
    attrs_other     String                   CODEC(ZSTD(1)),
    dropped_attrs   UInt32                   CODEC(ZSTD(1)),
    events          Array(Tuple(time_ns UInt64, name LowCardinality(String), attrs JSON, attrs_other String, dropped_attrs UInt32)) CODEC(ZSTD(1)),
    dropped_events  UInt32                   CODEC(ZSTD(1)),
    links           Array(Tuple(trace_id String, span_id String, trace_state String, flags UInt32, attrs JSON, attrs_other String, dropped_attrs UInt32)) CODEC(ZSTD(1)),
    dropped_links   UInt32                   CODEC(ZSTD(1)),
    scope_schema_url    String               CODEC(ZSTD(1)),
    scope_dropped_attrs UInt32               CODEC(ZSTD(1)),
    scope_attrs_other   String               CODEC(ZSTD(1)),
    end_ns              UInt64               CODEC(Delta, ZSTD(1)),
    service_type        LowCardinality(String)  CODEC(ZSTD(1))
) ENGINE = ReplacingMergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(start_ns), 'UTC')
ORDER BY (intDiv(start_ns, 300000000000), trace_id, start_ns, span_id, kind)
TTL toDateTime(least(intDiv(start_ns, 1000000000) + (7 * 86400), 4294967295))
SETTINGS ttl_only_drop_parts = 1, index_granularity = 2048;

CREATE TABLE traces (
    day           Date                                                 CODEC(ZSTD(1)),
    trace_id      FixedString(16)                                      CODEC(ZSTD(1)),
    start_ns      SimpleAggregateFunction(min, Int64)                  CODEC(ZSTD(1)),
    end_ns        SimpleAggregateFunction(max, Int64)                  CODEC(ZSTD(1)),
    root          SimpleAggregateFunction(min, Tuple(UInt8, Int64, FixedString(8), String, String)) CODEC(ZSTD(1)),
    services      SimpleAggregateFunction(groupUniqArrayArray, Array(String)) CODEC(ZSTD(1)),
    last_start_ns SimpleAggregateFunction(max, Int64)                         CODEC(Delta, ZSTD(1)),
    buckets       SimpleAggregateFunction(groupUniqArrayArray(4096), Array(Int64))  CODEC(ZSTD(1))
) ENGINE = AggregatingMergeTree
PARTITION BY day
ORDER BY trace_id
TTL toDateTime(least(intDiv(last_start_ns, 1000000000) + (7 * 86400), 4294967295))
SETTINGS index_granularity = 1024, ttl_only_drop_parts = 1;

CREATE TABLE resources (
    day            Date                    CODEC(ZSTD(1)),
    resource_id    UInt128                 CODEC(ZSTD(1)),
    service        LowCardinality(String)  CODEC(ZSTD(1)),
    attrs          JSON                    CODEC(ZSTD(1)),
    attrs_other    String                  CODEC(ZSTD(1)),
    dropped_attrs  UInt32                  CODEC(ZSTD(1)),
    schema_url     String                  CODEC(ZSTD(1)),
    entity_refs    String                  CODEC(ZSTD(1))
) ENGINE = ReplacingMergeTree
PARTITION BY day
ORDER BY (service, resource_id)
TTL toDateTime(least(((toUInt32(day) + 1) * 86400) + (7 * 86400), 4294967295));

CREATE TABLE tag_names (
    scope  LowCardinality(String)  CODEC(ZSTD(1)),  -- span | resource | event | link | instrumentation
    key    String                  CODEC(ZSTD(1))
) ENGINE = ReplacingMergeTree
ORDER BY (scope, key);

CREATE TABLE tag_values (
    scope     LowCardinality(String)  CODEC(ZSTD(1)),
    key       String                  CODEC(ZSTD(1)),
    value     String                  CODEC(ZSTD(1)),
    val_type  LowCardinality(String)  CODEC(ZSTD(1))  -- string | int | float | bool
) ENGINE = ReplacingMergeTree
ORDER BY (scope, key, value, val_type);

CREATE MATERIALIZED VIEW spans_mv TO spans AS
SELECT trace_id AS trace_id, span_id AS span_id, parent_span_id AS parent_span_id,
       start_ns AS start_ns, duration_ns AS duration_ns, service AS service,
       resource_id AS resource_id, name AS name, kind AS kind,
       status_code AS status_code, status_message AS status_message,
       trace_state AS trace_state, flags AS flags,
       scope_name AS scope_name, scope_version AS scope_version,
       scope_attrs AS scope_attrs, attrs AS attrs, attrs_other AS attrs_other,
       dropped_attrs AS dropped_attrs, events AS events, dropped_events AS dropped_events,
       links AS links, dropped_links AS dropped_links,
       scope_schema_url AS scope_schema_url,
       scope_dropped_attrs AS scope_dropped_attrs,
       scope_attrs_other AS scope_attrs_other, end_ns AS end_ns,
       service_type AS service_type
FROM trace_landing WHERE row_kind = 0;

CREATE MATERIALIZED VIEW resources_mv TO resources AS
SELECT day AS day, resource_id AS resource_id, service AS service, attrs AS attrs,
       attrs_other AS attrs_other, dropped_attrs AS dropped_attrs, schema_url AS schema_url,
       entity_refs AS entity_refs
FROM trace_landing WHERE row_kind = 1;

CREATE MATERIALIZED VIEW traces_mv TO traces AS
SELECT toDate(fromUnixTimestamp64Nano(s), 'UTC') AS day, trace_id, s AS start_ns, e AS end_ns,
       r AS root, sv AS services,
       ls AS last_start_ns, bk AS buckets
FROM (SELECT trace_id, min(start_ns) AS s,
             max(toInt64(least(toUInt64(start_ns) + toUInt64(duration_ns), 9223372036854775807))) AS e,
             min((toUInt8(parent_span_id != toFixedString('', 8)), start_ns, span_id,
                  toString(service), toString(name))) AS r,
             groupUniqArray(toString(service)) AS sv,
             max(start_ns) AS ls,
             groupUniqArray(4096)(intDiv(start_ns, 300000000000)) AS bk
      FROM trace_landing WHERE row_kind = 0
      GROUP BY trace_id);

CREATE MATERIALIZED VIEW tag_names_mv TO tag_names AS
SELECT tag_scope AS scope, tag_key AS key
FROM trace_landing WHERE row_kind = 2;

CREATE MATERIALIZED VIEW tag_values_mv TO tag_values AS
SELECT tag_scope AS scope, tag_key AS key, tag_value AS value, tag_type AS val_type
FROM trace_landing WHERE row_kind = 3;
```

**Four discriminating values, five views, five targets.** `row_kind` says which
landed event a row is; a row sets that kind's columns and the rest default.

| `row_kind` | the landed event | emitted |
|---|---|---|
| 0 | a span | one per decoded span |
| 1 | a resource | one per distinct `(resource_id, day)` **in the push** |
| 2 | a tag name | one per distinct `(scope, key)` **in the push** |
| 3 | a tag value | one per distinct `(scope, key, value, type)` **in the push** |

- **The discriminator is `row_kind`, not `kind`.** A span carries an OTLP span
  kind in a column already called `kind`, which `spans` and its
  `ReplacingMergeTree` key both use; two columns cannot both be `kind`.
- **`service`, `attrs`, `attrs_other` and `dropped_attrs` are shared between
  kind 0 and kind 1.** A span's attributes and a resource's are never on one
  row, so one JSON column serves both, the way `log_landing.timestamp_ns`
  serves a line time and a pattern bucket.
- **The sorting key has two runs because the kinds do.** After the
  discriminator it mirrors `spans`' own `(trace_id, start_ns, span_id, kind)`;
  `tag_key, tag_value` last order kinds 2 and 3 inside themselves, where the
  four span columns are all at their defaults.
- **There is no cache of anything already written, anywhere on this path.**
  Kinds 1, 2 and 3 are deduplicated **inside the push only**, from the push's
  own decoded spans. A push that fails re-emits everything next time. The two
  shipped signals can key a commit-promoted LRU on a time bucket, so a
  registration lost to a fan-out failure reappears at the next bucket;
  `tag_names` and `tag_values` are time-less by contract (`docs/api.md` §4.3)
  and have no heal interval at all.
- **`async_insert = 0` is on the landing table because the query pin cannot
  reach the table setting.** `executeQuery.cpp` disjoins
  `table->areAsynchronousInsertsEnabled()` into a local that both the
  eligibility and the execution block read, and the query setting's explicit
  `0` is never consulted again. What the `CREATE` value defends against is a
  server-wide `<merge_tree>` default; nothing defends against a deliberate
  `ALTER TABLE … MODIFY SETTING` on this table.
- **`event_id` is the landed event's identity and the writer never sets it.**
  It is not the retry mechanism: that is the `insert_deduplication_token` the
  writer mints per sealed block.
- **The landing table has no `_dist` wrapper**, for the reason `log_landing`
  has none: a push carries many trace ids, so inserting the push itself
  through a `Distributed` wrapper would split one push per shard and one push
  would stop being one block. The routing happens one step later, on the way
  out of the two per-trace views (§7).
- **The catalogs carry no TTL; the other three targets carry one in their
  `CREATE`.** `spans` expires on `start_ns`, `traces` on `last_start_ns`, and
  `resources` on the end of its `day`, each plus `PULSUS_RETENTION_DAYS` in the
  saturating form. `tag_names` and `tag_values` carry a deduplication window
  and no TTL, because `docs/api.md` §4.3 requires catalog entries to outlive
  span retention.
- **Six deduplication windows, one per write-path table.** A view's insert
  into its target carries a block id derived from the source block, and only a
  table with a window recognises the repeat.

## 5. Profiles

**Query shapes served:** flamegraph merge over `(profile type, service, selector, time range)`; profile-value time series; diff between two ranges.

```sql
CREATE TABLE profile_samples (
    type_id        LowCardinality(String),        -- e.g. process_cpu:cpu:nanoseconds:cpu:nanoseconds
    service        LowCardinality(String),
    fingerprint    UInt128,
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
    fingerprint  UInt128,
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
    fingerprint  UInt128
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

## 6. Rules, and how the schema is created

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
```

### How the schema is created

**`schema/schema.sql` is the DDL and `schema/schema.sh` applies it. The binary creates no schema.**

```sh
schema/schema.sh                      # single-node
PULSUS_CLUSTER=prod schema/schema.sh  # clustered
schema/schema.sh --print              # render the statements, send nothing
```

The script reads the same environment variables the binary does ([configuration.md §3](configuration.md)), gates on the server version, **drops the configured database**, creates it, and sends one statement per request — the HTTP interface refuses a multi-statement body. 60 statements single-node, 75 clustered.

**There are no migrations.** No numbered list, no bookkeeping table, no checksum drift guard. A schema change is an edit to `schema.sql` and another run of the script; the cost is the data in the database, and the development workflow is to drop and recreate. Nothing is upgraded in place, including retention: `PULSUS_RETENTION_DAYS` is declared in each table's `CREATE`, so changing it is a rebuild.

**One file, two variants.** A line prefixed `--@single` is used on a single node, one prefixed `--@cluster` on a cluster, and every other line by both. A statement that exists in one variant only carries the prefix on all of its lines. The clustered side is where the `Replicated*` engines, `ON CLUSTER`, the `_dist` wrappers and the replication paths live.

**Tokens are double-brace.** `{{db}}`, `{{on_cluster}}`, `{{retention_days}}` and the rest are substituted by the script and, for the two things read at run time, by `pulsus-schema`'s own renderer. ClickHouse's `{shard}` and `{replica}` are the **server's** macros and must reach it as those exact characters: a substitution matching a single brace pair eats them, and the server accepts the result without complaint — every shard then joins one replica set. Nothing in the file may be a single-brace token, and `crates/pulsus-schema/tests/schema_file.rs` holds that, along with the rule that every replication path names the table of its own `CREATE`.

**Two things are read out of the file at run time**, neither of them DDL: a materialized view's own projection, which `rebuild-metrics` and `rebuild-traces` replay a landing window through, and a table's declared column list, from which the trace fetch derives its projection.

---

## 7. Distributed layout

Enabled by `PULSUS_CLUSTER`. Every table becomes `ReplicatedMergeTree`-family, and every **per-shard** table but the three landing tables gets a Distributed wrapper. Eight tables get none. Three are the landing tables — `log_landing`, `metric_landing` and `trace_landing` (issues #603 and #586; the reason, and what it costs, are below, in [ingest-one-source-table.md](ingest-one-source-table.md), and in §4.3's own bullet for the trace landing table). The other five are the cluster-wide ones, one replica set each spanning every shard (`/clickhouse/tables/all/<db>.<table>`), read from the local replica without fan-out, so a wrapper would put a row on one shard and leave every other shard's read missing it permanently: `metric_metadata`, `trace_tag_catalog`, `resources`, `tag_names` and `tag_values` — the last row of the table below is theirs. **Sharding keys are chosen so that reads join and aggregate shard-locally** (finding #2):

| Table | Sharding key | Why |
|-------|--------------|-----|
| `metric_samples`, `metric_hist_samples`, `metric_samples_5m/_1h`, `metric_series` | `cityHash64(fingerprint)` | the fingerprint is the series ID, which includes the metric name (issue #623), so the shard key is the series identity: a series still lives whole on one shard, per-series evaluation and tier `GROUP BY` stay shard-local, and same-labelset metrics spread across the cluster. **One read is not reduced shard-locally, and neither is the one it replaces** (issue #549): the grouped instant read's window pipeline is not pushed to shards, so each shard returns its matched rows and the reduction happens at the coordinator — measured on a two-shard fixture, 40 series over 60 steps, the follower returned 960 rows of 960 on BOTH routes, so the change neither worsens nor improves that hop. What it moves is the coordinator's hop to the client, 2,400 rows to 240 on that fixture |
| `metric_labels` | `cityHash64(fingerprint)` | one lookup row per series (issue #623). **Nothing is inserted through the wrapper**: one kind-2 landing row writes a series' activity row and its lookup row on the node that took the push, so every series read that nests the two runs shard-local (`distributed_product_mode = 'local'`) and finds a series' lookup row beside its activity |
| `log_samples`, `log_streams`, `log_streams_idx`, `log_metrics_5s`, `log_patterns` | `cityHash64(fingerprint)` | **this expression governs reads, not writes** (issue #603). The writer inserts into `log_landing` under its bare name and the views write that shard's local targets, so a fingerprint's rows sit wherever its pushes landed and can sit on several shards. The reads tolerate it: the stream-resolution `GROUP BY fingerprint HAVING ...` counts distinct `(key, val)` pairs and `log_streams_idx`'s sorting key covers its whole row, so a cross-shard duplicate changes nothing; stage-2 hydration already keeps one row per fingerprint; the rollup, `/patterns` and volume reads merge partial aggregates at the initiator. What it ends is shard-local **label discovery**, whose semi-join asked whether a fingerprint was active *anywhere* and could only be answered per shard while one fingerprint's index and rollup rows co-resided — see the paragraph below the tables  **The key hashes the column rather than being the column** (issue #498): a `Distributed` sharding key must evaluate to an integer type ClickHouse accepts, and `UInt128` is not one. Measured on ClickHouse 26.3.29.7, a two-shard fixture: `Distributed(..., fingerprint)` creates without complaint and then answers every insert with `Code: 53. DB::Exception: Sharding key expression does not evaluate to an integer type`, leaving `count()` at 0; `Distributed(..., cityHash64(fingerprint))` creates, inserts and reads the 128-bit value back intact. One fingerprint still maps to one shard, now through both 64-bit halves rather than the low one. |
| `trace_spans`, `trace_attrs_idx`, `trace_edges`, `trace_recent`, `trace_error_spans`, `spans`, `traces` | `cityHash64(trace_id)` | a trace is whole on one shard; span-level intersections, trace assembly, and the service-graph half-row pairing (both edge halves share `trace_id`, so the query-time join is shard-local) are all shard-local. `spans` and `traces` are the two tables the TraceQL read design queries (issues #584 to #586); their wrappers carry the key as a literal rather than through `Ddl::Dist`, for the reason §4.3 gives, and `the_routing_wrappers_use_the_family_sharding_expression` is what keeps the two forms identical |
| `profile_samples`, `profile_series`, `profile_series_idx` | `cityHash64(fingerprint)` | same co-sharding argument as logs |
| `rules`, catalogs, bookkeeping | (replicated to all shards via a shard-less replication path — one cluster-wide replica set, no Distributed writes) | tiny, read-everywhere; **prerequisite: `{replica}` macros must be unique across the whole cluster**, not merely within a shard |

Fan-out analysis for the canonical operations:

| Operation | Shards doing work | What crosses the network |
|-----------|-------------------|--------------------------|
| Trace by ID | **unmeasured, by owner decision of 2026-10-02** | one trace |
| PromQL selector fetch | all (each holds a disjoint series subset) | only matched series' samples, already time-cut |
| PromQL grouped instant read (issue #549) | all (each holds a disjoint series subset) | the same matched rows, time-cut the same way — the reduction to the answer happens at the coordinator, not on the shard |
| PromQL gauge-on-tier | all, **partial aggregation per shard** | per-step aggregate states, not samples |
| LogQL stream resolution + read | all; each stage's own partials complete per shard and merge at the initiator | matched log lines only |
| TraceQL search | all; intersections shard-local (a trace's index rows and spans co-reside) | top-K candidates per shard |
| Label/tag discovery | all, twice for logs: the activity scan, then the index scan | deduplicated key/value sets, plus (logs) the active-fingerprint set once to the initiator and once into each shard's copy of the rendered index scan |

Every local table but the two landing tables gets a Distributed wrapper of this shape (the schema controller renders one per table from the sharding-key column above):

```sql
CREATE TABLE log_samples_dist AS log_samples
ENGINE = Distributed('{cluster}', pulsus, log_samples, cityHash64(fingerprint));

CREATE TABLE metric_samples_dist AS metric_samples
ENGINE = Distributed('{cluster}', pulsus, metric_samples, cityHash64(fingerprint));
```

The two derived trace tables (#560) get the same wrapper, from the Traces family's expression, and a block written through `trace_spans_dist` reaches each shard's local `trace_spans`, whose views write that shard's `trace_recent` and `trace_error_spans` — so a trace's derived rows sit on the shard that holds its spans:

```sql
CREATE TABLE trace_recent_dist AS trace_recent
ENGINE = Distributed('{cluster}', pulsus, trace_recent, cityHash64(trace_id));

CREATE TABLE trace_error_spans_dist AS trace_error_spans
ENGINE = Distributed('{cluster}', pulsus, trace_error_spans, cityHash64(trace_id));
```

A failing view behaves the same way on each shard as on a single node: see §4.1, "What a failing view leaves behind (#560)". Clustered, the repeated-block rule is the `Replicated*` engines' own `replicated_deduplication_window`, and it applies to `trace_spans` as well as to both derived tables.

Two invariants the schema controller enforces, because co-location silently breaks without them: **every table in a signal family uses the byte-identical sharding expression** (raw, tiers, series/index tables alike — a divergence would put a series' rollups on a different shard than its samples), and **every insert that has a `_dist` wrapper goes through it or computes the same expression client-side** — the trace writer never freelances shard placement. **The two landing tables are the deliberate exception** (issue #603): a logs or metrics push carries many fingerprints, so a `Distributed` insert would split one push into one insert per shard and would return before the shards held the rows — either alone ends "one push is one block", which is the guarantee that change was made for. Those two writes are placed by the connection, and the views put each block's derived rows on the shard that took it. Cluster configs use `internal_replication = true` (the underlying tables are `ReplicatedMergeTree`; the Distributed layer must write each block to one replica and let replication fan it out, or rows duplicate).

**What logs label discovery costs once placement is decoupled, in bytes and in marks** (issue #603). The three discovery scans used to read `FROM log_streams_idx_dist … WHERE fingerprint IN (SELECT DISTINCT fingerprint FROM log_metrics_<res>_dist …)` under `distributed_product_mode = 'local'`, whose exactness rested on a stream's index rows and its rollup rows being on the same shard. They are not, any more: a `log_streams_idx` row is written once per `(key, val, fingerprint, month)`, by the view off the block that first registered that stream in that month — one shard — while a `log_metrics_<res>` row is written for every `(fingerprint, bucket)` a push touches, on every shard that ever took one. So a shard evaluating locally could only answer *was this fingerprint active on this shard*, where the question is *was it active anywhere*. The activity scan is therefore **a statement of its own**, dispatched first, and its result is rendered into the index scan as a literal `fingerprint IN (…)` list — the shape `/series` already used. The answer is the one the subquery expressed.

- **Bytes: bounded in rows and in rendered text, unmeasured on the wire.** The active-fingerprint set crosses once to the initiator and once into each shard's copy of the rendered statement, where before it never left its shard. Both directions are bounded by shipped guards — the set at `PULSUS_MAX_STREAMS` rows, checked inside the scan's own loop, and the statement at the rendered-SQL ceiling — and by nothing stated here. No byte figure is given: a fingerprint does not render as bare digits (`FpLiteral` writes `toUInt128('…')` around the decimal value), so the rendered form is wider than its digits and wider again than the 16 bytes a binary set would carry. **It cannot be measured from a single node.** What would measure it: on the two-shard fixture, one discovery query per statement shape and each shard's own `ProfileEvents['NetworkSendBytes']`/`['NetworkReceiveBytes']` from `system.query_log` with the shape beside each figure — the per-shard, coordinator-inclusive Tier-1 instrument §9 defines. It belongs with the M1 re-capture below.
- **Marks: unsettled, and no equality is claimed.** `log_streams_idx` is `PARTITION BY month ORDER BY (key, val, fingerprint)`; a discovery scan bounds `month` and fixes or ranges `key`, and `fingerprint` is the third key component, so within one `key` the marks are ordered by `val` before it and a `fingerprint IN (…)` set — a subquery's or a literal list's — has no contiguous mark range to prune. That argument does not close: on a split fixture the subquery form's set is empty on the shard holding the index row, and what the optimizer does with an empty `IN` set decides the marks. Nobody has run both shapes on two shards. `crates/pulsus-read/tests/live_logs_cluster_discovery.rs` records both figures and bounds each shape by its own scan budget; it asserts no equality.
- **Discovery gains a cap it did not have.** A window with more active streams than `PULSUS_MAX_STREAMS` is now `422 query_too_broad` where the subquery form answered — on single-node deployments as well as clustered ones. That makes discovery consistent with stage-1 stream resolution.

Reader-issued settings in clustered mode: `optimize_skip_unused_shards = 1`, `optimize_distributed_group_by_sharding_key = 1` (so `GROUP BY fingerprint` shapes skip the coordinator re-aggregation the co-sharding makes unnecessary), `distributed_aggregation_memory_efficient = 1`, `prefer_localhost_replica = 1`, and `skip_unavailable_shards` per `PULSUS_SKIP_UNAVAILABLE_SHARDS`. Where a coordinator-built `IN (...)` list would be large, the planner switches to the JOIN/subquery form so the filter executes as a remote-local subquery rather than shipping the set twice. There is **no** `rand()` sharding anywhere in the schema.

**TraceQL search reader-settings contract** (issue #57). Precondition: `trace_spans` and `trace_attrs_idx` co-shard on the byte-identical `cityHash64(trace_id)` expression, so a trace's spans and index rows always co-reside — every Phase-1 `GROUP BY trace_id` completes on whole groups per shard, and every Phase-2 `trace_id IN (batch)` read (batches are ≤ 32 explicit ids, never a large coordinator-built set) prunes to the owning shards under `optimize_skip_unused_shards`. Every search query — generators and hydration/value batches alike — additionally carries server-side budgets with throw semantics: `max_rows_to_read = PULSUS_TRACEQL_SCAN_BUDGET_ROWS` + `read_overflow_mode = 'throw'` (non-indexable generators are budget-limited → `422 query_too_broad`, never silently slow), `max_bytes_to_read` + the same throw mode, `max_result_bytes` + `result_overflow_mode = 'throw'`, and `max_block_size = TRACE_SEARCH_MAX_BLOCK_ROWS` (4096 rows). Enforcement of the byte ceilings is **block-granular**, but (issue #57 re-audit) the transient is now HARD-bounded, not merely accepted-and-documented: every string value the search response returns (`name`/`service`, and `select()`-projected attribute values) is truncated at the SOURCE with a hard **byte** ceiling — `if(length(col) <= 8192, col, substringUTF8(col, 1, 2048)) AS col` (`TRACE_STR_COL_CAP` = 8192 bytes; the fallback branch cuts at 2048 UTF-8 code points, each ≤ 4 bytes, so it too never exceeds the byte ceiling) — so the driver's one transiently-buffered result block is bounded at ≤ `TRACE_SEARCH_MAX_BLOCK_ROWS` rows × (2 × `TRACE_STR_COL_CAP` string bytes + fixed-width columns) ≈ ≤ ~67 MB, never a-priori row-unbounded. **Live-verified on 24.8:** the result-side budget (`max_result_bytes` + `result_overflow_mode = 'throw'`) does not throw on **unwrapped passthrough columns** in streamed `SELECT` shapes; the source-truncation projection above makes its accounting **effective** on the hydration/root/value reads — a **deliberate hardening**. Layer 1 (64 MiB `max_result_bytes` per query) is therefore the practical **per-batch** byte bound on the search's Phase-2 reads, firing server-side before the driver materializes anything; Layer 2 — the engine's request-scoped 256 MiB retention counter (charged per row/entry as results stream) — remains the binding bound on **cross-batch retained accumulation** (merge tuples, the membership sets the hydration statement's predicate columns fill, heap-held response summaries, root summaries), which survives each batch's charge release and which no per-query server setting can see. A breach of either layer is a `422`, never an OOM. The engine's bounded-consumption guarantee is Rust-side (bounded generator transfer + the retention counter); ClickHouse's own generator-aggregation memory is additionally bounded (issue #57 re-audit, sub-problem B) by a dedicated generator-only ceiling — `max_memory_usage = PULSUS_TRACEQL_GENERATOR_MAX_MEMORY_BYTES` (512 MiB default) + `max_bytes_before_external_group_by = 0` (throw-not-spill) — so a dense common-value prefix's `GROUP BY trace_id` aggregation state is hard-bounded too: a breach is server code 241 (`MEMORY_LIMIT_EXCEEDED`) → `422 query_too_broad`, never an OOM (read-cost, as opposed to memory, is bounded by prefix confinement + the per-query server budgets; a read-bounded common-value generator SQL shape is tracked in issue #63). The "TraceQL search" and "Trace by ID" fan-out rows above are confirmed by Tier-1 per-stage/per-shard evidence on the 2-shard fixture (`docs/benchmarks/m4-traces-read-path.md` — coordinator-inclusive `system.query_log` rows verdicted against a client-computed `cityHash64(trace_id) % total_weight` roster, the same methodology that graduated the logs family); one caveat noted there, now partly closed: the trace-by-ID single-shard confinement was proven under the §7 reader-issued settings, which the search engine injects in clustered mode and which **the fetch handler now injects too** (issue #587 — the fetch has its own settings root, carrying the clustered-reader block, the catalog read budgets and `final = 1`, and deliberately carrying neither `distributed_product_mode` nor `load_balancing`: the first has no subquery over a `Distributed` table to act on, and the second closes none of the replication-visibility states it was proposed for). **What is NOT closed is the confinement itself**: the fetch's statements are not the one that evidence measured, and whether the shard prune reduces them to the owning shard is **unmeasured by owner decision of 2026-10-02**. Both are correct without it — the coordinator fans out, the shards that do not hold the trace return nothing, and the answer is the same; what is lost is work, not correctness.

**Status: logs family graduated (M1, issue #16) and the traces read-path rows confirmed (M4, issue #57 — `docs/benchmarks/m4-traces-read-path.md`, 2-shard fixture, same methodology, wired into `schema-it-cluster` as hard verdicts); metrics/profiles remain design intent, not observed behavior.** Co-sharding is necessary but not sufficient — Distributed plans can still merge at the initiator or ship large sets if the generated SQL doesn't cooperate, which is why graduation requires *per-stage, exact-shard-roster* evidence, not just a terminal-query spot check (three rounds of CODE review on issue #16 caught successively narrower gaps: the first draft graduated on terminal-stage-only evidence with the discovery row uncovered; the second added per-stage evidence but silently excluded the coordinator's own shard from every row — under `prefer_localhost_replica = 1` the initiator's local-shard read is logged as its own `is_initial_query = 1` row, not a separate `is_initial_query = 0` sub-query row, and a filter that keeps only `is_initial_query = 0` misses it entirely; the third accepted any shard count for fingerprint-pruned stages without deriving which shards were *expected*, so a genuinely lost `system.query_log` row was indistinguishable from correct `optimize_skip_unused_shards` pruning). The `log_samples`/`log_streams`/`log_streams_idx`/`log_metrics_5s` sharding row above, and both logs-relevant rows of the Fan-out analysis table ("LogQL stream resolution + read" and "Label/tag discovery"), are confirmed by Tier-1 evidence for the SHAPE they state — which stages run shard-locally and what crosses the network — and, where a stage prunes by `fingerprint`, not for any shard count, which is selector-dependent (issue #498, below). **A stage with no `fingerprint` condition is not affected**: its expected roster is unconditionally the whole cluster, derived from no fingerprint, so no sharding key moves it — which is the case for label/tag discovery and for stream resolution (docs/schemas.md §9's two-tier model): per-shard `system.query_log` + `EXPLAIN PIPELINE`, captured separately for **every** stage each shape executes (resolution, hydration, samples/rollup read, and the discovery query in its own right) and **every** shard including the coordinator's own, verified against a **client-computed expected shard roster** (a cumulative-weight slot→shard map from `system.clusters`/`system.macros`, the sharding key modulo `total_weight` per queried fingerprint for pruned stages — `fingerprint % total_weight` when this evidence was captured, `cityHash64(fingerprint) % total_weight` since issue #498 widened the column). **Which shard owns which fingerprint moved with that key, so the capture's roster, placement and per-shard row counts are superseded and are marked as such where they appear — and so is whether a fingerprint-scoped stage participates on a subset at all: measured against a live server, the ten-fingerprint stage that reached three of four shards under the previous key reaches all four under the current one. The execution shape and the capture method are not superseded.** **Issue #603 supersedes the same figures a second time and further**: the writer no longer inserts through the `_dist` wrappers at all, so placement follows the connection that took the push rather than the sharding key, and one fingerprint's rows can sit on several shards — the roster, the placement and the per-shard row counts are superseded whatever the key is, and "LogQL stream resolution + read" now merges each stage's partials at the initiator rather than completing shard-locally. No read's ANSWER changes; label discovery becomes two statements (above). **The re-capture has not been done.** It needs the four-shard fixture, the same corpus **pushed through the ingest API** rather than loaded through the `_dist` wrappers, a rerun of `xtask bench --dist` on the 4-shard fixture (`docs/benchmarks/m1-logs-read-path.md`), and — belonging with the same capture — the per-shard network figure for each discovery statement shape — showing stage-1 stream resolution executing shard-locally (reaching the full 4-shard roster), hydration/samples joining and reading shard-locally (narrowing to *exactly* the computed owning subset for narrow fingerprint predicates, with the excluded shard's absence proven, not assumed), and the label/tag discovery query itself fanning out across the full roster with only deduplicated results crossing the network. This is topology mechanics a 4-shard cluster demonstrates at any corpus scale. **Latency at Tier-2 scale (1 TB/7d) is separately tracked and unvalidated (issue #25)**; it does not gate this graduation, which is about which node does the work, not how fast. The traces rows ("Trace by ID" and "TraceQL search") were confirmed the same way in M4 — per-stage, coordinator-inclusive, roster-verdicted evidence on the 2-shard fixture (`docs/benchmarks/m4-traces-read-path.md`, `cityHash64(trace_id) % total_weight` client-side derivation, run as hard CI verdicts by `cargo xtask bench traces-read` — **for the search stages; the trace-by-ID stage's roster verdict is exempted there by the same owner decision, so that stage is captured and its coordinator-row check verdicted while no shard set is claimed for it**), with the trace-by-ID caveat noted there (proven under the §7 reader-issued settings, which the fetch handler now injects — issue #587; and superseded in the other direction by the same change, which gave the fetch different statements whose shard prune is unmeasured by owner decision). The metrics/profiles rows above remain design intent until the M3/M5 multi-shard benchmarks confirm them the same way; those snapshots then join the CI regression set.

---

## 8. Cross-cutting defaults

| Concern | Decision |
|---------|----------|
| `index_granularity` | 8192 default everywhere; revisit per-table only with benchmark evidence |
| `PREWHERE` | planner always places the most selective low-cardinality predicate (`metric_name`, `service`, `type_id`) in `PREWHERE`; time predicates in `WHERE` (partitions already prune them) |
| Partitioning | **daily** for raw sample/span tables (short TTL, whole-part drops); **monthly** for series/index/tier tables (long-lived, low-churn) |
| TTL | `ttl_only_drop_parts = 1` on all raw tables; per-tier retention on rollups; `PULSUS_STORAGE_POLICY` for hot/cold volumes |
| Dedup strategy | metadata: `ReplacingMergeTree` + duplicate-tolerant reads (`LIMIT 1 BY`, `GROUP BY`); samples: append-only `MergeTree`, with a retried push suppressed at ingest by the writer that accepted the original, inside `PULSUS_INGEST_DEDUP_WINDOW` (issue #494), and writer batch atomicity below that |
| Codecs | timestamps `DoubleDelta`, gauge-like floats `Gorilla`, counters/ids `Delta`/`T64`, payloads/labels `ZSTD(3..5)`, everything wrapped in `ZSTD(1)` minimum |
| Minimum ClickHouse | 26.3 LTS (the supported LTS line; older servers do not tag an HTTP-200 mid-stream exception, so it cannot be told apart from result text — issue #412. All MVs are classic incremental — no refreshable-MV or scheduler dependency) |

---

## 9. Validation plan

The schemas are accepted only with benchmark evidence, produced by the M-milestone e2e harness on a reference 4-node cluster (8 vCPU, local NVMe per node) and a single-node baseline:

**Datasets.** Metrics, two tiers: an accuracy corpus of 10k series (counter/gauge/histogram mix) for differential testing, and a **scale corpus of 5M active series (churning to ~20M distinct over 30 days) — the design-target cardinality**, exercising label-cache memory and refresh, `metric_series` volume, selector resolution past the cache cap, and shard balance under `cityHash64(fingerprint)`. The scale corpus deliberately includes skewed metrics (one metric at ~2M series, mid-cardinality metrics at ~500k, a long tail at ≤10k) so label resolution — the cache matcher and the lookup and activity reads of §2.1 — is measured where each is weakest. Both use 30 days of **mixed source resolutions** — 1s, 15s, 60s, 5m, plus deliberately irregular/jittered push cadences — because PulsusDB assumes no scrape interval and the engine's interval-derived semantics (extrapolation, staleness) must be validated across all of them. Logs: 1 TB over 7 days across 50 services / 5k streams. Traces: 100M spans over 7 days. Profiles: 1M profiles over 7 days.

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
