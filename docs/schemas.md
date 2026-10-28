# PulsusDB ClickHouse Schemas

**Parts of this document describe the previous trace search engine, which issue #602 part 2c-2b deleted — its test binaries, goldens and benches in part 2c-2b-1, its code and the unit tests inside it in part 2c-2b-2; the search route's design is `docs/TraceQL/sql-schema.md` §5.2–§5.3.** **Tag names and unnarrowed tag values are read from `tag_names` and `tag_values` since issue #598 part 1; the passages on `trace_tag_catalog` describe the read before it.** **Since #598 part 2 the narrowed tag values and the span names are read from `spans`.** The authoritative storage design. **`log_patterns` now also carries `level` (keyed after `pattern`) and `log_landing` `pattern_level`, for `/patterns` by level; the §3.1 `log_patterns` text predates that, and `schema/schema.sql` is authoritative.** **Since #597 `compare()` is read from `spans`, `traces` and `resources` in one statement; §4.2's text on `compare()` describes the read before it.** **Since #599 the service graph is read from `service_edges`, per-minute edge totals a refreshable view (`service_edges_mv`) appends from `spans`, over the minutes it has finalized, and from `spans` for the calls not wholly inside those minutes; a half stored after its interval became eligible can be in neither (`docs/api.md` §4.5); the `trace_edges` passages describe the read before it. §8's "no refreshable-MV or scheduler dependency" no longer holds.** **Issue #602 part 3b-2 deleted the six tables of the previous trace store (`trace_spans`, `trace_attrs_idx`, `trace_tag_catalog`, `trace_edges`, `trace_recent`, `trace_error_spans`) and their views; passages outside §4 and §7 that name them describe the store before it.** **`metric_exemplars` and landing kind 4 (issue #501) are not described here; `schema/schema.sql` defines them.** Every table is specified with its full DDL, the queries it exists to serve, and the read path those queries take — including the SQL PulsusDB generates. [architecture.md](architecture.md) summarizes these decisions; this document is the reference the schema controller implements. **Stale in §6:** the binary no longer reads a view's projection out of `schema/schema.sql`; the rebuild commands that did are removed. **Since #601 part 4b the trace store holds seven tables, not five, and the counts, targets, storage figures and statement contracts below predate it, except the clustering and sharding-key passages, which name the new tables.** `tag_bucket_values` holds each 5-minute bucket's span-attribute values per service and key, storing at most 101 distinct `(value, type)` pairs: up to 100 answer exactly, and a 101st marks the bucket overflowed. It answers a values read for a `span.<key>` whose `q` is a single filter of exactly one `resource.service.name` equality; when a whole bucket of the window is overflowed, the request is answered by the span read in a second statement — the one exception this part adds to one statement per request. `error_spans` holds the error spans' non-attribute columns; it answers a single-filter, non-structural search or non-`compare()` metrics query with a top-level `status = error` that reads only `status`, `statusMessage`, `kind`, `name`, `duration`, `span:id`, `trace:id`, `resource.service.name`, `traceDuration`, `rootName` or `rootServiceName`. Measured on g1: the store is 43.89 B/span over the seven tables (`tag_bucket_values` 0.734, `error_spans` 0.245), and between replicas an error span's columns cross once more in `error_spans`, 0.253 B/span (0.63% of the span stream), while `tag_bucket_values` replicates to every node. The current statements are `docs/api.md` §4.2 and §4.3 and `schema/schema.sql`. **Since #601 part 4c the trace store holds eight tables: `service_bucket_resources` records which resources each service's spans used in each 5-minute bucket, and answers a values read for a `resource.<key>` whose `q` is exactly one `resource.service.name` equality over the whole window (0.0019 B/span on g1, replicated to every node). A statement that keeps one row per span identity computes every value it reads from those rows inside that step, and a metrics `by()` on a resource attribute reads a label computed once per resource.** **Parts of this document describe the schema controller, the migration catalogue and the TTL rotation, which no longer exist. The schema is now `schema/schema.sql`, built by `schema/schema.sh`; the binary creates no schema, and retention is the TTL each table is created with.** **The rollup levels `metric_samples_<N>s` and their views (issue #18 part A) are built and read for the functions `docs/api.md` §3.6 lists (issue #18 part B).**

**The optimization target is read latency**: dashboard panels, log searches, and trace lookups must be index-served, bounded, and shard-local wherever possible. Ingestion adapts to the schema, never the reverse.

---

## 1. Why not the first-generation layout

ClickHouse observability layers of the first generation typically share one shape: a single generic samples table for logs *and* metrics, a label inverted index consulted before every read, JSON labels reconstructed at query time, and distributed tables sharded for write convenience. That shape has well-understood failure modes, which this design treats as requirements to engineer away:

| # | Failure mode in first-generation schemas | PulsusDB design response |
|---|------------------------------------------|--------------------------|
| 1 | Every label query is a **two-stage lookup** (label index → fingerprint set → samples); high-cardinality selectors create huge intermediate fingerprint sets | Metrics: label resolution moves to an in-process cache; SQL receives a bounded, sorted `fingerprint IN` list, or a JOIN fallback past a threshold. Logs/traces: intersections run *inside* the index table as a single `GROUP BY ... HAVING` pass, and the planner starts from the most selective matcher with hard caps |
| 2 | Label index distributed tables sharded by **`rand()`** — every label lookup broadcasts to all shards and nothing joins shard-locally | All index tables are **co-sharded with their data tables** (same sharding key). Intersections, joins, and per-series aggregation execute shard-locally; only reduced results cross the network |
| 3 | **No log-body text index** — `|= "substring"` scans every log line in range | a `text(tokenizer = ngrams(4))` index on the body column; line filters compile to `body LIKE '%…%'` / `match(body, …)` predicates that skip non-matching granules |
| 4 | Samples **ordering key is configurable** — a wrong choice silently destroys either per-series reads or time scans | Ordering keys are **fixed** per table and chosen from the dominant query shape. No deployment-time ordering knobs |
| 5 | Trace payload table ordered only for **trace-ID fetch**; service + time search depends entirely on the attribute index | `trace_spans` carries a **projection** physically ordered by `(service, timestamp_ns)` — both access patterns are primary-index reads on the same table |
| 6 | **Per-day series metadata** — a 30-day query touches 30 index partitions and re-reads the same series 30 times | Series and label-index tables partition **monthly**; a 30-day query touches 1–2 partitions and each series appears once or twice |
| 7 | **Regex and negative matchers** fall off the `(key, val)` index | Metrics: matched in-process against cached labels (regex is a RAM problem, not a scan problem). Logs/traces: regex/negative matchers evaluated over the *values of one key* (a single index prefix range), never over raw data |
| 8 | **Generated SQL quality** ignored — no `PREWHERE`, no rollup routing, repeated index scans, coordinator-only aggregation | The planner is specified alongside the schema (§ per signal): time and low-cardinality predicates ride in `PREWHERE`, rollups are routed automatically, every intersection is a single pass, aggregation is pushed to shards |
| 9 | **One generic data model** for metrics and logs despite opposite query patterns | Four signals, four schemas. Nothing is shared but conventions |

Fixes proposed elsewhere that this design deliberately **rejects**, and why:

- **`tokenbf_v1` on the labels JSON column** — duplicates the label index's job with false positives and storage cost, and doesn't help the second-stage sample read. The log *body* carries the `text` index, where there is no better structure.
- **`Map(String, String)` label columns** — measurably slower to extract from than JSON strings in ClickHouse, and PulsusDB rarely extracts labels in SQL at all (labels resolve in-process or from a hydration read).
- **Materializing arbitrary user labels onto every sample row** — repeats low-cardinality values billions of times and ties the schema to today's queries. The single exception is `service` (§3.2): OpenTelemetry guarantees it exists, it has ideal clustering properties, and the planner can *always* derive it — so it earns a place in the ordering key. No other label gets one.
- **`minmax` skip indexes on unclustered string columns** — near-zero granule skipping unless data is physically clustered by that column; where we need that clustering we buy it explicitly (ordering key or projection).
- **`ReplacingMergeTree` for sample data** — merge-time dedup forces `FINAL` or wrong results. Sample tables are plain `MergeTree`; only metadata tables use `ReplacingMergeTree`, and their read shapes (`LIMIT 1 BY`, `GROUP BY`) are duplicate-tolerant by construction.

Conventions used below: `<db>` defaults to `pulsus`; in clustered mode every table becomes `Replicated*`, and every per-shard table but `log_landing`, `metric_landing`, `trace_landing`, `metric_label_values` and `service_edges` also gets a `_dist` Distributed wrapper — those five and the cluster-wide tables get none (§7 lists all eleven); a `TTL` clause shows the token of the setting it renders: `{{retention_days}}` is `PULSUS_RETENTION_DAYS`, which has no default; `{{metrics_catalogue_retention_days}}`, on the five metric catalogue tables, the largest of that and every rollup level's retention; `{{level_retention_days}}`, a rollup level's `PULSUS_ROLLUP_RETENTION_DAYS` item, or `PULSUS_RETENTION_DAYS` when that is unset; and `{{metrics_landing_retention_hours}}`, `{{log_landing_retention_hours}}` and `{{trace_landing_retention_hours}}`, the three landing tables' hours ([configuration.md §7](configuration.md)). Label keys follow the canonical label model ([architecture.md §2.3](architecture.md)): log label keys are normalized at ingest (`service.name` → `service_name`, before fingerprinting); trace attribute keys are stored verbatim; **OTLP metric names and label keys follow Prometheus v3.13.0's OTLP receiver instead** (issue #461) — the metric name gains its unit and type suffixes, attribute keys are sanitized with the reference's `key`/`key_` prefix rule and collisions merge with `;`, resource attributes become `job`/`instance` plus a `target_info` series rather than per-series labels, and the strategy is selectable with `PULSUS_OTLP_TRANSLATION_STRATEGY` ([configuration.md §5](configuration.md)); remote-write names and labels arrive already translated and are stored verbatim; the promoted physical column is named `service` on the logs, traces, and profiles tables (metrics deliberately have none — reads there are `metric_name` + `fingerprint` driven). **Every DDL block in this document mirrors `schema/schema.sql`, which is applied to a fresh ClickHouse in CI** (`crates/pulsus-schema/tests/live_schema.rs`) — an unapplyable table definition is a build failure, not a docs bug. **The generated-SQL examples below are a different matter and no suite executes one**: several carry `{placeholders}` and could not run as written. Each is instead bound to the code that renders it where one exists — §2.3's grouped instant read is asserted byte for byte against its builder by `the_grouped_statement_in_schemas_md_is_the_one_the_builder_renders`, and §4.2's shapes by the TraceQL SQL suites' doc-consistency tests. Latency figures in §9 are targets to validate, not guarantees.

---

## 2. Metrics

**Query shapes served:** instant/range PromQL over one metric with label selectors (dominant); label/series discovery; long-range dashboards (30d+); high-frequency `count by` meta-queries.

The schema's PromQL obligation is **fetch shapes, plus one reduction**: full PromQL evaluation — all functions, operators and subqueries — happens in the engine against the columns below, with a single enumerated exception. `min`, `max`, `count` and `group` over a plain instant selector are reduced in ClickHouse SQL (§2.3, issue #549), because each is exactly reproducible in a statement with no runtime condition, and so are `sum`, `avg`, `count`, `min` and `max` over `rate`, `irate` or `increase` of a plain range selector (issue #579), at any node of the query. Everything else evaluates in the engine, so language coverage is still independent of the schema ([architecture.md §5.1](architecture.md)). One planned extension: **native histogram samples get dedicated storage in M7** (a histogram-typed samples table or serialized sparse-histogram column, designed in that milestone); until then OTLP exponential histograms flatten to classic `_bucket`/`_sum`/`_count` series at ingest.

### 2.1 Tables

```sql
CREATE TABLE metric_samples (
    org_id       LowCardinality(String),     -- the tenant (issue #635)
    fingerprint  UInt128   CODEC(ZSTD(1)),   -- the series ID
    unix_milli   Int64    CODEC(DoubleDelta, ZSTD(1)),
    value        Float64  CODEC(Gorilla, ZSTD(1))
) ENGINE = MergeTree
PARTITION BY toDate(fromUnixTimestamp64Milli(unix_milli), 'UTC')
ORDER BY (org_id, fingerprint, unix_milli)
TTL toDateTime(least(intDiv(unix_milli, 1000) + ({{retention_days}} * 86400), 4294967295))
SETTINGS ttl_only_drop_parts = 1,
         primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 1, min_columns_to_activate_adaptive_write_buffer = 1;
```

- **The tenant leads every metrics key** (issue #635). A metrics request names its tenant in `X-Scope-OrgID`; no header is the empty tenant `''`, the single-tenant deployment. Every metrics table carries `org_id LowCardinality(String)` first in its sorting key, every view copies it from `metric_landing`, and every metrics table a read names carries `org_id = <tenant>` in its own `WHERE`, so two tenants' equal series IDs never meet in one statement. The five tables that collapse rows with equal keys would otherwise merge two tenants' series into one row. Partitions and the `_dist` sharding key are unchanged. Measured on 200 metrics over ten tenants: the column costs 0.005 bytes per sample row, and one metric's read is unchanged.
- **The series ID leads the key after the tenant** (issue #623). `fingerprint` is the series ID: the top 32 bits of `cityHash64(metric_name)`, then the low 96 bits of the 128-bit hash of `metric_name ++ 0xFF ++` the label buffer (`pulsus_model::series_fingerprint`). One label set under two names is two series, and a metric's series share the name prefix, so they sort together as they did when `metric_name` led the key: reading one metric's IDs reads one key range. Measured: 1,000 IDs of one of 200 metrics, a day merged into one part, read 81,920 sample rows; a uniform hash puts them in about 763 granules, 6.25 million rows. Two names sharing a prefix share their granules; identity is unaffected. Among 100,000 names about one pair is expected to share a 32-bit prefix (298 pairs at 24 bits); two names sharing a prefix each read the other's samples in that range, and no series is merged (issue #635).
- **Sample tables are read only with an exact ID list and a time range.** Every flexible match — the metric name, regexes, label matchers — runs on the lookup table below, which yields the IDs.
- **Each series is contiguous** → per-series reads (every PromQL evaluation) are sequential scans of a few granules.
- **Daily partitions** on the raw table: retention drops whole partitions (`ttl_only_drop_parts`), and time predicates prune partitions before the index is even consulted.
- **No string data.** The fetch hot path moves only `(UInt64, Int64, Float64)` columns.
- **The whole key stays in the in-memory index** (issue #623). At the engine's default ratio of `0.9`, a part where the leading column is nearly unique per granule drops the later key columns from the index it keeps in memory, and a one-series read stops pruning on them: measured on a mixed-volume demo part at ratio 0.912, one series for a day selected 91 marks at the default and 2 with the setting at `1`, same answer. `metric_hist_samples` carries the same setting.
- **Resolution-agnostic.** Sample timestamps are stored **verbatim at millisecond precision** — never quantized, bucketed, or aligned. PulsusDB assumes nothing about the source scrape/export interval: 1s, 15s, 5m, or irregular push cadences all land as-is, per-series intervals may differ and drift, and the PromQL engine derives actual intervals from the data (as Prometheus does for extrapolation and staleness) rather than from configuration. Rollup tiers (§2.2) are *optional derived data* at operator-chosen resolutions; they never constrain or replace what raw ingestion accepts.
- **Admitted metric timestamp domain and TTL (issue #137, mirroring #131).** Ingest admits a metric data point only if its UTC day lies in `[1970-01-01, 2106-02-06]` (days `0..=49_709`, `pulsus_model::Date::start_of_day_utc_ms_datetime_safe`); a data point outside that domain is rejected (OTLP metrics: per-point partial success; remote write: per-sample drop counted in `rejected_total`). Two wrap mechanisms motivate the gate: `PARTITION BY toDate(...)` evaluates in the 16-bit `Date` domain and wraps for days past 2149-06-06, and the delete-TTL evaluates the row timestamp in the 32-bit `DateTime` domain and wraps for instants past 2106-02-07T06:28:15Z (u32-seconds maximum, `4294967295`); day `49_710` (2106-02-07) is excluded because only part of it is u32-representable. The TTL is the one `schema/schema.sql` creates `metric_samples` and `metric_hist_samples` (§2.4) with, `toDateTime(least(intDiv(unix_milli, 1000) + retention_days * 86400, 4294967295))`, so for a stored row with epoch-seconds `s = intDiv(unix_milli, 1000)` the operative expiry is `expiry(s) = min(s + retention_days * 86400, 4294967295)` — i.e. `min(configured_expiry, 2106-02-07T06:28:15Z)`. If `s + retention_days * 86400 <= 4294967295`, the expiry equals the configured instant, bit-identical to the pre-#137 expression; otherwise the expiry is `4294967295`, the actual retention is `4294967295 - s`, and the shortfall vs the configured value is `s + retention_days * 86400 - 4294967295`, which grows with `retention_days`. For the enforced range `retention_days >= 1` (`schema/schema.sh` refuses `< 1`), a row at the last admitted day (`49_709`, `s = 4_294_943_999`) has actual retention capped at `4_294_967_295 - 4_294_943_999 = 23_296 s ≈ 0.27 days (~6.5 hours)`. For every enforced `retention_days >= 1`, the saturating form strictly dominates the pre-#137 expression: pre-#137, a row with `s + retention_days * 86400 > 4294967295` wrapped to a ~1970-epoch expiry and its part became drop-eligible immediately or near-immediately after insert (`ttl_only_drop_parts = 1`); under the saturating form the same row becomes drop-eligible no earlier than 2106-02-07T06:28:15Z. The admission cutoff is deliberately not coupled to `retention_days`: retention is set only when `schema/schema.sh` builds the database, a whole number of days from `1` to `4294967295` ([configuration.md §7](configuration.md)), and changing it is a rebuild; the cutoff is fixed by the 32-bit TTL domain instead, and the saturating form keeps a row whose configured expiry passes 2106-02-07T06:28:15Z until that instant.

```sql
CREATE TABLE metric_labels (                       -- the lookup
    org_id       LowCardinality(String),
    metric_name  LowCardinality(String),
    fingerprint  UInt128  CODEC(Delta(8), ZSTD(1)),
    labels       String  CODEC(ZSTD(5)),              -- canonical JSON, sorted keys, no __name__
    first_seen   SimpleAggregateFunction(min, Int64) CODEC(ZSTD(1)),
    last_seen    SimpleAggregateFunction(max, Int64) CODEC(ZSTD(1))
) ENGINE = AggregatingMergeTree
ORDER BY (org_id, metric_name, fingerprint)
TTL toDateTime(least(((intDiv(last_seen, 86400000) + 1) * 86400) + ({{metrics_catalogue_retention_days}} * 86400), 4294967295))
SETTINGS allow_dimensions_outside_sorting_key = 1, min_columns_to_activate_adaptive_write_buffer = 1;

CREATE TABLE metric_series (                       -- the activity
    org_id       LowCardinality(String),
    day          Date,
    fingerprint  UInt128  CODEC(ZSTD(1)),
    metric_name  LowCardinality(String),
    hours        SimpleAggregateFunction(groupBitOr, UInt32)   -- bit h: samples in hour h, UTC
) ENGINE = AggregatingMergeTree
PARTITION BY day
ORDER BY (org_id, fingerprint)
TTL toDateTime(least((toUInt64(toUInt16(day)) + 1 + {{metrics_catalogue_retention_days}}) * 86400, 4294967295))
SETTINGS allow_dimensions_outside_sorting_key = 1, ttl_only_drop_parts = 1;

CREATE TABLE metric_label_index (                  -- the label index (issue #635)
    org_id       LowCardinality(String),
    key          LowCardinality(String),
    value        String   CODEC(ZSTD(1)),
    fingerprint  UInt128  CODEC(ZSTD(1)),
    last_seen    Int64    CODEC(DoubleDelta, ZSTD(1))   -- the kind-2 row's unix_milli
) ENGINE = ReplacingMergeTree(last_seen)
ORDER BY (org_id, key, value, fingerprint)
TTL toDateTime(least(((intDiv(last_seen, 86400000) + 1) * 86400) + ({{metrics_catalogue_retention_days}} * 86400), 4294967295));

CREATE TABLE metric_label_values (                 -- the index's distinct values
    org_id       LowCardinality(String),
    key          LowCardinality(String),
    value        String   CODEC(ZSTD(1)),
    last_seen    Int64    CODEC(DoubleDelta, ZSTD(1))   -- max(unix_milli) of the block's rows
) ENGINE = ReplacingMergeTree(last_seen)
ORDER BY (org_id, key, value)
TTL toDateTime(least(((intDiv(last_seen, 86400000) + 1) * 86400) + ({{metrics_catalogue_retention_days}} * 86400), 4294967295));
```

- **The label index answers a read with no metric name** (issue #635). The same kind-2 row feeds `metric_label_index_mv`, one row per label of the series, and `metric_label_values_mv`, one row per key and value; each carries the kind-2 row's `unix_milli` as `last_seen` (the values view the block's `max`), the version a merge keeps and the instant the row's TTL counts from. A series read with no metric-name scope, no `__name__` matcher and at least one label matcher that does not match the empty string takes its IDs from the index: matchers that do not match `""` intersect their rows, the others remove theirs with `NOT IN`, and a regex runs on `metric_label_values`' distinct values, the index then reading only those values' rows. A stored empty value reads as absent, as `JSONExtractString` reads it, so the two rows that test presence carry `value != ''`. Every other read renders the lookup statement unchanged. `/labels` and `/label/{name}/values` answer a filter with no metric name as `SELECT DISTINCT` over the index. Each kind-2 row writes one index row per label: measured on a 200,000-series block, the views raise the insert's CPU from 902 to 2,571 ms and its rows written from 600,021 to 1,848,989, and the merged index stores 99.8 bytes per series.

- **The lookup answers every matcher; the activity answers the window** (issue #623). One kind-2 landing row feeds both views: `metric_labels_mv` writes the series' lookup row and `metric_series_mv` its activity row. `metric_labels` holds **one row per series**, keyed `(metric_name, fingerprint)`: a name equality, or a regex with a literal prefix, is a key range; other name regexes run once per distinct name; label matchers read the rows the name leaves, by `JSONExtractString(labels, '<key>')` (absent is `''`). `metric_series` holds one row per series per UTC day, `hours` a 24-bit mask of the hours the series had samples in, OR'd together on merge. No read joins the two: a read takes the IDs the lookup selects and keeps those the activity table finds in the window. `labels` and `metric_name` are functions of `fingerprint` that sit outside their tables' keys and are not aggregates, which 26.8 refuses unless the table sets `allow_dimensions_outside_sorting_key = 1`; both tables set it, and since one fingerprint has one label set and one name, a merge cannot pick between different values.
- **First and last seen.** The lookup view sets both `first_seen` and `last_seen` to the kind-2 row's `unix_milli`, the hour the writer registered the series in; merges keep the `min` and the `max`. A read takes `min(first_seen)` and `max(last_seen)` by fingerprint, exact across unmerged parts and shards. No read uses them yet.
- **Activity and lookup rows expire with the samples.** `metric_series` keeps a day until its last sample has expired (`day + 1 + C`, `C` the largest of `PULSUS_RETENTION_DAYS` and every rollup level's retention, [configuration.md §7](configuration.md)), capped as the sample tables are. `metric_labels`, `metric_label_index` and `metric_label_values` keep a row until the UTC day of its `last_seen` is past `C`; the label sets are no longer kept permanently.
- **Exact to the hour, no `FINAL`.** The writer registers a series once per hour it has samples in (`pulsus_model::ACTIVITY_BUCKET_MS`, fixed), skipping known `(metric_name, fingerprint, hour)` triples through an in-process LRU. A read bounds `day` by the window's first and last UTC day and tests each row's `hours` against that day's hours of the window, so an unmerged row is tested on its own and a merge changes no answer. Measured: 300 random windows of up to six days over 1,000 series active in a random tenth of 168 hours answered exactly the hourly form's sets (`activity_is_exact_to_the_hour`). The discovery endpoints built on this table (`/api/v1/series`, `/labels`, `/label/{name}/values`, docs/api.md §3.3) therefore answer the series active in the window's hours: a bounded superset of the reference's exact-sample-window set, never a subset and never a false empty.

#### The four statements

`up{job="api", status=~"5.."}` over 2026-09-07 22:30 to 23:30 UTC. The window renders as `day BETWEEN d0 AND d1` and the mask `multiIf(day = d0 AND day = d1, bits(h0, h1), day = d0, bits(h0, 23), day = d1, bits(0, h1), 16777215)`, `bits(a, b) = (1 << (b + 1)) - (1 << a)`; across two days the first branch cannot hold and renders `0`. A regex matcher adds one `0 * match('', <pattern>) = 0` line to the activity read: the compile probe (§2.3).

Statement 1, series IDs — the fallback's `IN` set and the `info()` cardinality probe:

```sql
SELECT fingerprint
FROM metric_series
WHERE org_id = ''
  AND day BETWEEN '2026-09-07' AND '2026-09-07'
  AND 0 * match('', '(?-s)^(?:5..)$') = 0
  AND bitAnd(hours, multiIf(day = '2026-09-07' AND day = '2026-09-07', 12582912, day = '2026-09-07', 12582912, day = '2026-09-07', 16777215, 16777215)) != 0
  AND fingerprint IN (
    SELECT fingerprint
    FROM metric_labels
    WHERE org_id = ''
      AND metric_name = 'up'
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
  WHERE org_id = ''
    AND metric_name = 'up'
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
WHERE org_id = ''
  AND day BETWEEN '2026-09-07' AND '2026-09-07'
  AND bitAnd(hours, multiIf(…)) != 0
ORDER BY metric_name
```

Statement 4, labels for IDs the request window already bounds — the fallback fetch's label hydration, over the IDs that returned samples:

```sql
SELECT fingerprint, any(name) AS metric_name, any(label_text) AS labels
FROM (
  SELECT fingerprint, metric_name AS name, labels AS label_text
  FROM metric_labels
  WHERE org_id = ''
    AND metric_name IN ('up', 'down')
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
    org_id       LowCardinality(String),
    metric_name  LowCardinality(String),
    metric_type  LowCardinality(String),   -- counter | gauge | histogram | summary
    help         String,
    unit         String,
    updated_ns   Int64
) ENGINE = ReplacingMergeTree(updated_ns)
ORDER BY (org_id, metric_name, metric_type, help, unit)
TTL toDateTime(least(intDiv(updated_ns, 1000000000) + ({{metrics_catalogue_retention_days}} * 86400), 4294967295));
```

`metric_type` also drives the planner: counter functions on rollup tiers are only legal because the type is known. **One row is one distinct descriptor of a name** (issue #500): the key is the whole `(org_id, metric_name, metric_type, help, unit)`, so a name with two descriptors — two writers', say — keeps both, and `/api/v1/metadata` lists each. **`updated_ns` is the `ReplacingMergeTree` version column** (issue #26, mirroring `log_streams`' `ReplacingMergeTree(updated_ns)`): a resend of the same descriptor collapses on merge to its latest, and the `TTL` counts `retention_days` from it, so a descriptor still pushed never expires and one no longer pushed leaves. The writer emits a row (receiver-injected `now_ns`) unless it sent the same descriptor within the hour (a bounded cache of the descriptors it sent, promoted only when the block commits; issue #623) — idempotent on repeats, and the hourly resend keeps a live descriptor inside its retention. On a single-host demo every push re-sending every descriptor came to 345,362 rows an hour, 39% of all landed rows.

**`metric_metadata.metric_name` is keyed by the BASE family name, never a derived-series name** (issue #27 architect plan, task-manager-pinned docs contract). A receiver that flattens one metric descriptor into several physical series — a histogram's `<name>_bucket`/`<name>_sum`/`<name>_count`, an exponential histogram's identical shape, or a summary's quantile series plus `<name>_sum`/`<name>_count` — registers exactly **one** `metric_metadata` row for `<name>` itself, typed `histogram`/`summary`, never one row per suffixed series. **Any consumer resolving a metric family's type must strip a trailing `_bucket`, `_sum`, or `_count` suffix (and, for a Summary's quantile series, no suffix at all — the quantile series shares the base name verbatim, distinguished only by its `quantile` label) before looking the family up in `metric_metadata`.** This is the contract issue #30 (label cache)/#31/#32 (PromQL planner, counter-function legality, rollup eligibility) implement against — not tribal knowledge. A lookup that fails to strip suffixes will find no metadata row for `<name>_bucket`/`<name>_sum`/`<name>_count` at all (they were never registered under those names) and must not misinterpret that absence as "unknown metric".

### 2.2 Rollup levels

The rollup levels hold per-series summaries of each bucket of a fixed size, one table and one timed view per level (issue #18 part A). A PromQL range query's response grid follows its time range; each eligible selector reads the level that range selects, a finer level, or raw ([api.md](api.md) §3.6). The statements are rendered from one template at the end of `schema/schema.sql`.

**The settings.** `PULSUS_ROLLUP_LEVELS` lists the levels in seconds, smallest first, and defaults to `5,60,720,7200,86400`; set and empty, it builds no levels ([configuration.md §7](configuration.md)). `PULSUS_ROLLUP_RANGES` gives, for each level after the first, the query range in seconds from which it is the resolution (default `3600,86400,604800,7776000`), and `PULSUS_ROLLUP_COUNTER_BUCKETS` the buckets a `rate` or `increase` range covers at least (default `5`); their rules are in configuration.md §7. `schema/schema.sh` refuses a list with exit `2` before it contacts the server, and `pulsus_schema::parse_rollup_levels` refuses the same lists with the same messages, unless: every item is a whole number from 1 to 86400 with no leading zero, and none is empty; each divides 86400; one below 60 divides 60; each is greater than, and a multiple of, the one before; there are at most 8; and `2 × max(first, 60) + 870 ≤ 3600 × PULSUS_METRICS_LANDING_RETENTION_HOURS`. Changing the set is a rebuild.

| | |
|---|---|
| table | `metric_samples_<N>s` for a level of `N` seconds: `MergeTree`, `ORDER BY (org_id, fingerprint, t_end)`, `PARTITION BY toDate(fromUnixTimestamp64Milli(t_end - 1), 'UTC')`, `index_granularity = 1024`, `COMMENT 'rollup_res_ms=<N>000 rollup_from_ms=<F> counter_buckets=<C>'`, `<F>` `0` for the first level and that level's range followed by `000` for the others, `<C>` the counter bucket count, which the server reads back to choose levels; clustered, a `metric_samples_<N>s_dist` wrapper on `cityHash64(fingerprint)` |
| bucket | `(t_end − N s, t_end]`, so a bucket never crosses a UTC day |
| projection | `by_parent`, `SELECT *` ordered by `(intDiv(t_end - 1, <next level's ms>), org_id, fingerprint, t_end)`, on every level whose next level is shorter than a day: the next level's run reads its window through it. A level whose next level is a day has none: that run reads its source's day partition |
| expiry | the start of the UTC day of `t_end − 1 ms` + the level's `PULSUS_ROLLUP_RETENTION_DAYS` item, `PULSUS_RETENTION_DAYS` when that is unset, dropped by whole part (`ttl_only_drop_parts = 1`) ([configuration.md §7](configuration.md)) |
| view | `metric_samples_<N>s_mv`, `REFRESH <timer> APPEND TO metric_samples_<N>s`; each run writes its data rows and its run rows in one statement, one part per partition, with `max_threads = 2` |

**A data row** (`kind = 0`) summarizes one series in one bucket:

| column | value |
|---|---|
| `t_end` | the bucket's end |
| `t_last`, `last` | the time and value of the bucket's last sample that is not a stale marker; `t_last = 0` when the bucket holds only stale markers |
| `min`, `max`, `sum`, `count` | over the bucket's non-NaN values |
| `inc` | the counter increase inside the bucket: over its samples that are not stale markers, `v − q` when `v ≥ q`, else `v`, and `0` when either is NaN; `q` is the series' previous sample that is not a stale marker, from the run's samples or, when the run holds none before it, from the level's own newest row before the run; a `q` more than 24 hours (86,400,000 ms) before `v` is not used and the step counts `0` |
| `flags` | bit 1 (`1`): the bucket's last sample is a stale marker; bit 2 (`2`): the bucket holds a NaN that is not a stale marker |
| `built_at` | the run's clock, in milliseconds |

A coarser level's row is composed from the rows of the level below in its bucket: `count`, `sum` and `inc` summed, `min` and `max` over the rows with `count > 0`, `last` and `t_last` from the row with the largest `t_last`, flag 1 from the row with the largest `t_end`, flag 2 set when any row has it.

**Run rows.** Every run that has a window, whether or not it found samples, appends a run row: `kind = 1`, `org_id = ''`, `fingerprint = 0`, `t_last` = the window's start, `t_end` = its end, `built_at` = the run's clock, other columns `0`. A window that crosses midnight writes one run row per UTC day. A level's coverage is the union of its run rows' `(t_last, t_end]`, and its watermark the largest `t_end`.

**Timers.** With `p = max(first level, 60 s)`, the first level runs `EVERY p`. Level `i > 1` runs `EVERY R_i`, offset by `d_i mod R_i` seconds when that is not 0, where `d_1 = ⌈150 s ÷ p⌉ × p` and `d_i = d_{i−1} + 60 s`. Each is written in the largest of `DAY`, `HOUR` and `MINUTE` that divides it, else `SECOND`. For the default set:

| level | timer |
|---|---|
| 5 s | `EVERY 1 MINUTE` |
| 60 s | `EVERY 1 MINUTE` |
| 720 s | `EVERY 12 MINUTE OFFSET 300 SECOND` |
| 7200 s | `EVERY 2 HOUR OFFSET 360 SECOND` |
| 86400 s | `EVERY 1 DAY OFFSET 420 SECOND` |

**The first level's read.** It reads `metric_landing` (`kind = 0`) with exact repeats of `(series, time, value bits)` dropped and the samples of one millisecond ordered by value bits. A run's window ends at the last whole period at least 150 s ago, the settle time an insert may still commit in. It starts at the watermark (with no run row yet, one period before that end), covers five periods at most per run, and starts no earlier than `metric_landing` still holds: `now − H hours + 720 s`, rounded up to a period. A sample in the window takes as its previous sample one less than 120 s before the window's start. Landing is keyed by `(kind, intDiv(unix_milli, 300000), org_id, metric_name, fingerprint, unix_milli)`, and the read names the 5-minute blocks it needs, so a run reads only them.

**Every other level's read.** It reads the level below, only inside that level's run rows. A run builds from the first bucket after its watermark that one source run interval covers wholly, ten buckets at most, and only up to where that interval ends. Buckets a hole in the source's run rows touches are not built at this level. A run with no such bucket writes nothing.

**Clustered.** On each shard one replica, the one with the smallest `replica_num`, builds that shard's slice of the fingerprint range; the others select nothing (§7).

### 2.3 Read paths (generated SQL)

**`rate(http_requests_total{job="api", status=~"5.."}[5m])`, 24h window, 60s step.** The label cache resolves both matchers (regex included) in-process → sorted fingerprints. One fetch:

```sql
SELECT fingerprint, unix_milli, value
FROM metric_samples
WHERE org_id = {tenant}
  AND unix_milli >  {start - 300000 - lookback}
  AND unix_milli <= {end}
  AND fingerprint IN (toUInt128('101'), toUInt128('205'), toUInt128('990'), ...)
ORDER BY fingerprint, unix_milli, reinterpretAsUInt64(value)
```

At one `(fingerprint, unix_milli)`, distinct floats are read in value-bits order. **Each selector reads the two sample tables in two concurrent statements** (issue #623): the float read above and the same selection over `metric_hist_samples`, sent together, merged in the reader with a histogram winning a key both tables hold. The second statement costs server time, not latency. A statement over a table holding none of the metric's rows still costs about 3 ms; the concurrent pair overlaps it, and one statement over both tables (`UNION ALL`) pays it in series — measured on a single-host demo, one float series: the pair answered in 6.69 ms, the one statement in 10.11 ms (median of 40, one client). Skipping the statement a series' cached value kinds say is empty is not done: a sample of the other kind with an old timestamp, ingested after the cache's last refresh, would be left out.

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

**`max by (status) (http_requests_total)`, one hour, 15 s step — the grouped instant read (issue #549).** `min`, `max`, `count` and `group` over a **plain** instant selector — no range, no `offset`, no `@`, no subquery context, one concrete metric name with no other `__name__` matcher or none at all (issue #579) — do not take the fetch above. They compile into ONE statement per fingerprint chunk, which returns the answer already reduced:

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
        if(countIf(NOT is_hist AND NOT isNaN(v)) = 0, argMaxIf(v, transform(fingerprint, fps, arrayEnumerate(fps), toUInt32(0)), NOT is_hist),
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
                    PARTITION BY fingerprint ORDER BY ts, is_hist, reinterpretAsUInt64(v)
                    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING),
                  ts + lookback) AS cover_end
          FROM (
            SELECT fingerprint, unix_milli AS ts, value AS v, CAST(0, 'UInt8') AS is_hist,
                   reinterpretAsUInt64(value) = 9218868437227405314 AS stale
            FROM metric_samples
            WHERE org_id = '' AND unix_milli > 1782906900000 AND unix_milli <= 1782910800000 AND fingerprint IN fps
            UNION ALL
            SELECT fingerprint, unix_milli AS ts, CAST(0, 'Float64') AS v, CAST(1, 'UInt8') AS is_hist,
                   reinterpretAsUInt64(sum) = 9218868437227405314 AS stale
            FROM metric_hist_samples
            WHERE org_id = '' AND unix_milli > 1782906900000 AND unix_milli <= 1782910800000 AND fingerprint IN fps
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

**Same query over 30 days, step 1h.** 361 points per series, 2 hours apart from `start`, at a step up to 2 h; the 7200 s level when `start` is on a 2-hour boundary, a finer level otherwise ([api.md](api.md) §3.6).

**`count by (job) (up)`** — **the cache-only answer was withdrawn** (issue #33; [architecture.md §5.1](architecture.md)): the label cache records activity at bucket granularity and cannot tell "had a sample inside the 5-minute staleness lookback" from "active somewhere in an up-to-24h-old bucket", and it returned 69 series where the reference correctly returned 57. `count`/`group` read samples like every other aggregation. What issue #549 changed is the SHAPE of that read, not the rule: over a plain instant selector it is one statement per fingerprint chunk that applies the lookback in SQL (§2.3), so the answer is still computed from real samples.

### 2.4 Native histogram samples

The M7 extension foreshadowed in §2 lands as a **separate, dedicated samples table** — `metric_hist_samples` — storing Prometheus native (sparse) histograms in their integer wire form. Float samples in `metric_samples` (§2.1) are untouched: the float fetch hot path, its EXPLAIN gate, and its migration checksum cannot regress. The two tables share the same identity, ordering, partitioning, TTL, and — in clustered mode — sharding key, so a series' float and histogram samples always co-reside (see co-sharding note below).

```sql
CREATE TABLE metric_hist_samples (
    org_id             LowCardinality(String),
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
PARTITION BY toDate(fromUnixTimestamp64Milli(unix_milli), 'UTC')
ORDER BY (org_id, fingerprint, unix_milli)
TTL toDateTime(least(intDiv(unix_milli, 1000) + ({{retention_days}} * 86400), 4294967295))
SETTINGS ttl_only_drop_parts = 1, min_columns_to_activate_adaptive_write_buffer = 1;
```

- **Identity and access shape are byte-identical to `metric_samples`** (§2.1): the tenant and then the series ID lead the key and cluster each series, `unix_milli` orders within it — same PK/ordering key `(org_id, fingerprint, unix_milli)`, same daily partitioning, same `ttl_only_drop_parts` retention. Per-series reads are the same sequential granule scans; the codecs on `fingerprint`/`unix_milli` match §2.1 exactly. Timestamps are stored **verbatim at millisecond precision** (§2.1's resolution-agnostic rule).
- **TTL and admitted timestamp domain (issue #137).** The TTL is the one `schema/schema.sql` creates the table with, the same saturating `toDateTime(least(intDiv(unix_milli, 1000) + retention_days * 86400, 4294967295))` as §2.1's, with §2.1's expiry formula and saturation semantics. Admission on every hist ingest path is gated by §2.1's domain (days `0..=49_709`).
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
    org_id               LowCardinality(String),              -- the push's tenant (issue #653)
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
ORDER BY (org_id, kind, service, fingerprint, timestamp_ns)
SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, min_columns_to_activate_adaptive_write_buffer = 1;
-- `schema/schema.sql` creates it with these as well:
--   TTL toDateTime(least(intDiv(received_ms, 1000)
--     + ({{log_landing_retention_hours}} * 3600), 4294967295))
--   SETTINGS
--     non_replicated_deduplication_window = {{log_dedup_window}}   -- replicated_… on a cluster
--   SETTINGS
--     replicated_deduplication_window_seconds = 3600   -- cluster only
```

**The five views.** Three kinds, five views: `log_streams_idx_mv` is an `ARRAY JOIN` over the very same kind-1 row `log_streams_mv` takes, rather than a kind of its own, because a kind of its own would make the writer land the `labels` blob twice per stream. It is correct whether the server applies its `WHERE` before or after the `ARRAY JOIN` — a kind-0 or kind-2 row carries `labels = ''`, `JSONExtractKeysAndValues('', 'String')` is the empty array, and an `ARRAY JOIN` over an empty array emits no row.

```sql
CREATE MATERIALIZED VIEW log_samples_mv TO log_samples AS
SELECT org_id AS org_id, service AS service, fingerprint AS fingerprint,
       timestamp_ns AS timestamp_ns, severity AS severity, body AS body,
       structured_metadata AS structured_metadata
FROM log_landing WHERE kind = 0;

CREATE MATERIALIZED VIEW log_streams_mv TO log_streams AS
SELECT org_id AS org_id, month AS month, fingerprint AS fingerprint, service AS service,
       labels AS labels, updated_ns AS updated_ns
FROM log_landing WHERE kind = 1;

CREATE MATERIALIZED VIEW log_streams_idx_mv TO log_streams_idx AS
SELECT org_id AS org_id, month, kv.1 AS key, kv.2 AS val, fingerprint
FROM log_landing
ARRAY JOIN JSONExtractKeysAndValues(labels, 'String') AS kv
WHERE kind = 1;

-- {res_ns} is PULSUS_LOG_ROLLUP_RESOLUTION; the view and its target are
-- named for it (log_metrics_5s_mv by default).
CREATE MATERIALIZED VIEW log_metrics_5s_mv TO log_metrics_5s AS
SELECT org_id AS org_id, fingerprint AS fingerprint,
       intDiv(timestamp_ns, {res_ns}) * {res_ns} AS bucket_ns,
       count() AS count, countIf(timestamp_ns = bucket_ns) AS count_at,
       sum(length(body)) AS bytes, sumIf(length(body), timestamp_ns = bucket_ns) AS bytes_at
FROM log_landing WHERE kind = 0
GROUP BY org_id, fingerprint, bucket_ns;

-- Pattern rows are pre-aggregated in Rust before they land, so this view
-- copies one landed row per (fingerprint, bucket, template) through and
-- aliases `pattern_count` back to the target's `count`. `timestamp_ns`
-- carries the bucket floor on a kind-2 row.
CREATE MATERIALIZED VIEW log_patterns_mv TO log_patterns AS
SELECT org_id AS org_id, fingerprint AS fingerprint, timestamp_ns AS bucket_ns,
       pattern AS pattern, pattern_count AS count
FROM log_landing WHERE kind = 2;
```

**Neither the rollup view nor the index view is second-level any more.** Both used to read a target table (`log_samples`, `log_streams`); both now read `log_landing`, so exactly one view fires per landed row per target. For `log_metrics_5s` that is load-bearing rather than tidy: its `count`/`bytes` are `SimpleAggregateFunction(sum, UInt64)`, and a second-level view firing as well as a first-level one would double every count.

```sql
CREATE TABLE log_streams (
    org_id       LowCardinality(String),
    month        Date,                          -- toStartOfMonth(first write in month)
    fingerprint  UInt128,
    service      LowCardinality(String),        -- resource service.name ('' if absent)
    labels       String  CODEC(ZSTD(5)),        -- canonical JSON, sorted keys
    updated_ns   Int64
) ENGINE = ReplacingMergeTree(updated_ns)
PARTITION BY month
ORDER BY (org_id, fingerprint)
TTL toDateTime(least((toUInt32(addMonths(month, 1)) * 86400) + ({{retention_days}} * 86400), 4294967295))   -- the month's end plus retention
SETTINGS ttl_only_drop_parts = 1;

CREATE TABLE log_streams_idx (
    org_id       LowCardinality(String),
    month        Date,
    key          LowCardinality(String),
    val          String,
    fingerprint  UInt128
) ENGINE = ReplacingMergeTree
PARTITION BY month
ORDER BY (org_id, key, val, fingerprint)
TTL toDateTime(least((toUInt32(addMonths(month, 1)) * 86400) + ({{retention_days}} * 86400), 4294967295))   -- the month's end plus retention
SETTINGS ttl_only_drop_parts = 1;
-- populated by log_streams_idx_mv over log_landing's kind-1 rows:
--   ARRAY JOIN JSONExtractKeysAndValues(labels, 'String')
```

```sql
CREATE TABLE log_samples (
    org_id        LowCardinality(String),
    service       LowCardinality(String),
    fingerprint   UInt128,
    timestamp_ns  Int64   CODEC(DoubleDelta, ZSTD(1)),
    severity      Int8    DEFAULT 0,             -- OTel SeverityNumber (0 = unset)
    body          String  CODEC(ZSTD(1)),
    structured_metadata String DEFAULT '',        -- per-entry Loki structured metadata (issue #97); added by additive ALTER, see note below
    INDEX idx_body_ngrams body TYPE text(tokenizer = ngrams(4)),
    INDEX idx_severity severity TYPE minmax GRANULARITY 4
) ENGINE = MergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(timestamp_ns), 'UTC')
ORDER BY (org_id, service, fingerprint, timestamp_ns)
TTL toDateTime(least(intDiv(timestamp_ns, 1000000000) + ({{retention_days}} * 86400), 4294967295))
SETTINGS ttl_only_drop_parts = 1, min_columns_to_activate_adaptive_write_buffer = 1;
```

```sql
-- PULSUS_LOG_ROLLUP_RESOLUTION (default 5s) sets the bucket size and names the table;
-- the MV bucket expression is rendered from it. count_at/bytes_at: lines exactly on bucket_ns.
CREATE TABLE log_metrics_5s (
    org_id LowCardinality(String), fingerprint UInt128,
    bucket_ns    Int64,                          -- intDiv(timestamp_ns, {res_ns}) * {res_ns}
    count    SimpleAggregateFunction(sum, UInt64), bytes    SimpleAggregateFunction(sum, UInt64),
    count_at SimpleAggregateFunction(sum, UInt64), bytes_at SimpleAggregateFunction(sum, UInt64),
    PROJECTION by_time (SELECT org_id, bucket_ns, fingerprint ORDER BY (org_id, bucket_ns, fingerprint))
) ENGINE = AggregatingMergeTree PARTITION BY toDate(fromUnixTimestamp64Nano(bucket_ns), 'UTC')
ORDER BY (org_id, fingerprint, bucket_ns)
TTL toDateTime(least(intDiv(bucket_ns, 1000000000) + ({{retention_days}} * 86400), 4294967295))   -- log_samples' retention
SETTINGS ttl_only_drop_parts = 1, deduplicate_merge_projection_mode = 'rebuild', lightweight_mutation_projection_mode = 'rebuild';
-- populated by log_metrics_5s_mv over log_landing's kind-0 rows; by_time serves the discovery read
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
    org_id       LowCardinality(String),
    fingerprint  UInt128,
    bucket_ns    Int64,                          -- intDiv(timestamp_ns, 10s) * 10s
    pattern      String  CODEC(ZSTD(1)),
    count        SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(bucket_ns), 'UTC')
ORDER BY (org_id, fingerprint, bucket_ns, pattern)
TTL toDateTime(least(intDiv(bucket_ns, 1000000000) + ({{retention_days}} * 86400), 4294967295))
SETTINGS ttl_only_drop_parts = 1;
```

Raw log timestamps in `log_samples` are stored verbatim at nanosecond precision; the rollup is derived. A bucket is `[b, b + res)` and a window is `(t − range, t]`, so the rollup answers a window exactly when `t` and `range` are multiples of `res`: the window is the buckets `[t − range, t)`, less the lines exactly on `t − range` (`count_at`), plus those exactly on `t`. The rollup statement reads each row as two points — its `count_at` at `b`, the rest at `b + res` — and runs the sliding statement's grid arithmetic over them (`sql.rs`'s `metric_range_rollup`). Which queries read it, and the one documented limit on that equality (a non-empty `__error__` in a line's structured metadata), is [architecture.md §3.2](architecture.md)'s (issue #614).

- **`log_patterns` primary-key order is `(org_id, fingerprint, bucket_ns, pattern)`** (issue #171, `bucket_ns` **before** `pattern`; the tenant leads, issue #653): a `/api/logs/v1/patterns` read is a `fingerprint IN (...)` + bounded `bucket_ns` window, so putting `bucket_ns` second prunes at the PK level inside each fingerprint's key range, not only via daily partitions. The template is a **deterministic, stateless** token-class rendering of the line body (digit/length classification, `key=value`/`key:value` awareness, 1 KiB prefix / 64-token / 512-byte caps, whitespace runs collapse) — NOT a drain-style online clusterer, whose order-dependent per-stream mutable state would emit different templates on different shards/replicas/retries and break both the mergeable `sum` and idempotent re-inserts. Templates are normalized (whitespace-collapsed), documented as "not round-trip matchable". **Count semantics** are exact on the clean ingest path and **best-effort approximate under ingest-failure re-sends**, at parity with `log_metrics` (§2.2's tier caveat): the writer never auto-replays a block that could have committed (an `InsertUncertain` batch is spooled audit-only, never re-inserted), so the only over-count vector is a client-level re-send after a 5xx/timeout ack (steady-state zero), and **there is no under-count vector left from a failing flush** (issue #603): the pattern rows ride the same landed block as the lines they were extracted from, so there is no separate patterns insert to fail, and a push whose block does not land lands no lines either. What remains is the fan-out residual every target of `log_landing` shares — a view that throws while the server processes the insert leaves it unrecorded which targets kept the block's rows (§4.1, "What a failing view leaves behind"). A single request that emits more than 10 000 distinct templates (pathological — the extraction caps make templates low-cardinality by construction) drops the excess from pattern accounting only (log lines untouched, counted via the writer's `patterns_dropped_total`), an under-count event folded into the same approximate semantics; the next batch resumes discovery.

- **`service` leads the samples ordering key.** This is the one label promoted to a physical column (populated from resource `service.name`; user-visible as the `service_name` label per the canonical label model), and it earns it three ways: (a) OpenTelemetry guarantees `service.name` on every resource (the collector defaults it to `unknown_service`), so it is never missing; (b) it is the natural clustering dimension — a service's streams sit contiguously, so service-scoped searches (the human default) read a compact range instead of granules scattered across all tenants of the table; (c) **the planner can always supply it**: stream resolution returns full label sets, so even a query that never mentions `service` gets `service IN (...)` injected from the resolved streams, keeping the primary index engaged. No other label is materialized — finding #9's counter-argument (row width, schema coupling) applies to everything else.
- **`(org_id, service, fingerprint, timestamp_ns)` is fixed** (finding #4; the tenant leads it, issue #653). Per-stream time reads are sequential; multi-stream reads within one service are near-sequential.
- **Body `text` index** (finding #3): one `text(tokenizer = ngrams(4))` index serves substrings and anchored regex literals; it replaced the deprecated `tokenbf_v1`/`ngrambf_v1` pair in #624 part 3b (#235). The planner mints **one** predicate per line filter — `body LIKE '%needle%'` for `|=`/`!=`, `match(body, …)` for `|~`/`!~` — and lets ClickHouse's own index analysis decide which 4-grams it tests. Every LogQL read sends `query_plan_direct_read_from_text_index = 0` (`read_query_settings`): with the direct read on, a statement whose `WHERE` holds a line filter leaves a query-condition-cache entry under its `PREWHERE service = …` condition that makes the next statement with that condition count too few lines (measured on 26.3.29.7); turning it off corrects that and leaves the condition cache and the index analysis on. It mints **no `hasToken` prefilter of its own**: `hasToken()` is an exact whole-token membership test, so ANDing one onto a substring search dropped matching lines, kept excluded ones and failed outright on a needle containing `_` (issue #450). Measured on 26.3.17.110 with the bloom-filter pair, over 10M rows / 1.28 GiB of bodies, needle `wxyze73b62205bc78c841234`: `body LIKE` reads **0.97M rows**, `position(body, …) > 0` reads all **10.0M** (`ngrambf_v1` serves `LIKE` and does not serve `position`), and the old wrong-but-fast `hasToken` conjunction read 8,192. Residual: a needle shorter than the 4-gram length engages no index and reads the full selector/time-bounded range (still PK-bounded, never the table) — scale behaviour of short needles is issue #25's.
- **Monthly stream/index partitions** (finding #6): one row per stream per month; a 30-day label query touches ≤ 2 partitions.
- **`structured_metadata` is per-entry, not per-stream** (issue #97): Loki push carries optional per-entry structured metadata (protobuf `EntryAdapter.structuredMetadata` or a JSON `values` third element), stored here as a canonical sorted-key JSON String — the `log_streams.labels` representation, not `Map(String,String)` (§1 rejects Map for label-shaped data). Empty string = none (pre-#97 rows, and OTLP-logs rows without an instrumentation scope). As of #109, an OTLP-logs row's `InstrumentationScope` is stored here rather than as stream labels, matching grafana/loki 3.4.2 (which places scope identity in structured metadata, not indexed labels): the scope name/version under keys `scope_name`/`scope_version` (each emitted only when non-empty), and each other scope attribute under its stored name (`log_label_name`, the reference's label namer, issue #507: a run of characters outside `[A-Za-z0-9]` becomes one `_`, a name of four or more bytes that starts and ends with `__` keeps those affixes, and a leading digit gains `key_` — e.g. `scope.attr.foo` → `scope_attr_foo`, `a..b` → `a_b`, `9s` → `key_9s`, `team` unchanged), with no `scope_` prefix. The whole ordered list — every attribute, then `scope_name`/`scope_version` — is then resolved by `pulsus_model::resolve_structured_metadata`, the one seam every structured-metadata producer funnels through (issues #259, #381). That function **is** Prometheus' `labels.Builder` as Loki's distributor drives it (`pkg/distributor/distributor.go:697-722` @ v3.7.4), and its rule is two-tier rather than positional: *a pair that was `Set` — renamed, or carrying `utf8.RuneError` — beats a pair that was not, wherever either sits in wire order; among pairs `Set` onto one name the last wins; among pairs that were never `Set` the reference keeps them all as duplicate labels*, which this key-unique column collapses keeping the last (what a JSON consumer observes of the reference's duplicate-keyed object). It is **not** last-write-wins in wire order — that cannot explain why both orders of `{a.b="x", a_b="keep"}` store `a_b="x"`. An **empty attribute value is a `Del`, not a kept pair**, and the interaction between the two is the rule copied verbatim from `pulsus_model::resolve_structured_metadata`'s primitive 7 (`tests/copied_rule.rs` asserts the copy): <!-- copied-rule:del-vs-set:start -->**`del` drops BASE entries only, so a `Set` outranks it.** An empty value deletes every pair stored under its name that the builder did not `Set`, and a rename or a U+FFFD rewrite re-adds the name in either wire order, because `add` is emitted whether or not `del` holds that name.<!-- copied-rule:del-vs-set:end --> Identity fields are inside that rule, not beside it: an attribute `scope_name=""` takes a plain `scope_name` with it, and does **not** take one carrying U+FFFD — measured, `{scope_name="", scope_name="N\ufffd"}` stores `scope_name="N "` while the control `{scope_name="", scope_name="N"}` stores nothing. A value containing U+FFFD has every occurrence rewritten to a SPACE, and that rewrite is itself the `Set`. Name and version are additionally suppressed at their own append site when empty (#108). On THIS path the keys reaching the builder are already `log_label_name` fixed points (`log_label_name` is idempotent and both identity names are fixed points), so the rename branch cannot fire and `add` carries the U+FFFD rewrites and nothing else. **When no value carries U+FFFD, `add` is empty** and the rule degenerates to the by-name delete plus keep-last — which is why appending identity last makes it win a non-empty collision with an attribute. When one does, that pair is `Set` and the two-tier rule above is what decides. That is the reference's own shape at the same place: its OTLP translation runs `LabelNamer.Build` over every attribute key before the distributor sees it (`pkg/loghttp/push/otlp.go:602-614` @ v3.7.4), whereas the push transport hands the builder raw names. docs/api.md §8.2 rows 5-6 carry the measured cases, and its residuals are in `docs/benchmarks/logs-differential-ledger.md` under `structured-metadata-collision-resolution`. It is added by **additive ALTER** (migration ids 21/22) rather than by mutating the frozen initial `CREATE`, so upgraded and fresh deployments converge byte-identically; a fresh DB runs `CREATE` (no column) then `ADD COLUMN IF NOT EXISTS`. Structured metadata never enters `stream_fingerprint` (a stream pushed with vs. without it fingerprints identically); on the read path it fans into the response stream label set alongside the base labels (grafana/loki 3.4.2 default, `categorize_labels` off), so an entry carrying distinct metadata forms its own result stream and a `| key="value"` pipeline filter selects on it.
- **Admitted log timestamp domain and TTL (issue #137, mirroring #131).** Ingest admits a log record only if its UTC day lies in `[1970-01-01, 2106-02-06]` (days `0..=49_709`, `pulsus_model::Date::start_of_day_utc_datetime_safe`); a record outside that domain is rejected (OTLP logs: per-record partial success; Loki push: whole-request 400 — Loki is all-or-nothing). The same two wrap mechanisms as §2.1 motivate the gate: the 16-bit `Date` partition key wraps for days past 2149-06-06, and the delete-TTL evaluates the row timestamp in the 32-bit `DateTime` domain and wraps for instants past 2106-02-07T06:28:15Z (u32-seconds maximum, `4294967295`); day `49_710` (2106-02-07) is excluded because only part of it is u32-representable. The TTL is the one `schema/schema.sql` creates `log_samples` with, `toDateTime(least(intDiv(timestamp_ns, 1000000000) + retention_days * 86400, 4294967295))`, so for a stored row with epoch-seconds `s = floor(timestamp_ns / 1e9)` the operative expiry is `expiry(s) = min(s + retention_days * 86400, 4294967295)`, with exactly §2.1's saturation semantics: a clamped row's shortfall vs the configured value is `s + retention_days * 86400 - 4294967295` (growing with `retention_days`); for the enforced `retention_days >= 1` (`schema/schema.sh` refuses `< 1`) a row at the last admitted day has actual retention capped at `23_296 s ≈ 0.27 days (~6.5 hours)`; for every enforced `retention_days >= 1` the saturating form strictly dominates the pre-#137 wrapping expression; and the admission cutoff is deliberately not coupled to `retention_days` (§2.1's rationale).

### 3.2 Read paths (generated SQL)

**`{service_name="checkout", env="prod"} |= "connection refused"`, last 6h, limit 100.**

Stage 1 — stream resolution, a *single pass* over the index (finding #1): each `(key, val)` pair is a primary-prefix range read; the intersection happens inside the scan:

```sql
SELECT fingerprint
FROM log_streams_idx
WHERE org_id = '' AND month = '2026-07-01'
  AND ((key = 'service_name' AND val = 'checkout') OR (key = 'env' AND val = 'prod'))
GROUP BY fingerprint
HAVING uniqExact(key, val) = 2
```

Regex/negative matchers (finding #7) resolve within one key's index prefix, e.g. `env=~"prod|staging"` becomes `key = 'env' AND match(val, ...)` — a scan over the distinct *values of that key*, never over samples. The planner orders matchers by selectivity (cheap `count()` probes on index prefixes) and aborts with "query too broad" past `PULSUS_LOGQL_SCAN_BUDGET_BYTES` or past the per-query memory ceiling `PULSUS_LOGQL_READ_MAX_MEMORY_BYTES` (issue #398: `max_memory_usage` + `max_bytes_before_external_group_by=0`, throw-not-spill, on every LogQL read — a memory breach is the same `422` as a byte-budget breach, never a `500`; a lowered metric statement is the one read whose own memory breach is not returned: that query runs again on the raw-scan read under the same ceiling; a timeout of a lowered statement is returned as a timeout, as for any read).

Stage 2 — hydration (needed for response labels anyway): `SELECT fingerprint, service, labels FROM log_streams WHERE org_id = '' AND fingerprint IN (...)` → also yields the `service` set for stage 3.

Stage 3 — samples, primary-index + skip-index served:

```sql
SELECT fingerprint, timestamp_ns, body
FROM log_samples
PREWHERE service = 'checkout'
WHERE org_id = '' AND fingerprint IN (18374..., 99120...)
  AND timestamp_ns >  {now - 6h} AND timestamp_ns <= {now}
  AND body LIKE '%connection refused%'   -- exact, and prunable by the body's text index
ORDER BY timestamp_ns DESC
LIMIT 100
```

**`sum by (service_name) (rate({env="prod"}[5m]))`** at a 15 s step — counted in the database, one row per series and grid point rather than one per line. `count_over_time` and `rate` read no `body`; `bytes_over_time` and `bytes_rate` read `length(body)` in place of the first `count()`. Each line's `lo` and `hi` are the first and last grid points whose window `(g - 5m, g]` holds it; the deltas at `lo` and `hi + 1`, summed cumulatively, are each window's total:

```sql
SELECT fingerprint, toInt64({start} + k * 15000000000) AS bucket_ns, toUInt64(v) AS n, structured_metadata
FROM (
  SELECT fingerprint, structured_metadata, k0,
         sum(dv) OVER cum AS v, sum(dc) OVER cum AS p, leadInFrame(k0, 1, {kend}) OVER whole AS k1
  FROM (
    SELECT fingerprint, structured_metadata, d.1 AS k0, sum(d.2) AS dv, sum(d.3) AS dc
    FROM (
      SELECT fingerprint, structured_metadata,
             greatest(intDiv(timestamp_ns - {start - 300s} + 14999999999, 15000000000) - 20, 0) AS lo,
             least(intDiv(timestamp_ns - {start - 600s} + 14999999999, 15000000000) - 21, {kmax}) AS hi,
             count() AS m, count() AS c
      FROM log_samples
      PREWHERE service IN (...)
      WHERE org_id = '' AND fingerprint IN (...)
        AND timestamp_ns > {start - 300s} AND timestamp_ns <= {end}
      GROUP BY fingerprint, structured_metadata, lo, hi
      HAVING lo <= hi
    )
    ARRAY JOIN [(lo, toInt64(m), toInt64(c)), (hi + 1, -toInt64(m), -toInt64(c))] AS d
    GROUP BY fingerprint, structured_metadata, k0
  )
  WINDOW cum AS (PARTITION BY fingerprint, structured_metadata ORDER BY k0 ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
         whole AS (PARTITION BY fingerprint, structured_metadata ORDER BY k0 ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
)
ARRAY JOIN range(k0, least(k1, {kend})) AS k
WHERE p > 0
```

`{kmax}` is the last grid index, `({end} - {start}) / 15s`, and `{kend}` is one more.

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
- **`absent_over_time` reads the column too.** Its label set is
  metadata-independent (`syntax/extractor.go:46-47` forces
  `noLabels = true`, and `labels.go:667-668` then returns
  `EmptyLabelsResult`), but every raw scan decodes into the one row type,
  which carries the column. It once kept a lean projection, and the read
  then failed to decode wherever the selector resolved a stream.
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
    WHERE org_id = '' AND fingerprint IN (...) AND bucket_ns >= {start} AND bucket_ns < {end}
    GROUP BY pattern, ts_ns
)
GROUP BY pattern
ORDER BY total DESC, pattern ASC
LIMIT 1000
```

`org_id` and `fingerprint IN` engage the `(org_id, fingerprint, bucket_ns, pattern)` primary-key prefix (granule pruning), daily partitions prune the window, and the aggregation + top-K + LIMIT all execute in ClickHouse — the client decodes ≤ 1000 already-assembled series. `step` is floored to the 10s ingest bucket; the `(end-start)/step` grid is capped at 11,000 (else 400), and the response is the Loki-interop envelope (docs/api.md §2.6).

**Live tail** polls stage 3's shape with a monotonic `timestamp_ns >` cursor; line-filter pushdown identical.

**Where LogQL SQL fragments come from, and what that guarantees (issue #286).** Every ClickHouse `match(...)` a LogQL read can render is written in ONE leaf module, `crates/pulsus-read/src/logql/predicate.rs`, and the builders above take its output as a type rather than as text: predicate fragments arrive as `CheckedFragment`, string literals as `CheckedLiteral`, month partition literals as `MonthLiteral`, and a metric read's bucket/aggregate columns as a four-inhabitant `MetricShape`. Each newtype's field carries no visibility modifier and the module is a flat leaf, so no other module can build one — a hand-written `format!("match(body, {})", ch_string(pat))` is still a perfectly legal `String`, but it no longer compiles at a builder. The guarantee bought is #240's: an uncompilable user regex is a `400` at plan time, never a ClickHouse `500` mid-query.

The limits are as important as the claim, and are recorded verbatim in that module's doc: the property is closed at the type boundary and open at the one unwrap point per type (`as_sql()`); the file itself is the trust base, so a mint added *inside* it is a failing census (`tests/logqltest_provenance.rs` check H), not a compile error; a sibling macro *invoked* in the leaf expands inside it and reaches the private field, which is a disclosed residual with a committed fixture; `unsafe` is outside the boundary entirely; and the six **table-name** parameters deliberately stay `&str`, with no enforced property, because the only candidate mechanisms were a wire change (backtick-quoting via `ch_ident`) or sealing `PlanCtx`, and a `Checked`-shaped name on an unchecked value is worse than an honest `&str`. `tests/logqltest_provenance.rs` check G inventories every `match(` spelling across `crates/*/src` — an inventory and drift detector, explicitly **not** a gate.

**Query-text admission (issue #35).** Every read-path query — LogQL, PromQL/metrics, and TraceQL — carries `max_query_size = 8 MiB` as a per-request session setting: ClickHouse's own SQL-text parse-buffer cap defaults to 262,144 bytes, well under the literal `fingerprint IN (...)` list a stage2/stage3 read renders at the documented 100k-stream cap (~2.2 MiB). Because `services`/line-filter text and metrics fan-out width are not bounded by any single constant, a rendered-SQL admission guard rejects any query text at or past the 8 MiB cap *before* dispatch as a clean `422 query_too_broad`, rather than letting an oversized request fail with an opaque ClickHouse parse error — the guaranteed-admitted envelope (100k worst-case fingerprints + a generous services/line-filter margin) fits comfortably inside it. Residual: the guaranteed-admitted envelope arithmetic assumes the shipped caps — should the stream cap or metrics cache/fanout caps ever become operator-configurable, the envelope must be re-derived against `MAX_QUERY_TEXT_BYTES`; scale considerations route to #25.

---

## 4. Traces

**Query shapes served:** trace-by-ID fetch (latency-critical); TraceQL search = attributes + intrinsics + time (human-facing); tag discovery; TraceQL metrics aggregations.

### 4.1 Tables

Issues #584 to #586, #599 and #601. `schema/schema.sql` is the definition; this section lists what it holds for
traces and cites the statement each fact is read from. `{{retention_days}}` is `PULSUS_RETENTION_DAYS` and
`{{trace_landing_retention_hours}}` is `PULSUS_TRACE_LANDING_RETENTION_HOURS` (`docs/configuration.md`).

**Tables.** Engines are the single-node form; clustered, each is the `Replicated` engine of the same family, with
the replica path in the last column.

| table | engine | sorting key | partition | TTL on | replica path | statement |
|---|---|---|---|---|---|---|
| `trace_landing` | `MergeTree` | `(org_id, row_kind, trace_id, start_ns, span_id, kind, tag_key, tag_value)` | hour of `received_ms` | `received_ms` + `{{trace_landing_retention_hours}}` h | per shard | `schema/schema.sql:430-490` |
| `spans` | `ReplacingMergeTree` | `(org_id, intDiv(start_ns, 300000000000), trace_id, start_ns, span_id, kind)` | UTC day of `start_ns` | `start_ns` + `{{retention_days}}` d | per shard | `schema/schema.sql:345-398` |
| `error_spans` | `ReplacingMergeTree` | `(org_id, intDiv(start_ns, 300000000000), trace_id, start_ns, span_id, kind)` | UTC day of `start_ns` | `start_ns` + `{{retention_days}}` d | per shard | `schema/schema.sql:891-913` |
| `traces` | `AggregatingMergeTree` | `(org_id, trace_id)` | `day` | end of the UTC day of `last_start_ns` + `{{retention_days}}` d | per shard | `schema/schema.sql:492-510` |
| `resources` | `ReplacingMergeTree` | `(org_id, service, resource_id)` | `day` | end of `day` + `{{retention_days}}` d | cluster-wide | `schema/schema.sql:325-343` |
| `tag_names` | `ReplacingMergeTree(last_start_ns)` | `(org_id, scope, key)` | none | end of the UTC day of `last_start_ns` + `{{retention_days}}` d | cluster-wide | `schema/schema.sql:400-412` |
| `tag_values` | `ReplacingMergeTree(last_start_ns)` | `(org_id, scope, key, value, val_type)` | none | end of the UTC day of `last_start_ns` + `{{retention_days}}` d | cluster-wide | `schema/schema.sql:414-428` |
| `tag_bucket_values` | `AggregatingMergeTree` | `(org_id, service, scope, key, bucket)` | UTC day of `bucket` | `bucket` + `{{retention_days}}` d | cluster-wide | `schema/schema.sql:874-889` |
| `service_bucket_resources` | `ReplacingMergeTree` | `(org_id, service, bucket, resource_id)` | UTC day of `bucket` | `bucket` + `{{retention_days}}` d | cluster-wide | `schema/schema.sql:944-957` |
| `service_edges` | `ReplacingMergeTree(ver)` | `(org_id, conn_type, minute, client_minute, client, server)` | `toDate(minute)` | `minute` + `{{retention_days}}` d, less 240 s where `conn_type = ''` | per shard | `schema/schema.sql:777-795` |

- Every TTL is the saturating form `toDateTime(least(… , 4294967295))`. Every table but `tag_names` and
  `tag_values` sets `ttl_only_drop_parts = 1`; those two carry `last_start_ns`, the latest start of a span carrying
  the row, which their views copy from `start_ns` and a merge keeps as the replacing version, and lose a row as it
  falls due.
- Every table but `service_edges` carries a deduplication window of `{{trace_dedup_window}}` blocks
  (`non_replicated_deduplication_window` single-node, `replicated_deduplication_window` clustered).
- `spans` carries three bloom-filter indexes (`idx_service`, `idx_name`, `idx_resource`) and the
  `{{trace_attr_indexes}}` token, which renders one index per attribute `PULSUS_TRACEQL_INDEXED_ATTRIBUTES` names
  (`schema/schema.sql:407-409`; `docs/configuration.md` §3).
- `trace_landing` sets `async_insert = 0` (`schema/schema.sql:509`).
- Clustered, `spans`, `traces` and `error_spans` have a `_dist` wrapper sharded on `cityHash64(trace_id)`
  (`schema/schema.sql:560-562`, `:564-566`, `:933-935`); no other trace table has one (§7).

- Every table's first column is `org_id`, the tenant (issue #738), and the first column of its sorting key; the
  `_dist` wrappers are `AS` their table and carry it.

**Views.** Every insert-triggered view reads `trace_landing` and copies `org_id` from it; the refreshable one reads
`spans` and its own target, pairs a client and a server span only within one `org_id` and writes each edge under
it, and reads and writes its markers under the empty tenant (`org_id = ''`), one set per run.

| view | target | reads | filter | statement |
|---|---|---|---|---|
| `spans_mv` | `spans` (`_dist` clustered) | `trace_landing` | `row_kind = 0` | `schema/schema.sql:723-755` |
| `error_spans_mv` | `error_spans` (`_dist` clustered) | `trace_landing` | `row_kind = 0 AND status_code = 2` | `schema/schema.sql:942-946` |
| `traces_mv` | `traces` (`_dist` clustered) | `trace_landing` | `row_kind = 0`, grouped by `org_id`, `trace_id` | `schema/schema.sql:847-872` |
| `tag_bucket_values_mv` | `tag_bucket_values` | `trace_landing` | `row_kind = 0 AND service_type = 'string'`, grouped by tenant, bucket, service, scope, key | `schema/schema.sql:948-965` |
| `service_bucket_resources_mv` | `service_bucket_resources` | `trace_landing` | `row_kind = 0 AND service_type = 'string'`, grouped by tenant, bucket, service, resource | `schema/schema.sql:959-963` |
| `resources_mv` | `resources` | `trace_landing` | `row_kind = 1` | `schema/schema.sql:724-736` |
| `tag_names_mv` | `tag_names` | `trace_landing` | `row_kind = 2` | `schema/schema.sql:772-779` |
| `tag_values_mv` | `tag_values` | `trace_landing` | `row_kind = 3` | `schema/schema.sql:766-775` |
| `service_edges_mv` | `service_edges`, `REFRESH EVERY 1 MINUTE APPEND` | `spans`, `service_edges` | the hold rule, `docs/api.md` §4.5 | `schema/schema.sql:797-845` |

So `row_kind` 0 is a span, 1 a resource, 2 a tag name and 3 a tag value: the four filters above are the only
readers of each kind.

**What a failing view leaves behind.** A view that throws while the server processes a landing insert fails it. The insert fails: the caller receives `HTTP 500` with `Code: 395`, and is not told which targets kept the block's rows. The design record measured that outcome over 300 trials of one throwing view and three healthy sibling views over one source table (`docs/traceql-schema-migration.md` §6.3): the throwing view's own target held nothing in 300 of 300; the source rows were present in 297 of 300 and absent in 3; the three healthy siblings committed 28, 25 and 32 times out of 300, and all three together 7 times against about 0.25 if they were independent. Nothing else held on every trial. A retry of the same block re-runs every view, and each target's deduplication window or collapse rule absorbs the rows written twice. The same holds for `log_landing` and `metric_landing`.

### 4.2 Read paths (generated SQL)

**Trace by ID** — the fetch reads three of §4.1's tables (`spans`, `traces`, `resources`). One statement answers an indexed trace and a second answers two cases, each named with its count in `docs/TraceQL/server-implementation.md` §3.5:

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

The full text of all three is `docs/TraceQL/sql-schema.md` §5.4, which is where they are byte-frozen.

`kind` is projected (issue #75) as the fetch assembler's `(span_id, kind)` de-duplication key **and** as the rendered value: since issue #587 the response renders `kind` **from the column**, as the protocol's own signed `Int32`, because there is no decoded payload to take it from. Its reason for existing is unchanged — it keeps a Zipkin shared span's SERVER and CLIENT sides (identical `(trace_id, span_id)`, different `kind`) as two distinct spans on retrieval, while remaining a genuine no-op for OTLP (span ids are unique per trace) and still de-duplicating identical at-least-once replays.

**Search, metrics, tag discovery and the service graph** each run statements compiled from the query over §4.1's tables; `docs/TraceQL/server-implementation.md` describes them and `docs/query-lowering.md` records how each is lowered.

**The tenant (issue #738).** Every statement above reads each of §4.1's tables with `org_id = <tenant>` as the first conjunct of the `WHERE` after its `FROM`, the request's `X-Scope-OrgID` (`org_id = ''` with no header), and the trace read path refuses to send one that does not. It inspects every table source a statement has — after `FROM`, every `JOIN` form, each comma-joined source and a bare `IN` set — refuses a trace table read as a join target or a set rather than through a subquery carrying the term, refuses a table function it cannot see into (`merge`, or `cluster`/`remote` without a `view(…)` query), and refuses any shape it does not recognise: a comment, a double-quoted name, or a parenthesised source that is not a query. The service graph's marker reads take `org_id = '' AND conn_type = ''` instead, the job's markers being one set per run.

**Ordering and partiality contracts** (public, docs/api.md §4.2): `traces[]` is ordered by the max timestamp of each trace's exactly-matched spans, descending, `trace_id` ascending as the tiebreak. A search is answered whole or refused, so every search answers `{"completedJobs":1,"totalJobs":1}`. Budget breaches are hard `422`s, never partial results.

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
TTL toDateTime(least(intDiv(timestamp_ns, 1000000000) + ({{retention_days}} * 86400), 4294967295))
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

The script reads the same environment variables the binary does ([configuration.md §3](configuration.md)), gates on the server version, **drops the configured database**, creates it, and sends one statement per request — the HTTP interface refuses a multi-statement body. 52 statements single-node, 67 clustered.

**There are no migrations.** No numbered list, no bookkeeping table, no checksum drift guard. A schema change is an edit to `schema.sql` and another run of the script; the cost is the data in the database, and the development workflow is to drop and recreate. Nothing is upgraded in place, including retention: `PULSUS_RETENTION_DAYS`, `PULSUS_ROLLUP_RETENTION_DAYS` and the three landing hours are each declared in the `CREATE` of the tables whose TTL they set ([configuration.md §7](configuration.md)), so changing one is a rebuild.

**One file, two variants.** A line prefixed `--@single` is used on a single node, one prefixed `--@cluster` on a cluster, and every other line by both. A statement that exists in one variant only carries the prefix on all of its lines. The clustered side is where the `Replicated*` engines, `ON CLUSTER`, the `_dist` wrappers and the replication paths live.

**Tokens are double-brace.** `{{db}}`, `{{on_cluster}}`, `{{retention_days}}` and the rest are substituted by the script and, for the two things read at run time, by `pulsus-schema`'s own renderer. ClickHouse's `{shard}` and `{replica}` are the **server's** macros and must reach it as those exact characters: a substitution matching a single brace pair eats them, and the server accepts the result without complaint — every shard then joins one replica set. Nothing in the file may be a single-brace token, and `crates/pulsus-schema/tests/schema_file.rs` holds that, along with the rule that every replication path names the table of its own `CREATE`.

**Two things are read out of the file at run time**, neither of them DDL: a materialized view's own projection, which `rebuild-metrics`, `rebuild-logs` and `rebuild-traces` replay a landing window through, and a table's declared column list, from which the trace fetch derives its projection.

---

## 7. Distributed layout

Enabled by `PULSUS_CLUSTER`. Every table becomes `ReplicatedMergeTree`-family, and every **per-shard** table but the three landing tables, `metric_label_values` and `service_edges` gets a Distributed wrapper. Eleven tables get none. Three are the landing tables — `log_landing`, `metric_landing` and `trace_landing` (issues #603 and #586; the reason, and what it costs, are below, and in [ingest-one-source-table.md](ingest-one-source-table.md)). `metric_label_values` is per-shard and gets none because every statement names it locally, inside a read dispatched with `distributed_product_mode = 'local'`, beside the same shard's label index rows (issue #635). `service_edges` is per-shard and has none because the service graph reads it only through `cluster(…, view(…))`, one per-shard statement that also pairs that shard's spans (issue #599). The other six are the cluster-wide ones, one replica set each spanning every shard (`/clickhouse/tables/all/<db>.<table>`), read from the local replica without fan-out, so a wrapper would put a row on one shard and leave every other shard's read missing it permanently: `metric_metadata`, `resources`, `tag_names`, `tag_values`, `tag_bucket_values` and `service_bucket_resources` — the last row of the table below is theirs. **Sharding keys are chosen so that reads join and aggregate shard-locally** (finding #2):

| Table | Sharding key | Why |
|-------|--------------|-----|
| `metric_samples`, `metric_hist_samples`, `metric_series`, the rollup levels `metric_samples_<N>s` | `cityHash64(fingerprint)` | the fingerprint is the series ID, which includes the metric name (issue #623). Placement does not follow the key: a push lands on the shard that took it, so one series' samples can sit on several shards (issue #603), and every metrics read fans out to every shard through the wrappers with sharding-key pruning off (`optimize_skip_unused_shards = 0`, set by `metrics_read_settings` in `crates/pulsus-read/src/metrics/exec.rs`). On each shard one replica, the one with the smallest `replica_num`, builds that shard's slice of the fingerprint range at every level: the first level from every replica's `metric_landing`, read through `clusterAllReplicas` with exact repeats dropped, and each other level from its own shard's level below. If a replica does not answer, the run writes nothing (`skip_unavailable_shards = 0`) and the next run retries it (issue #18 part A). **One read is not reduced shard-locally, and neither is the one it replaces** (issue #549): the grouped instant read's window pipeline is not pushed to shards, so each shard returns its matched rows and the reduction happens at the coordinator — measured on a two-shard fixture, 40 series over 60 steps, the follower returned 960 rows of 960 on BOTH routes, so the change neither worsens nor improves that hop. What it moves is the coordinator's hop to the client, 2,400 rows to 240 on that fixture |
| `metric_labels` | `cityHash64(fingerprint)` | one lookup row per series (issue #623). **Nothing is inserted through the wrapper**: one kind-2 landing row writes a series' activity row and its lookup row on the node that took the push, so every series read that nests the two runs shard-local (`distributed_product_mode = 'local'`) and finds a series' lookup row beside its activity |
| `log_samples`, `log_streams`, `log_streams_idx`, `log_metrics_5s`, `log_patterns` | `cityHash64(fingerprint)` | **this expression governs reads, not writes** (issue #603). The writer inserts into `log_landing` under its bare name and the views write that shard's local targets, so a fingerprint's rows sit wherever its pushes landed and can sit on several shards. The reads tolerate it: the stream-resolution `GROUP BY fingerprint HAVING ...` counts distinct `(key, val)` pairs and `log_streams_idx`'s sorting key covers its whole row, so a cross-shard duplicate changes nothing; stage-2 hydration already keeps one row per fingerprint; the rollup, `/patterns` and volume reads merge partial aggregates at the initiator. What it ends is shard-local **label discovery**, whose semi-join asked whether a fingerprint was active *anywhere* and could only be answered per shard while one fingerprint's index and rollup rows co-resided — see the paragraph below the tables  **The key hashes the column rather than being the column** (issue #498): a `Distributed` sharding key must evaluate to an integer type ClickHouse accepts, and `UInt128` is not one. Measured on ClickHouse 26.3.29.7, a two-shard fixture: `Distributed(..., fingerprint)` creates without complaint and then answers every insert with `Code: 53. DB::Exception: Sharding key expression does not evaluate to an integer type`, leaving `count()` at 0; `Distributed(..., cityHash64(fingerprint))` creates, inserts and reads the 128-bit value back intact. One fingerprint still maps to one shard, now through both 64-bit halves rather than the low one. |
| `spans`, `traces`, `error_spans` | `cityHash64(trace_id)` | a trace is whole on one shard; span-level intersections, trace assembly, and the service-graph half-row pairing (both edge halves share `trace_id`, so the query-time join is shard-local) are all shard-local. `spans` and `traces` are the two tables the TraceQL read design queries (issues #584 to #586); their wrappers carry the key as a literal rather than through `Ddl::Dist`, and `the_routing_wrappers_use_the_family_sharding_expression` is what keeps the two forms identical |
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

Every table but the eleven listed at the start of this section gets a Distributed wrapper of this shape (the schema controller renders one per table from the sharding-key column above):

```sql
CREATE TABLE log_samples_dist AS log_samples
ENGINE = Distributed('{cluster}', pulsus, log_samples, cityHash64(fingerprint));

CREATE TABLE metric_samples_dist AS metric_samples
ENGINE = Distributed('{cluster}', pulsus, metric_samples, cityHash64(fingerprint));
```

A failing view behaves the same way on each shard as on a single node: see §4.1, "What a failing view leaves behind". Clustered, the repeated-block rule is the `Replicated*` engines' own `replicated_deduplication_window`, and it applies to every trace table that carries a window.

Two invariants the schema controller enforces, because co-location silently breaks without them: **every table in a signal family uses the byte-identical sharding expression** (raw, tiers, series/index tables alike — a divergence would put a series' rollups on a different shard than its samples), and **every insert that has a `_dist` wrapper goes through it or computes the same expression client-side** — the trace writer never freelances shard placement. **The two landing tables are the deliberate exception** (issue #603): a logs or metrics push carries many fingerprints, so a `Distributed` insert would split one push into one insert per shard and would return before the shards held the rows — either alone ends "one push is one block", which is the guarantee that change was made for. Those two writes are placed by the connection, and the views put each block's derived rows on the shard that took it. Cluster configs use `internal_replication = true` (the underlying tables are `ReplicatedMergeTree`; the Distributed layer must write each block to one replica and let replication fan it out, or rows duplicate).

**What logs label discovery costs once placement is decoupled, in bytes and in marks** (issue #603). The three discovery scans used to read `FROM log_streams_idx_dist … WHERE fingerprint IN (SELECT DISTINCT fingerprint FROM log_metrics_<res>_dist …)` under `distributed_product_mode = 'local'`, whose exactness rested on a stream's index rows and its rollup rows being on the same shard. They are not, any more: a `log_streams_idx` row is written once per `(key, val, fingerprint, month)`, by the view off the block that first registered that stream in that month — one shard — while a `log_metrics_<res>` row is written for every `(fingerprint, bucket)` a push touches, on every shard that ever took one. So a shard evaluating locally could only answer *was this fingerprint active on this shard*, where the question is *was it active anywhere*. The activity scan is therefore **a statement of its own**, dispatched first, and its result is rendered into the index scan as a literal `fingerprint IN (…)` list — the shape `/series` already used. The answer is the one the subquery expressed.

- **Bytes: bounded in rows and in rendered text, unmeasured on the wire.** The active-fingerprint set crosses once to the initiator and once into each shard's copy of the rendered statement, where before it never left its shard. Both directions are bounded by shipped guards — the set at `PULSUS_MAX_STREAMS` rows, checked inside the scan's own loop, and the statement at the rendered-SQL ceiling — and by nothing stated here. No byte figure is given: a fingerprint does not render as bare digits (`FpLiteral` writes `toUInt128('…')` around the decimal value), so the rendered form is wider than its digits and wider again than the 16 bytes a binary set would carry. **It cannot be measured from a single node.** What would measure it: on the two-shard fixture, one discovery query per statement shape and each shard's own `ProfileEvents['NetworkSendBytes']`/`['NetworkReceiveBytes']` from `system.query_log` with the shape beside each figure — the per-shard, coordinator-inclusive Tier-1 instrument §9 defines. It belongs with the M1 re-capture below.
- **Marks: unsettled, and no equality is claimed.** `log_streams_idx` is `PARTITION BY month ORDER BY (key, val, fingerprint)`; a discovery scan bounds `month` and fixes or ranges `key`, and `fingerprint` is the third key component, so within one `key` the marks are ordered by `val` before it and a `fingerprint IN (…)` set — a subquery's or a literal list's — has no contiguous mark range to prune. That argument does not close: on a split fixture the subquery form's set is empty on the shard holding the index row, and what the optimizer does with an empty `IN` set decides the marks. Nobody has run both shapes on two shards. `crates/pulsus-read/tests/live_logs_cluster_discovery.rs` records both figures and bounds each shape by its own scan budget; it asserts no equality.
- **Discovery gains a cap it did not have.** A window with more active streams than `PULSUS_MAX_STREAMS` is now `422 query_too_broad` where the subquery form answered — on single-node deployments as well as clustered ones. That makes discovery consistent with stage-1 stream resolution.

Reader-issued settings: every logs, metrics and trace read sends the answer pins (`QuerySettings::answer_pins`): `skip_unavailable_shards` per `PULSUS_SKIP_UNAVAILABLE_SHARDS`, `skip_unavailable_shards_mode = 'unavailable_or_table_missing'`, `distributed_group_by_no_merge = 0`, `enable_parallel_replicas = 0`, `max_parallel_replicas = 1`, every `*_overflow_mode = 'throw'` (the span-name and value reads then set `distinct_overflow_mode = 'break'`, their 10,000-value cut), and `optimize_skip_unused_shards`. That last one is `0` on logs and metrics reads, because since #603 their rows sit on the shard that took the push, not the one the sharding key names. It is `1` on sharded trace reads, whose rows are placed by `cityHash64(trace_id)`, and `0` on the tag catalogue reads, whose tables are copied whole to every node. Sharded trace reads also send `optimize_distributed_group_by_sharding_key = 1`, `distributed_aggregation_memory_efficient = 1` and `prefer_localhost_replica = 1` (`QuerySettings::clustered_reader`). Where a coordinator-built `IN (...)` list would be large, the planner switches to the JOIN/subquery form so the filter executes as a remote-local subquery rather than shipping the set twice. There is **no** `rand()` sharding anywhere in the schema.

**TraceQL search reader-settings contract** (issue #57). **This is the previous search engine's contract: no request reaches that engine since issue #602 part 2b, and part 2c deletes it. The search statement's reads are `docs/TraceQL/sql-schema.md` §5.2–§5.3.** Precondition: `trace_spans` and `trace_attrs_idx` co-shard on the byte-identical `cityHash64(trace_id)` expression, so a trace's spans and index rows always co-reside — every Phase-1 `GROUP BY trace_id` completes on whole groups per shard, and every Phase-2 `trace_id IN (batch)` read (batches are ≤ 32 explicit ids, never a large coordinator-built set) prunes to the owning shards under `optimize_skip_unused_shards`. Every search query — generators and hydration/value batches alike — additionally carries server-side budgets with throw semantics: `max_rows_to_read = PULSUS_TRACEQL_SCAN_BUDGET_ROWS` + `read_overflow_mode = 'throw'` (non-indexable generators are budget-limited → `422 query_too_broad`, never silently slow), `max_bytes_to_read` + the same throw mode, `max_result_bytes` + `result_overflow_mode = 'throw'`, and `max_block_size = TRACE_SEARCH_MAX_BLOCK_ROWS` (4096 rows). Enforcement of the byte ceilings is **block-granular**, but (issue #57 re-audit) the transient is now HARD-bounded, not merely accepted-and-documented: every string value the search response returns (`name`/`service`, and `select()`-projected attribute values) is truncated at the SOURCE with a hard **byte** ceiling — `if(length(col) <= 8192, col, substringUTF8(col, 1, 2048)) AS col` (`TRACE_STR_COL_CAP` = 8192 bytes; the fallback branch cuts at 2048 UTF-8 code points, each ≤ 4 bytes, so it too never exceeds the byte ceiling) — so the driver's one transiently-buffered result block is bounded at ≤ `TRACE_SEARCH_MAX_BLOCK_ROWS` rows × (2 × `TRACE_STR_COL_CAP` string bytes + fixed-width columns) ≈ ≤ ~67 MB, never a-priori row-unbounded. **Live-verified on 24.8:** the result-side budget (`max_result_bytes` + `result_overflow_mode = 'throw'`) does not throw on **unwrapped passthrough columns** in streamed `SELECT` shapes; the source-truncation projection above makes its accounting **effective** on the hydration/root/value reads — a **deliberate hardening**. Layer 1 (64 MiB `max_result_bytes` per query) is therefore the practical **per-batch** byte bound on the search's Phase-2 reads, firing server-side before the driver materializes anything; Layer 2 — the engine's request-scoped 256 MiB retention counter (charged per row/entry as results stream) — remains the binding bound on **cross-batch retained accumulation** (merge tuples, the membership sets the hydration statement's predicate columns fill, heap-held response summaries, root summaries), which survives each batch's charge release and which no per-query server setting can see. A breach of either layer is a `422`, never an OOM. The engine's bounded-consumption guarantee is Rust-side (bounded generator transfer + the retention counter); ClickHouse's own generator-aggregation memory is additionally bounded (issue #57 re-audit, sub-problem B) by a dedicated generator-only ceiling — `max_memory_usage = PULSUS_TRACEQL_GENERATOR_MAX_MEMORY_BYTES` (512 MiB default) + `max_bytes_before_external_group_by = 0` (throw-not-spill) — so a dense common-value prefix's `GROUP BY trace_id` aggregation state is hard-bounded too: a breach is server code 241 (`MEMORY_LIMIT_EXCEEDED`) → `422 query_too_broad`, never an OOM (read-cost, as opposed to memory, is bounded by prefix confinement + the per-query server budgets; a read-bounded common-value generator SQL shape is tracked in issue #63). The "TraceQL search" and "Trace by ID" fan-out rows above are confirmed by Tier-1 per-stage/per-shard evidence on the 2-shard fixture (`docs/benchmarks/m4-traces-read-path.md` — coordinator-inclusive `system.query_log` rows verdicted against a client-computed `cityHash64(trace_id) % total_weight` roster, the same methodology that graduated the logs family); one caveat noted there, now partly closed: the trace-by-ID single-shard confinement was proven under the §7 reader-issued settings, which the search engine injects in clustered mode and which **the fetch handler now injects too** (issue #587 — the fetch has its own settings root, carrying the clustered-reader block and the catalog read budgets, without `final` since #601 part 4a (a span stored more than once is read as [api.md §1.0](api.md) states), and deliberately carrying neither `distributed_product_mode` nor `load_balancing`: the first has no subquery over a `Distributed` table to act on, and the second closes none of the replication-visibility states it was proposed for). **What is NOT closed is the confinement itself**: the fetch's statements are not the one that evidence measured, and whether the shard prune reduces them to the owning shard is **unmeasured by owner decision of 2026-10-02**. Both are correct without it — the coordinator fans out, the shards that do not hold the trace return nothing, and the answer is the same; what is lost is work, not correctness.

**Status: logs family graduated (M1, issue #16) and the traces read-path rows confirmed (M4, issue #57 — `docs/benchmarks/m4-traces-read-path.md`, 2-shard fixture, same methodology, wired into `schema-it-cluster` as hard verdicts); metrics/profiles remain design intent, not observed behavior.** Co-sharding is necessary but not sufficient — Distributed plans can still merge at the initiator or ship large sets if the generated SQL doesn't cooperate, which is why graduation requires *per-stage, exact-shard-roster* evidence, not just a terminal-query spot check (three rounds of CODE review on issue #16 caught successively narrower gaps: the first draft graduated on terminal-stage-only evidence with the discovery row uncovered; the second added per-stage evidence but silently excluded the coordinator's own shard from every row — under `prefer_localhost_replica = 1` the initiator's local-shard read is logged as its own `is_initial_query = 1` row, not a separate `is_initial_query = 0` sub-query row, and a filter that keeps only `is_initial_query = 0` misses it entirely; the third accepted any shard count for fingerprint-pruned stages without deriving which shards were *expected*, so a genuinely lost `system.query_log` row was indistinguishable from correct `optimize_skip_unused_shards` pruning). The `log_samples`/`log_streams`/`log_streams_idx`/`log_metrics_5s` sharding row above, and both logs-relevant rows of the Fan-out analysis table ("LogQL stream resolution + read" and "Label/tag discovery"), are confirmed by Tier-1 evidence for the SHAPE they state — which stages run shard-locally and what crosses the network — and, where a stage prunes by `fingerprint`, not for any shard count, which is selector-dependent (issue #498, below). **A stage with no `fingerprint` condition is not affected**: its expected roster is unconditionally the whole cluster, derived from no fingerprint, so no sharding key moves it — which is the case for label/tag discovery and for stream resolution (docs/schemas.md §9's two-tier model): per-shard `system.query_log` + `EXPLAIN PIPELINE`, captured separately for **every** stage each shape executes (resolution, hydration, samples/rollup read, and the discovery query in its own right) and **every** shard including the coordinator's own, verified against a **client-computed expected shard roster** (a cumulative-weight slot→shard map from `system.clusters`/`system.macros`, the sharding key modulo `total_weight` per queried fingerprint for pruned stages — `fingerprint % total_weight` when this evidence was captured, `cityHash64(fingerprint) % total_weight` since issue #498 widened the column). **Which shard owns which fingerprint moved with that key, so the capture's roster, placement and per-shard row counts are superseded and are marked as such where they appear — and so is whether a fingerprint-scoped stage participates on a subset at all: measured against a live server, the ten-fingerprint stage that reached three of four shards under the previous key reaches all four under the current one. The execution shape and the capture method are not superseded.** **Issue #603 supersedes the same figures a second time and further**: the writer no longer inserts through the `_dist` wrappers at all, so placement follows the connection that took the push rather than the sharding key, and one fingerprint's rows can sit on several shards — the roster, the placement and the per-shard row counts are superseded whatever the key is, and "LogQL stream resolution + read" now merges each stage's partials at the initiator rather than completing shard-locally. No read's ANSWER changes; label discovery becomes two statements (above). **The re-capture is the `bench-cluster` job** (`xtask bench logs-read --dist`): it lands the corpus through every shard in turn, and passes only if every stage of every shape reaches every shard, a shard counting as reached when it logged the stage's `QueryFinish` row. The per-shard network figure for each discovery statement shape is not captured. This is topology mechanics a 4-shard cluster demonstrates at any corpus scale. **Latency at Tier-2 scale (1 TB/7d) is separately tracked and unvalidated (issue #25)**; it does not gate this graduation, which is about which node does the work, not how fast. The traces rows ("Trace by ID" and "TraceQL search") were confirmed the same way in M4 — per-stage, coordinator-inclusive, roster-verdicted evidence on the 2-shard fixture (`docs/benchmarks/m4-traces-read-path.md`, `cityHash64(trace_id) % total_weight` client-side derivation, run as hard CI verdicts by `cargo xtask bench traces-read` — **for the search stages; the trace-by-ID stage's roster verdict is exempted there by the same owner decision, so that stage is captured and its coordinator-row check verdicted while no shard set is claimed for it**), with the trace-by-ID caveat noted there (proven under the §7 reader-issued settings, which the fetch handler now injects — issue #587; and superseded in the other direction by the same change, which gave the fetch different statements whose shard prune is unmeasured by owner decision). The metrics/profiles rows above remain design intent until the M3/M5 multi-shard benchmarks confirm them the same way; those snapshots then join the CI regression set.

---

## 8. Cross-cutting defaults

| Concern | Decision |
|---------|----------|
| `index_granularity` | 8192, except 2048 on `spans` and `error_spans`, and 1024 on `metric_exemplars`, the rollup levels, `service_edges` and `traces` |
| `PREWHERE` | planner always places the most selective low-cardinality predicate (`metric_name`, `service`, `type_id`) in `PREWHERE`; time predicates in `WHERE` (partitions already prune them) |
| Partitioning | **daily** for raw sample/span tables (short TTL, whole-part drops); **monthly** for series/index tables; the rollup levels by UTC day of `t_end − 1 ms` |
| TTL | each table's, in [configuration.md §7](configuration.md); `PULSUS_STORAGE_POLICY` and `PULSUS_COLD_VOLUME` move old parts to a cold volume ([configuration.md §7](configuration.md)) |
| Dedup strategy | metadata: `ReplacingMergeTree` + duplicate-tolerant reads (`LIMIT 1 BY`, `GROUP BY`); samples: append-only `MergeTree`, with a retried push suppressed at ingest by the writer that accepted the original, inside `PULSUS_INGEST_DEDUP_WINDOW` (issue #494), and writer batch atomicity below that; spans and error spans: `ReplacingMergeTree`, whose copies of one span are read as [api.md §1.0](api.md) states |
| Codecs | timestamps `DoubleDelta`, gauge-like floats `Gorilla`, counters/ids `Delta`/`T64`, payloads/labels `ZSTD(3..5)`, everything wrapped in `ZSTD(1)` minimum |
| Minimum ClickHouse | 26.8 LTS (the supported LTS line; servers before 26.3 do not tag an HTTP-200 mid-stream exception, so it cannot be told apart from result text — issue #412. All MVs are classic incremental — no refreshable-MV or scheduler dependency) |

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

**Rollup accuracy (M3).** Issue #18 part B compares each rollup-served function with the raw route over aligned windows and asserts that each stays within the distance from raw measured for it on issue #18.

**Storage amplification (M5).** Profile rows carry tree/function arrays plus the original payload; the M5 run measures bytes/profile at high frequency with shared symbol tables. If amplification is unacceptable, the fallback design (payload, function dictionary, and compact tree samples in separate tables) replaces it behind the same read path.
