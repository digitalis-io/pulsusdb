# PromQL rollups: building the levels

**Built.** Part A (#742), part B (#744) and the follow-up that chooses the resolution by the query's time range (issue #18) are on `main`; every statement about code below is true of `main` with that follow-up. Tiering (#743 part A) and a level's own retention and move age (#743 part B) are on main.

## 1. The setting

### 1.1 `PULSUS_ROLLUP_LEVELS`, `PULSUS_ROLLUP_RANGES`, `PULSUS_ROLLUP_COUNTER_BUCKETS`

| variable | default | meaning |
|---|---|---|
| `PULSUS_ROLLUP_LEVELS` | `60,720,7200,86400` | the rollup levels, in seconds, smallest first. Set and empty builds no levels, and every query reads raw |
| `PULSUS_ROLLUP_RANGES` | `10800,57600,604800,7776000` | for each level, the query range in seconds from which it is the resolution (`reading.md` §2); a range below the first reads raw. Read only when there are levels |
| `PULSUS_ROLLUP_COUNTER_BUCKETS` | `5` | the buckets of a level under 720 s that a `rate` or `increase` range covers at least for that level to answer it; from 720 s one bucket answers. The distances in `docs/api.md` §3.6 hold for `5` |

They are schema-build settings, documented beside the other schema settings (`PULSUS_ROLLUP_LEVELS`, `docs/configuration.md:179-181`). `schema/schema.sh` reads them with no colon in the default, so unset gives the default and set-but-empty gives none (`rollup_levels`, `rollup_ranges`, `counter_buckets`, `schema/schema.sh:60-62`); `RenderCtx::for_tests` is `ctx_from_vars` of the database and the retention alone, so it takes `DEFAULT_ROLLUP_LEVELS`, `DEFAULT_ROLLUP_RANGES` and `DEFAULT_ROLLUP_COUNTER_BUCKETS` (`crates/pulsus-schema/src/vars.rs:293-295`). The binary has no rollup setting: it learns the levels, their switch points and the counter bucket count from the tables' comments (`reading.md` §2).

Changing the set is a rebuild: drop the database and run `schema/schema.sh` again. A new set builds from what landing still holds.

### 1.2 Validation

`schema/schema.sh` exits `2` before contacting the server (`bad_levels`, `schema/schema.sh:148-151`), and `pulsus_schema::parse_rollup_levels` returns an error (`parse_rollup_levels`, `crates/pulsus-schema/src/render.rs:314`), at the first rule that fails, in this order and with the same messages:

| rule | why |
|---|---|
| no empty item | a level names a table, `metric_samples_<N>s` |
| each a whole number of seconds from 1 to 86400, no leading zero | the same |
| each divides 86400 | a bucket never crosses a UTC day, so a day's rows share one partition and one expiry |
| a level below 60 divides 60 | the first level runs every minute and builds whole minutes (§3.3) |
| each greater than the one before | each level is built from the one below |
| each a multiple of the one before | a coarser bucket is a whole number of finer buckets |
| at most 8 levels | each level is three objects and a timer; eight cover 5 s to 1 day with room |
| `2 × max(R_1, 60) + 870 ≤ H × 3600`, with `R_1` the first level and `H` = `PULSUS_METRICS_LANDING_RETENTION_HOURS` (default 6, `metrics_landing_hours`, `schema/schema.sh:54`) | a normal first-level run reads back to `now − (2p + 150 s + 120 s)`, and 600 s more is kept spare. `14400` with `H = 6` is refused: `2 × 14,400 + 870 = 29,670 > 21,600` |

`PULSUS_ROLLUP_RANGES` is checked after the levels, and only when there are levels (`bad_ranges`, `schema/schema.sh:208`; `parse_rollup_ranges`, `crates/pulsus-schema/src/render.rs:367`), the first failing rule the message, `PULSUS_ROLLUP_RANGES: ` before it in the script:

| rule | message |
|---|---|
| no empty item | `an empty item in '<text>'` |
| each a whole number of seconds from 1 to 999999999, no leading zero | `<item> is not a whole number of seconds from 1 to 999999999` |
| one for each level | `<n> ranges for <m> levels: give one for each level` |
| each greater than the one before | `<a> then <b>: the ranges must increase` |
| each at least its level | `<range> is shorter than its level <w>` |

`PULSUS_ROLLUP_COUNTER_BUCKETS` is a whole number from 2 to 1000 with no leading zero, else `<v> is not a whole number from 2 to 1000` (`parse_rollup_counter_buckets`, `crates/pulsus-schema/src/render.rs:415`). One bucket would give a `rate` window one point at the levels under 720 s.

`the_script_renders_exactly_what_this_crate_renders` (`crates/pulsus-schema/tests/schema_file.rs:477`) holds the script's render and the crate's equal, and `the_script_and_the_crate_refuse_the_same_level_lists` (`crates/pulsus-schema/tests/schema_file.rs:2816`) and `the_script_and_the_crate_refuse_the_same_ranges_and_counter_buckets` (`crates/pulsus-schema/tests/schema_file.rs:3014`) hold their refusals equal.

### 1.3 The default, and what each set costs

One day, 4,000 series. The bytes of the levels, plus the projections the next level's run reads (a level whose next level is a day carries none, §2.2), as a share of raw `metric_samples` (from §6's table):

| set | 15 s scrape | 60 s scrape |
|---|---:|---:|
| `5,60,720,7200,86400` | 765.7 % | 1,843.4 % |
| **`60,720,7200,86400` (default)** | **103.8 %** | **634.0 %** |
| `720,7200,86400` | 7.4 % | 39.3 % |

| choice | reason |
|---|---|
| no 5 s level by default | raw samples are the 5 s data (owner, 2026-10-28): a range below the first level's switch point, 3 h by default, reads raw (owner, 2026-10-31) |
| 60 s first | a range from 3 h reads the 60 s level (owner, 2026-10-31). At a 15 s scrape it holds a quarter of raw's rows and 53.4 % of raw's bytes |
| a deployment scraping every 60 s | the 60 s level thins nothing there (336 % of raw, the same rows): set `720,7200,86400`. `docs/configuration.md` says so beside the setting |
| a deployment scraping every few seconds | add a first level of 5 or 15 s, which divides 60 |

Only sets drawn from 5, 60, 720, 7200 and 86400 s were measured; the `flags` column (§2.2) is not in these figures. On a 60 s level of 2,000 series over one day with 1 % of rows flagged, it took 40,770 bytes compressed, 1.8 % of the table's 2,245,802. Output not kept.

## 2. The tables

### 2.1 One template, rendered once per level

`schema/schema.sql` holds one level template, one contiguous block at the end of the file (`schema/schema.sql:989-1120`). Its lines carry a prefix that `expand_levels` (`crates/pulsus-schema/src/render.rs:670`) and the script's `awk` resolve before the `--@single`/`--@cluster` pass; the marker and exactly one space after it are removed, so a line may then carry a mode marker:

| prefix | rendered for |
|---|---|
| `--@level ` | every level |
| `--@level-first ` | the first level only (reads landing) |
| `--@level-next ` | every level after the first (reads the level below) |
| `--@level-feeds ` | every level whose next level is shorter than a day: the projection the next level's run reads |

The block is replaced, in place, by its lines for each level in turn, smallest first: for each level its table, its wrapper (clustered) and its view. Tokens, for level `i` of `R_i` seconds, are written as text, never computed in milliseconds, so the script and the crate print the same digits:

| token | value |
|---|---|
| `{{level}}` | `<R_i>s`, as in `metric_samples_60s` |
| `{{level_ms}}` | `R_i × 1000` |
| `{{source}}` | `metric_samples_<R_{i−1}>s`, the table of level `i − 1` (empty for the first) |
| `{{parent_ms}}` | `R_{i+1} × 1000`, the projection key (empty for the last) |
| `{{timer}}` | §3.3 |
| `{{period_ms}}` | the first level's run period, `max(R_1, 60) × 1000` |

### 2.2 The level table

The table's `CREATE TABLE` and its wrapper, as `schema/schema.sql:1004-1031` carries them:

```sql
--@level CREATE TABLE IF NOT EXISTS {{db}}.metric_samples_{{level}}{{on_cluster}}
--@level (
--@level     kind UInt8,
--@level     org_id LowCardinality(String),
--@level     fingerprint UInt128 CODEC(ZSTD(1)),
--@level     t_end Int64 CODEC(DoubleDelta, ZSTD(1)),
--@level     t_last Int64 CODEC(DoubleDelta, ZSTD(1)),
--@level     last Float64 CODEC(Gorilla(8), ZSTD(1)),
--@level     min Float64 CODEC(Gorilla(8), ZSTD(1)),
--@level     max Float64 CODEC(Gorilla(8), ZSTD(1)),
--@level     sum Float64 CODEC(Gorilla(8), ZSTD(1)),
--@level     count UInt32 CODEC(T64, ZSTD(1)),
--@level     inc Float64 CODEC(Gorilla(8), ZSTD(1)),
--@level     flags UInt8 CODEC(T64, ZSTD(1)),
--@level     built_at Int64 CODEC(DoubleDelta, ZSTD(1))
--@level-feeds     , PROJECTION by_parent (SELECT * ORDER BY (intDiv(t_end - 1, {{parent_ms}}), org_id, fingerprint, t_end))
--@level )
--@level --@single  ENGINE = MergeTree
--@level --@cluster ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{{db}}.metric_samples_{{level}}', '{replica}')
--@level PARTITION BY toDate(fromUnixTimestamp64Milli(t_end - 1), 'UTC')
--@level ORDER BY (org_id, fingerprint, t_end)
--@level TTL toDateTime(least((intDiv(t_end - 1, 86400000) + 1) * 86400 + ({{level_retention_days}} * 86400), 4294967295))
--@level-cold , toDateTime(least((intDiv(t_end - 1, 86400000) + 1) * 86400 + ({{level_hot_days}} * 86400), 4294967295)) TO VOLUME '{{cold_volume}}'
--@level --@single  SETTINGS ttl_only_drop_parts = 1, index_granularity = 1024, non_replicated_deduplication_window = {{metrics_dedup_window}}{{storage_policy}}
--@level --@cluster SETTINGS ttl_only_drop_parts = 1, index_granularity = 1024, replicated_deduplication_window = {{metrics_dedup_window}}, replicated_deduplication_window_seconds = {{dedup_window_seconds}}{{storage_policy}}
--@level COMMENT 'rollup_res_ms={{level_ms}} rollup_from_ms={{from_ms}} counter_buckets={{counter_buckets}}';
--@level --@cluster CREATE TABLE IF NOT EXISTS {{db}}.metric_samples_{{level}}{{dist_suffix}}{{on_cluster}} AS {{db}}.metric_samples_{{level}}
--@level --@cluster ENGINE = Distributed('{{cluster}}', '{{db}}', 'metric_samples_{{level}}', cityHash64(fingerprint));
```

| column | value |
|---|---|
| `kind` | `0` a data row, `1` a run row (§2.3) |
| `t_end` | the bucket's end |
| `t_last`, `last` | the time and value of the bucket's last sample that is not a stale marker; `t_last = 0` when the bucket holds only stale markers |
| `min`, `max`, `sum`, `count` | over the bucket's values that are not NaN |
| `inc` | the counter increase inside the bucket: over its samples that are not stale markers, `v − q` when `v ≥ q`, else `v`, and `0` when either is NaN; `q` is the predecessor of §3.1 |
| `flags` | bit 1 (`1`): the bucket's last sample is a stale marker; bit 2 (`2`): the bucket holds a NaN that is not a stale marker |
| `built_at` | the run's clock, in milliseconds |

| property | rule |
|---|---|
| main key | fingerprint first, for queries. A parent-block main key would make queries read 4 to 120 times more rows (§6.3) |
| projection `by_parent` | the next level's bucket first; only that level's run reads it (§3.4). A level whose next level is a day has none: the day run reads the source's day partition whole, and with the projection it read the same marks (§3.4) |
| expiry | in the table's own DDL. Every row of a UTC day expires at the day's end plus the level's retention (`PULSUS_ROLLUP_RETENTION_DAYS`, else `PULSUS_RETENTION_DAYS`); with `PULSUS_ROLLUP_HOT_DAYS` its `--@level-cold ` line moves the part to the cold volume at the day's end plus the move age; and `ttl_only_drop_parts` drops a day's part whole |
| comment | `rollup_res_ms=<ms> rollup_from_ms=<ms> counter_buckets=<n>`, which the binary reads to discover the levels, the query range from which each is the resolution and the counter bucket count (`reading.md` §2). `{{from_ms}}` is the level's range followed by `000`, written as text as `{{level_ms}}` is: 7,776,000,000 exceeds 32 bits |
| wrapper | a wrapper created `AS` a table with a projection carries the columns and no projection |

### 2.3 Run rows

Every run that has a window, whether or not it found samples, appends a run row with `kind = 1`, `org_id = ''`, `fingerprint = 0`, `t_last = lo`, `t_end = hi`, `built_at` = the run's start and every other column `0`: one row for each UTC day its `(lo, hi]` touches, each cut at midnight, so each lies in its day's partition. A run with no window, and every run on a replica that is not its shard's builder (§4.1), writes nothing.

The run row is a `UNION ALL` branch of the run's own `SELECT`. With the block pins `max_insert_threads = 1` and `min_insert_block_size_rows` (`schema/schema.sql:1129`), a run's data rows and its run row are one part per partition, so a replica holds both or neither. Measured: each one-hour first-level run wrote 240,001 rows, 240,000 data rows and one run row, into one part (`measure/projection-run/10_60s_parts.tsv`).

A level's coverage on a shard is the union of its run rows' `(t_last, t_end]`. A run row expires with its day's data.

### 2.4 `metric_landing`'s key

The first level reads `metric_landing` by sample time, so landing is keyed by the 5-minute block of the sample time, `intDiv(unix_milli, 300000)`, after `kind`, whatever the set (`schema/schema.sql:288`):

```sql
ORDER BY (kind, intDiv(unix_milli, 300000), org_id, metric_name, fingerprint, unix_milli)
```

Its partition and expiry are those it had before. The cost is §6.1's: landing is 1.30 times its size on the earlier key, `(org_id, kind, metric_name, fingerprint, unix_milli)`, at a 15 s scrape and 2.73 times at 60 s.

## 3. The statements

### 3.1 The view

Each level's view, as `schema/schema.sql:1032-1120` carries it. `--@level-first` lines build the first level from `metric_landing`; `--@level-next` lines build every later level from `{{source}}`, the level below.

```sql
--@level CREATE MATERIALIZED VIEW {{db}}.metric_samples_{{level}}_mv{{on_cluster}} REFRESH {{timer}} APPEND TO {{db}}.metric_samples_{{level}}
--@level --@single  AS WITH toUInt8(1) AS builder, toUInt256(1) AS n_shards, toUInt256(0) AS shard_ix,
--@level --@cluster AS WITH (SELECT toUInt8(any(replica_num) = (SELECT min(replica_num) FROM system.clusters WHERE cluster = '{{cluster}}' AND shard_num = (SELECT any(shard_num) FROM system.clusters WHERE cluster = '{{cluster}}' AND is_local))) FROM system.clusters WHERE cluster = '{{cluster}}' AND is_local) AS builder,
--@level --@cluster (SELECT toUInt256(uniqExact(shard_num)) FROM system.clusters WHERE cluster = '{{cluster}}') AS n_shards,
--@level --@cluster (SELECT toUInt256(any(shard_num) - 1) FROM system.clusters WHERE cluster = '{{cluster}}' AND is_local) AS shard_ix,
--@level      toUInt256('340282366920938463463374607431768211456') AS space,
--@level      intDiv(space, greatest(n_shards, toUInt256(1))) AS slice_w,
--@level      slice_w * shard_ix AS slice_lo,
--@level      if(shard_ix + 1 = n_shards, space - 1, slice_w * (shard_ix + 1) - 1) AS slice_hi,
--@level      toInt64(toUnixTimestamp64Milli(now64())) AS now_ms,
--@level-first      (SELECT (lo, hi, lo < hi) FROM (
--@level-first         SELECT intDiv(n - 150000, {{period_ms}}) * {{period_ms}} AS ready,
--@level-first                intDiv(n - {{metrics_landing_retention_hours}} * 3600000 + 720000 + {{period_ms}} - 1, {{period_ms}}) * {{period_ms}} AS oldest,
--@level-first                if(w = 0, ready - {{period_ms}}, greatest(w, oldest)) AS lo,
--@level-first                least(ready, lo + 5 * {{period_ms}}) AS hi
--@level-first         FROM (SELECT toInt64(toUnixTimestamp64Milli(now64())) AS n, max(t_end) AS w
--@level-first               FROM {{db}}.metric_samples_{{level}} WHERE org_id = '' AND fingerprint = 0 AND kind = 1))) AS plan,
--@level-next      (SELECT (b - {{level_ms}}, least(intDiv(c.2, {{level_ms}}) * {{level_ms}}, b + 9 * {{level_ms}}), c.2 >= b) FROM (
--@level-next         SELECT arrayFirst(m -> greatest(intDiv(m.1 + {{level_ms}} - 1, {{level_ms}}) * {{level_ms}} + {{level_ms}}, (intDiv(w, {{level_ms}}) + 1) * {{level_ms}}) <= m.2, u) AS c,
--@level-next                greatest(intDiv(c.1 + {{level_ms}} - 1, {{level_ms}}) * {{level_ms}} + {{level_ms}}, (intDiv(w, {{level_ms}}) + 1) * {{level_ms}}) AS b
--@level-next         FROM (SELECT any(w0) AS w,
--@level-next                      arrayFold((acc, x) -> if(empty(acc) OR x.1 > acc[-1].2, arrayPushBack(acc, x), arrayPushBack(arrayPopBack(acc), (acc[-1].1, greatest(acc[-1].2, x.2)))),
--@level-next                                arraySort(groupArray((t_last, t_end))), CAST([], 'Array(Tuple(Int64, Int64))')) AS u
--@level-next               FROM (SELECT (SELECT max(t_end) FROM {{db}}.metric_samples_{{level}} WHERE org_id = '' AND fingerprint = 0 AND kind = 1) AS w0, t_last, t_end
--@level-next                     FROM {{db}}.{{source}} WHERE org_id = '' AND fingerprint = 0 AND kind = 1)
--@level-next               WHERE t_end > w0))) AS plan,
--@level      tupleElement(plan, 1) AS lo,
--@level      tupleElement(plan, 2) AS hi,
--@level      builder = 1 AND tupleElement(plan, 3) AS work
--@level-first SELECT toUInt8(0) AS kind, org_id, fingerprint, (intDiv(unix_milli - 1, {{level_ms}}) + 1) * {{level_ms}} AS t_end,
--@level-first   maxIf(unix_milli, bits != 9218868437227405314) AS t_last, argMaxIf(value, (unix_milli, bits), bits != 9218868437227405314) AS last,
--@level-first   minIf(value, NOT isNaN(value)) AS min, maxIf(value, NOT isNaN(value)) AS max,
--@level-first   sumIf(value, NOT isNaN(value)) AS sum, toUInt32(countIf(NOT isNaN(value))) AS count,
--@level-first   sum(d) AS inc, toUInt8(if(argMax(bits, (unix_milli, bits)) = 9218868437227405314, 1, 0) + if(countIf(isNaN(value) AND bits != 9218868437227405314) > 0, 2, 0)) AS flags, any(now_ms) AS built_at
--@level-first FROM (
--@level-first   SELECT org_id, fingerprint, unix_milli, bits, value,
--@level-first     if(q IS NULL OR isNaN(value) OR isNaN(assumeNotNull(q)), 0.,
--@level-first        if(value >= assumeNotNull(q), value - assumeNotNull(q), value)) AS d
--@level-first   FROM (
--@level-first     SELECT org_id, fingerprint, unix_milli, bits, value,
--@level-first       if(isNull(q0), if(unix_milli - tp0 <= 86400000, p0, NULL), if(unix_milli - t0 <= 86400000, q0, NULL)) AS q
--@level-first     FROM (
--@level-first       SELECT org_id, fingerprint, unix_milli, bits, reinterpretAsFloat64(bits) AS value,
--@level-first         anyLast(if(bits = 9218868437227405314, NULL, toNullable(reinterpretAsFloat64(bits)))) OVER (PARTITION BY org_id, fingerprint ORDER BY unix_milli, bits ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS q0,
--@level-first         anyLast(if(bits = 9218868437227405314, NULL, toNullable(unix_milli))) OVER (PARTITION BY org_id, fingerprint ORDER BY unix_milli, bits ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS t0
--@level-first       FROM (SELECT DISTINCT org_id, fingerprint, unix_milli, reinterpretAsUInt64(value) AS bits
--@level-first --@single             FROM {{db}}.metric_landing
--@level-first --@cluster             FROM clusterAllReplicas('{{cluster}}', {{db}}.metric_landing)
--@level-first             WHERE work AND kind = 0
--@level-first               AND intDiv(unix_milli, 300000) BETWEEN intDiv(lo - 120000, 300000) AND intDiv(hi, 300000)
--@level-first               AND unix_milli > lo - 120000 AND unix_milli <= hi
--@level-first               AND fingerprint BETWEEN toUInt128(slice_lo) AND toUInt128(slice_hi))) AS w
--@level-first     LEFT JOIN (
--@level-first       SELECT org_id, fingerprint,
--@level-first              toNullable(argMaxIf(last, t_last, t_last > 0)) AS p0,
--@level-first              toNullable(maxIf(t_last, t_last > 0)) AS tp0
--@level-first       FROM {{db}}.metric_samples_{{level}}
--@level-first       WHERE kind = 0 AND t_end <= lo AND t_end > lo - 86400000
--@level-first         AND (org_id, fingerprint) IN (SELECT org_id, fingerprint
--@level-first --@single  FROM {{db}}.metric_landing
--@level-first --@cluster FROM clusterAllReplicas('{{cluster}}', {{db}}.metric_landing)
--@level-first                               WHERE work AND kind = 0
--@level-first                                 AND intDiv(unix_milli, 300000) BETWEEN intDiv(lo - 120000, 300000) AND intDiv(hi, 300000)
--@level-first                                 AND unix_milli > lo - 120000 AND unix_milli <= hi
--@level-first                                 AND fingerprint BETWEEN toUInt128(slice_lo) AND toUInt128(slice_hi)
--@level-first                               GROUP BY org_id, fingerprint HAVING minIf(unix_milli, reinterpretAsUInt64(value) != 9218868437227405314) > lo)
--@level-first       GROUP BY org_id, fingerprint) AS z USING (org_id, fingerprint))
--@level-first   WHERE unix_milli > lo)
--@level-first GROUP BY org_id, fingerprint, t_end
--@level-next SELECT toUInt8(0) AS kind, org_id, fingerprint, (intDiv(te - 1, {{level_ms}}) + 1) * {{level_ms}} AS t_end,
--@level-next   max(tl) AS t_last, argMax(lv, tl) AS last,
--@level-next   minIf(mn, ct > 0) AS min, maxIf(mx, ct > 0) AS max,
--@level-next   sum(sm) AS sum, toUInt32(sum(ct)) AS count, sum(ic) AS inc, toUInt8(bitAnd(argMax(fl, te), 1) + bitAnd(groupBitOr(fl), 2)) AS flags, any(now_ms) AS built_at
--@level-next FROM (SELECT org_id, fingerprint, t_end AS te, t_last AS tl, last AS lv, min AS mn, max AS mx,
--@level-next              sum AS sm, count AS ct, inc AS ic, flags AS fl
--@level-next       FROM {{db}}.{{source}}
--@level-next       WHERE work AND kind = 0
--@level-next         AND intDiv(t_end - 1, {{level_ms}}) BETWEEN intDiv(lo, {{level_ms}}) AND intDiv(hi - 1, {{level_ms}})
--@level-next         AND t_end > lo AND t_end <= hi)
--@level-next GROUP BY org_id, fingerprint, t_end
--@level UNION ALL
--@level SELECT toUInt8(1), '', toUInt128(0), r.2, r.1, 0., 0., 0., 0., toUInt32(0), 0., toUInt8(0), now_ms
--@level FROM system.one
--@level ARRAY JOIN if(work, arrayMap(d -> (greatest(lo, toInt64(d) * 86400000), least(hi, (toInt64(d) + 1) * 86400000)),
--@level                                range(toUInt64(intDiv(lo, 86400000)), toUInt64(intDiv(hi - 1, 86400000) + 1))),
--@level                     CAST([], 'Array(Tuple(Int64, Int64))')) AS r
--@level SETTINGS max_threads = 2, max_memory_usage = 4294967296, max_execution_time = 50,
--@level --@cluster          skip_unavailable_shards = 0,
--@level          max_insert_threads = 1, max_insert_block_size = 100000000, min_insert_block_size_rows = 100000000, min_insert_block_size_bytes = 10000000000;
```

| part | rule |
|---|---|
| `builder`, slice | a replica builds only if it is its shard's builder (§4.1), and only its shard's slice of the fingerprint range. On one node it is the builder and the slice is the whole range |
| `plan`, one scalar subquery | each view computes its window inside one scalar subquery returning `(lo, hi, has_work)`, and takes `lo`, `hi` and `work` from it with `tupleElement`. The projection `by_parent` applies only when the bounds are constants when the statement is analysed. Measured on the development server: with `lo` and `hi` computed by `arrayFold` and `arrayFirst` directly in `WITH`, two 720 s runs read 20,907 and 17,835 rows, the whole 60 s table, without the projection; with the scalar subquery the building run read 9,959 rows through `by_parent`. Output not kept |
| first level's window | `ready` = `⌊(now − 150 s) ÷ p⌋ × p`; `lo` = one period before `ready` on the first run, else the later of the watermark `w` and the oldest start landing still holds, `now − H × 3600 s + 720 s` rounded up to a period; `hi` = the earlier of `ready` and `lo + 5p`; work when `lo < hi` |
| `t_last`, `last` | the last sample that is not a stale marker (`maxIf`, `argMaxIf` on `bits != 9218868437227405314`, the stale marker's bits) |
| `flags` | bit 1 when the bucket's last sample, by `(unix_milli, bits)`, is a stale marker; bit 2 when it holds a NaN that is not one. A later level takes bit 1 from its last source row and bit 2 from any (`bitAnd(argMax(fl, te), 1) + bitAnd(groupBitOr(fl), 2)`) |
| predecessor `q` | the series' previous sample that is not a stale marker: `anyLast` over the preceding rows, a stale marker given as NULL (`q0`, `t0`). When the run holds no earlier sample of the series, it is the level's own newest row before `lo` that has `t_last > 0`, at most a day back (`p0`, `tp0`, the `LEFT JOIN … USING (org_id, fingerprint)`), for the series whose first non-stale sample in the run comes after `lo`. A predecessor more than 86,400,000 ms before the sample is not used, and that step counts `0` |
| `inc` | `v − q` when `v ≥ q`, else `v`; `0` when there is no `q` or either is NaN. `count`, `sum`, `min` and `max` skip NaN |
| `work` | false when there is no window or the replica is not the builder: the data branches read nothing and the run-row branch emits nothing |
| settings | `max_threads = 2` (§5), `max_memory_usage = 4294967296`, `max_execution_time = 50`, the block pins, and, clustered, `skip_unavailable_shards = 0`, so a replica that does not answer fails the run and no run row is written |

**Why the predecessor comes from the level's own table.** Measured on the development server: 200 series at a 60 s scrape with 30 % of scrapes missing and a 20-minute gap on every tenth series, built by 180 separate one-minute runs. With the predecessor taken only from the run's two-minute lookback, the step into a bucket was lost in 2,270 of 24,644 buckets (8,181 of increase); with the level's own row, the increase matched the increase computed from every sample in all 24,644. Keyed by fingerprint alone, a second tenant's samples on the same 200 fingerprints hid tenant `a`'s predecessor in the same 2,270 buckets; keyed by `(org_id, fingerprint)`, in none. Cost, 2,000 series at 15 s, 60 one-minute runs: rows read per run 69,950 → 139,898 on average (90,113 → 180,225 at most), 21 → 33 ms on average, 16.7 → 18.6 MB; the rows equal those without the join (120,000 rows, 0 differ). Output not kept.

**Why the predecessor skips stale markers.** With the immediately preceding sample as `q`, `100, stale, 130` stored `inc` 0. Measured on the development server, 2,000 counters over one hour at 15 s, every seventh series stale for three scrapes then resuming, 1 % ordinary NaN on 40 series, against an expectation computed from each series' non-stale samples: the immediate-predecessor form was wrong on 287 series, the `anyLast` form on none; 118–124 ms and 69–88 MB against 108–121 ms and 77–82 MB. Output not kept.

### 3.2 A later level's window, and repeats

| rule | |
|---|---|
| a later level's `lo` | with `w` its own watermark (the largest `t_end` of its run rows) and `U` the local source's run intervals merged: `lo = b − W` for the smallest multiple `b` of `W` with `b > w` and `(b − W, b]` inside one interval of `U`. If there is none, the run writes nothing. Otherwise it writes a run row ending at `hi ≥ b > w`. So every run either moves the watermark forward or finds no work. Buckets between `w` and `lo` touch a hole in the source: they are never built at this level, and are read from finer levels or raw |
| a later level's `hi` | the largest multiple of `W`, at most `lo + 10 × W`, with `(lo, hi]` inside that same interval of `U`. A run row and its data are one part, so a replica that holds the source's run row holds its data |
| a later level's row | `count`, `sum` and `inc` summed; `min` and `max` over the rows with `count > 0`; `last` and `t_last` from the row with the largest `t_last` (a source row with `t_last = 0` never wins it) |
| repeats (D12) | the first level drops exact `(fingerprint, time, value bits)` repeats and orders values at one millisecond by value bits, as the raw route reads them (`reinterpretAsUInt64`, `crates/pulsus-read/src/metrics/sample_sql.rs:99`). A level that counted a repeat twice would answer `sum_over_time` and `avg_over_time` unlike raw on the same stored data. A client that sends a sample twice still has two stored rows |

Measured effect of dropping repeats: one day, 4,000 series at 15 s, 80 series with every sample stored twice and 80 with a second value at one millisecond every 100th sample. With the `DISTINCT`, the 60 s level built from that corpus equalled the one built without repeats in all 5,760,000 rows, every column as bits. Without it, all 112,320 rows of the doubled series had twice the `count` and `sum` (one bucket: `count` 8 against 4, `sum` 12 against 6), and `inc` differed in 1,743 of the 93,600 rows of the same-millisecond series; 28 other rows differed in the last bit of `inc`, by addition order. The `DISTINCT`'s cost, one-minute run, three runs: 176,038 rows read, 21–25 ms, 15.6 MB, against 94,118 rows, 13 ms, 6.4 MB without it. Output not kept (`measure/README.md`).

### 3.3 Timers, derived from the sizes

Let `p = max(R_1, 60 s)` be the first level's period, and `d_i` the delay after a bucket's end by which level `i` has built it (`timer_unit`, `crates/pulsus-schema/src/render.rs:640`).

| level | `{{timer}}` | builds up to | `d_i` |
|---|---|---|---|
| first | `EVERY p` | `⌊(now − 150 s) ÷ p⌋ × p` | `⌈150 s ÷ p⌉ × p` |
| `i > 1` | `EVERY R_i OFFSET (d_i mod R_i)`, the offset omitted when it is 0 | the largest multiple of `R_i` its local source's run rows cover (§3.2) | `d_{i−1} + 60 s` |

Units: the largest of `DAY`, `HOUR`, `MINUTE` that divides the value, else `SECOND`. Offsets are in `SECOND`.

The default set:

| level | timer | `d_i` | worst age of its watermark |
|---|---|---:|---:|
| 60 s | `EVERY 1 MINUTE` | 180 s | 270 s (`2p + 150 s`) |
| 720 s | `EVERY 12 MINUTE OFFSET 240 SECOND` | 240 s | 960 s (`R + d`) |
| 7200 s | `EVERY 2 HOUR OFFSET 300 SECOND` | 300 s | 7,500 s |
| 86400 s | `EVERY 1 DAY OFFSET 360 SECOND` | 360 s | 86,760 s |

The server accepted `EVERY 12 MINUTE OFFSET 240 SECOND`, `EVERY 2 HOUR OFFSET 300 SECOND`, `EVERY 1 DAY OFFSET 360 SECOND`, `EVERY 6 HOUR OFFSET 420 SECOND`, `EVERY 5 MINUTE OFFSET 240 SECOND`, `EVERY 90 SECOND` and `EVERY 1 MINUTE`. For a view created at 14:49:58 the next runs were at 14:52, 16:05, 00:06, 18:07, 14:54, 14:51:00 and 14:50, each a multiple of its period plus its offset. Output not kept.

| rule | why |
|---|---|
| a run builds only buckets its source level has passed | the order runs fire in never decides correctness: a coarser run that fires before its source sees the old watermark and builds less |
| `d_i = d_{i−1} + 60 s` | the source's run ended at least a minute earlier; runs over their normal window took at most 128 ms (§6.2) |
| no `DEPENDS ON` | the watermark orders runs by their data on one server or many; `DEPENDS ON` orders views on one server only |
| a missed or failed run | the next starts from the same watermark. The first level catches up at most 5 periods of samples per run, the others 10 buckets. An empty window still writes its run row, so the watermark moves |
| never older than landing | a first-level run starts no earlier than `now − H × 3600 s + 720 s`. A stop longer than landing holds leaves a hole in the run rows, which is read from finer levels or raw (`reading.md` §3) |
| the first run on a database already holding samples | the first level starts one period back and writes a run row from there. Earlier history has no run row and reads raw. A coarser level builds from the level below, back to that level's oldest run row |
| settle 150 s | `received_ms` is stamped at admission, and the insert may commit up to the landing budget, 120 s, later (`WriterRuntime::landing_budget`, `docs/ingest-one-source-table.md:417-421`). A sample later than that stays in raw only |
| lookback 120 s | the first level reads two minutes before its window for each series' previous sample; a series with none there takes it from the level's own table, up to a day back (§3.1) |

### 3.4 A run reads only its window

`read_rows` per run, one day, 4,000 series, each table merged to one part per partition (the worst case for "before"), three runs each, from `system.query_log`. "Before" is the same statements on landing's earlier key and on levels without a projection. Output not kept.

| run | before | after | how |
|---|---:|---:|---|
| first level (5 s) from landing, 15 s scrape | 1,920,002 | 165,380 | landing keyed by `intDiv(unix_milli, 300000)`; the filter names the blocks: two 5-minute blocks for one minute plus two of lookback. The first level's read does not depend on its size |
| first level from landing, 60 s scrape | 479,998 | 51,584 | same |
| 60 s from 5 s, 15 s scrape | 8,190,976 | 16,384 | `by_parent` on the 5 s level, used by the run (`system.query_log.projections`) |
| 60 s from 5 s, 60 s scrape | 5,759,996 | 5,120 | same |
| 720 s from 60 s | 5,760,000 / 5,755,904 | 49,152 | same, on the 60 s level |
| 7200 s from 720 s | 480,000 | 40,960 | same |
| 86400 s from 7200 s | 48,000 | 48,000 | reads the whole day by design |

Without the block condition the engine did not use the projection's expression key: the same statements read 23,039,986 rows. So every build statement names the block. These were measured before the first level's predecessor join (§3.1), which doubles its rows read.

**The day level carries no projection below it.** A day bucket is the 7200 s table's whole day partition, so the plain table reads the same marks: measured with the projection, one 86400 s run read 6,628 rows with `projections = []` (parts of 4,001 and 801 rows: 4,802 rows of data, 1,825 of the run-row scan, the one-row run-row branch). Output not kept. `--@level-feeds` lines are kept only for a level whose next level is shorter than a day (`Feeds`, `crates/pulsus-schema/src/render.rs:619`).

**The recorded run** (`measure/projection-run/`). One node; 4,000 series at 15 s over three hours, landed through a `metric_landing` with §2.4's key; the 60 s and 720 s tables as they then stood, without `flags`; three one-hour first-level runs, then the 720 s run for one bucket, three times on an emptied table.

| what | value |
|---|---|
| first-level run, one hour | 240,001 rows written, 240,000 data rows and one run row, in one part per run |
| 720 s run with the block condition | `projections` names `<db>.metric_samples_60s.by_parent`; `read_rows` 49,153; 15–17 ms; 13.5 MB; 4,001 rows written, 4,000 data rows and one run row |
| the same without the `intDiv(t_end - 1, 720000) BETWEEN` line | `projections` = `[]`; `read_rows` 240,002: the one source part whose `t_end` range holds the window (240,001 of the table's 720,003 rows) and the one-row run-row branch; 28–32 ms |

Per day at a 15 s scrape and 4,000 series, a first-level run every minute read 1,440 × 165,380 = 238 million rows before the predecessor join, about ten times the day's 23,040,000 raw rows, against 2.76 billion on landing's earlier key (1,440 × 1,920,002). A one-minute block would read three minutes per run, but makes landing 3.05 times its size at a 15 s scrape and 5.60 times at 60 s; the 5-minute block costs 1.30 and 2.73 times (§6.1).

## 4. What an observer sees when part of it fails

| failure | observed | answers |
|---|---|---|
| a run fails, on its memory limit, its time limit or a replica that does not answer | no rows and no run row; the next run retries the same window | the window is past the watermark, so it is read from finer levels or raw |
| a level's view is stopped | its run rows stop; levels above it build up to its last run row and then find no work | queries read the newer part of every window from finer levels or raw |
| a stop longer than landing holds | the first level resumes at `now − H × 3600 s + 720 s`; the run rows have a hole, and every coarser level leaves unbuilt exactly the buckets it touches | the hole is read raw |
| a coarser run fires before its source's | it sees the old watermark and builds less | unchanged |
| a run's data part is on a replica and its run row is not | cannot happen: they are one part (§2.3) | — |

### 4.1 Cluster

| rule | |
|---|---|
| builder | one per shard: the replica with the lowest `replica_num` of its shard in `system.clusters`; the others compute `builder = 0`, select nothing and write no run row |
| first level's read | `clusterAllReplicas` over `metric_landing` for this shard's fingerprint slice, exact repeats dropped (§3.2), so a replica behind its peer cannot hide a sample and the replicas' copies collapse to one. The read cost multiplies by the replica count |
| a replica not answering | `skip_unavailable_shards = 0` fails the run; no run row; the next run retries the window |
| coarser reads | the builder's local source level, only inside its local run rows (§3.2) |
| a builder down | its shard's run rows stop. That shard's coverage ends, so every query reads finer levels or raw after it (`reading.md` §3) |
| `max_threads` | in the view's statement, so on every replica with no server configuration |

`each_shard_builds_its_slice_once` (`crates/pulsus-schema/tests/live_cluster.rs:2025`) checks one row per bucket per shard and a push counted once with one replica's fetches stopped. It runs in CI's cluster job; no figure here comes from a cluster.

## 5. Lower priority than inserts and queries

A dashboard read (1,000 series, 6 hours of raw, grouped by minute) and an insert of 200,000 rows, 8 of each, with two concurrent rollup statements over a two-hour window running in a loop:

| rollup statements carry | read, median ms | insert, median ms | one rollup statement, median ms |
|---|---:|---:|---:|
| no rollup running | 91 | 42 | — |
| nothing | 324 | 123 | 445 |
| `priority = 10` | 316 | 138 | 451 |
| `workload` in a lower-priority workload (`CREATE RESOURCE … (MASTER THREAD, WORKER THREAD)`, `CREATE WORKLOAD`) | 354 | 192 | 450 |
| the same workload with `max_concurrent_threads = 2` | 130 | 50 | 892 |
| `max_threads = 2` | 125 | 56 | 602 |

| decision | reason |
|---|---|
| `max_threads = 2` on every rollup statement, in the schema | the read was 37 % slower than idle instead of 256 %, the insert 33 % instead of 193 %. It needs no server configuration and travels with the view to every replica |
| not `priority` | no measured effect |
| not workloads | they matched `max_threads = 2` only with a thread limit, and they are server objects every server of a cluster must carry |
| disk I/O | not measured separately: the corpus was in the page cache, so the runs were CPU-bound |

**Under sustained load.** A level falls behind only when a run takes longer than its period. With the cap, a statement over a two-hour window (1,920,000 samples; 3,839,997 rows read) took 602 ms median, about 3.2 million samples a second, and a first-level run covers one minute. So a level keeps its period until one shard's slice receives about 190 million samples a minute. Beyond that the first level falls behind by the run's excess each minute, and each query reads that much more from raw.

## 6. Measurements

One UTC day, 4,000 series: half counters resetting every 500–799 samples, 40 resetting every second sample, the rest gauges. 15 s scrape: 23,040,000 rows; 60 s scrape: 5,760,000. Sizes after `OPTIMIZE … FINAL`, from `system.parts`. Output not kept.

### 6.1 Size

| table | 15 s: bytes | of raw | 60 s: bytes | of raw |
|---|---:|---:|---:|---:|
| raw `metric_samples` | 111,140,723 | 100 % | 17,863,454 | 100 % |
| landing, earlier key | 62,734,798 | — | 15,450,115 | — |
| landing, 5-minute block key | 81,307,328 | ×1.30 of earlier | 42,207,125 | ×2.73 of earlier |
| landing, 1-minute block key | 191,252,554 | ×3.05 | 86,574,085 | ×5.60 |
| 5 s level / its projection | 421,111,914 / 314,538,008 | 379 % / +283 % | 60,002,018 / 156,032,979 | 336 % / +873 % |
| 60 s level / projection | 59,338,066 / 47,856,786 | 53.4 % / +43.1 % | 60,052,681 / 46,188,422 | 336 % / +259 % |
| 720 s level / projection | 3,095,820 / 4,451,098 | 2.79 % / +4.0 % | 2,414,327 / 4,019,628 | 13.5 % / +22.5 % |
| 7200 s level / projection | 553,069 / 626,756 | 0.50 % / +0.56 % | 497,598 / 610,841 | 2.79 % / +3.42 % |
| 86400 s level | 88,871 | 0.08 % | 87,945 | 0.49 % |
| all levels and projections | 851,660,388 | 766 % | 329,906,439 | 1,847 % |

The table predates `flags` and the projection rule of §2.2: the five-level set `5,60,720,7200,86400` no longer stores the 7200 s projection, so its total is 765.7 % at 15 s and 1,843.4 % at 60 s; the default set, 60 s first, is 103.8 % and 634.0 % (§1.3).

Landing holds 6 hours by default (`metrics_landing_hours`, `schema/schema.sh:54`), so its daily figure is four times what it stores.

| cost | figures | the alternative |
|---|---|---|
| a level at or below the scrape interval stores more than raw and saves nothing | the 60 s level at a 60 s scrape: 336 % of raw, the same rows; a 5 s level at 15 s: 379 % | choose levels above the scrape interval (§1.3) |
| the projection a level carries for the next level's run is the largest cost after the level itself | the five-level set at 15 s: the 5 s level's projection is 283 % of raw and the 60 s projection 43.1 %, of 765.7 % | build the second level from landing instead of the first level: no projection, each run reads its own window of landing, and the chain is broken at that step. Not measured |
| landing grows with its block key | ×1.30 at 15 s, ×2.73 at 60 s, of a 6-hour buffer | keep the earlier key and read 11.6 times the rows per first-level run (§3.4) |

### 6.2 Cost per run

§3.4. Every run over its normal window, one period or one bucket, took at most 128 ms and about 13 MB (output not kept). The recorded runs in `measure/projection-run/07_query_log.tsv`: the 720 s run 15–17 ms and 13,539,569 bytes; the first-level runs, one hour each rather than one minute, 589–618 ms and at most 263,947,623 bytes.

### 6.3 Why the main key stays fingerprint first

Same day, 15 s scrape, three runs, `read_rows` of a query:

| level, window | 1 series: fingerprint key / parent-block key | 100 series: fingerprint key / parent-block key |
|---|---:|---:|
| 5 s, 1 h | 2,048 / 121,856 | 124,004 / 952,420 |
| 60 s, 1 d | 2,048 / 244,736 | 245,860 / 4,869,220 |
| 720 s, 1 d | 1,024 / 23,552 | 97,380 / 420,708 |

### 6.4 What this evidence cannot see

| gap | status |
|---|---|
| cluster behaviour (§4.1) | no figure here; `each_shard_builds_its_slice_once` runs in CI's cluster job |
| sizes of sets other than levels drawn from 5, 60, 720, 7200 and 86400 s | not measured |
| per-run reads under live merges with the block keys | not measured as figures; `each_run_reads_its_window` (`crates/pulsus-schema/tests/live_rollup_levels.rs:837`) asserts that every first-level run reads less than landing holds and that every 720 s and 7200 s run that wrote data read through `by_parent` |
| disk I/O contention (§5) | not measured |
| the sizes of §6.1 with `flags` | not re-measured; `flags` alone is §1.3's 1.8 % |
