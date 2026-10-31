# PromQL rollups: reading the levels

**Built.** Part A (#742), part B (#744) and the follow-up that chooses the resolution by the query's time range (issue #18) are on `main`; every statement about code below is true of `main` with that follow-up. Tiering (#743 part A) and a level's own retention and move age (#743 part B) are on main.

The user's account of the same rule is `docs/api.md` §3.6 (`Rollup levels`, `docs/api.md:962`).

## 1. What a rollup answer is

The read path hands the engine pseudo-samples built from the level's rows, in time order, followed by the finer levels' and raw samples where the level does not cover (§3). The engine evaluates the function unchanged. What a selector's consumer reads is recorded on it by a pass after planning (`assign_rollup_use`, `crates/pulsus-promql/src/plan.rs:1402`):

| `RollupUse` | consumer |
|---|---|
| `Last` | an instant selector (so also `timestamp`), `last_over_time`, `present_over_time`, `absent_over_time` |
| `Max`, `Min`, `Sum`, `Avg` | `max_over_time`, `min_over_time`, `sum_over_time`, `avg_over_time` |
| `Counter` | `rate`, `increase` |
| none | every other function: the selector reads raw |

The points each level row gives (`level_points`, `crates/pulsus-read/src/metrics/rollup.rs:673`; `counter`, `crates/pulsus-read/src/metrics/rollup.rs:717`):

| `RollupUse` | points from one level row |
|---|---|
| `Last` | `(t_last, last)` when `t_last > 0`; then `(t_end, stale marker)` when `flags & 1` |
| `Max`, `Min` | `(t_end, max)` or `(t_end, min)` when `count > 0`; else `(t_end, last)` when `flags & 2`, every value being NaN and `last` the last of them, its payload kept; else none |
| `Sum`, `Avg` | `(t_end, NaN)` when `flags & 2`; else `(t_end, sum)` or `(t_end, sum / count)` when `count > 0`; else none |
| `Counter` | a counter `c` that never resets: the first row's `last`, then plus each later row's `inc`; the point `(t_last, c)`. At levels of 720 s and more, also `(t_end − res + 1, c − inc)` before it, one millisecond after the bucket's start. A row with `t_last = 0` gives no point and still adds its `inc`; a row whose `last` is NaN gives `(t_last, last)`, adds its `inc` and leaves `p` |

A counter's raw samples after a level row continue `c` at levels of 720 s and more: each adds `v − p` when `v ≥ p`, else `v`, `p` being the previous raw value or the last level row's `last`; a NaN or a native-histogram sample passes through as itself. At a level under 720 s they do not: the raw samples go to an overlay (`RawOverlay`, `crates/pulsus-promql/src/value.rs:254`), and a `rate` or `increase` window that reaches into time no level covers is evaluated over the overlay's samples alone, which are the raw route's own (`o.touches`, `crates/pulsus-promql/src/eval/mod.rs:2314`). So at those levels a window touching raw answers exactly as without rollups.

Stale markers and NaN read as raw for the bit-equal functions: a routed `Last` selector is evaluated only at bucket ends (§2), so a stale marker's exact time inside its bucket never decides an answer, and a marker placed at `t_end` gives what raw gives there; range functions drop markers, so only the last non-stale sample matters to them.

### 1.1 Distance from raw, measured on the engine

`each_served_function_is_within_its_distance` (`crates/pulsus-read/tests/live_metrics_rollup.rs:870`): corpus D, 260 series over 8 days at a 15 s scrape, built at every level of the five-level set `5,60,720,7200,86400`; each function sent with routing on and off; per level `res`, step `res`, over the last day at 60 s, the last 3 days at 720 s and all 8 days at 7200 s and 86400 s; `rate`, `increase` over `5 × res`, the others over `res` and `5 × res`. `d = |a − b| / |b|` (`|a|` when `b = 0`; 1 when only one answer is present); p99 by nearest rank. Output: `measure/results/live-metrics-rollup.txt`. Run before the levels followed the query's time range and before the 5 s default, so each level was reached by its step; the windows a level serves are the same bucket-end windows today.

Bit-equal at every level and on every series kind: `max_over_time`, `min_over_time`, `last_over_time`, `present_over_time`, `absent_over_time`, `timestamp` and the bare selector. `sum_over_time` and `avg_over_time` differed by at most 2.39e-13 (addition order).

`rate` and `increase` gave the same figures, p99 (maximum in brackets), beside the bound the test holds them to:

| counters | 60 s, `[5m]` | 720 s, `[1h]` | 7200 s, `[10h]` | 86400 s, `[5d]` |
|---|---|---|---|---|
| normal (100 series) | 0.0110 (0.0558); bound 0.0218 | 0.00415 (0.00420); bound 0.00828 | 0.000413 (0.000418); bound 0.000826 | 0.0000345 (0.0000348); bound 0.0000684 |
| reset every second sample (20) | 0.000573 (0.000573); bound 0.2 | 0.00400 (0.00401); bound 0.00826 | 0.000399 (0.000416); bound 0.000822 | 0.0000346 (0.0000346); bound 0.0000686 |
| stopping (20) | 0.0179 (1.0); bound 0.0218 | 0.0850 (0.524); bound 0.141 | 0.0996 (0.101); bound 1.0 | 0.0515 (0.0515); bound 1.0 |
| missing 30 % of scrapes (20) | 0.0693 (0.198); bound 0.42 | 0.0138 (0.0138); bound 0.049 | 0.00131 (0.00131); bound 0.00494 | 0.000109 (0.000109); bound 0.000368 |

The bounds are the published figures of `docs/api.md` §3.6: twice the larger of the Python port's p99 (`measure/partB-ports/bounds.py`) and the earlier design measurement, rounded up. The test's bound for `avg_over_time` on gauges (0.0057 at 60 s, 0.0214 above) is wider than what corpus D gives, because each corpus D series has its own phase inside a scrape interval and no sample sits on a bucket edge. With every sample on an edge, jitter moves samples between buckets, and `avg_over_time`'s p99 measured 0.358 at 60 s, 0.595 at 720 s, 0.0557 at 7200 s and 0.0246 at 86400 s; output not kept.

**A counter that stops or goes stale near a level's newest bucket.** `a_window_touching_raw_is_raw` (`crates/pulsus-read/tests/live_metrics_rollup.rs:1118`) cuts the 720 s level's run rows at `W` and compares `rate(m[1h])` at a 12 m step, step by step. For a counter that ends in a stale marker, steps just after `W` differed from raw by up to 3.75 % (at `W + 48 min`; the same step with the level uncut, 17.6 %). Counters that keep counting are unaffected. The owner accepted this figure; `docs/api.md` §3.6 states it. Output: `measure/results/live-metrics-rollup.txt`.

**Two other differences, both in `docs/api.md` §3.6.** From the 720 s level and coarser, a `rate` or `increase` window holding one sample of a counter has a value where raw has none, when a level row serves that sample: the row's start point and its sample are two points. A step between two samples more than 24 hours apart is counted by no level (the predecessor rule of `building.md` §3.1): a counter sampled at 23:59:30 and next at 00:00:00 a day later reads `increase(x[5d])` 0 from the 86400 s level at that midnight, where raw reads 45. `stale_and_nan_read_as_raw` (`crates/pulsus-read/tests/live_metrics_rollup.rs:1785`) asserts that counter's routed and raw values at every level exactly: absent at 60 s on both routes, 0 routed and absent raw at 720 s and 7200 s, 0 routed and 45 raw at 86400 s.

**Why the start point is one per row.** One start point per window, at `max(bucket start, lo) + 1`, against one per row: equal p99 and maximum on normal and sparse counters at 720 s and 7200 s; on stopping counters per row is closer, p99 0.0704 against 0.125 at 720 s and a 15 s scrape, 0.0634 against 0.118 at a 60 s scrape, equal at 7200 s (`measure/partB-ports/startpoints.txt`).

#### Port: counters, and the rejected last-only rule

`rate`, relative difference from the raw answer, p99. 100 counters resetting every 500–799 samples ("normal") and 20 resetting every second sample ("fast reset"), 14 days; window `5 × res`, except 60 s for the 5 s level. Beside each figure, the rejected rule of reading each bucket's `last` only. Script and output: `measure/counter_distance.py`, `measure/results/counter_15s.txt`, `measure/results/counter_60s.txt`.

| level, range | 15 s scrape, normal | 15 s, fast reset | 60 s scrape, normal | 60 s, fast reset |
|---|---|---|---|---|
| 5 s, 1 m | exact (last-only: exact) | exact | exact | exact |
| 60 s, 5 m | 1.07 % (last-only 14.2 %) | 9.98 % (last-only 100 %) | p99 0, worst 30.1 % (last-only 33.3 %) | exact |
| 720 s, 1 h | 0.414 % (last-only 23.6 %) | 0.413 % (last-only 100 %) | 1.67 % (last-only 19.5 %) | 1.69 % (last-only 100 %) |
| 7200 s, 10 h | 0.0413 % (last-only 84.9 %) | 0.0411 % | 0.165 % (last-only 28.0 %) | 0.166 % |
| 86400 s, 5 d | 0.0034 % (last-only 100 %) | 0.0034 % | 0.0137 % (last-only 90.7 %) | 0.0138 % |

The port places one start point per window, at its first row (`inc+start`); the read places one per row, which measured the same on these counters. The start point is used only from 720 s (§1). At 5 s and 60 s it overstated by up to a scrape interval: p99 33 % at 5 s (15 s scrape) and 22 % at 60 s (60 s scrape). Without it those levels are as above.

#### Port: gauges

60 s and 720 s levels, windows aligned as §2 requires, 4,000 series over one day, level rows built before `flags` and the stale-marker rules of `building.md` §3.1. Script and output: `measure/gauge_distance.py`, `measure/results/gauge_15s.txt`, `measure/results/gauge_60s.txt`.

| function | 15 s scrape | 60 s scrape |
|---|---|---|
| `last_over_time`, instant selector, `max_over_time`, `min_over_time` | exact in every window | exact in every window |
| `sum_over_time` | addition order only, at most 3.06e-14 | exact at 60 s; at most 1.29e-15 at 720 s |
| `avg_over_time` | a mean of bucket means: p99 0.29 % at 60 s, 1.07 % at 720 s | exact at 60 s; at most 2.46e-15 at 720 s |

The 7200 s and 86400 s levels were not measured for gauges in the port; the 5 s level is measured by B2 on corpus E. `last`, `max` and `min` compose exactly up the chain, so they are exact on windows ending on bucket ends, which is every window the grid evaluates; `sum` differs from raw only in the order of addition, within 1e-12.

### 1.2 Kept on raw

| function | why |
|---|---|
| `irate`, `delta`, `resets` | measured from the 720 s level, 15 s scrape: `irate` p99 81 %, `delta` median 94 %, `resets` missed in 774 of 11,600 windows (`measure/results/gauge_15s.txt`) |
| `idelta`, `changes`, `count_over_time`, `quantile_over_time`, `stddev_over_time`, `stdvar_over_time`, `mad_over_time`, `first_over_time`, `ts_of_*`, `deriv`, `predict_linear`, `double_exponential_smoothing` | no column carries what they read. Not measured |
| native-histogram samples | the levels hold floats; a routed selector's native-histogram series are read raw over the whole window, beside the level's rows for its float series |

## 2. Choosing the level

The binary reads the levels at start and then every 60 s (`spawn_level_loop`, `LEVEL_REFRESH`, `crates/pulsus-read/src/metrics/rollup.rs:173`), and, clustered, the shard count; a failed read keeps the last good list, and nothing routes before the first success (`refresh_levels`, `crates/pulsus-read/src/metrics/rollup.rs:128`). Each level's width, switch point and the counter bucket count come from the comment the schema build wrote (`building.md` §2.2; `discovery_sql`, `crates/pulsus-read/src/metrics/rollup.rs:80`); the counter bucket count is the first row's:

```sql
SELECT name, toUInt32(extract(comment, 'rollup_res_ms=(\\d+)')) AS res_ms,
       toUInt64(extract(comment, 'rollup_from_ms=(\\d+)')) AS from_ms,
       toUInt32(extract(comment, 'counter_buckets=(\\d+)')) AS counter_buckets
FROM system.tables
WHERE database = {db} AND startsWith(comment, 'rollup_res_ms=') AND engine != 'Distributed'
ORDER BY res_ms
```
```sql
SELECT toUInt32(uniqExact(shard_num)) AS shards FROM system.clusters WHERE cluster = {cluster}
```

| # | rule |
|---|---|
| R1 | **Resolution.** For a range query, `res` is the largest level whose switch point is at most `end − start`, the widest level any selector may read; below the smallest level's, every selector reads raw. Defaults: levels 60, 720, 7200 and 86400 s, switch points 3 h, 16 h, 7 d and 90 d (`resolution`, `crates/pulsus-read/src/metrics/rollup.rs:214`) |
| R2 | **Grid.** The request's own times; when they are not bucket ends, the query is evaluated at the bucket ends and each step takes the bucket that holds it, the steps after the last whole bucket at their own times (owner, 2026-10-31) (`lattice`, `crates/pulsus-read/src/metrics/rollup.rs:300`; the step loop of `evaluate_counted_with`, `crates/pulsus-promql/src/eval/mod.rs:305`) |
| R2a | **Rounding.** At the request's own steps: in a range query, a selector that is eligible, is read by `rate` or `increase` and has no level under `route` is rounded. Of the levels of width at most `res` whose time fits (G2): the largest `w` with `⌈r / w⌉ ≥ min_buckets(w)` (R2b) is read over `⌈r / w⌉ × w`; else `min_buckets(w) × w` of the smallest; else raw, unchanged. The rounded selector then routes under the unchanged `route` (`round_counter_range`, `crates/pulsus-read/src/metrics/rollup.rs:236`). A rounded `increase` is answered as `increase(x[r']) × r / r'`, floats and native histograms alike, and a rounded `rate` is not scaled, so `increase` stays `rate × r`. A rounded selector marks the answer `exactness: "rollup"`, even when only raw served it. The range asked is kept as `written_range_ms` |
| R2b | **Bucket-end range.** At the bucket ends a selector with range `r` is read over `n × L`, `n = max(⌈r / L⌉, min_buckets)`, never shorter than asked; `min_buckets` is `counter_buckets` for `rate` and `increase` at a level under 720 s and one otherwise, in `fits`, `round_counter_range` and here (`lattice_range`, `crates/pulsus-read/src/metrics/rollup.rs:292`; `min_buckets`, `crates/pulsus-read/src/metrics/rollup.rs:272`). `increase` and `sum_over_time` are scaled back to `r` |
| R3 | **Per selector**, the level read is the largest of width at most `res` that passes G1 to G3; none means raw (`route`, `crates/pulsus-read/src/metrics/rollup.rs:355`). G2 decides per selector whether the grid's times are a level's bucket ends |
| R4 | **Any resolution.** The route is decided before the pushdown (`routed_series`, `crates/pulsus-read/src/metrics/exec.rs:4614`). A selector under a pushed `sum`, `avg`, `count`, `min` or `max` over `rate` or `increase`, alone or in a one-to-one match of two range functions (issue #579 part 4), whose route is the 60 s level, is not read by this route: its node reads the level's coverage first (`level_coverage`, `crates/pulsus-read/src/metrics/exec.rs:2480`) and runs in the database over the level rows and the raw samples around what they do not cover, falling back to this route; a coarser route declines the node. How a selector's series were found does not matter (`eligible`, `crates/pulsus-read/src/metrics/rollup.rs:411`): one the label cache could not answer first reads its series and labels by the statement the raw fallback hydrates by, under the same settings, then reads as §3; its native-histogram read is over the same series statement (`fetch_rollup`, `crates/pulsus-read/src/metrics/exec.rs:2770`) |
| R5 | **Instant query** (`step = 0`): `res` is the smallest level, G2 applies to its time, and nothing is rounded |
| R6 | **Setup only.** The levels, switch points and counter bucket count are set when the schema is built, validated by `schema/schema.sh` and `pulsus_schema` with the same messages (`building.md` §1.2), and stored in each level table's comment; the binary reads them from there and has no setting for them |

| guard | rule (`fits`, `crates/pulsus-read/src/metrics/rollup.rs:375`; `excluded`, `crates/pulsus-read/src/metrics/rollup.rs:401`) |
|---|---|
| G1 range | a range function's range is a whole multiple of the width and at least one width, `counter_buckets` widths for `rate` and `increase` at a level under 720 s; otherwise a finer level that meets it, else raw. So `rate(x[1h])` at a 1 h step on 30 days reads the 720 s level, five 12-minute buckets, and `rate(x[5m])` reads the 60 s level |
| G2 time | the window's end, the grid time minus `offset` (or the `@` time minus `offset`), is a bucket end of the level; otherwise a finer level whose width divides it, else raw. `x offset 90s` on a 7-day query reads raw at the default levels: its window ends fall 30 s past a minute |
| G3 kind | `rollup_use` is set (§1); not in a subquery, `anchored` or `smoothed`, `info()`'s selector, or under a histogram-statistics function; the selector names its metric and carries no `__name__` matcher beside the name |

The published distances (§1.1, `docs/api.md` §3.6) were measured at `counter_buckets` = 5 and hold for that value.

## 3. Coverage and freshness

Per fingerprint chunk (`rollup_chunk`, `crates/pulsus-read/src/metrics/exec.rs:2905`), with `todo` starting as the selector's fetch window:

1. For the chosen level, then each finer level from coarsest to finest, while `todo` is not empty: level fetches over the intervals of `todo`, at most 64 to a statement, in order and never merged (`MAX_STATEMENT_INTERVALS`). Their run rows give one coverage `C`; the data rows with `t_end` inside `C ∩ todo` are kept; `C` is taken out of `todo`.
2. Raw reads over the intervals of what remains, at most 64 to a statement, with the raw route's statement and the intervals `OR`ed (`sample_fetch_within`, `crates/pulsus-read/src/metrics/sample_sql.rs:123`); samples outside what remains are dropped. For a counter at a level under 720 s, each remaining interval is first widened by the range on both sides, inside the window, so the overlay holds every raw sample a touching window needs (`let read = if over`, `crates/pulsus-read/src/metrics/exec.rs:2985`).
3. Per series, every kept level row and raw sample in time order, a level row at its `t_end`, assembled into points (§1, `assemble`, `crates/pulsus-read/src/metrics/rollup.rs:622`).

The selector's native-histogram samples are read raw over the whole window beside the chunks (`build_hist_chunk_sqls`, `crates/pulsus-read/src/metrics/exec.rs:4913`) and merged as the raw route merges them. Every row a level fetch returns, run rows included, is charged to the query's sample budget as it drains.

The level fetch over one interval `(lo, hi]` (`level_fetch`, `crates/pulsus-read/src/metrics/rollup.rs:417`), `{shard}` being `_shard_num` clustered and `toUInt32(1)` on one node, `{table}` the level's table with the distributed suffix when clustered:

```sql
SELECT {shard} AS shard, kind, fingerprint, t_end, t_last, last, min, max, sum, count, inc, flags
FROM {table}
WHERE (kind = 0 AND org_id = {tenant} AND fingerprint IN ({fps}) AND t_end > {lo} AND t_end <= {hi})
   OR (kind = 1 AND org_id = '' AND fingerprint = 0 AND t_end > {lo} AND t_last < {hi})
ORDER BY kind, fingerprint, t_end
```

Over more intervals, each half's time condition is the `OR` of one condition per interval: `((t_end > a1 AND t_end <= b1) OR (t_end > a2 AND t_end <= b2))` and `((t_end > a1 AND t_last < b1) OR …)`.

| rule | |
|---|---|
| why one statement | a distributed read answers each shard from one replica, and a run row is in the same part as its data (`building.md` §2.3). So a run row in the answer proves that replica returned that run's data. Two statements could be answered by two replicas, and coverage from one could admit data the other lacks |
| coverage `C` | for each shard of the discovered count, the union of its returned run rows' `(t_last, t_end]`, intersected over shards. A shard that returned no run row covers nothing (`coverage`, `crates/pulsus-read/src/metrics/rollup.rs:494`) |
| with the levels built in order | `(lo, W_L]` from `L`, `(W_L, W_K]` from each finer level `K`, and the newest part from raw, so every refresh ends on the newest raw sample |
| history before a level's first run, and any hole | read from finer levels or raw |

The property this rests on was measured on the development cluster while an earlier rollup design was prototyped: with fetches stopped on one replica of a shard, a read through the distributed table on that replica returned neither the rows nor the coverage row of a block inserted on its peer. That output was not kept. `a_run_row_is_read_with_its_data` (`crates/pulsus-read/tests/live_metrics_rollup_cluster.rs:293`) checks it for this read in CI's cluster job.

How much of a window its level does not serve, default set, levels up to date (`building.md` §3.3); a range under 3 h reads raw whole:

| query range | level | after its watermark, at most | of which raw, at most | share of the window |
|---|---|---:|---:|---:|
| 3 h | 60 s | 270 s | 270 s | 2.5 % |
| 16 h | 720 s | 960 s | 270 s | 1.7 % |
| 7 d | 7200 s | 7,500 s | 270 s | 1.2 % |
| 90 d | 86400 s | 86,760 s | 270 s | 1.1 % |

### 3.1 Rows read

The same B2 run recorded `read_rows` of both routes per query (`measure/results/live-metrics-rollup.txt`). For the normal counters' `rate` and the gauges' `avg_over_time`:

| level | `rate(c_normal[5 × res])`: routed / raw | `avg_over_time(g[res])`: routed / raw |
|---|---:|---:|
| 60 s, 1 day | 151,552 / 1,163,264 | 157,696 / 1,156,608 |
| 720 s, 3 days | 37,888 / 2,326,528 | 50,140 / 2,316,800 |
| 7200 s, 8 days | 588,863 / 4,653,056 | 24,936 / 4,645,376 |
| 86400 s, 8 days | 10,280 / 4,653,056 | 1,305 / 2,896,896 |

The routed reads include the finer levels and raw after each level's watermark.

## 4. How users see the levels

### 4.1 The user documentation

`docs/api.md` §3.6 states §2's rule in users' terms, the table of §1, the distances of §1.1, freshness, and the largest level each query range may read with the defaults; that table is there, not here.

### 4.2 From a running server

`GET /api/v1/status/runtimeinfo` carries `rollupLevels`: the discovered levels in duration form, smallest first (`rollup_levels`, `crates/pulsus-server/src/prom_api/encode.rs:934`; `status_runtimeinfo`, `crates/pulsus-server/src/prom_api/handlers.rs:753`). `["1m","12m","2h","1d"]` for the default; `[]` when none are built and before the first discovery.

| candidate | verdict |
|---|---|
| `runtimeinfo` | chosen: it already reports a fact the binary reads from the database, `storageRetention`, and clients that do not know the new field ignore it |
| `status/flags` | not chosen: it is the map of the binary's own flags, and the levels are not a flag of the binary |
| `status/config` | not chosen: it is the binary's configuration file, and the levels are not in it |
| a new endpoint | not chosen: one more path to document where an existing one fits |

### 4.3 Explain

With `X-Pulsus-Explain: 1`, a routed selector has:

| stage | note |
|---|---|
| `rollup_series`, for a selector the label cache could not answer | the statement that reads its series and labels (`rollup_series_sql`, `crates/pulsus-read/src/metrics/exec.rs:2758`) |
| `rollup_route` | `<table>, use <RollupUse>, step <s>, range <r>`, and ` (asked <written>)` after the range when it was rounded (`push_rollup_route`, `crates/pulsus-read/src/metrics/exec.rs:4318`) |
| `hist_sample_fetch` | the selector's native-histogram read, when it has one |
| `rollup_piece`, one per interval served, in time order, from the first chunk | `<table>.<column> served (a, b]`, `raw (a, b]`, or `<table> served nothing`, each with its statement |

`exactness` is `"rollup"` when the query's steps take the value of the bucket that holds them (R2), when a selector's range was rounded (R2a), even when only raw served it (`e.rollup |=`, `crates/pulsus-read/src/metrics/exec.rs:1601`), or when a kept level row served the selector in some chunk (for `Max`, `Min`, `Sum` and `Avg`, a row with `count > 0`) and, for a counter under 720 s, some step's window touches no interval read from raw (`level_window_exists`, `crates/pulsus-read/src/metrics/rollup.rs:776`); otherwise `"raw-exact"` (`exactness: if e.rollup`, `crates/pulsus-server/src/prom_api/encode.rs:281`).

## 5. What this evidence cannot see

| gap | status |
|---|---|
| clustered reads (§3) | no figure here; `a_run_row_is_read_with_its_data` runs in CI's cluster job |
| §1.1's distances come from one synthetic corpus at a 15 s scrape, and corpus E at 5 s for the 5 s level, phases off the bucket edges | the edge case is the measured `avg_over_time` figure above; other scrape intervals were measured only in the ports |
| an ordinary NaN in a counter: raw `rate` reads NaN when the window's first or last value is NaN, the level reads a number | stated in `docs/api.md` §3.6; not measured |
| the cost of a routed query at production size | §3.1 is corpus D only |
