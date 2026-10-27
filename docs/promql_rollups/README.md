# PromQL rollups: design

**Built.** Part A (#742), part B (#744) and the follow-up that chooses the resolution by the query's time range (issue #18) are on `main`; every statement about code in these documents is true of `main` with that follow-up. Tiering of the raw tables (#743 part A) is on main; a level's own retention and move age (#743 part B) are not started.

A rollup level is a table of per-series bucket summaries of one width, built on a timer from the level below it, and read instead of raw samples at the resolution chosen by the query's time range. Its answers approximate the raw answer, by amounts measured per function and level.

| document | subject |
|---|---|
| `docs/promql_rollups/README.md` | the decisions, the parts, the rejected alternatives |
| `docs/promql_rollups/building.md` | the setting, the tables, the statements that build each level, their timers, cost and failures |
| `docs/promql_rollups/reading.md` | what a rollup answer is and how far it is from raw, choosing a level, coverage and freshness, what users see |
| `docs/promql_rollups/tests.md` | the tests on `main` that hold each rule |
| `docs/promql_rollups/measure/README.md` | the scripts and recorded outputs behind the figures, and which outputs were not kept |

The user's account is `docs/api.md` §3.6 and the setting's is `docs/configuration.md` §7. Server figures were measured on one development server with 16 cores; the port figures come from Python ports. No figure was measured on a cluster: the cluster tests run in CI's cluster job (`tests.md`).

## 1. Decisions

"Owner" marks a decision the owner took; the others follow from them.

| # | decision | owner | specified in |
|---|---|---|---|
| D1 | A rollup-served answer approximates the raw answer. The column a function reads is chosen by what the data is: a counter's increase, a gauge's last value, minimum, maximum or sum. Every other function reads raw, unchanged | owner | `reading.md` §1 |
| D2 | The levels are chosen per deployment when the schema is built: `PULSUS_ROLLUP_LEVELS`, a list of seconds read by `schema/schema.sh` and the crate's renderer | owner | `building.md` §1 |
| D3 | The default is `5,60,720,7200,86400` | owner (2026-10-26) | `building.md` §1.3 |
| D4 | One table per level, each with its expiry in its own `CREATE TABLE`, so each level can carry its own retention and storage placement. One data row per series and bucket, `kind = 0, t_end, t_last, last, min, max, sum, count, inc, flags, built_at`, and one run row per run. No metric type is needed to build a row | owner (one table per level) | `building.md` §2 |
| D5 | The levels form a chain: the first is built from raw, each other from the one below, each by a refreshable view on its own timer, with the timer and offset derived from the level sizes. A level builds only inside its source's run rows | owner (chain, timers) | `building.md` §3 |
| D6 | A run reads only its own window | — | `building.md` §3.4 |
| D7 | A selector reads the level chosen by the query's time range (`reading.md` §2) inside that level's coverage, and the rest of its window from finer levels, then raw, so every chart refresh shows the newest samples | owner (newest samples) | `reading.md` §2, §3 |
| D8 | Rollup building runs at a lower priority than inserts and queries: every rollup statement carries `max_threads = 2`, in the schema | owner (lower priority) | `building.md` §5 |
| D9 | Users can see the levels: in the user documentation, in `runtimeinfo` and in explain | owner | `reading.md` §4 |
| D10 | Native histograms read raw. Classic histogram buckets are counters | — | `reading.md` §1.2 |
| D11 | A level's coverage is the union of its run rows' intervals, intersected over shards, read in the same statement as its data. Nothing outside it is read from the level | — | `reading.md` §3 |
| D12 | The first level drops exact repeats and orders values at one millisecond by value bits, as the raw fetch does | — | `building.md` §3.2 |
| D13 | A level row keeps the bucket's last sample that is not a stale marker, and `flags` records a stale end and a NaN, so the bit-equal functions read stale markers and NaN as raw | — | `building.md` §2.2, `reading.md` §1 |
| D14 | The first level's predecessor for `inc` is the previous sample that is not a stale marker, keyed by `(org_id, fingerprint)`, taken from the level's own table when the run holds none, and not used when more than 24 hours old | — | `building.md` §3.1 |
| D15 | At a level under 720 s, a `rate` or `increase` window that reaches into time no level covers is computed wholly from raw | — | `reading.md` §1 |
| D16 | A level carries the projection its next level's run reads only when that next level is shorter than a day | — | `building.md` §2.2 |
| D17 | The query ranges at which the resolution switches to each level, and the counter bucket count, are set when the schema is built and stored in each level table's comment, where the server reads them | owner (2026-10-26) | `building.md` §1, `reading.md` §2 |

**How the engine is meant to be used, and what differs.** The engine's ordinary rollup is an insert-time materialized view into an aggregating table, as `log_metrics_<res>` is built (`schema/schema.sql:568`, `log_metrics_{{log_rollup_suffix}}_mv`). The levels use refreshable views on timers instead, as `service_edges_mv` is built (`schema/schema.sql:813`). That follows the owner's decision on timers. What it costs, and how the cost is kept down, is `building.md` §3.4 and §5.

## 2. Parts

Each part was its own plan, review, branch and pull request inside issue #18.

| part | what it built |
|---|---|
| A (#742) | `PULSUS_ROLLUP_LEVELS` and its validation in `schema/schema.sh` and `pulsus_schema::parse_rollup_levels`; the level template (`schema/schema.sql:965-1090`), its prefixes, run rows and derived timers; `metric_landing`'s block key; the removal of the earlier design's `metric_rollup` table, its plan table, their views and wrapper, the settle constant and the window algebra's reset cap |
| B (#744) | discovery, eligibility and the level choice; the one-statement level fetch and its coverage; the finer levels and raw after a level; the points of `reading.md` §1 and the raw overlay; `rollupLevels` in `runtimeinfo`; the explain stages and `exactness`; `docs/api.md` §3.6. In part A's first-level statement: the predecessor of D14, and `flags` with the last non-stale sample of D13 |

The window algebra of issue #18 part 3 (#740) stays as the raw functions' arithmetic (`summarize_counter`, `crates/pulsus-promql/src/eval/functions.rs:187`), and the raw fetch's value-bits order (`reinterpretAsUInt64`, `crates/pulsus-read/src/metrics/sample_sql.rs:99`), which D12 builds the first level in.

## 3. Rejected alternatives

| alternative | why not |
|---|---|
| answers identical bit for bit to the raw route, from ordered bucket summaries with reset lists and a raw fallback for every inexact piece | replaced by the owner's decision for approximate answers chosen by data type. Its table and views (#741) were removed by part A |
| one table holding every tier, with a `res` column | one table cannot carry a separate expiry and storage placement per tier |
| tier resolutions fixed in the schema file | replaced by `PULSUS_ROLLUP_LEVELS`: the right levels depend on the deployment's scrape interval (`building.md` §1.3) |
| rate from each bucket's last value only | measured far worse than the reset-corrected `inc` column: up to 100 % on a counter that resets often (`reading.md` §1.1) |
| at a level under 720 s, thinning the raw samples after a level to one per bucket, so a window across the watermark reads like the level | it kept normal counters at the level's distance (p99 0.0109 for every watermark position) but moved counters that stop near the watermark away from raw: for windows ending five minutes after it, p99 0.228 and maximum 0.617, where the raw samples as read give 0, and altered the answer of a window wholly in raw. Chosen instead: such a window is read wholly from raw, exact by construction (`measure/partB-ports/tail.txt`, `tail_stop.txt`) |
| at a level under 720 s, folding the raw samples after a level into the counter as read | windows ending one to four minutes after the 60 s level's watermark were 13.7 % to 17.0 % from raw (p99) on normal counters (`measure/partB-ports/tail.txt`). At 720 s and above the same fold showed no such step (`measure/partB-ports/tail720.txt`), so it is kept there |
| keeping a bucket's final sample, and adding three columns for its last non-stale sample and its NaN | the same answers as `flags` at +24.9 % of the 60 s level's bytes, against +1.8 % (output not kept) |
| keeping the functions a stale marker or NaN affects on raw | the bit-equal functions would lose every level |
| a projection on the level below a day level | the day run reads its source's day partition whole either way (`building.md` §3.4) |
| one start point per window, at its first row | measured the same as one per row on normal and sparse counters, and further from raw on stopping counters (`reading.md` §1.1) |
| `irate`, `delta`, `resets` from a level | measured: `irate` p99 81 % at the 720 s level, `delta` median 94 %, and `resets` misses resets (`reading.md` §1.2) |
| a parent-block main key on each level | every query would read 4 to 120 times more rows (`building.md` §6.3); the block key is a projection that only the runs read |
| `DEPENDS ON` between the level views | it orders views on one server only; the run rows order the runs by their data on one server or many |
| `priority`, or server workloads, for lower priority | `priority` had no measured effect; workloads matched `max_threads = 2` only with a thread limit, and are server objects each server of a cluster must carry (`building.md` §5) |
| reading coverage and data in separate statements | the two can come from different replicas, so coverage could authorise data the answer lacks (`reading.md` §3) |
| a new status endpoint for the levels, or `status/flags`, or `status/config` | `runtimeinfo` already reports a fact the binary reads from the schema (`reading.md` §4.2) |
| LTTB, stored or applied to responses | it selects points and is not an aggregate, so no PromQL function can be computed from it |

## 4. Later

Each level's expiry is its own table's TTL, rendered from `{{retention_days}}`, the raw retention. The levels stay on the local volume; a level's own retention and move age are #743 part B, not started.
