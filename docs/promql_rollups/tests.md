# PromQL rollups: tests

**Built.** Part A (#742), part B (#744) and the follow-up that chooses the resolution by the query's time range (issue #18) are on `main`; every test below is on `main` with that follow-up. Tiering and a longer retention per level (#743) are not started.

"Live" tests run against one server when `PULSUS_TEST_CLICKHOUSE=1`; "cluster" tests run on the two-shard fixture in CI's cluster job. The last run of `crates/pulsus-read/tests/live_metrics_rollup.rs` recorded during part B's work, nine tests, passed (`measure/results/live-metrics-rollup.txt`); it was not re-run on the merged commit. No cluster run is recorded here.

## Part A: building

| test | kind | what it holds |
|---|---|---|
| `the_rollup_levels_render_from_one_template` (`crates/pulsus-schema/tests/schema_file.rs:2510`) | unit | for the default set, `5,60,720,7200,86400` and `720,86400`, single node and clustered: one table, wrapper and view per level; each table's comment and TTL; a projection keyed by the next level exactly where the next level is shorter than a day; each view's timer as `building.md` §3.3 computes it; the first view reads `metric_landing` with `DISTINCT`, each other the level below; every run has its run-row branch, `max_threads = 2` and the block condition |
| `the_script_renders_exactly_what_this_crate_renders` (`crates/pulsus-schema/tests/schema_file.rs:460`) | unit | the script's render and the crate's are equal, level sets included |
| `the_script_and_the_crate_refuse_the_same_level_lists` (`crates/pulsus-schema/tests/schema_file.rs:2702`) | unit | each invalid list of `building.md` §1.2 is refused by both, the script with exit `2`, with the same message |
| `a_level_is_its_aggregates` (`crates/pulsus-schema/tests/live_rollup_levels.rs:579`) | live | the 60 s rows equal the aggregates and `inc` computed in the test from the landed samples, exact repeats dropped and values at one millisecond in value-bits order; each later level equals its aggregates from the level below |
| `an_empty_window_moves_the_watermark` (`crates/pulsus-schema/tests/live_rollup_levels.rs:678`) | live | a run over a window with no samples writes its run row and no data row, and the watermark moves past it |
| `a_hole_is_skipped_at_every_level` (`crates/pulsus-schema/tests/live_rollup_levels.rs:723`) | live | a stop longer than landing holds leaves a hole in the first level's run rows; every later level leaves unbuilt exactly the buckets the hole touches and builds again from the first bucket wholly covered after it; every refresh moves the watermark or finds no work |
| `each_run_reads_its_window` (`crates/pulsus-schema/tests/live_rollup_levels.rs:837`) | live | every first-level run reads less than landing holds; every 720 s and 7200 s run that wrote data read its source through `by_parent`; the 86400 s run reads within its day partition's bound |
| `a_level_waits_for_its_source` (`crates/pulsus-schema/tests/live_rollup_levels.rs:1014`) | live | where the 60 s level covers time the 720 s level does not, the 7200 s runs build only what the 720 s run rows cover |
| `runs_split_at_midnight_and_expire_by_day` (`crates/pulsus-schema/tests/live_rollup_levels.rs:1047`) | live | a run crossing midnight leaves one run row per day; with retention 4, every row of day `T−4` is dropped and every row of `T−2` kept |
| `a_sparse_counter_keeps_its_steps` (`crates/pulsus-schema/tests/live_rollup_levels.rs:1199`) | live | per tenant, every 60 s row's `inc` equals the increase computed from all landed samples, for a sparse tenant and a dense one on the same fingerprints; a predecessor exactly 86,400,000 ms before a sample is used, one 86,400,001 ms before is not, and two samples 25 h apart inside one run count no step |
| `stale_markers_and_nan_at_the_levels` (`crates/pulsus-schema/tests/live_rollup_levels.rs:1392`) | live | `last`, `t_last` and `flags` for a stale end, a stale-only bucket, NaN payloads, and counters resuming across a stale marker, at every level; `inc` counts the step over a stale marker and none across 24 hours |
| `each_shard_builds_its_slice_once` (`crates/pulsus-schema/tests/live_cluster.rs:1993`) | cluster | one series pushed to both shards, one sample held only by a replica that is not its shard's builder: the level has one data row per bucket, counting every sample, and each shard one run row from its builder |

## Part B: reading

| test | kind | what it holds |
|---|---|---|
| `b1b_each_selector_records_what_its_consumer_reads` (`crates/pulsus-promql/src/plan.rs:6923`) | unit | `rollup_use` per selector as `reading.md` §1's table; `in_subquery` only inside a subquery |
| `b1_the_router_chooses_the_largest_level_that_fits` (`crates/pulsus-read/src/metrics/rollup.rs:997`) | unit | with no cap on the width, the chosen level, or none, for every served and every raw-only function at each alignment and range boundary, `@` on and off the grid with the request start off and on it; the cap is T1 and T2 |
| `b1c_assemble` (`crates/pulsus-read/src/metrics/rollup.rs:1255`) | unit | a counter's points with and without start points, its continuation into raw at 720 s, and the overlay at 60 s |
| `s3_flags_and_empty_rows` (`crates/pulsus-read/src/metrics/rollup.rs:1358`) | unit | each row of `reading.md` §1's points table, for every `flags` value and `t_last = 0` |
| `b1d_coverage_intersects_every_shard` (`crates/pulsus-read/src/metrics/rollup.rs:1471`) | unit | coverage is the intersection over every shard; a shard with no run row covers nothing |
| `b1e_the_level_fetch_statement` (`crates/pulsus-read/src/metrics/rollup.rs:1514`) | unit | the level fetch's text, single node and clustered |
| `b1f_eligible_refuses_the_unrouted_classes` (`crates/pulsus-read/src/metrics/rollup.rs:1542`) | unit | no metric name, each `__name__` matcher form and the histogram-statistics functions are not eligible; whatever resolved a selector's series, it is (T3) |
| `duration_text_is_the_largest_whole_unit` (`crates/pulsus-read/src/metrics/rollup.rs:1578`) | unit | `1d`, `2h`, `12m`, `1m`, `5s` |
| `each_served_function_is_within_its_distance` (`crates/pulsus-read/tests/live_metrics_rollup.rs:830`) | live | every served function at every level within its bound, the bit-equal ones as bits (`reading.md` §1.1) |
| `a_window_reads_the_finer_levels_and_raw_after_its_level` (`crates/pulsus-read/tests/live_metrics_rollup.rs:920`) | live | with the 720 s level behind the 60 s level, a 12 h query reads 720 s, then 60 s, then raw, in that order, and ends on the newest sample |
| `a_window_touching_raw_is_raw` (`crates/pulsus-read/tests/live_metrics_rollup.rs:1083`) | live | at 60 s, every `rate` and `increase` step ending after the watermark equals raw as bits, native histograms included, and a query wholly after it is `raw-exact`; at 720 s, stopped and stale counters stay within the uncut level's distance plus 0.0218, and the counter continues into raw across a reset |
| `a_hole_reads_raw` (`crates/pulsus-read/tests/live_metrics_rollup.rs:1307`) | live | an hour with no 60 s run rows is read raw |
| `a_hole_in_the_middle_keeps_the_counter` (`crates/pulsus-read/tests/live_metrics_rollup.rs:1371`) | live | a 720 s hole inside a `rate` window is filled from 60 s, and the counter carries across it within the 720 s bounds |
| `the_unrouted_classes_read_raw` (`crates/pulsus-read/tests/live_metrics_rollup.rs:1428`) | live | each unrouted class equals routing off as bits; a metric with float and native-histogram series is served from a level for the floats; a histogram-only metric is `raw-exact` |
| `a_level_added_later_is_used` (`crates/pulsus-read/tests/live_metrics_rollup.rs:1598`) | live | a database built with `60,300,7200,86400` reads `metric_samples_300s` at a 5 m step |
| `a_sparse_counter_keeps_its_steps` (`crates/pulsus-read/tests/live_metrics_rollup.rs:1672`) | live | the sparse counters' `increase` within the sparse 60 s bound, per tenant |
| `stale_and_nan_read_as_raw` (`crates/pulsus-read/tests/live_metrics_rollup.rs:1778`) | live | at every level, stale markers and NaN payloads read as raw for the bit-equal functions; a counter resuming after a stale marker counts its step; the single-sample and 24-hour cases of `reading.md` §1.1 exactly |
| `a_run_row_is_read_with_its_data` (`crates/pulsus-read/tests/live_metrics_rollup_cluster.rs:177`) | cluster | with one replica's fetches stopped, a new run's interval is served from the level only when its data rows come in the same answer as its run row |
| `b9_exactness_is_rollup_when_a_level_served` (`crates/pulsus-server/src/prom_api/encode.rs:1723`) | unit | `"raw-exact"` and `"rollup"` |
| `b6_runtimeinfo_lists_the_rollup_levels` (`crates/pulsus-server/src/prom_api/handlers.rs:1745`) | unit | `rollupLevels` for two level sets, and `[]` before discovery |

## Follow-up: the level follows the query's range

| id | test | kind | what it holds |
|---|---|---|---|
| T1 | `the_owners_table` (`crates/pulsus-read/src/metrics/rollup.rs:821`) | unit | each row of the owner's table at a 1 s step gets its resolution, `S` equal to it and exactly its points; the 5 s rows at 15 s give 20, 60 and 120, 10 min at 15 s 40; 7 days at 5 m 84 from `start + 2 h`; 7 days at 1 d 7; 7 days less 1 ms at 5 m the 720 s level and 839; 1 h less 1 ms 5 s and 720; 24 h at 15 m `S` 1,440 s and 60; never more points than asked |
| T1b | `the_grid_does_not_overflow` (`crates/pulsus-read/src/metrics/rollup.rs:900`) | unit | grids at the ends of `i64` are refused or built without overflow; an `S` above `i64::MAX` keeps the request's grid; 11,001 daily points served, 11,002 refused |
| T1d | `any_routes_is_one_routed_selector` (`crates/pulsus-read/src/metrics/rollup.rs:920`) | unit | on the 7-day 2 h grid, `rate(x[5m])` routes, `irate(x[5m])` does not, their sum does, and nothing routes with no levels |
| T2 | `the_guards` (`crates/pulsus-read/src/metrics/rollup.rs:949`) | unit | on a 7-day grid, `rate(x[5m])` reads 60 s, `rate(x[2h])` 720 s, `max_over_time(x[2h])` 7200 s, `x offset 90s` 5 s; an instant query off every bucket end reads raw, on the 5 s one 5 s |
| T3 | `b1f_eligible_refuses_the_unrouted_classes` (`crates/pulsus-read/src/metrics/rollup.rs:1542`) | unit | `eligible` reads the selector alone |
| T4 | `a_week_reads_two_hour_rows` (`crates/pulsus-read/tests/live_metrics_rollup.rs:2047`) | live | a 7-day query at step 5 m whose series the label cache cannot answer: 84 points per series at multiples of 7200 s, the `rollup_grid`, `rollup_series` and `rollup_route` stages, bit-equal `max_over_time` and `rate` within the 7200 s bound against routing off at step 2 h |
| T5 | `the_script_and_the_crate_refuse_the_same_ranges_and_counter_buckets` (`crates/pulsus-schema/tests/schema_file.rs:2890`) | unit | each refusal of `PULSUS_ROLLUP_RANGES` and `PULSUS_ROLLUP_COUNTER_BUCKETS` with the same message on both sides; their limits accepted; ranges unread with no levels |
| T6 | `the_default_levels_carry_their_switch_points` (`crates/pulsus-schema/tests/schema_file.rs:2792`) | unit | the five default level tables and their comments, `7776000000` written as digits |
| T7 | `the_discovery_reads_the_switch_points` (`crates/pulsus-schema/tests/live_rollup_levels.rs:1542`) | live | the level discovery returns the five default levels, their switch points and the counter bucket count |
| T8 | the live part B cases above | live | each asks a range inside the band of the level it names; `each_served_function_is_within_its_distance` adds a 5 s column on corpus E, 5 s scrape, bit-equal or within 1e-12 |
| T9 | `a_raw_only_query_keeps_its_grid` (`crates/pulsus-read/tests/live_metrics_rollup.rs:2190`) | live | with the levels loaded, `irate` and `deriv` over 7 days at 5 m from a start off every grid keep the request's 2,017 points per series, bit-equal to the engine without levels, with no `rollup_grid` stage |
| T10 | `a_query_no_level_serves_on_the_changed_grid_reads_raw` (`crates/pulsus-read/tests/live_metrics_rollup.rs:2226`) | live | a query no level serves on the changed grid reads raw on its own 13 points with no route, though a level fits its own grid; an instant query reads the 5 s level within 1e-12; a range query with `start = end` reads raw, one point |
