# PromQL rollups: tests

**Built.** Part A (#742) and part B (#744) are on `main`; every test below is on `main` at `1cb423d4`. Tiering and a longer retention per level (#743) are not started.

"Live" tests run against one server when `PULSUS_TEST_CLICKHOUSE=1`; "cluster" tests run on the two-shard fixture in CI's cluster job. The last run of `crates/pulsus-read/tests/live_metrics_rollup.rs` recorded during part B's work, nine tests, passed (`measure/results/live-metrics-rollup.txt`); it was not re-run on the merged commit. No cluster run is recorded here.

## Part A: building

| test | kind | what it holds |
|---|---|---|
| `the_rollup_levels_render_from_one_template` (`crates/pulsus-schema/tests/schema_file.rs:2481`) | unit | for the default set, `5,60,720,7200,86400` and `720,86400`, single node and clustered: one table, wrapper and view per level; each table's comment and TTL; a projection keyed by the next level exactly where the next level is shorter than a day; each view's timer as `building.md` §3.3 computes it; the first view reads `metric_landing` with `DISTINCT`, each other the level below; every run has its run-row branch, `max_threads = 2` and the block condition |
| `the_script_renders_exactly_what_this_crate_renders` (`crates/pulsus-schema/tests/schema_file.rs:459`) | unit | the script's render and the crate's are equal, level sets included |
| `the_script_and_the_crate_refuse_the_same_level_lists` (`crates/pulsus-schema/tests/schema_file.rs:2663`) | unit | each invalid list of `building.md` §1.2 is refused by both, the script with exit `2`, with the same message |
| `a_level_is_its_aggregates` (`crates/pulsus-schema/tests/live_rollup_levels.rs:567`) | live | the 60 s rows equal the aggregates and `inc` computed in the test from the landed samples, exact repeats dropped and values at one millisecond in value-bits order; each later level equals its aggregates from the level below |
| `an_empty_window_moves_the_watermark` (`crates/pulsus-schema/tests/live_rollup_levels.rs:666`) | live | a run over a window with no samples writes its run row and no data row, and the watermark moves past it |
| `a_hole_is_skipped_at_every_level` (`crates/pulsus-schema/tests/live_rollup_levels.rs:711`) | live | a stop longer than landing holds leaves a hole in the first level's run rows; every later level leaves unbuilt exactly the buckets the hole touches and builds again from the first bucket wholly covered after it; every refresh moves the watermark or finds no work |
| `each_run_reads_its_window` (`crates/pulsus-schema/tests/live_rollup_levels.rs:825`) | live | every first-level run reads less than landing holds; every 720 s and 7200 s run that wrote data read its source through `by_parent`; the 86400 s run reads within its day partition's bound |
| `a_level_waits_for_its_source` (`crates/pulsus-schema/tests/live_rollup_levels.rs:1002`) | live | where the 60 s level covers time the 720 s level does not, the 7200 s runs build only what the 720 s run rows cover |
| `runs_split_at_midnight_and_expire_by_day` (`crates/pulsus-schema/tests/live_rollup_levels.rs:1035`) | live | a run crossing midnight leaves one run row per day; with retention 4, every row of day `T−4` is dropped and every row of `T−2` kept |
| `a_sparse_counter_keeps_its_steps` (`crates/pulsus-schema/tests/live_rollup_levels.rs:1187`) | live | per tenant, every 60 s row's `inc` equals the increase computed from all landed samples, for a sparse tenant and a dense one on the same fingerprints; a predecessor exactly 86,400,000 ms before a sample is used, one 86,400,001 ms before is not, and two samples 25 h apart inside one run count no step |
| `stale_markers_and_nan_at_the_levels` (`crates/pulsus-schema/tests/live_rollup_levels.rs:1380`) | live | `last`, `t_last` and `flags` for a stale end, a stale-only bucket, NaN payloads, and counters resuming across a stale marker, at every level; `inc` counts the step over a stale marker and none across 24 hours |
| `each_shard_builds_its_slice_once` (`crates/pulsus-schema/tests/live_cluster.rs:1989`) | cluster | one series pushed to both shards, one sample held only by a replica that is not its shard's builder: the level has one data row per bucket, counting every sample, and each shard one run row from its builder |

## Part B: reading

| test | kind | what it holds |
|---|---|---|
| `b1b_each_selector_records_what_its_consumer_reads` (`crates/pulsus-promql/src/plan.rs:6923`) | unit | `rollup_use` per selector as `reading.md` §1's table; `in_subquery` only inside a subquery |
| `b1_the_router_chooses_the_largest_level_that_fits` (`crates/pulsus-read/src/metrics/rollup.rs:718`) | unit | the chosen level, or none, for every served and every raw-only function at each alignment and range boundary, `@` on and off the grid with the request start off and on it |
| `b1c_assemble` (`crates/pulsus-read/src/metrics/rollup.rs:976`) | unit | a counter's points with and without start points, its continuation into raw at 720 s, and the overlay at 60 s |
| `s3_flags_and_empty_rows` (`crates/pulsus-read/src/metrics/rollup.rs:1079`) | unit | each row of `reading.md` §1's points table, for every `flags` value and `t_last = 0` |
| `b1d_coverage_intersects_every_shard` (`crates/pulsus-read/src/metrics/rollup.rs:1192`) | unit | coverage is the intersection over every shard; a shard with no run row covers nothing |
| `b1e_the_level_fetch_statement` (`crates/pulsus-read/src/metrics/rollup.rs:1235`) | unit | the level fetch's text, single node and clustered |
| `b1f_eligible_refuses_the_unrouted_classes` (`crates/pulsus-read/src/metrics/rollup.rs:1263`) | unit | no metric name, each `__name__` matcher form, the fallback resolution and the histogram-statistics functions are not eligible |
| `duration_text_is_the_largest_whole_unit` (`crates/pulsus-read/src/metrics/rollup.rs:1301`) | unit | `1d`, `2h`, `12m`, `1m`, `5s` |
| `each_served_function_is_within_its_distance` (`crates/pulsus-read/tests/live_metrics_rollup.rs:637`) | live | every served function at every level within its bound, the bit-equal ones as bits (`reading.md` §1.1) |
| `a_window_reads_the_finer_levels_and_raw_after_its_level` (`crates/pulsus-read/tests/live_metrics_rollup.rs:778`) | live | with the 720 s level behind the 60 s level, a 12 h query reads 720 s, then 60 s, then raw, in that order, and ends on the newest sample |
| `a_window_touching_raw_is_raw` (`crates/pulsus-read/tests/live_metrics_rollup.rs:934`) | live | at 60 s, every `rate` and `increase` step ending after the watermark equals raw as bits, native histograms included, and a query wholly after it is `raw-exact`; at 720 s, stopped and stale counters stay within the uncut level's distance plus 0.0218, and the counter continues into raw across a reset |
| `a_hole_reads_raw` (`crates/pulsus-read/tests/live_metrics_rollup.rs:1152`) | live | an hour with no 60 s run rows is read raw |
| `a_hole_in_the_middle_keeps_the_counter` (`crates/pulsus-read/tests/live_metrics_rollup.rs:1210`) | live | a 720 s hole inside a `rate` window is filled from 60 s, and the counter carries across it within the 720 s bounds |
| `the_unrouted_classes_read_raw` (`crates/pulsus-read/tests/live_metrics_rollup.rs:1272`) | live | each unrouted class equals routing off as bits; a metric with float and native-histogram series is served from a level for the floats; a histogram-only metric is `raw-exact` |
| `a_level_added_later_is_used` (`crates/pulsus-read/tests/live_metrics_rollup.rs:1425`) | live | a database built with `60,300,7200,86400` reads `metric_samples_300s` at a 5 m step |
| `a_sparse_counter_keeps_its_steps` (`crates/pulsus-read/tests/live_metrics_rollup.rs:1487`) | live | the sparse counters' `increase` within the sparse 60 s bound, per tenant |
| `stale_and_nan_read_as_raw` (`crates/pulsus-read/tests/live_metrics_rollup.rs:1586`) | live | at every level, stale markers and NaN payloads read as raw for the bit-equal functions; a counter resuming after a stale marker counts its step; the single-sample and 24-hour cases of `reading.md` §1.1 exactly |
| `a_run_row_is_read_with_its_data` (`crates/pulsus-read/tests/live_metrics_rollup_cluster.rs:177`) | cluster | with one replica's fetches stopped, a new run's interval is served from the level only when its data rows come in the same answer as its run row |
| `b9_exactness_is_rollup_when_a_level_served` (`crates/pulsus-server/src/prom_api/encode.rs:1723`) | unit | `"raw-exact"` and `"rollup"` |
| `b6_runtimeinfo_lists_the_rollup_levels` (`crates/pulsus-server/src/prom_api/handlers.rs:1745`) | unit | `rollupLevels` for two level sets, and `[]` before discovery |
