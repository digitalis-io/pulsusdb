# Reproducing the numbers in docs/promql_rollups/

**Built.** Part A (#742) and part B (#744) are on `main` (`docs/promql_rollups/README.md` §2). The files here are the measurements behind the design and one recorded run of the built read path's live tests.

The Python ports take their inputs as arguments, or generate them, and need no server. The statements in `projection-run/` name the database they ran in, `arch18r12_m`, which was dropped afterwards.

## What is here

| file | what it is | backs |
|---|---|---|
| `counter_distance.py` | an independent port in Python of the extrapolated `rate` and of the level chain, generating its own counters: 100 resetting every 500–799 samples, 20 resetting every second sample. Usage `counter_distance.py SCRAPE_SECONDS DAYS` | `reading.md` §1.1 |
| `results/counter_15s.txt`, `results/counter_60s.txt` | its output for `15 14` and `60 14`. Per level and counter kind, three rules: `last only` (rejected), `inc`, and `inc+start` | `reading.md` §1.1 |
| `gauge_distance.py` | a port in Python comparing raw samples with 60 s and 720 s level rows for gauges and counters. Usage `gauge_distance.py INPUT_DIR SCRAPE_SECONDS` | `reading.md` §1.1, §1.2 |
| `results/gauge_15s.txt`, `results/gauge_60s.txt` | its output. Rows are keyed by level, range, series kind (`c` counter, `c2` counter resetting every second sample, `g` gauge) and function | `reading.md` §1.1, §1.2 |
| `projection-run/01_metric_landing_ddl.sql` … `03_metric_samples_720s_ddl.sql` | the rendered single-node DDL of landing with its block key and of the 60 s and 720 s levels, as they stood before `flags` was added | `building.md` §2, §3.4 |
| `projection-run/04_first_level_run_{0,1,2}.sql`, `05_720s_run.sql`, `06_720s_run_without_block_condition.sql` | the statements as run, with `lo`, `hi` and `now_ms` as literals; `first.tmpl` and `up.tmpl` are the same statements with those placeholders | `building.md` §3.4 |
| `projection-run/07_query_log.tsv` | `system.query_log` for every run: rows read and written, time, memory, `projections` | `building.md` §3.4 |
| `projection-run/08_720s_rows.tsv`, `09_60s_rows.tsv`, `10_60s_parts.tsv` | the rows of both levels by `kind`, and the 60 s level's parts | `building.md` §2.3, §3.4 |
| `projection-run/corpus_bounds.txt` | the corpus's first and last millisecond | `building.md` §3.4 |
| `results/live-metrics-rollup.txt` | the last run of `crates/pulsus-read/tests/live_metrics_rollup.rs` recorded during part B's work, against the development server, all nine tests passing; it was not re-run on the merged commit: per function, level and series kind, `n`, p99 and maximum of the distance from raw, and `read_rows` of both routes per query; the cut-level comparisons of `a_window_touching_raw_is_raw` | `reading.md` §1.1, §3.1 |
| `partB-ports/bounds.py`, `partB-ports/bounds.txt` | a port of the read rule (§1 of `reading.md`) on generated counters and gauges, 14 days, 40 series per kind; output for `15` and then `60`. The test bounds of `reading.md` §1.1 are twice the larger of its p99 and the earlier measurement | `reading.md` §1.1 |
| `partB-ports/bounds_fixed.py`, `partB-ports/bounds_fixed.txt` | the same for sparse counters with the predecessor taken up to 24 hours back; output for `15` and then `60` | `reading.md` §1.1 |
| `partB-ports/tail.py`, `partB-ports/tail.txt` | `rate[5m]` at a 60 s step over a 60 s level whose watermark is k minutes before the window end, the raw samples after it as read or thinned to one per bucket; output for `15` and then `60` | `README.md` §3 |
| `partB-ports/tail_stop.py`, `partB-ports/tail_stop.txt` | the same for counters that stop near the watermark; output for `15` | `README.md` §3 |
| `partB-ports/tail720.py`, `partB-ports/tail720.txt` | `rate[1h]` at a 12 m step over a 720 s level, the watermark k steps before the end, raw folded into the counter as read | `README.md` §3 |
| `partB-ports/startpoints.py`, `partB-ports/startpoints.txt` | one start point per window against one per row, on normal, stopped and sparse counters; output for `15 7` and then `60 7` | `reading.md` §1.1 |

Edits made when the files were copied here, and no other: two comment lines in `counter_distance.py`, two in `partB-ports/startpoints.py` and two in `partB-ports/tail720.py` were reworded, and `gauge_distance.py` gained two comment lines giving its usage. The code is as it ran. Every port was re-run after the edits. `counter_distance.py` and all six `partB-ports/` scripts reproduced their kept outputs byte for byte; `gauge_distance.py`'s output in `results/` is that re-run. `results/live-metrics-rollup.txt` is the test run's output less the two lines naming the build directory.

**The gauge port's inputs are not here.** `INPUT_DIR` holds `raw<S>.tsv` (`fingerprint`, `unix_milli`, `value`), `lv60_<S>.tsv` and `lv720_<S>.tsv` (`fingerprint`, `t_end`, `t_last`, `last`, `min`, `max`, `sum`, `count`) and `kinds.tsv` (`fingerprint`, kind). They were exported from a database since dropped: 4,000 series over one day, about 150 MB. The statements that generated the corpus, built the level rows and exported them were not kept. Those level rows were built by an earlier statement without the exact-repeat drop of D12.

**How the recorded run was driven is not here.** The statement files are what ran. The commands that loaded 4,000 series at 15 s over three hours into landing, and that read `system.query_log` and `system.parts` into the `.tsv` files, were not kept.

## Figures whose output was not kept

Each was measured on a database that has since been dropped. The figure is stated in the document with its instrument; no file here reproduces it.

| figure | document |
|---|---|
| dropping exact repeats: 5,760,000 rows equal; 112,320 rows with doubled `count` and `sum`; 1,743 `inc` rows; the `DISTINCT`'s cost | `building.md` §3.2 |
| the timer clauses the server accepted, and their next run times | `building.md` §3.3 |
| run reads before and after the block keys, and 23,039,986 rows without the block condition | `building.md` §3.4 |
| reads and inserts under concurrent rollup statements, and the sustained-load rate | `building.md` §5 |
| sizes of landing, the levels and their projections | `building.md` §1.3, §6.1 |
| runs over their normal window: at most 128 ms and about 13 MB | `building.md` §3.3, §6.2 |
| query reads by main key | `building.md` §6.3 |
| a distributed read through a lagging replica returning neither data nor coverage | `reading.md` §3 |
| a view's window computed in `WITH` against one scalar subquery: 20,907 and 17,835 rows against 9,959 | `building.md` §3.1 |
| the first level's predecessor from its own table: 2,270 of 24,644 buckets short without it; the cost, 69,950 → 139,898 rows per run | `building.md` §3.1 |
| the predecessor skipping stale markers: 287 series wrong with the immediate predecessor; 118–124 ms against 108–121 ms | `building.md` §3.1 |
| the `flags` column's size, 40,770 bytes (1.8 %), and the three-column alternative's, +24.9 % | `building.md` §1.3, `README.md` §3 |
| the 86400 s run reading 6,628 rows with `projections = []` | `building.md` §3.4 |
| `avg_over_time` with every sample on a bucket edge: p99 0.358, 0.595, 0.0557, 0.0246 | `reading.md` §1.1 |
