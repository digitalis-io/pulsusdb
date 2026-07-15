# M2: Three-path label-resolution benchmark, started on the 5M-series scale corpus

> **This report establishes CI-scale evidence (`[1_000, 10_000, 50_000]`
> series per metric) plus two structural, scale-invariant findings.** Per
> the two-tier evidence model (docs/schemas.md §9) and this issue's
> task-manager-adjudicated rescope, the design-target `500_000`/`5_000_000`
> cardinalities are **not yet executed** — they are the documented,
> reproducible manual follow-up (see "Reproduction" below) that precedes
> the M3 decision gate, not part of this issue's own acceptance criteria.
> Every wall-clock/`read_rows` number in this report is CI-scale and
> **UNVALIDATED** against the §9 latency targets. Two things *are*
> scale-invariant across all three committed cardinalities: (a) idx loses
> to the SQL fallback on the wide-scan selectors (`Regex5xx`, `NegBroad`)
> at every tested cardinality, and (b) the `EXPLAIN indexes = 1`
> granule-pruning finding. The bounded positive-equality result
> (`NarrowEq`, `BroadEq`) is explicitly **not** scale-invariant — it is a
> **cardinality-dependent crossover** (sql_fallback wins at 1,000 series,
> idx has overtaken by 50,000) — see "Comparative result" below; the
> crossover itself, not a uniform win/loss verdict, is the M3 signal.

Issue: [#34](https://github.com/digitalis-io/pulsusdb/issues/34). The M2 half
of docs/schemas.md §2.1's strategy-ladder decision gate
(docs/architecture.md §10); the M3 milestone closes the ship/no-ship
decision for `metric_series_idx` and/or incremental refresh. Evidence model:
docs/schemas.md §9's two-tier model (issue #16).

## What this proves, and what it doesn't

- **Proves (this report):** all three §2.1 strategy-ladder paths — the real
  `pulsus_read::metrics::LabelCache`/`SeriesResolver::resolve` (path 1), the
  real `pulsus_read::metrics::sql::historical_series_subquery` SQL fallback
  (path 2), and a bench-local `metric_series_idx` prototype (path 3) —
  resolve **identical fingerprint sets** for four selector shapes
  (`NarrowEq`, `BroadEq`, `Regex5xx`, `NegBroad`, including the pure-negative
  case and series that omit the negated label entirely) across three
  cardinality tiers and two activity-bucket sizes: 48 cross-path correctness
  checks, all passing, on a live ClickHouse 24.8 instance, with the
  resolution windows anchored to a single frozen reference instant, an
  activity guard band, and an enforced drift bound so the result is
  reproducible **by construction**, not by favourable timing (see "Harness"
  below). Also proves: (a) idx's win/loss pattern against the SQL fallback
  by selector shape *and* cardinality — a scale-invariant loss on wide
  scans, a cardinality-dependent crossover on bounded positive-equality
  selectors (a material M3 signal — see "Comparative result" below), and
  (b) a structural property of `metric_series`'s schema (see "Structural
  finding" below), both confirmed live on this run.
- **Does not prove:** the §9 latency targets at design-target scale (5M
  active series per metric).

## Harness

`cargo xtask bench metrics-labels` (`xtask/src/bench/metrics_labels/`),
extending the `#16` harness (`xtask/src/bench/`) with a second scenario.
`xtask/src/bench/query_log.rs` was extracted from `queries.rs` (mechanical
relocation, `pub(crate)`, no behavioural change to `logs-read`) so both
scenarios read `system.query_log` evidence through the same reader.

- **`corpus.rs`** — deterministic `metric_series` corpus generator. Per
  cardinality tier `C`, one metric `metric_C` with `C` series; series `i`'s
  labels (`job="j{i%8}"`, `region`, `env`, `status` cycling
  `200/404/500/503`, `pod="pod-{i}"` unique) are a pure function of `i` —
  same `LabelSet::from_verbatim`/`metric_fingerprint` primitives the product
  writer and reader agree on.
  - **Absent-label series.** A deterministic ~1/10 of series omit `job`
    entirely and a deterministic ~1/13 omit `status` entirely, so the
    cross-path correctness gate exercises Prometheus's absent-label-as-`""`
    semantics for `NegBroad` (`job!="j0"` must match a series with no `job`
    label at all).
  - **Activity staggering + guard band + decorrelation.** Each series gets
    **exactly one** row (not one per bucket per series), in a bucket chosen
    by `assign_bucket`, which reserves the window's earliest bucket as a
    **guard band never assigned to any series**, and mixes the series
    ordinal through a splitmix64-style avalanche (`mix64`) before taking the
    bucket-count modulus. The mix step is load-bearing, not cosmetic: found
    live during this fix — with a *raw* `i % guard_span` assignment, the
    default `1h`/24h shape gives `guard_span = 24`, a multiple of `job`'s
    own modulus (`8`), so *every* bucket's series shared exactly one `job`
    residue, and the over-inclusion probe's `job="j0"` selector matched
    **zero** series in the "current" bucket for every tested cardinality —
    not the expected `~C / guard_span`. `mix64` decorrelates bucket choice
    from label residues.
  - **Frozen reference instant + enforced drift bound.** `metrics_labels::run`
    captures one `ref_ms` per bucket-size pass, before generating anything;
    the corpus, the SQL-fallback/idx-resolution bounds, and the resolver's
    `DataWindow` all derive from that single value — never re-read the wall
    clock independently. `LabelCache::refresh()`'s own internal sweep
    (product code, its own wall-clock "now") is the one thing this
    reference instant cannot control; the guard band neutralizes up to one
    `bucket_ms` of drift, and `paths::run_all` now **asserts** the actual
    elapsed wall-clock time (corpus load + idx build + the refresh sweep)
    stayed under that one-`bucket_ms` bound immediately before path
    resolution begins — converting a would-be silent cross-path mismatch
    into a loud, immediate, actionable failure.
- **`idx.rs`** — the bench-local `metric_series_idx` prototype: §2.1's
  verbatim single-node DDL (`ReplacingMergeTree ORDER BY (metric_name, key,
  val, bucket, fingerprint)`, `ON CLUSTER` appended under `--dist` so the
  local table exists on every shard before the `_dist` wrapper is created),
  populated by `INSERT ... SELECT ... ARRAY JOIN
  JSONExtractKeysAndValues(labels, 'String')` over `metric_series`. **Never
  added to `pulsus-schema`'s migration catalog or `run_init`.**
  **Resolution-SQL scope (recorded, not fixed):** the prototype's parity
  evidence covers only the four benchmarked selector classes (bounded
  positive equality, regex, single-negative) — none of which is an
  *empty-accepting* matcher, i.e. one a label-less series (zero rows for
  that key) must, under Prometheus's absent-label-as-`""` semantics, either
  match or not match. Two **opposite**, both verified, failure modes if run
  against this prototype's current SQL:
  - `job!=""` (pure-negative form) wrongly **includes** an absent-`job`
    series — `countIf` over zero `key = 'job'` rows is trivially `0` (so
    the `HAVING` passes), but `"" != ""` is *false*, so the series should
    have been excluded. A false positive.
  - `job=~".*"` (positive-branch form) wrongly **excludes** an absent-`job`
    series — zero `key = 'job'` rows means the fingerprint can never reach
    `GROUP BY` at all, but `.*` matches `""`, so the series should have
    been included. A false negative.

  Both are **known, recorded open cases for the M3 idx design if it
  ships**, deliberately not generalized here (the general resolution
  semantics are the M3 ship design's decision, not this evidence run's).
- **`paths.rs`** — the three path runners, `SelectorKind`, and the idx
  prototype's resolution SQL (`idx_resolve_sql`, the #11 logs-idx
  `uniqExactIf`/`countIf` conditional-aggregation shape adapted
  metric-scoped, **no `key IN (...)` prefilter** — an earlier version's
  prefilter silently dropped fingerprints lacking the negated key entirely,
  breaking Prometheus absence semantics; fixed, exercised by the corpus's
  absent-label series above). The **cross-path correctness gate**: before
  recording any perf evidence, `run_all` asserts all three paths' sorted,
  deduplicated fingerprint sets are identical for every `(tier, selector)`
  cell — `anyhow::ensure!`, hard-fails the run on any mismatch. A pure-Rust,
  no-database complement lives in `paths.rs`'s own unit tests.
- **`report.rs`** — JSON (`docs/benchmarks/data/*.json`) + markdown evidence
  rendering, **plus a mechanical consistency test** (`consistency_tests`,
  runs in the ordinary `cargo test -p xtask` pass): loads the *committed*
  evidence artifact and asserts this report's specific comparative
  *claims* (the win/loss split below, the structural `read_rows` ratio, the
  semantic candidate ratio) against it directly. Honest scope: this asserts
  **claims, not bytes** — a future regeneration whose numbers drift but
  preserve every stated conclusion (idx still wins the same selectors at
  the same tiers, the ratios stay on the same side of their thresholds)
  passes; a regeneration whose numbers flip a stated conclusion fails the
  build. It is not a literal diff against this markdown file's prose or
  table values. Every table below is the literal `render_markdown` output
  for the one canonical run this report documents, copied verbatim.

Two profiles, hard-bounded (architect plan amendment #2 — CI cannot silently
run the 5M-series shape): `--profile ci` always uses the fixed
`[1_000, 10_000, 50_000]` set and hard-errors on any `--metric-cardinalities`
override; `--profile full` uses the design-target `[10_000, 500_000,
5_000_000]` set by default, manual-only.

## Query set

| Selector | Matcher | Approx. selectivity (per tier, before absent-label carve-outs) |
|---|---|---|
| `NarrowEq` | `pod="pod-0"` — unique per series | 1 series |
| `BroadEq` | `job="j0"` | ~C/8 |
| `Regex5xx` | `status=~"5.."` | ~C/2 (`500`/`503`) |
| `NegBroad` | `job!="j0"` (pure-negative idx case) | ~7C/8 |

Benchmarked across cardinalities `C ∈ {1_000, 10_000, 50_000}` and activity
buckets `{1h, 1d}` — 24 cells per bucket size, 48 total.

## Cross-path correctness gate (Tier-1 claim)

Verified live in this session (podman, ClickHouse 24.8): all 48 cells
passed — every `(tier, selector)` cell's cache/SQL-fallback/idx-prototype
fingerprint sets were identical, path 1 never degraded to `SqlFallback`,
`matched_series` reflects the absent-label carve-outs correctly (e.g.
`metric_1000`/`BroadEq`: 100, not the naive ~125; `metric_1000`/`NegBroad`:
900 = 1000 − 100, correctly *including* the absent-`job` series), and the
guard-band drift assertion never tripped. This is a genuine, scale-invariant
(Tier-1) claim: the product's own cache resolver, the product's own
SQL-fallback builder, and the bench-local idx prototype's conditional-
aggregation SQL agree on *correctness*, including Prometheus absent-label
semantics — independent of cardinality or bucket size.

## CI-scale results (recorded, not gated)

Corpus: cardinalities `[1_000, 10_000, 50_000]`, seed 42, both `1h`
(3,600,000 ms) and `1d` (86,400,000 ms) activity buckets, 24h window
(`PULSUS_CACHE_WINDOW`'s own default). 61,000 `metric_series` rows per
bucket-size pass (loaded in 682 ms / 686 ms via direct RowBinary insert),
294,206 `metric_series_idx` rows per pass (built + ARRAY-JOIN-populated in
196 ms / 211 ms, ~2.6–2.8 MB on disk). Single ClickHouse 24.8 node (podman,
this session). Full machine-readable evidence:
[`data/metrics-labels-ci.json`](data/metrics-labels-ci.json).

The full per-`(bucket_ms, path, tier, selector)` cell table, copied verbatim
from this run's `render_markdown` output:

| bucket_ms | path | metric | cardinality | selector | matched_series | wall p50 (ms) | wall p95 (ms) | wall p99 (ms) | read_rows | selected/total marks | read_bytes | memory_usage |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 3600000 | cache | metric_1000 | 1000 | pod="pod-0" | 1 | 0.2901 | 0.5156 | 0.8334 | 0 | 0/0 | 0 | 0 |
| 3600000 | sql_fallback | metric_1000 | 1000 | pod="pod-0" | 1 | 5.8439 | 5.8452 | 5.8452 | 1000 | 1/11 | 91635 | 4205908 |
| 3600000 | idx_prototype | metric_1000 | 1000 | pod="pod-0" | 1 | 6.3266 | 8.5223 | 8.5223 | 8192 | 1/37 | 243381 | 4238076 |
| 3600000 | cache | metric_1000 | 1000 | job="j0" | 100 | 0.3318 | 0.6008 | 0.7942 | 0 | 0/0 | 0 | 0 |
| 3600000 | sql_fallback | metric_1000 | 1000 | job="j0" | 100 | 3.4516 | 5.9789 | 5.9789 | 1000 | 1/11 | 91635 | 4205116 |
| 3600000 | idx_prototype | metric_1000 | 1000 | job="j0" | 100 | 6.8695 | 7.3522 | 7.3522 | 8192 | 1/37 | 243381 | 4236684 |
| 3600000 | cache | metric_1000 | 1000 | status=~"5.." | 462 | 0.9590 | 2.0934 | 2.7255 | 0 | 0/0 | 0 | 0 |
| 3600000 | sql_fallback | metric_1000 | 1000 | status=~"5.." | 462 | 5.3681 | 5.8890 | 5.8890 | 1000 | 1/11 | 91635 | 4202476 |
| 3600000 | idx_prototype | metric_1000 | 1000 | status=~"5.." | 462 | 7.1658 | 8.8621 | 8.8621 | 8192 | 1/37 | 243381 | 4238220 |
| 3600000 | cache | metric_1000 | 1000 | job!="j0" | 900 | 0.3927 | 0.7225 | 1.1453 | 0 | 0/0 | 0 | 0 |
| 3600000 | sql_fallback | metric_1000 | 1000 | job!="j0" | 900 | 4.1754 | 5.4497 | 5.4497 | 1000 | 1/11 | 91635 | 4198204 |
| 3600000 | idx_prototype | metric_1000 | 1000 | job!="j0" | 900 | 6.9727 | 7.4187 | 7.4187 | 8192 | 1/37 | 243381 | 4235940 |
| 3600000 | cache | metric_10000 | 10000 | pod="pod-0" | 1 | 7.1116 | 9.7438 | 10.9231 | 0 | 0/0 | 0 | 0 |
| 3600000 | sql_fallback | metric_10000 | 10000 | pod="pod-0" | 1 | 7.6030 | 7.7181 | 7.7181 | 10000 | 1/11 | 926340 | 4205396 |
| 3600000 | idx_prototype | metric_10000 | 10000 | pod="pod-0" | 1 | 7.9230 | 7.9489 | 7.9489 | 8192 | 1/37 | 241998 | 4238556 |
| 3600000 | cache | metric_10000 | 10000 | job="j0" | 1000 | 9.3316 | 11.0317 | 13.0261 | 0 | 0/0 | 0 | 0 |
| 3600000 | sql_fallback | metric_10000 | 10000 | job="j0" | 1000 | 8.2134 | 8.2890 | 8.2890 | 10000 | 1/11 | 926340 | 4196892 |
| 3600000 | idx_prototype | metric_10000 | 10000 | job="j0" | 1000 | 6.8033 | 9.1152 | 9.1152 | 8192 | 1/37 | 237568 | 4236748 |
| 3600000 | cache | metric_10000 | 10000 | status=~"5.." | 4616 | 16.3970 | 18.4827 | 21.2339 | 0 | 0/0 | 0 | 0 |
| 3600000 | sql_fallback | metric_10000 | 10000 | status=~"5.." | 4616 | 8.4077 | 10.2616 | 10.2616 | 10000 | 1/11 | 926340 | 4169180 |
| 3600000 | idx_prototype | metric_10000 | 10000 | status=~"5.." | 4616 | 13.0349 | 13.4091 | 13.4091 | 16384 | 2/37 | 484366 | 4238156 |
| 3600000 | cache | metric_10000 | 10000 | job!="j0" | 9000 | 9.1009 | 11.3630 | 13.1486 | 0 | 0/0 | 0 | 0 |
| 3600000 | sql_fallback | metric_10000 | 10000 | job!="j0" | 9000 | 10.7284 | 10.9960 | 10.9960 | 10000 | 1/11 | 926340 | 4133852 |
| 3600000 | idx_prototype | metric_10000 | 10000 | job!="j0" | 9000 | 18.7667 | 19.0857 | 19.0857 | 57344 | 7/37 | 1736909 | 4236068 |
| 3600000 | cache | metric_50000 | 50000 | pod="pod-0" | 1 | 31.8323 | 36.0056 | 37.3371 | 0 | 0/0 | 0 | 0 |
| 3600000 | sql_fallback | metric_50000 | 50000 | pod="pod-0" | 1 | 18.3966 | 18.4742 | 18.4742 | 50000 | 6/11 | 4341721 | 5001881 |
| 3600000 | idx_prototype | metric_50000 | 50000 | pod="pod-0" | 1 | 6.5362 | 7.4598 | 7.4598 | 8192 | 1/37 | 289882 | 4238076 |
| 3600000 | cache | metric_50000 | 50000 | job="j0" | 5000 | 37.1641 | 42.1775 | 43.2280 | 0 | 0/0 | 0 | 0 |
| 3600000 | sql_fallback | metric_50000 | 50000 | job="j0" | 5000 | 20.1298 | 20.8358 | 20.8358 | 50000 | 6/11 | 4676185 | 5032345 |
| 3600000 | idx_prototype | metric_50000 | 50000 | job="j0" | 5000 | 13.0654 | 14.5326 | 14.5326 | 16384 | 2/37 | 475136 | 4235852 |
| 3600000 | cache | metric_50000 | 50000 | status=~"5.." | 23077 | 73.9909 | 83.2327 | 105.4231 | 0 | 0/0 | 0 | 0 |
| 3600000 | sql_fallback | metric_50000 | 50000 | status=~"5.." | 23077 | 34.6221 | 39.6229 | 39.6229 | 50000 | 6/11 | 4676185 | 10418026 |
| 3600000 | idx_prototype | metric_50000 | 50000 | status=~"5.." | 23077 | 37.7213 | 39.5926 | 39.5926 | 48446 | 6/37 | 1254479 | 11408307 |
| 3600000 | cache | metric_50000 | 50000 | job!="j0" | 45000 | 37.1309 | 42.9901 | 46.0518 | 0 | 0/0 | 0 | 0 |
| 3600000 | sql_fallback | metric_50000 | 50000 | job!="j0" | 45000 | 40.4701 | 41.6706 | 41.6706 | 50000 | 6/11 | 4676185 | 10001591 |
| 3600000 | idx_prototype | metric_50000 | 50000 | job!="j0" | 45000 | 80.4791 | 91.2687 | 91.2687 | 245054 | 30/37 | 7495510 | 22175990 |
| 86400000 | cache | metric_1000 | 1000 | pod="pod-0" | 1 | 0.3120 | 0.4405 | 0.6577 | 0 | 0/0 | 0 | 0 |
| 86400000 | sql_fallback | metric_1000 | 1000 | pod="pod-0" | 1 | 4.6517 | 5.1688 | 5.1688 | 1000 | 1/11 | 91635 | 4263252 |
| 86400000 | idx_prototype | metric_1000 | 1000 | pod="pod-0" | 1 | 5.8031 | 6.0503 | 6.0503 | 8192 | 1/37 | 243381 | 4238076 |
| 86400000 | cache | metric_1000 | 1000 | job="j0" | 100 | 0.3769 | 0.5721 | 0.8548 | 0 | 0/0 | 0 | 0 |
| 86400000 | sql_fallback | metric_1000 | 1000 | job="j0" | 100 | 4.6927 | 6.4807 | 6.4807 | 1000 | 1/11 | 91635 | 4204092 |
| 86400000 | idx_prototype | metric_1000 | 1000 | job="j0" | 100 | 6.4635 | 8.5101 | 8.5101 | 8192 | 1/37 | 243381 | 4236076 |
| 86400000 | cache | metric_1000 | 1000 | status=~"5.." | 462 | 1.0974 | 1.5207 | 1.8720 | 0 | 0/0 | 0 | 0 |
| 86400000 | sql_fallback | metric_1000 | 1000 | status=~"5.." | 462 | 4.7365 | 5.3424 | 5.3424 | 1000 | 1/11 | 91635 | 4201964 |
| 86400000 | idx_prototype | metric_1000 | 1000 | status=~"5.." | 462 | 7.4647 | 7.9952 | 7.9952 | 8192 | 1/37 | 243381 | 4238220 |
| 86400000 | cache | metric_1000 | 1000 | job!="j0" | 900 | 0.3994 | 0.5816 | 0.7855 | 0 | 0/0 | 0 | 0 |
| 86400000 | sql_fallback | metric_1000 | 1000 | job!="j0" | 900 | 5.9406 | 6.1037 | 6.1037 | 1000 | 1/11 | 91635 | 4198204 |
| 86400000 | idx_prototype | metric_1000 | 1000 | job!="j0" | 900 | 6.6323 | 7.1596 | 7.1596 | 8192 | 1/37 | 243381 | 4235364 |
| 86400000 | cache | metric_10000 | 10000 | pod="pod-0" | 1 | 6.9086 | 9.0284 | 9.7645 | 0 | 0/0 | 0 | 0 |
| 86400000 | sql_fallback | metric_10000 | 10000 | pod="pod-0" | 1 | 6.8759 | 7.6793 | 7.6793 | 10000 | 1/11 | 926340 | 4205844 |
| 86400000 | idx_prototype | metric_10000 | 10000 | pod="pod-0" | 1 | 5.9958 | 6.6273 | 6.6273 | 8192 | 1/37 | 241998 | 4238076 |
| 86400000 | cache | metric_10000 | 10000 | job="j0" | 1000 | 8.5374 | 10.1206 | 11.9623 | 0 | 0/0 | 0 | 0 |
| 86400000 | sql_fallback | metric_10000 | 10000 | job="j0" | 1000 | 7.7362 | 9.3786 | 9.3786 | 10000 | 1/11 | 926340 | 4197404 |
| 86400000 | idx_prototype | metric_10000 | 10000 | job="j0" | 1000 | 7.4333 | 7.7719 | 7.7719 | 8192 | 1/37 | 237568 | 4236684 |
| 86400000 | cache | metric_10000 | 10000 | status=~"5.." | 4616 | 16.0556 | 18.0820 | 19.0109 | 0 | 0/0 | 0 | 0 |
| 86400000 | sql_fallback | metric_10000 | 10000 | status=~"5.." | 4616 | 10.5209 | 12.2647 | 12.2647 | 10000 | 1/11 | 926340 | 4168668 |
| 86400000 | idx_prototype | metric_10000 | 10000 | status=~"5.." | 4616 | 14.6918 | 14.9409 | 14.9409 | 16384 | 2/37 | 484366 | 4237324 |
| 86400000 | cache | metric_10000 | 10000 | job!="j0" | 9000 | 8.8293 | 10.8396 | 11.8349 | 0 | 0/0 | 0 | 0 |
| 86400000 | sql_fallback | metric_10000 | 10000 | job!="j0" | 9000 | 13.1875 | 13.7677 | 13.7677 | 10000 | 1/11 | 926340 | 4133340 |
| 86400000 | idx_prototype | metric_10000 | 10000 | job!="j0" | 9000 | 18.3852 | 18.9674 | 18.9674 | 57344 | 7/37 | 1736909 | 4235876 |
| 86400000 | cache | metric_50000 | 50000 | pod="pod-0" | 1 | 29.2295 | 31.8054 | 33.0841 | 0 | 0/0 | 0 | 0 |
| 86400000 | sql_fallback | metric_50000 | 50000 | pod="pod-0" | 1 | 15.4188 | 16.0644 | 16.0644 | 50000 | 6/11 | 4341721 | 5001881 |
| 86400000 | idx_prototype | metric_50000 | 50000 | pod="pod-0" | 1 | 5.9025 | 7.1156 | 7.1156 | 8192 | 1/37 | 289882 | 4238140 |
| 86400000 | cache | metric_50000 | 50000 | job="j0" | 5000 | 32.2718 | 37.7713 | 40.0077 | 0 | 0/0 | 0 | 0 |
| 86400000 | sql_fallback | metric_50000 | 50000 | job="j0" | 5000 | 19.6763 | 22.7533 | 22.7533 | 50000 | 6/11 | 4676185 | 5001881 |
| 86400000 | idx_prototype | metric_50000 | 50000 | job="j0" | 5000 | 13.0844 | 13.4207 | 13.4207 | 16384 | 2/37 | 475136 | 4236876 |
| 86400000 | cache | metric_50000 | 50000 | status=~"5.." | 23077 | 76.8405 | 89.0839 | 96.9558 | 0 | 0/0 | 0 | 0 |
| 86400000 | sql_fallback | metric_50000 | 50000 | status=~"5.." | 23077 | 39.5126 | 41.7604 | 41.7604 | 50000 | 6/11 | 4676185 | 10418026 |
| 86400000 | idx_prototype | metric_50000 | 50000 | status=~"5.." | 23077 | 49.3251 | 50.0562 | 50.0562 | 48446 | 6/37 | 1254479 | 11408307 |
| 86400000 | cache | metric_50000 | 50000 | job!="j0" | 45000 | 36.1458 | 42.8903 | 55.3677 | 0 | 0/0 | 0 | 0 |
| 86400000 | sql_fallback | metric_50000 | 50000 | job!="j0" | 45000 | 46.8775 | 46.9766 | 46.9766 | 50000 | 6/11 | 4676185 | 10014135 |
| 86400000 | idx_prototype | metric_50000 | 50000 | job!="j0" | 45000 | 79.7330 | 89.9922 | 89.9922 | 245054 | 30/37 | 7495510 | 24708052 |

## Comparative result: a scale-invariant wide-scan loss, and a cardinality crossover on bounded positive selectors

Two distinct claims, not one uniform verdict — reading directly off the
per-cell table above, across **all three** committed cardinalities:

**1. Idx loses to the SQL fallback on the wide scans (`Regex5xx`,
`NegBroad`) at every tested cardinality — scale-invariant within this
report's range.**

| Selector | 1,000 | 10,000 | 50,000 |
|---|---|---|---|
| `Regex5xx` (1h) | sql 5.37 / idx 7.17 — **sql** | sql 8.41 / idx 13.03 — **sql** | sql 34.62 / idx 37.72 — **sql** |
| `NegBroad` (1h) | sql 4.18 / idx 6.97 — **sql** | sql 10.73 / idx 18.77 — **sql** | sql 40.47 / idx 80.48 — **sql** |

**2. Idx vs. sql_fallback on the bounded positive-equality selectors
(`NarrowEq`, `BroadEq`) is a *cardinality-dependent crossover*, not a
uniform win or loss** — this is the corrected claim (an earlier draft of
this report, checking only `metric_50000`, overclaimed a uniform "idx
wins"):

| Selector | 1,000 | 10,000 | 50,000 |
|---|---|---|---|
| `NarrowEq` (1h) | sql 5.84 / idx 6.33 — **sql** | sql 7.60 / idx 7.92 — **sql** (narrow) | sql 18.40 / idx 6.54 — **idx** (2.8×) |
| `BroadEq` (1h) | sql 3.45 / idx 6.87 — **sql** | sql 8.21 / idx 6.80 — **idx** | sql 20.13 / idx 13.07 — **idx** (1.5×) |
| `NarrowEq` (1d) | sql 4.65 / idx 5.80 — **sql** | sql 6.88 / idx 6.00 — **idx** (narrow) | sql 15.42 / idx 5.90 — **idx** (2.6×) |
| `BroadEq` (1d) | sql 4.69 / idx 6.46 — **sql** | sql 7.74 / idx 7.43 — **idx** (narrow) | sql 19.68 / idx 13.08 — **idx** (1.5×) |

At the smallest committed cardinality (1,000 series), sql_fallback wins
**every** selector, including the bounded positive ones — idx's fixed
per-query overhead (a `GROUP BY`/`HAVING` aggregation vs. sql_fallback's
plain filtered scan) dominates when there is little data to scan either
way. Somewhere around 10,000 series the crossover happens — `BroadEq` has
already crossed in both bucket passes; `NarrowEq`'s crossover point sits
close enough to 10,000 that the two bucket passes land on opposite sides of
it (a narrow, noise-sensitive margin, not asserted as a hard fact by the
consistency test below). By 50,000 series idx has clearly overtaken both
bounded positive selectors in both bucket passes (1.5–2.8× faster).

**The crossover itself — not "idx wins" or "idx loses" — is the M3-gate
signal.** The idx prototype's `metric_name, key, val, bucket, fingerprint`
ordering genuinely helps a bounded positive-equality selector
(`WHERE key = X AND val = Y` prunes the primary key's `key`/`val` prefix
directly) once the per-query fixed overhead is amortized by enough
candidate rows, and genuinely hurts on the two shapes that force a wide
scan regardless of cardinality — `Regex5xx` (no `val` equality to prune
on) and especially `NegBroad` (no key filter at all, by design — see
"Harness" above; `read_rows` 245,054 vs. sql_fallback's 50,000, the
"intended wide scan" cost of the absence-semantics fix landing exactly
where predicted). Whether the design-target scale (10k/500k/5M) sits well
past this corpus's crossover point, and by how much, is exactly what the
`full`-profile run needs to answer — this CI-scale evidence establishes
that the crossover exists and roughly where it starts, not where it ends.
Confirmed live and pinned by `report.rs`'s
`idx_vs_sql_fallback_on_bounded_positive_selectors_crosses_over_by_cardinality`
and
`idx_loses_to_sql_fallback_on_wide_scans_at_every_committed_cardinality`
mechanical consistency tests.

## Structural finding: `metric_series`'s primary key does not prune by time once fingerprint cardinality is non-trivial

`metric_series`'s `ORDER BY (metric_name, fingerprint, unix_milli)`
(docs/schemas.md §2.1, unmodified — this benchmark never alters product
DDL) sorts physically **by fingerprint before by time**. Once a metric has
enough distinct fingerprints that a single 8,192-row granule spans many
different fingerprints (true at all three tested cardinalities), each
granule's `unix_milli` min/max already spans nearly the metric's *entire*
registered time range. `EXPLAIN indexes = 1`, captured live for
`metric_50000`/`BroadEq` in this run, confirms this directly —
**byte-identical** between the `1h` and `1d` bucket passes:

```
PrimaryKey
  Keys: metric_name, unix_milli
  Parts: 1/3        -- metric-name part-pruning works correctly
  Granules: 6/8      -- IDENTICAL for the 1h AND 1d bucket passes
```

`read_rows` for the SQL fallback path equals the **entire metric's row
count** (1,000 / 10,000 / 50,000, exactly `C` — see the per-cell table
above) for *every* selector and *both* bucket sizes — the `unix_milli`
bound prunes zero additional granules beyond what `metric_name` already
achieves.

**M3-relevant reading:** neither path 2 nor path 3, as currently specified,
give `PULSUS_SERIES_ACTIVITY_BUCKET` any measurable read-cost leverage over
a narrow historical query once a metric's cardinality is non-trivial.

## Over-inclusion: two ratios, structural and semantic

The day-bucket over-inclusion probe now captures **two** observables per
bucket size (issue #34 CODE review round-2 [valid] finding #5): the
physical `read_rows` cost (round-1's only capture) and the deduplicated
**matched candidate count** the same query returns — the semantic
observable docs/schemas.md §2.1 actually describes. Copied verbatim from
this run:

| cardinality | read_rows (1h) | read_rows (1d) | read_rows ratio (1d/1h) | matched_candidates (1h) | matched_candidates (1d) | candidate ratio (1d/1h) |
|---|---|---|---|---|---|---|
| 1000 | 1000 | 1000 | 1.00 | 6 | 100 | 16.67 |
| 10000 | 10000 | 10000 | 1.00 | 38 | 1000 | 26.32 |
| 50000 | 50000 | 50000 | 1.00 | 224 | 5000 | 22.32 |

**Both ratios are real and both are explained:**

- **`read_rows` ratio ≈ 1.00 — structural, explained solely by the
  `EXPLAIN indexes = 1` evidence above.** Both bucket sizes already read
  the entire metric regardless of window width (`Parts 1/3`, `Granules
  6/8`, byte-identical), so there is no *additional* physical read cost
  left for bucket coarseness to add.
- **Candidate ratio ≈ 17–26× — the semantic over-inclusion §2.1 describes,
  now actually demonstrable at CI scale.** With this corpus's
  single-bucket-per-series staggering, a 10-minute window floored to a `1h`
  bucket matches only the series whose `mix64`-staggered bucket happens to
  be the "current" one (`matched_candidates_1h`: 6 / 38 / 224 — close to
  the naive `~C/8 / guard_span ≈ C/(8×24)` order of magnitude), while
  floored to a `1d` bucket (this corpus's 24h window yields only 1–2 `1d`
  buckets total) it matches nearly the metric's entire `job="j0"`
  candidate set (`matched_candidates_1d`: 100 / 1000 / 5000, matching the
  cross-path correctness gate's own full-window `job="j0"` counts exactly).
  This is the effect a `1d` bucket "dragging that whole day's series …
  through label matching" actually looks like, measured directly.

This refines the previous draft of this report, which measured only
`read_rows`, found it saturated at 1.00, and speculated (without direct
evidence) that a churning corpus would be needed to see any over-inclusion
effect. That speculation is now moot: the effect was observable all along,
in the right observable (`matched_candidates`, not `read_rows`), on this
same flat, non-churning corpus.

## Refresh-sweep cost (path 1)

The refresh-sweep table, copied verbatim from this run's `render_markdown`
output:

| bucket_ms | kind | resident_series | wall_ms | read_rows | query_duration_ms |
|---|---|---|---|---|---|
| 3600000 | full_sweep | 50000 | 505.06 | 61000 | 47 |
| 3600000 | incremental_sweep | 2557 | 16.26 | 61000 | 10 |
| 86400000 | full_sweep | 50000 | 545.81 | 61000 | 37 |
| 86400000 | incremental_sweep | 61000 | 126.69 | 61000 | 29 |

`resident_series` for the full sweep (50,000, not 61,000) reflects
`CacheSnapshot::by_fingerprint`'s *global* (not per-metric) dedup: this
corpus's `series_labels(i)` depends only on `i`, so low-ordinal series in
different tiers legitimately share fingerprints (identical label sets —
the documented "two rows sharing a fingerprint carry the exact same label
set" case, docs/architecture.md §5.2); `by_metric` correctly keeps the
tiers' fingerprint lists disjoint per metric (the cross-path gate proves
this).

`read_rows` is **identical** (61,000) across all four rows — the same
structural cause as above: the hand-copied incremental sweep SQL's extra
`AND unix_milli > {last_bucket_floor}` prunes zero additional granules.
The wall-time reduction (16.26–126.69 ms vs. ~505–546 ms) is real but is a
**result-size** reduction (far fewer rows *returned*), not a physical-I/O
reduction.

**Sweep cost, linear projection to the design-target scale (labelled
estimate, not measured).** This run's full-sweep wall time was 505.06 ms
(`1h` pass) / 545.81 ms (`1d` pass) over 61,000 total `metric_series` rows.
The `full` profile's total row count (this generator's staggered,
one-row-per-series model) is `10_000 + 500_000 + 5_000_000 = 5,510,000` —
90.33× this run's row count. Scaling **linearly**:

```
505.06 ms × (5,510,000 / 61,000) ≈ 45,621 ms ≈ 45.6 s
545.81 ms × (5,510,000 / 61,000) ≈ 49,302 ms ≈ 49.3 s
```

**~46–49 seconds** — not hours.

**Where "hours" legitimately belongs: the matcher-timing loop, shown as a
formula (labelled estimate, not measurement).** Path 1's matcher latency
(`cache` rows in the per-cell table above) is `O(candidates in the metric)`
per single `resolve()` call. The reproduction command's default
`--matcher-reps 1000` runs this timed loop once per selector per tier; the
**summed per-selector cache p50s at `metric_50000`** (the four `cache` p50
values in the per-cell table above, same bucket-size pass) are:

```
1h pass: 31.8323 + 37.1641 + 73.9909 + 37.1309 = 180.1182 ms
1d pass: 29.2295 + 32.2718 + 76.8405 + 36.1458 = 174.4876 ms
```

Projecting linearly to the `full` profile's three tiers
(`10_000 + 500_000 + 5_000_000` series, i.e. `0.2 + 10 + 100 = 110.2` times
the `50_000`-series cost each), at the default `--matcher-reps 1000`:

```
hours_per_pass = (Σ_selectors p50_ms_at_50k) × matcher_reps × 110.2 / (1000 × 3600)

1h pass:  180.1182 × 1000 × 110.2 / 3,600,000 ≈ 5.51 h
1d pass:  174.4876 × 1000 × 110.2 / 3,600,000 ≈ 5.34 h
both passes: ≈ 10.85 h
```

This is the honest source of any "hours" claim about an unmodified
full-profile run — **not** the sweep (~46–49 s, above). A practical
full-profile run should reduce `--matcher-reps`; at `--matcher-reps 20` the
same formula projects **both passes ≈ 13 minutes**
(`180.1182 × 20 × 110.2 / 3,600,000 + 174.4876 × 20 × 110.2 / 3,600,000 ≈
0.110 h + 0.107 h ≈ 13.0 min`) — see "Reproduction" below. Both projections
are **linear extrapolations, not measurements**; only a real `full` run
confirms or refutes either.

## §9 target mapping (UNVALIDATED)

| Query | §9 target (nearest analog) | CI-scale (recorded) |
|---|---|---|
| PromQL instant, one metric, ≤100 series | < 50 ms | `metric_1000`/`NarrowEq`: cache 0.2901 ms — UNVALIDATED (design-target corpus not run) |
| PromQL range 24h/60s incl. `rate` + `sum by` | < 150 ms | Not directly exercised (this benchmark targets label *resolution*, not full evaluation) — UNVALIDATED |
| Log label/series discovery, 7d | < 100 ms | Structural analog only (metrics discovery, not logs) — UNVALIDATED |

No target above is marked hit or missed at M2 closure — every row is
CI-scale and not comparable to the §9 figures, per the two-tier evidence
model.

## M3 decision-gate inputs

- **Ship `metric_series_idx`?** The evidence is a genuine, cardinality-
  dependent split, not a uniform verdict either way. On wide scans
  (`Regex5xx`, `NegBroad`) idx loses at every committed cardinality —
  `NegBroad` substantially (1.7–2×, plus a much larger absolute
  `read_rows`) — so a selector mix dominated by negative/wide selectors
  argues against shipping as-is, regardless of scale. On bounded
  positive-equality selectors (`NarrowEq`, `BroadEq`) idx *loses* at 1,000
  series and *wins* by 50,000 (1.5–2.8×) — encouraging only if the
  design-target scale sits well past this corpus's crossover point, which
  this CI-scale evidence cannot determine (it shows the crossover exists
  and roughly where it starts, not where the design-target 5M-series tier
  sits relative to it). The empty-accepting-matcher scope gap (recorded
  above, both directions) and the design-target-scale run are both still
  needed before a ship decision is defensible.
- **Ship incremental refresh?** The wall-time case is encouraging (4–33×
  faster than the full sweep at CI scale, by result size) but the read-cost
  case is not proven (`read_rows` identical to the full sweep — see above).
  A design-target-scale run is needed to make this decision with
  confidence either way.
- **Neither, current state is acceptable?** Cannot be ruled out at CI scale
  — the cache matcher (path 1) is fast in every measured cell (< 90 ms even
  at 50,000 series), and the SQL fallback, while reading the whole metric,
  stayed under 47 ms wall time even at 50,000 series in this report.
  Whether this holds at 5M series is exactly what the `full`-profile run
  answers.

**This report does not close the M3 gate — it was never intended to.**
Per the title ("started on the 5M-series scale corpus") and this issue's
own task-manager-adjudicated rescope, the acceptance criterion "all three
paths benchmarked at each cardinality" is satisfied **as rescoped**: the
committed CI-scale evidence (this report) plus a documented, reproducible
`full`-profile procedure (below) — actually *executing* `500_000`/`5_000_000`
is the tracked manual follow-up that must run **before** the M3 decision
gate closes, not a requirement of this issue.

## Reproduction

CI-scale (what this report's numbers were produced with):

```
cargo xtask bench metrics-labels \
    --http-url http://127.0.0.1:19123 --database pulsus_bench_metrics_ci \
    --profile ci --seed 42 --reps 3 --matcher-reps 200 \
    --out docs/benchmarks/data/metrics-labels-ci.json \
    --report-out /tmp/metrics-labels-ci.md
```

**Full profile (manual — not run in this session; see "Refresh-sweep cost"
above for the labelled cost projections).** The design-target
5M-active-series-per-metric corpus, with `--matcher-reps` reduced from the
`1000` default to keep the matcher-timing loop's projected cost in minutes
rather than hours:

```
cargo xtask bench metrics-labels \
    --http-url http://<node>:8123 --database pulsus_bench_metrics_full \
    --profile full --seed 42 --reps 5 --matcher-reps 20 \
    --out docs/benchmarks/data/metrics-labels-full.json \
    --report-out docs/benchmarks/m2-metrics-label-resolution-tier2.md
```

This sandboxed session's environment could not sustain the `full` profile
(the sandbox is not provisioned for a multi-hour, multi-GB run); the
CI-scale numbers above are the largest evidence sustainable here, honestly
marked as such per the two-tier model's precedent (`m1-logs-read-path.md`).

## Out of scope (per the architect plan)

`metric_series_idx` DDL was never added to `pulsus-schema`'s migration
catalog, `run_init`, or `LabelCache` — both remain M3 ship decisions. No
change to `logql`'s (M1) benchmark behaviour beyond the mechanical
`query_log.rs` relocation. No new PRNG, bulk-load path, or JSON crate
dependency. No wall-clock latency asserted against §9 targets. The `full`
profile was not run in CI and no `full`-scale JSON artifact is committed.
Generalizing the idx prototype's SQL to handle empty-accepting matchers is
explicitly deferred to the M3 ship design (see "Harness" above), not fixed
here.
