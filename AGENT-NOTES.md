# Issue #507 — coder hand-over notes (branch `issue-507-group-key`)

Paths below are relative to the repository root unless they start with
`agent-work/`, which is relative to the agent work directory.

This file is a working note for the next coder. It is not a design record
and is removed before the pull request is opened.

## Binding plan

Revision 10 (#507 comments 5658948931, 5658949766, 5658950218, 5658950568)
as deltas on revision 9, which builds on revisions 8 and 7; addendum 1
(5659296931), addendum 2 (5659581366). Addendum 3 (option A, the ingest
namer) FAILED review 5660897193: **do not build option A until the
task-manager sends the verdict.**

## Done (committed on this branch)

- Key route end to end: S1, today's route, L; the fold under the range
  step's rules; 159 and the client deadline are the timeout response.
- Live tests in `crates/pulsus-read/tests/query_log_gates.rs` for criteria
  3, 4, 5, 8, 9, 19 (single node and two shards, with its CI step), 22, 23,
  29, 31, 44; `explain_indexes.rs` criterion 10;
  `crates/pulsus-server/tests/logs_detected_live.rs` criterion 21. All pass
  against a local 26.3 server (prefix `c507gk2`).
- Hermetic tests for criteria 5 (group document), 6, 7, 23 (text cap).
- Reference citations (file, line and pinned tag) in the new code comments;
  stale parser comments rewritten.
- Docs: `docs/reference-defects-we-do-not-copy.md` entries 25–32 (count test
  renamed `…_holds_thirty_two`); `docs/api.md` §2 parser/key-route block;
  `docs/features.md:72` sentences; `docs/configuration.md` and
  `docs/schemas.md` memory and timeout sentences; `docs/query-to-sql.md`
  §1.1 rewritten as "the extracted-field group key", rows in §1.2/§1.3/§8;
  every moved LogQL code citation in `docs/query-to-sql.md` and
  `docs/query-lowering.md` remapped by line mapping against `origin/main`;
  four regenerators run; the reviewed fallback divergence table updated;
  the JSON expression flip for fixture r60 recorded;
  `tests/golden/logql_error_details/oracle_probe.txt` corrected.
- `cargo nextest run -p pulsus-read --no-fail-fast`: all pass after the
  compile-site list gained the probe's site (last full run before that fix:
  2277 run, 1 failed, which that fix addresses).

## In progress

- The break campaign. Runner: `agent-work/coder507gk-notes/breaks/run.py`
  with specs `breaks/hermetic.py` (55 hermetic breaks, run at `dcd5a4c8`)
  and `breaks/live.py` (24 live breaks, written, not yet run). Each break
  needs a clean tree; output per break in `breaks/out/<id>.txt`.
- Hermetic result: 53 red, 2 green, both findings to fix before reporting:
  - c12 green: `logqltest_corpus::eval_approx_is_admitted_only_where_the_aggregation_lowers`
    lacks criterion 12's four cases (bare `sum(sum_over_time(… | json | unwrap c …))`
    and bare `avg_over_time(…) by (x)` admitted; bare `without (x)` and a
    filter after the unwrap refused). Add them, then re-run c12.
  - bk13b green: the range state's failed-conversion zero is not covered by
    `client_agg::tests::a_preserved_error_passes_the_check_and_a_failed_conversion_counts_zero`
    (only the instant state is). Extend the test to the range state, then
    re-run bk13b.

## Next

1. Live breaks: K1, q12 (crit 3); rules off, depth, key budget (crit 4);
   window (8); 241 and ceiling (9); fingerprint IN (10); SETTINGS (11);
   L as two statements, single node and two shards (19); detected fields
   (21); key budget (22); staging cap and retained points in L (23); the
   four rn1/rn2 breaks (29, 42); 159 (31); K2, K3 (44).
2. Measurements: criterion 16 (per-line cost, release, interleaved with a
   build of `874b5619`), 17 (live cost), 28 (K ladder to 16,000).
3. Full gauntlet (nextest workspace + doc tests, clippy, fmt, live suites).
4. Stop and report to the task-manager with deviations (D1–D6, see the
   report draft), then wait for the option A verdict.

## Deviations to report

- D1 crit 23 hermetic fixture; D2 criterion 3 instant rows run as one-point
  range queries; D3 criterion 44 adds a decided row; D4 criterion 19
  compares within the summation bound; D5 criterion 19 corpus 400,000 rows.
- D6 defect numbering: the plan's #29 and #33 were withdrawn, so entries
  are numbered 25–32 contiguously (29 renamed repeat, 30 quoted value,
  31 replacement character) and the grouped `avg_over_time` entry 32 is
  added because its mechanism is now located (the ledger row already said
  "reference defect 32").
- Criterion 24's `parser-extracted vector` grep still returns the main
  sentence: revision 9 withdrew the rename that removed it.
- Criterion 40's first grep returns nothing (plan said "only the quoted old
  requirement"; the replacement text does not use those words).
- The "unwrapped fold removes nothing" break (crit 20) has no target: the
  shipped unwrapped fold is replaced by the key route's fold.
