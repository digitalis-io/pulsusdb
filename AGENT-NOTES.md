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

## Break campaign (done except the findings below)

Runner `agent-work/coder507gk-notes/breaks/run.py`; specs `breaks/hermetic.py`
(55) and `breaks/live.py` (24); one output file per break in
`breaks/out/<id>.txt` (head, command, exit, full output). Each break is
applied alone to a clean tree and restored with `git checkout -- <file>`.

- Hermetic, run at `dcd5a4c8`: 53 red. The 2 that stayed green were fixed at
  `1c314e2f` and re-run red: c12 (criterion 12's four cases added to
  `logqltest_corpus::eval_approx_is_admitted_only_where_the_aggregation_lowers`)
  and bk13b (the range state added to
  `client_agg::tests::a_preserved_error_passes_the_check_and_a_failed_conversion_counts_zero`).
- Live, run at `1c314e2f` against the local 26.3 server and the two-shard
  cluster: 23 red, 1 green.
  - **L31 was green, now fixed.** Mapping server code 159 in S1 to today's
    route left `the_key_statement_timeout_is_the_timeout_response` green,
    because the client's stream deadline always arrived first. The test now
    runs both arms: the server's own `max_execution_time` (0.3 s) below a
    20-second client deadline, and a 1-second client deadline with no server
    limit. The test-only knobs carry the server limit into the statement's
    own `SETTINGS`, which the server honours over the request's. Both breaks
    are red at `84aa62c3`: L31 `Ok(Matrix(...))` where the timeout response
    is required; L31t `[(Key, 159), (Raw, 0)]` where no raw scan may follow.
- Control at `1c314e2f` before the live breaks: `query_log_gates` +
  `explain_indexes` 74 run, 74 passed; `logs_detected_live` 7 run, 7 passed;
  the two-shard `the_undecided_rows_come_from_one_read` 1 run, 1 passed.

## Measurements (set up, not run)

- Two detached measurement worktrees, uncommitted harness files inside:
  `agent-work/coder507gk-main-wt` (at `874b5619`) and
  `agent-work/coder507gk-head-wt` (at `1c314e2f`). Each has
  `crates/pulsus-read/tests/zz_arch507gk8_parser_cost.rs` (the plan's
  per-line cost harness) and the plan's `zz_arch507gk_perf` appended to
  `crates/pulsus-read/tests/query_log_gates.rs` (the head copy also labels
  key and lane statements). Remove both worktrees after measuring
  (`git worktree remove --force`).
- Inputs regenerated from the plan's generators in
  `agent-work/coder507gk-notes/cost/`: `logfmt_ordinary_lines.tsv` (SHA-256
  prefix `a592611b02bc1110`), `unpack_ordinary_lines.hex`
  (`f9dd6b1c39e90d46`); both equal the reviewer's.
- Scripts: `cost/parser_cost.sh` (criteria 16, 28) and `cost/perf.sh`
  (criterion 17), queries in `cost/perf_queries*.txt`. Not yet done: build
  the release/test binaries into `cost/bins/` (`cost_main`, `cost_head`,
  `qlg_main`, `qlg_head`), load the realistic corpus into
  `c507gk2_perfsrc.ls_real` and `ls_realu` (the plan's SQL, revision 5
  appendix 1), write `cost/base_ns.txt`, then run both scripts.

## Containers

Stopped (not removed) at the pause: `coder507gk-ch` (HTTP 58123, holds the
`c507gk_*` and `c507gk2_*` test databases), `coder507gk-keeper`,
`coder507gk-shard1` (58221), `coder507gk-shard2` (58222), and the reference
containers `coder507gk-ref`, `coder507gk-ref463`. Start them again with
`podman start <name>` (keeper before the shards). At the end: drop the
`c507gk_*`/`c507gk2_*` databases, remove all six containers and the
`coder507gk-net` network.

## Exact next step

1. Run the measurements (criteria 16, 17, 28) and keep their output.
2. Full gauntlet: `cargo nextest run --workspace`, `cargo test --workspace --doc`,
   clippy with `-D warnings`, `cargo fmt --all -- --check`, the live suites.
3. Stop and report to the task-manager (option A waits for the verdict).

**No attribution lines anywhere** (owner): no session link and no tool name
in a commit message, a pull request, an issue comment, `docs/` or code. The
branch was rewritten once to remove the trailers it used to carry; nothing
had been pushed.

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
- Criterion 23's two cap breaks are emulations: R1's 600 KB undecided line
  was removed in revision 8, so "today's staging cap in L" is applied to the
  key route's fold instead (L23a), and "retained window points on the
  partials" counts folded samples (L23b). Both red.
- Criterion 12's refused "filter after the unwrap" case is written under an
  outer `sum` so that the filter is the only reason it is refused.
