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

## Measurements (done)

Output and a summary in `agent-work/coder507gk-notes/cost/`
(`parser_cost_out3.txt`, `perf_out.txt`, `measurements_summary.txt`).

- Criterion 16 and 28: the plan's per-line cost harness, release, five
  rounds with the base commit and the branch interleaved. On the ordinary
  lines the branch's range lies below the base's for all five shapes. The
  K ladder to 16,000 names: each step at most 6.87 times a quarter as many
  names for `| logfmt` and `| unpack` (a quadratic cost would give 16).
- Criterion 17: the plan's engine harness over the realistic corpus
  (2,000,000 rows in one hour) and its undecided-heavy variant, three
  repetitions per window, both builds interleaved.
- The two measurement worktrees and their build subdirectories are removed,
  and the three measurement databases are dropped.

## Containers

Running again since the pause was lifted: `coder507gk-ch` (HTTP 58123, holds the
`c507gk_*` and `c507gk2_*` test databases), `coder507gk-keeper`,
`coder507gk-shard1` (58221), `coder507gk-shard2` (58222), and the reference
containers `coder507gk-ref`, `coder507gk-ref463` (stopped; they are the
reference build and are only needed when a capture has to be retaken). Start
any of them with `podman start <name>` (the keeper before the shards). At the end: drop the
`c507gk_*`/`c507gk2_*` databases, remove all six containers and the
`coder507gk-net` network.

## Exact next step

Option A (the ingest namer) is the only work left. Addendum 4 is with the
reviewer; build nothing until the verdict arrives. Then: implement, re-run
the gauntlet, push to the GitHub remote, open the pull request, and post the
implementation notes.

**No attribution lines and no tool names anywhere** (owner): not in a commit
message, a pull request, an issue comment, `docs/` or code. The branch was
rewritten once to remove the trailers it used to carry; nothing had been
pushed.

## The gauntlet at `191a14af`

- `cargo nextest run --workspace --no-fail-fast`: 7193 run, 7193 passed, 36
  skipped.
- `cargo test --workspace --doc`: passed.
- `cargo clippy --workspace --all-targets -- -D warnings`: exit 0.
- `cargo fmt --all -- --check`: exit 0.
- `cargo nextest run -p pulsus-read --no-fail-fast`: 2277 run, 2277 passed.
- Live: `query_log_gates` + `explain_indexes` 74 passed; `injected_settings`
  3 passed; `logs_detected_live` 7 passed; the two-shard
  `the_undecided_rows_come_from_one_read` 1 passed; and
  `logql_line_filter_differential`, `query_text_cap_live`,
  `series_stream_cap`, `patterns_explain` all passed.
