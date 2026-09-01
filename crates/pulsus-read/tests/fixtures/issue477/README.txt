Issue #477 wave 2 — the six criteria the wave-1 review declined, staged.

The review marked Q23, Q25, Q26, Q27 and Q28/Q29 `[unverified]` and
refused to infer their answers from source, which was the correct call:
their mutation matrices, archive trees, diagnostic streams and fixpoint
corpora lived in a scratch directory and were thrown away. A criterion
nobody can re-run is not a criterion. Each is committed here with the
command that runs it.

  Q23  ac10i_trees/   the stale-unit spelling scan over six trees
       cargo nextest run -p pulsus-read --test traces_metrics_sql \
         -E 'test(=the_stale_unit_scan_answers_the_committed_verdict_on_every_staged_tree)'

  Q25  ac14_rows/     the clock-ownership rule over 17 inputs
       cargo nextest run -p pulsus-read --test traces_metrics_live \
         -E 'test(=the_clock_scan_answers_the_committed_verdict_on_every_staged_input)'

  Q29  ac14_forms/    18 declaration forms x 3 variants
       cargo nextest run -p pulsus-read --test traces_metrics_live \
         -E 'test(=every_declaration_form_owns_its_own_clock_read)'

  Q28  fixpoint/      the rename instrument's well-formedness rules,
                      over 19 malformed, 6 synthetic and 4 real streams
       python3 crates/pulsus-read/tests/fixtures/issue477/fixpoint/rows.py

  Q26  toolchain/     what `cargo check` propagates, demands and cannot
  Q27               see, and what `cargo test --doc` is blind to
       CARGO_TARGET_DIR=<outside any source tree> CARGO_INCREMENTAL=0 \
         bash crates/pulsus-read/tests/fixtures/issue477/toolchain/toolchain.sh

The three Rust runners are ordinary tests and run in `--workspace`; the
two scripts are not, and are run by hand. The toolchain script EDITS THE
WORKING TREE and restores it, so it refuses to start on a dirty one.

What is NOT reproduced, and why: several rows of the plan's published
tables compared the current rule against rules that were superseded
before implementation and exist nowhere in this tree (Q23's `v8` and `v9`
columns, Q25's `V2n` and its whole `old (column rule)` column). Those are
history of the plan, not properties of the shipped gates, and they are
not re-created here. Where a figure in the plan was a count over the real
1 500-line domain file or the two real metrics roots, this corpus's own
count is different and is committed as measured — the historical figures
are re-derivable by pointing the same rule at `2f78c53`.
