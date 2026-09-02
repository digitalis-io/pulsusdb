Issue #477 Q28 — the fixpoint rename instrument and its input corpus.

`fixpoint.py` is the instrument that produced AC10(i)'s compiler
partition: it rewrites a banned spelling at every error-level primary
span a `cargo ... --message-format=json` stream reports, iterating to a
fixpoint. What it will REFUSE to act on is part of the claim, because a
partial rewrite driven by a malformed stream would produce a partition
nobody could tell from a real one. W1-W6 are documented in the script.

`streams/` holds 29 inputs:

  m01..m19  malformed, one per rule/reason pair, exit 2
  a01..a06  well-formed synthetic streams, exit 0
  r01..r03  REAL `cargo check --message-format=json` captures (clean,
            warning-only, and one type error with a secondary span),
            paths scrubbed, exit 0
  r04       a REAL `cargo test` transcript — a stream that interleaves
            harness text on the same handle, refused at W1

`tree/` is the staged root the guard confines rewrites to; `outside/` is
the file two of the malformed streams aim at, and `tree/escape.rs` is a
symlink into it, which is how a path with no `..` component can still
leave the root. `rows.py` copies both per stream, so an accepted stream
that rewrites the tree cannot make the next row's unchanged-tree
assertion vacuous.

Run:  python3 crates/pulsus-read/tests/fixtures/issue477/fixpoint/rows.py

It prints the table and compares it with `expected_rows.txt`; exit 1
prints the diff. `--update` is the only way that file is written.
