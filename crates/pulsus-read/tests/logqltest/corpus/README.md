# The `logqltest` corpus directory

Every `.test` file here is discovered from disk, never from a list. Two
sites read this directory and both filter on the `.test` extension:

* `logqltest_corpus.rs::corpus_files` — the hermetic replay and the file
  floor;
* `logqltest_replay.rs::classify` — the provenance/reachability walk that
  the live differential leg replays.

A `.md` file in this directory is therefore invisible to both. This one is
documentation and nothing reads it except the test that checks the rule
below is still true.

## The `bNN` prefix is a batch label, not an identifier

The `bNN` prefix is a human batch label with **no machine meaning**.
Discovery is `read_dir` plus a sort on the full file name; nothing parses
the prefix, nothing keys on it, and no ordering depends on it. It records
which batch of corpus work a file arrived with, which is useful to a
reader and to nobody else.

**Duplicates are expected and already normal.**

Duplicated prefixes on disk today: b8, b10, b21, b24, b25, b26.

That sentence is computed from `read_dir` by
`logqltest_corpus.rs::the_corpus_readme_lists_every_duplicated_batch_prefix`
and must appear here exactly once, so it cannot rot in either direction —
a new duplicate family reddens that test, and so does an entry for a
family that stopped duplicating.

So: **do not rename a file to avoid a prefix collision.** Renaming moves
every `path:line` citation of it (the two `*_sites.tsv` datasets, the
provenance tables, the ledger rows) for no behavioural gain, and issue
#388's rebase declined to rename `b26_json_expr.test` or
`b26_line_filter_pushdown.test` for exactly that reason. Pick the batch
number that says when the file arrived.

## Adding a file

Adding a `.test` file moves several committed counts at once, and each of
them is asserted against a walk of this directory rather than against
prose:

* the file floor in `logqltest_corpus.rs::corpus_dir_is_populated`;
* `TOTAL_DIRECTIVES`, `PROVENANCE_PERMITS`, `REACHABLE` and the two
  reason breakdowns in `logqltest_replay.rs`;
* `CAPTURED` / `TOTAL` and the provenance-marker tables in
  `logqltest_provenance.rs`;
* `PLACED_SPAN_SECS` in `logqltest_replay.rs`, because the live leg's
  placement charges each case its own sample span plus a gap of
  `max(MIN_GAP_SECS, that span)`. A single-entry case costs
  `MIN_GAP_SECS` and nothing else; a wide one costs twice its own span.

`tests/logqltest/PROVENANCE.md` is the authority on what a new row has to
carry and how it must have been measured.
