Issue #477 Q23 — the AC10(i) stale-unit scan over six trees.

Six directories, each a small stand-in for the two metrics roots
(`crates/pulsus-read/src/traces` + `crates/pulsus-server/src/traces_api`)
in one state. Every `.txt` here is copied to `<tmp>/<tree>/<stem>.rs` by
the runner and scanned by the SHIPPED rule — `stale_spellings` in
`crates/pulsus-read/tests/traces_metrics_sql.rs` — so the rows exercise
the gate that ships, not a copy of it.

Run:
  cargo nextest run -p pulsus-read --test traces_metrics_sql \
    -E 'test(=the_stale_unit_scan_answers_the_committed_verdict_on_every_staged_tree)'

The six states, one per mutation class the gate's own doc comment names.
The verdict and the occurrence count are the runner's own output, pasted:

  base  files=2 occurrences=7 step_ms=0 RED   nothing renamed
  e1    files=2 occurrences=1 step_ms=3 RED   code renamed, one prose
                                              mention left
  e2    files=2 occurrences=4 step_ms=3 RED   one root renamed, the other
                                              untouched
  e3    files=2 occurrences=1 step_ms=3 RED   renamed except inside a
                                              string literal
  e3b   files=3 occurrences=0 step_ms=2 GREEN all five spellings renamed
                                              to ANOTHER wrong name,
                                              consistently
  e4    files=2 occurrences=2 step_ms=3 RED   code renamed, residue in
                                              comments only — the state
                                              `rustc` reports clean

`e3b` is GREEN on purpose: it is the published, accepted defeat of this
best-effort scan, which compares spellings and not meanings, so it can
tell an inconsistent rename from a consistent one and nothing more.
`base` is RED for two reasons at once — seven banned occurrences AND no
`step_ms` anywhere, which is the anti-vacuity half firing.

These reconstruct the six mutation CLASSES. The occurrence counts the
plan published (75 / 73 lines / 5 files on `base`, and the per-tree
figures) belong to the real tree at `2f78c53` and are re-derivable there
directly; they are not properties of this fixture, and the runner asserts
this fixture's own committed counts instead.
