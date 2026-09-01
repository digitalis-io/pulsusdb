Issue #477 Q26 and Q27 — the toolchain's contribution to AC10(i).

Run (the script edits the working tree and restores it, so it refuses to
start on a dirty one):

  CARGO_TARGET_DIR=<outside any source tree> CARGO_INCREMENTAL=0 \
    bash crates/pulsus-read/tests/fixtures/issue477/toolchain/toolchain.sh

Measured on this branch at 08df1d5995c36540aafbee9e9829518b384318f2, pasted verbatim. These
are NOT an expectation file and nothing asserts them: they are counts
over the tree as it stood, and they move when the tree does. What a
reviewer re-runs and reads is the SHAPE — exit codes, and which of the
two commands sees the planted defect.

== Q27(a) cargo test --workspace --doc, clean tree ==
exit=0 warnings=0 errors=0

== Q27(b) cargo doc --no-deps, clean tree ==
exit=0 compiler_messages=246 broken_intra_doc_links=60 at_metrics_plan.rs=0 []

== Q27(c) the same two commands with ONE stale intra-doc link ==
doctest   exit=0 warnings=0 errors=0
cargo doc exit=0 compiler_messages=247 broken_intra_doc_links=61 at_metrics_plan.rs=1 ['crates/pulsus-read/src/traces/metrics_plan.rs:34']

  The doctest step is exit 0 with nothing to say on BOTH trees: it is
  structurally blind to the class. `cargo doc` goes 60 -> 61 and the
  added one is exactly the planted link. CI runs the first and not the
  second, which is the whole of the argument.

== Q26(a) control D — a unit made WRONG in place, nothing renamed ==
exit=0 — a wrong unit compiles clean, which is why the scan exists

== Q26(b) the compiler propagates a rename to a fixpoint, and stops short ==
step_ms occurrences in the two roots, before: 86
  round 1 cargo_check_exit=101 guard_exit=0 success=False error_msgs=50 error_sites=50 occurrences_renamed=32
  round 2 cargo_check_exit=101 guard_exit=0 success=False error_msgs=8 error_sites=8 occurrences_renamed=0
  STALLED — the compiler still demands these sites and the rewrite reached none of them:
  crates/pulsus-read/src/traces/metrics_plan.rs	1193	no-op
  crates/pulsus-read/src/traces/metrics_plan.rs	1204	no-op
  crates/pulsus-read/src/traces/metrics_plan.rs	1325	no-op
  crates/pulsus-read/src/traces/metrics_plan.rs	1785	no-op
  crates/pulsus-read/src/traces/metrics_plan.rs	336	no-op
  crates/pulsus-read/src/traces/metrics_plan.rs	336	no-op
  crates/pulsus-read/src/traces/metrics_plan.rs	718	no-op
  crates/pulsus-read/src/traces/metrics_plan.rs	718	no-op
step_ms occurrences left in the two roots when the loop stopped: 50

  Renaming two struct fields and one accessor made the compiler demand
  50 sites; a line-level rewrite reached 32 of them and then stalled on
  eight it cannot express. Fifty occurrences are still there and the
  compiler asked for none of them — the listing the script prints is
  doc comments, string literals inside test messages, and local
  bindings. That is the residue AC10(i) exists for, and it is also why
  AC10(i) is published as best-effort: `cargo check` propagates a
  rename, never demands one, and Q26(a) shows it cannot see a unit at
  all.

  The stall is a bound on the INSTRUMENT, not on the claim, and it is
  reported rather than smoothed over: the loop stops the moment a round
  makes no progress, because a loop that spins on the same eight sites
  hides them behind a round count.
