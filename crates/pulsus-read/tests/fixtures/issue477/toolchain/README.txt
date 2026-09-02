Issue #477 Q26 and Q27 — the toolchain's contribution to AC10(i).

Run (the script edits the working tree and restores it, so it refuses to
start on a dirty one). Every restore is VERIFIED: a failed one prints the
paths still dirty and exits 65 rather than printing "restored" and 0. The
transcript below predates that fix — the wave-4 review found this script
reporting a successful restore in an environment whose git index was not
writable, and the wave-4 re-run is what the fixed script produces.

  CARGO_TARGET_DIR=<outside any source tree> CARGO_INCREMENTAL=0 \
    bash crates/pulsus-read/tests/fixtures/issue477/toolchain/toolchain.sh

Measured on this branch at d30817dbf32b74ffeaf86521a77e9a24e633f49a, pasted
verbatim below, including the residue listing. These are NOT an expectation
file and nothing asserts them: they are counts over the tree as it stood,
and they move when the tree does. What a reviewer re-runs and reads is the
SHAPE — exit codes, which of the two commands sees the planted defect, and
the compiler messages at the stalled sites.

WHAT CHANGED IN THIS FILE ON THE WAVE-2 REVIEW'S RULING. The previous
version said the script stops at eight sites that are "doc comments,
string literals inside test messages, and local bindings", and said the
same of the residue it lists. Neither was true. The eight sites are
ordinary Rust code — an accessor body, a struct-literal key, a field
shorthand, an assertion and a local binding — and the residue is a
mixture including private field declarations and function parameters. The
script now prints the compiler's own message and the mutated source line
at each stalled site, so the reason is shown rather than named, and it
states in as many words what the run does and does not establish. The
correction is kept visible here rather than quietly rewritten: a wrong
description of an instrument's own bound is what a reader uses to judge
what the result still covers.

READING THE Q26(b) OUTPUT. The seed renames two `pub step_ms: i64,` field
declarations and one accessor, and nothing else. Round 1 shows the
compiler demanding 54 sites the seed never touched and the rewrite
answering 34 of them. Round 2 stalls: the rewrite substitutes the old name
on the line the primary span points at, and at all eight remaining sites
that line already holds the new name — the edit the compiler wants is on a
different line (the private `step_ms: i64` field declaration, or the
`for (start_s, end_s, step_ms, …)` binding a shorthand refers to). That is
a bound on the instrument. The loop stops on the first round that makes no
progress rather than spinning, because a round count would hide the stall.

VERBATIM OUTPUT
===============
== Q27(a) cargo test --workspace --doc, clean tree ==
exit=0 warnings=0 errors=0

== Q27(b) cargo doc --no-deps, clean tree ==
exit=0 compiler_messages=247 broken_intra_doc_links=60 at_metrics_plan.rs=0 []

== Q27(c) the same two commands with ONE stale intra-doc link ==
 crates/pulsus-read/src/traces/metrics_plan.rs | 1 +
 1 file changed, 1 insertion(+)
doctest   exit=0 warnings=0 errors=0
cargo doc exit=0 compiler_messages=248 broken_intra_doc_links=61 at_metrics_plan.rs=1 ['crates/pulsus-read/src/traces/metrics_plan.rs:34']

== Q26(a) control D — a unit made WRONG in place, nothing renamed ==
 crates/pulsus-read/src/traces/metrics_plan.rs | 4 ++--
 1 file changed, 2 insertions(+), 2 deletions(-)
exit=0 — a wrong unit compiles clean, which is why the scan exists

== Q26(b) the compiler propagates a rename to a fixpoint, and stops short ==
step_ms occurrences in the two roots, before: 94
  round 1 cargo_check_exit=101 guard_exit=0 success=False error_msgs=54 error_sites=54 occurrences_renamed=34
  round 2 cargo_check_exit=101 guard_exit=0 success=False error_msgs=8 error_sites=8 occurrences_renamed=0
  STALLED — the compiler still demands these sites and the rewrite reached none of them:
  crates/pulsus-read/src/traces/metrics_plan.rs	1276	no-op
  crates/pulsus-read/src/traces/metrics_plan.rs	1287	no-op
  crates/pulsus-read/src/traces/metrics_plan.rs	1408	no-op
  crates/pulsus-read/src/traces/metrics_plan.rs	1868	no-op
  crates/pulsus-read/src/traces/metrics_plan.rs	332	no-op
  crates/pulsus-read/src/traces/metrics_plan.rs	332	no-op
  crates/pulsus-read/src/traces/metrics_plan.rs	756	no-op
  crates/pulsus-read/src/traces/metrics_plan.rs	756	no-op
  what the compiler is asking for at each (its own message, and the
  line as the mutated tree has it):
  crates/pulsus-read/src/traces/metrics_plan.rs:332
      compiler: error[E0615]: attempted to take value of method `step_units` on type `&TraceMetricsPlan`
      line now: self.step_units
  crates/pulsus-read/src/traces/metrics_plan.rs:332
      compiler: error[E0615]: attempted to take value of method `step_units` on type `&metrics_plan::TraceMetricsPlan`
      line now: self.step_units
  crates/pulsus-read/src/traces/metrics_plan.rs:756
      compiler: error[E0560]: struct `TraceMetricsPlan` has no field named `step_units`
      line now: step_units: params.step_units,
  crates/pulsus-read/src/traces/metrics_plan.rs:756
      compiler: error[E0560]: struct `metrics_plan::TraceMetricsPlan` has no field named `step_units`
      line now: step_units: params.step_units,
  crates/pulsus-read/src/traces/metrics_plan.rs:1276
      compiler: error[E0425]: cannot find value `step_units` in this scope
      line now: step_units,
  crates/pulsus-read/src/traces/metrics_plan.rs:1287
      compiler: error[E0425]: cannot find value `step_units` in this scope
      line now: assert_eq!(axis.step_units, step_units);
  crates/pulsus-read/src/traces/metrics_plan.rs:1408
      compiler: error[E0425]: cannot find value `step_units` in this scope
      line now: step_units,
  crates/pulsus-read/src/traces/metrics_plan.rs:1868
      compiler: error[E0425]: cannot find value `step_units` in this scope
      line now: let params = MetricsParams { step_units, ..PARAMS };
  primary error spans: 8
step_ms occurrences left in the two roots when the loop stopped: 56

WHY IT STOPPED, and it is a limit of THIS INSTRUMENT:
  the rewrite is line-local — it substitutes the old name on the line
  the compiler's primary span points at. Every stalled site above is a
  no-op because that line no longer holds the old name; the edit the
  compiler is asking for is on a DIFFERENT line, and the messages
  printed beside each site say which. The sites are ordinary Rust
  code, not comments or literals.

WHAT THIS RUN ESTABLISHES:
  - a wrong unit compiles clean (Q26(a) above);
  - the compiler PROPAGATES a seed rename without being asked: renaming
    two struct fields and one accessor made it demand sites the seed
    never touched, and 34 of the 94 occurrences were rewritten
    from its demands alone;
  - it never DEMANDED a rename: it went quiet as soon as the types
    agreed, which is the Q26 claim.

WHAT IT DOES NOT ESTABLISH:
  nothing at all about the 56 occurrences left. They were not
  reached, and this run cannot say whether the compiler would demand
  them under a rewrite able to express the stalled sites. They are
  listed below as a record of what was NOT reached — not as a
  classification of it. Reading that list shows the mixture: private
  field declarations, function parameters, doc comments and panic
  messages alike, i.e. whatever the seed rename's blast radius missed.
crates/pulsus-read/src/traces/metrics_plan.rs:34:/// `step_ms` is whole milliseconds (issue #477 (d)), already defaulted by
crates/pulsus-read/src/traces/metrics_plan.rs:48:/// `first_ms + i * step_ms`, label `L` covering the RIGHT-CLOSED instant
crates/pulsus-read/src/traces/metrics_plan.rs:81:    /// `ceil(ts_ms / step_ms) * step_ms`. An instant landing exactly on a
crates/pulsus-read/src/traces/metrics_plan.rs:213:    step_ms: i64,
crates/pulsus-read/src/traces/metrics_plan.rs:378:        self.step_ms as f64 / 1_000.0
crates/pulsus-read/src/traces/metrics_plan.rs:1210:    /// Every row is `(start_s, end_s, step_ms) -> (points, first_ms,
crates/pulsus-read/src/traces/metrics_plan.rs:1215:        for (start_s, end_s, step_ms, points, first_ms, last_ms) in [
crates/pulsus-read/src/traces/metrics_plan.rs:1280:                .unwrap_or_else(|e| panic!("{start_s}..{end_s} step {step_ms}ms: {e}"));
crates/pulsus-read/src/traces/metrics_plan.rs:1285:                "{start_s}..{end_s} step {step_ms}ms"
crates/pulsus-read/src/traces/metrics_plan.rs:1397:        // (step_ms, width_ms, expected)
crates/pulsus-read/src/traces/metrics_plan.rs:1398:        for (step_ms, width_ms, want) in [
crates/pulsus-read/src/traces/metrics_plan.rs:1417:                        "step {step_ms}ms / {width_ms}ms"
crates/pulsus-read/src/traces/metrics_plan.rs:1426:                    "step {step_ms}ms / {width_ms}ms"
crates/pulsus-read/src/traces/metrics_plan.rs:1429:                    panic!("step {step_ms}ms / {width_ms}ms: wanted {want:?}, got {got:?}")
crates/pulsus-read/src/traces/metrics_plan.rs:1867:        for step_ms in [0, -60] {
crates/pulsus-read/src/traces/metrics_plan.rs:1979:        // step_ms = i64::MAX: step_ns only exists in i128; the snapped end
crates/pulsus-read/src/traces/metrics_sql.rs:518:/// smallest multiple of `step_ms` that is `>=` the span's instant.
crates/pulsus-read/src/traces/metrics_sql.rs:559:pub fn range_bucket_expr(step_ms: i64) -> String {
crates/pulsus-read/src/traces/metrics_sql.rs:560:    let step_ns = i128::from(step_ms) * 1_000_000;
crates/pulsus-read/src/traces/metrics_sql.rs:563:         INTERVAL {step_ns} NANOSECOND)) + {step_ms}"
crates/pulsus-read/src/traces/metrics_sql.rs:574:/// (`INTERVAL {step_ms} MILLISECOND`), not seconds: live ClickHouse 24.8
crates/pulsus-read/src/traces/metrics_sql.rs:584:/// nanoseconds, so `step_ms <= i64::MAX / 10^6`.
crates/pulsus-read/src/traces/metrics_sql.rs:591:    step_ms: i64,
crates/pulsus-read/src/traces/metrics_sql.rs:595:        range_bucket_expr(step_ms)
crates/pulsus-read/src/traces/metrics_sql.rs:690:    step_ms: i64,
crates/pulsus-read/src/traces/metrics_sql.rs:696:        range_bucket_expr(step_ms)
crates/pulsus-read/src/traces/metrics_sql.rs:735:    step_ms: i64,
crates/pulsus-read/src/traces/metrics_sql.rs:743:        range_bucket_expr(step_ms)
crates/pulsus-read/src/traces/metrics_sql.rs:808:    step_ms: i64,
crates/pulsus-read/src/traces/metrics_sql.rs:814:        range_bucket_expr(step_ms)
crates/pulsus-read/src/traces/metrics_sql.rs:890:    step_ms: i64,
crates/pulsus-read/src/traces/metrics_sql.rs:895:        range_bucket_expr(step_ms)
crates/pulsus-read/src/traces/metrics_sql.rs:942:    step_ms: i64,
crates/pulsus-read/src/traces/metrics_sql.rs:950:        range_bucket_expr(step_ms)
crates/pulsus-read/src/traces/metrics_sql.rs:971:    step_ms: i64,
crates/pulsus-read/src/traces/metrics_sql.rs:976:        range_bucket_expr(step_ms)
crates/pulsus-read/src/traces/metrics_sql.rs:1004:    step_ms: i64,
crates/pulsus-read/src/traces/metrics_sql.rs:1007:    let inner = exemplar_duration_inner(spans_table, filter, window, step_ms);
crates/pulsus-read/src/traces/metrics_sql.rs:1030:    step_ms: i64,
crates/pulsus-read/src/traces/metrics_sql.rs:1033:    let inner = exemplar_duration_inner(spans_table, filter, window, step_ms);

== restored ==
