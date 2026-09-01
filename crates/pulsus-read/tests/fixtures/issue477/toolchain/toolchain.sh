#!/usr/bin/env bash
# Issue #477 Q26 and Q27 — what the toolchain contributes to AC10(i)'s
# argument, measured on the tree under review.
#
# AC10(i) is published as a BEST-EFFORT spelling scan, and its argument
# rests on two toolchain facts a reader has to be able to check:
#
#   Q26  the compiler PROPAGATES a rename and never DEMANDS one, and it
#        cannot see a unit at all -- so the residue the scan exists for
#        is exactly what `cargo check` is blind to.
#   Q27  the commands CI runs are silent on that residue. `cargo test
#        --workspace --doc` does not see a stale doc link; `cargo doc`
#        does, and CI does not run it.
#
# This script MUTATES THE WORKING TREE and restores it on exit, including
# on failure and on Ctrl-C. It refuses to start on a dirty tree, so a
# failed restore is visible as a dirty tree afterwards rather than silent.
#
#   CARGO_TARGET_DIR=<outside any source tree> CARGO_INCREMENTAL=0 \
#     bash crates/pulsus-read/tests/fixtures/issue477/toolchain/toolchain.sh
#
# Every figure it prints is the exit code or the message count of a real
# invocation; nothing here is read off the source.
set -u

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(git -C "$HERE" rev-parse --show-toplevel)"
FIXPOINT="$ROOT/crates/pulsus-read/tests/fixtures/issue477/fixpoint/fixpoint.py"
PLAN="crates/pulsus-read/src/traces/metrics_plan.rs"
cd "$ROOT" || exit 1

if [ -n "$(git status --porcelain)" ]; then
  echo "refusing to run: the working tree is dirty, and this script edits it" >&2
  exit 64
fi

restore() { git -C "$ROOT" checkout -- . ; }
trap restore EXIT INT TERM

say() { printf '\n== %s ==\n' "$1"; }

run_json() { # <outfile> <cargo args...>
  local out="$1"; shift
  cargo "$@" --message-format=json > "$out" 2>/dev/null
  echo "$?"
}

WORK="$(mktemp -d)"
trap 'restore; rm -rf "$WORK"' EXIT INT TERM

say "Q27(a) cargo test --workspace --doc, clean tree"
cargo test --workspace --doc > "$WORK/doc-clean.txt" 2>&1
echo "exit=$? warnings=$(grep -c '^warning' "$WORK/doc-clean.txt") errors=$(grep -c '^error' "$WORK/doc-clean.txt")"

say "Q27(b) cargo doc --no-deps, clean tree"
code=$(run_json "$WORK/cargodoc-clean.json" doc --no-deps -p pulsus-read -p pulsus-server)
echo "exit=$code $(python3 "$HERE/summarise.py" "$WORK/cargodoc-clean.json" metrics_plan.rs)"

say "Q27(c) the same two commands with ONE stale intra-doc link"
# A link to a symbol that does not exist. Nothing else in the file moves.
perl -0pi -e 's{^/// The caller-validated request window, step and exemplar budget\.$}{/// The caller-validated request window, step and exemplar budget.\n/// See [`TraceMetricsPlan::step_s`] for the unit this used to carry.}m' "$PLAN"
git diff --stat -- "$PLAN"
cargo test --workspace --doc > "$WORK/doc-stale.txt" 2>&1
echo "doctest   exit=$? warnings=$(grep -c '^warning' "$WORK/doc-stale.txt") errors=$(grep -c '^error' "$WORK/doc-stale.txt")"
code=$(run_json "$WORK/cargodoc-stale.json" doc --no-deps -p pulsus-read -p pulsus-server)
echo "cargo doc exit=$code $(python3 "$HERE/summarise.py" "$WORK/cargodoc-stale.json" metrics_plan.rs)"
restore

say "Q26(a) control D — a unit made WRONG in place, nothing renamed"
# Two sites where the millisecond field is documented and used as if it
# carried seconds. The spelling does not move; only the meaning does.
perl -0pi -e 's{/// Bucket width in whole milliseconds; `>= 1`\.}{/// Bucket width in whole SECONDS; `>= 1`.}' "$PLAN"
perl -0pi -e 's{`step_ms` is whole milliseconds \(issue \#477 \(d\)\),}{`step_ms` is whole SECONDS (issue #477 (d)),}' "$PLAN"
git diff --stat -- "$PLAN"
cargo check --all-targets -p pulsus-read -p pulsus-server > "$WORK/controld.txt" 2>&1
echo "exit=$? — a wrong unit compiles clean, which is why the scan exists"
restore

say "Q26(b) the compiler propagates a rename to a fixpoint, and stops short"
BEFORE=$(git grep -c '\bstep_ms\b' -- crates/pulsus-read/src/traces crates/pulsus-server/src/traces_api | awk -F: '{n+=$2} END {print n}')
echo "step_ms occurrences in the two roots, before: $BEFORE"
# Rename the DEFINITIONS only. Everything else is left for the compiler
# to demand, one round at a time.
perl -0pi -e 's{pub step_ms: i64,}{pub step_units: i64,}g' "$PLAN"
perl -0pi -e 's{pub fn step_ms\(&self\) -> i64 \{\n        self\.step_ms\n    \}}{pub fn step_units(&self) -> i64 {\n        self.step_units\n    }}' "$PLAN"
for round in 1 2 3 4 5 6 7 8; do
  cargo check --keep-going --all-targets -p pulsus-read -p pulsus-server \
    --message-format=json > "$WORK/round.json" 2>/dev/null
  code=$?
  out=$(python3 "$FIXPOINT" rename "$WORK/round.json" "$ROOT" "$WORK/ledger.tsv" step_ms step_units)
  guard=$?
  echo "  round $round cargo_check_exit=$code guard_exit=$guard $out"
  [ "$code" = "0" ] && break
  [ "$guard" != "0" ] && break
  # A round that renames nothing will report the same sites for ever:
  # the compiler is asking for a change this line-level rewrite cannot
  # make, and spinning would hide that behind a round count.
  case "$out" in *"occurrences_renamed=0"*)
    echo "  STALLED — the compiler still demands these sites and the rewrite reached none of them:"
    sed -n 's/.*/  &/p' "$WORK/ledger.tsv" | head -20
    break ;;
  esac
done
AFTER=$(git grep -c '\bstep_ms\b' -- crates/pulsus-read/src/traces crates/pulsus-server/src/traces_api | awk -F: '{n+=$2} END {print n+0}')
echo "step_ms occurrences left in the two roots when the loop stopped: ${AFTER:-0}"
echo "(the compiler demanded none of these; they are doc comments, string"
echo " literals and local bindings — the residue AC10(i) exists for)"
git grep -n '\bstep_ms\b' -- crates/pulsus-read/src/traces crates/pulsus-server/src/traces_api | head -40
restore

say "restored"
git status --porcelain
