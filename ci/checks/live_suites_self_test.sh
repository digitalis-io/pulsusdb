#!/usr/bin/env bash
# Issue #843 part 3: self-test for ci/live-suites.sh, the runner that starts
# a job's live suites 4 at a time from its list in ci/suites/. Each case runs
# the committed runner over a list of fake suites in a throwaway directory.
# Run from the repo root.
set -euo pipefail

ROOT="$PWD"
T="$(mktemp -d)"
trap 'rm -rf "$T"' EXIT
pass=0
ok()  { pass=$((pass + 1)); echo "ok   $1"; }
bad() { echo "FAIL $1" >&2; [ -f "$T/out" ] && cat "$T/out" >&2; exit 1; }

# run_list <list> <out> [VAR=VALUE...]: the runner over <list>, its status in $status.
run_list() {
    local list="$1" out="$2"; shift 2
    set +e
    env CARGO_TARGET_DIR="$T/target" "$@" "$ROOT/ci/live-suites.sh" "$list" >"$out" 2>&1
    status=$?
    set -e
}

# A fake suite: three marked lines with pauses between them, so suites that
# run together interleave if their output is not kept apart.
lines() { echo "echo '$1: line 1'; sleep 0.3; echo '$1: line 2'; sleep 0.3; echo '$1: line 3'"; }

# ---- 1: a failure is named; a lane runs its members one after another;
#         each suite's output is printed on its own.
mkdir -p "$T/t1"
libtest_failure="echo 'running 2 tests'; echo 'test b::w ... ok'; echo 'test b::x ... FAILED'; \
echo; echo 'failures:'; echo; echo '---- b::x stdout ----'; echo 'assertion failed'; echo; \
echo 'failures:'; echo '    b::x'; echo; \
echo 'test result: FAILED. 1 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s'; exit 101"
stamp() { echo "date +%s.%N > \$OUT/$1.$2"; }
{
    printf '# suite\tlane\twhen\tenv\tcommand\n'
    printf 'a\tpool\tall\tOUT=%s\t%s\n' "$T/t1" "$(lines a)"
    printf 'b\tpool\tall\tOUT=%s\t%s; %s\n' "$T/t1" "$(lines b)" "$libtest_failure"
    printf 'l1\tl\tall\tOUT=%s\t%s; %s; sleep 1; %s\n' "$T/t1" "$(stamp l1 start)" "$(lines l1)" "$(stamp l1 end)"
    printf 'l2\tl\tall\tOUT=%s\t%s; %s; %s\n' "$T/t1" "$(stamp l2 start)" "$(lines l2)" "$(stamp l2 end)"
} >"$T/t1.tsv"
run_list "$T/t1.tsv" "$T/out"
cp "$T/out" "$T/out1"

[ "$status" -eq 1 ] || bad "1: a failing suite must exit 1, got $status"
ok "1: exit 1 when a suite fails"

grep -qP '^b\tFAILED\t' "$T/out1" || bad "1: the summary does not list suite b as FAILED"
grep -qP '^a\tok\t' "$T/out1" || bad "1: the summary does not list suite a as ok"
grep -qx 'b: b::x' "$T/out1" || bad "1: the summary does not name test b::x of suite b"
if grep -q '^b: b::w$' "$T/out1"; then bad "1: a passing test is named as failed"; fi
ok "1: the summary names suite b and its test b::x"

for f in l1.start l1.end l2.start l2.end; do
    [ -s "$T/t1/$f" ] || bad "1: lane member did not record $f"
done
awk -v s1="$(cat "$T/t1/l1.start")" -v e1="$(cat "$T/t1/l1.end")" -v s2="$(cat "$T/t1/l2.start")" \
    'BEGIN { exit !(s1 < e1 && e1 <= s2) }' || bad "1: lane l's members overlap or ran out of list order"
ok "1: lane l runs l1, then l2, never together"

for s in a b l1 l2; do
    awk -v s="$s" '
        index($0, "::group::" s " ") == 1 { inside = 1; groups++; next }
        $0 == "::endgroup::" { inside = 0; next }
        inside && /^(a|b|l1|l2): line [0-9]$/ { if (index($0, s ": ") != 1) stray++; else own++ }
        END { exit !(groups == 1 && own == 3 && stray == 0) }
    ' "$T/out1" || bad "1: the group for suite $s does not hold exactly its own three lines"
done
ok "1: each group holds only its own suite's lines"

# ---- 2: a line whose `when` does not match the event is skipped, and listed.
mkdir -p "$T/t2"
{
    printf 'push_only\tpool\tnot-pull-request\tOUT=%s\ttouch "$OUT/push_only.ran"\n' "$T/t2"
    printf 'every\tpool\tall\tOUT=%s\ttouch "$OUT/every.ran"\n' "$T/t2"
} >"$T/t2.tsv"
run_list "$T/t2.tsv" "$T/out" GITHUB_EVENT_NAME=pull_request
[ "$status" -eq 0 ] || bad "2: a run with nothing failing must exit 0, got $status"
[ -e "$T/t2/every.ran" ] || bad "2: the suite with when=all did not run"
[ ! -e "$T/t2/push_only.ran" ] || bad "2: the when=not-pull-request suite ran on a pull request"
grep -qP '^push_only\tskipped' "$T/out" || bad "2: the skipped suite is not listed as skipped"
[ "$(grep -cP '^[a-z_]+\tskipped' "$T/out")" -eq 1 ] || bad "2: expected exactly 1 skipped suite"
ok "2: when=not-pull-request is skipped on a pull request and listed"

[ "$pass" -eq 5 ] || bad "expected 5 checks, ran $pass"
echo "live-suites self-test: all $pass checks passed"
