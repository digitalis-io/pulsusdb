#!/usr/bin/env bash
# Issue #843 part 3: runs one job's live suites 4 at a time.
#
#   ci/live-suites.sh ci/suites/<job>.tsv
#
# The list: one line per suite, five tab-separated fields; `#` lines are
# comments.
#
#   suite    unique within the list; names its log and its summary row
#   lane     `pool`, or a lane name. A lane's members share one slot and run
#            one after another in list order
#   when     `all`; `not-pull-request` (the step's former
#            `if: github.event_name != 'pull_request'`); or
#            `dispatch-or-schedule` (its former `if: github.event_name ==
#            'workflow_dispatch' || github.event_name == 'schedule'`),
#            matched against GITHUB_EVENT_NAME
#   env      the step's own variables, `K=V` separated by spaces
#   command  the step's `run:`, run by `bash -e`. A multi-line `run:` is one
#            line: a backslash-newline becomes a space, any other line break
#            `; `
#
# Before any suite starts, every package's test binaries are built with one
# `cargo test --no-run` per package, so the suites only check freshness. A
# build failure exits 1 before any suite starts. Lines start in list order as
# slots free up. Each suite writes <target>/test-summary/<suite>.log; when it
# ends, the log is printed whole inside one `::group::`. Every suite runs to
# the end; the summary at the end lists each suite's result and seconds, and
# each failed suite's test names. Exit 1 if any suite failed or could not
# start.
set -uo pipefail

SLOTS=4
here="$(cd "$(dirname "$0")" && pwd)"
list="${1:?usage: ci/live-suites.sh <list.tsv>}"
target_dir="${CARGO_TARGET_DIR:-target}"
out_dir="$target_dir/test-summary"

die() { echo "::error::live suites: $*"; exit 1; }

# ---- read and check the list --------------------------------------------
names=() lanes=() whens=() envs=() cmds=()
declare -A seen=()
n=0
while IFS= read -r line || [ -n "$line" ]; do
  n=$((n + 1))
  case "$line" in '' | '#'*) continue ;; esac
  # Tab is whitespace to `read`, which would merge an empty field into the
  # next; split on a character that is not.
  IFS=$'\x1f' read -r suite lane when env cmd extra <<<"${line//$'\t'/$'\x1f'}"
  [ -n "${cmd:-}" ] && [ -z "${extra:-}" ] || die "$list:$n: expected 5 tab-separated fields"
  [[ "$suite" =~ ^[A-Za-z0-9_.-]+$ ]] || die "$list:$n: bad suite name '$suite'"
  [ -z "${seen[$suite]:-}" ] || die "$list:$n: suite '$suite' is listed twice"
  seen[$suite]=1
  [[ "$lane" =~ ^[A-Za-z0-9_.-]+$ ]] || die "$list:$n: bad lane '$lane'"
  case "$when" in all | not-pull-request | dispatch-or-schedule) ;;
    *) die "$list:$n: bad when '$when'" ;; esac
  for kv in $env; do
    [[ "$kv" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || die "$list:$n: bad env entry '$kv'"
  done
  names+=("$suite") lanes+=("$lane") whens+=("$when") envs+=("$env") cmds+=("$cmd")
done <"$list" || die "cannot read $list"
[ "${#names[@]}" -gt 0 ] || die "$list lists no suite"

selected() {
  case "$1" in
    all) return 0 ;;
    not-pull-request) [ "${GITHUB_EVENT_NAME:-}" != pull_request ] ;;
    dispatch-or-schedule) [ "${GITHUB_EVENT_NAME:-}" = workflow_dispatch ] ||
      [ "${GITHUB_EVENT_NAME:-}" = schedule ] ;;
  esac
}

# ---- build every test binary first --------------------------------------
declare -A pkg_tests=()
pkg_order=()
for i in "${!names[@]}"; do
  selected "${whens[$i]}" || continue
  read -ra words <<<"${cmds[$i]}"
  pkg="" test=""
  for j in "${!words[@]}"; do
    case "${words[$j]}" in
      -p) pkg="${words[$((j + 1))]:-}" ;;
      --test) test="${words[$((j + 1))]:-}" ;;
    esac
  done
  [ -n "$pkg" ] && [ -n "$test" ] || continue
  [ -n "${pkg_tests[$pkg]+x}" ] || pkg_order+=("$pkg")
  case " ${pkg_tests[$pkg]:-} " in *" --test $test "*) ;;
    *) pkg_tests[$pkg]="${pkg_tests[$pkg]:-} --test $test" ;; esac
done
for pkg in "${pkg_order[@]}"; do
  # shellcheck disable=SC2086 # the --test list is meant to split
  build=(cargo test --no-run -p "$pkg" ${pkg_tests[$pkg]})
  echo "::group::build $pkg"
  echo "${build[*]}"
  "${build[@]}"
  status=$?
  echo "::endgroup::"
  [ "$status" -eq 0 ] || die "the build failed: ${build[*]} (exit $status); no suite was started"
done

# ---- run -----------------------------------------------------------------
"$here/test-summary.sh" init || echo "::warning::live suites: could not reset $out_dir"
state="$out_dir/live-suites"
mkdir -p "$state" || die "cannot create $state"

# run_suite <index>: one suite, its output to its log; writes
# "<status> <seconds>" to its result file. Status "start" means its command
# could not be started.
run_suite() {
  local i="$1" suite="${names[$1]}" first started status
  local log="$out_dir/${names[$1]}.log"
  local -a env_words
  read -ra env_words <<<"${envs[$i]}"
  started=$(date +%s)
  read -r first _ <<<"${cmds[$i]}"
  if ! : >"$log" 2>/dev/null; then
    echo "start 0 cannot write $log" >"$state/$suite.result"
    return
  fi
  if ! command -v "$first" >/dev/null 2>&1; then
    echo "cannot start: '$first' is not a command" >>"$log"
    echo "start 0 '$first' is not a command" >"$state/$suite.result"
    return
  fi
  env "${env_words[@]}" bash -e -c "${cmds[$i]}" >>"$log" 2>&1 </dev/null
  status=$?
  echo "$status $(($(date +%s) - started))" >"$state/$suite.result"
}

# failed_tests <log>: the failed test names libtest or nextest printed.
failed_tests() {
  sed 's/\x1b\[[0-9;]*m//g' "$1" | awk '
    $0 == "failures:" { names = 1; next }
    names && /^    [^ ]+$/ { sub(/^ +/, ""); print; next }
    { names = 0 }
    /^ *(FAIL|TIMEOUT|SIGSEGV|SIGABRT|SIGKILL) \[/ {
      sub(/^ *[A-Z]+ \[[^]]*\] */, ""); sub(/^\( *[0-9]+\/ *[0-9]+\) */, ""); print
    }' | awk '!seen[$0]++'
}

# libtest suites have no row of their own in rows.tsv (the nextest ones do,
# from ci/test-summary.sh run); add one from their `test result:` lines.
add_row() {
  local suite="$1" status="$2" secs="$3" counts
  counts=$(sed 's/\x1b\[[0-9;]*m//g' "$out_dir/$suite.log" | awk '
    /^test result: / {
      for (i = 1; i <= NF; i++) {
        if ($(i + 1) ~ /^passed/) p += $i
        if ($(i + 1) ~ /^failed/) f += $i
        if ($(i + 1) ~ /^ignored/) g += $i
      }
      found = 1
    }
    END { if (found) printf "%d\t%d\t%d", p, f, g }')
  [ -n "$counts" ] || return 0
  printf '%s\t%s\t%s\t%s\n' "$suite" "$counts" "$secs" \
    "$([ "$status" -eq 0 ] && echo pass || echo FAIL)" >>"$out_dir/rows.tsv" ||
    echo "::warning::live suites: could not record a summary row for $suite"
}

# units: a pool suite, or a lane at its first member's position.
units=()
declare -A lane_members=()
for i in "${!names[@]}"; do
  if ! selected "${whens[$i]}"; then
    echo "skipped 0" >"$state/${names[$i]}.result"
    continue
  fi
  if [ "${lanes[$i]}" = pool ]; then
    units+=("$i")
  else
    [ -n "${lane_members[${lanes[$i]}]+x}" ] || units+=("lane:${lanes[$i]}")
    lane_members[${lanes[$i]}]="${lane_members[${lanes[$i]}]:-} $i"
  fi
done

members_of() {
  case "$1" in lane:*) echo "${lane_members[${1#lane:}]}" ;; *) echo "$1" ;; esac
}

declare -A unit_of_pid=()
running=0

# finish_one: wait for any unit to end, then print its suites' logs.
finish_one() {
  local pid i result status secs label
  wait -n -p pid
  running=$((running - 1))
  for i in $(members_of "${unit_of_pid[$pid]}"); do
    result=$(cat "$state/${names[$i]}.result" 2>/dev/null || echo "start 0 no result")
    read -r status secs _ <<<"$result"
    case "$status" in
      0) label="ok ($secs s)" ;;
      start) label="NOT STARTED" ;;
      *) label="FAILED ($secs s)" ;;
    esac
    echo "::group::${names[$i]} — $label"
    cat "$out_dir/${names[$i]}.log" 2>/dev/null
    echo "::endgroup::"
    [ "$status" = start ] || add_row "${names[$i]}" "$status" "$secs"
  done
}

for unit in "${units[@]}"; do
  while [ "$running" -ge "$SLOTS" ]; do finish_one; done
  (
    for i in $(members_of "$unit"); do run_suite "$i"; done
  ) &
  unit_of_pid[$!]="$unit"
  running=$((running + 1))
  members=""
  for i in $(members_of "$unit"); do members="$members ${names[$i]}"; done
  echo "started:$members"
done
while [ "$running" -gt 0 ]; do finish_one; done

# ---- summary -------------------------------------------------------------
echo
echo "== live suites: $list =="
failed=0
failures=""
for i in "${!names[@]}"; do
  read -r status secs reason <<<"$(cat "$state/${names[$i]}.result" 2>/dev/null || echo "start 0 no result")"
  case "$status" in
    0) printf '%s\tok\t%s\n' "${names[$i]}" "$secs" ;;
    skipped) printf '%s\tskipped\t-\n' "${names[$i]}" ;;
    start)
      printf '%s\tNOT STARTED\t-\n' "${names[$i]}"
      echo "::error::live suite ${names[$i]} could not start: $reason"
      failed=1
      ;;
    *)
      printf '%s\tFAILED\t%s\n' "${names[$i]}" "$secs"
      tests=$(failed_tests "$out_dir/${names[$i]}.log")
      if [ -n "$tests" ]; then
        failures="$failures$(sed "s/^/${names[$i]}: /" <<<"$tests")"$'\n'
        echo "::error::live suite ${names[$i]} failed: $(paste -sd ' ' <<<"$tests")"
      else
        failures="$failures${names[$i]}: (no test named; its log is in its group above)"$'\n'
        echo "::error::live suite ${names[$i]} failed (exit $status)"
      fi
      failed=1
      ;;
  esac
done
if [ -n "$failures" ]; then
  echo
  echo "== failed tests =="
  printf '%s' "$failures"
fi
exit "$failed"
