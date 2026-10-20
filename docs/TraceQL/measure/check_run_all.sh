#!/usr/bin/env bash
# A walk of run_all.sh's steps without running them:
#   - run_all.sh parses;
#   - every script it calls compiles (python3 -m py_compile, bash -n);
#   - every "$HERE/<path>" it names exists;
#   - every run_all.sh row of the inventory quotes a line run_all.sh has;
#   - no step calls fetch_compare.py, which times the shipped routes and takes
#     other arguments than the step's.
#
#   check_run_all.sh [MEASURE_DIR]      the walk; MEASURE_DIR defaults to this file's directory
#   check_run_all.sh --self-test        the walk over broken copies, each of which must fail
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)

walk() {
  local M=$1 bad=0 path f line tmp
  tmp=$(mktemp -d)
  bash -n "$M/run_all.sh" || { echo "run_all.sh does not parse" >&2; bad=1; }
  while IFS= read -r path; do
    case $path in
      *'$'*) f=$M/$(dirname "$path") ;;   # a path built from a variable: its directory must exist
      *) f=$M/$path ;;
    esac
    if [ ! -e "$f" ]; then echo "run_all.sh calls $path, which does not exist" >&2; bad=1; continue; fi
    case $path in
      *'$'*) ;;
      *.py) PYTHONPYCACHEPREFIX=$tmp python3 -m py_compile "$f" || { echo "$path does not compile" >&2; bad=1; } ;;
      *.sh) bash -n "$f" || { echo "$path does not parse" >&2; bad=1; } ;;
    esac
  done < <(grep -o '"\$HERE/[^"]*"' "$M/run_all.sh" | sed 's/^"\$HERE\///; s/"$//' | sort -u)
  # each line as the inventory quotes it: whitespace runs as one space, no indent
  sed 's/^[[:space:]]*//; s/[[:space:]]*$//' "$M/run_all.sh" | tr -s ' \t' '  ' > "$tmp/lines"
  while IFS= read -r line; do
    if ! grep -qxF -- "$line" "$tmp/lines"; then
      echo "the inventory quotes a line run_all.sh no longer has: ${line:0:100}" >&2; bad=1
    fi
  done < <(awk -F'\t' '$1 == "run_all.sh" { print $4 }' "$M/results-inventory.tsv")
  if grep -n 'fetch_compare\.py' "$M/run_all.sh" >&2; then
    echo "run_all.sh calls fetch_compare.py, the shipped-route script" >&2; bad=1
  fi
  rm -rf "$tmp"
  return $bad
}

if [ "${1:-}" = --self-test ]; then
  fails=0
  T=$(mktemp -d)
  trap 'rm -rf "$T"' EXIT
  copy() { rm -rf "$T/m"; cp -r "$HERE" "$T/m"; }
  copy
  if ! walk "$T/m" 2>/dev/null; then echo "self-test: the unbroken copy fails the walk" >&2; fails=1; fi
  copy
  sed -i 's#"\$HERE/push_otlp.py"#"$HERE/no_such_step.py"#' "$T/m/run_all.sh"
  if walk "$T/m" 2>/dev/null; then echo "self-test: a copy naming a missing script passes" >&2; fails=1; fi
  copy
  sed -i 's#"\$HERE/fetch_compare_design.py"#"$HERE/fetch_compare.py"#' "$T/m/run_all.sh"
  if walk "$T/m" 2>/dev/null; then echo "self-test: a copy calling fetch_compare.py passes" >&2; fails=1; fi
  copy
  printf '\ndef (:\n' >> "$T/m/fetch_compare_design.py"
  if walk "$T/m" 2>/dev/null; then echo "self-test: a copy calling a script with a syntax error passes" >&2; fails=1; fi
  [ $fails = 0 ] && echo "check_run_all.sh self-test: 4 of 4 cases as expected"
  exit $fails
fi

if walk "${1:-$HERE}"; then echo "run_all.sh: every step's script exists and compiles, and the inventory follows it"; exit 0; fi
exit 1
