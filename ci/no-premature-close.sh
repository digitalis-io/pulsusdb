#!/usr/bin/env bash
#
# Fails when a PR would close an issue listed in
# ci/issues-that-must-stay-open.txt.
#
# WHY THIS EXISTS. An issue can carry named remaining work while the
# defect it was filed on is fixed. Merging with "Closes #N" in the PR
# body then closes the thread and the remaining work goes with it —
# silently, because nothing about the merge looks wrong. "The issue stays
# open" is otherwise a note somebody has to remember, and this makes it a
# property of the PR that a machine checks.
#
# WHAT IT SCANS: the PR body (through the PR_BODY environment variable —
# never shell-substituted into the script, since a PR body is untrusted
# text that anyone can write) and every commit message in the PR's range.
#
# WHAT IT CANNOT SEE, stated rather than papered over:
#
#   * an issue closed BY HAND in the GitHub UI. No CI step can reach
#     that, and this one does not pretend to;
#   * a closing keyword composed into a MERGE COMMIT message after CI has
#     run. The realistic path is covered — `gh pr merge --squash` takes
#     the PR body as the squash body, and the body is scanned — but a
#     message typed at merge time is not.
#
# `--self-test` runs the matcher against its own accept and reject cases
# and needs neither a PR nor a git range, so it runs unconditionally in
# CI. It is the committed red path for the matcher itself; the step is
# the red path for the trailer.

set -euo pipefail

MANIFEST="$(dirname "$0")/issues-that-must-stay-open.txt"

# GitHub's closing keywords, all inflections, case-insensitive, followed
# by `#<n>` on a word boundary — so `#2781` never satisfies `#278`.
close_pattern() {
  printf '(close[sd]?|fix(e[sd])?|resolve[sd]?)[[:space:]]*:?[[:space:]]*#%s([^0-9]|$)' "$1"
}

# 0 = the text would close issue $2.
would_close() {
  printf '%s\n' "$1" | grep -Eiq "$(close_pattern "$2")"
}

self_test() {
  local failures=0
  # Spellings that MUST be caught.
  local accept=(
    'Closes #278'
    'closes #278'
    'Fixed #278'
    'This resolves #278 at last'
    'Fixes: #278'
  )
  # Spellings that must NOT be caught: a bare reference is how a commit
  # cites the issue it belongs to, which is the normal case, and a longer
  # number merely starts with the same digits.
  local reject=(
    '#278'
    'see #278 for context'
    'Closes #2781'
    'Closes #27'
  )
  local case_
  for case_ in "${accept[@]}"; do
    if ! would_close "$case_" 278; then
      echo "self-test: matcher FAILED to catch ${case_@Q}" >&2
      failures=$((failures + 1))
    fi
  done
  for case_ in "${reject[@]}"; do
    if would_close "$case_" 278; then
      echo "self-test: matcher wrongly caught ${case_@Q}" >&2
      failures=$((failures + 1))
    fi
  done
  if [ "$failures" -ne 0 ]; then
    echo "no-premature-close.sh: $failures self-test case(s) failed — the matcher no longer" >&2
    echo "does what this script claims, so its green result would mean nothing." >&2
    exit 1
  fi
  echo "no-premature-close.sh: self-test passed (${#accept[@]} accept, ${#reject[@]} reject)"
}

if [ "${1:-}" = "--self-test" ]; then
  self_test
  exit 0
fi

if [ ! -f "$MANIFEST" ]; then
  echo "no-premature-close.sh: missing $MANIFEST" >&2
  exit 1
fi

BASE="${GITHUB_BASE_REF:-main}"
if git rev-parse --verify --quiet "origin/$BASE" >/dev/null; then
  RANGE="origin/$BASE..HEAD"
else
  RANGE="$BASE..HEAD"
fi
COMMITS="$(git log --format='%B' "$RANGE" 2>/dev/null || true)"

status=0
while IFS= read -r line; do
  case "$line" in
    '#'* | '') continue ;;
  esac
  issue="${line%% *}"
  reason="${line#* }"
  if [ "$reason" = "$line" ] || [ -z "${reason// /}" ]; then
    echo "no-premature-close.sh: issue #$issue is listed with no reason." >&2
    echo "  An exemption nobody explained is what this file exists to stop." >&2
    status=1
    continue
  fi
  where=""
  if would_close "${PR_BODY:-}" "$issue"; then
    where="the PR body"
  fi
  if would_close "$COMMITS" "$issue"; then
    if [ -n "$where" ]; then
      where="$where and a commit message"
    else
      where="a commit message"
    fi
  fi
  if [ -n "$where" ]; then
    echo "no-premature-close.sh: $where carries a GitHub closing keyword for issue #$issue," >&2
    echo "  which must stay open: $reason" >&2
    echo "  Reference it without the keyword (\"#$issue\", or \"part of #$issue\") — or, if the" >&2
    echo "  work named above has landed, delete its line from $MANIFEST in this same PR." >&2
    status=1
  fi
done < "$MANIFEST"

if [ "$status" -eq 0 ]; then
  echo "no-premature-close.sh: no listed issue would be closed by this PR."
fi
exit "$status"
