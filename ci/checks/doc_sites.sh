#!/bin/sh
# Issue #494: the documentation manifest check.
#
# WHY THIS EXISTS. The change suppresses a retried push at ingest, which
# falsifies sentences scattered across documentation and code comments —
# "raw-path reads dedup at query time", "exact-once only by writer batch
# atomicity". A list of those sites in a plan is a note someone has to
# remember. This turns the list into a committed artifact and checks BOTH
# halves of it: a site marked `rewrite` must have changed, and a site
# marked `leave` must not have.
#
# WHAT A ROW IS
#
#   <verdict> <file>:<start>-<end>
#
# with the line range taken **in the merge base**, because that is the
# revision whose sentences are being judged. The range names a block of
# text; the check then looks for that text in the working tree rather than
# at the same line numbers, so an unrelated insertion earlier in the file
# does not turn an untouched site into a failure. (`docs/configuration.md`
# gained three rows in §5 in this very change, which moved every later
# line in the file without changing one of them.)
#
# WHAT IT CANNOT SEE, stated rather than papered over:
#
#   * a site nobody put in the manifest. The set is derived by the search
#     published beside it in `doc_sites.expected`, and re-derivable, but
#     the check cannot invent a row.
#   * whether a rewritten sentence is TRUE. It checks that the text moved,
#     not that it moved to something correct.
#
# Usage:
#   REPO=<repository root> BASE=<revision> ROOT=<tree to check> \
#   MANIFEST=<path> EXPECTED=<path> sh ci/checks/doc_sites.sh
set -eu
REPO=${REPO:-$(git rev-parse --show-toplevel)}
cd "$REPO"
ROOT=${ROOT:-$REPO}
MANIFEST=${MANIFEST:-$REPO/ci/checks/doc_sites.txt}
EXPECTED=${EXPECTED:-$REPO/ci/checks/doc_sites.expected}
BASE=${BASE:?set BASE to the merge-base revision}

tmp_base=$(mktemp); tmp_a=$(mktemp); tmp_b=$(mktemp)
trap 'rm -f "$tmp_base" "$tmp_a" "$tmp_b" "$tmp_a.n" "$tmp_b.n"' EXIT INT TERM
fail() { echo "doc-sites: $1" >&2; exit 1; }

[ -s "$MANIFEST" ] || fail "empty or missing manifest"
[ -s "$EXPECTED" ] || fail "empty or missing expected set"

sites=$(awk '{print $2}' "$MANIFEST" | LC_ALL=C sort)
dups=$(printf '%s\n' "$sites" | LC_ALL=C uniq -d)
[ -z "$dups" ] || fail "duplicate site rows: $(printf '%s' "$dups" | tr '\n' ' ')"
if ! printf '%s\n' "$sites" | LC_ALL=C cmp -s - "$EXPECTED"; then
  fail "site set differs from the committed expected set"
fi

# Whether file `$2` contains the exact block of lines in file `$1`.
contains_block() {
  # `grep -F -f` with a multi-line pattern file matches each line
  # independently, which is not what is wanted; compare windows instead.
  block_lines=$(wc -l < "$1")
  total=$(wc -l < "$2")
  [ "$block_lines" -gt 0 ] || return 1
  last=$((total - block_lines + 1))
  [ "$last" -ge 1 ] || return 1
  i=1
  while [ "$i" -le "$last" ]; do
    if sed -n "${i},$((i + block_lines - 1))p" "$2" | cmp -s - "$1"; then
      return 0
    fi
    i=$((i + 1))
  done
  return 1
}

n=0
while read -r verdict site; do
  [ -n "${verdict:-}" ] || continue
  file=${site%%:*}; range=${site#*:}
  start=${range%%-*}; end=${range#*-}
  case "$start$end" in *[!0-9]*) fail "bad range: $site" ;; esac
  [ "$start" -ge 1 ] && [ "$end" -ge "$start" ] || fail "zero or reversed range: $site"

  git show "$BASE:$file" > "$tmp_base" 2>/dev/null || fail "path absent at merge base: $file"
  [ -f "$ROOT/$file" ] || fail "path absent in the tree: $file"
  base_lines=$(wc -l < "$tmp_base")
  [ "$end" -le "$base_lines" ] || fail "range past EOF at the merge base: $site"

  sed -n "${start},${end}p" "$tmp_base" > "$tmp_a"

  case "$verdict" in
    leave)
      contains_block "$tmp_a" "$ROOT/$file" || fail "leave row changed: $site" ;;
    rewrite)
      if contains_block "$tmp_a" "$ROOT/$file"; then
        fail "rewrite row unchanged: $site"
      fi
      # A whitespace-only edit is not a rewrite. Squeeze both sides and
      # look again: if the text is still there, nothing was said.
      tr -s '[:space:]' ' ' < "$tmp_a" > "$tmp_a.n"
      tr -s '[:space:]' ' ' < "$ROOT/$file" > "$tmp_b.n"
      if grep -qF -- "$(cat "$tmp_a.n")" "$tmp_b.n"; then
        fail "rewrite row changed only in whitespace: $site"
      fi
      # R2: no follow-up issue number is invented. An issue number the
      # base text already carried may stay, and this issue's own number
      # may be added; anything else is a number nobody agreed to.
      for ref in $(grep -oE '#[0-9]+' "$ROOT/$file" | LC_ALL=C sort -u); do
        [ "$ref" = "#494" ] && continue
        grep -qF -- "$ref" "$tmp_base" || fail "new issue number $ref in rewrite row: $site"
      done ;;
    *) fail "unknown verdict '$verdict' for $site" ;;
  esac
  n=$((n + 1))
done < "$MANIFEST"

[ "$n" -gt 0 ] || fail "empty list"
echo "doc-sites: checked $n sites"
