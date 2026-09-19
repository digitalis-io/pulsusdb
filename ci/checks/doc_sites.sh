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
#   PULSUSDB_DOC_SITES_UPSTREAM=<base branch commit> \
#   REPO=<repository root> BASE=<revision> ROOT=<tree to check> \
#   MANIFEST=<path> EXPECTED=<path> sh ci/checks/doc_sites.sh
set -eu
REPO=${REPO:-$(git rev-parse --show-toplevel)}
cd "$REPO"
ROOT=${ROOT:-$REPO}
MANIFEST=${MANIFEST:-$REPO/ci/checks/doc_sites.txt}
EXPECTED=${EXPECTED:-$REPO/ci/checks/doc_sites.expected}

fail() { echo "doc-sites: $1" >&2; exit 1; }

# Every issue reference in `$1`, normalised to `#N`, one per line, sorted.
#
# Both spellings, because a guard that reads only one is a guard with a
# door beside it. Round 2 of this issue's code review walked through the
# hash-only version by writing `follow-up issue 999999`, which names an
# issue to every reader and matched nothing.
issue_refs() {
  grep -ioE '(#|issues?[[:space:]]+(number[[:space:]]+)?#?)[0-9]+' "$1" 2>/dev/null \
    | grep -oE '[0-9]+' \
    | sed 's/^/#/' \
    | LC_ALL=C sort -u
}

[ -s "$MANIFEST" ] || fail "empty or missing manifest"
[ -s "$EXPECTED" ] || fail "empty or missing expected set"
# The revision the ranges are taken in is frozen IN the manifest, not
# passed by the caller: the rows are claims about that revision's
# sentences, so a moving base would quietly change what they assert. A
# caller may override it, which is what the self-test does.
BASE=${BASE:-$(awk '$1 == "#" && $2 == "base" { print $3; exit }' "$MANIFEST")}
[ -n "$BASE" ] || fail "no frozen base revision in $MANIFEST"
git cat-file -e "$BASE^{commit}" 2>/dev/null \
  || fail "the frozen base revision $BASE is not in this clone (fetch-depth)"

# The revision the added-lines comparison is taken against: everything
# that is NOT this change. It is **SUPPLIED, never derived**, and this is
# the third shape of this rule — the first two were derived and both were
# steered.
#
#   round 2  compared against the frozen base, which predates the merge,
#            so the base branch's own text read as this change's;
#   round 3  read the revision from the manifest, and moving the manifest
#            and the protected text in one edit hid the edit;
#   round 4  derived it from the commit graph, and the review produced
#            three shapes that select the wrong parent — a change already
#            merged to the base, a later unrelated feature merge (whose
#            parent then hid a protected addition), and a "newest merge"
#            ordering steered by commit dates, which a contributor sets.
#
# A contributor controls parent order, merge topology and commit dates,
# so nothing inside the repository can say which commits are not theirs.
# The base branch is a fact the CI system knows and the repository cannot
# forge, so the CI system passes it in. Run by hand, it must be passed by
# hand: the check refuses rather than guessing, because every guess so
# far has been wrong.
UPSTREAM=${PULSUSDB_DOC_SITES_UPSTREAM:-}
[ -n "$UPSTREAM" ] || fail "no upstream revision supplied. Set
  PULSUSDB_DOC_SITES_UPSTREAM to the base branch commit this change is measured
  against. In CI that is the pull request's base commit, which the workflow
  passes. By hand, pass the commit your branch is measured against, e.g.
  PULSUSDB_DOC_SITES_UPSTREAM=\$(git merge-base HEAD origin/main).
  It is deliberately NOT derived from the commit graph: a contributor controls
  parent order and commit dates, so a derivation can be steered."
git cat-file -e "$UPSTREAM^{commit}" 2>/dev/null \
  || fail "the supplied upstream revision $UPSTREAM is not in this clone (fetch-depth)"

tmp_base=$(mktemp); tmp_a=$(mktemp); tmp_b=$(mktemp); tmp_up=$(mktemp)
trap 'rm -f "$tmp_base" "$tmp_a" "$tmp_b" "$tmp_up" "$tmp_a.n" "$tmp_b.n"' EXIT INT TERM

sites=$(awk '$1 != "#" && NF { print $2 }' "$MANIFEST" | LC_ALL=C sort)
dups=$(printf '%s\n' "$sites" | LC_ALL=C uniq -d)
[ -z "$dups" ] || fail "duplicate site rows: $(printf '%s' "$dups" | tr '\n' ' ')"
if ! printf '%s\n' "$sites" | LC_ALL=C cmp -s - "$EXPECTED"; then
  fail "site set differs from the committed expected set"
fi

# Whether file `$2` contains the exact block of lines in file `$1`.
#
# One `awk` process rather than a `sed | cmp` per candidate window.
# `grep -F -f` cannot do this — a multi-line pattern file matches each line
# independently — and the loop this replaces piped `sed` into a `cmp -s`
# that exits at the first differing byte, so `sed` was killed by EPIPE on
# nearly every window and printed `couldn't flush stdout: Broken pipe` to
# stderr thousands of times while the check passed.
contains_block() {
  awk -v blockfile="$1" '
    BEGIN {
      n = 0
      while ((getline line < blockfile) > 0) { block[++n] = line }
      if (n == 0) { exit 1 }
    }
    { buf[NR] = $0 }
    END {
      for (i = 1; i + n - 1 <= NR; i++) {
        ok = 1
        for (j = 1; j <= n; j++) {
          if (buf[i + j - 1] != block[j]) { ok = 0; break }
        }
        if (ok) { exit 0 }
      }
      exit 1
    }
  ' "$2"
}

n=0
while read -r verdict site rest; do
  [ -n "${verdict:-}" ] || continue
  [ "$verdict" != "#" ] || continue
  # `rest` is the optional `refs=#a,#b` column, read by
  # `check_issue_references` from the manifest directly.
  case "${rest:-}" in
    "" | refs=*) ;;
    *) fail "unknown trailing column ${rest:?} for $site" ;;
  esac
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
      # The issue-reference rule is checked per FILE after this loop, in
      # `check_issue_references`: it needs every rewrite row's base block
      # for that file at once, and it needs the lines the change ADDED.
      ;;
    *) fail "unknown verdict '$verdict' for $site" ;;
  esac
  n=$((n + 1))
done < "$MANIFEST"

[ "$n" -gt 0 ] || fail "empty list"

# R2: no follow-up issue number is invented.
#
# The rule is about the text a rewrite PUT THERE, so it is checked against
# the lines the change added and against the protected blocks — not
# against the file as a whole. An earlier version allowed any reference
# that occurred anywhere in the base file, and round 1 of this issue's code
# review walked straight through it: `follow-up #1` inserted into a rewrite
# row passed, because `#1` occurs elsewhere in that file. The reference was
# invented; the guard had simply looked in the wrong place.
#
# Allowed, per file: every reference already inside one of that file's
# protected base blocks, plus `#494` — this issue, which the rewrites are
# for and which the manifest's frozen base names — plus whatever that
# file's rows list in an explicit third column, `refs=#a,#b`.
#
# The third column exists because a correction sometimes has to cite the
# issue that explains the code it is correcting: `plan.rs`'s stale reason
# is replaced by the real one, which names the two branches issue #507
# added. Listing it in the manifest turns that from a reference the guard
# happened to allow into a recorded decision — which is R2's actual
# content. An invented follow-up number is one nobody wrote down.
check_issue_references() {
  files=$(awk '$1 == "rewrite" { print $2 }' "$MANIFEST" \
    | sed 's/:.*//' | LC_ALL=C sort -u)
  for file in $files; do
    git show "$BASE:$file" > "$tmp_base" 2>/dev/null \
      || fail "path absent at merge base: $file"

    # The references the protected blocks of THIS file already carried.
    : > "$tmp_a"
    awk -v want="$file" '$1 == "rewrite" { print $2 }' "$MANIFEST" \
      | while IFS= read -r site; do
          [ "${site%%:*}" = "$file" ] || continue
          range=${site#*:}; start=${range%%-*}; end=${range#*-}
          sed -n "${start},${end}p" "$tmp_base" >> "$tmp_a"
        done
    awk -v want="$file" '
      $1 == "rewrite" {
        split($2, s, ":")
        if (s[1] != want) { next }
        for (i = 3; i <= NF; i++) {
          if ($i ~ /^refs=/) {
            sub(/^refs=/, "", $i)
            m = split($i, r, ",")
            for (k = 1; k <= m; k++) { print r[k] }
          }
        }
      }
    ' "$MANIFEST" >> "$tmp_a"
    issue_refs "$tmp_a" > "$tmp_a.n" || true

    # The lines this change ADDED to the file, measured against the
    # supplied upstream revision rather than against the frozen base. A
    # line the base branch wrote is not a line this change added, and
    # before this distinction existed the first merge of the base branch
    # reported its own `#556` as an invented reference on
    # `docs/architecture.md`. A file absent upstream is wholly new, so
    # every line of it is this change's.
    if git cat-file -e "$UPSTREAM:$file" 2>/dev/null; then
      git show "$UPSTREAM:$file" > "$tmp_up"
    else
      : > "$tmp_up"
    fi
    diff --unchanged-line-format= --old-line-format= --new-line-format='%L' \
      "$tmp_up" "$ROOT/$file" > "$tmp_b" || true

    for ref in $(issue_refs "$tmp_b"); do
      [ "$ref" = "#494" ] && continue
      grep -qx -- "$ref" "$tmp_a.n" \
        || fail "new issue number $ref in rewrite row: $file"
    done
  done
}
check_issue_references

echo "doc-sites: checked $n sites (added lines measured against $UPSTREAM)"
