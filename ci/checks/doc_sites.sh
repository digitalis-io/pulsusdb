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
# WHAT THIS IS, AND WHAT IT IS NOT. **It is a drift detector. It is not a
# control against a determined author**, and the difference is not a
# caveat — it decides what the output means.
#
# What it catches, and has caught:
#
#   * a `rewrite` row whose text is still in the tree: the change said it
#     would replace that sentence and did not.
#   * a `leave` row whose text has changed: the change touched something
#     the manifest said it would not.
#   * a new issue number inside a protected block. **Once on this branch
#     on text a person actually wrote**, as against ten times on the
#     self-test's injected attacks: after the base branch was merged, the
#     base branch's own `#556` on `docs/architecture.md` read as a number
#     this change had invented, and the check exited 1 saying so. That is
#     the whole record of it catching drift, stated as one rather than
#     rounded up. (The `refs=` column came from the opposite case — a
#     reference an earlier version of this rule ALLOWED, which a code
#     review found and the column now records as a decision.)
#   * a `refs=` column declaring a reference the named file does not
#     carry. The column records a decision, and a decision naming a
#     reference nobody wrote down is not one. Measured on the version
#     before this rule: `refs=#25,#559,#999999` on
#     `docs/schemas.md:174-174` printed `checked 27 sites` and exited 0.
#   * a `refs=` column on a `leave` row, where nothing reads it, and a
#     column whose text only looks like a declaration. `refs=#25, #559`
#     declares two references and records one: the reader takes field 3,
#     and `#559` lands in field 4, which nothing looks at.
#   * a manifest this script's own parsers would read differently. Six
#     of them read this file, and three disagreements were measured: the
#     row loop skipped a final record with no newline after it while
#     every `awk` reader read it; a carriage return survived into the
#     last field of both and made the message quote text that looks
#     correct; and a NUL byte inside `rewrite` was dropped by `read` and
#     kept by `awk`, so the row loop checked a row every `awk` reader
#     skipped, and the declaration on that row went unchecked.
#
# Citations that have drifted are a different check in a different file:
# `every_design_record_citation_still_points_at_what_it_names` in
# `crates/pulsus-read/tests/design_record_drift_gate.rs`. It found ten
# wrong citations on this branch. Those are not this script's catches and
# are named here only so the two are not confused.
#
# What it does not catch: **a change that edits protected content and the
# workflow step that supplies the comparison base in the same commit.**
# The base is assigned in `.github/workflows/ci.yml`, and that file ships
# in the tree under review. Measured, not reasoned:
#
#   $ git show origin/main:.github/workflows/ci.yml | grep -c doc_sites
#   0
#   $ PULSUSDB_DOC_SITES_UPSTREAM=HEAD sh ci/checks/doc_sites.sh; echo $?
#   doc-sites: checked 27 sites (added lines measured against HEAD)
#   0
#
# The first says this change adds the steps that set the variable. The
# second says a base of `HEAD` makes the added-line set empty, so the
# issue-reference half checks nothing and still exits 0.
#
# **This is how every step in this workflow works, not a property of this
# one.** `.github/workflows/ci.yml` defines 191 steps across 11 jobs, 147
# of which run a command, and all of them are read from the tree under
# review. This check is conspicuous only because it reads like a security
# control. It is not one. Its value is that it fails when documentation
# drifts away from the code by accident, which is what happens.
#
# Three other things it cannot see, stated rather than papered over:
#
#   * a site nobody put in the manifest. The set is derived by the search
#     published beside it in `doc_sites.expected`, and re-derivable, but
#     the check cannot invent a row.
#   * whether a rewritten sentence is TRUE. It checks that the text moved,
#     not that it moved to something correct.
#   * whether a declared reference is cited in the PASSAGE the row names,
#     rather than somewhere else in the same file. The `refs=` rule below
#     is per FILE. Measured: `refs=#351` added to the
#     `docs/schemas.md:1089-1089` row with no text change printed
#     `checked 27 sites` and exited 0, because #351 is already elsewhere
#     in that file. Two narrower rules were written and run rather than
#     ruled out on paper, and both are worse:
#
#       - "inside the row's range in the working tree": the ranges are
#         coordinates in the FROZEN BASE and the tree has moved. Line 174
#         of `docs/schemas.md` in the tree carries neither #25 nor #559,
#         so this rule refuses the committed manifest.
#       - "among the lines this change added": it holds against the
#         frozen base (#25 once, #559 three times, #507 once), but on a
#         run with no pull request the workflow supplies the checked-out
#         commit (`.github/workflows/ci.yml:98`), the added-line set is
#         empty, and every declaration reads as unused. Declarations are
#         cumulative too: a later change that adds lines to
#         `docs/schemas.md` does not re-add #25.
#
#     One cost of the per-file rule, stated because it will be met: a
#     change that removes a file's LAST mention of a declared reference
#     must prune that entry from the column, and the message names the
#     reference and the file. Removing one mention of several changes
#     nothing.
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
#
# This reading stops at the first non-digit, so it also reads `#987654`
# out of `#987654x`. That is the safe side for the lines a change ADDED,
# which is the one place it is used: a token that looks like a reference
# is refused unless it is licensed.
issue_refs() {
  grep -ioE '(#|issues?[[:space:]]+(number[[:space:]]+)?#?)[0-9]+' "$1" 2>/dev/null \
    | grep -oE '[0-9]+' \
    | sed 's/^/#/' \
    | LC_ALL=C sort -u
}

# The same references, but only where the reference is a whole token:
# it starts at the beginning of a line or after a character that is not
# a letter, digit or underscore, and its digit run ENDS the token, with
# no letter, digit or underscore after it. Used for the two sets that
# SATISFY a rule — what a file carries, for the presence rule, and what
# the protected blocks and declarations allow — where reading too much
# would accept a reference nobody wrote.
#
# The first `grep` takes the one boundary character before the reference
# (none at the start of a line) and the digits together with any word
# characters after them; the second keeps only the matches that stop at
# the digits. With `issue_refs` here, `refs=#987654` was satisfied by a
# file whose only match was `not-an-issue #987654x`, and then by one
# whose only match was `tissue 987654`, `abc#987654` or `x_#987654` —
# measured, each `checked 27 sites`, exit 0. `#987654.`, `#987654,`,
# `(#987654)`, `issue 987654` and `#987654` at the start of a line still
# count. One consequence of consuming the boundary character: in
# `#1#2` the second reference has `1` before it and is not counted.
standalone_issue_refs() {
  grep -ioE '(^|[^0-9A-Za-z_])(#|issues?[[:space:]]+(number[[:space:]]+)?#?)[0-9]+[0-9A-Za-z_]*' "$1" 2>/dev/null \
    | grep -ixE '[^0-9A-Za-z_]?(#|issues?[[:space:]]+(number[[:space:]]+)?#?)[0-9]+' \
    | grep -oE '[0-9]+' \
    | sed 's/^/#/' \
    | LC_ALL=C sort -u
}

[ -s "$MANIFEST" ] || fail "empty or missing manifest"
[ -s "$EXPECTED" ] || fail "empty or missing expected set"
# A carriage return is read differently by every parser here: `read`
# leaves it in the last field, `awk` leaves it inside the last field,
# and neither treats it as a separator. The record then fails somewhere
# downstream with a message quoting text that LOOKS correct, because a
# carriage return does not print. Measured on the version before this
# rule: a `rewrite` row carrying a column and ending CRLF passed, exit
# 0. One check, once, over the whole file, naming the cause. It runs
# before the frozen base is read, because a base revision with a
# carriage return glued to it fails at `git cat-file` instead.
! grep -Fq "$(printf '\r')" "$MANIFEST" \
  || fail "the manifest has carriage returns; it must use Unix line endings"
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
tmp_have=$(mktemp); tmp_loop=$(mktemp); tmp_view=$(mktemp)
trap 'rm -f "$tmp_base" "$tmp_a" "$tmp_b" "$tmp_up" "$tmp_a.n" "$tmp_b.n" \
  "$tmp_have" "$tmp_loop" "$tmp_view"' EXIT INT TERM

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
ln=0
# `|| [ -n "${verdict:-}" ]` runs the body once more for a final record
# with no newline after it. Without it the row loop skipped that record
# entirely — its verdict, its range, its file's presence in the tree and
# its column all unchecked — while every `awk` reader below still read
# it. Measured on the version before this rule, with the last record's
# protected text replaced: `checked 26 sites`, exit 0.
while read -r verdict site rest || [ -n "${verdict:-}" ]; do
  ln=$((ln + 1))
  [ -n "${verdict:-}" ] || continue
  [ "$verdict" != "#" ] || continue
  # `rest` is the optional `refs=#a,#b` column, read by
  # `check_issue_references` from the manifest directly.
  #
  # The column must say what it appears to say. `rest` is the whole
  # remainder of the record with trailing blanks trimmed, so a second
  # column and a space inside the column are both caught here, and a
  # record with trailing spaces is not.
  case "${rest:-}" in
    "") ;;
    refs=*)
      printf '%s\n' "$rest" | grep -qxE 'refs=#[0-9]+(,#[0-9]+)*' \
        || fail "malformed refs= column for $site: '$rest' (want refs=#N or refs=#N,#M)" ;;
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
      # Nothing reads a `refs=` column on a `leave` row: the declaration
      # reader below takes `rewrite` rows only. A column here records a
      # decision that has no effect, which is worse than none.
      case "${rest:-}" in
        refs=*) fail "refs= column on a leave row: $site" ;;
      esac
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
  # This loop's own view of the record, for the comparison after the
  # loop. Written here rather than derived afterwards, because the
  # point of the comparison is what THIS parser saw.
  printf '%s %s %s %s\n' "$ln" "$verdict" "$site" "${rest:-}" >> "$tmp_loop"
  n=$((n + 1))
done < "$MANIFEST"

[ "$n" -gt 0 ] || fail "empty list"

# Six parsers read this file: the frozen-base row above, the site set
# above, the row loop above, and three `awk` readers in
# `check_issue_references` below. They are different parsers, and a byte
# one of them drops and another keeps makes a row checked by one and
# skipped by the other. Measured on the version before this rule: a NUL
# byte inside `rewrite` is dropped by `read` and kept by `awk`, so the
# row loop checked the row while every `awk` reader skipped it, and
# `refs=#25,#559,#999999` on that row passed with exit 0 although
# `docs/schemas.md` carries no #999999.
#
# Enumerated over every byte value 0 to 255 in the verdict field, under
# two locales, comparing the whole record view each family builds: NUL is
# the only byte the two families read differently; the rule is
# written against the parsers rather than against that byte, because the
# next divergence will be a different byte. So: the row loop's view of
# every record, compared byte for byte with the `awk` view of the same
# records, before any `awk` reader below is consulted.
awk '$1 != "#" && NF {
  r = ""
  for (i = 3; i <= NF; i++) { r = (i == 3 ? $i : r " " $i) }
  printf "%s %s %s %s\n", NR, $1, $2, r
}' "$MANIFEST" > "$tmp_view"
if ! cmp -s "$tmp_loop" "$tmp_view"; then
  k=$(awk 'NR == FNR { loop[FNR] = $0; ln = FNR; next }
           { rn = FNR; if (!k && $0 != loop[FNR]) { k = FNR } }
           END { if (!k) { k = (ln < rn ? ln : rn) + 1 }; print k }' \
      "$tmp_loop" "$tmp_view")
  show() { sed -n "${k}p" "$1" | cut -d' ' -f2- | tr -c '\11\12\40-\176' '?' | tr -d '\n'; }
  # `FNR` and not `NR`: with two files `sed -n "${k}p"` would number the
  # concatenation, so a short first file would print the wrong record.
  where=$(awk -v k="$k" 'FNR == k { print $1; exit }' "$tmp_view" "$tmp_loop")
  fail "the row loop and the manifest readers disagree about the record on
  line ${where:-?} of $MANIFEST:
  the row loop read      '$(show "$tmp_loop")'
  the manifest reader    '$(show "$tmp_view")'
  A byte one parser drops and another keeps makes a row checked by one and
  skipped by the other. A '?' above stands for a byte outside tab, newline
  and printable ASCII; a missing line means one parser saw fewer records."
fi

# Every reference listed in a `refs=` column on a `rewrite` row naming
# file `$1`, one per line, in manifest order and exactly as written.
# The body is the block that used to sit inline in
# `check_issue_references`, wrapped in a function so the presence rule
# below and the allowance feed read the same declarations.
declared_refs() {
  awk -v want="$1" '
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
  ' "$MANIFEST"
}

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
    declared_refs "$file" >> "$tmp_a"
    standalone_issue_refs "$tmp_a" > "$tmp_a.n" || true

    # The column licenses; it must also RECORD. Every reference it
    # declares has to be in the working-tree copy of the file the row
    # names, or the column is a decision about text nobody wrote. It is
    # a rule about the FILE and not about the passage, for the reason
    # given in the header. This runs before the added-lines comparison
    # below and does not depend on it, so it holds on a run whose
    # added-line set is empty.
    standalone_issue_refs "$ROOT/$file" > "$tmp_have" || true
    for ref in $(declared_refs "$file"); do
      grep -Fqx -- "$ref" "$tmp_have" \
        || fail "declared reference $ref is absent from $file"
    done

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
      grep -Fqx -- "$ref" "$tmp_a.n" \
        || fail "new issue number $ref in rewrite row: $file"
    done
  done
}
check_issue_references

echo "doc-sites: checked $n sites (added lines measured against $UPSTREAM)"
