#!/bin/sh
# Issue #494: the red path for `doc_sites.sh`.
#
# WHY THIS EXISTS. "The manifest check passed" on a clean tree says
# nothing about whether the check can still fail. Round 4 of this issue's
# review found two ways past an earlier version of it — a rewrite that
# changed only in whitespace, and a duplicate row that kept the row count
# right — and both are attacks here. Each runs against the COMMITTED
# script and manifest, in a scratch copy of the tree, and must fail with
# its own message.
#
# The scratch copy is a `git worktree`-free clone of the working tree into
# a temporary directory: the attacks edit files, and nothing they edit may
# be the tree CI is about to build.
set -eu
REPO=${REPO:-$(git rev-parse --show-toplevel)}
cd "$REPO"
# The frozen base lives in the manifest; the attacks below copy the
# manifest verbatim, so they inherit it.

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT INT TERM
fail() { echo "doc-sites-self-test: $1" >&2; exit 1; }

# A copy of every file the manifest names, plus the manifest itself.
copy_tree() {
  rm -rf "$work/tree"
  mkdir -p "$work/tree"
  while read -r verdict site; do
    [ -n "${site:-}" ] || continue
    [ "$verdict" != "#" ] || continue
    file=${site%%:*}
    mkdir -p "$work/tree/$(dirname "$file")"
    cp "$REPO/$file" "$work/tree/$file"
  done < "$REPO/ci/checks/doc_sites.txt"
  cp "$REPO/ci/checks/doc_sites.txt" "$work/manifest.txt"
  cp "$REPO/ci/checks/doc_sites.expected" "$work/expected.txt"
}

# Runs the check against the scratch copy and requires it to fail with a
# message containing `$1`.
expect_fail() {
  want=$1
  set +e
  out=$(REPO="$REPO" ROOT="$work/tree" MANIFEST="$work/manifest.txt" \
        EXPECTED="$work/expected.txt" \
        sh "$REPO/ci/checks/doc_sites.sh" 2>&1)
  code=$?
  set -e
  [ "$code" -ne 0 ] || fail "attack '$want' was not caught: the check exited 0"
  case "$out" in
    *"$want"*) echo "  caught: $out" ;;
    *) fail "attack '$want' produced the wrong message: $out" ;;
  esac
}

# Runs the check with NO upstream revision supplied and requires it to
# refuse. Used by the graph-shape tests below: a check that derives a
# revision from anywhere answers here, and answering is the failure.
expect_refusal_without_upstream() {
  what=$1
  set +e
  out=$(REPO="$REPO" ROOT="$work/tree" MANIFEST="$work/manifest.txt" \
        EXPECTED="$work/expected.txt" PULSUSDB_DOC_SITES_UPSTREAM= \
        sh "$REPO/ci/checks/doc_sites.sh" 2>&1)
  code=$?
  set -e
  [ "$code" -ne 0 ] \
    || fail "$what: the check answered with no upstream supplied, so something derives one"
  case "$out" in
    *"no upstream revision supplied"*) echo "  refused: $what" ;;
    *) fail "$what: refused for the wrong reason: $out" ;;
  esac
}

# Builds a commit object, given a tree-ish and any number of `-p` parents
# after it. The identity is per command because a CI runner has none, and
# the objects are unreferenced: only their shape is ever read.
throwaway_commit() {
  GIT_AUTHOR_NAME="doc-sites self-test" GIT_AUTHOR_EMAIL="self-test@invalid" \
  GIT_COMMITTER_NAME="doc-sites self-test" GIT_COMMITTER_EMAIL="self-test@invalid" \
  GIT_AUTHOR_DATE="${THROWAWAY_DATE:-}" GIT_COMMITTER_DATE="${THROWAWAY_DATE:-}" \
    git -C "$REPO" commit-tree "$@" -m "doc-sites self-test, unreferenced"
}

# **This script writes to the git object store**: the graph-shape tests
# below build throwaway commits and temporary refs. A read-only object
# store — a checkout mounted read-only, or one owned by another user —
# makes it refuse here rather than midway through with a `git` error.
probe=$(throwaway_commit "HEAD^{tree}" -p HEAD 2>/dev/null) \
  || fail "the git object store is not writable, and this script writes to it:
  the graph-shape tests build throwaway commits and temporary refs. Run it in a
  checkout you can write to."
[ -n "$probe" ] || fail "the git object store is not writable"

[ -n "${PULSUSDB_DOC_SITES_UPSTREAM:-}" ] || fail "set PULSUSDB_DOC_SITES_UPSTREAM to the
  base branch commit this change is measured against, the same value the workflow passes
  to the check. The self-test runs the check, and the check refuses without it."

BASE=$(awk '$1 == "#" && $2 == "base" { print $3; exit }' "$REPO/ci/checks/doc_sites.txt")
[ -n "$BASE" ] || fail "the manifest carries no frozen base revision"

first_rewrite=$(awk '$1 == "rewrite" { print $2; exit }' "$REPO/ci/checks/doc_sites.txt")
first_leave=$(awk '$1 == "leave" { print $2; exit }' "$REPO/ci/checks/doc_sites.txt")
[ -n "$first_rewrite" ] || fail "the manifest has no rewrite row to attack"
[ -n "$first_leave" ] || fail "the manifest has no leave row to attack"

rewrite_file=${first_rewrite%%:*}
rewrite_start=${first_rewrite#*:}; rewrite_start=${rewrite_start%%-*}
leave_file=${first_leave%%:*}
leave_start=${first_leave#*:}; leave_start=${leave_start%%-*}

# The two checkout shapes the derivation has to tell apart. A merge made
# on the branch puts upstream in the SECOND parent; a pull-request
# checkout builds a merge whose FIRST parent is the base branch and whose
# second is this change. The first shape is whatever this clone is; the
# second is built here with `git commit-tree`, because the derivation
# shipped wrong once by being green in the first shape and picking this
# change itself in the second. The object is unreferenced and costs one
# loose object per run.
echo "self-test: the clean copy must pass"
copy_tree
out=$(REPO="$REPO" ROOT="$work/tree" MANIFEST="$work/manifest.txt" \
      EXPECTED="$work/expected.txt" sh "$REPO/ci/checks/doc_sites.sh" 2>&1) \
  || fail "the clean copy must pass: $out"
echo "  $out"

echo "self-test: a rewrite changed only in whitespace"
copy_tree
git show "$BASE:$rewrite_file" | sed -n "${rewrite_start}p" | sed 's/ /  /' > "$work/ws"
awk -v n="$rewrite_start" -v f="$work/ws" '
  NR == n { while ((getline line < f) > 0) print line; next } { print }
' "$work/tree/$rewrite_file" > "$work/patched"
mv "$work/patched" "$work/tree/$rewrite_file"
expect_fail "changed only in whitespace"

echo "self-test: a duplicate row at the same row count"
copy_tree
grep -v -x -F "leave $first_leave" "$work/manifest.txt" > "$work/m2"
printf 'rewrite %s\n' "$first_rewrite" >> "$work/m2"
mv "$work/m2" "$work/manifest.txt"
expect_fail "duplicate site rows"

echo "self-test: a different site swapped in"
copy_tree
sed "s#^leave ${leave_file}:${leave_start}-#leave ${leave_file}:$((leave_start + 1))-#" \
  "$work/manifest.txt" > "$work/m2"
mv "$work/m2" "$work/manifest.txt"
expect_fail "site set differs from the committed expected set"

echo "self-test: a leave row changed"
copy_tree
awk -v n="$leave_start" 'NR == n { print "doc-sites self-test: this line was changed"; next } { print }' \
  "$work/tree/$leave_file" > "$work/patched"
mv "$work/patched" "$work/tree/$leave_file"
expect_fail "leave row changed"

echo "self-test: a new issue number in a rewrite row"
copy_tree
awk -v n="$rewrite_start" 'NR == n { print $0 " (follow-up #999999)"; next } { print }' \
  "$work/tree/$rewrite_file" > "$work/patched"
mv "$work/patched" "$work/tree/$rewrite_file"
expect_fail "new issue number #999999"

echo "self-test: an issue number that is elsewhere in the base file"
# Round 1 of this issue's code review walked through the earlier guard with
# exactly this: a reference that occurs SOMEWHERE in the frozen base file,
# but not inside the block the row protects. The number is picked from the
# base file itself, so the attack cannot go stale.
copy_tree
elsewhere=$(
  git show "$BASE:$rewrite_file" \
    | grep -oE '#[0-9]+' \
    | LC_ALL=C sort -u \
    | while IFS= read -r ref; do
        git show "$BASE:$rewrite_file" \
          | sed -n "${rewrite_start},${rewrite_start}p" \
          | grep -qF -- "$ref" || { echo "$ref"; break; }
      done
)
[ -n "$elsewhere" ] || fail "the base file carries no reference outside the protected row"
awk -v n="$rewrite_start" -v ref="$elsewhere" \
  'NR == n { print $0 " (follow-up " ref ")"; next } { print }' \
  "$work/tree/$rewrite_file" > "$work/patched"
mv "$work/patched" "$work/tree/$rewrite_file"
expect_fail "new issue number $elsewhere"

echo "self-test: a refs= column does not license a different number"
# The explicit allowlist is per number, not a switch that turns the rule
# off for the file it appears on.
copy_tree
refs_file=$(awk '$1 == "rewrite" && $3 ~ /^refs=/ { split($2, s, ":"); print s[1]; exit }' \
  "$REPO/ci/checks/doc_sites.txt")
if [ -n "$refs_file" ]; then
  refs_start=$(awk -v f="$refs_file" \
    '$1 == "rewrite" && $3 ~ /^refs=/ { split($2, s, ":"); if (s[1] == f) { split(s[2], r, "-"); print r[1]; exit } }' \
    "$REPO/ci/checks/doc_sites.txt")
  awk -v n="$refs_start" 'NR == n { print $0 " (follow-up #888888)"; next } { print }' \
    "$work/tree/$refs_file" > "$work/patched"
  mv "$work/patched" "$work/tree/$refs_file"
  expect_fail "new issue number #888888"
else
  fail "no row carries a refs= column, so this attack cannot be run"
fi

echo "self-test: an issue reference with no hash"
# Round 2 of this issue's code review: `follow-up issue 999999` names an
# issue to every reader and matched a hash-only guard not at all.
copy_tree
awk -v n="$rewrite_start" 'NR == n { print $0 " (follow-up issue 999999)"; next } { print }' \
  "$work/tree/$rewrite_file" > "$work/patched"
mv "$work/patched" "$work/tree/$rewrite_file"
expect_fail "new issue number #999999"

echo "self-test: a number the merged upstream head introduced is still not licensed"
# The added-lines comparison runs against the merged upstream head, not
# the base, so that main's own issue numbers are not reported as ones
# this change invented. The hole that would open is "any number main
# mentions is allowed". It is not: the allowed set is still the base's
# protected blocks plus the `refs=` column, and a number this change puts
# on a NEW line is caught whatever main says elsewhere in the file.
#
# The number is derived — present in the rewrite file at the merged head
# and absent from it at the base — so the attack cannot go stale.
copy_tree
MERGEDREV=$(git -C "$REPO" rev-parse "$PULSUSDB_DOC_SITES_UPSTREAM^{commit}")
if [ "$MERGEDREV" != "$(git -C "$REPO" rev-parse "$BASE^{commit}")" ]; then
  git show "$BASE:$rewrite_file" | grep -oE '#[0-9]+' \
    | LC_ALL=C sort -u > "$work/base.refs"
  git show "$MERGEDREV:$rewrite_file" | grep -oE '#[0-9]+' \
    | LC_ALL=C sort -u > "$work/merged.refs"
  from_main=$(LC_ALL=C comm -13 "$work/base.refs" "$work/merged.refs" | head -1)
  [ -n "$from_main" ] || fail "the merged head introduced no reference into $rewrite_file"
  awk -v n="$rewrite_start" -v ref="$from_main" \
    'NR == n { print $0 " (follow-up " ref ")"; next } { print }' \
    "$work/tree/$rewrite_file" > "$work/patched"
  mv "$work/patched" "$work/tree/$rewrite_file"
  expect_fail "new issue number $from_main"
else
  fail "the supplied upstream revision is the frozen base, so this attack cannot be run"
fi

# ---------------------------------------------------------------------
# The three commit-graph shapes round 4 of issue #494's code review built
# against a DERIVED comparison revision. Two of them made the check exit
# 0 against the wrong revision, and one of those hid protected text added
# to that revision's parent — the thing the check exists to catch. The
# third steered the "newest merge" choice with commit dates, which a
# contributor sets.
#
# The revision is no longer derived, so none of these can select
# anything. They are kept as NEGATIVE tests: with no revision supplied
# the check must refuse, and it must refuse **with these shapes present
# in the repository**. A future revision that derives again answers here
# instead of refusing, whatever it derives from, and that is the failure.
#
# **What they cannot do**, stated rather than left to be found: they
# cannot put a shape on `HEAD`, because the self-test does not move the
# working tree. They are reachable objects and temporary refs, so they
# reach a derivation that scans refs or `--all`; a derivation that reads
# only `$BASE..HEAD` would not see them, and is caught by the refusal
# itself rather than by the shape.
# ---------------------------------------------------------------------
echo "self-test: a change already merged to the base branch selects nothing"
copy_tree
absorbed=$(throwaway_commit "HEAD^{tree}" -p "$BASE" -p HEAD)
git -C "$REPO" update-ref refs/doc-sites-self-test/absorbed "$absorbed"
expect_refusal_without_upstream "the change already merged to the base"

echo "self-test: a later unrelated feature merge selects nothing"
copy_tree
feature=$(throwaway_commit "$BASE^{tree}" -p "$BASE")
later=$(throwaway_commit "HEAD^{tree}" -p HEAD -p "$feature")
git -C "$REPO" update-ref refs/doc-sites-self-test/feature "$later"
expect_refusal_without_upstream "a later unrelated feature merge"

echo "self-test: a merge back-dated ahead of its own ancestor selects nothing"
copy_tree
THROWAWAY_DATE="2030-01-01T00:00:00+0000" older=$(throwaway_commit "HEAD^{tree}" -p HEAD -p "$BASE")
newer=$(THROWAWAY_DATE="2001-01-01T00:00:00+0000" throwaway_commit "HEAD^{tree}" -p "$older" -p "$feature")
git -C "$REPO" update-ref refs/doc-sites-self-test/backdated "$newer"
expect_refusal_without_upstream "a descendant merge dated before its ancestor"

git -C "$REPO" update-ref -d refs/doc-sites-self-test/absorbed
git -C "$REPO" update-ref -d refs/doc-sites-self-test/feature
git -C "$REPO" update-ref -d refs/doc-sites-self-test/backdated

echo "self-test: an empty manifest"
copy_tree
: > "$work/manifest.txt"
expect_fail "empty or missing manifest"

echo "doc-sites-self-test: ten attacks caught, three graph shapes refused, the clean copy passed"
