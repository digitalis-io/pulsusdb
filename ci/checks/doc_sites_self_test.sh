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

echo "self-test: the clean copy must pass"
copy_tree
REPO="$REPO" ROOT="$work/tree" MANIFEST="$work/manifest.txt" \
  EXPECTED="$work/expected.txt" sh "$REPO/ci/checks/doc_sites.sh"

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

echo "self-test: an empty manifest"
copy_tree
: > "$work/manifest.txt"
expect_fail "empty or missing manifest"

echo "doc-sites-self-test: nine attacks caught, the clean copy passed"
