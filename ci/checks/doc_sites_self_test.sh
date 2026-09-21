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
# a throwaway directory: the attacks edit files, and nothing they edit may
# be the tree CI is about to build.
#
# **WHAT THESE ATTACKS ESTABLISH, AND WHAT THEY DO NOT.** Twenty-six
# attacks, and they divide in four. **Seven edit the protected
# CONTENT**: the whitespace-only rewrite, the changed `leave` row, and
# the five issue-number attacks. **Thirteen edit the MANIFEST and leave
# the protected content alone**: the duplicate row, the swapped site, the
# empty manifest, and the ten `refs=`, record-shape and literal-edit
# attacks. One of the thirteen also copies an extra file into the scratch
# tree, which no record protects. **Five edit both**: the final record
# with no newline whose protected text was also replaced, and the four
# that declare a reference and append a line carrying it only inside a
# longer token, `#999999x`, `#999999_`, `abc#999999` and `x_#999999`.
# **One edits the protected block ITSELF**, in a throwaway commit that
# stands in for the frozen base, and adds a line to the tree. (An earlier
# version of this paragraph said every attack edits the content. Three of
# its own ten contradicted it, which is the defect this file's own
# subject is — a claim wider than its evidence.)
#
# **Three runs here must PASS**, and they are not decoration. A red path
# cannot see a rule that has stopped licensing anything: with the
# supplied upstream revision equal to the checked-out commit the
# added-line set is empty, the allowed set is never consulted, and
# deleting the `refs=` column's feed into it leaves every attack above
# green. The licensing run is the only test that shows the column still
# licenses. The clean copy and the unterminated final record are the
# other two.
#
# **The counts above are asserted, not narrated.** Each harness
# increments a counter and the totals are checked before the closing
# line is printed, so deleting a test body fails the run rather than
# quietly printing a smaller number.
#
# **None of the twenty-six touches the workflow**, and that is the division that
# decides what a green run means. Editing content or manifest is the
# shape of an accident, and accidents are what the check is for. It is
# not the shape of a determined author, who would edit the content and the
# step that supplies the comparison base together — the base is assigned
# in `.github/workflows/ci.yml`, which ships in the tree under review, and
# `PULSUSDB_DOC_SITES_UPSTREAM=HEAD` makes the added-line set empty and
# the check exit 0. There is no attack here for that, and adding one would
# not help: the scope statement at the top of `doc_sites.sh` records it,
# with the two commands that measured it, because it is a property of how
# this workflow is defined rather than a hole in this script. Every one of
# the workflow's 148 command-running steps has it.
#
# So: a green run here means the drift detector still detects drift. It
# does not mean the manifest cannot be bypassed.
set -eu
REPO=${REPO:-$(git rev-parse --show-toplevel)}
cd "$REPO"
# The frozen base lives in the manifest; the attacks below copy the
# manifest verbatim, so they inherit it.

work=$(mktemp -d)
# Every reference this script creates lives under one prefix, and the
# trap removes all of them however the script ends. An earlier version
# deleted them on the last line, so a run that failed — which is what a
# break test makes it do — left `refs/doc-sites-self-test/absorbed`
# behind in the repository it was testing.
SELF_TEST_REFS=refs/doc-sites-self-test
cleanup() {
  rm -rf "$work"
  git -C "$REPO" for-each-ref --format='%(refname)' "$SELF_TEST_REFS/**" 2>/dev/null \
    | while IFS= read -r r; do
        [ -n "$r" ] && git -C "$REPO" update-ref -d "$r" 2>/dev/null || true
      done
}
trap cleanup EXIT INT TERM
fail() { echo "doc-sites-self-test: $1" >&2; exit 1; }

# A copy of every file the manifest names, plus the manifest itself.
# `$1` is the manifest to read and to install as the scratch copy; it
# defaults to the committed one. A test that needs a manifest of its own
# shape passes it here rather than editing `$work/manifest.txt`
# afterwards, so that this loop reads it too — it is the same parser as
# the check's row loop and has the same blind spots.
copy_tree() {
  src=${1:-$REPO/ci/checks/doc_sites.txt}
  rm -rf "$work/tree"
  mkdir -p "$work/tree"
  # `|| [ -n "${verdict:-}" ]` for the same reason the check's row loop
  # has it: without it a final record with no newline after it is never
  # read, so its file is missing from the scratch tree and the check
  # refuses with `path absent in the tree` — measured, on the clean
  # self-test, once the check itself started reading that record.
  while read -r verdict site || [ -n "${verdict:-}" ]; do
    [ -n "${site:-}" ] || continue
    [ "$verdict" != "#" ] || continue
    file=${site%%:*}
    mkdir -p "$work/tree/$(dirname "$file")"
    cp "$REPO/$file" "$work/tree/$file"
  done < "$src"
  cp "$src" "$work/manifest.txt"
  cp "$REPO/ci/checks/doc_sites.expected" "$work/expected.txt"
}

# Replaces the record whose WHOLE TEXT is `$1` with `$2`, once, in the
# scratch manifest. Whole-line equality, not a substitution: written as
# `sed "s#^leave ${leave_file}:${leave_start}-#…#"` the path was read as
# a basic regular expression, so `docs/116.md` matched `docs/116Xmd` too
# and the edit hit two records; the `#` delimiter would also break on a
# path containing one. The literal-path attack below carries such a path
# and is the case that turns red if this is written as a substitution.
set_record() {
  awk -v old="$1" -v new="$2" '
    !patched && $0 == old { print new; patched = 1; next }
    { print }
  ' "$work/manifest.txt" > "$work/m2"
  mv "$work/m2" "$work/manifest.txt"
}

# Replaces the exact text `$2` with `$3` on the `rewrite` record naming
# site `$1`, once. Positional: `index()` finds the exact text and
# `substr()` rebuilds the record around it. `sub()` would read `$2` as a
# regular expression and `&` in `$3` as the matched text. The committed
# column carries no metacharacter, so the two agree on it; the
# metacharacter-column attack below puts one there, and it is the case
# that turns red if this is written as a substitution.
set_column() {
  awk -v site="$1" -v old="$2" -v new="$3" '
    !patched && $1 == "rewrite" && $2 == site {
      p = index($0, old)
      if (p) { $0 = substr($0, 1, p - 1) new substr($0, p + length(old)); patched = 1 }
    }
    { print }
  ' "$work/manifest.txt" > "$work/m2"
  mv "$work/m2" "$work/manifest.txt"
}

# What ran, counted by the harnesses and asserted at the end. A closing
# line carrying a constant is a claim wider than its evidence: round 1
# of this plan's review deleted five test bodies, left the constant
# alone, and the suite still printed the old number and exited 0.
attacks=0
must_pass=0
refusals=0

# Runs the check against the scratch copy and requires it to PASS with a
# message containing `$2`. `$1` names the run in any failure message.
expect_pass() {
  what=$1
  want=$2
  set +e
  out=$(REPO="$REPO" ROOT="$work/tree" MANIFEST="$work/manifest.txt" \
        EXPECTED="$work/expected.txt" \
        sh "$REPO/ci/checks/doc_sites.sh" 2>&1)
  code=$?
  set -e
  [ "$code" -eq 0 ] || fail "$what: the check should have passed: $out"
  case "$out" in
    *"$want"*) echo "  passed: $out" ;;
    *) fail "$what: passed with the wrong message: $out" ;;
  esac
  must_pass=$((must_pass + 1))
}

# Runs the check against the scratch copy and requires it to fail with a
# message containing `$1`. `$base_override`, when set, is passed as the
# check's `BASE`, standing in for the frozen base revision; empty, the
# check reads the revision from the manifest as it always does.
base_override=
expect_fail() {
  want=$1
  set +e
  out=$(REPO="$REPO" ROOT="$work/tree" MANIFEST="$work/manifest.txt" \
        EXPECTED="$work/expected.txt" BASE="$base_override" \
        sh "$REPO/ci/checks/doc_sites.sh" 2>&1)
  code=$?
  set -e
  [ "$code" -ne 0 ] || fail "attack '$want' was not caught: the check exited 0"
  case "$out" in
    *"$want"*) echo "  caught: $out" ;;
    *) fail "attack '$want' produced the wrong message: $out" ;;
  esac
  attacks=$((attacks + 1))
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
  refusals=$((refusals + 1))
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

# The first `rewrite` row that carries a `refs=` column, and that
# column's text, derived rather than written down so no test here goes
# stale when a record moves.
refs_row_site=$(awk '$1 == "rewrite" {
  for (i = 3; i <= NF; i++) if ($i ~ /^refs=/) { print $2; exit }
}' "$REPO/ci/checks/doc_sites.txt")
refs_row_col=$(awk '$1 == "rewrite" {
  for (i = 3; i <= NF; i++) if ($i ~ /^refs=/) { print $i; exit }
}' "$REPO/ci/checks/doc_sites.txt")
[ -n "$refs_row_site" ] && [ -n "$refs_row_col" ] \
  || fail "no rewrite row carries a refs= column, so the refs= tests cannot be run"
refs_row_file=${refs_row_site%%:*}
refs_row_first=${refs_row_col#refs=}; refs_row_first=${refs_row_first%%,*}

# A column with more than one entry, for the attack that puts a space
# after a comma: with one entry there is no comma to widen.
refs_multi_site=$(awk '$1 == "rewrite" {
  for (i = 3; i <= NF; i++) if ($i ~ /^refs=#[0-9]+,/) { print $2; exit }
}' "$REPO/ci/checks/doc_sites.txt")
[ -n "$refs_multi_site" ] \
  || fail "no refs= column lists two references, so the spaced-column attack cannot be run"

# A `leave` record whose file NO other record names. The three
# unterminated-record tests move this one to the end: if the reader that
# builds the scratch tree skips it, that file is missing from the tree
# and the check says `path absent in the tree` — which is how those
# tests see a defect in `copy_tree` and not only in the check.
unique_leave=$(awk '
  NR == FNR { if ($1 != "#" && NF) { split($2, s, ":"); c[s[1]]++ }; next }
  $1 == "leave" { split($2, s, ":"); if (c[s[1]] == 1) { print $2; exit } }
' "$REPO/ci/checks/doc_sites.txt" "$REPO/ci/checks/doc_sites.txt")
[ -n "$unique_leave" ] \
  || fail "no leave record names a file of its own, so the unterminated-record tests cannot be run"
unique_leave_file=${unique_leave%%:*}
unique_leave_start=${unique_leave#*:}; unique_leave_start=${unique_leave_start%%-*}

manifest_records=$(awk '$1 != "#" && NF' "$REPO/ci/checks/doc_sites.txt" | wc -l)
[ "$manifest_records" -gt 0 ] || fail "the manifest has no records"

rewrite_file=${first_rewrite%%:*}
rewrite_start=${first_rewrite#*:}; rewrite_start=${rewrite_start%%-*}
leave_file=${first_leave%%:*}
leave_start=${first_leave#*:}; leave_start=${leave_start%%-*}
leave_end=${first_leave##*-}

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
expect_pass "the clean copy" "checked $manifest_records sites"

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
set_record "leave $first_leave" "leave ${leave_file}:$((leave_start + 1))-${leave_end}"
expect_fail "site set differs from the committed expected set"

echo "self-test: a record edit matches a path literally, not as a pattern"
# `$leave_file` carries a `.`, a metacharacter in a basic regular
# expression. This manifest carries both `$leave_file` and the path that
# differs from it only at that position. The literal edit touches one
# record, two distinct sites remain, and the check refuses with `site set
# differs`. A substitution matches both records, they collapse onto one
# site, and the check refuses with `duplicate site rows` instead — which
# is how this case sees which edit ran.
copy_tree
case "$leave_file" in
  *.*) ;;
  *) fail "the first leave row's path has no '.', so the literal-path attack cannot be run" ;;
esac
twin=${leave_file%.*}X${leave_file##*.}
cp "$work/tree/$leave_file" "$work/tree/$twin"
printf 'leave %s:%s-%s\n' "$twin" "$leave_start" "$leave_end" >> "$work/manifest.txt"
set_record "leave $first_leave" "leave ${leave_file}:$((leave_start + 1))-${leave_end}"
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
git -C "$REPO" update-ref "$SELF_TEST_REFS/absorbed" "$absorbed"
expect_refusal_without_upstream "the change already merged to the base"

echo "self-test: a later unrelated feature merge selects nothing"
copy_tree
feature=$(throwaway_commit "$BASE^{tree}" -p "$BASE")
later=$(throwaway_commit "HEAD^{tree}" -p HEAD -p "$feature")
git -C "$REPO" update-ref "$SELF_TEST_REFS/feature" "$later"
expect_refusal_without_upstream "a later unrelated feature merge"

echo "self-test: a merge back-dated ahead of its own ancestor selects nothing"
copy_tree
THROWAWAY_DATE="2030-01-01T00:00:00+0000" older=$(throwaway_commit "HEAD^{tree}" -p HEAD -p "$BASE")
newer=$(THROWAWAY_DATE="2001-01-01T00:00:00+0000" throwaway_commit "HEAD^{tree}" -p "$older" -p "$feature")
git -C "$REPO" update-ref "$SELF_TEST_REFS/backdated" "$newer"
expect_refusal_without_upstream "a descendant merge dated before its ancestor"

echo "self-test: a declared reference licenses a line this change adds"
# The only test here that shows the `refs=` column still LICENSES
# anything. Deleting the column's feed into the allowed set leaves every
# red path above green, because with the supplied upstream revision
# equal to the checked-out commit the added-line set is empty.
copy_tree
printf 'doc-sites self-test: a licensed reference %s\n' "$refs_row_first" \
  >> "$work/tree/$refs_row_file"
expect_pass "a licensed reference on an added line" "checked $manifest_records sites"

echo "self-test: a final record with no newline is still checked"
# A known record is MOVED to the end rather than trusting which record
# the manifest happens to end with. The site set is sorted before it is
# compared, so moving a record changes nothing else.
grep -v -x -F "leave $unique_leave" "$REPO/ci/checks/doc_sites.txt" > "$work/m2"
printf 'leave %s' "$unique_leave" >> "$work/m2"
copy_tree "$work/m2"
expect_pass "a final record with no newline" "checked $manifest_records sites"

echo "self-test: a declared reference the text does not carry"
# Per entry, not per column: the other entries on this row are present,
# and the message names the one that is not.
copy_tree
set_column "$refs_row_site" "$refs_row_col" "$refs_row_col,#999999"
expect_fail "declared reference #999999 is absent"

# The declared number occurs in the file only as the start of a longer
# token. `#999999x` and `#999999_` are not issue references, so they do
# not record `#999999`. The appended line also reaches the added-lines
# rule, which reads `#999999` out of it and finds it licensed by the
# declaration, so the refusal can only come from the presence rule.
# Before the digit run had to end the token, both of these passed with
# `checked 27 sites`.
for suffix in x _; do
  echo "self-test: a declared reference present only as #N followed by '$suffix'"
  copy_tree
  printf 'doc-sites self-test: not-an-issue #999999%s\n' "$suffix" \
    >> "$work/tree/$refs_row_file"
  set_column "$refs_row_site" "$refs_row_col" "$refs_row_col,#999999"
  expect_fail "declared reference #999999 is absent from $refs_row_file"
done

# The same at the START of the token: a letter or an underscore right
# before the `#` makes `abc#999999` and `x_#999999` something other than
# a reference to #999999. As above, the added-lines rule reads #999999
# out of the appended line and finds it licensed, so the refusal can only
# come from the presence rule. Before the reference had to start the
# token, both of these passed with `checked 27 sites`.
for prefix in abc x_; do
  echo "self-test: a declared reference present only as #N preceded by '$prefix'"
  copy_tree
  printf 'doc-sites self-test: not-an-issue %s#999999\n' "$prefix" \
    >> "$work/tree/$refs_row_file"
  set_column "$refs_row_site" "$refs_row_col" "$refs_row_col,#999999"
  expect_fail "declared reference #999999 is absent from $refs_row_file"
done

echo "self-test: a longer token in a protected block does not allow its number"
# The allowed set is read from the protected blocks at the frozen base.
# That revision cannot be edited, so this case builds a throwaway commit
# identical to it except that the first rewrite row's protected line
# also carries `#999999x`, and passes it as the check's `BASE`. A line
# the change adds then cites #999999, with no declaration. `#999999x` is
# not a reference to #999999, so nothing allows it and the check must
# refuse. Read with the added-lines extractor instead, the protected
# block would allow #999999 and the check would pass: measured, exit 0.
copy_tree
git show "$BASE:$rewrite_file" \
  | awk -v n="$rewrite_start" 'NR == n { print $0 " not-an-issue #999999x"; next } { print }' \
  > "$work/base_file"
base_mode=$(git -C "$REPO" ls-tree "$BASE" -- "$rewrite_file" | cut -d' ' -f1)
base_blob=$(git -C "$REPO" hash-object -w "$work/base_file")
GIT_INDEX_FILE="$work/index" git -C "$REPO" read-tree "$BASE"
GIT_INDEX_FILE="$work/index" git -C "$REPO" update-index \
  --cacheinfo "$base_mode,$base_blob,$rewrite_file"
base_tree=$(GIT_INDEX_FILE="$work/index" git -C "$REPO" write-tree)
base_override=$(throwaway_commit "$base_tree" -p "$BASE")
printf 'doc-sites self-test: follow-up #999999\n' >> "$work/tree/$rewrite_file"
expect_fail "new issue number #999999 in rewrite row: $rewrite_file"
base_override=

echo "self-test: a refs= column on a leave row"
# #494 is allowed on every file, so what is caught is the row the column
# sits on, not the number in it.
copy_tree
awk -v site="$first_leave" '
  !patched && $1 == "leave" && $2 == site { print $0 " refs=#494"; patched = 1; next }
  { print }
' "$work/manifest.txt" > "$work/m2"
mv "$work/m2" "$work/manifest.txt"
expect_fail "refs= column on a leave row"

echo "self-test: a refs= entry with no hash"
copy_tree
set_column "$refs_row_site" "$refs_row_col" "$refs_row_col,999999"
expect_fail "malformed refs= column"

echo "self-test: a space inside a refs= column"
# The shape the presence rule alone cannot see: every entry it can read
# is present, because the entry after the space was dropped before it
# got there.
copy_tree
awk -v site="$refs_multi_site" '
  !patched && $1 == "rewrite" && $2 == site {
    for (i = 3; i <= NF; i++) if ($i ~ /^refs=#[0-9]+,/) { sub(/,/, ", ", $i); patched = 1 }
  }
  { print }
' "$work/manifest.txt" > "$work/m2"
mv "$work/m2" "$work/manifest.txt"
expect_fail "malformed refs= column"

echo "self-test: a refs= column that is a pattern, not a literal"
# The comparison against the file's references is literal (`grep -Fqx` in
# `doc_sites.sh`). Nothing can reach it carrying a regular expression,
# because this rule refuses the column first — which is why reverting
# that `-F` alone reddens no case here. Measured with GNU grep 3.12:
# `grep -qx -- '#98765.'` matches `#987654` and `grep -Fqx` does not.
copy_tree
set_column "$refs_row_site" "$refs_row_col" "refs=${refs_row_first}."
expect_fail "malformed refs= column"

echo "self-test: a column edit matches its column text literally"
# Two edits in a row. The first sets the column to a text that IS a basic
# regular expression; the second appends an entry to that exact text.
# `index()` finds it and the record reads `refs=#[0-9]*,#999999`. A
# substitution would match `refs=#` — the same pattern with zero digits —
# and leave `[0-9]*` behind, giving `refs=#[0-9]*,#999999[0-9]*`. R2d
# refuses both and quotes the column it read, so the two spell different
# messages and this case sees which edit ran.
copy_tree
set_column "$refs_row_site" "$refs_row_col" 'refs=#[0-9]*'
set_column "$refs_row_site" 'refs=#[0-9]*' 'refs=#[0-9]*,#999999'
expect_fail "malformed refs= column for $refs_row_site: 'refs=#[0-9]*,#999999' (want"

echo "self-test: a manifest with Windows line endings"
copy_tree
awk '{ printf "%s\r\n", $0 }' "$work/manifest.txt" > "$work/m2"
mv "$work/m2" "$work/manifest.txt"
expect_fail "carriage returns"

echo "self-test: a byte one parser drops and another keeps"
# A NUL inside `rewrite`: `read` drops it and `awk` keeps it, so the row
# loop checks a row every `awk` reader skips — and the declaration on
# that row, here naming a reference the file does not carry, goes
# unchecked. Measured on the version before the rule: exit 0.
copy_tree
awk -v site="$refs_row_site" -v col="$refs_row_col" '
  !patched && $1 == "rewrite" && $2 == site {
    print "rew" sprintf("%c", 0) "rite " site " " col ",#999999"
    patched = 1
    next
  }
  { print }
' "$work/manifest.txt" > "$work/m2"
mv "$work/m2" "$work/manifest.txt"
expect_fail "disagree about the record on"

echo "self-test: a final record with no newline whose protected text changed"
grep -v -x -F "leave $unique_leave" "$REPO/ci/checks/doc_sites.txt" > "$work/m2"
printf 'leave %s' "$unique_leave" >> "$work/m2"
copy_tree "$work/m2"
# By CONTENT, not by line number: the range is a coordinate in the
# frozen base and this file's lines have moved since, so patching line
# $unique_leave_start of the tree copy would edit a different sentence
# and leave the protected one in place. Measured: it did.
blockline=$(git show "$BASE:$unique_leave_file" | sed -n "${unique_leave_start}p")
awk -v t="$blockline" '
  !patched && $0 == t { print "doc-sites self-test: the final record text was replaced"; patched = 1; next }
  { print }
' "$work/tree/$unique_leave_file" > "$work/patched"
mv "$work/patched" "$work/tree/$unique_leave_file"
expect_fail "leave row changed"

echo "self-test: a final record with no newline carrying a refs= column"
grep -v -x -F "leave $unique_leave" "$REPO/ci/checks/doc_sites.txt" > "$work/m2"
printf 'leave %s refs=#494' "$unique_leave" >> "$work/m2"
copy_tree "$work/m2"
expect_fail "refs= column on a leave row"

echo "self-test: an empty manifest"
copy_tree
: > "$work/manifest.txt"
expect_fail "empty or missing manifest"

[ "$attacks" -eq 26 ] && [ "$must_pass" -eq 3 ] && [ "$refusals" -eq 3 ] \
  || fail "the suite ran $attacks attacks, $must_pass must-pass runs and $refusals refusals; it must run 26, 3 and 3"
echo "doc-sites-self-test: $attacks attacks caught, $must_pass must-pass runs passed, $refusals graph shapes refused"
