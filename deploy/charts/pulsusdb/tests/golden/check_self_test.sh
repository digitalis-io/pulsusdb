#!/usr/bin/env bash
# Self-test for check.sh (issue #506). Each case runs check.sh against a
# copy of the chart in a scratch directory, so the committed goldens are
# never written, and judges the result against the committed bytes with
# `cmp` — never with check.sh's own comparison, which is the thing under
# test.
#
# Run from the repo root:
#   deploy/charts/pulsusdb/tests/golden/check_self_test.sh
set -uo pipefail

CHART="deploy/charts/pulsusdb"
GOLDENS="single cluster"
SCRATCH=$(mktemp -d)
trap 'rm -rf "$SCRATCH"' EXIT
failures=0

# A fresh copy of the chart; prints the directory to run check.sh from.
fresh() {
    local dir="$SCRATCH/$1"
    mkdir -p "$dir/deploy/charts"
    cp -a "$CHART" "$dir/deploy/charts/"
    echo "$dir"
}

# Runs the copy's check.sh from its own root; prints its exit status.
run_check() {
    local dir="$1"; shift
    (cd "$dir" && bash "$CHART/tests/golden/check.sh" "$@" > "$dir.out" 2>&1)
    echo $?
}

same_as_committed() {
    local dir="$1" g
    for g in $GOLDENS; do
        cmp -s "$CHART/tests/golden/$g.yaml" "$dir/$CHART/tests/golden/$g.yaml" || return 1
    done
}

verdict() {
    if [ "$2" = "pass" ]; then echo "ok:   $1"; else echo "FAIL: $1"; failures=$((failures + 1)); fi
}

# 1. Control: the committed goldens pass.
d=$(fresh control)
[ "$(run_check "$d")" = 0 ] && verdict "check passes on the committed goldens" pass \
    || verdict "check passes on the committed goldens" fail

# 2. --update on an unmodified chart rewrites both goldens to their committed bytes.
d=$(fresh update)
if [ "$(run_check "$d" --update)" = 0 ] && same_as_committed "$d"; then
    verdict "--update reproduces the committed bytes" pass
else
    verdict "--update reproduces the committed bytes" fail
fi

# 3. The check accepts what --update wrote.
[ "$(run_check "$d")" = 0 ] && verdict "check passes after --update" pass \
    || verdict "check passes after --update" fail

# 4. A changed header line is drift.
d=$(fresh header)
sed -i '1s/.*/# changed/' "$d/$CHART/tests/golden/single.yaml"
[ "$(run_check "$d")" = 1 ] && verdict "a changed header line fails the check" pass \
    || verdict "a changed header line fails the check" fail

# 5. A trailing blank line is drift.
d=$(fresh trailing)
printf '\n' >> "$d/$CHART/tests/golden/cluster.yaml"
[ "$(run_check "$d")" = 1 ] && verdict "a trailing blank line fails the check" pass \
    || verdict "a trailing blank line fails the check" fail

# 6. Control: a changed rendered line is drift.
d=$(fresh body)
sed -i 's/^kind: ServiceAccount$/kind: ServiceAccountX/' "$d/$CHART/tests/golden/single.yaml"
[ "$(run_check "$d")" = 1 ] && verdict "a changed rendered line fails the check" pass \
    || verdict "a changed rendered line fails the check" fail

# 7. A failed render writes nothing and fails --update.
d=$(fresh render-fails)
mkdir -p "$d.bin"
printf '#!/bin/sh\necho "helm: deliberate failure" >&2\nexit 1\n' > "$d.bin/helm"
chmod +x "$d.bin/helm"
status=$(PATH="$d.bin:$PATH" run_check "$d" --update)
if [ "$status" != 0 ] && same_as_committed "$d"; then
    verdict "a failed render fails --update and leaves both goldens unchanged" pass
else
    verdict "a failed render fails --update and leaves both goldens unchanged" fail
fi

# 8. Only the second render fails: --update writes neither golden, so a
#    golden that differs from its render keeps its bytes.
d=$(fresh second-render-fails)
helm_bin=$(command -v helm)
mkdir -p "$d.bin"
printf '#!/bin/sh\ncase "$*" in *topology=cluster*) echo "helm: deliberate failure" >&2; exit 1 ;; esac\nexec "%s" "$@"\n' "$helm_bin" > "$d.bin/helm"
chmod +x "$d.bin/helm"
sed -i 's/^kind: ServiceAccount$/kind: ServiceAccountX/' "$d/$CHART/tests/golden/single.yaml"
cp "$d/$CHART/tests/golden/single.yaml" "$d.single-before"
status=$(PATH="$d.bin:$PATH" run_check "$d" --update)
if [ "$status" != 0 ] && cmp -s "$d.single-before" "$d/$CHART/tests/golden/single.yaml" \
    && cmp -s "$CHART/tests/golden/cluster.yaml" "$d/$CHART/tests/golden/cluster.yaml"; then
    verdict "a failed second render fails --update and writes neither golden" pass
else
    verdict "a failed second render fails --update and writes neither golden" fail
fi

echo "check_self_test: $failures failed"
[ "$failures" = 0 ]
