#!/usr/bin/env bash
# Golden snapshot drift check (issue #38 architect plan, "Testing Approach":
# "helm template snapshot comparison ... to catch unintended manifest drift
# in review"). Re-renders the chart with the same value sets `single.yaml`/
# `cluster.yaml` were captured with, strips `kind: Secret` documents (the
# bundled ClickHouse password is randomly generated per-render — see each
# golden file's header comment — so Secret content can never be stable),
# and compares header plus render, byte for byte, with the committed file.
# `--update` writes exactly the bytes the check compares, and only after
# every render has succeeded: one failed render and no golden is written.
#
# Run from the repo root:
#   deploy/charts/pulsusdb/tests/golden/check.sh
#
# Regenerate goldens after a deliberate template change:
#   deploy/charts/pulsusdb/tests/golden/check.sh --update
set -euo pipefail

CHART_DIR="deploy/charts/pulsusdb"
GOLDEN_DIR="$CHART_DIR/tests/golden"
UPDATE=0
[ "${1:-}" = "--update" ] && UPDATE=1

# The header every golden starts with. Held here, not read back from the
# golden, so the check covers it like every other byte.
header() {
    cat <<'HEADER'
# Golden snapshot (issue #38) — `helm template` output for manifest-drift review.
# Regenerate with:
#   deploy/charts/pulsusdb/tests/golden/check.sh --update
# `kind: Secret` documents are deliberately excluded — the bundled ClickHouse
# password is randomly generated per-install (`clickhouse-secret.yaml`'s
# `lookup`-guarded `randAlphaNum`), so a Secret's rendered content is never
# stable across renders and would make this file permanently noisy.

HEADER
}

strip_secrets() {
    # Drops any YAML document whose `kind:` is exactly `Secret`.
    awk 'BEGIN{doc=""} /^---$/{if (doc !~ /\nkind: Secret[ \t]*\n/ && doc !~ /^kind: Secret[ \t]*\n/) printf "%s---\n", doc; doc=""; next} {doc = doc $0 "\n"} END{if (doc !~ /\nkind: Secret[ \t]*\n/ && doc !~ /^kind: Secret[ \t]*\n/) printf "%s", doc}'
}

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# Writes the expected golden bytes for one value set to "$WORK/$name".
# Every step's status is tested explicitly: this runs on the left of `||`,
# where `set -e` does not apply.
expected() {
    local name="$1"; shift
    if ! helm template pulsusdb "$CHART_DIR" "$@" > "$WORK/$name.raw"; then
        echo "::error::helm template failed for $name; nothing compared or written"
        return 1
    fi
    { header && strip_secrets < "$WORK/$name.raw"; } > "$WORK/$name" || return 1
}

SETS="single cluster"
args_for() {
    case "$1" in
        single) echo "--set initJob.retentionDays=7" ;;
        cluster) echo "--set initJob.retentionDays=7 --set topology=cluster --set clickhouse.shards=3" ;;
    esac
}

# Every render first: one failure and nothing is compared or written.
for name in $SETS; do
    # shellcheck disable=SC2046
    expected "$name" $(args_for "$name") || exit 1
done

fail=0
for name in $SETS; do
    golden="$GOLDEN_DIR/$name.yaml"
    if [ "$UPDATE" = "1" ]; then
        cp "$WORK/$name" "$golden" || exit 1
        echo "updated $golden"
    elif cmp -s "$WORK/$name" "$golden"; then
        echo "ok: $name"
    else
        echo "::error::golden drift in $golden (run with --update to refresh after a deliberate change)"
        diff -u "$golden" "$WORK/$name" || true
        fail=1
    fi
done
exit $fail
