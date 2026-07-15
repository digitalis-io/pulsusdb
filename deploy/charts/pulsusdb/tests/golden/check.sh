#!/usr/bin/env bash
# Golden snapshot drift check (issue #38 architect plan, "Testing Approach":
# "helm template snapshot comparison ... to catch unintended manifest drift
# in review"). Re-renders the chart with the same value sets `single.yaml`/
# `cluster.yaml` were captured with, strips `kind: Secret` documents (the
# bundled ClickHouse password is randomly generated per-render — see each
# golden file's header comment — so Secret content can never be stable),
# and diffs the result against the committed golden file.
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

strip_secrets() {
    # Drops any YAML document whose `kind:` is exactly `Secret`.
    awk 'BEGIN{doc=""} /^---$/{if (doc !~ /\nkind: Secret[ \t]*\n/ && doc !~ /^kind: Secret[ \t]*\n/) printf "%s---\n", doc; doc=""; next} {doc = doc $0 "\n"} END{if (doc !~ /\nkind: Secret[ \t]*\n/ && doc !~ /^kind: Secret[ \t]*\n/) printf "%s", doc}'
}

render_stripped() {
    local extra_args=("$@")
    helm template pulsusdb "$CHART_DIR" "${extra_args[@]}" | strip_secrets
}

check_one() {
    local name="$1"; shift
    local golden="$GOLDEN_DIR/$name.yaml"
    local header
    header=$(head -n 8 "$golden")
    local rendered
    rendered=$(render_stripped "$@")
    if [ "$UPDATE" = "1" ]; then
        { printf '%s\n' "$header"; printf '%s\n' "$rendered"; } > "$golden"
        echo "updated $golden"
        return 0
    fi
    local want
    want=$(tail -n +9 "$golden")
    if [ "$rendered" != "$want" ]; then
        echo "::error::golden drift in $golden (run with --update to refresh after a deliberate change)"
        diff <(echo "$want") <(echo "$rendered") || true
        return 1
    fi
    echo "ok: $name"
}

fail=0
check_one single || fail=1
check_one cluster --set topology=cluster --set clickhouse.shards=3 || fail=1
exit $fail
