#!/bin/sh
# Creates the pulsusdb schema. Drops the database first and builds it fresh.
#
# The binary does not create schema. This script is the only thing that
# does, and `schema/schema.sql` is the only place the DDL is written.
#
#   schema/schema.sh                 # single-node, database from the environment
#   PULSUS_CLUSTER=prod schema/schema.sh
#   schema/schema.sh --print         # render the statements, send nothing
#
# There are no migrations. A schema change is an edit to `schema.sql` and
# another run of this script, which drops what is there and recreates it.
#
# Configuration comes from the same environment variables the binary reads
# (docs/configuration.md). `PULSUS_CLUSTER` selects the clustered variant:
# `Replicated*` engines, `ON CLUSTER`, the `_dist` wrappers.
#
# ClickHouse's own `{shard}` and `{replica}` macros must reach the server as
# those exact characters. Every substitution here is a double-brace
# `{{token}}`, which cannot match them, and no expression below matches a
# single brace pair.
set -eu

here=$(dirname "$0")
sql="$here/schema.sql"

mode=cluster
[ -n "${PULSUS_CLUSTER:-}" ] || mode=single
print_only=0

while [ $# -gt 0 ]; do
    case "$1" in
        --print) print_only=1 ;;
        --mode)
            shift
            case "${1:-}" in
                single | cluster) mode=$1 ;;
                *) echo "schema.sh: --mode takes single or cluster" >&2; exit 2 ;;
            esac
            ;;
        -h | --help) sed -n '2,25p' "$0"; exit 0 ;;
        *) echo "schema.sh: unknown argument $1" >&2; exit 2 ;;
    esac
    shift
done

db=${CLICKHOUSE_DB:-pulsus}
cluster=${PULSUS_CLUSTER:-}
dist_suffix=${PULSUS_DIST_SUFFIX:-_dist}
storage_policy=${PULSUS_STORAGE_POLICY:-}
retention_days=${PULSUS_RETENTION_DAYS:-7}
rollup=${PULSUS_LOG_ROLLUP_RESOLUTION:-5s}
metrics_landing_hours=${PULSUS_METRICS_LANDING_RETENTION_HOURS:-6}
log_landing_hours=${PULSUS_LOG_LANDING_RETENTION_HOURS:-6}
trace_landing_hours=${PULSUS_TRACE_LANDING_RETENTION_HOURS:-6}
metrics_dedup=${PULSUS_METRICS_DEDUP_WINDOW:-10000}
log_dedup=${PULSUS_LOG_DEDUP_WINDOW:-10000}
trace_dedup=${PULSUS_TRACE_DEDUP_WINDOW:-10000}

# `pulsus_schema::DEDUP_WINDOW_SECONDS`. Not a knob: the startup check in
# `pulsus-server` holds it against the landing budget.
dedup_window_seconds=3600

# Names reach the server inside SQL, so they are identifiers and nothing
# else. Refusing here is simpler than quoting, and the binary's own
# configuration check is no wider.
for pair in "database:$db" "cluster:$cluster" "dist_suffix:$dist_suffix" \
            "storage_policy:$storage_policy"; do
    what=${pair%%:*}
    value=${pair#*:}
    case "$value" in
        '') ;;
        *[!A-Za-z0-9_]*)
            echo "schema.sh: $what must be letters, digits and underscores: $value" >&2
            exit 2
            ;;
    esac
done

if [ "$mode" = cluster ] && [ -z "$cluster" ]; then
    echo "schema.sh: --mode cluster needs PULSUS_CLUSTER set to the cluster name" >&2
    exit 2
fi

# `{{log_rollup_suffix}}` names a table, so it has to be the same string
# `pulsus_schema::rollup_suffix` produces: whole minutes, then whole
# seconds, then milliseconds, then nanoseconds.
case "$rollup" in
    *ms) rollup_ns=$(( ${rollup%ms} * 1000000 )) ;;
    *ns) rollup_ns=${rollup%ns} ;;
    *s)  rollup_ns=$(( ${rollup%s} * 1000000000 )) ;;
    *m)  rollup_ns=$(( ${rollup%m} * 60000000000 )) ;;
    *)
        echo "schema.sh: PULSUS_LOG_ROLLUP_RESOLUTION must end in ns, ms, s or m: $rollup" >&2
        exit 2
        ;;
esac
if [ "$rollup_ns" -le 0 ]; then
    echo "schema.sh: PULSUS_LOG_ROLLUP_RESOLUTION must be positive: $rollup" >&2
    exit 2
fi
if [ $((rollup_ns % 1000000)) -eq 0 ]; then
    millis=$((rollup_ns / 1000000))
    if [ $((millis % 60000)) -eq 0 ]; then
        rollup_suffix="$((millis / 60000))m"
    elif [ $((millis % 1000)) -eq 0 ]; then
        rollup_suffix="$((millis / 1000))s"
    else
        rollup_suffix="${millis}ms"
    fi
else
    rollup_suffix="${rollup_ns}ns"
fi

on_cluster=""
cluster_token=""
route_suffix=""
if [ "$mode" = cluster ]; then
    on_cluster=" ON CLUSTER '$cluster'"
    cluster_token=$cluster
    route_suffix=$dist_suffix
fi

policy_token=""
[ -z "$storage_policy" ] || policy_token=", storage_policy = '$storage_policy'"

other=single
[ "$mode" = single ] && other=cluster

render() {
    sed -e "/^--@$other /d" -e "s/^--@$mode *//" -e "/^--/d" "$sql" |
        sed -e "s|{{db}}|$db|g" \
            -e "s|{{on_cluster}}|$on_cluster|g" \
            -e "s|{{cluster}}|$cluster_token|g" \
            -e "s|{{dist_suffix}}|$dist_suffix|g" \
            -e "s|{{route_suffix}}|$route_suffix|g" \
            -e "s|{{retention_days}}|$retention_days|g" \
            -e "s|{{log_rollup_suffix}}|$rollup_suffix|g" \
            -e "s|{{log_rollup_ns}}|$rollup_ns|g" \
            -e "s|{{metrics_landing_retention_hours}}|$metrics_landing_hours|g" \
            -e "s|{{log_landing_retention_hours}}|$log_landing_hours|g" \
            -e "s|{{trace_landing_retention_hours}}|$trace_landing_hours|g" \
            -e "s|{{metrics_dedup_window}}|$metrics_dedup|g" \
            -e "s|{{log_dedup_window}}|$log_dedup|g" \
            -e "s|{{trace_dedup_window}}|$trace_dedup|g" \
            -e "s|{{dedup_window_seconds}}|$dedup_window_seconds|g" \
            -e "s|{{storage_policy}}|$policy_token|g"
}

rendered=$(render)

# A token this script does not know would otherwise reach the server as
# literal braces.
if printf '%s\n' "$rendered" | grep -n '{{'; then
    echo "schema.sh: the lines above carry a token this script does not render" >&2
    exit 2
fi

if [ "$print_only" = 1 ]; then
    printf '%s\n' "$rendered"
    exit 0
fi

server=${CLICKHOUSE_SERVER:-localhost}
port=${CLICKHOUSE_HTTP_PORT:-8123}
url="http://$server:$port/"

# `CLICKHOUSE_AUTH` is `user:password`, split on the FIRST colon — the same
# variable and the same rule the binary reads (docs/configuration.md §2), so
# a deployment configures the credential once.
auth=""
[ -z "${CLICKHOUSE_AUTH:-}" ] || auth="--user ${CLICKHOUSE_AUTH}"

# `curl` exits 0 when ClickHouse answers with an error in the body, so every
# request below carries --fail-with-body.

# shellcheck disable=SC2086
ask() {
    curl -sS --fail-with-body $auth "$url" --data-binary "$1"
}

# shellcheck disable=SC2086
send_file() {
    curl -sS --fail-with-body $auth "$url" --data-binary "@$1"
}

version=$(ask 'SELECT version() FORMAT TSVRaw')
major=${version%%.*}
rest=${version#*.}
minor=${rest%%.*}
case "$major$minor" in
    *[!0-9]*)
        echo "schema.sh: could not read a version from '$version'" >&2
        exit 1
        ;;
esac
if [ "$major" -lt 26 ] || { [ "$major" -eq 26 ] && [ "$minor" -lt 3 ]; }; then
    echo "schema.sh: clickhouse $version is below the minimum supported version 26.3" >&2
    exit 1
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM

# The HTTP interface refuses a multi-statement body (`Code: 62`), so each
# statement is its own request. A statement ends at a line whose last
# character is a semicolon, which is exact for this file: no other line
# carries one, and `tests/schema_file.rs` holds that.
printf '%s\n' "$rendered" | awk -v dir="$tmp" '
    { stmt = stmt $0 "\n" }
    /;$/ { n += 1; printf "%s", stmt > sprintf("%s/%04d.sql", dir, n); close(sprintf("%s/%04d.sql", dir, n)); stmt = "" }
    END {
        if (stmt ~ /[^[:space:]]/) { print "schema.sh: the file ends in an unterminated statement" > "/dev/stderr"; exit 1 }
    }
'

echo "schema.sh: dropping database $db and building it fresh ($mode)"
ask "DROP DATABASE IF EXISTS \`$db\`${on_cluster} SYNC" >/dev/null

count=0
for f in "$tmp"/*.sql; do
    send_file "$f" >/dev/null
    count=$((count + 1))
done

echo "schema.sh: $count statements applied to $db"
