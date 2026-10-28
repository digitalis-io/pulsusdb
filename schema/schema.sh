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
# Configuration comes from the environment (docs/configuration.md).
# `PULSUS_RETENTION_DAYS` is required, and the binary reads it back from the
# database. `PULSUS_CLUSTER` selects the clustered variant (`Replicated*`, `ON CLUSTER`, `_dist`).
# PULSUS_STORAGE_POLICY and PULSUS_COLD_VOLUME move old parts to a cold volume (docs/configuration.md §3).
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
retention_days=${PULSUS_RETENTION_DAYS:-}
rollup=${PULSUS_LOG_ROLLUP_RESOLUTION:-5s}
metrics_landing_hours=${PULSUS_METRICS_LANDING_RETENTION_HOURS:-6}
log_landing_hours=${PULSUS_LOG_LANDING_RETENTION_HOURS:-6}
trace_landing_hours=${PULSUS_TRACE_LANDING_RETENTION_HOURS:-6}
metrics_dedup=${PULSUS_METRICS_DEDUP_WINDOW:-10000}
log_dedup=${PULSUS_LOG_DEDUP_WINDOW:-10000}
trace_dedup=${PULSUS_TRACE_DEDUP_WINDOW:-10000}
rollup_levels=${PULSUS_ROLLUP_LEVELS-5,60,720,7200,86400}
rollup_ranges=${PULSUS_ROLLUP_RANGES-3600,86400,604800,7776000}
counter_buckets=${PULSUS_ROLLUP_COUNTER_BUCKETS-5}
cold_volume=${PULSUS_COLD_VOLUME:-}
metrics_hot=${PULSUS_METRICS_HOT_DAYS:-}
logs_hot=${PULSUS_LOGS_HOT_DAYS:-}
traces_hot=${PULSUS_TRACES_HOT_DAYS:-}
rollup_retention=${PULSUS_ROLLUP_RETENTION_DAYS-}
retention_given=${PULSUS_ROLLUP_RETENTION_DAYS+1}
rollup_hot=${PULSUS_ROLLUP_HOT_DAYS-}
hot_given=${PULSUS_ROLLUP_HOT_DAYS+1}

# `pulsus_schema::DEDUP_WINDOW_SECONDS`. Not a knob: the startup check in
# `pulsus-server` holds it against the landing budget.
dedup_window_seconds=3600

# `pulsus_schema::GRAPH_JOB_MAX_ROWS`: the rows one run of the service-graph
# job may read (issue #599). Not a knob.
graph_job_max_rows=60000000

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

# Retention has no default (issue #497). This is the one place it is set:
# the tables' TTLs and the database comment the binary reads it back from
# are both rendered from it (docs/configuration.md §7).
case "$retention_days" in
    '')
        echo "schema.sh: PULSUS_RETENTION_DAYS is not set; retention has no default (docs/configuration.md §7)" >&2
        exit 2
        ;;
    0* | *[!0-9]*)
        echo "schema.sh: PULSUS_RETENTION_DAYS must be a whole number of days from 1 to 4294967295: $retention_days" >&2
        exit 2
        ;;
esac
if [ "${#retention_days}" -gt 10 ] || [ "$retention_days" -gt 4294967295 ]; then
    echo "schema.sh: PULSUS_RETENTION_DAYS must be a whole number of days from 1 to 4294967295: $retention_days" >&2
    exit 2
fi
for pair in "PULSUS_METRICS_LANDING_RETENTION_HOURS:$metrics_landing_hours" \
            "PULSUS_LOG_LANDING_RETENTION_HOURS:$log_landing_hours" \
            "PULSUS_TRACE_LANDING_RETENTION_HOURS:$trace_landing_hours"; do
    what=${pair%%:*}
    value=${pair#*:}
    case "$value" in
        '' | 0* | *[!0-9]* | ????*)
            echo "schema.sh: $what must be a whole number of hours from 1 to 168: $value" >&2
            exit 2
            ;;
    esac
    if [ "$value" -gt 168 ]; then
        echo "schema.sh: $what must be a whole number of hours from 1 to 168: $value" >&2
        exit 2
    fi
done

# Issue #18: `PULSUS_ROLLUP_LEVELS`, the rollup levels in seconds, smallest
# first; set and empty is no levels. The same rules, in the same order and
# with the same messages, as `pulsus_schema::parse_rollup_levels`; the first
# rule that fails is the message.
bad_levels() {
    echo "schema.sh: PULSUS_ROLLUP_LEVELS: $1" >&2
    exit 2
}
if [ -n "$rollup_levels" ]; then
    case "$rollup_levels" in
        ,* | *, | *,,*) bad_levels "an empty item in '$rollup_levels'" ;;
    esac
    old_ifs=$IFS
    IFS=,
    set -f
    # shellcheck disable=SC2086
    set -- $rollup_levels
    set +f
    IFS=$old_ifs
    for item in "$@"; do
        case "$item" in
            0* | *[!0-9]* | ??????*)
                bad_levels "$item is not a whole number of seconds from 1 to 86400" ;;
        esac
        [ "$item" -le 86400 ] ||
            bad_levels "$item is not a whole number of seconds from 1 to 86400"
    done
    for item in "$@"; do
        [ $((86400 % item)) -eq 0 ] || bad_levels "$item does not divide a day (86400)"
    done
    for item in "$@"; do
        if [ "$item" -lt 60 ] && [ $((60 % item)) -ne 0 ]; then
            bad_levels "$item is below 60 and does not divide 60"
        fi
    done
    prev=""
    for item in "$@"; do
        if [ -n "$prev" ] && [ "$item" -le "$prev" ]; then
            bad_levels "$prev then $item: the levels must increase"
        fi
        prev=$item
    done
    prev=""
    for item in "$@"; do
        if [ -n "$prev" ] && [ $((item % prev)) -ne 0 ]; then
            bad_levels "$item is not a multiple of $prev"
        fi
        prev=$item
    done
    [ "$#" -le 8 ] || bad_levels "more than 8 levels"
    period=$1
    [ "$period" -ge 60 ] || period=60
    if [ $((2 * period + 870)) -gt $((metrics_landing_hours * 3600)) ]; then
        bad_levels "the first level $1 outlives metric_landing: 2 × $period + 870 > $metrics_landing_hours × 3600"
    fi
    set --
fi

# Issue #18 follow-up: `PULSUS_ROLLUP_RANGES`, the query range in seconds
# from which each level after the first is the resolution, read only when
# there are levels; set and empty is an empty list. The same rules, in the
# same order and with the same messages, as
# `pulsus_schema::parse_rollup_ranges`.
bad_ranges() {
    echo "schema.sh: PULSUS_ROLLUP_RANGES: $1" >&2
    exit 2
}
if [ -n "$rollup_levels" ]; then
    case "$rollup_ranges" in
        ,* | *, | *,,*) bad_ranges "an empty item in '$rollup_ranges'" ;;
    esac
    old_ifs=$IFS
    IFS=,
    set -f
    # shellcheck disable=SC2086
    set -- $rollup_ranges
    set +f
    IFS=$old_ifs
    for item in "$@"; do
        case "$item" in
            0* | *[!0-9]* | ??????????*)
                bad_ranges "$item is not a whole number of seconds from 1 to 999999999" ;;
        esac
    done
    n_ranges=$#
    n_levels=$(printf '%s\n' "$rollup_levels" | awk -F, '{ print NF }')
    [ $((n_ranges + 1)) -eq "$n_levels" ] ||
        bad_ranges "$n_ranges ranges for $n_levels levels: give one for each level after the first"
    prev=""
    for item in "$@"; do
        if [ -n "$prev" ] && [ "$item" -le "$prev" ]; then
            bad_ranges "$prev then $item: the ranges must increase"
        fi
        prev=$item
    done
    level_of=$(printf '%s\n' "$rollup_levels" | cut -d, -f2-)
    for item in "$@"; do
        level=${level_of%%,*}
        level_of=${level_of#*,}
        [ "$item" -ge "$level" ] || bad_ranges "$item is shorter than its level $level"
    done
    set --
fi

# Issue #18 follow-up: `PULSUS_ROLLUP_COUNTER_BUCKETS`, the buckets of a
# level a `rate` or `increase` range covers at least. The same rule and
# message as `pulsus_schema::parse_rollup_counter_buckets`.
case "$counter_buckets" in
    '' | 0* | *[!0-9]* | ?????*) bad_counter_buckets=1 ;;
    *) bad_counter_buckets=0 ;;
esac
if [ "$bad_counter_buckets" -eq 1 ] || [ "$counter_buckets" -lt 2 ] ||
    [ "$counter_buckets" -gt 1000 ]; then
    echo "schema.sh: PULSUS_ROLLUP_COUNTER_BUCKETS: $counter_buckets is not a whole number from 2 to 1000" >&2
    exit 2
fi

# Issue #743 part A: tiering. The same rules, in the same order and with the
# same messages, as `pulsus_schema::validate_tiering`.
bad_tiering() {
    echo "schema.sh: $1" >&2
    exit 2
}
if [ -n "$cold_volume" ]; then
    [ -n "$storage_policy" ] ||
        bad_tiering "PULSUS_COLD_VOLUME is set and PULSUS_STORAGE_POLICY is not"
    case "$cold_volume" in
        *[!A-Za-z0-9_]*) bad_tiering "cold_volume must be letters, digits and underscores: $cold_volume" ;;
    esac
fi
for pair in "PULSUS_METRICS_HOT_DAYS:$metrics_hot" "PULSUS_LOGS_HOT_DAYS:$logs_hot" \
            "PULSUS_TRACES_HOT_DAYS:$traces_hot"; do
    what=${pair%%:*}
    value=${pair#*:}
    if [ -n "$cold_volume" ] && [ -z "$value" ]; then
        bad_tiering "$what is not set; with PULSUS_COLD_VOLUME set each of PULSUS_METRICS_HOT_DAYS, PULSUS_LOGS_HOT_DAYS and PULSUS_TRACES_HOT_DAYS is required"
    fi
    if [ -z "$cold_volume" ] && [ -n "$value" ]; then
        bad_tiering "$what is set and PULSUS_COLD_VOLUME is not"
    fi
    [ -n "$value" ] || continue
    case "$value" in
        0* | *[!0-9]* | ???????????*)
            bad_tiering "$what must be a whole number of days from 1 to $retention_days: $value" ;;
    esac
    [ "$value" -le "$retention_days" ] ||
        bad_tiering "$what must be a whole number of days from 1 to $retention_days: $value"
done
# A family moves when a cold volume is set and its move age is under the
# retention; its `--@cold-<family>` lines are kept, else dropped.
cold_metrics='/^--@cold-metrics /d'
cold_logs='/^--@cold-logs /d'
cold_traces='/^--@cold-traces /d'
if [ -n "$cold_volume" ]; then
    [ "$metrics_hot" -ge "$retention_days" ] || cold_metrics='s/^--@cold-metrics *//'
    [ "$logs_hot" -ge "$retention_days" ] || cold_logs='s/^--@cold-logs *//'
    [ "$traces_hot" -ge "$retention_days" ] || cold_traces='s/^--@cold-traces *//'
fi

# Issue #743 part B: each rollup level's own retention and move age. The
# same rules, in the same order and with the same messages, as
# `pulsus_schema::validate_tiering`. `level_moves` holds one item per level,
# its move age when it moves and empty when it does not; `catalogue_days` is
# the metric catalogue's retention, the largest of the retention and every
# level's.
level_n=0
[ -z "$rollup_levels" ] || level_n=$(printf '%s\n' "$rollup_levels" | awk -F, '{ print NF }')
level_list() {
    case "$2" in
        '' | ,* | *, | *,,*) bad_tiering "$1: an empty item in '$2'" ;;
    esac
    list_n=$(printf '%s\n' "$2" | awk -F, '{ print NF }')
    [ "$list_n" -eq "$level_n" ] || bad_tiering "$1: $list_n items for $level_n levels"
}
not_days() {
    case "$1" in
        0* | *[!0-9]* | ???????????*) return 0 ;;
    esac
    return 1
}
if [ -n "$retention_given" ]; then
    level_list PULSUS_ROLLUP_RETENTION_DAYS "$rollup_retention"
    rest_l=$rollup_levels,
    rest_r=$rollup_retention,
    while [ -n "$rest_l" ]; do
        level=${rest_l%%,*}; rest_l=${rest_l#*,}
        item=${rest_r%%,*}; rest_r=${rest_r#*,}
        if not_days "$item" || [ "$item" -lt "$retention_days" ] || [ "$item" -gt 4294967295 ]; then
            bad_tiering "PULSUS_ROLLUP_RETENTION_DAYS: $item for the ${level}s level is not a whole number of days from $retention_days to 4294967295"
        fi
    done
    rest_l=$rollup_levels,
    rest_r=$rollup_retention,
    rest_g=""
    [ -z "$rollup_ranges" ] || rest_g=$rollup_ranges,
    while [ -n "$rest_g" ]; do
        level=${rest_l%%,*}; rest_l=${rest_l#*,}
        item=${rest_r%%,*}; rest_r=${rest_r#*,}
        from=${rest_g%%,*}; rest_g=${rest_g#*,}
        need=$(( (from + 86399) / 86400 + 1 ))
        [ "$item" -ge "$need" ] ||
            bad_tiering "PULSUS_ROLLUP_RETENTION_DAYS: $item for the ${level}s level is too short: the next level's switch point, $from s, requires $need retention days"
    done
fi
level_moves=""
if [ -n "$hot_given" ]; then
    [ -n "$cold_volume" ] ||
        bad_tiering "PULSUS_ROLLUP_HOT_DAYS is set and PULSUS_COLD_VOLUME is not"
    level_list PULSUS_ROLLUP_HOT_DAYS "$rollup_hot"
    rest_l=$rollup_levels,
    rest_h=$rollup_hot,
    rest_r=""
    [ -z "$rollup_retention" ] || rest_r=$rollup_retention,
    while [ -n "$rest_l" ]; do
        level=${rest_l%%,*}; rest_l=${rest_l#*,}
        item=${rest_h%%,*}; rest_h=${rest_h#*,}
        keep=$retention_days
        if [ -n "$rest_r" ]; then
            keep=${rest_r%%,*}; rest_r=${rest_r#*,}
        fi
        if not_days "$item" || [ "$item" -gt "$keep" ]; then
            bad_tiering "PULSUS_ROLLUP_HOT_DAYS: $item for the ${level}s level is not a whole number of days from 1 to $keep"
        fi
        [ "$item" -ge "$keep" ] || level_moves="$level_moves$item"
        level_moves="$level_moves,"
    done
fi
catalogue_days=$retention_days
rest_r=""
[ -z "$rollup_retention" ] || rest_r=$rollup_retention,
while [ -n "$rest_r" ]; do
    item=${rest_r%%,*}; rest_r=${rest_r#*,}
    [ "$item" -le "$catalogue_days" ] || catalogue_days=$item
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

# Issue #595 part 2: `PULSUS_TRACEQL_INDEXED_ATTRIBUTES`, the trace
# attributes `spans` carries a bloom-filter index for. The same rule the
# binary's configuration check applies (`pulsus_config::
# parse_indexed_attribute`): comma-separated `span.<key>` or `event.<key>`,
# the key dot-separated segments of letters, digits, `_` and `-`, at most
# 16, none twice. `{{trace_attr_indexes}}` renders one index per item, as
# `pulsus_schema`'s renderer does; `the_script_renders_exactly_what_this_
# crate_renders` holds the two together.
indexed=${PULSUS_TRACEQL_INDEXED_ATTRIBUTES:-}
attr_indexes=""
bad_attr() {
    echo "schema.sh: PULSUS_TRACEQL_INDEXED_ATTRIBUTES: $1" >&2
    exit 2
}
case "$indexed" in
    ,* | *, | *,,*) bad_attr "an empty item in '$indexed'" ;;
esac
bt='`'
n=0
seen=","
old_ifs=$IFS
IFS=,
for item in $indexed; do
    case "$item" in
        span.*) key=${item#span.}; root=span ;;
        event.*) key=${item#event.}; root=event ;;
        resource.*) bad_attr "$item: a resource attribute is read through resource_id, which the span table already indexes" ;;
        *) bad_attr "$item: the scope must be span. or event." ;;
    esac
    case "$key" in
        '' | .* | *. | *..* | *[!A-Za-z0-9_.-]*)
            bad_attr "$item: the key must be dot-separated segments of letters, digits, '_' and '-'" ;;
    esac
    [ "${#key}" -le 128 ] || bad_attr "$item: the key is longer than 128 bytes"
    case "$seen" in
        *",$item,"*) bad_attr "$item is listed twice" ;;
    esac
    seen="$seen$item,"
    n=$((n + 1))
    [ "$n" -le 16 ] || bad_attr "names more than 16 attributes"
    path=$(printf '%s' "$key" | sed 's/\./%2E/g')
    if [ "$root" = span ]; then
        expr="arrayMap(x -> ifNull(x, ''), arrayConcat(attrs.$bt$path$bt.:${bt}Array(Nullable(String))$bt, [attrs.$bt$path$bt.:String]))"
    else
        expr="arrayMap(x -> ifNull(x, ''), arrayConcat(events.attrs.$bt$path$bt.:String, arrayFlatten(events.attrs.$bt$path$bt.:${bt}Array(Nullable(String))$bt)))"
    fi
    attr_indexes="$attr_indexes,
    INDEX idx_attr_$n $expr TYPE bloom_filter(0.01) GRANULARITY 1"
done
IFS=$old_ifs

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
# The tables that never move (docs/configuration.md §3) are on the server's
# `default` policy once a cold volume is set, so nothing can move them.
policy_local_token=$policy_token
[ -z "$cold_volume" ] || policy_local_token=""

other=single
[ "$mode" = single ] && other=cluster

# The rollup levels (issue #18): the `--@level*` block of schema.sql, once per
# level, smallest first. `pulsus_schema::expand_levels` does the same.
expand_levels() {
    ROLLUP_LEVELS=$rollup_levels ROLLUP_RANGES=$rollup_ranges \
    COUNTER_BUCKETS=$counter_buckets LEVEL_RETENTION=$rollup_retention \
    LEVEL_MOVES=$level_moves awk '
    function unit(v) {
        if (v % 86400 == 0) return (v / 86400) " DAY"
        if (v % 3600 == 0) return (v / 3600) " HOUR"
        if (v % 60 == 0) return (v / 60) " MINUTE"
        return v " SECOND"
    }
    function rep(s, k, v,   at) {
        while ((at = index(s, k)) > 0) s = substr(s, 1, at - 1) v substr(s, at + length(k))
        return s
    }
    function flush(   i, j, p, d, off, timer, line) {
        if (nb == 0) return
        p = (lv[1] > 60) ? lv[1] : 60
        d = int((150 + p - 1) / p) * p
        for (i = 1; i <= n; i++) {
            if (i == 1) timer = "EVERY " unit(p)
            else {
                d += 60
                off = d % lv[i]
                timer = "EVERY " unit(lv[i]) (off ? " OFFSET " off " SECOND" : "")
            }
            for (j = 1; j <= nb; j++) {
                if (bk[j] == "first" && i != 1) continue
                if (bk[j] == "next" && i == 1) continue
                if (bk[j] == "feeds" && (i == n || lv[i + 1] >= 86400)) continue
                if (bk[j] == "cold" && mv[i] == "") continue
                line = rep(bl[j], "{{level}}", lv[i] "s")
                line = rep(line, "{{level_ms}}", lv[i] "000")
                line = rep(line, "{{source}}", i > 1 ? "metric_samples_" lv[i - 1] "s" : "")
                line = rep(line, "{{parent_ms}}", i < n ? lv[i + 1] "000" : "")
                line = rep(line, "{{period_ms}}", p "000")
                line = rep(line, "{{from_ms}}", i == 1 ? "0" : rg[i - 1] "000")
                line = rep(line, "{{counter_buckets}}", ENVIRON["COUNTER_BUCKETS"])
                line = rep(line, "{{timer}}", timer)
                line = rep(line, "{{level_retention_days}}", ENVIRON["LEVEL_RETENTION"] == "" ? "{{retention_days}}" : kr[i])
                line = rep(line, "{{level_hot_days}}", mv[i])
                print line
            }
        }
        nb = 0
    }
    BEGIN {
        n = ENVIRON["ROLLUP_LEVELS"] == "" ? 0 : split(ENVIRON["ROLLUP_LEVELS"], lv, ",")
        if (ENVIRON["ROLLUP_RANGES"] != "") split(ENVIRON["ROLLUP_RANGES"], rg, ",")
        if (ENVIRON["LEVEL_RETENTION"] != "") split(ENVIRON["LEVEL_RETENTION"], kr, ",")
        split(ENVIRON["LEVEL_MOVES"], mv, ",")
    }
    {
        kind = ""
        if (index($0, "--@level-first ") == 1) { kind = "first"; rest = substr($0, 16) }
        else if (index($0, "--@level-next ") == 1) { kind = "next"; rest = substr($0, 15) }
        else if (index($0, "--@level-feeds ") == 1) { kind = "feeds"; rest = substr($0, 16) }
        else if (index($0, "--@level-cold ") == 1) { kind = "cold"; rest = substr($0, 15) }
        else if (index($0, "--@level ") == 1) { kind = "all"; rest = substr($0, 10) }
        if (kind != "") { nb++; bk[nb] = kind; bl[nb] = rest; next }
        flush()
        print
    }
    END { flush() }'
}

render() {
    expand_levels < "$sql" | sed -e "/^--@$other /d" -e "s/^--@$mode *//" \
        -e "$cold_metrics" -e "$cold_logs" -e "$cold_traces" -e "/^--/d" |
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
            -e "s|{{graph_job_max_rows}}|$graph_job_max_rows|g" \
            -e "s|{{storage_policy}}|$policy_token|g" \
            -e "s|{{storage_policy_local}}|$policy_local_token|g" \
            -e "s|{{cold_volume}}|$cold_volume|g" \
            -e "s|{{metrics_hot_days}}|$metrics_hot|g" \
            -e "s|{{logs_hot_days}}|$logs_hot|g" \
            -e "s|{{traces_hot_days}}|$traces_hot|g" \
            -e "s|{{metrics_catalogue_retention_days}}|$catalogue_days|g" |
        TRACE_ATTR_INDEXES=$attr_indexes awk '{
            i = index($0, "{{trace_attr_indexes}}")
            if (i) $0 = substr($0, 1, i - 1) ENVIRON["TRACE_ATTR_INDEXES"] substr($0, i + 22)
            print
        }'
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

# A refused request prints the server's answer to stderr and returns curl's
# own exit code, so a caller under `set -e` still stops with it.
# shellcheck disable=SC2086
ask() {
    if ask_body=$(curl -sS --fail-with-body $auth "$url" --data-binary "$1"); then
        printf '%s\n' "$ask_body"
    else
        ask_rc=$?
        echo "schema.sh: the server refused a request; it said:" >&2
        printf '%s\n' "$ask_body" >&2
        return "$ask_rc"
    fi
}

# **Prints the statement and the server's answer when one fails.** The body
# `--fail-with-body` writes is the only thing that says WHY, and sending it
# to /dev/null left a Kubernetes Job whose whole log was
# `curl: (22) The requested URL returned error: 500`.
# shellcheck disable=SC2086
send_file() {
    if ! body=$(curl -sS --fail-with-body $auth "$url" --data-binary "@$1" 2>&1); then
        echo "schema.sh: the server refused this statement:" >&2
        cat "$1" >&2
        echo "schema.sh: the server said:" >&2
        printf '%s\n' "$body" >&2
        return 1
    fi
}

# **Wait for the server.** This runs as a Kubernetes Job and as a one-shot
# compose service, neither of which is ordered against ClickHouse's own
# readiness — the e2e harness starts services by order alone, and the first
# attempt met a port nothing was listening on yet (`curl` exit 7).
#
# Five minutes by default, not seconds: the chart's Job is installed
# alongside ClickHouse's own StatefulSet, which may be pulling an image or
# waiting on a volume, and the chart's behavioural suite scales ClickHouse to
# zero and back deliberately. A Job that gave up inside that window would
# leave a release that can never become ready. `PULSUS_SCHEMA_WAIT_SECONDS`
# shortens it where a caller wants a fast refusal.
wait_seconds=${PULSUS_SCHEMA_WAIT_SECONDS:-300}
ready=0
for _ in $(seq 1 "$wait_seconds"); do
    if curl -sS -o /dev/null "$url" --data-binary 'SELECT 1' 2>/dev/null; then
        ready=1
        break
    fi
    sleep 1
done
if [ "$ready" = 0 ]; then
    echo "schema.sh: clickhouse at $server:$port did not answer within ${wait_seconds}s" >&2
    exit 1
fi

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
if [ "$major" -lt 26 ] || { [ "$major" -eq 26 ] && [ "$minor" -lt 8 ]; }; then
    echo "schema.sh: clickhouse $version is below the minimum supported version 26.8" >&2
    exit 1
fi

# **And wait for the CLUSTER to be whole**, when there is one. Every
# statement below carries `ON CLUSTER`, and distributed DDL waits for each
# host to acknowledge: with a shard still starting it returns "There are N
# unfinished hosts" after `distributed_ddl_task_timeout` and the whole run
# fails. The chart installs this Job alongside every shard's StatefulSet, so
# that is the normal first-install race, not an edge case.
#
# `clusterAllReplicas` connects to every replica itself, and
# `skip_unavailable_shards = 1` makes it answer with the number that did
# rather than refusing — so the server's own view is the readiness test.
if [ "$mode" = cluster ]; then
    want_hosts=$(ask "SELECT count() FROM system.clusters \
                      WHERE cluster = '$cluster' FORMAT TSVRaw")
    case "$want_hosts" in
        '' | *[!0-9]*)
            echo "schema.sh: cluster '$cluster' is not defined on $server:$port" >&2
            exit 1
            ;;
        0)
            echo "schema.sh: cluster '$cluster' is not defined on $server:$port" >&2
            exit 1
            ;;
    esac
    whole=0
    for _ in $(seq 1 "$wait_seconds"); do
        live=$(ask "SELECT count() FROM clusterAllReplicas('$cluster', system.one) \
                    SETTINGS skip_unavailable_shards = 1 FORMAT TSVRaw" 2>/dev/null || echo 0)
        if [ "$live" = "$want_hosts" ]; then
            whole=1
            break
        fi
        sleep 1
    done
    if [ "$whole" = 0 ]; then
        echo "schema.sh: only $live of $want_hosts hosts in cluster '$cluster' answered \
within ${wait_seconds}s; ON CLUSTER DDL would fail on the rest" >&2
        exit 1
    fi
    echo "schema.sh: all $want_hosts hosts in cluster '$cluster' answered"
fi

# Issue #743: the policy, and its cold volume, exist on every host before
# anything is dropped; a server refuses the first CREATE otherwise, after the
# drop. The cold volume is not the first, and the policy moves nothing for
# disk pressure (`move_factor` 0), so only the TTL rules move parts.
if [ -n "$storage_policy" ]; then
    if [ -n "$cold_volume" ]; then
        policy_cond="volume_name = '$cold_volume' AND volume_priority > 1 AND move_factor = 0"
        policy_what="storage policy '$storage_policy' with a volume '$cold_volume' after its first and move_factor 0"
    else
        policy_cond="volume_priority = 1"
        policy_what="storage policy '$storage_policy'"
    fi
    if [ "$mode" = cluster ]; then
        policy_from="clusterAllReplicas('$cluster', system.storage_policies)"
        policy_want=$want_hosts
    else
        policy_from="system.storage_policies"
        policy_want=1
    fi
    policy_have=$(ask "SELECT count() FROM $policy_from \
                       WHERE policy_name = '$storage_policy' AND $policy_cond FORMAT TSVRaw")
    if [ "$policy_have" != "$policy_want" ]; then
        echo "schema.sh: $policy_what is defined on $policy_have of $policy_want hosts; every server's configuration must carry it (docs/configuration.md §3)" >&2
        exit 1
    fi
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
    send_file "$f"
    count=$((count + 1))
done

echo "schema.sh: $count statements applied to $db"
