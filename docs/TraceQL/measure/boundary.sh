#!/usr/bin/env bash
# The window rule: start inclusive, end exclusive, with the bucket bound and the
# day bound rendered from the SAME last-included nanosecond. Proves both edges
# against one span, and proves the day bound cannot drop a row the row bound
# admits. Usage: boundary.sh CH_URL DB
set -euo pipefail
CH=${1%/} DB=$2 B=300000000000
q() { curl --fail-with-body -sS "$CH/" --data-binary "$1"; }
T=$(q "SELECT max(start_ns) FROM $DB.spans FORMAT TSV")
win() { # start end -> rows matching that span
  local s=$1 e=$2
  q "SELECT count() FROM $DB.spans WHERE start_ns >= $s AND start_ns < $e AND intDiv(start_ns, $B) BETWEEN $((s / B)) AND $(( (e - 1) / B )) AND start_ns = $T FORMAT TSV"
}
echo "span at $T"
echo "  window [T, T+1)      -> $(win $T $((T+1)))   (must be 1: start is inclusive)"
echo "  window [T-1, T)      -> $(win $((T-1)) $T)   (must be 0: end is exclusive)"
echo "  window [T-3600s, T)  -> $(win $((T-3600000000000)) $T)   (must be 0)"
echo "  window [T, T+3600s)  -> $(win $T $((T+3600000000000)))   (must be 1)"
# the day bound: the last nanosecond of a UTC day, with the window ending at the
# next midnight, must still be read - the row bound and the prune agree
DAY_END=$(q "SELECT toInt64(toUnixTimestamp(toStartOfDay(fromUnixTimestamp64Nano($T))) + 86400) * 1000000000 FORMAT TSV")
echo "  window [T, midnight) -> $(win $T $DAY_END)   (must be 1: the last day partition is read)"

# The day bound as the generated statements render it, on the day-partitioned
# tables. Rendered from the window's END a window that stops at midnight reads
# two days; rendered from the last nanosecond it includes, E - 1, it reads one.
days() { q "SELECT dateDiff('day', toDate(fromUnixTimestamp64Nano($1)), toDate(fromUnixTimestamp64Nano($2))) + 1 FORMAT TSV"; }
echo "day partitions a [T, midnight) window reads"
echo "  rendered from end       -> $(days $T $DAY_END)   (2: the next day is read for nothing)"
echo "  rendered from end - 1   -> $(days $T $((DAY_END - 1)))   (1)"
# and the row at the last nanosecond of the day is still found under the narrow
# day bound, which is the direction that would lose rows
LAST=$((DAY_END - 1))
echo "  a trace whose spans end at that last nanosecond is still selected -> $(q "SELECT count() FROM $DB.traces WHERE day >= toDate(fromUnixTimestamp64Nano($T)) AND day <= toDate(fromUnixTimestamp64Nano($LAST)) AND end_ns >= $T FORMAT TSV" ) rows (must be > 0)"
