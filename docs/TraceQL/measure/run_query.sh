#!/usr/bin/env bash
# Runs one SQL file against ClickHouse REPS times and prints one TSV line:
#   name mode reps read_rows read_bytes result_rows returned_bytes peak_memory ms_min ms_median ms_max
# returned_bytes is the size of the RowBinary response body, which is what
# PulsusDB's client receives. mode=warm runs once unmeasured first; mode=cold
# drops the mark, uncompressed, query-condition and primary-index caches before
# every run and reads with direct I/O, so neither a ClickHouse cache nor the page
# cache serves it. Every run carries final=1, so a retried span is never counted
# twice. Environment: CH (HTTP URL), NAME (defaults to the file name).
# Usage: run_query.sh FILE.sql warm|cold REPS
set -euo pipefail
f=$1 mode=$2 reps=$3; name=${NAME:-$(basename "$f" .sql)}
CH=${CH%/}
q() { curl --fail-with-body -sS "$CH/$1" --data-binary @-; }
extra="&final=1"; [ "$mode" = cold ] && extra="$extra&min_bytes_to_use_direct_io=1"
[ "$mode" = warm ] && q "?default_format=RowBinary$extra" < "$f" > /dev/null
ids=(); sizes=()
for i in $(seq 1 "$reps"); do
  if [ "$mode" = cold ]; then
    for c in "MARK CACHE" "UNCOMPRESSED CACHE" "QUERY CONDITION CACHE" "PRIMARY INDEX CACHE"; do echo "SYSTEM DROP $c" | q "" >/dev/null 2>&1 || true; done
  fi
  id="tqd-$name-$mode-$i-$RANDOM"; ids+=("'$id'")
  sizes+=("$(q "?query_id=$id&default_format=RowBinary$extra" < "$f" | wc -c)")
done
echo "SYSTEM FLUSH LOGS" | q "" >/dev/null
stats=$(echo "SELECT any(read_rows), any(read_bytes), any(result_rows), max(memory_usage), min(query_duration_ms), quantileExact(0.5)(query_duration_ms), max(query_duration_ms) FROM system.query_log WHERE query_id IN ($(IFS=,; echo "${ids[*]}")) AND type='QueryFinish' FORMAT TSV" | q "")
r=$(echo "$stats" | cut -f1-3); rest=$(echo "$stats" | cut -f4-)
printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$name" "$mode" "$reps" "$r" "${sizes[0]}" "$rest"
