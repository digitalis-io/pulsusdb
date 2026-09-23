#!/usr/bin/env bash
# What a span costs on the wire between replicas: inserts the corpus into a
# replicated span table on replica 1 and reads, from replica 2's part_log, the
# bytes it fetched. That is the cross-zone cost of replication factor 2.
# Usage: replication_bytes.sh CH1 CH2 SRC_HOST_PORT SRC_DB   (SRC holds tqd_g1.spans)
set -euo pipefail
CH1=${1%/} CH2=${2%/} SRC=$3 SRCDB=$4 DB=tqd_repl
q1() { curl --fail-with-body -sS "$CH1/" --data-binary "$1"; }
q2() { curl --fail-with-body -sS "$CH2/" --data-binary "$1"; }
q1 "DROP DATABASE IF EXISTS $DB ON CLUSTER tqd SYNC" >/dev/null
q1 "CREATE DATABASE $DB ON CLUSTER tqd" >/dev/null
COLS=$(q1 "SELECT replaceAll(substring(create_table_query, position(create_table_query, '(') + 1, position(create_table_query, ') ENGINE') - position(create_table_query, '(') - 1), '\`', '') FROM remote('$SRC', system, tables) WHERE database = '$SRCDB' AND name = 'spans' FORMAT TSVRaw")
q1 "CREATE TABLE $DB.spans ON CLUSTER tqd ($COLS) ENGINE = ReplicatedReplacingMergeTree('/tqd/repl_spans','{replica}') PARTITION BY toDate(fromUnixTimestamp64Nano(start_ns)) ORDER BY (intDiv(start_ns, 300000000000), trace_id, start_ns, span_id, kind) SETTINGS ttl_only_drop_parts = 1, index_granularity = 2048" >/dev/null
q1 "INSERT INTO $DB.spans SELECT * FROM remote('$SRC', $SRCDB, spans)" >/dev/null
q2 "SYSTEM SYNC REPLICA $DB.spans" >/dev/null
q1 "OPTIMIZE TABLE $DB.spans FINAL" >/dev/null; q2 "SYSTEM SYNC REPLICA $DB.spans" >/dev/null
q1 "SYSTEM FLUSH LOGS" >/dev/null; q2 "SYSTEM FLUSH LOGS" >/dev/null
rows=$(q1 "SELECT count() FROM $DB.spans FORMAT TSV")
echo "rows on each replica: $rows / $(q2 "SELECT count() FROM $DB.spans FORMAT TSV")"
echo "part bytes on each:   $(q1 "SELECT sum(bytes_on_disk) FROM system.parts WHERE database='$DB' AND active FORMAT TSV") / $(q2 "SELECT sum(bytes_on_disk) FROM system.parts WHERE database='$DB' AND active FORMAT TSV")"
echo "replica 2 fetched:    $(q2 "SELECT count(), sum(size_in_bytes) FROM system.part_log WHERE database='$DB' AND event_type = 'DownloadPart' FORMAT TSV")  (parts, bytes)"
echo "bytes per span fetched by replica 2: $(q2 "SELECT round(sum(size_in_bytes) / $rows, 3) FROM system.part_log WHERE database='$DB' AND event_type = 'DownloadPart' FORMAT TSV")"
q1 "DROP DATABASE $DB ON CLUSTER tqd SYNC" >/dev/null
