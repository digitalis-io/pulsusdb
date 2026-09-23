#!/usr/bin/env bash
# Measures how many copies of a part an S3-compatible bucket holds when two
# ClickHouse replicas offload the same data, for each mechanism the
# open-source server offers. Environment:
#   CH1, CH2          HTTP URLs of two replicas of one shard sharing a keeper
#   S3URL             bucket URL as the servers see it, e.g. http://store:9000/bucket
#   S3KEY, S3SECRET   credentials
# Server configuration: docs/TraceQL/measure/offload-server.xml (disks s3, s3zc,
# plainrw; policies s3only, s3zc, tiered, tieredzc, plainrw; cluster tqd).
set -uo pipefail
q() { curl --fail-with-body -sS "$1" --data-binary "$2"; }
objs() { q "$CH1" "SELECT count(), sum(_size) FROM s3('$S3URL/$1/**', '$S3KEY', '$S3SECRET', 'One') FORMAT TSV" 2>/dev/null | tr '\t' ' ' || echo "0 0"; }
DB=tqd_offload
q "$CH1" "DROP DATABASE IF EXISTS $DB ON CLUSTER tqd SYNC" >/dev/null
q "$CH1" "CREATE DATABASE $DB ON CLUSTER tqd" >/dev/null
cols="ts DateTime64(9), trace_id FixedString(16), name LowCardinality(String), dur UInt64, attr String"
# 2,000,000 rows over two days; one partition per day.
gen="SELECT toDateTime64('2026-09-01 00:00:00',9) + toIntervalMillisecond(number*86), reinterpretAsFixedString(cityHash64(number) * 1000003 + number), 'op'||toString(number % 50), number % 100000, repeat(hex(cityHash64(number)), 4) FROM numbers(2000000)"
settle() { # wait until neither replica holds inactive parts
  for i in $(seq 1 60); do
    n=$( (q "$CH1" "SELECT count() FROM system.parts WHERE database='$DB' AND table='$1' AND NOT active FORMAT TSV"; q "$CH2" "SELECT count() FROM system.parts WHERE database='$DB' AND table='$1' AND NOT active FORMAT TSV") | paste -sd+ | bc)
    [ "$n" = 0 ] && break; sleep 2; done; sleep 3; }
parts() { q "$1" "SELECT disk_name, count(), sum(bytes_on_disk) FROM system.parts WHERE database='$DB' AND table='$2' AND active GROUP BY disk_name ORDER BY disk_name FORMAT TSV" | tr '\t\n' ' ;'; }
mk() { # table policy extra
  q "$CH1" "CREATE TABLE $DB.$1 ON CLUSTER tqd ($cols) ENGINE=ReplicatedMergeTree('/tqd/$1','{replica}') PARTITION BY toDate(ts) ORDER BY (name, ts) ${4:-} SETTINGS storage_policy='$2', old_parts_lifetime=1 $3"; }
load() { q "$CH1" "INSERT INTO $DB.$1 $gen"; q "$CH2" "SYSTEM SYNC REPLICA $DB.$1"; q "$CH1" "OPTIMIZE TABLE $DB.$1 ON CLUSTER tqd FINAL"; q "$CH2" "SYSTEM SYNC REPLICA $DB.$1"; settle $1; }
report() { echo "  ch1 parts: $(parts $CH1 $1)"; echo "  ch2 parts: $(parts $CH2 $1)"; echo "  bucket prefix '$2' objects bytes: $(objs $2)"; }

echo "== A. replicated table on an S3 disk, zero-copy off"
mk a_s3 s3only ""; load a_s3; report a_s3 s3
echo "== B. replicated table on an S3 disk, zero-copy on"
mk b_zc s3zc ", allow_remote_fs_zero_copy_replication=1"; load b_zc; report b_zc zc
echo "== C. replicated, local hot volume, TTL move to S3 cold, zero-copy off"
q "$CH1" "DROP DATABASE $DB ON CLUSTER tqd SYNC"; q "$CH1" "CREATE DATABASE $DB ON CLUSTER tqd"; sleep 5
echo "  bucket before: s3=$(objs s3) zc=$(objs zc)"
mk c_tier tiered "" "TTL toDateTime(ts) + INTERVAL 1 DAY TO VOLUME 'cold'"; load c_tier
q "$CH1" "ALTER TABLE $DB.c_tier ON CLUSTER tqd MATERIALIZE TTL" >/dev/null; sleep 10; settle c_tier; report c_tier s3
echo "== D. replicated, local hot volume, TTL move to S3 cold, zero-copy on"
mk d_tierzc tieredzc ", allow_remote_fs_zero_copy_replication=1" "TTL toDateTime(ts) + INTERVAL 1 DAY TO VOLUME 'cold'"; load d_tierzc
q "$CH1" "ALTER TABLE $DB.d_tierzc ON CLUSTER tqd MATERIALIZE TTL" >/dev/null; sleep 10; settle d_tierzc; report d_tierzc zc
echo "== E. retention: DROP PARTITION of one day on each table"
q "$CH1" "ALTER TABLE $DB.c_tier ON CLUSTER tqd DROP PARTITION '2026-09-01'"; q "$CH1" "ALTER TABLE $DB.d_tierzc ON CLUSTER tqd DROP PARTITION '2026-09-01'"; sleep 8
echo "  after drop: s3=$(objs s3) zc=$(objs zc)"
echo "== F. the shared-storage engine"
q "$CH1" "CREATE TABLE $DB.f_shared ($cols) ENGINE=SharedMergeTree ORDER BY ts" 2>&1 | head -c 300; echo
echo "== G. replicated table on a plain_rewritable disk"
q "$CH1" "CREATE TABLE $DB.g_prw ON CLUSTER tqd ($cols) ENGINE=ReplicatedMergeTree('/tqd/g_prw','{replica}') ORDER BY ts SETTINGS storage_policy='plainrw'" 2>&1 | head -c 400; echo
echo "== server warnings mentioning zero-copy"
q "$CH1" "SELECT message FROM system.warnings WHERE message ILIKE '%zero%copy%' FORMAT TSV"
