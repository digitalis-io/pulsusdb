#!/usr/bin/env bash
# The corpus and the three statements behind issue #558's row/byte
# figures, committed so anyone can re-take them.
#
# **Run-once by a human, never from CI.** It writes 240,000 rows into a
# database it creates, and every figure it prints is corpus- and
# setting-sensitive: `read_bytes` moves with `max_block_size`, with the
# stored string lengths and with the server's own defaults. A wall-time or
# byte assertion in CI is not what this is for (see the standing rule on
# scale-invariant gates); the committed identities are
# `traces_search_explain.rs`'s granule gate and
# `traces_search_live.rs`'s statement-count test.
#
# What it measures: the three statements a `select()` of one attribute
# sends, before and after issue #558.
#
#   after   the batch hydration statement carrying one select SLOT
#   plain   the same statement with the slot removed — the control that
#           says what the slot itself costs
#   before  the value read issue #558 deleted, against `trace_attrs_idx`
#
# "Before" was TWO statements — `plain` plus the value read — so its
# totals are their sum. `after` is one.
#
# Usage:
#   CH_URL=http://<host>:<port>/ DB=<database> BLOCK=65409 \
#     bash docs/benchmarks/issue558-value-read-bytes.sh
#
# `BLOCK` is `max_block_size`; run it at more than one value, because
# `read_bytes` moves with it and a single figure says less than a pair.
set -euo pipefail

: "${CH_URL:?set CH_URL to the ClickHouse HTTP endpoint, e.g. http://<host>:<port>/}"
: "${DB:?set DB to a database name this script may create and drop}"
: "${BLOCK:=65409}"

SPANS=60000            # one span per trace, three attributes each
ATTRS=$((SPANS * 3))   # the index rows those attributes become
WINDOW_END=$((1700000000000000000 + SPANS * 1000000))

q() { curl -sS --fail-with-body "$CH_URL" --data-binary "$1"; }
# The same, under a chosen `query_id`, so `system.query_log` can be read
# back for exactly this statement.
qid_run() { curl -sS --fail-with-body "$CH_URL?query_id=$1" --data-binary "$2"; }

echo "-- instruments --"
q "SELECT version()"
curl --version | sed -n '1p'

q "DROP DATABASE IF EXISTS $DB" >/dev/null
q "CREATE DATABASE $DB" >/dev/null

# The two tables, cut down to the columns these three statements read.
q "CREATE TABLE $DB.trace_spans (
     trace_id FixedString(16), span_id FixedString(8), parent_id FixedString(8),
     name LowCardinality(String), service LowCardinality(String),
     timestamp_ns Int64, duration_ns Int64, status_code Int8,
     status_message String DEFAULT '', kind Int8,
     scope_name LowCardinality(String) DEFAULT '',
     scope_version LowCardinality(String) DEFAULT '',
     attr_key Array(LowCardinality(String)), attr_scope Array(LowCardinality(String)),
     attr_val Array(String), attr_type Array(LowCardinality(String)),
     attr_num Array(Nullable(Float64))
   ) ENGINE = MergeTree
   PARTITION BY toDate(fromUnixTimestamp64Nano(timestamp_ns))
   ORDER BY (trace_id, timestamp_ns)" >/dev/null

q "CREATE TABLE $DB.trace_attrs_idx (
     date Date, key LowCardinality(String), val String, scope LowCardinality(String),
     val_type LowCardinality(String) DEFAULT '', val_num Nullable(Float64),
     timestamp_ns Int64, trace_id FixedString(16), span_id FixedString(8), duration_ns Int64
   ) ENGINE = ReplacingMergeTree
   PARTITION BY date
   ORDER BY (key, val, scope, timestamp_ns, trace_id, span_id)" >/dev/null

# One span per trace, three attributes: `env` and `service.name` at
# resource scope and `foo` at span scope. `foo` is the one the statements
# below project, and it takes 97 distinct values so it is neither a
# constant nor unique.
q "INSERT INTO $DB.trace_spans SELECT
     toFixedString(unhex(leftPad(lower(hex(number)), 32, '0')), 16),
     toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8),
     toFixedString(unhex('0000000000000000'), 8),
     'op', if(number % 8 = 0, 'checkout', concat('svc-', toString(number % 8))),
     1700000000000000000 + toInt64(number) * 1000000, 1000000, 0, '', 1, '', '',
     ['env','service.name','foo'], ['resource','resource','span'],
     ['prod', if(number % 8 = 0, 'checkout', concat('svc-', toString(number % 8))),
      concat('F', toString(number % 97))],
     ['string','string','string'], [NULL, NULL, NULL]
   FROM numbers($SPANS)" >/dev/null

# The same three attributes as index rows, one row per (span, attribute).
q "INSERT INTO $DB.trace_attrs_idx SELECT
     toDate(fromUnixTimestamp64Nano(1700000000000000000 + toInt64(intDiv(number,3)) * 1000000)),
     arrayElement(['env','service.name','foo'], toUInt32(number % 3) + 1),
     multiIf(number % 3 = 0, 'prod',
             number % 3 = 1, if(intDiv(number,3) % 8 = 0, 'checkout',
                                concat('svc-', toString(intDiv(number,3) % 8))),
             concat('F', toString(intDiv(number,3) % 97))),
     arrayElement(['resource','resource','span'], toUInt32(number % 3) + 1),
     'string', NULL,
     1700000000000000000 + toInt64(intDiv(number,3)) * 1000000,
     toFixedString(unhex(leftPad(lower(hex(intDiv(number,3))), 32, '0')), 16),
     toFixedString(unhex(leftPad(lower(hex(intDiv(number,3))), 16, '0')), 8), 1000000
   FROM numbers($ATTRS)" >/dev/null

echo "-- corpus --"
q "SELECT * FROM (
     SELECT 'span rows' AS t, count() AS n FROM $DB.trace_spans
     UNION ALL SELECT 'index rows' AS t, count() AS n FROM $DB.trace_attrs_idx
   ) ORDER BY t FORMAT TSV"

# The batch: the 32 most recent trace ids, which is `exec::BATCH_TRACES`.
IDS=$(q "SELECT arrayStringConcat(arrayMap(x -> concat('unhex(', char(39), x, char(39), ')'),
                                           groupArray(lower(hex(trace_id)))), ', ')
         FROM (SELECT trace_id FROM $DB.trace_spans ORDER BY timestamp_ns DESC LIMIT 32)
         FORMAT TSVRaw")
WINDOW="timestamp_ns > 1700000000000000000 AND timestamp_ns <= $WINDOW_END"

# The settings are pinned here rather than taken from the server, so two
# runs on two servers compare. They are the production search settings
# plus the block size under test.
SETTINGS="SETTINGS max_block_size=$BLOCK, use_query_condition_cache=0,
          optimize_move_to_prewhere=1, max_rows_to_read=50000000,
          max_bytes_to_read=17179869184, read_overflow_mode='throw',
          max_result_bytes=67108864, result_overflow_mode='throw'"

HYD_COLS="trace_id, span_id, parent_id, service, name, timestamp_ns, duration_ns,
          status_code, status_message, kind, scope_name, scope_version"

run () {  # $1 = label, $2 = statement
  local qid="issue558-$1-$BLOCK-$(date +%s%N)"
  qid_run "$qid" "$2 FORMAT Null
     $SETTINGS" >/dev/null
  echo "$qid"
}

A=$(run after "
WITH arrayFirstIndex((k, s) -> k = 'foo' AND s = 'span', attr_key, attr_scope) AS fs0
SELECT $HYD_COLS,
       [fs0 != 0] AS attr_slot,
       [if(length(attr_val[fs0]) <= 8192, attr_val[fs0],
           substringUTF8(attr_val[fs0], 1, 2048))] AS attr_slot_val,
       [attr_type[fs0]] AS attr_slot_type,
       CAST([NULL] AS Array(Nullable(Float64))) AS attr_slot_num
FROM $DB.trace_spans
WHERE trace_id IN ($IDS) AND $WINDOW
ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC
LIMIT 10001 BY trace_id")

B=$(run plain "
SELECT $HYD_COLS
FROM $DB.trace_spans
WHERE trace_id IN ($IDS) AND $WINDOW
ORDER BY trace_id ASC, timestamp_ns ASC, span_id ASC
LIMIT 10001 BY trace_id")

C=$(run value-read "
SELECT trace_id, span_id,
       any(if(length(val) <= 8192, val, substringUTF8(val, 1, 2048))) AS v,
       any(val_type) AS t
FROM $DB.trace_attrs_idx
WHERE date >= toDate('2023-11-14') AND date <= toDate('2023-11-16')
  AND key = 'foo' AND scope = 'span'
  AND $WINDOW
  AND trace_id IN ($IDS)
GROUP BY trace_id, span_id")

q "SYSTEM FLUSH LOGS" >/dev/null
sleep 1

echo "-- read_rows / read_bytes at max_block_size=$BLOCK --"
q "SELECT multiIf(query_id = '$A', 'after (one statement)',
                  query_id = '$B', 'plain control',
                  'before: the deleted value read') AS statement,
          read_rows, read_bytes
   FROM system.query_log
   WHERE query_id IN ('$A','$B','$C') AND type = 'QueryFinish'
   ORDER BY statement FORMAT TSV"

echo "-- before TOTAL = plain + value read --"
q "SELECT sum(read_rows) AS read_rows, sum(read_bytes) AS read_bytes
   FROM system.query_log
   WHERE query_id IN ('$B','$C') AND type = 'QueryFinish' FORMAT TSV"

q "DROP DATABASE IF EXISTS $DB" >/dev/null
echo "-- dropped $DB --"
