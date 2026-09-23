#!/usr/bin/env bash
# The descendant climb's bound, and what a query sees when it is hit. Builds
# three traces in a scratch database - an ordinary one, a chain deeper than the
# bound, and a cycle - and runs the climb over each.
# Usage: recursion_bound.sh CH_URL [MAX_DEPTH]
set -euo pipefail
CH=${1%/} MAX=${2:-64} DB=tqd_reclimit
q() { curl --fail-with-body -sS "$CH/" --data-binary "$1"; }
q "DROP DATABASE IF EXISTS $DB SYNC" >/dev/null
q "CREATE DATABASE $DB" >/dev/null
q "CREATE TABLE $DB.spans (trace_id FixedString(16), span_id FixedString(8), parent_span_id FixedString(8),
       start_ns Int64, duration_ns Int64, service LowCardinality(String))
   ENGINE = ReplacingMergeTree ORDER BY (trace_id, span_id)" >/dev/null
# t1: a 10-span chain, frontend at the root and payment at the leaf
q "INSERT INTO $DB.spans SELECT toFixedString('t1', 16), reinterpretAsFixedString(toUInt64(number + 1)),
          if(number = 0, toFixedString('', 8), reinterpretAsFixedString(toUInt64(number))),
          1000 + number, 1, if(number = 0, 'frontend', if(number = 9, 'payment', 'mid'))
   FROM numbers(10)" >/dev/null
# t2: a chain one longer than the bound
q "INSERT INTO $DB.spans SELECT toFixedString('t2', 16), reinterpretAsFixedString(toUInt64(number + 1)),
          if(number = 0, toFixedString('', 8), reinterpretAsFixedString(toUInt64(number))),
          1000 + number, 1, if(number = 0, 'frontend', if(number = $MAX + 1, 'payment', 'mid'))
   FROM numbers($MAX + 2)" >/dev/null
# t3: two spans that are each other's parent, plus a payment leaf under them
q "INSERT INTO $DB.spans VALUES
   (toFixedString('t3',16), reinterpretAsFixedString(toUInt64(1)), reinterpretAsFixedString(toUInt64(2)), 1000, 1, 'mid'),
   (toFixedString('t3',16), reinterpretAsFixedString(toUInt64(2)), reinterpretAsFixedString(toUInt64(1)), 1001, 1, 'mid'),
   (toFixedString('t3',16), reinterpretAsFixedString(toUInt64(3)), reinterpretAsFixedString(toUInt64(1)), 1002, 1, 'payment')" >/dev/null
climb() { # trace -> matched, unresolved
  q "WITH RECURSIVE climb AS (
         SELECT trace_id, span_id AS b, parent_span_id AS cur, toUInt8(0) AS found, 0 AS depth
         FROM $DB.spans WHERE trace_id = toFixedString('$1', 16) AND service = 'payment'
         UNION ALL
         SELECT c.trace_id, c.b, x.parent_span_id, toUInt8(x.service = 'frontend'), c.depth + 1
         FROM climb AS c
         INNER JOIN $DB.spans AS x ON x.trace_id = c.trace_id AND x.span_id = c.cur
         WHERE c.found = 0 AND c.depth < $MAX)
     SELECT (SELECT count() FROM (SELECT DISTINCT trace_id, b FROM climb WHERE found = 1)) AS matched,
            (SELECT count() FROM climb WHERE found = 0 AND depth >= $MAX AND cur != toFixedString('', 8)) AS unresolved
     FORMAT TSV"
}
echo "trace              matched unresolved   expected"
echo "ordinary chain     $(climb t1)          1 matched, 0 unresolved"
echo "deeper than bound  $(climb t2)          0 matched, >0 unresolved -> the reader answers 422"
echo "cycle              $(climb t3)          0 matched, >0 unresolved -> the reader answers 422"
q "DROP DATABASE $DB SYNC" >/dev/null
