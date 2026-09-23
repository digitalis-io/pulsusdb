#!/usr/bin/env bash
# A Zipkin shared span carries both halves of one call under ONE span id, so the
# service graph needs a second join branch. Builds one ordinary pair and one
# shared pair and runs both branches over them.
#   ordinary: client c1 (svc-a, CLIENT) -> server s1 (svc-b, SERVER, parent = c1)
#   shared:   client c2 (svc-a, CLIENT) and server (svc-c, SERVER, SAME id, zipkin.shared)
# Expected: two rpc edges, svc-a -> svc-b and svc-a -> svc-c, one call each.
# Usage: shared_span_edges.sh CH_URL
set -euo pipefail
CH=${1%/} DB=tqd_shared
q() { curl --fail-with-body -sS "$CH/?json_type_escape_dots_in_keys=1" --data-binary "$1"; }
q "DROP DATABASE IF EXISTS $DB SYNC" >/dev/null
q "CREATE DATABASE $DB" >/dev/null
q "CREATE TABLE $DB.spans (trace_id FixedString(16), span_id FixedString(8), parent_span_id FixedString(8),
       start_ns Int64, duration_ns Int64, service LowCardinality(String), kind UInt8,
       status_code UInt8, attrs JSON)
   ENGINE = ReplacingMergeTree ORDER BY (trace_id, span_id, kind)" >/dev/null
q "INSERT INTO $DB.spans FORMAT JSONEachRow
{\"trace_id\":\"0123456789abcdef\",\"span_id\":\"c1______\",\"parent_span_id\":\"\",\"start_ns\":1000,\"duration_ns\":10,\"service\":\"svc-a\",\"kind\":3,\"status_code\":0,\"attrs\":{}}
{\"trace_id\":\"0123456789abcdef\",\"span_id\":\"s1______\",\"parent_span_id\":\"c1______\",\"start_ns\":1001,\"duration_ns\":9,\"service\":\"svc-b\",\"kind\":2,\"status_code\":0,\"attrs\":{}}
{\"trace_id\":\"0123456789abcdef\",\"span_id\":\"c2______\",\"parent_span_id\":\"\",\"start_ns\":2000,\"duration_ns\":20,\"service\":\"svc-a\",\"kind\":3,\"status_code\":0,\"attrs\":{}}
{\"trace_id\":\"0123456789abcdef\",\"span_id\":\"c2______\",\"parent_span_id\":\"\",\"start_ns\":2000,\"duration_ns\":18,\"service\":\"svc-c\",\"kind\":2,\"status_code\":0,\"attrs\":{\"zipkin.shared\":true}}" >/dev/null
q "SELECT client, server, connection_type, count() AS calls
   FROM (
     SELECT c.service AS client, s.service AS server, if(c.kind = 3, 'rpc', 'messaging') AS connection_type
     FROM (SELECT trace_id, span_id, service, kind FROM $DB.spans WHERE kind IN (3, 4)) AS c
     INNER JOIN (SELECT trace_id, parent_span_id, service, kind FROM $DB.spans
                 WHERE kind IN (2, 5) AND NOT coalesce(attrs.\`zipkin%2Eshared\`.:Bool, false)) AS s
         ON s.trace_id = c.trace_id AND s.parent_span_id = c.span_id
     WHERE (c.kind = 3 AND s.kind = 2) OR (c.kind = 4 AND s.kind = 5)
     UNION ALL
     SELECT c.service, s.service, if(c.kind = 3, 'rpc', 'messaging')
     FROM (SELECT trace_id, span_id, service, kind FROM $DB.spans WHERE kind IN (3, 4)) AS c
     INNER JOIN (SELECT trace_id, span_id, service, kind FROM $DB.spans
                 WHERE kind IN (2, 5) AND coalesce(attrs.\`zipkin%2Eshared\`.:Bool, false)) AS s
         ON s.trace_id = c.trace_id AND s.span_id = c.span_id
     WHERE (c.kind = 3 AND s.kind = 2) OR (c.kind = 4 AND s.kind = 5))
   GROUP BY client, server, connection_type
   ORDER BY server FORMAT TSV" | awk -F'\t' 'BEGIN{print "client\tserver\ttype\tcalls"} {print}'
q "DROP DATABASE $DB SYNC" >/dev/null
