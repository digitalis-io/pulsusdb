#!/usr/bin/env bash
# The corpus and the two statements behind issue #559's byte figures,
# committed so anyone can re-take them.
#
# **Run-once by a human, and it REFUSES to run from CI** — the check is
# below, not just this sentence. It writes `SPANS` span rows and
# `SPANS * ATTRS_PER_SPAN` index rows into a database it creates, and
# every figure it prints is corpus- and setting-sensitive: `read_bytes`
# moves with `max_block_size`, with attributes per span, with the match
# fraction and with the server's own defaults. A wall-time or byte
# assertion in CI is not what this is for (see the standing rule on
# scale-invariant gates); the committed identities are
# `traces_metrics_explain.rs`'s granule gates.
#
# What it measures: the ONE statement a `{ span.k >= T } | rate()` sends,
# on each of the two lowerings.
#
#   today   the attribute condition as a semi-join against the attribute
#           index — the shape shipped before issue #559
#   after   the same condition as a locate-then-test predicate over the
#           span row's own arrays — what ships now
#
# It also prints the index SUBQUERY alone, because the difference between
# the whole `today` statement and its subquery is the outer span scan, and
# that difference is the one term the measurements separate cleanly.
#
# **Two things this script does NOT let you conclude.**
#
#   1. The two coefficients an earlier revision fitted to the `today`
#      side — "12.33 bytes per index row plus 32.67 per matching row" —
#      are a FIT to one corpus, not a decomposition into column groups.
#      The stored uncompressed widths do not reproduce them: the
#      predicate columns total 21.003964 bytes per index row and the two
#      identifier columns 24, against the fitted 12.33 and 32.67. A
#      filter-only control that selects no identifiers also moves with
#      selectivity, so the selectivity term is not identifier
#      materialisation either. Report the points; do not report the fit
#      as a cause.
#   2. Wall time. It is not printed, and it is not a design input here:
#      measured on one machine the direction flips with load — at load
#      average 0.89 the semi-join was faster and at 7.10 it was slower.
#      SERVER CPU is printed, because it does not depend on how many
#      threads the server happened to spread the work over:
#      `UserTimeMicroseconds + SystemTimeMicroseconds` summed over the
#      statement's threads. It is still one run on one machine and moves
#      with load; take it as a magnitude and a direction, not a constant.
#
# The `after` side DOES have a model that held on two independently built
# corpora: `24 + 11 * ATTRS_PER_SPAN` bytes per span row on top of the
# outer scan. Check it against the printed delta.
#
# Usage:
#   CH_URL=http://<host>:<port>/ DB=<database> BLOCK=65409 \
#     SPANS=300000 ATTRS_PER_SPAN=8 MATCH_FRACTION=0.5 \
#     bash docs/benchmarks/issue559-metrics-filter-bytes.sh
#
# Run it at more than one `BLOCK`, more than one `ATTRS_PER_SPAN` and at
# least three `MATCH_FRACTION` values: a single figure from any of the
# three says less than a series.
set -euo pipefail

# The CI refusal, before anything is created. `CI` is set by GitHub
# Actions and by every other runner this project has met; `GITHUB_ACTIONS`
# and `GITHUB_JOB` are that runner's own. Fail-closed: any of the three
# being set, to anything non-empty, stops the script.
#
# Set `PULSUS_BENCH_ALLOW_CI=1` to override, which exists so that a person
# who genuinely wants this inside a container that happens to export `CI`
# can say so deliberately.
for ci_var in CI GITHUB_ACTIONS GITHUB_JOB; do
  if [ -n "${!ci_var:-}" ] && [ -z "${PULSUS_BENCH_ALLOW_CI:-}" ]; then
    echo "refusing to run: $ci_var is set, and this script is run-once by a human." >&2
    echo "Every figure it prints is corpus- and setting-sensitive, so it is not a" >&2
    echo "gate. Set PULSUS_BENCH_ALLOW_CI=1 to override." >&2
    exit 2
  fi
done

: "${CH_URL:?set CH_URL to the ClickHouse HTTP endpoint, e.g. http://<host>:<port>/}"
: "${DB:?set DB to a database name this script may create and drop}"
: "${BLOCK:=65409}"
: "${SPANS:=300000}"
: "${ATTRS_PER_SPAN:=8}"
: "${MATCH_FRACTION:=0.5}"

ATTR_ROWS=$((SPANS * ATTRS_PER_SPAN))
START_NS=1700000000000000000
WINDOW_END=$((START_NS + SPANS * 1000000))

q() { curl -sS --fail-with-body "$CH_URL" --data-binary "$1"; }
qid_run() { curl -sS --fail-with-body "$CH_URL?query_id=$1" --data-binary "$2"; }

echo "-- instruments --"
q "SELECT version()"
curl --version | sed -n '1p'
echo "BLOCK=$BLOCK SPANS=$SPANS ATTRS_PER_SPAN=$ATTRS_PER_SPAN MATCH_FRACTION=$MATCH_FRACTION"

q "DROP DATABASE IF EXISTS $DB" >/dev/null
q "CREATE DATABASE $DB" >/dev/null

# The two tables, cut down to the columns these statements read. No
# projection: what the projection costs is a separate measurement and is
# recorded in the differential ledger, not here.
q "CREATE TABLE $DB.trace_spans (
     trace_id FixedString(16), span_id FixedString(8), parent_id FixedString(8),
     name LowCardinality(String), service LowCardinality(String),
     timestamp_ns Int64, duration_ns Int64, status_code Int8,
     kind Int8,
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

# One span per trace, `ATTRS_PER_SPAN` attributes each. Element 0 is the
# one the filter reads: `k`, numeric, at span scope. A span matches iff
# `number % 1000 < MATCH_FRACTION * 1000`, which is what makes the match
# fraction a parameter rather than a property of the data.
MATCH_BAND=$(q "SELECT toUInt32(round($MATCH_FRACTION * 1000)) FORMAT TSVRaw")
echo "match band: number % 1000 < $MATCH_BAND"

q "INSERT INTO $DB.trace_spans SELECT
     toFixedString(unhex(leftPad(lower(hex(number)), 32, '0')), 16),
     toFixedString(unhex(leftPad(lower(hex(number)), 16, '0')), 8),
     toFixedString(unhex('0000000000000000'), 8),
     'op', if(number % 50 = 0, 'checkout', concat('svc-', toString(number % 50))),
     $START_NS + toInt64(number) * 1000000, 1000000, 0, 1,
     arrayConcat(['k'], arrayMap(i -> concat('f', toString(i)),
                                 range(toUInt32($ATTRS_PER_SPAN - 1)))),
     arrayMap(i -> 'span', range(toUInt32($ATTRS_PER_SPAN))),
     arrayConcat([if(number % 1000 < $MATCH_BAND, '500', '200')],
                 arrayMap(i -> concat('v', toString(number % 97)),
                          range(toUInt32($ATTRS_PER_SPAN - 1)))),
     arrayMap(i -> if(i = 0, 'int', 'string'), range(toUInt32($ATTRS_PER_SPAN))),
     arrayConcat([toNullable(if(number % 1000 < $MATCH_BAND, 500., 200.))],
                 arrayMap(i -> CAST(NULL AS Nullable(Float64)),
                          range(toUInt32($ATTRS_PER_SPAN - 1))))
   FROM numbers($SPANS)" >/dev/null

# The same attributes as index rows, one row per (span, attribute).
q "INSERT INTO $DB.trace_attrs_idx SELECT
     toDate(fromUnixTimestamp64Nano($START_NS + toInt64(intDiv(number, $ATTRS_PER_SPAN)) * 1000000)),
     if(number % $ATTRS_PER_SPAN = 0, 'k',
        concat('f', toString(number % $ATTRS_PER_SPAN))),
     if(number % $ATTRS_PER_SPAN = 0,
        if(intDiv(number, $ATTRS_PER_SPAN) % 1000 < $MATCH_BAND, '500', '200'),
        concat('v', toString(intDiv(number, $ATTRS_PER_SPAN) % 97))),
     'span',
     if(number % $ATTRS_PER_SPAN = 0, 'int', 'string'),
     if(number % $ATTRS_PER_SPAN = 0,
        toNullable(if(intDiv(number, $ATTRS_PER_SPAN) % 1000 < $MATCH_BAND, 500., 200.)),
        CAST(NULL AS Nullable(Float64))),
     $START_NS + toInt64(intDiv(number, $ATTRS_PER_SPAN)) * 1000000,
     toFixedString(unhex(leftPad(lower(hex(intDiv(number, $ATTRS_PER_SPAN))), 32, '0')), 16),
     toFixedString(unhex(leftPad(lower(hex(intDiv(number, $ATTRS_PER_SPAN))), 16, '0')), 8),
     1000000
   FROM numbers($ATTR_ROWS)" >/dev/null

echo "-- corpus --"
q "SELECT * FROM (
     SELECT 'span rows' AS t, count() AS n FROM $DB.trace_spans
     UNION ALL SELECT 'index rows' AS t, count() AS n FROM $DB.trace_attrs_idx
     UNION ALL SELECT 'matching spans' AS t, count() AS n FROM $DB.trace_spans
       WHERE attr_val[1] = '500'
   ) ORDER BY t FORMAT TSV"

# Pinned here rather than taken from the server, so two runs on two
# servers compare.
SETTINGS="SETTINGS max_block_size=$BLOCK, use_query_condition_cache=0,
          optimize_move_to_prewhere=1, max_rows_to_read=50000000,
          max_bytes_to_read=17179869184, read_overflow_mode='throw',
          max_result_bytes=67108864, result_overflow_mode='throw'"

WINDOW="timestamp_ns >= $START_NS AND timestamp_ns < $WINDOW_END"
DATES="date >= toDate(fromUnixTimestamp64Nano($START_NS))
       AND date <= toDate(fromUnixTimestamp64Nano($WINDOW_END))"
BUCKET="toUnixTimestamp64Milli(toStartOfInterval(fromUnixTimestamp64Nano(timestamp_ns - 1),
        INTERVAL 60000000000 NANOSECOND)) + 60000"

run () {  # $1 = label, $2 = statement
  local qid="issue559-$1-$BLOCK-$ATTRS_PER_SPAN-$MATCH_BAND-$(date +%s%N)"
  qid_run "$qid" "$2 FORMAT Null
     $SETTINGS" >/dev/null
  echo "$qid"
}

TODAY=$(run today "
SELECT $BUCKET AS t, uniqExact(trace_id, span_id) AS n
FROM $DB.trace_spans
WHERE $WINDOW
  AND (trace_id, span_id) IN (SELECT trace_id, span_id FROM $DB.trace_attrs_idx
      WHERE $DATES AND $WINDOW AND key = 'k' AND val_num >= 500 AND scope = 'span')
GROUP BY t
ORDER BY t ASC")

SUB=$(run subquery "
SELECT trace_id, span_id FROM $DB.trace_attrs_idx
WHERE $DATES AND $WINDOW AND key = 'k' AND val_num >= 500 AND scope = 'span'")

AFTER=$(run after "
WITH arrayFirstIndex((k, s) -> k = 'k' AND s = 'span', attr_key, attr_scope) AS pi0
SELECT $BUCKET AS t, uniqExact(trace_id, span_id) AS n
FROM $DB.trace_spans
WHERE $WINDOW
  AND ((pi0 != 0) AND ifNull(attr_num[pi0] >= 500, 0))
GROUP BY t
ORDER BY t ASC")

MATCHALL=$(run match-all "
SELECT $BUCKET AS t, uniqExact(trace_id, span_id) AS n
FROM $DB.trace_spans
WHERE $WINDOW
GROUP BY t
ORDER BY t ASC")

q "SYSTEM FLUSH LOGS" >/dev/null
sleep 1

echo "-- read_rows / read_bytes / server CPU microseconds --"
q "SELECT multiIf(query_id = '$TODAY', '1 today (whole statement)',
                  query_id = '$SUB',   '2 today (index subquery alone)',
                  query_id = '$AFTER', '3 after (whole statement)',
                  '4 match-all control') AS statement,
          read_rows, read_bytes,
          ProfileEvents['UserTimeMicroseconds']
            + ProfileEvents['SystemTimeMicroseconds'] AS cpu_us
   FROM system.query_log
   WHERE query_id IN ('$TODAY','$SUB','$AFTER','$MATCHALL') AND type = 'QueryFinish'
   ORDER BY statement FORMAT TSV"

echo "-- the two separations the measurements support --"
echo "   today whole - today subquery  = the outer span scan"
echo "   after whole - match-all       = the attribute arrays, model 24 + 11*ATTRS_PER_SPAN"
q "SELECT
     (SELECT read_bytes FROM system.query_log
       WHERE query_id = '$TODAY' AND type = 'QueryFinish')
     - (SELECT read_bytes FROM system.query_log
         WHERE query_id = '$SUB' AND type = 'QueryFinish') AS outer_span_scan_bytes,
     (SELECT read_bytes FROM system.query_log
       WHERE query_id = '$AFTER' AND type = 'QueryFinish')
     - (SELECT read_bytes FROM system.query_log
         WHERE query_id = '$MATCHALL' AND type = 'QueryFinish') AS attribute_array_bytes,
     round(attribute_array_bytes / $SPANS, 3) AS per_span,
     24 + 11 * $ATTRS_PER_SPAN AS model_per_span
   FORMAT TSV"

echo "-- the stored widths, which the fitted coefficients do NOT reproduce --"
echo "   (system.parts_columns names the COLUMN in \`column\`; \`name\` is the part)"
q "SELECT column,
          round(sum(column_data_uncompressed_bytes) / sum(rows), 6) AS bytes_per_index_row
   FROM system.parts_columns
   WHERE database = '$DB' AND table = 'trace_attrs_idx' AND active
     AND column IN ('date','key','scope','val_num','timestamp_ns','trace_id','span_id')
   GROUP BY column ORDER BY column FORMAT TSV"

q "DROP DATABASE IF EXISTS $DB" >/dev/null
echo "-- dropped $DB --"
