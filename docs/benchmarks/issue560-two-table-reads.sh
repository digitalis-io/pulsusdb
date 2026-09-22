#!/usr/bin/env bash
# The corpora and the statements behind issue #560's figures: the two
# derived trace tables `trace_recent` and `trace_error_spans`, what they
# cost to store, and what the empty search `{}` and `{ status = error }`
# read from them against the span-table scan they replace.
#
# **Run-once by a human, and it REFUSES to run from CI** — the check is
# below. It builds corpus C1 (2,000,000 spans, twice) and corpus C2
# (101,000 traces) in a database it creates, and every figure it prints is
# corpus- and setting-sensitive. The committed identities are the live
# suites' relations (`traces_search_pushdown_live.rs`), never these
# figures.
#
# Usage:
#   DB=<database this script may create and drop> \
#     bash docs/benchmarks/issue560-two-table-reads.sh <clickhouse-http-endpoint>
#
# `DB` defaults to `issue560_bench`; the script refuses to touch a database
# of that name that already exists, and drops it when it finishes.
#
# stdout is exactly one `key<TAB>value` line per figure, in a fixed order;
# the instruments and progress go to stderr.
#
# **What each line is.**
#
#   c1_identity             docs/traceql-schema-migration.md §4's identity
#                           digest over the two C1 span tables (layout and
#                           content), so a reader can tell the corpus was
#                           rebuilt exactly. C1 is built by §4's corpus
#                           statements, copied here less the attribute
#                           index `attrs_old`, which no figure below reads.
#   recent_*                the shipped `trace_recent` (ts_max, ts_min,
#                           both `CODEC(T64, ZSTD(1))`), built from C1 and
#                           `OPTIMIZE … FINAL`: rows, bytes on disk, bytes
#                           per row
#   record_shape_bytes_on_disk      the design record's first shape: ts_max
#                           only, no codec
#   ts_max_only_same_codec_bytes    ts_max only, `CODEC(T64, ZSTD(1))`
#   error_*                 the shipped `trace_error_spans`
#   codec_*                 bytes per row of `trace_recent` with the
#                           (ts_max, ts_min) pair under six codecs
#   bucket_60_rows          rows at a 60 s bucket instead of 300 s
#   rows_per_trace          recent_rows / distinct traces, 1 + d/B measured
#   tail_<τ>_*              the window (1700000000, E] with E τ seconds
#                           before the end of its bucket. extra_without_ts_min
#                           = traces a ts_max-only read admits that the span
#                           scan does not; lost_if_upper_bounded = traces a
#                           `ts_max <= E` bound loses; tail_100_lost and
#                           tail_100_extra = the shipped read against the
#                           span scan
#   c2_<N>_*                corpus C2: 1,000 matching traces in a one-second
#                           window, and N traces wholly after it in the same
#                           bucket. Of the first 100,000 candidates the
#                           generator ranks (the default
#                           `reader.traceql_max_candidates`), how many are
#                           matching traces, for the ts_max-only shape and
#                           the shipped one
#   fragment_*              `trace_recent` written as eight inserts that split
#                           every trace's spans by sipHash64(span_id) % 8, read
#                           before any merge: rows unmerged and merged, and
#                           the τ = 100 s candidate sets against the span scan
#   old_* / recent_* / new_*   `read_rows read_bytes SelectedMarks result_rows`
#                           from system.query_log for the span scan and the
#                           derived-table generator, `{}` and
#                           `{ status = error }`, over three hours and over
#                           300 s. Each statement runs three times under its
#                           own query_id; the script stops if the three
#                           disagree. Marks are one part layout's reading,
#                           taken after this script's own `OPTIMIZE … FINAL`.
set -euo pipefail

for ci_var in CI GITHUB_ACTIONS GITHUB_JOB; do
  if [ -n "${!ci_var:-}" ] && [ -z "${PULSUS_BENCH_ALLOW_CI:-}" ]; then
    echo "refusing to run: $ci_var is set, and this script is run-once by a human." >&2
    echo "Set PULSUS_BENCH_ALLOW_CI=1 to override." >&2
    exit 2
  fi
done

CH="${1:?usage: DB=<database> $0 <clickhouse-http-endpoint>}"
CH="${CH%/}"
DB="${DB:-issue560_bench}"
case "$DB" in
  *[!A-Za-z0-9_]*) echo "refusing: DB '$DB' is not a bare identifier" >&2; exit 2 ;;
esac

# Pinned here rather than taken from the server, so two runs on two
# servers compare.
PIN="max_insert_threads=1&max_threads=1&max_block_size=65409&max_execution_time=3600"
q()  { curl -sS --fail-with-body "$CH/?database=$DB&$PIN" --data-binary "$1"; }
qd() { curl -sS --fail-with-body "$CH/" --data-binary "$1"; }
out() { printf '%s\t%s\n' "$1" "$2"; }

if [ "$(qd "SELECT count() FROM system.databases WHERE name = '$DB'")" != "0" ]; then
  echo "refusing to touch existing database '$DB'" >&2
  exit 2
fi
cleanup() { curl -sS "$CH/" --data-binary "DROP DATABASE IF EXISTS $DB" >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "-- instruments --" >&2
qd "SELECT version()" >&2
curl --version | sed -n '1p' >&2

qd "CREATE DATABASE $DB" >/dev/null

# ---------------------------------------------------------------------
# Corpus C1 — docs/traceql-schema-migration.md §4's construction
# ---------------------------------------------------------------------
echo "-- building C1 --" >&2
for T in spans_old spans_new; do
  EXTRA=""
  [ "$T" = spans_new ] && EXTRA=",
    attr_key Array(LowCardinality(String)), attr_scope Array(LowCardinality(String)),
    attr_val Array(String), attr_type Array(LowCardinality(String)), attr_num Array(Nullable(Float64))"
  q "CREATE TABLE $DB.$T (
  trace_id FixedString(16), span_id FixedString(8), parent_id FixedString(8),
  name LowCardinality(String), service LowCardinality(String),
  timestamp_ns Int64 CODEC(DoubleDelta, ZSTD(1)), duration_ns Int64 CODEC(T64, ZSTD(1)),
  status_code Int8, kind Int8, payload_type Int8, shared UInt8, status_message String,
  scope_name LowCardinality(String), scope_version LowCardinality(String),
  payload String CODEC(ZSTD(3))$EXTRA
) ENGINE = MergeTree
PARTITION BY toDate(fromUnixTimestamp64Nano(timestamp_ns))
ORDER BY (trace_id, timestamp_ns) SETTINGS ttl_only_drop_parts = 1" >/dev/null
done
for i in 0 1 2 3 4 5 6 7; do LO=$((i*250000))
 for T in spans_new spans_old; do
  COLS=""
  [ "$T" = spans_new ] && COLS=",
  ['service.name','deployment.environment','k8s.cluster','http.method','http.status_code','http.target','user.id','request.id'],
  ['resource','resource','resource','span','span','span','span','span'],
  [concat('svc-', toString(intDiv(n,3) % 20)), 'prod', 'eu-west-1',
   ['GET','POST','PUT','DELETE'][(n % 4) + 1], ['200','400','500','503'][(n % 4) + 1],
   concat('/api/v1/r', toString(n % 50)), concat('u-', toString(intDiv(n,12))), concat('r-', toString(n))],
  ['string','string','string','string','int','string','string','string'],
  [NULL,NULL,NULL,NULL, toFloat64([200,400,500,503][(n % 4) + 1]), NULL,NULL,NULL]"
  q "INSERT INTO $DB.$T SELECT
  reinterpretAsFixedString(sipHash128(intDiv(n,12))), reinterpretAsFixedString(sipHash64(n)),
  reinterpretAsFixedString(sipHash64(intDiv(n,12)*12)),
  concat('GET /op/', toString(n % 50)), concat('svc-', toString(intDiv(n,3) % 20)),
  toInt64(1700000000000000000 + intDiv(n,12)*64800000 + (n%12)*100000000),
  toInt64(100000 + (n % 997) * 3000000),
  if(n % 100 = 0, toInt8(2), toInt8(0)), toInt8(n % 5), toInt8(0), toUInt8(0), '', 'scope', '1.0',
  repeat(substring(concat(lower(hex(sipHash128(n))), lower(hex(sipHash128(n+1))),
                          lower(hex(sipHash128(n+2))), lower(hex(sipHash128(n+3)))), 1, 100), 4)$COLS
FROM (SELECT number AS n FROM numbers($LO, 250000))" >/dev/null
 done
done
for T in spans_old spans_new; do q "OPTIMIZE TABLE $DB.$T FINAL" >/dev/null; done

out c1_identity "$(q "WITH
  (SELECT arrayStringConcat(groupArray(concat(table,'|',name,'|',toString(rows),'|',toString(marks))), ';')
     FROM (SELECT table, name, rows, marks FROM system.parts
           WHERE database='$DB' AND active AND table IN ('spans_old','spans_new')
           ORDER BY table, name)) AS layout,
  (SELECT arrayStringConcat(arraySort(groupArray(concat(k,'=',v))), ';') FROM (
     SELECT 'a_rows' AS k, toString(count()) AS v FROM spans_new
     UNION ALL SELECT 'b_traces',   toString(uniqExact(trace_id))        FROM spans_new
     UNION ALL SELECT 'c_names',    toString(uniqExact(name))            FROM spans_new
     UNION ALL SELECT 'd_services', toString(uniqExact(service))         FROM spans_new
     UNION ALL SELECT 'e_errors',   toString(countIf(status_code = 2))   FROM spans_new
     UNION ALL SELECT 'f_tsmin',    toString(min(timestamp_ns))          FROM spans_new
     UNION ALL SELECT 'g_tsmax',    toString(max(timestamp_ns))          FROM spans_new
     UNION ALL SELECT 'h_cells',    toString(sum(length(attr_key)))      FROM spans_new
     UNION ALL SELECT 'i_sumdur',   toString(sum(duration_ns))           FROM spans_new
     UNION ALL SELECT 'j_ckold',    toString(sum(sipHash64(trace_id, timestamp_ns, name, service, status_code, duration_ns))) FROM spans_old
     UNION ALL SELECT 'k_cknew',    toString(sum(sipHash64(trace_id, timestamp_ns, name, service, status_code, duration_ns))) FROM spans_new
  )) AS content
SELECT lower(hex(SHA256(concat(layout, '#', content)))) FORMAT TSVRaw")"

# ---------------------------------------------------------------------
# The derived tables, and what they cost to store
# ---------------------------------------------------------------------
echo "-- building the derived tables --" >&2
# $1 table, $2 the two time-column declarations, $3 the SELECT's time
# expressions, $4 bucket width in ns.
recent_table() {
  q "CREATE TABLE $DB.$1 (
       date Date, bucket UInt32, trace_id FixedString(16), $2
     ) ENGINE = AggregatingMergeTree PARTITION BY date ORDER BY (bucket, trace_id)
     SETTINGS ttl_only_drop_parts = 1" >/dev/null
  q "INSERT INTO $DB.$1 SELECT toDate(fromUnixTimestamp64Nano(timestamp_ns)) AS date,
       toUInt32(intDiv(timestamp_ns, $4)) AS bucket, trace_id, $3
     FROM $DB.spans_new GROUP BY date, bucket, trace_id" >/dev/null
  q "OPTIMIZE TABLE $DB.$1 FINAL" >/dev/null
}
bytes_of() { q "SELECT sum(bytes_on_disk) FROM system.parts WHERE database = '$DB' AND table = '$1' AND active FORMAT TSVRaw"; }
rows_of()  { q "SELECT sum(rows) FROM system.parts WHERE database = '$DB' AND table = '$1' AND active FORMAT TSVRaw"; }
per_row()  { q "SELECT toDecimalString($(bytes_of "$1") / $(rows_of "$1"), 3) FORMAT TSVRaw"; }

PAIR_MAX="max(timestamp_ns), min(timestamp_ns)"
recent_table recent \
  "ts_max SimpleAggregateFunction(max, Int64) CODEC(T64, ZSTD(1)), ts_min SimpleAggregateFunction(min, Int64) CODEC(T64, ZSTD(1))" \
  "$PAIR_MAX" 300000000000
recent_table record_shape "ts_max SimpleAggregateFunction(max, Int64)" "max(timestamp_ns)" 300000000000
recent_table ts_max_codec "ts_max SimpleAggregateFunction(max, Int64) CODEC(T64, ZSTD(1))" "max(timestamp_ns)" 300000000000

q "CREATE TABLE $DB.errors (
     date Date, trace_id FixedString(16), span_id FixedString(8),
     timestamp_ns Int64 CODEC(DoubleDelta, ZSTD(1)), duration_ns Int64 CODEC(T64, ZSTD(1)),
     service LowCardinality(String), name LowCardinality(String), kind Int8
   ) ENGINE = ReplacingMergeTree PARTITION BY date ORDER BY (timestamp_ns, trace_id, span_id)
   SETTINGS ttl_only_drop_parts = 1" >/dev/null
q "INSERT INTO $DB.errors (date, trace_id, span_id, timestamp_ns, duration_ns, service, name, kind)
   SELECT toDate(fromUnixTimestamp64Nano(timestamp_ns)), trace_id, span_id, timestamp_ns,
          duration_ns, service, name, kind
   FROM $DB.spans_new WHERE status_code = 2" >/dev/null
q "OPTIMIZE TABLE $DB.errors FINAL" >/dev/null

out recent_rows "$(rows_of recent)"
out recent_bytes_on_disk "$(bytes_of recent)"
out recent_bytes_per_row "$(per_row recent)"
out record_shape_bytes_on_disk "$(bytes_of record_shape)"
out ts_max_only_same_codec_bytes "$(bytes_of ts_max_codec)"
out error_rows "$(rows_of errors)"
out error_bytes_on_disk "$(bytes_of errors)"
out error_bytes_per_row "$(per_row errors)"

codec_line() {  # $1 key, $2 codec clause (may be empty)
  recent_table "codec_$1" \
    "ts_max SimpleAggregateFunction(max, Int64) $2, ts_min SimpleAggregateFunction(min, Int64) $2" \
    "$PAIR_MAX" 300000000000
  out "codec_$1" "$(per_row "codec_$1")"
}
codec_line T64_ZSTD3 "CODEC(T64, ZSTD(3))"
codec_line T64_ZSTD1 "CODEC(T64, ZSTD(1))"
codec_line Delta_ZSTD1 "CODEC(Delta, ZSTD(1))"
codec_line ZSTD1 "CODEC(ZSTD(1))"
codec_line DoubleDelta_ZSTD1 "CODEC(DoubleDelta, ZSTD(1))"
codec_line none ""

recent_table bucket_60 "ts_max SimpleAggregateFunction(max, Int64)" "max(timestamp_ns)" 60000000000
out bucket_60_rows "$(rows_of bucket_60)"
out rows_per_trace "$(q "SELECT toDecimalString($(rows_of recent) / (SELECT uniqExact(trace_id) FROM $DB.spans_new), 6) FORMAT TSVRaw")"

# ---------------------------------------------------------------------
# The window's tail: what ts_min buys
# ---------------------------------------------------------------------
B=300000000000
S=1700000000000000000
day_of() { date -u -d "@$(( $1 / 1000000000 ))" +%F; }
# The date and bucket prune the reader renders for (lo, hi], both floors.
prune() {
  echo "date >= toDate('$(day_of "$1")') AND date <= toDate('$(day_of "$2")')
    AND bucket >= $(( $1 / B )) AND bucket <= $(( $2 / B ))"
}
count_except() { q "SELECT count() FROM (SELECT * FROM ($1) EXCEPT SELECT * FROM ($2)) FORMAT TSVRaw"; }

# $1 tau in seconds; the window ends tau seconds before the end of the
# bucket that 1700005400 s falls in (bucket 5666684).
tail_sets() {
  local E=$(( (1700005400000000000 / B + 1) * B - $1 * 1000000000 ))
  T_SPAN="SELECT trace_id FROM $DB.spans_new WHERE timestamp_ns > $S AND timestamp_ns <= $E GROUP BY trace_id"
  local P; P=$(prune $S $E)
  T_SHIP="SELECT trace_id FROM $DB.recent WHERE $P AND ts_max > $S AND ts_min <= $E GROUP BY trace_id"
  T_MAXONLY="SELECT trace_id FROM $DB.recent WHERE $P AND ts_max > $S GROUP BY trace_id"
  T_UPPER="SELECT trace_id FROM $DB.recent WHERE $P AND ts_max > $S AND ts_max <= $E GROUP BY trace_id"
}
for tau in 200 100 50; do
  tail_sets $tau
  out "tail_${tau}_extra_without_ts_min" "$(count_except "$T_MAXONLY" "$T_SPAN")"
done
for tau in 200 100 50; do
  tail_sets $tau
  out "tail_${tau}_lost_if_upper_bounded" "$(count_except "$T_SPAN" "$T_UPPER")"
done
tail_sets 100
out tail_100_lost "$(count_except "$T_SPAN" "$T_SHIP")"
out tail_100_extra "$(count_except "$T_SHIP" "$T_SPAN")"

# ---------------------------------------------------------------------
# Corpus C2: the empty answer at the candidate ceiling
# ---------------------------------------------------------------------
# One span per trace. 1,000 matching traces inside the one-second window
# (E - 1 s, E]; N traces wholly after E, inside E's bucket, so a ts_max-only
# read ranks every one of them above every matching trace.
echo "-- C2 --" >&2
C2_E=$(( 5666684 * B + 100000000000 ))
C2_S=$(( C2_E - 1000000000 ))
C2_P=$(prune $C2_S $C2_E)
for N in 99999 100000; do
  q "DROP TABLE IF EXISTS $DB.c2_spans" >/dev/null
  q "DROP TABLE IF EXISTS $DB.c2_recent" >/dev/null
  q "CREATE TABLE $DB.c2_spans (trace_id FixedString(16), timestamp_ns Int64)
     ENGINE = MergeTree ORDER BY (trace_id, timestamp_ns)" >/dev/null
  q "INSERT INTO $DB.c2_spans SELECT reinterpretAsFixedString(sipHash128('match', number)),
       $C2_S + 1 + number * 999000 FROM numbers(1000)" >/dev/null
  q "INSERT INTO $DB.c2_spans SELECT reinterpretAsFixedString(sipHash128('tail', number)),
       $C2_E + 1000000 + number * 1000000 FROM numbers($N)" >/dev/null
  q "CREATE TABLE $DB.c2_recent (date Date, bucket UInt32, trace_id FixedString(16),
       ts_max SimpleAggregateFunction(max, Int64), ts_min SimpleAggregateFunction(min, Int64))
     ENGINE = AggregatingMergeTree PARTITION BY date ORDER BY (bucket, trace_id)" >/dev/null
  q "INSERT INTO $DB.c2_recent SELECT toDate(fromUnixTimestamp64Nano(timestamp_ns)) AS date,
       toUInt32(intDiv(timestamp_ns, $B)) AS bucket, trace_id, max(timestamp_ns), min(timestamp_ns)
     FROM $DB.c2_spans GROUP BY date, bucket, trace_id" >/dev/null
  TRUE_SET="SELECT trace_id FROM $DB.c2_spans WHERE timestamp_ns > $C2_S AND timestamp_ns <= $C2_E"
  for shape in record_shape shipped; do
    COND="ts_max > $C2_S"
    [ "$shape" = shipped ] && COND="$COND AND ts_min <= $C2_E"
    declare "C2_${N}_${shape}=$(q "SELECT count() FROM (
         SELECT trace_id, toInt64(max(ts_max)) AS bound_ts FROM $DB.c2_recent
         WHERE $C2_P AND $COND GROUP BY trace_id
         ORDER BY bound_ts DESC, trace_id ASC LIMIT 100000)
       WHERE trace_id IN ($TRUE_SET) FORMAT TSVRaw")"
  done
done
out c2_99999_record_shape_true "$C2_99999_record_shape"
out c2_100000_record_shape_true "$C2_100000_record_shape"
out c2_99999_shipped_true "$C2_99999_shipped"
out c2_100000_shipped_true "$C2_100000_shipped"

# ---------------------------------------------------------------------
# A trace split across inserts, read before any merge
# ---------------------------------------------------------------------
echo "-- the fragmented table --" >&2
q "CREATE TABLE $DB.fragment (
     date Date, bucket UInt32, trace_id FixedString(16),
     ts_max SimpleAggregateFunction(max, Int64) CODEC(T64, ZSTD(1)),
     ts_min SimpleAggregateFunction(min, Int64) CODEC(T64, ZSTD(1))
   ) ENGINE = AggregatingMergeTree PARTITION BY date ORDER BY (bucket, trace_id)
   SETTINGS ttl_only_drop_parts = 1" >/dev/null
q "SYSTEM STOP MERGES $DB.fragment" >/dev/null
for i in 0 1 2 3 4 5 6 7; do
  q "INSERT INTO $DB.fragment SELECT toDate(fromUnixTimestamp64Nano(timestamp_ns)) AS date,
       toUInt32(intDiv(timestamp_ns, $B)) AS bucket, trace_id, max(timestamp_ns), min(timestamp_ns)
     FROM $DB.spans_new WHERE sipHash64(span_id) % 8 = $i GROUP BY date, bucket, trace_id" >/dev/null
done
out fragment_partial_rows "$(q "SELECT count() FROM $DB.fragment FORMAT TSVRaw")"
tail_sets 100
F_E=$(( (1700005400000000000 / B + 1) * B - 100000000000 ))
F_SET="SELECT trace_id FROM $DB.fragment WHERE $(prune $S $F_E) AND ts_max > $S AND ts_min <= $F_E GROUP BY trace_id"
FRAG_SPAN=$(q "SELECT count() FROM ($T_SPAN) FORMAT TSVRaw")
FRAG_RECENT=$(q "SELECT count() FROM ($F_SET) FORMAT TSVRaw")
FRAG_LOST=$(count_except "$T_SPAN" "$F_SET")
FRAG_EXTRA=$(count_except "$F_SET" "$T_SPAN")
q "SYSTEM START MERGES $DB.fragment" >/dev/null
q "OPTIMIZE TABLE $DB.fragment FINAL" >/dev/null
out fragment_merged_rows "$(q "SELECT count() FROM $DB.fragment FORMAT TSVRaw")"
out fragment_span_candidates "$FRAG_SPAN"
out fragment_recent_candidates "$FRAG_RECENT"
out fragment_lost "$FRAG_LOST"
out fragment_extra "$FRAG_EXTRA"

# ---------------------------------------------------------------------
# What the two generators read
# ---------------------------------------------------------------------
echo "-- the reads --" >&2
READ_SETTINGS="use_query_condition_cache=0&optimize_move_to_prewhere=1&max_block_size=65409"
# $1 label, $2 statement: three runs, each under its own query_id, and the
# three query_log rows must agree.
read_line() {
  local ids=() qid
  for rep in 1 2 3; do
    qid="issue560-$1-$rep-$(date +%s%N)"
    curl -sS --fail-with-body "$CH/?database=$DB&query_id=$qid&$READ_SETTINGS" \
      --data-binary "$2 FORMAT Null" >/dev/null
    ids+=("'$qid'")
  done
  local list; list=$(IFS=,; echo "${ids[*]}")
  local rows=""
  for _ in $(seq 1 40); do
    qd "SYSTEM FLUSH LOGS" >/dev/null
    rows=$(qd "SELECT read_rows, read_bytes, ProfileEvents['SelectedMarks'], result_rows
               FROM system.query_log WHERE query_id IN ($list) AND type = 'QueryFinish'
               ORDER BY query_id FORMAT TSVRaw")
    [ "$(printf '%s\n' "$rows" | grep -c .)" = 3 ] && break
    sleep 0.25
  done
  if [ "$(printf '%s\n' "$rows" | grep -c .)" != 3 ] || [ "$(printf '%s\n' "$rows" | sort -u | wc -l)" != 1 ]; then
    echo "the three runs of $1 disagree or are missing:" >&2
    printf '%s\n' "$rows" >&2
    exit 1
  fi
  out "$1" "$(printf '%s\n' "$rows" | head -1 | tr '\t' ' ')"
}
TAIL="GROUP BY trace_id ORDER BY bound_ts DESC, trace_id ASC LIMIT 100001"
old_empty() { echo "SELECT trace_id, max(timestamp_ns) AS bound_ts FROM $DB.spans_old
  WHERE timestamp_ns > $1 AND timestamp_ns <= $2 $TAIL"; }
old_error() { echo "SELECT trace_id, max(timestamp_ns) AS bound_ts FROM $DB.spans_old
  WHERE timestamp_ns > $1 AND timestamp_ns <= $2 AND (status_code = 2) $TAIL"; }
new_empty() { echo "SELECT trace_id, toInt64(max(ts_max)) AS bound_ts FROM $DB.recent
  WHERE $(prune "$1" "$2") AND ts_max > $1 AND ts_min <= $2 $TAIL"; }
new_error() { echo "SELECT trace_id, max(timestamp_ns) AS bound_ts FROM $DB.errors
  WHERE date >= toDate('$(day_of "$1")') AND date <= toDate('$(day_of "$2")')
    AND timestamp_ns > $1 AND timestamp_ns <= $2 $TAIL"; }
W3H_E=1700010800000000000
W300_E=1700000300000000000
read_line old_empty_3h "$(old_empty $S $W3H_E)"
read_line recent_empty_3h "$(new_empty $S $W3H_E)"
read_line old_empty_300s "$(old_empty $S $W300_E)"
read_line recent_empty_300s "$(new_empty $S $W300_E)"
read_line old_error_3h "$(old_error $S $W3H_E)"
read_line new_error_3h "$(new_error $S $W3H_E)"
read_line old_error_300s "$(old_error $S $W300_E)"
read_line new_error_300s "$(new_error $S $W300_E)"

echo "-- dropping $DB --" >&2
