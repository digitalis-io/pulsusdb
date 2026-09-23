#!/usr/bin/env bash
# The awkward-topology checks: every statement whose correctness depends on a
# shape the ordinary corpus does not contain - same-time siblings, an orphan, a
# cycle, a chain at the depth bound and one past it, and a resource shared by a
# selection span and baseline spans.
#
#   edge_checks.sh CH_URL [DB]
#
# The fixture directory must be under the server's user_files_path; pass it as
# WORKDIR. Writes results/edge-checks.tsv: one row per check, with the expected
# value beside the value the statement returned and PASS or FAIL.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
CH=${1%/} DB=${2:-tqd_edge} WORK=${3:-/corpus}
BASE=${EDGE_BASE_S:-1790200000}
S_NS=${BASE}000000000; E_NS=$((BASE + 60))000000000
R=$HERE/results; mkdir -p "$R"
SQL=$(mktemp -d); trap 'rm -rf "$SQL"' EXIT
q() { curl --fail-with-body -sS "$CH/?final=1&json_type_escape_dots_in_keys=1" --data-binary "$1"; }

case "$DB" in
  *edge*) ;;
  *) echo "refusing to rebuild database '$DB': this script drops and recreates it," \
          "so its name must end in 'edge'" >&2; exit 2 ;;
esac
q "DROP DATABASE IF EXISTS $DB" >/dev/null

python3 "$HERE/fixture/make_edge_fixture.py" "$WORK/edge" "$BASE" > "$SQL/fixture.json"
"$HERE/load_staging.sh" "$CH" "$DB" edge >/dev/null
python3 - "$CH" "$HERE/schema.sql" "$DB" <<'PY'
import re, sys, urllib.request
ch, path, db = sys.argv[1].rstrip('/'), sys.argv[2], sys.argv[3]
sql = re.sub(r'--[^\n]*', '', open(path).read()).replace('tqd_g1', db)
for st in [s.strip() for s in sql.split(';') if s.strip()]:
    if st.startswith('CREATE FUNCTION'):
        continue
    urllib.request.urlopen(urllib.request.Request(
        ch + '/?json_type_escape_dots_in_keys=1', data=st.encode()), timeout=600).read()
PY
# the ids come from the generator, never retyped
tid() { python3 -c "import json,sys;print([t for t in json.load(open(sys.argv[1]))['traces'] if t.startswith(sys.argv[2])][0])" "$SQL/fixture.json" "$1"; }
T1=$(tid ee01); T2=$(tid ee02)
python3 "$HERE/make_sql.py" "$DB" "$S_NS" "$E_NS" "$SQL" "$T1" "$T2" >/dev/null

pass=0; fail=0
check() {   # check NAME EXPECTED ACTUAL
  local v=PASS; [ "$2" = "$3" ] || { v=FAIL; fail=$((fail + 1)); }
  [ "$v" = PASS ] && pass=$((pass + 1))
  printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$v" >> "$R/edge-checks.tsv"
}
# the spans one statement returns, as `<trace prefix>:<span ids>` in trace order
spans_of() {
  q "DROP TABLE IF EXISTS $DB.edge_out" >/dev/null
  q "CREATE TABLE $DB.edge_out ENGINE = Memory AS $(cat "$SQL/$1.sql")" >/dev/null
  q "SELECT arrayStringConcat(groupArray(r), ' ') FROM (
       SELECT concat(substring(lower(hex(trace_id)), 1, 4), ':',
                     arrayStringConcat(arraySort(arrayMap(x -> substring(hex(x.1), 15), spans)), ',')) AS r
       FROM $DB.edge_out ${2:-} ORDER BY r) FORMAT TSV"
}
unresolved_of() {
  q "SELECT toString(max(unresolved)) FROM $DB.edge_out FORMAT TSV"
}

printf 'check\texpected\tgot\tverdict\n' > "$R/edge-checks.tsv"

# --- the nine non-transitive structural forms on ee06 -----------------------
#   P -> A2 -> B1 -> A1     and     P -> P2 -> {A3, B2}
#   span ids: 01 P, 02 A2, 03 B1, 04 A1, 05 P2, 06 A3, 07 B2
check st01_child_plain    'ee06:03' "$(spans_of st01_child_plain   "WHERE substring(lower(hex(trace_id)), 1, 4) = 'ee06'")"
check st03_child_union    'ee06:02,03' "$(spans_of st03_child_union  "WHERE substring(lower(hex(trace_id)), 1, 4) = 'ee06'")"
check st04_parent_plain   'ee06:03' "$(spans_of st04_parent_plain  "WHERE substring(lower(hex(trace_id)), 1, 4) = 'ee06'")"
check st06_parent_union   'ee06:03,04' "$(spans_of st06_parent_union "WHERE substring(lower(hex(trace_id)), 1, 4) = 'ee06'")"
check st07_sibling_plain  'ee06:07' "$(spans_of st07_sibling_plain "WHERE substring(lower(hex(trace_id)), 1, 4) = 'ee06'")"
check st09_sibling_union  'ee06:06,07' "$(spans_of st09_sibling_union "WHERE substring(lower(hex(trace_id)), 1, 4) = 'ee06'")"
# the negated forms return the B spans the relation does not hold for, including
# every B span of a trace with no A span at all (ee07)
check st02_child_neg   'ee03:41 ee04:42 ee06:07 ee07:01' "$(spans_of st02_child_neg)"
check st05_parent_neg  'ee03:41 ee04:42 ee06:07 ee07:01' "$(spans_of st05_parent_neg)"
check st08_sibling_neg 'ee03:41 ee04:42 ee05:02 ee06:03 ee07:01' "$(spans_of st08_sibling_neg)"

# --- the six transitive forms ----------------------------------------------
# ee03 is 65 spans: the B span is 64 parent links below the A root, the bound.
# ee04 is 66: one link past it, so it does not match and the climb reports it.
# ee05 is a two-span cycle: it cannot resolve either.
check st10_descendant_plain 'ee03:41 ee05:02 ee06:03' "$(spans_of st10_descendant_plain "WHERE row_kind = 'match'")"
check st10_overflow         '2' "$(unresolved_of)"
check st11_descendant_neg   'ee04:42 ee06:07 ee07:01' "$(spans_of st11_descendant_neg "WHERE row_kind = 'match'")"
check st12_descendant_union 'ee03:01,41 ee05:01,02 ee06:02,03' "$(spans_of st12_descendant_union "WHERE row_kind = 'match'")"
check st13_ancestor_plain   'ee05:02 ee06:03' "$(spans_of st13_ancestor_plain "WHERE row_kind = 'match'")"
check st13_overflow         '1' "$(unresolved_of)"
check st14_ancestor_neg     'ee03:41 ee04:42 ee06:07 ee07:01' "$(spans_of st14_ancestor_neg "WHERE row_kind = 'match'")"
check st15_ancestor_union   'ee05:01,02 ee06:03,04' "$(spans_of st15_ancestor_union "WHERE row_kind = 'match'")"

# --- nested-set numbering ---------------------------------------------------
# ee01: a root, two children at the SAME instant, a grandchild, and an orphan.
# ee02: a two-span cycle with a child hanging off it, beside a well-formed root.
det() { q "$(cat "$SQL/nested_$1_detail.sql") FORMAT TSV" |
        awk '{printf "%s:%s-%s/%s ", substr($1, 15), $2, $3, $4}' | sed 's/ $//'; }
agg() { q "$(cat "$SQL/nested_$1.sql") FORMAT TSV" | tr '\t' ' '; }
check nested_ee01_detail '01:1-8/-1 02:2-5/1 04:3-4/2 03:6-7/1 05:9-10/-1' "$(det $T1)"
check nested_ee01_totals '5 2 1 10 5 2 0' "$(agg $T1)"
check nested_ee02_detail '04:1-4/-1 05:2-3/1 01:5-10/-1 02:6-9/5 03:7-8/6' "$(det $T2)"
check nested_ee02_totals '5 2 1 10 5 2 0' "$(agg $T2)"

# --- compare(): one resource under one selection span and two baseline spans -
cmp_row() { q "SELECT concat(toString(sum(if(side = 'selection', n, 0))), '/',
                             toString(sum(if(side = 'baseline', n, 0))))
               FROM ($(cat "$SQL/c08_compare.sql")) WHERE scope = '$1' AND key = '$2' AND value = '$3' FORMAT TSV"; }
check compare_shared_resource '1/2' "$(cmp_row resource k8s.pod.name pod-x)"
check compare_status_message  '1/0' "$(cmp_row intrinsic statusMessage boom)"
check compare_event_name      '1/0' "$(cmp_row event name exception)"
check compare_link_span       '1/0' "$(cmp_row link spanId 0000000000000009)"
check compare_kind_keyword    '6/2' "$(cmp_row intrinsic kind server)"

# --- the overflow signal when NOTHING matches -------------------------------
# A database holding only the over-bound chain: the climb resolves no pair, so
# the match rows are empty and the only thing the reader can act on is the
# overflow row. A statement that carries the count as a column of the matches
# returns nothing at all here.
DEEP=${DB}_deep_edge
q "DROP DATABASE IF EXISTS $DEEP" >/dev/null
mkdir -p "$WORK/edgedeep"
grep '"trace_id":"ee04' "$WORK/edge/spans.jsonl" > "$WORK/edgedeep/spans.jsonl"
"$HERE/load_staging.sh" "$CH" "$DEEP" edgedeep >/dev/null
python3 - "$CH" "$HERE/schema.sql" "$DEEP" <<'PY'
import re, sys, urllib.request
ch, path, db = sys.argv[1].rstrip('/'), sys.argv[2], sys.argv[3]
sql = re.sub(r'--[^\n]*', '', open(path).read()).replace('tqd_g1', db)
for st in [s.strip() for s in sql.split(';') if s.strip()]:
    if st.startswith('CREATE FUNCTION'):
        continue
    urllib.request.urlopen(urllib.request.Request(
        ch + '/?json_type_escape_dots_in_keys=1', data=st.encode()), timeout=600).read()
PY
python3 "$HERE/make_sql.py" "$DEEP" "$S_NS" "$E_NS" "$SQL/deep" >/dev/null
q "DROP TABLE IF EXISTS $DEEP.edge_out" >/dev/null
q "CREATE TABLE $DEEP.edge_out ENGINE = Memory AS $(cat "$SQL/deep/st10_descendant_plain.sql")" >/dev/null
check deep_no_match '0' "$(q "SELECT toString(countIf(row_kind = 'match')) FROM $DEEP.edge_out FORMAT TSV")"
check deep_overflow '1' "$(q "SELECT toString(max(unresolved)) FROM $DEEP.edge_out FORMAT TSV")"
q "DROP DATABASE IF EXISTS $DEEP" >/dev/null

q "DROP TABLE IF EXISTS $DB.edge_out" >/dev/null
printf 'checks %d, failed %d\n' "$((pass + fail))" "$fail"
[ "$fail" -eq 0 ]
