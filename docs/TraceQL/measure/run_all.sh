#!/usr/bin/env bash
# From a clean checkout to every number the documents cite. Generates the
# corpus, loads both stores, waits until the reference has made the load
# query-visible, builds the schema and the layout alternatives, runs the query
# suite, the drills, the fixture and the comparisons, and writes every file
# under results/.
#
#   run_all.sh WORKDIR CH_URL REFERENCE_URL REFERENCE_OTLP_URL [REPL1_URL REPL2_URL SRC_HOST_PORT]
#
# WORKDIR must be the ClickHouse server's user_files_path, so file('g1/spans.jsonl')
# resolves. REFERENCE_OTLP_URL may be the receiver base or the full /v1/traces path.
# REPL1_URL and REPL2_URL are two replicas of ONE cluster (they need not be CH_URL,
# and CH_URL usually is not one of them); SRC_HOST_PORT is a native-protocol
# address from which they can read the loaded corpus. Given all three, the run
# also measures what a span costs between replicas.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
WORK=$1 CH=${2%/} REF=${3%/} REFOTLP=$4 REPL1=${5:-} REPL2=${6:-} SRC=${7:-}
END=${CORPUS_END_S:-1790095601}; START=$((END - 10800)); S_NS=${START}000000000; E_NS=${END}000000000
R=$HERE/results; mkdir -p "$WORK" "$R"
q() { curl --fail-with-body -sS "$CH/" --data-binary "$1"; }

echo "== corpus"; python3 "$HERE/gen_corpus.py" "$WORK/g1" "$END" 2000000 > "$R/corpus-summary.json"
SPANS=$(python3 -c "import json;print(json.load(open('$R/corpus-summary.json'))['spans'])")
# What the reference must end up holding: this corpus keeps a retried push once,
# and the reference keeps both copies (§6.1 F21), so its count is the corpus's
# spans plus the spans inside the 40 duplicated bodies.
REFSPANS=$(python3 -c "import json;d=json.load(open('$R/corpus-summary.json'));print(d['spans'] + d['dup_spans'])")

echo "== load the reference"; python3 "$HERE/push_otlp.py" "$REFOTLP" "$WORK/g1" 4
echo "== wait until the reference has made the load query-visible"
prev=-1; stable=0; t0=$(date +%s)
while :; do
  n=$(curl --fail-with-body -sS -G "$REF/api/metrics/query_range" --data-urlencode 'q={ } | count_over_time()' \
        --data-urlencode "start=$START" --data-urlencode "end=$END" --data-urlencode step=3600s |
      python3 -c "import json,sys;d=json.load(sys.stdin);print(sum(int(s.get('value',0)) for x in d.get('series',[]) for s in x.get('samples',[])))")
  # The comparison is against THIS corpus, so the reference must hold this
  # corpus and nothing else. `>= SPANS` would also pass on a reference that
  # still holds an earlier run's spans in the same window, which compares two
  # different populations and says nothing; the exact number is known, so
  # require it. It is `spans + dup_spans`, not `spans`: the reference keeps both
  # copies of a retried push and this design keeps one, which is §6.1's F21.
  if [ "$n" -gt "$REFSPANS" ]; then
    echo "   STOP: the reference reports $n spans in the window; this corpus is $REFSPANS" \
         "($SPANS spans, and the reference keeps both copies of the retried bodies)." >&2
    echo "   It was not empty when the load started. Drop its data volume and start it again." >&2
    exit 1
  fi
  if [ "$n" -eq "$REFSPANS" ] && [ "$n" = "$prev" ]; then stable=$((stable + 1)); else stable=0; fi
  [ "$stable" -ge 1 ] && { echo "   visible: $n spans after $(( $(date +%s) - t0 ))s"; break; }
  [ $(( $(date +%s) - t0 )) -gt 1800 ] && { echo "   GAVE UP waiting: $n of $REFSPANS after 30 min" >&2; exit 1; }
  prev=$n; sleep 20
done

echo "== staging + schema"
# re-runnable: every database below belongs to this script, and CREATE TABLE
# without IF NOT EXISTS is a 500 against a server that already ran it
for d in tqd_g1 tqd_fx tqd_cat tqd_edge; do q "DROP DATABASE IF EXISTS $d SYNC" >/dev/null; done
"$HERE/load_staging.sh" "$CH" tqd_g1 g1
apply() { python3 - "$CH" "$1" <<'PY'
import re, sys, urllib.request
ch, path = sys.argv[1].rstrip('/'), sys.argv[2]
for st in [s.strip() for s in re.sub(r'--[^\n]*', '', open(path).read()).split(';') if s.strip()]:
    urllib.request.urlopen(urllib.request.Request(ch + '/?json_type_escape_dots_in_keys=1&max_memory_usage=4000000000',
                                                  data=st.encode()), timeout=3600).read()
PY
}
python3 "$HERE/apply_schema.py" "$CH" tqd_g1
for t in spans resources traces tag_names tag_values; do q "OPTIMIZE TABLE tqd_g1.$t FINAL" >/dev/null; done
echo "== layout alternatives"; apply "$HERE/layouts.sql"

echo "== statements"; python3 "$HERE/make_sql.py" tqd_g1 "$S_NS" "$E_NS"
echo "== query suite"; export CH; : > "$R/g1-new-design.tsv"
for f in "$HERE"/sql/*.sql; do
  "$HERE/run_query.sh" "$f" warm 5 >> "$R/g1-new-design.tsv"
  "$HERE/run_query.sh" "$f" cold 3 >> "$R/g1-new-design.tsv"
done

echo "== the alternative layouts, on the shapes that discriminate"; : > "$R/layout-comparison.tsv"
# Every table layouts.sql builds, against every shape. A shape a layout cannot
# serve records why rather than being left out: `l_resource_inline` carries only
# the columns the resource question needs, so the two fetch shapes, which
# project the whole span row, have no cell there.
for tbl in spans l_trace_first_8192 l_trace_first_1024 l_trace_first_512 l_service_first l_resource_inline l_lz4; do
  for shape in b01_trace_by_id_20 b02_trace_by_id_1000 m02_quantiles_by_route m05_instant_avg_by_name t03_tag_values_filtered; do
    out="$WORK/${shape}_${tbl}.sql"
    if [ "$tbl" = l_service_first ] && [ "${shape#b0}" != "$shape" ]; then
      sed -e "s/tqd_g1\.spans\b/tqd_g1.$tbl/g" \
          -e "s/(intDiv(start_ns, 300000000000), trace_id) IN/(intDiv(start_ns, 300000000000), service, trace_id) IN/" \
          -e "s/(SELECT (k, toFixedString(unhex(/(SELECT (k, sv, toFixedString(unhex(/" "$HERE/sql/$shape.sql" > "$out"
      python3 - "$out" <<'PY'
import sys
p = sys.argv[1]; s = open(p).read()
s = s.replace("(SELECT (min(start_ns), max(end_ns))", "(SELECT (min(start_ns), max(end_ns), groupUniqArrayArray(services))")
s = s.replace(") AS k))", ") AS k) ARRAY JOIN ext.3 AS sv)")
open(p, 'w').write(s)
PY
    else
      sed "s/tqd_g1\.spans\b/tqd_g1.$tbl/g" "$HERE/sql/$shape.sql" > "$out"
    fi
    if ! NAME="${shape}__${tbl}" "$HERE/run_query.sh" "$out" warm 5 >> "$R/layout-comparison.tsv" 2>/dev/null; then
      why=$(curl -sS "$CH/" --data-binary @"$out" 2>&1 | tr '\n' ' ' | cut -c1-200)
      printf '%s\tNOT_APPLICABLE\t%s\n' "${shape}__${tbl}" "$why" >> "$R/layout-comparison.tsv"
    fi
  done
done

echo "== the newest-slice-first loop"; : > "$R/g1-new-design-sliced.tsv"
P=(python3 "$HERE/search_sliced.py" "$CH" tqd_g1 "$S_NS" "$E_NS")
"${P[@]}" s01_empty '1' '' 5 >> "$R/g1-new-design-sliced.tsv"
"${P[@]}" s02_service "service = 'checkout'" service 5 >> "$R/g1-new-design-sliced.tsv"
"${P[@]}" s04_status_code_ge_500 "coalesce(attrs.\`http%2Eresponse%2Estatus_code\`.:Int64 >= 500, false)" '' 5 >> "$R/g1-new-design-sliced.tsv"
"${P[@]}" s08_user_point "attrs.\`app%2Euser%2Eid\`.:String = 'u-10013'" '' 5 >> "$R/g1-new-design-sliced.tsv"
"${P[@]}" s12_select "status_code = 2" resource_id 5 >> "$R/g1-new-design-sliced.tsv"

echo "== the reference, same queries"; python3 "$HERE/http_bench.py" reference "$REF" "$START" "$END" 5 > "$R/g1-reference-warm.tsv"
echo "== trace fetch, interleaved"; : > "$R/g1-fetch-compare.tsv"
for tid in 50fb0cd99260ac2a15d0a6f208126742 9e0ae95131b5bedbeea2c9eb5234f1ec; do
  python3 "$HERE/fetch_compare.py" "$CH" tqd_g1 "$REF" "$tid" 21 >> "$R/g1-fetch-compare.tsv"; done
echo "== the reference's broad search, three calls"; : > "$R/g1-broad-search-check.tsv"
for i in 1 2 3; do python3 "$HERE/broad_search_check.py" "$CH" tqd_g1 "$REF" "$START" "$END" | tr '\n' ' ' >> "$R/g1-broad-search-check.tsv"; echo >> "$R/g1-broad-search-check.tsv"; done

echo "== correctness"; python3 "$HERE/ground_truth.py" "$WORK/g1/spans.jsonl" > "$R/g1-ground-truth.tsv"
python3 "$HERE/agreement.py" "$REF" "$CH" tqd_g1 "$START" "$END" > "$R/g1-agreement-raw.tsv"
python3 - "$R" <<'PY' > "$R/g1-agreement.tsv"
import sys
R = sys.argv[1]
gt = {}
for l in open(f'{R}/g1-ground-truth.tsv'):
    p = l.rstrip('\n').split('\t')
    if len(p) == 2 and p[0] != 'case': gt[p[0]] = p[1]
print('case\tcorpus\tpulsus\treference\tpulsus_matches_corpus')
for l in open(f'{R}/g1-agreement-raw.tsv'):
    p = l.rstrip('\n').split('\t')
    if len(p) < 4 or p[0] == 'case': continue
    g = gt.get(p[0], '-')
    print(f'{p[0]}\t{g}\t{p[2]}\t{p[1]}\t{"yes" if g == p[2] else "NO"}')
PY

echo "== the fixture, in both stores"
FB=$((END + 1800))
python3 "$HERE/fixture/make_fixture.py" "$WORK/fixture" "$FB"
python3 "$HERE/otlp_to_rows.py" "$WORK/fixture" > "$WORK/fixture/spans.jsonl"
"$HERE/load_staging.sh" "$CH" tqd_fx fixture
python3 "$HERE/apply_schema.py" "$CH" tqd_fx
python3 "$HERE/push_otlp.py" "$REFOTLP" "$WORK/fixture" 1
prev=-1; t0=$(date +%s)
while :; do
  n=$(curl --fail-with-body -sS -G "$REF/api/search" --data-urlencode 'q={}' --data-urlencode "start=$((FB-1))" \
        --data-urlencode "end=$((FB+10))" --data-urlencode limit=20 |
      python3 -c "import json,sys;print(len(json.load(sys.stdin).get('traces',[])))")
  [ "$n" -ge 3 ] && [ "$n" = "$prev" ] && { echo "   fixture visible after $(( $(date +%s) - t0 ))s"; break; }
  [ $(( $(date +%s) - t0 )) -gt 900 ] && { echo "   GAVE UP on fixture visibility: $n of 3" >&2; exit 1; }
  prev=$n; sleep 15
done
python3 "$HERE/fixture/fixture_answers.py" "$CH" tqd_fx "$REF" "$((FB-1))" "$((FB+10))" > "$R/fixture-answers.tsv"
echo "== the fifteen structural forms on that fixture"
python3 "$HERE/fixture/structural_answers.py" "$CH" tqd_fx "$((FB-1))" "$((FB+10))" \
        > "$R/fixture-structural.tsv"
echo "== the awkward topologies: same-time siblings, an orphan, a cycle, the depth bound"
"$HERE/edge_checks.sh" "$CH" tqd_edge "$WORK" | tee "$R/edge-checks-summary.txt"

echo "== the query catalogue: every query in the TraceQL corpus"
CORPUS_TQL=${CORPUS_TQL:-$(cd "$HERE/../../.." && pwd)/crates/pulsus-traceql/tests/corpus}
# A fixed base, not one derived from the corpus window: the catalogue lives in
# its own database, and the command printed in docs/TraceQL/query-catalogue.md
# has to reproduce the committed answers whenever it is run.
CATB=${CATALOGUE_BASE_S:-1790000000}
python3 "$HERE/fixture/make_catalogue_fixture.py" "$WORK/cat" "$CATB" > "$R/catalogue-fixture.json"
"$HERE/load_staging.sh" "$CH" tqd_cat cat
python3 "$HERE/apply_schema.py" "$CH" tqd_cat
# The perturbation suite runs FIRST, because it writes
# results/perturbations.tsv and the catalogue document quotes it: the other
# order would describe the previous run's rules (review round 5). It is one of
# seven files under results/ that a later step of the same run reads;
# measure/README.md lists all seven with the line that writes each and the line
# that reads it.
echo "== one rule changed at a time: the run must notice each one"
python3 "$HERE/perturb_check.py" "$CH" tqd_cat "$CORPUS_TQL" "$WORK/cat/spans.jsonl" \
        "$WORK/perturb" "${CATB}000000000" "$((CATB + 60))000000000" | tail -3

python3 "$HERE/catalogue.py" "$CH" tqd_cat "$CORPUS_TQL" "$WORK/cat/spans.jsonl" "$HERE" \
        "${CATB}000000000" "$((CATB + 60))000000000" | tee "$R/catalogue-summary.txt"

# Every number and set the documents state, against the thing it counts.
# It runs after the catalogue because one of its rules reads that run's result.
echo "== the counted claims the documents make, against the tree"
python3 "$HERE/claims_check.py"

echo "== storage totals, compression, rows per span"
q "SELECT table, sum(rows) AS rows, sum(bytes_on_disk) AS bytes, round(sum(bytes_on_disk)/$SPANS, 3) AS b_per_span
   FROM system.parts WHERE database='tqd_g1' AND active AND table IN ('spans','resources','traces','tag_names','tag_values')
   GROUP BY table ORDER BY table FORMAT TSVWithNames" > "$R/storage.tsv"
q "WITH (SELECT sum(bytes_on_disk) FROM system.parts WHERE database='tqd_g1' AND active AND table IN ('spans','resources','traces','tag_names','tag_values')) AS total,
        (SELECT sum(bytes_on_disk) FROM system.parts WHERE database='tqd_g1' AND active AND table='spans') AS span,
        (SELECT sum(data_compressed_bytes) FROM system.parts WHERE database='tqd_g1' AND active AND table='spans') AS comp,
        (SELECT sum(data_uncompressed_bytes) FROM system.parts WHERE database='tqd_g1' AND active AND table='spans') AS uncomp,
        (SELECT (SELECT count() FROM tqd_g1.spans FINAL) + (SELECT count() FROM tqd_g1.resources FINAL)
              + (SELECT count() FROM tqd_g1.traces FINAL) + (SELECT count() FROM tqd_g1.tag_names FINAL)
              + (SELECT count() FROM tqd_g1.tag_values FINAL)) AS rows
   SELECT total AS total_bytes, total/$SPANS AS bytes_per_span, (total-span)/span*100 AS index_overhead_pct,
          uncomp/comp AS compression, rows AS total_rows, rows/$SPANS AS rows_per_span
   FORMAT TSVWithNames" >> "$R/storage.tsv"
q "SELECT name, data_compressed_bytes AS bytes, round(data_compressed_bytes/$SPANS, 3) AS b_per_span
   FROM system.columns WHERE database='tqd_g1' AND table='spans' ORDER BY bytes DESC FORMAT TSVWithNames" >> "$R/storage.tsv"

echo "== what max_dynamic_paths defaults to, measured at the boundary"
"$HERE/json_paths_default.sh" "$CH" > "$R/json-paths-default.tsv"

echo "== R6 denominators: returned bytes against the trace's stored bytes"
{ printf 'trace\treturned_bytes\tstored_uncompressed_bytes\tratio\n'
  for pair in "50FB0CD99260AC2A15D0A6F208126742:b01_trace_by_id_20" "9E0AE95131B5BEDBEEA2C9EB5234F1EC:b02_trace_by_id_1000"; do
    tid=${pair%%:*}; shape=${pair##*:}
    ret=$(curl --fail-with-body -sS "$CH/?default_format=RowBinary&final=1" --data-binary @"$HERE/sql/$shape.sql" | wc -c)
    den=$(q "SELECT sum(byteSize(*)) FROM tqd_g1.spans WHERE trace_id = unhex('$tid') SETTINGS final = 1 FORMAT TSV")
    printf '%s\t%s\t%s\t%s\n' "$tid" "$ret" "$den" "$(python3 -c "print(round($ret/$den, 3))")"
  done; } > "$R/r6-denominators.tsv"

echo "== the comparison table"
python3 - "$R" "$HERE/baseline" <<'PY' > "$R/comparison.tsv"
import sys
R, B = sys.argv[1], sys.argv[2]
new = {}
for r in open(f'{R}/g1-new-design.tsv'):
    p = r.rstrip('\n').split('\t')
    if len(p) < 10: continue
    new.setdefault(p[0], {})[p[1]] = {'rows': p[3], 'ret': p[6], 'med': p[9]}
def load(f, cols):
    out = {}
    try: lines = open(f)
    except FileNotFoundError: return out
    for r in lines:
        p = r.rstrip('\n').split('\t')
        if not p or p[0].startswith(('#', 'case', 'name', 'shape')): continue
        out[p[0]] = dict(zip(cols, p[1:]))
    return out
ref = load(f'{R}/g1-reference-warm.tsv', ['status', 'bytes', 'min', 'med', 'max'])
# today's three figures are an INPUT, not something this run produces: it has no
# PulsusDB server of the old design to measure. `measure/baseline/g1-today.tsv`
# carries them with the date and the command that produced them, and
# `measure/README.md` says how to regenerate it. Everything under results/ is
# written by this script.
tod = load(f'{B}/g1-today.tsv', ['status', 'bytes', 'min', 'med', 'max', 'stmts', 'rows', 'chbytes'])
print('shape\tnew_ms_warm\tnew_ms_cold\tnew_rows\tnew_ret_bytes\tref_ms\tref_bytes\ttoday_ms\ttoday_stmts')
for k in sorted(set(new) | set(ref)):
    n = new.get(k, {}); w = n.get('warm', {}); c = n.get('cold', {}); r = ref.get(k, {}); t = tod.get(k, {})
    print('\t'.join([k, w.get('med', '-'), c.get('med', '-'), w.get('rows', '-'), w.get('ret', '-'),
                     r.get('med', '-'), r.get('bytes', '-'),
                     (t.get('med', '-') if t.get('status') == '200' else t.get('status', '-')), t.get('stmts', '-')]))
PY

echo "== drills"
{ echo "--- the window rule at both edges"; "$HERE/boundary.sh" "$CH" tqd_g1
  echo "--- retention: dropping one day"
  # its own copy, so the drill cannot empty a table the layout comparison reads
  q "DROP TABLE IF EXISTS tqd_g1.l_drop_probe SYNC" >/dev/null
  q "CREATE TABLE tqd_g1.l_drop_probe AS tqd_g1.spans" >/dev/null
  q "INSERT INTO tqd_g1.l_drop_probe SELECT * FROM tqd_g1.spans" >/dev/null
  q "OPTIMIZE TABLE tqd_g1.l_drop_probe FINAL" >/dev/null
  "$HERE/retention.sh" "$CH" tqd_g1 l_drop_probe \
      "$(q "SELECT toString(min(toDate(fromUnixTimestamp64Nano(start_ns)))) FROM tqd_g1.spans FORMAT TSV")"
  echo "--- the recursive climb's bound"; "$HERE/recursion_bound.sh" "$CH" 64
  echo "--- nested-set numbering on a known tree"; "$HERE/nested_set_check.sh" "$CH"
  echo "--- shared-span service edges"; "$HERE/shared_span_edges.sh" "$CH"
  if [ -n "$REPL1" ] && [ -n "$REPL2" ] && [ -n "$SRC" ]; then
    echo "--- replication bytes"; "$HERE/replication_bytes.sh" "$REPL1" "$REPL2" "$SRC" tqd_g1
  else echo "--- replication bytes: skipped (no replica pair given)"; fi
} > "$R/drills.txt" 2>&1
tail -n +1 "$R/drills.txt" | head -40

echo "== done; results are in $R"
