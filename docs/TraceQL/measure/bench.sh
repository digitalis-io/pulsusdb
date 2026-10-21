#!/usr/bin/env bash
# functional-requirements.md §6.3 (R10, T-P1): the shipped routes against the
# reference. One corpus g1, generated at the run's own time, into both stores
# from the same bodies; every §6.3 shape timed over HTTP with the two stores
# alternating which goes first; then the verdict on §6.3's rule.
#
#   bench.sh PULSUS_URL CH_URL REF_URL REF_OTLP WORK OUT
#
# Both stores must start empty. BENCH_END_S fixes the corpus's end (default:
# now). OUT receives corpus-summary.json, run.txt, paired.tsv, fetch.tsv,
# verdict.tsv, summary.md and bodies/, each side's first body per shape; the
# exit status is the verdict's.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
PULSUS=${1%/} CH=${2%/} REF=${3%/} REFOTLP=$4 WORK=$5 OUT=$6
END=${BENCH_END_S:-$(date +%s)}; START=$((END - 10800))
mkdir -p "$WORK" "$OUT"

echo "== corpus"; python3 "$HERE/gen_corpus.py" "$WORK/g1" "$END" 2000000 > "$OUT/corpus-summary.json"
SPANS=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['spans'])" "$OUT/corpus-summary.json")
DUP=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['dup_spans'])" "$OUT/corpus-summary.json")

echo "== load PulsusDB"; python3 "$HERE/push_otlp.py" "$PULSUS" "$WORK/g1" 4
echo "== load the reference"; python3 "$HERE/push_otlp.py" "$REFOTLP" "$WORK/g1" 4

# PulsusDB keeps a retried push once, so it must hold exactly `spans`; the
# reference keeps both copies, so `spans + dup_spans` (run_all.sh's rule). More
# than that is a store that was not empty; it and the 30-minute cap exit 1.
wait_for() { # NAME URL WANT
  local name=$1 url=$2 want=$3 n prev=-1 stable=0 t0
  t0=$(date +%s)
  while :; do
    n=$(curl --fail-with-body -sS -G "$url" --data-urlencode 'q={ } | count_over_time()' \
          --data-urlencode "start=$START" --data-urlencode "end=$END" --data-urlencode step=3600s |
        python3 -c "import json,sys;d=json.load(sys.stdin);print(sum(int(float(s.get('value',0))) for x in d.get('series',[]) for s in x.get('samples',[])))")
    if [ "$n" -gt "$want" ]; then
      echo "   STOP: $name reports $n spans in the window; this corpus is $want there. It was not empty." >&2
      exit 1
    fi
    if [ "$n" -eq "$want" ] && [ "$n" = "$prev" ]; then stable=$((stable + 1)); else stable=0; fi
    [ "$stable" -ge 1 ] && { echo "   $name: $n spans after $(( $(date +%s) - t0 ))s"; return 0; }
    [ $(( $(date +%s) - t0 )) -gt 1800 ] && { echo "   GAVE UP waiting for $name: $n of $want after 30 min" >&2; exit 1; }
    prev=$n; sleep 20
  done
}
echo "== wait until both stores hold the corpus"
wait_for PulsusDB "$PULSUS/api/traces/v1/metrics/query_range" "$SPANS"
wait_for 'the reference' "$REF/api/metrics/query_range" $((SPANS + DUP))

# the two traces: the first in spans.jsonl order with exactly 20 spans, and the
# one with the most spans (ties: the smallest id)
read -r T20 N20 TBIG NBIG < <(python3 - "$WORK/g1/spans.jsonl" <<'PY'
import json, sys
n, order = {}, []
for line in open(sys.argv[1]):
    t = json.loads(line)['trace_id']
    if t not in n: n[t] = 0; order.append(t)
    n[t] += 1
t20 = next(t for t in order if n[t] == 20)
tbig = min(n, key=lambda t: (-n[t], t))
print(t20, n[t20], tbig, n[tbig])
PY
)
# t02's key: the first candidate some span of the corpus carries and the
# reference answers with at least one value over the window. None is a failed
# run, naming every candidate tried.
CANDIDATES="rpc.method http.request.method db.operation.name db.system.name messaging.system http.route"
CARRIED=$(python3 - "$WORK/g1/spans.jsonl" $CANDIDATES <<'PY'
import json, sys
want, seen = sys.argv[2:], set()
for line in open(sys.argv[1]):
    seen.update(a[0] for a in json.loads(line)['attrs'] if a[0] in want)
    if len(seen) == len(want): break
print(' '.join(k for k in want if k in seen))
PY
)
T02_KEY= TRIED=
for key in $CANDIDATES; do
  case " $CARRIED " in *" $key "*) ;; *) TRIED="$TRIED $key(no span carries it)"; continue ;; esac
  n=$(curl --fail-with-body -sS -G "$REF/api/v2/search/tag/span.$key/values" \
        --data-urlencode "start=$START" --data-urlencode "end=$END" |
      python3 -c "import json,sys;print(len(json.load(sys.stdin).get('tagValues') or []))")
  TRIED="$TRIED $key($n values)"
  if [ "$n" -gt 0 ]; then T02_KEY=$key; break; fi
done
if [ -z "$T02_KEY" ]; then
  echo "   STOP: no t02 key: the reference answers none of the candidates:$TRIED" >&2
  exit 1
fi
{
  echo "date	$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "end_s	$END"
  echo "commit	$(git -C "$HERE" rev-parse HEAD)"
  echo "spans	$SPANS"
  echo "dup_spans	$DUP"
  echo "trace_20	$T20	$N20 spans"
  echo "trace_largest	$TBIG	$NBIG spans"
  echo "t02_key	$T02_KEY"
  echo "t02_candidates_tried	${TRIED# }"
  echo "nproc	$(nproc)"
  # the two images the workflow starts, read from the files it reads them from
  echo "clickhouse_image	$(grep -o -m1 '[a-z]*/clickhouse-server@sha256:[0-9a-f]*' "$HERE/../../../.github/workflows/traceql-reference-bench.yml")"
  echo "reference_image	deploy/e2e/compose.single.yaml's pinned image, $(sed -n 's/^ *image: [^ ]*:3\.0\.2@\(sha256:[0-9a-f]*\)$/\1/p' "$HERE/../../../deploy/e2e/compose.single.yaml")"
} > "$OUT/run.txt"
cat "$OUT/run.txt"

echo "== every shape, paired"
BENCH_T20=$T20 BENCH_TBIG=$TBIG BENCH_T02_KEY=$T02_KEY BENCH_BODIES="$OUT/bodies" python3 "$HERE/http_bench.py" paired "$PULSUS" "$REF" "$START" "$END" 5 "$CH" > "$OUT/paired.tsv"
echo "== trace fetch, interleaved"
# a fetch that fails on one side exits 1 and still writes the trace's two
# lines, the failed side's ending `failed`, so the verdict charges that side
# alone and the run goes on to the verdict; one that exits writing no line
# (not like for like, or a crash) charges both sides
: > "$OUT/fetch.tsv"
for tid in "$T20" "$TBIG"; do
  python3 "$HERE/fetch_compare.py" "$PULSUS" "$REF" "$tid" 21 > "$WORK/fetch-one.tsv" || true
  if [ "$(grep -c . "$WORK/fetch-one.tsv")" != 2 ]; then
    printf 'PulsusDB\ttrace=%s\tfailed\nthe reference\ttrace=%s\tfailed\n' "${tid:0:8}" "${tid:0:8}" > "$WORK/fetch-one.tsv"
  fi
  cat "$WORK/fetch-one.tsv" >> "$OUT/fetch.tsv"
done

echo "== verdict"
status=0
python3 "$HERE/bench_verdict.py" "$OUT" > "$OUT/verdict.tsv" || status=$?
cat "$OUT/verdict.tsv"
exit $status
