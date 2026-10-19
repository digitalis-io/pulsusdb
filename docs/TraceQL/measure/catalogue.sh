#!/bin/sh
# The TraceQL catalogue against the shipped compiler (issue #600), from an
# empty database to `catalogue.py`'s verdict. Single node.
#
#   catalogue.sh CH_HOST CH_HTTP_PORT DATABASE WORK_DIR BIN_DIR
#
# BIN_DIR holds `pulsusdb` and `xtask`, built from this tree. DATABASE is
# dropped and rebuilt by `schema/schema.sh`. Exits non-zero when the fixture
# does not land or `catalogue.py` does not pass.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../../.." && pwd)
ch=$1 port=$2 db=$3 work=$4 bin=$5
mkdir -p "$work"
q() { curl -sS --fail-with-body "http://$ch:$port/" --data-binary "$1"; }

CLICKHOUSE_SERVER=$ch CLICKHOUSE_HTTP_PORT=$port CLICKHOUSE_DB=$db sh "$root/schema/schema.sh" --mode single

# Two hours ago, on a minute: inside the default seven-day retention, and the
# step's grid starts at the fixture's first span.
base=$(( ( $(date -u +%s) - 7200 ) / 60 * 60 ))
start=${base}000000000 end=$(( base + 60 ))000000000
python3 "$here/fixture/make_catalogue_fixture.py" "$work/cat" "$base" > "$work/fixture.json"
want=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["spans"])' "$work/fixture.json")

# A port the system hands out, so no fixed port can collide.
api=$(python3 -c 'import socket; s = socket.socket(); s.bind(("localhost", 0)); print(s.getsockname()[1])')
PULSUS_HOST=localhost PULSUS_PORT=$api CLICKHOUSE_SERVER=$ch CLICKHOUSE_HTTP_PORT=$port \
    CLICKHOUSE_DB=$db "$bin/pulsusdb" > "$work/server.log" 2>&1 &
pid=$!
trap 'kill "$pid" 2>/dev/null || true' EXIT
timeout 60 sh -c "until curl -sf http://localhost:$api/ready > /dev/null; do sleep 1; done"
python3 "$here/push_otlp.py" "http://localhost:$api" "$work/cat" 1

# A push is acknowledged once the old tables hold it; the landing insert that
# feeds `spans` and its views is done when every span is there and no insert
# into `trace_landing` is running.
landed="SELECT concat(toString((SELECT count() FROM (SELECT DISTINCT trace_id, span_id FROM $db.spans))), ',',
    toString((SELECT count() FROM system.processes WHERE query_kind = 'Insert'
              AND position(query, 'trace_landing') > 0 AND current_database = '$db')))"
i=0
until [ "$(q "$landed")" = "$want,0" ]; do
    i=$((i + 1))
    [ "$i" -le 60 ] || { echo "catalogue.sh: the fixture did not land: $(q "$landed")" >&2; exit 1; }
    sleep 1
done
kill "$pid"

"$bin/xtask" traceql-catalogue --host "$ch" --port "$port" --database "$db" \
    --corpus "$root/crates/pulsus-traceql/tests/corpus" --extra "$here/catalogue-extra.tsv" \
    --start-ns "$start" --end-ns "$end" > "$work/catalogue-run.jsonl"
python3 "$here/catalogue.py" "$work/catalogue-run.jsonl" "$work/cat/spans.jsonl" "$start" "$end"
