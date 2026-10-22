#!/bin/sh
# The TraceQL catalogue against the shipped compiler (issue #600), from an
# empty database to `catalogue.py`'s verdict. On one node, or on the cluster `PULSUS_CLUSTER` names.
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

# On a cluster the schema is the clustered one, `pulsusdb` and the command go
# through the `_dist` tables, and the run waits for every replica.
cluster=${PULSUS_CLUSTER:-}
if [ -n "$cluster" ]; then
    mode=cluster spans=spans_dist procs="clusterAllReplicas('$cluster', system.processes)"
else
    mode=single spans=spans procs=system.processes
fi
CLICKHOUSE_SERVER=$ch CLICKHOUSE_HTTP_PORT=$port CLICKHOUSE_DB=$db sh "$root/schema/schema.sh" --mode "$mode"

# Two hours ago, on a minute: inside the default seven-day retention, and the
# step's grid starts at the fixture's first span.
base=${CATALOGUE_BASE:-$(( ( $(date -u +%s) - 7200 ) / 60 * 60 ))}
start=${base}000000000 end=$(( base + 120 ))000000000
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

# A push is acknowledged once its insert into `trace_landing` has committed;
# the run still waits until every span is in `spans` and no insert into
# `trace_landing` is running. On a cluster the views write through the
# `_dist` tables, flushed here on the node that took the push.
landed="SELECT concat(toString((SELECT count() FROM (SELECT DISTINCT trace_id, span_id FROM $db.$spans))), ',',
    toString((SELECT count() FROM $procs WHERE query_kind = 'Insert'
              AND position(query, 'trace_landing') > 0 AND current_database = '$db')))"
flush() {
    [ -n "$cluster" ] || return 0
    for t in $(q "SELECT name FROM system.tables WHERE database = '$db' AND engine = 'Distributed' FORMAT TSV"); do
        q "SYSTEM FLUSH DISTRIBUTED $db.$t" > /dev/null
    done
}
i=0
until flush && [ "$(q "$landed")" = "$want,0" ]; do
    i=$((i + 1))
    [ "$i" -le 60 ] || { echo "catalogue.sh: the fixture did not land: $(q "$landed")" >&2; exit 1; }
    sleep 1
done

# Every replica holds what its shard was sent, and every shard holds some of
# the fixture: a read may reach any replica.
if [ -n "$cluster" ]; then
    for t in $(q "SELECT table FROM system.replicas WHERE database = '$db' ORDER BY table FORMAT TSV"); do
        q "SYSTEM SYNC REPLICA ON CLUSTER '$cluster' $db.$t" > /dev/null
    done
    shards=$(q "SELECT max(shard_num) FROM system.clusters WHERE cluster = '$cluster'")
    placed="SELECT concat(toString(count()), ',', toString(min(same)), ',', toString(sum(top))) FROM (
        SELECT s, min(n) = max(n) AND min(n) > 0 AS same, max(n) AS top FROM (
            SELECT getMacro('shard') AS s, hostName() AS h, uniqExact(trace_id, span_id) AS n
            FROM clusterAllReplicas('$cluster', $db.spans) GROUP BY s, h) GROUP BY s)"
    [ "$(q "$placed")" = "$shards,1,$want" ] || {
        echo "catalogue.sh: the fixture is not on every shard and replica: $(q "$placed")" >&2; exit 1; }
fi
kill "$pid"

"$bin/xtask" traceql-catalogue --host "$ch" --port "$port" --database "$db" ${cluster:+--cluster "$cluster"} \
    --corpus "$root/crates/pulsus-traceql/tests/corpus" --extra "$here/catalogue-extra.tsv" \
    --start-ns "$start" --end-ns "$end" > "$work/catalogue-run.jsonl"
python3 "$here/catalogue.py" "$work/catalogue-run.jsonl" "$work/cat/spans.jsonl" "$start" "$end"
