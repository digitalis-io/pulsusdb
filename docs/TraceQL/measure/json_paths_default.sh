#!/usr/bin/env bash
# What `max_dynamic_paths` defaults to, asked of the database rather than of its
# documentation. `server-implementation.md` §7 names that default in a risk row:
# past it a JSON value's extra paths stop being their own subcolumn and go to
# shared data, so a filter on a rare key stops being a single subcolumn read.
#
# The reading is a boundary, not one dramatic value. Two probes, and a control:
#
#   per value   one row holding N distinct paths, N = 1023, 1024, 1025, 1100
#               -> dynamic paths kept, and paths pushed to shared data
#   per part    one insert of 2,000 rows, each row one distinct path, one part
#               -> distinct dynamic paths the part holds
#   control     the same insert into a column declared JSON(max_dynamic_paths=8)
#               -> 8, which is what says the figure is the setting and not the
#                  probe
#
# Usage: json_paths_default.sh CH_URL
set -euo pipefail
CH=${1%/} DB=tqd_jpaths
q() { curl --fail-with-body -sS "$CH/" --data-binary "$1"; }
q "DROP DATABASE IF EXISTS $DB SYNC" >/dev/null
q "CREATE DATABASE $DB" >/dev/null
q "CREATE TABLE $DB.per_value (id UInt32, j JSON) ENGINE = MergeTree ORDER BY id" >/dev/null
for n in 1023 1024 1025 1100; do
  q "INSERT INTO $DB.per_value SELECT $n, toJSONString(mapFromArrays(
       arrayMap(i -> concat('k', toString(i)), range($n)), range($n)))::JSON" >/dev/null
done
q "CREATE TABLE $DB.per_part (id UInt32, j JSON) ENGINE = MergeTree ORDER BY id" >/dev/null
q "INSERT INTO $DB.per_part SELECT number,
     toJSONString(map(concat('k', toString(number)), number))::JSON FROM numbers(2000)" >/dev/null
q "CREATE TABLE $DB.declared (id UInt32, j JSON(max_dynamic_paths=8)) ENGINE = MergeTree ORDER BY id" >/dev/null
q "INSERT INTO $DB.declared SELECT number,
     toJSONString(map(concat('k', toString(number)), number))::JSON FROM numbers(2000)" >/dev/null

printf 'probe\tpaths_offered\tdynamic\tshared\n'
q "SELECT concat('per_value_', toString(id)), toString(id),
          toString(length(JSONDynamicPaths(j))), toString(length(JSONSharedDataPaths(j)))
   FROM $DB.per_value ORDER BY id FORMAT TSV"
q "SELECT 'per_part_one_insert', '2000',
          toString((SELECT uniqExact(p) FROM $DB.per_part ARRAY JOIN JSONDynamicPaths(j) AS p)),
          toString((SELECT sum(length(JSONSharedDataPaths(j))) FROM $DB.per_part)) FORMAT TSV"
q "SELECT 'control_declared_8', '2000',
          toString((SELECT uniqExact(p) FROM $DB.declared ARRAY JOIN JSONDynamicPaths(j) AS p)),
          toString((SELECT sum(length(JSONSharedDataPaths(j))) FROM $DB.declared)) FORMAT TSV"
q "SELECT 'parts_in_per_part', '-', toString(count()), '-' FROM system.parts
   WHERE database='$DB' AND table='per_part' AND active FORMAT TSV"
q "DROP DATABASE $DB SYNC" >/dev/null
