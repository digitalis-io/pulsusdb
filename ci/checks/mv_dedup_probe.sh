#!/bin/sh
# Issue #494: does ClickHouse's block deduplication protect a materialized
# view's target table?
#
# WHY THIS EXISTS. The obvious alternative to suppressing a retried push at
# ingest is to let the storage engine collapse the duplicate — a
# `ReplacingMergeTree`, or ClickHouse's own insert-block deduplication. This
# probe is the measurement that ruled that route out, kept so the ruling can
# be re-run rather than remembered:
#
#   after first insert      src=2 roll=2
#   identical block again   src=2 roll=4      <- the source row was dropped,
#   again, setting explicit src=2 roll=6         the view fired anyway
#
# The protective setting (`deduplicate_blocks_in_dependent_materialized_views`)
# is on by default and does not change that outcome, because the target has
# no deduplication window of its own. So for the tables issue #494 covers,
# whose rollup targets carry no window, a duplicate INSERT adds to the
# rollup and nothing afterwards subtracts from it, which is why the
# suppression has to happen BEFORE the insert.
#
# Block deduplication DOES work for a view's target when the target carries
# its own window (issue #560): the view's insert into the target carries a
# block id derived from the source block, and a target with
# `non_replicated_deduplication_window` set recognises the repeat and drops
# it. The last step shows it:
#
#   target with its own window, identical block twice   src=2 roll=2
#
# NOT wired into CI: it creates and drops a database, and it is evidence for
# a decision rather than a regression check.
#
# Usage:
#   CH=<clickhouse http base> DB_PREFIX=<prefix you own> DB=<prefix>_<name> \
#     sh ci/checks/mv_dedup_probe.sh
set -eu
: "${CH:?set CH to the ClickHouse HTTP base}"
: "${DB_PREFIX:?set DB_PREFIX to a database-name prefix you own}"
: "${DB:?set DB to a database name starting with DB_PREFIX; it is created and dropped}"

case "$DB" in
  "$DB_PREFIX"_*) ;;
  *) echo "refusing: DB '$DB' is not under DB_PREFIX '$DB_PREFIX'" >&2; exit 2 ;;
esac
case "$DB" in
  *[!A-Za-z0-9_]*) echo "refusing: DB '$DB' is not a bare identifier" >&2; exit 2 ;;
esac

# --fail makes curl exit non-zero on an HTTP error, so `set -e` stops the
# script rather than carrying on against a database that was never created.
q() { curl -sS --fail-with-body "$CH/" --data-binary "$1"; }

exists=$(q "SELECT count() FROM system.databases WHERE name = '$DB'")
if [ "$exists" != "0" ]; then
  echo "refusing to touch existing database '$DB'" >&2
  exit 2
fi
cleanup() { curl -sS "$CH/" --data-binary "DROP DATABASE IF EXISTS $DB" >/dev/null 2>&1 || true; }
on_signal() { cleanup; echo "interrupted" >&2; exit 130; }
trap cleanup EXIT
trap on_signal INT TERM

q "CREATE DATABASE $DB"
q "CREATE TABLE $DB.src (fp UInt64, ts Int64, body String)
   ENGINE = MergeTree ORDER BY (fp, ts)
   SETTINGS non_replicated_deduplication_window = 100"
q "CREATE TABLE $DB.roll (fp UInt64, bucket Int64,
     cnt SimpleAggregateFunction(sum, UInt64))
   ENGINE = AggregatingMergeTree ORDER BY (fp, bucket)"
q "CREATE MATERIALIZED VIEW $DB.roll_mv TO $DB.roll AS
   SELECT fp, intDiv(ts, 5) AS bucket, count() AS cnt
   FROM $DB.src GROUP BY fp, bucket"

echo "setting default: $(q "SELECT value FROM system.settings
  WHERE name = 'deduplicate_blocks_in_dependent_materialized_views'")"

q "INSERT INTO $DB.src VALUES (1,10,'a'),(1,11,'b')"
echo "after first insert      src=$(q "SELECT count() FROM $DB.src") roll=$(q "SELECT sum(cnt) FROM $DB.roll")"

q "INSERT INTO $DB.src VALUES (1,10,'a'),(1,11,'b')"
echo "identical block again   src=$(q "SELECT count() FROM $DB.src") roll=$(q "SELECT sum(cnt) FROM $DB.roll")"

curl -sS --fail-with-body "$CH/?deduplicate_blocks_in_dependent_materialized_views=1" \
  --data-binary "INSERT INTO $DB.src VALUES (1,10,'a'),(1,11,'b')"
echo "again, setting explicit src=$(q "SELECT count() FROM $DB.src") roll=$(q "SELECT sum(cnt) FROM $DB.roll")"

# Issue #560: the same source, view and block as above, with one
# difference — the target carries its own deduplication window. The
# repeated block is now dropped at the target as well as at the source.
q "CREATE TABLE $DB.src2 (fp UInt64, ts Int64, body String)
   ENGINE = MergeTree ORDER BY (fp, ts)
   SETTINGS non_replicated_deduplication_window = 100"
q "CREATE TABLE $DB.roll2 (fp UInt64, bucket Int64,
     cnt SimpleAggregateFunction(sum, UInt64))
   ENGINE = AggregatingMergeTree ORDER BY (fp, bucket)
   SETTINGS non_replicated_deduplication_window = 100"
q "CREATE MATERIALIZED VIEW $DB.roll2_mv TO $DB.roll2 AS
   SELECT fp, intDiv(ts, 5) AS bucket, count() AS cnt
   FROM $DB.src2 GROUP BY fp, bucket"
q "INSERT INTO $DB.src2 VALUES (1,10,'a'),(1,11,'b')"
q "INSERT INTO $DB.src2 VALUES (1,10,'a'),(1,11,'b')"
echo "target with its own window, identical block twice   src=$(q "SELECT count() FROM $DB.src2") roll=$(q "SELECT sum(cnt) FROM $DB.roll2")"
