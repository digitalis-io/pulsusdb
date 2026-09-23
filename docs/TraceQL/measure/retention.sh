#!/usr/bin/env bash
# Retention is a partition drop: no ALTER ... DELETE, no row rewrite. Drops one
# day partition from a copy of the span table and reports the time, the parts
# and rows before and after, and that no merge and no ALTER ... DELETE was scheduled.
# Usage: retention.sh CH_URL DB TABLE DAY
set -euo pipefail
CH=${1%/} DB=$2 T=$3 DAY=$4
q() { curl --fail-with-body -sS "$CH/" --data-binary "$1"; }
echo "before: $(q "SELECT count(), sum(rows), sum(bytes_on_disk) FROM system.parts WHERE database='$DB' AND table='$T' AND active FORMAT TSV")  (parts, rows, bytes)"
id="retention-$RANDOM"
t0=$(date +%s.%N); q "ALTER TABLE $DB.$T DROP PARTITION '$DAY' SETTINGS mutations_sync = 2" >/dev/null; t1=$(date +%s.%N)
echo "drop seconds: $(echo "$t1 - $t0" | bc)"
echo "after:  $(q "SELECT count(), sum(rows), sum(bytes_on_disk) FROM system.parts WHERE database='$DB' AND table='$T' AND active FORMAT TSV")  (parts, rows, bytes)"
echo "mutations scheduled: $(q "SELECT count() FROM system.mutations WHERE database='$DB' AND table='$T' FORMAT TSV")"
echo "merges running:      $(q "SELECT count() FROM system.merges WHERE database='$DB' AND table='$T' FORMAT TSV")"
