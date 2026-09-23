#!/usr/bin/env bash
# Creates the staging table and loads one corpus into it.
# Usage: load_staging.sh CH_URL DB CORPUS_NAME     (the corpus directory must be
# under the server's user_files_path, and hold spans.jsonl)
set -euo pipefail
CH=$1 DB=$2 CORPUS=$3
COLS="trace_id String, span_id String, parent_span_id String, name String, kind UInt8, start_ns Int64, end_ns Int64, status_code UInt8, status_message String, service String, resource String, scope_name String, scope_version String, scope_attrs String, attrs String, events String, links String"
q() { curl --fail-with-body -sS "${CH%/}/" --data-binary "$1"; }
q "CREATE DATABASE IF NOT EXISTS $DB"
q "$(sed 's/CREATE TABLE IF NOT EXISTS raw/CREATE TABLE IF NOT EXISTS '"$DB"'.raw/' "$(dirname "$0")/staging.sql" | grep -v '^--')"
q "INSERT INTO $DB.raw SELECT * FROM file('$CORPUS/spans.jsonl', JSONEachRow, '$COLS') SETTINGS input_format_json_read_arrays_as_strings = 1, input_format_json_read_objects_as_strings = 1"
q "SELECT 'staged rows', count() FROM $DB.raw FORMAT TSV"
