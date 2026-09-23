#!/usr/bin/env bash
# The nested-set numbering on a tree whose answer is known by hand:
#
#     root ........ left 1, right 8, parent -1
#       A ......... left 2, right 5, parent 1
#         C ....... left 3, right 4, parent 2
#       B ......... left 6, right 7, parent 1
#
# One counter is incremented on entry and on exit, so n spans occupy 1..2n.
# Usage: nested_set_check.sh CH_URL
set -euo pipefail
CH=${1%/} DB=tqd_nested
q() { curl --fail-with-body -sS "$CH/" --data-binary "$1"; }
q "DROP DATABASE IF EXISTS $DB SYNC" >/dev/null
q "CREATE DATABASE $DB" >/dev/null
q "CREATE TABLE $DB.spans (span_id String, parent_span_id String, start_ns Int64, name String)
   ENGINE = MergeTree ORDER BY span_id" >/dev/null
q "INSERT INTO $DB.spans VALUES ('r','',100,'root'), ('a','r',200,'A'), ('c','a',300,'C'), ('b','r',400,'B')" >/dev/null
q "WITH RECURSIVE
    walk AS (SELECT span_id, parent_span_id, name, [start_ns] AS path, 0 AS depth
             FROM $DB.spans WHERE parent_span_id = ''
             UNION ALL
             SELECT c.span_id, c.parent_span_id, c.name, arrayConcat(p.path, [c.start_ns]), p.depth + 1
             FROM $DB.spans AS c INNER JOIN walk AS p ON p.span_id = c.parent_span_id
             WHERE p.depth < 64),
    ordered AS (SELECT span_id, parent_span_id, name, depth, path,
                       row_number() OVER (ORDER BY path ASC) AS r FROM walk),
    sized AS (SELECT o.span_id AS span_id, o.parent_span_id AS parent_span_id, o.name AS name,
                     o.depth AS depth, o.r AS r, count() AS subtree
              FROM ordered AS o
              INNER JOIN ordered AS d ON arraySlice(d.path, 1, length(o.path)) = o.path
              GROUP BY o.span_id, o.parent_span_id, o.name, o.depth, o.r),
    numbered AS (SELECT span_id, parent_span_id, name, depth, subtree,
                        2 * r - 1 - depth AS nested_set_left,
                        nested_set_left + 2 * subtree - 1 AS nested_set_right
                 FROM sized)
   SELECT n.name AS span, n.nested_set_left AS left, n.nested_set_right AS right,
          if(n.parent_span_id = '', -1, p.nested_set_left) AS parent
   FROM numbered AS n LEFT JOIN numbered AS p ON p.span_id = n.parent_span_id
   ORDER BY left FORMAT TSV" > "${TMPDIR:-/tmp}/tqd-nested.$$" || true
printf 'span\tleft\tright\tparent\texpected\n'
while IFS=$'\t' read -r span l r par; do
  case "$span" in
    root) e="1 8 -1";; A) e="2 5 1";; C) e="3 4 2";; *) e="6 7 1";;
  esac
  printf '%s\t%s\t%s\t%s\t%s\n' "$span" "$l" "$r" "$par" "$e"
done < "${TMPDIR:-/tmp}/tqd-nested.$$"
rm -f "${TMPDIR:-/tmp}/tqd-nested.$$"
q "DROP DATABASE $DB SYNC" >/dev/null
