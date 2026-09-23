WITH RECURSIVE
    1790084801000000000 AS s, 1790095601000000000 AS e,
    keys AS (SELECT (arrayJoin(range(toInt64(5966949), toInt64(5966985) + 1)), toFixedString(unhex('50FB0CD99260AC2A15D0A6F208126742'), 16)) AS k),
    sp AS (SELECT span_id, parent_span_id, start_ns FROM tqd_g1.spans
           WHERE (intDiv(start_ns, 300000000000), trace_id) IN (SELECT k FROM keys)
             AND start_ns >= s AND start_ns < e),
    roots AS (SELECT span_id, parent_span_id, start_ns FROM sp
              WHERE parent_span_id = toFixedString('', 8)
                 OR parent_span_id NOT IN (SELECT span_id FROM sp)),
    walk AS (
        SELECT span_id, parent_span_id, [(start_ns, span_id)] AS path, 0 AS depth, 0 AS phase
        FROM roots
        UNION ALL
        SELECT c.span_id, c.parent_span_id, arrayConcat(p.path, [(c.start_ns, c.span_id)]),
               p.depth + 1, p.phase
        FROM sp AS c INNER JOIN walk AS p ON p.span_id = c.parent_span_id
        WHERE p.depth < 10000 AND NOT has(arrayMap(x -> x.2, p.path), c.span_id)),
    un AS (SELECT span_id, parent_span_id, start_ns FROM sp
           WHERE span_id NOT IN (SELECT span_id FROM walk)),
    up AS (
        SELECT span_id AS x, parent_span_id AS cur, [(start_ns, span_id)] AS seen,
               (start_ns, span_id) AS mk, 0 AS d
        FROM un
        UNION ALL
        SELECT u.x, n.parent_span_id, arrayConcat(u.seen, [(n.start_ns, n.span_id)]),
               least(u.mk, (n.start_ns, n.span_id)), u.d + 1
        FROM up AS u INNER JOIN un AS n ON n.span_id = u.cur
        WHERE u.d < 10000 AND NOT has(arrayMap(y -> y.2, u.seen), n.span_id)),
    promoted AS (SELECT u.span_id AS span_id, u.parent_span_id AS parent_span_id, u.start_ns AS start_ns
                 FROM un AS u
                 INNER JOIN (SELECT x, min(mk) AS mk FROM up GROUP BY x) AS c ON c.x = u.span_id
                 WHERE c.mk = (u.start_ns, u.span_id)),
    walk2 AS (
        SELECT span_id, parent_span_id, [(start_ns, span_id)] AS path, 0 AS depth, 1 AS phase
        FROM promoted
        UNION ALL
        SELECT c.span_id, c.parent_span_id, arrayConcat(p.path, [(c.start_ns, c.span_id)]),
               p.depth + 1, p.phase
        FROM un AS c INNER JOIN walk2 AS p ON p.span_id = c.parent_span_id
        WHERE p.depth < 10000 AND NOT has(arrayMap(x -> x.2, p.path), c.span_id)),
    tour AS (SELECT span_id, parent_span_id, depth, path, phase FROM walk
             UNION ALL
             SELECT span_id, parent_span_id, depth, path, phase FROM walk2),
    ordered AS (SELECT span_id, parent_span_id, depth, path, phase,
                       row_number() OVER (ORDER BY phase ASC, path ASC) AS r
                FROM tour),
    sized AS (SELECT o.span_id AS span_id, o.parent_span_id AS parent_span_id,
                     o.depth AS depth, o.r AS r, o.phase AS phase, count() AS subtree
              FROM ordered AS o
              INNER JOIN ordered AS d
                  ON d.phase = o.phase AND arraySlice(d.path, 1, length(o.path)) = o.path
              GROUP BY o.span_id, o.parent_span_id, o.depth, o.r, o.phase),
    numbered AS (SELECT span_id, parent_span_id, depth, subtree, phase,
                        2 * r - 1 - depth AS nested_set_left,
                        nested_set_left + 2 * subtree - 1 AS nested_set_right
                 FROM sized)
SELECT count() AS spans, max(depth) AS max_depth,
       min(nested_set_left) AS min_left, max(nested_set_right) AS max_right,
       uniqExact(nested_set_left) AS distinct_left,
       countIf(nested_set_parent < 0) AS roots,
       (SELECT count() FROM sp WHERE span_id NOT IN (SELECT span_id FROM tour)) AS unnumbered
FROM (SELECT n.span_id AS span_id, n.depth AS depth, n.nested_set_left AS nested_set_left,
             n.nested_set_right AS nested_set_right,
             if(n.parent_span_id = toFixedString('', 8)
                OR n.parent_span_id NOT IN (SELECT span_id FROM ordered)
                OR n.span_id IN (SELECT span_id FROM promoted),
                -1, p.nested_set_left) AS nested_set_parent
      FROM numbered AS n
      LEFT JOIN numbered AS p ON p.span_id = n.parent_span_id)
SETTINGS max_recursive_cte_evaluation_depth = 10001
