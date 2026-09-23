SELECT series, groupArray((t, v)) AS points,
       groupArray((t, ex.1, ex.2, ex.3)) AS exemplars
FROM (SELECT series, t, v, ex FROM (SELECT toString('') AS series, (intDiv(start_ns - 1, 60000000000) + 1) * 60000 AS t, count() AS v, argMax((lower(hex(trace_id)), lower(hex(span_id)), duration_ns), (duration_ns, span_id)) AS ex FROM tqd_cat.spans WHERE start_ns >= 1790000000000000000 AND start_ns < 1790000060000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966666 AND 5966666 AND (((parent_span_id = toFixedString('', 8) OR (trace_id, parent_span_id) NOT IN (SELECT trace_id, span_id FROM tqd_cat.spans WHERE start_ns >= 1790000000000000000 AND start_ns < 1790000060000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966666 AND 5966666)) AND true)) GROUP BY series, t) ORDER BY series, t)
GROUP BY series ORDER BY series
SETTINGS final = 1, json_type_escape_dots_in_keys = 1, max_recursive_cte_evaluation_depth = 10001, output_format_json_quote_64bit_integers = 0
FORMAT JSONCompact
