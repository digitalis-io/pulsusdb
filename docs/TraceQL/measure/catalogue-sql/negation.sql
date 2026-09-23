WITH
    1790000000000000000 AS s, 1790000060000000000 AS e,
    (SELECT (groupArray(trace_id), groupArray(keys))
     FROM (SELECT trace_id, max(start_ns) AS last,
                  groupUniqArray(intDiv(start_ns, 300000000000)) AS keys
           FROM tqd_cat.spans
           WHERE start_ns >= 1790000000000000000 AND start_ns < 1790000060000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966666 AND 5966666
             AND (NOT (multiIf(dynamicType(attrs.`a`) != 'None', (coalesce(attrs.`a`.:Int64 = 1, false) OR coalesce(attrs.`a`.:Float64 = 1, false)), multiIf(has((SELECT groupArray(resource_id) FROM tqd_cat.resources WHERE dynamicType(attrs.`a`) != 'None'), resource_id), has((SELECT groupArray(resource_id) FROM tqd_cat.resources WHERE (coalesce(attrs.`a`.:Int64 = 1, false) OR coalesce(attrs.`a`.:Float64 = 1, false))), resource_id), multiIf(arrayExists(x -> dynamicType(x) != 'None', events.attrs.`a`), arrayExists(x -> x = 1, events.attrs.`a`.:Int64), multiIf(arrayExists(x -> dynamicType(x) != 'None', links.attrs.`a`), arrayExists(x -> x = 1, links.attrs.`a`.:Int64), multiIf(dynamicType(scope_attrs.`a`) != 'None', (coalesce(scope_attrs.`a`.:Int64 = 1, false) OR coalesce(scope_attrs.`a`.:Float64 = 1, false)), false)))))))
           GROUP BY trace_id
           ORDER BY last DESC, trace_id ASC
           LIMIT 20)) AS top
SELECT lower(hex(m.trace_id)) AS trace_id, t.root_service, t.root_name, t.start_ns,
       t.end_ns - t.start_ns AS trace_duration_ns, m.last, m.matched, m.spans
FROM (SELECT trace_id, max(start_ns) AS last, count() AS matched,
             arraySlice(arraySort(x -> (x.2, x.1), groupArray((lower(hex(span_id)), start_ns, duration_ns))), 1, 3) AS spans
      FROM tqd_cat.spans
      WHERE (intDiv(start_ns, 300000000000), trace_id) IN
            (SELECT arrayJoin(arrayFlatten(arrayMap((t, ks) -> arrayMap(k -> (k, t), ks), top.1, top.2))))
        AND start_ns >= 1790000000000000000 AND start_ns < 1790000060000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966666 AND 5966666
        AND (NOT (multiIf(dynamicType(attrs.`a`) != 'None', (coalesce(attrs.`a`.:Int64 = 1, false) OR coalesce(attrs.`a`.:Float64 = 1, false)), multiIf(has((SELECT groupArray(resource_id) FROM tqd_cat.resources WHERE dynamicType(attrs.`a`) != 'None'), resource_id), has((SELECT groupArray(resource_id) FROM tqd_cat.resources WHERE (coalesce(attrs.`a`.:Int64 = 1, false) OR coalesce(attrs.`a`.:Float64 = 1, false))), resource_id), multiIf(arrayExists(x -> dynamicType(x) != 'None', events.attrs.`a`), arrayExists(x -> x = 1, events.attrs.`a`.:Int64), multiIf(arrayExists(x -> dynamicType(x) != 'None', links.attrs.`a`), arrayExists(x -> x = 1, links.attrs.`a`.:Int64), multiIf(dynamicType(scope_attrs.`a`) != 'None', (coalesce(scope_attrs.`a`.:Int64 = 1, false) OR coalesce(scope_attrs.`a`.:Float64 = 1, false)), false)))))))
      GROUP BY trace_id) AS m
LEFT JOIN (SELECT trace_id, min(start_ns) AS start_ns, max(end_ns) AS end_ns,
                  max(root_service) AS root_service, max(root_name) AS root_name
           FROM tqd_cat.traces
           WHERE trace_id IN (SELECT arrayJoin(top.1))
           GROUP BY trace_id) AS t USING trace_id
ORDER BY m.last DESC, m.trace_id ASC
SETTINGS final = 1, json_type_escape_dots_in_keys = 1, max_recursive_cte_evaluation_depth = 10001, output_format_json_quote_64bit_integers = 0
FORMAT JSONCompact
