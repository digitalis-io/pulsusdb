SELECT lower(hex(span_id)) AS span FROM tqd_cat.spans AS s WHERE start_ns >= 1790000000000000000 AND start_ns < 1790000060000000000 AND intDiv(start_ns, 300000000000) BETWEEN 5966666 AND 5966666 AND ((arrayExists(x -> x.2 = 'exception', events))) ORDER BY span FORMAT TSV
SETTINGS final = 1, json_type_escape_dots_in_keys = 1, max_recursive_cte_evaluation_depth = 10001
