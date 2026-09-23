SELECT scope, key FROM tqd_g1.tag_names FINAL WHERE scope IN ('span','resource','event','link','instrumentation') ORDER BY scope, key LIMIT 10001
