#!/usr/bin/env python3
"""Applies `measure/schema.sql` to one database.

`schema.sql` is written against `tqd_g1`; every database this suite builds has
the same tables, so the name is substituted here rather than kept in four
copies. The three `CREATE FUNCTION` statements are server-wide and carry
`IF NOT EXISTS`, so applying the file to a second database is safe.

Usage: apply_schema.py CH_URL DB [SCHEMA_SQL]
"""
import os, re, sys, urllib.request

ch, db = sys.argv[1].rstrip('/'), sys.argv[2]
path = sys.argv[3] if len(sys.argv) > 3 else os.path.join(os.path.dirname(os.path.abspath(__file__)), 'schema.sql')
sql = re.sub(r'--[^\n]*', '', open(path).read()).replace('tqd_g1', db)
url = ch + '/?json_type_escape_dots_in_keys=1&max_memory_usage=4000000000'
for st in [s.strip() for s in sql.split(';') if s.strip()]:
    urllib.request.urlopen(urllib.request.Request(url, data=st.encode()), timeout=3600).read()
print(f'schema applied to {db}')
