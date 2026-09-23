#!/usr/bin/env python3
"""The answer each of the fifteen structural forms gives on the worked fixture,
with A = { resource.service.name = "checkout" } and
     B = { resource.service.name = "payment" }.
These are the literal expected values of test case T-A9.
Usage: structural_answers.py CH_URL DB START_S END_S"""
import sys, urllib.request
ch, DB, S, E = sys.argv[1].rstrip('/'), sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
SNS, ENS, B, MAX = S * 10**9, E * 10**9, 300_000_000_000, 64
W = f"start_ns >= {SNS} AND start_ns < {ENS} AND intDiv(start_ns, {B}) BETWEEN {SNS // B} AND {(ENS - 1) // B}"
A, Bf = "service = 'checkout'", "service = 'payment'"
def q(sql):
    return urllib.request.urlopen(urllib.request.Request(ch + '/', data=(sql + ' SETTINGS final = 1').encode()), timeout=300).read().decode().split()
def ids(rows): return ', '.join('…' + r[-4:] for r in sorted(rows)) or '(none)'
grouped = f"""SELECT lower(hex(arrayJoin(arrayMap(x -> x.1, hit)))) FROM (
    SELECT groupArrayIf(span_id, {A}) AS a_ids, groupArrayIf(parent_span_id, {A}) AS a_par,
           groupArrayIf((span_id, start_ns), {A}) AS a_spans,
           groupArrayIf((span_id, start_ns), {Bf}) AS b_spans, %s AS hit
    FROM {DB}.spans WHERE {W} AND (({A}) OR ({Bf})) GROUP BY trace_id) FORMAT TSV"""
# child/parent/sibling need the parent id, so carry it in the tuple
grouped2 = f"""SELECT lower(hex(arrayJoin(arrayMap(x -> x.1, %s)))) FROM (
    SELECT groupArrayIf(span_id, {A}) AS a_ids, groupArrayIf(parent_span_id, {A}) AS a_par,
           groupArrayIf((span_id, parent_span_id), {A}) AS a_spans,
           groupArrayIf((span_id, parent_span_id), {Bf}) AS b_spans
    FROM {DB}.spans WHERE {W} AND (({A}) OR ({Bf})) GROUP BY trace_id) FORMAT TSV"""
# The union modifier returns both sides of every pair the relation holds for, so
# the partner set is relation-specific: the parent of a child hit, the children
# of a parent hit, the other children of a sibling hit's parent. One expression
# for all three adds the wrong span - measured on trace ee06 of
# fixture/make_edge_fixture.py, where child union answered A1,A2,B1 for A2,B1.
#   tuple fields: 1 span_id, 2 parent_span_id
def u(keep, rel):
    partner = {
        'child':   f"arrayFilter(y -> has(arrayMap(x -> x.2, {keep}), y.1), a_spans)",
        'parent':  f"arrayFilter(y -> has(arrayMap(x -> x.1, {keep}), y.2), a_spans)",
        'sibling': (f"arrayFilter(y -> has(arrayMap(x -> x.2, {keep}), y.2) "
                    f"AND NOT has(arrayMap(x -> x.1, {keep}), y.1), a_spans)"),
    }[rel]
    return f"arrayConcat({keep}, {partner})"
CHILD   = "arrayFilter(x -> has(a_ids, x.2), b_spans)"
NCHILD  = "arrayFilter(x -> NOT has(a_ids, x.2), b_spans)"
PARENT  = "arrayFilter(x -> has(a_par, x.1), b_spans)"
NPARENT = "arrayFilter(x -> NOT has(a_par, x.1), b_spans)"
SIB     = "arrayFilter(x -> has(a_par, x.2) AND NOT has(a_ids, x.1), b_spans)"
NSIB    = "arrayFilter(x -> NOT (has(a_par, x.2) AND NOT has(a_ids, x.1)), b_spans)"
climb = f"""WITH RECURSIVE climb AS (
    SELECT trace_id, span_id AS seed, parent_span_id AS cur, 0 AS depth FROM {DB}.spans WHERE {W} AND (%s)
    UNION ALL
    SELECT c.trace_id, c.seed, x.parent_span_id, c.depth + 1 FROM climb AS c
    INNER JOIN (SELECT trace_id, span_id, parent_span_id FROM {DB}.spans WHERE {W}) AS x
        ON x.trace_id = c.trace_id AND x.span_id = c.cur
    WHERE c.depth < {MAX} - 1)
SELECT lower(hex(span_id)) FROM {DB}.spans WHERE {W} AND (%s) AND %s FORMAT TSV"""
desc_hit = f"""span_id IN (SELECT seed FROM climb WHERE cur IN (SELECT span_id FROM {DB}.spans WHERE {W} AND ({A})))"""
anc_hit  = f"""span_id IN (SELECT cur FROM climb)"""
cases = [
 ('{A} > {B}    child',        grouped2 % CHILD),
 ('{A} !> {B}   not child',    grouped2 % NCHILD),
 ('{A} &> {B}   child union',  grouped2 % u(CHILD, 'child')),
 ('{A} < {B}    parent',       grouped2 % PARENT),
 ('{A} !< {B}   not parent',   grouped2 % NPARENT),
 ('{A} &< {B}   parent union', grouped2 % u(PARENT, 'parent')),
 ('{A} ~ {B}    sibling',      grouped2 % SIB),
 ('{A} !~ {B}   not sibling',  grouped2 % NSIB),
 ('{A} &~ {B}   sibling union',grouped2 % u(SIB, 'sibling')),
 ('{A} >> {B}   descendant',           climb % (Bf, Bf, desc_hit)),
 ('{A} !>> {B}  not descendant',       climb % (Bf, Bf, 'NOT ' + desc_hit)),
 ('{A} &>> {B}  descendant union',     climb % (Bf, f"({Bf}) OR ({A})",
    f"((({Bf}) AND {desc_hit}) OR (({A}) AND span_id IN (SELECT cur FROM climb WHERE seed IN "
    f"(SELECT span_id FROM {DB}.spans WHERE {W} AND ({Bf})))))")),
 ('{A} << {B}   ancestor',             climb % (A, Bf, anc_hit)),
 ('{A} !<< {B}  not ancestor',         climb % (A, Bf, 'NOT ' + anc_hit)),
 ('{A} &<< {B}  ancestor union',       climb % (A, f"({Bf}) OR ({A})",
    f"((({Bf}) AND {anc_hit}) OR (({A}) AND span_id IN (SELECT seed FROM climb WHERE cur IN "
    f"(SELECT span_id FROM {DB}.spans WHERE {W} AND ({Bf})))))")),
]
print('form\tspans')
for name, sql in cases:
    print(f'{name}\t{ids(q(sql))}')
