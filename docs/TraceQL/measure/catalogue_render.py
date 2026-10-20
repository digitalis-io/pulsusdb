#!/usr/bin/env python3
"""The catalogue's disposition rule: which accepted queries the API answers
with a `400`, and why (`refusal`).

`catalogue.py` checks it against the shipped planner's disposition of every
query of the run. The statements are the shipped compiler's; the
TraceQL-to-SQL rules this module once rendered went with the design renderer
(#600 part 1b-2).
"""


DURATION_INTRINSICS = ('duration', 'span:duration')


def _walk(e):
    """every node of a field expression"""
    if not isinstance(e, dict): return
    yield e
    for k in ('l', 'r', 'e'):
        if k in e: yield from _walk(e[k])


def _spanset_filters(sp):
    if sp is None: return
    if sp['k'] == 'filter':
        yield sp['body']
    elif sp['k'] in ('sp_and', 'sp_or', 'struct'):
        yield from _spanset_filters(sp['l']); yield from _spanset_filters(sp['r'])


def refusal(q):
    """The reason the API answers `400`, or None.

    One rule per row of `docs/TraceQL/server-implementation.md` §3.2 whose
    **today** column reads `400`, plus the two the shipped planner adds. Each
    names the file that refuses it, and `catalogue.py` fails if this function
    and the shipped planner disagree on any query of the run.
    """
    for st in q['pipeline']:
        # `{A} | {B} && {C}` — search_plan.rs:1950-1956
        if st['k'] == 'filter_stage' and st['spanset']['k'] != 'filter':
            return ('a `|` stage must be a single { ... } filter, not a '
                    'cross-spanset or structural operation')
        # `| by(.b + .c)` — search_plan.rs:2011-2017
        if st['k'] == 'by' and st['key']['k'] not in ('attr', 'intrinsic'):
            return ('a grouping key must resolve to a single per-span value, so it '
                    'must be an attribute or an intrinsic')
    # `{ duration > 100 }` — filter.rs:1710-1715
    bodies = list(_spanset_filters(q['spanset']))
    bodies += [s['spanset']['body'] for s in q['pipeline']
               if s['k'] == 'filter_stage' and s['spanset']['k'] == 'filter']
    for body in bodies:
        for n in _walk(body):
            if n.get('k') != 'bin' or n['op'] not in ('=', '!=', '<', '<=', '>', '>='):
                continue
            for a, b in ((n['l'], n['r']), (n['r'], n['l'])):
                if (a.get('k') == 'intrinsic' and a['name'] in DURATION_INTRINSICS
                        and b.get('k') == 'lit' and b['t'] != 'duration'):
                    return 'duration requires a duration literal'
    metric = next((s for s in q['pipeline'] if s['k'] == 'metric'), None)
    if metric is not None or any(s['k'] == 'compare' for s in q['pipeline']):
        # metrics_plan.rs:417-424
        if q['spanset']['k'] != 'filter':
            return ('cross-spanset and structural expressions are not supported by '
                    'metrics queries')
    if metric is not None:
        # `docs/api.md` §4.4, "Function set": `by()` takes any key, and the
        # target is `duration`, an attribute or `span:childCount`; any other
        # intrinsic target is a `400` (#600 part 1b-1)
        args = metric['args']
        if metric['fn'] not in ('rate', 'count_over_time') and args:
            a = args[0]
            if a.get('k') == 'intrinsic' and a['name'] not in DURATION_INTRINSICS + (
                    'span:childCount', 'childCount'):
                return 'an intrinsic target other than duration and span:childCount'
    return None
