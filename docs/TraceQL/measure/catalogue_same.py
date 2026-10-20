#!/usr/bin/env python3
"""The same catalogue run through several entry points (issue #600 part 2):
one node, and a cluster through each shard's node, over one fixture at one
base time. Every query must answer the same in every run, no served query
may fail, and every metrics request must be bounded: 1 to 100 statements, no
sentinel row, and no statement returning more rows than its answer has
series (one, when it has none). Search answers are compared with the interpreter by `catalogue.py`.

    catalogue_same.py RUN_JSONL RUN_JSONL [RUN_JSONL ...]
"""
import json
import sys


def load(path):
    with open(path) as f:
        return {r["name"]: r for r in map(json.loads, f)}


def main(paths):
    runs = [load(p) for p in paths]
    names = sorted(set().union(*runs))
    differ, failed, unbounded = [], [], []
    metrics = 0
    for name in names:
        rs = [run.get(name) for run in runs]
        if any(r is None for r in rs):
            differ.append(f"{name}: missing from a run")
            continue
        for path, r in zip(paths, rs):
            if r.get("error") or r.get("instant_error"):
                failed.append(f"{name} in {path}: {(r.get('error') or r['instant_error'])[:200]}")
        key = [(r.get("disposition"), r.get("route"), r.get("result"), r.get("instant")) for r in rs]
        if any(k != key[0] for k in key[1:]):
            differ.append(f"{name}: the runs' answers differ")
        if rs[0].get("route") != "metrics" or rs[0].get("error"):
            continue
        metrics += 1
        for path, r in zip(paths, rs):
            if r.get("error") or r.get("instant_error"):
                continue
            for request, statements in r["statements"].items():
                # each request against its own answer (#600 part 1b-2)
                series = len(r["instant" if request == "instant" else "result"]["series"])
                over = r["overflow"].get(request)
                rows = [s[1] for s in statements]
                if over or not 1 <= len(rows) <= 100 or max(rows) > max(series, 1):
                    unbounded.append(f"{name} {request} in {path}: {len(rows)} statements, "
                                     f"rows {rows}, {series} series, overflow {over}")
    print(f"queries in every run                     {len(names)}")
    print(f"answers that differ between runs         {len(differ)}")
    print(f"served queries that failed               {len(failed)}")
    print(f"metrics queries checked                  {metrics}")
    print(f"metrics requests over their bound        {len(unbounded)}")
    for line in differ + failed + unbounded:
        print("  " + line)
    return 0 if not (differ or failed or unbounded) and metrics > 0 else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
