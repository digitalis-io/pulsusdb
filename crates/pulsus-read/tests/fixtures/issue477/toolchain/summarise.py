#!/usr/bin/env python3
"""Counts `broken_intra_doc_links` diagnostics in a cargo JSON stream.

`grep -c` over the raw stream counts a line per message and cannot tell a
diagnostic's own code from the same string appearing inside another
message's rendered text, so the count is taken from the parsed records.

Usage: summarise.py <stream.json> [<file substring>]
"""
import json
import sys


def main(argv):
    total = 0
    broken = []
    for line in open(argv[1], encoding="utf-8"):
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if rec.get("reason") != "compiler-message":
            continue
        total += 1
        msg = rec.get("message") or {}
        code = (msg.get("code") or {}).get("code") or ""
        if "broken_intra_doc_links" not in code:
            continue
        for sp in msg.get("spans") or []:
            if sp.get("is_primary"):
                broken.append(f"{sp['file_name']}:{sp['line_start']}")
    needle = argv[2] if len(argv) > 2 else None
    hits = [b for b in broken if needle is None or needle in b]
    print(f"compiler_messages={total} broken_intra_doc_links={len(broken)}", end="")
    if needle is not None:
        print(f" at_{needle}={len(hits)} {sorted(hits)}", end="")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
