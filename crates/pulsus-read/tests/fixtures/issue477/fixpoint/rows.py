#!/usr/bin/env python3
"""Issue #477 Q28 — every staged stream through the fixpoint guard, with
the tree's digest before and after.

Run from anywhere:

  python3 crates/pulsus-read/tests/fixtures/issue477/fixpoint/rows.py

Prints one row per stream and compares the whole table with
`expected_rows.txt`. Exit 0 means every stream answered exactly what is
committed; exit 1 prints the diff. `--update` rewrites the expectation,
which is the only way it is ever written.

The two invariants the table cannot show on its own are checked here:
after every rejection the staged tree is byte-identical and no ledger
exists, and the file OUTSIDE the root that two of the malformed streams
aim at is unchanged too.
"""

import difflib
import hashlib
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
STREAMS = os.path.join(HERE, "streams")


def digest(root):
    h = hashlib.sha256()
    for dirpath, dirnames, filenames in sorted(os.walk(root)):
        dirnames.sort()
        for name in sorted(filenames):
            path = os.path.join(dirpath, name)
            h.update(os.path.relpath(path, root).encode())
            with open(path, "rb") as fh:
                h.update(fh.read())
    return h.hexdigest()[:16]


def run(stream, tree, ledger):
    proc = subprocess.run(
        [
            sys.executable,
            os.path.join(HERE, "fixpoint.py"),
            "rename",
            os.path.join(STREAMS, stream),
            tree,
            ledger,
            "step_s",
            "step_ms",
        ],
        capture_output=True,
        text=True,
    )
    return proc.returncode, proc.stdout.strip()


def main(argv):
    rows = []
    scratch = tempfile.mkdtemp(prefix="pulsus-i477-fixpoint-")
    try:
        for stream in sorted(os.listdir(STREAMS)):
            # A fresh copy of the staged tree per stream: an accepted
            # stream REWRITES it, and a rejection invariant measured over
            # a tree a previous row already edited proves nothing.
            root = os.path.join(scratch, stream)
            shutil.copytree(os.path.join(HERE, "tree"), os.path.join(root, "tree"),
                            symlinks=True)
            shutil.copytree(os.path.join(HERE, "outside"), os.path.join(root, "outside"))
            tree = os.path.join(root, "tree")
            outside = os.path.join(root, "outside", "target.rs")
            ledger = os.path.join(root, "ledger.tsv")
            before = digest(tree)
            with open(outside, "rb") as fh:
                outside_before = fh.read()

            code, out = run(stream, tree, ledger)
            ledger_bytes = os.path.getsize(ledger) if os.path.exists(ledger) else 0
            with open(outside, "rb") as fh:
                outside_after = fh.read()
            note = ""
            if code == 2:
                # A refusal writes nothing, anywhere.
                note = (
                    f" tree_unchanged={digest(tree) == before}"
                    f" ledger_bytes={ledger_bytes}"
                    f" outside_unchanged={outside_after == outside_before}"
                )
            rows.append(f"{stream:<38} exit={code} {out}{note}")
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    table = "\n".join(rows) + "\n"

    expected_path = os.path.join(HERE, "expected_rows.txt")
    if "--update" in argv:
        with open(expected_path, "w") as fh:
            fh.write(table)
        print(table, end="")
        return 0
    print(table, end="")
    with open(expected_path) as fh:
        expected = fh.read()
    if table != expected:
        print("\nTABLE DIFFERS FROM expected_rows.txt:")
        print("".join(difflib.unified_diff(expected.splitlines(True), table.splitlines(True),
                                           "expected_rows.txt", "measured")))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
