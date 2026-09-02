"""Issue #477 Q26 — what the compiler is asking for at the sites the
line-level rewrite could not express.

Reads the SAME `cargo check --message-format=json` stream `fixpoint.py`
validated, through `fixpoint.py`'s own parser and its own refusal rules,
and prints for every primary error span: the file and line, the
compiler's message, and the source line as it stands in the MUTATED tree
at the moment the loop stopped.

It exists because the script used to name a reason for stopping instead
of showing one, and named the wrong one. Everything printed here is read
off the compiler's output or off the file; nothing is inferred.

  python3 stalled.py <round.json> <root>
"""

import os
import sys

HERE = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "fixpoint"))

import fixpoint  # noqa: E402


def main(argv):
    if len(argv) < 3:
        print(__doc__)
        return 64
    stream_path, root = argv[1], argv[2]
    try:
        recs = fixpoint.records(open(stream_path, encoding="utf-8").read())
    except fixpoint.Refused as refusal:
        print(f"{refusal}")
        return 2
    # Re-walk the messages so each primary span keeps its own text. The
    # validator above is what says the stream is well formed; this only
    # pairs a span with the message it came from.
    seen = []
    for rec in recs:
        msg = rec.get("message")
        if not isinstance(msg, dict) or msg.get("level") != "error":
            continue
        text = msg.get("rendered") or msg.get("message") or ""
        first = text.splitlines()[0] if text else "<no message>"
        for sp in msg.get("spans") or []:
            if not sp.get("is_primary"):
                continue
            rel, line = sp.get("file_name"), sp.get("line_start")
            full = os.path.join(root, rel)
            try:
                with open(full, encoding="utf-8") as fh:
                    src = fh.read().splitlines()[line - 1].strip()
            except (OSError, IndexError):
                src = "<unreadable>"
            seen.append((rel, line, first, src))
    for rel, line, text, src in sorted(seen):
        print(f"  {rel}:{line}")
        print(f"      compiler: {text}")
        print(f"      line now: {src}")
    print(f"  primary error spans: {len(seen)}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
