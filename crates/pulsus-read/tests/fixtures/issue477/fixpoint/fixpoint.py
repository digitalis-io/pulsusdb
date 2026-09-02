#!/usr/bin/env python3
"""Issue #477 Q26/Q28 — the compiler-driven rename instrument, and the
well-formedness rules that bound what it will act on.

This is the instrument that produced AC10(i)'s 44/17 compiler partition:
it reads a `cargo ... --message-format=json` stream, and rewrites the
banned spelling at every error-level primary span the compiler reported,
iterating to a fixpoint. AC10(i)'s argument rests on what this reports,
so what it will REFUSE to act on is part of the claim.

W1..W6 are validated before anything is written. A stream that fails any
of them is refused whole (exit 2), the tree is left byte-identical and no
ledger is produced -- a partial rewrite driven by a malformed stream is
the failure mode this exists to prevent.

  W1  every line parses as a JSON object carrying a `reason`
  W2  every `reason` is one cargo actually emits
  W3  exactly one `build-finished`, and it is the last record
  W4  every `compiler-message` carries a well-shaped `message` and every
      span carries `file_name`, `line_start`, `is_primary`, `level`
  W5  `build-finished.success` agrees with the ERROR census, and a failed
      build carries at least one error with a primary span
  W6  every error-level primary span path has no parent component, stays
      inside the supplied root, is a Rust source, and names a line that
      file has

Usage:
  fixpoint.py validate <stream.json> <root>
  fixpoint.py rename   <stream.json> <root> <ledger.tsv> <old> <new>

`validate` prints the census and exits 0, or prints `rule=Wn
reason=REASON` and exits 2. `rename` validates first and refuses the same
way; nothing is written on a refusal.
"""

import json
import os
import sys

REASONS = {
    "compiler-artifact",
    "compiler-message",
    "build-script-executed",
    "build-finished",
}


class Refused(Exception):
    def __init__(self, rule, reason):
        super().__init__(f"rule={rule} reason={reason}")
        self.rule = rule
        self.reason = reason


def records(text):
    """W1/W2: every line is a JSON object with a reason cargo emits."""
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        raise Refused("W1", "EMPTY-STREAM")
    out = []
    for ln in lines:
        try:
            rec = json.loads(ln)
        except ValueError:
            raise Refused("W1", "LINE-IS-NOT-JSON")
        if not isinstance(rec, dict) or "reason" not in rec:
            raise Refused("W1", "RECORD-CARRIES-NO-REASON")
        if rec["reason"] not in REASONS:
            raise Refused("W2", "UNKNOWN-REASON")
        out.append(rec)
    return out


def finished(recs):
    """W3: exactly one build-finished, and it is last."""
    at = [i for i, r in enumerate(recs) if r["reason"] == "build-finished"]
    if len(at) != 1:
        raise Refused("W3", "BUILD-FINISHED-RECORD-COUNT")
    if at[0] != len(recs) - 1:
        raise Refused("W3", "BUILD-FINISHED-IS-NOT-THE-LAST-RECORD")
    rec = recs[at[0]]
    if not isinstance(rec.get("success"), bool):
        raise Refused("W3", "BUILD-FINISHED-CARRIES-NO-SUCCESS")
    return rec["success"]


def spans(recs):
    """W4: the message and span shape. Returns (errors, primary_spans)."""
    errors = 0
    primary = []
    for rec in recs:
        if rec["reason"] != "compiler-message":
            continue
        msg = rec.get("message")
        if not isinstance(msg, dict) or not isinstance(msg.get("level"), str):
            raise Refused("W4", "COMPILER-MESSAGE-SHAPE")
        if not isinstance(msg.get("spans"), list):
            raise Refused("W4", "COMPILER-MESSAGE-SHAPE")
        is_error = msg["level"] == "error"
        errors += 1 if is_error else 0
        for sp in msg["spans"]:
            if not isinstance(sp, dict):
                raise Refused("W4", "PRIMARY-SPAN-SHAPE")
            name, line, is_primary = (
                sp.get("file_name"),
                sp.get("line_start"),
                sp.get("is_primary"),
            )
            # `isinstance(True, int)` is true in Python, so the line
            # number is checked for being an int AND not a bool.
            if (
                not isinstance(name, str)
                or not isinstance(line, int)
                or isinstance(line, bool)
                or not isinstance(is_primary, bool)
            ):
                raise Refused("W4", "PRIMARY-SPAN-SHAPE")
            if is_error and is_primary:
                primary.append((name, line))
    return errors, primary


def census(recs, ok, errors, primary):
    """W5: success agrees with the error census."""
    if ok and errors:
        raise Refused("W5", "BUILD-SUCCEEDED-BUT-ERRORS-WERE-REPORTED")
    if not ok and errors == 0:
        raise Refused("W5", "BUILD-FAILED-WITH-NO-ERROR-MESSAGE")
    if not ok and not primary:
        raise Refused("W5", "BUILD-FAILED-WITH-NO-PRIMARY-ERROR-SPAN")


def confine(primary, root):
    """W6: the paths the rewrite would touch."""
    root = os.path.realpath(root)
    resolved = []
    for name, line in primary:
        if os.path.isabs(name):
            raise Refused("W6", "PRIMARY-SPAN-PATH-IS-ABSOLUTE")
        if ".." in name.replace("\\", "/").split("/"):
            raise Refused("W6", "PRIMARY-SPAN-PATH-HAS-PARENT-COMPONENT")
        full = os.path.realpath(os.path.join(root, name))
        if full != root and not full.startswith(root + os.sep):
            raise Refused("W6", "PRIMARY-SPAN-PATH-ESCAPES-ROOT")
        if not full.endswith(".rs"):
            raise Refused("W6", "PRIMARY-SPAN-PATH-IS-NOT-A-RUST-SOURCE")
        if not os.path.isfile(full):
            raise Refused("W6", "PRIMARY-SPAN-PATH-IS-NOT-A-FILE")
        with open(full, encoding="utf-8") as fh:
            count = len(fh.read().splitlines())
        if line < 1 or line > count:
            raise Refused("W6", "PRIMARY-SPAN-LINE-OUT-OF-RANGE")
        resolved.append((full, name, line))
    return resolved


def validate(stream_path, root):
    with open(stream_path, encoding="utf-8") as fh:
        recs = records(fh.read())
    ok = finished(recs)
    errors, primary = spans(recs)
    census(recs, ok, errors, primary)
    resolved = confine(primary, root)
    return ok, errors, resolved


def main(argv):
    if len(argv) < 4:
        print(__doc__)
        return 64
    mode, stream_path, root = argv[1], argv[2], argv[3]
    try:
        ok, errors, resolved = validate(stream_path, root)
    except Refused as refusal:
        # Nothing is printed but the refusal: a figure printed beside a
        # rejection is a figure someone will quote.
        print(f"{refusal}")
        return 2
    if mode == "validate":
        print(f"success={ok} error_msgs={errors} error_sites={len(resolved)}")
        return 0
    if mode != "rename":
        print(f"unknown mode {mode!r}")
        return 64
    if len(argv) < 7:
        print("rename needs <ledger.tsv> <old> <new>")
        return 64
    ledger_path, old, new = argv[4], argv[5], argv[6]
    renamed = 0
    rows = []
    for full, rel, line in resolved:
        with open(full, encoding="utf-8") as fh:
            lines = fh.read().splitlines(keepends=True)
        before = lines[line - 1]
        after = before.replace(old, new)
        if after != before:
            lines[line - 1] = after
            with open(full, "w", encoding="utf-8") as fh:
                fh.write("".join(lines))
            renamed += 1
        rows.append(f"{rel}\t{line}\t{'renamed' if after != before else 'no-op'}")
    with open(ledger_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(sorted(rows)))
        if rows:
            fh.write("\n")
    print(f"success={ok} error_msgs={errors} error_sites={len(resolved)} occurrences_renamed={renamed}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
