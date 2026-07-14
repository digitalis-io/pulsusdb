#!/usr/bin/env python3
# Copyright: run-once, human-invoked provenance/extraction script for issue
# #29 (M2 promql-parser validation spike). NOT invoked by CI — CI is
# Rust-only (crates/pulsus-promql/tests/upstream_parser_corpus.rs) and only
# ever reads the committed output of this script.
#
# Extracts the `testExpr` test table from the upstream Prometheus
# `promql/parser/parse_test.go` (the table `TestParseExpressions` iterates)
# into a deterministic, one-JSON-object-per-line corpus:
#   {"input": "...", "should_fail": bool, "err_substr": "...|null"}
#
# Why not just `go run` the table: the cases live in an unexported
# package-internal `_test.go` slice that no external program can import: Go
# does not expose `_test.go` symbols outside `go test` of that exact
# package, so the only portable extraction mechanism is a text extractor
# over the Go source. This script is a hand-rolled, comment/string-aware
# Go-source scanner (not a regex-only grep) specifically because a naive
# grep silently corrupts the corpus on:
#   - embedded PromQL braces inside Go string literals (e.g.
#     `` `+{"some_metric"}` `` — a brace that is NOT Go structural syntax);
#   - raw (backtick) vs interpreted (double-quoted) Go string literals,
#     which decode differently (escape sequences only apply to the latter);
#   - `input:` fields built by concatenating multiple literals with `+`,
#     including two occurrences of `strings.Repeat(<literal>, N)`.
#
# `err_substr` is best-effort/informational only (the harness never gates
# on it — promql-parser's error text does not match Prometheus's verbatim);
# it is the first `errors.New(...)`/`fmt.Errorf(...)` string literal found
# in a failing case, or null if none is found.
#
# Usage (re-vendor on a Prometheus or promql-parser version bump):
#   python3 extract-upstream-cases.py \
#       --tag v3.13.0 \
#       --sha 40af9c2cdc0eda00f3622e867a27f6359f7295f3 \
#       --promql-parser-version 0.10.0
#
# add --source-file <path> to reuse an already-downloaded parse_test.go
# instead of fetching it again over the network.

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import urllib.request
from pathlib import Path

CORPUS_DIR = Path(__file__).parent
CORPUS_FILE = CORPUS_DIR / "prometheus-v3.13-parse-cases.jsonl"
MANIFEST_FILE = CORPUS_DIR / "manifest.json"

# Sanity bounds asserted after extraction (edge-case-1 mitigation: "the
# extractor asserts the count against a documented expected value so a
# botched extraction is loud"). Measured against Prometheus v3.13.0's
# `testExpr` table at commit 40af9c2cdc0eda00f3622e867a27f6359f7295f3.
# `SEGMENTED_CASE_COUNT` is the raw number of `{ ... }` items in the Go
# slice literal; `EXPECTED_CASE_COUNT` is the corpus line count after
# excluding the 2 invalid-UTF-8 cases documented in PROVENANCE.md (not
# representable as a Rust `&str`).
SEGMENTED_CASE_COUNT = 351
EXPECTED_CASE_COUNT = 349


# ---------------------------------------------------------------------
# Go string-literal decoding.
# ---------------------------------------------------------------------


class InvalidUtf8Literal(Exception):
    """Raised when a decoded Go string literal is not valid UTF-8.

    Go strings are arbitrary byte sequences, and `\\xHH`/`\\NNN` (octal)
    escapes insert a single raw *byte*, not a Unicode code point — unlike
    `\\uXXXX`/`\\UXXXXXXXX`, which insert the UTF-8 encoding of a code
    point. The pinned corpus contains two cases (`\\xff` in a matcher value
    and in a `label_replace` argument) that deliberately construct an
    invalid UTF-8 byte to test Prometheus's lexer-level UTF-8 validation.
    That check has no Rust equivalent to test: `promql_parser::parser::
    parse` takes `&str`, and Rust's type system already guarantees `&str`
    is valid UTF-8, so invalid-UTF-8 input cannot even be constructed to
    pass to it. These cases are therefore excluded from the corpus (see
    PROVENANCE.md), not silently mis-decoded as a different (valid) code
    point, which would fabricate a false divergence."""

    def __init__(self, raw: bytes):
        super().__init__(f"decoded literal is not valid UTF-8: {raw!r}")
        self.raw = raw


def decode_go_interpreted_string_bytes(body: str) -> bytes:
    """Decodes the body (without surrounding quotes) of a Go interpreted
    (double-quoted) string literal into the raw bytes it denotes: \\a \\b
    \\f \\n \\r \\t \\v \\\\ \\' \\" \\xHH \\NNN (octal) \\uXXXX
    \\UXXXXXXXX. `\\xHH`/octal escapes are single raw bytes (per the Go
    spec); `\\u`/`\\U` escapes are UTF-8-encoded code points; everything
    else is the source text's own UTF-8 bytes."""
    out = bytearray()
    i = 0
    n = len(body)
    simple = {
        "a": 0x07,
        "b": 0x08,
        "f": 0x0C,
        "n": 0x0A,
        "r": 0x0D,
        "t": 0x09,
        "v": 0x0B,
        "\\": 0x5C,
        "'": 0x27,
        '"': 0x22,
    }
    while i < n:
        c = body[i]
        if c != "\\":
            out += c.encode("utf-8")
            i += 1
            continue
        i += 1
        if i >= n:
            raise ValueError(f"truncated escape in {body!r}")
        e = body[i]
        if e in simple:
            out.append(simple[e])
            i += 1
        elif e == "x":
            out.append(int(body[i + 1 : i + 3], 16))
            i += 3
        elif e == "u":
            out += chr(int(body[i + 1 : i + 5], 16)).encode("utf-8")
            i += 5
        elif e == "U":
            out += chr(int(body[i + 1 : i + 9], 16)).encode("utf-8")
            i += 9
        elif e.isdigit():
            out.append(int(body[i : i + 3], 8) & 0xFF)
            i += 3
        else:
            raise ValueError(f"unknown escape \\{e} in {body!r}")
    return bytes(out)


def decode_string_literal_bytes(tok: str) -> bytes:
    tok = tok.strip()
    if tok.startswith('"'):
        if not tok.endswith('"'):
            raise ValueError(f"unterminated interpreted string: {tok!r}")
        return decode_go_interpreted_string_bytes(tok[1:-1])
    if tok.startswith("`"):
        if not tok.endswith("`"):
            raise ValueError(f"unterminated raw string: {tok!r}")
        # Raw strings apply no escape processing at all — the source text
        # (already UTF-8, since the Go source file is) is verbatim.
        return tok[1:-1].encode("utf-8")
    raise ValueError(f"not a string literal: {tok!r}")


def decode_string_literal(tok: str) -> str:
    raw = decode_string_literal_bytes(tok)
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as e:
        raise InvalidUtf8Literal(raw) from e


REPEAT_RE = re.compile(r"^strings\.Repeat\(\s*(.+?)\s*,\s*(\d+)\s*\)$", re.DOTALL)

# `fmt.Sprintf` is used in exactly two `input:` fields in the pinned
# revision (both `@`-modifier edge cases, out of the M2 subset regardless
# of decoding): formatting `float64(math.MaxInt64)+1` and
# `float64(math.MinInt64)-1` with Go's `%f` (6 decimal places). A general
# Go expression evaluator is out of scope for this extractor, so these two
# are hand-computed once (verified against Go's `%f` float formatting) and
# matched by exact source text. Any other `fmt.Sprintf` usage in a future
# re-vendor is an unhandled-pattern error (loud, not silent).
KNOWN_SPRINTF_VALUES = {
    "fmt.Sprintf(`foo @ %f`, float64(math.MaxInt64)+1)": "foo @ 9223372036854775808.000000",
    "fmt.Sprintf(`foo @ %f`, float64(math.MinInt64)-1)": "foo @ -9223372036854775808.000000",
}


def split_top_level(text: str, sep: str) -> list[str]:
    """Splits `text` on `sep`, skipping occurrences inside string literals
    or inside parens (so a comma inside `strings.Repeat(...)`'s argument
    list doesn't split an outer `+`-joined expression)."""
    parts = []
    buf: list[str] = []
    i = 0
    n = len(text)
    paren_depth = 0
    while i < n:
        c = text[i]
        if c == '"':
            j = i + 1
            while j < n:
                if text[j] == "\\":
                    j += 2
                    continue
                if text[j] == '"':
                    j += 1
                    break
                j += 1
            buf.append(text[i:j])
            i = j
            continue
        if c == "`":
            j = text.index("`", i + 1) + 1
            buf.append(text[i:j])
            i = j
            continue
        if c == "(":
            paren_depth += 1
        elif c == ")":
            paren_depth -= 1
        if paren_depth == 0 and text[i : i + len(sep)] == sep:
            parts.append("".join(buf))
            buf = []
            i += len(sep)
            continue
        buf.append(c)
        i += 1
    parts.append("".join(buf))
    return parts


def decode_value_expr(expr: str) -> str:
    """Decodes an `input:` field's Go expression: one or more string
    literals (interpreted or raw) and/or `strings.Repeat(<literal>, N)`
    calls, joined by `+`. Builds raw bytes first and decodes once at the
    end, so a concatenation spanning a raw + interpreted literal is
    checked for UTF-8 validity as a whole (see [`InvalidUtf8Literal`])."""
    out = bytearray()
    for term in split_top_level(expr, "+"):
        term = term.strip()
        m = REPEAT_RE.match(term)
        if m:
            lit, count = m.group(1), int(m.group(2))
            out += decode_string_literal_bytes(lit) * count
            continue
        if term in KNOWN_SPRINTF_VALUES:
            out += KNOWN_SPRINTF_VALUES[term].encode("utf-8")
            continue
        out += decode_string_literal_bytes(term)
    try:
        return bytes(out).decode("utf-8")
    except UnicodeDecodeError as e:
        raise InvalidUtf8Literal(bytes(out)) from e


# ---------------------------------------------------------------------
# Go-source scanning (comment/string-aware brace matching).
# ---------------------------------------------------------------------


def find_slice_body(text: str) -> str:
    """Returns the source text strictly between the outer `{` and `}` of
    `var testExpr = []struct { ... }{ <body> }`."""
    marker = "var testExpr = []struct {"
    start = text.index(marker)
    i = text.index("{", start)
    depth = 0
    struct_type_end = None
    j = i
    n = len(text)
    while j < n:
        c = text[j]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                struct_type_end = j
                break
        j += 1
    if struct_type_end is None:
        raise ValueError("could not find end of testExpr's struct-type field list")
    k = struct_type_end + 1
    if text[k] != "{":
        raise ValueError(f"expected slice literal to open right after struct type, got {text[k:k+20]!r}")
    depth = 0
    body_start = k + 1
    m = k
    while m < n:
        c = text[m]
        if c == '"':
            p = m + 1
            while p < n:
                if text[p] == "\\":
                    p += 2
                    continue
                if text[p] == '"':
                    p += 1
                    break
                p += 1
            m = p
            continue
        if c == "`":
            m = text.index("`", m + 1) + 1
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return text[body_start:m]
        m += 1
    raise ValueError("did not find end of testExpr slice literal")


def split_top_level_cases(body: str) -> list[str]:
    """Segments the `testExpr` slice-literal body into one raw source
    string per `{ ... }` case item, skipping over Go comment and string
    content so embedded PromQL braces never confuse the depth count."""
    cases = []
    i = 0
    n = len(body)
    depth = 0
    case_start = None
    while i < n:
        c = body[i]
        if c == "/" and i + 1 < n and body[i + 1] == "/":
            i = body.index("\n", i)
            continue
        if c == "/" and i + 1 < n and body[i + 1] == "*":
            i = body.index("*/", i) + 2
            continue
        if c == '"':
            j = i + 1
            while j < n:
                if body[j] == "\\":
                    j += 2
                    continue
                if body[j] == '"':
                    j += 1
                    break
                j += 1
            i = j
            continue
        if c == "`":
            i = body.index("`", i + 1) + 1
            continue
        if c == "{":
            if depth == 0:
                case_start = i
            depth += 1
            i += 1
            continue
        if c == "}":
            depth -= 1
            if depth == 0:
                cases.append(body[case_start : i + 1])
            i += 1
            continue
        i += 1
    return cases


def extract_case_field_expr(case_src: str, field: str) -> str | None:
    """Finds `field:` in `case_src` and returns the raw Go expression text
    up to (not including) the next top-level comma (outside strings/parens)."""
    m = re.search(rf"\b{field}\s*:\s*", case_src)
    if not m:
        return None
    rest = case_src[m.end() :]
    i = 0
    n = len(rest)
    paren_depth = 0
    while i < n:
        c = rest[i]
        if c == '"':
            j = i + 1
            while j < n:
                if rest[j] == "\\":
                    j += 2
                    continue
                if rest[j] == '"':
                    j += 1
                    break
                j += 1
            i = j
            continue
        if c == "`":
            i = rest.index("`", i + 1) + 1
            continue
        if c == "(":
            paren_depth += 1
        elif c == ")":
            paren_depth -= 1
        elif c == "," and paren_depth == 0:
            return rest[:i]
        i += 1
    raise ValueError(f"unterminated {field} field in case starting {case_src[:80]!r}")


ERR_LITERAL_RE = re.compile(r"(`[^`]*`|\"(?:[^\"\\]|\\.)*\")")


def extract_err_substr(case_src: str) -> str | None:
    """Best-effort, informational-only first error message: the first
    string literal argument to `errors.New(...)` or `fmt.Errorf(...)` in
    this case's source. Never used by the harness to gate a divergence."""
    for fn in ("errors.New(", "fmt.Errorf("):
        idx = case_src.find(fn)
        if idx == -1:
            continue
        m = ERR_LITERAL_RE.search(case_src, idx + len(fn))
        if m:
            return decode_string_literal(m.group(1))
    return None


def extract_cases(text: str) -> tuple[list[dict], int]:
    """Returns `(cases, segmented_count)`. `segmented_count` is the raw
    number of `{ ... }` items found in the `testExpr` slice literal
    (asserted against [`SEGMENTED_CASE_COUNT`]); `cases` may be shorter,
    since cases whose `input` decodes to invalid UTF-8 are skipped (see
    [`InvalidUtf8Literal`]) rather than corrupting the JSON corpus."""
    body = find_slice_body(text)
    raw_cases = split_top_level_cases(body)
    cases = []
    skipped = 0
    for raw in raw_cases:
        input_expr = extract_case_field_expr(raw, "input")
        if input_expr is None:
            raise ValueError(f"case with no input field: {raw[:120]!r}")
        try:
            input_val = decode_value_expr(input_expr)
        except InvalidUtf8Literal as e:
            skipped += 1
            print(
                f"SKIP (not representable as a Rust &str): input expr {input_expr!r} -> {e}",
                file=sys.stderr,
            )
            continue

        fail_expr = extract_case_field_expr(raw, "fail")
        should_fail = fail_expr is not None and fail_expr.strip() == "true"

        cases.append(
            {
                "input": input_val,
                "should_fail": should_fail,
                "err_substr": extract_err_substr(raw) if should_fail else None,
            }
        )
    if skipped:
        print(f"skipped {skipped} case(s) with invalid-UTF-8 input (see PROVENANCE.md)", file=sys.stderr)
    return cases, len(raw_cases)


# ---------------------------------------------------------------------
# CLI.
# ---------------------------------------------------------------------


def fetch_source(tag: str, sha: str) -> str:
    url = f"https://raw.githubusercontent.com/prometheus/prometheus/{sha}/promql/parser/parse_test.go"
    print(f"fetching {url}", file=sys.stderr)
    with urllib.request.urlopen(url, timeout=30) as resp:  # noqa: S310 (pinned, human-invoked, HTTPS)
        return resp.read().decode("utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tag", default="v3.13.0", help="Prometheus git tag (for PROVENANCE/manifest)")
    ap.add_argument(
        "--sha",
        default="40af9c2cdc0eda00f3622e867a27f6359f7295f3",
        help="Prometheus commit SHA the tag resolves to (pinned, not re-resolved at run time)",
    )
    ap.add_argument("--promql-parser-version", default="0.10.0")
    ap.add_argument(
        "--source-file",
        type=Path,
        default=None,
        help="reuse an already-downloaded parse_test.go instead of fetching",
    )
    args = ap.parse_args()

    text = args.source_file.read_text(encoding="utf-8") if args.source_file else fetch_source(args.tag, args.sha)

    cases, segmented_count = extract_cases(text)
    print(f"segmented {segmented_count} top-level case items, extracted {len(cases)} corpus lines", file=sys.stderr)
    if segmented_count != SEGMENTED_CASE_COUNT:
        print(
            f"FATAL: segmented case count {segmented_count} != expected {SEGMENTED_CASE_COUNT} "
            "(script's own documented sanity bound) — extraction is likely corrupted, "
            "not silently accepting this",
            file=sys.stderr,
        )
        sys.exit(1)
    if len(cases) != EXPECTED_CASE_COUNT:
        print(
            f"FATAL: extracted corpus line count {len(cases)} != expected {EXPECTED_CASE_COUNT} "
            "(script's own documented sanity bound) — extraction is likely corrupted, "
            "not silently accepting this",
            file=sys.stderr,
        )
        sys.exit(1)

    jsonl = "\n".join(json.dumps(c, ensure_ascii=False) for c in cases) + "\n"
    jsonl_bytes = jsonl.encode("utf-8")
    sha256 = hashlib.sha256(jsonl_bytes).hexdigest()
    fail_count = sum(1 for c in cases if c["should_fail"])
    print(f"should_fail=true: {fail_count} / {len(cases)}", file=sys.stderr)
    print(f"sha256: {sha256}", file=sys.stderr)

    CORPUS_FILE.write_bytes(jsonl_bytes)
    manifest = {
        "prometheus_tag": args.tag,
        "prometheus_sha": args.sha,
        "promql_parser_version": args.promql_parser_version,
        "case_count": len(cases),
        "sha256": sha256,
    }
    MANIFEST_FILE.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote {CORPUS_FILE} and {MANIFEST_FILE}", file=sys.stderr)


if __name__ == "__main__":
    main()
