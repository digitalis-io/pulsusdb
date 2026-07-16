#!/usr/bin/env python3
"""Run-once, human-invoked extractor for the vendored PromQL registry
(`registry-v3.13.json` + `registry-manifest.json`). NOT wired into CI —
CI is Rust-only; `tests/function_coverage.rs` only ever reads the files
this script wrote, re-verifying their SHA-256 first.

Sources, fetched at the pinned Prometheus tag/SHA:
  - `promql/parser/functions.go` — the function registry (name, arg
    types, variadic, return type, experimental flag);
  - `promql/parser/lex.go` — the aggregation-operator keywords (the
    "Aggregators." block of the keyword map) and the experimental pair
    (`IsExperimentalAggregator`: limitk, limit_ratio).

Re-run on a Prometheus reference-version bump (the re-vendor rule,
docs/architecture.md §5.1):

    python3 crates/pulsus-promql/tests/promqltest/coverage/extract-registry.py \
        --tag v3.13.0 \
        --sha 40af9c2cdc0eda00f3622e867a27f6359f7295f3

then update `PROVENANCE.md` and re-run
`cargo test -p pulsus-promql --test function_coverage`.
"""

import argparse
import hashlib
import json
import pathlib
import re
import sys
import urllib.request

RAW = "https://raw.githubusercontent.com/prometheus/prometheus/{sha}/{path}"

VALUE_TYPES = {
    "parser.ValueTypeVector": "vector",
    "parser.ValueTypeScalar": "scalar",
    "parser.ValueTypeMatrix": "matrix",
    "parser.ValueTypeString": "string",
    "ValueTypeVector": "vector",
    "ValueTypeScalar": "scalar",
    "ValueTypeMatrix": "matrix",
    "ValueTypeString": "string",
}


def fetch(sha: str, path: str) -> str:
    url = RAW.format(sha=sha, path=path)
    sys.stderr.write(f"fetching {url}\n")
    with urllib.request.urlopen(url) as resp:
        return resp.read().decode("utf-8")


def parse_functions(src: str) -> list[dict]:
    """Parses the `Functions = map[string]*Function{...}` literal."""
    entries = re.findall(r'"([a-zA-Z_0-9]+)":\s*\{(.*?)\n\t\}', src, re.S)
    if not entries:
        raise SystemExit("no registry entries found in functions.go — layout changed?")
    out = []
    for name, body in entries:
        m = re.search(r"Name:\s*\"([^\"]+)\"", body)
        if not m or m.group(1) != name:
            raise SystemExit(f"registry key {name!r} does not match its Name field")
        arg_types = []
        m = re.search(r"ArgTypes:\s*\[\]ValueType\{([^}]*)\}", body)
        if m:
            for tok in m.group(1).split(","):
                tok = tok.strip()
                if not tok:
                    continue
                if tok not in VALUE_TYPES:
                    raise SystemExit(f"unknown ValueType {tok!r} in {name}")
                arg_types.append(VALUE_TYPES[tok])
        variadic = 0
        m = re.search(r"Variadic:\s*(-?\d+)", body)
        if m:
            variadic = int(m.group(1))
        m = re.search(r"ReturnType:\s*(\S+?),", body)
        if not m or m.group(1) not in VALUE_TYPES:
            raise SystemExit(f"missing/unknown ReturnType in {name}")
        return_type = VALUE_TYPES[m.group(1)]
        experimental = bool(re.search(r"Experimental:\s*true", body))
        out.append(
            {
                "name": name,
                "arg_types": arg_types,
                "variadic": variadic,
                "return_type": return_type,
                "experimental": experimental,
            }
        )
    out.sort(key=lambda f: f["name"])
    return out


def parse_aggregators(src: str) -> list[dict]:
    """Parses the keyword map's "// Aggregators." block and the
    IsExperimentalAggregator pair from lex.go."""
    m = re.search(r"// Aggregators\.\n(.*?)\n\n", src, re.S)
    if not m:
        raise SystemExit("no '// Aggregators.' block found in lex.go — layout changed?")
    names = re.findall(r'"([a-z_0-9]+)":\s*[A-Z_]+,', m.group(1))
    if not names:
        raise SystemExit("no aggregator keywords parsed from lex.go")

    exp_m = re.search(
        r"func \(i ItemType\) IsExperimentalAggregator\(\) bool \{\n\treturn ([^\n]+)\n\}",
        src,
    )
    if not exp_m:
        raise SystemExit("IsExperimentalAggregator not found in lex.go — layout changed?")
    exp_tokens = set(re.findall(r"i == ([A-Z_]+)", exp_m.group(1)))
    token_by_name = dict(re.findall(r'"([a-z_0-9]+)":\s*([A-Z_]+),', m.group(1)))
    experimental_names = {n for n, tok in token_by_name.items() if tok in exp_tokens}
    if not experimental_names:
        raise SystemExit("no experimental aggregators resolved — layout changed?")

    return [
        {"name": n, "experimental": n in experimental_names} for n in sorted(names)
    ]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", required=True)
    ap.add_argument("--sha", required=True)
    args = ap.parse_args()

    functions_go = fetch(args.sha, "promql/parser/functions.go")
    lex_go = fetch(args.sha, "promql/parser/lex.go")

    functions = parse_functions(functions_go)
    aggregators = parse_aggregators(lex_go)

    registry = {
        "prometheus_tag": args.tag,
        "prometheus_sha": args.sha,
        "functions": functions,
        "aggregation_operators": aggregators,
    }

    here = pathlib.Path(__file__).resolve().parent
    registry_path = here / "registry-v3.13.json"
    registry_text = json.dumps(registry, indent=2, sort_keys=False) + "\n"
    registry_path.write_text(registry_text)

    manifest = {
        "prometheus_tag": args.tag,
        "prometheus_sha": args.sha,
        "sha256": hashlib.sha256(registry_text.encode()).hexdigest(),
        "function_count": len(functions),
        "experimental_function_count": sum(f["experimental"] for f in functions),
        "aggregation_operator_count": len(aggregators),
    }
    (here / "registry-manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n"
    )

    sys.stderr.write(
        f"wrote {registry_path.name}: {manifest['function_count']} functions "
        f"({manifest['experimental_function_count']} experimental), "
        f"{manifest['aggregation_operator_count']} aggregation operators\n"
    )


if __name__ == "__main__":
    main()
