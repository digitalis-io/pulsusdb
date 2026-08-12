# Issue #335 Stage D0 — the conformance evidence debt may only SHRINK.
#
# A `supported` disposition is a claim that a construct works. A claim with
# no witness is the shape this repository keeps paying for: `static.kind_enum`
# declared six span-kind spellings, probed one, carried `evidence: []`, and
# was dispositioned `supported` — and one of the five unprobed spellings was
# a value the reference accepts and our parser refused.
#
# BOTH SIDES ARE DERIVED FROM THE DATA. This is a SET DIFFERENCE against
# origin/main, not an allowlist and not a count. That matters for two
# constructions an earlier cut admitted:
#   * an allowlist plus a count is an EXPANDABLE allowlist — a new debt row
#     lands by bumping the number;
#   * a count alone cannot see a compensating swap — one row gains evidence
#     while another arrives without it, and the total never moves.
# A set difference has neither construction: any arrival is named.
#
# $old is `git show origin/main:<dispositions.json>` slurped in.
#
# Output: the constructs whose `supported`-with-no-evidence status is NEW on
# this branch, newline-joined. THE FILTER MUST ALWAYS PRODUCE EXACTLY ONE
# VALUE — hence `join`, not `.[]`. Under `jq -e` a filter that emits nothing
# exits **4**, which the caller reports as GATE ERROR, so ending in `.[]`
# made the PASS case (an empty difference) look like a broken gate. Measured
# while replaying this job against rebuilt git states: control rc=4 → GATE
# ERROR. A string is truthy in jq, including the empty one, so the joined
# form exits 0 whether the difference is empty or not, and the caller decides
# on the TEXT. A compile error still exits 3 and a runtime error 5, both of
# which the caller reports as GATE ERROR rather than swallowing.

def debt:
  [ .entries[]
    | select(.status == "supported" and ((.evidence // []) | length) == 0)
    | .construct ];

($old[0] | debt) as $main
| (debt - $main)
| sort
| join("\n")
