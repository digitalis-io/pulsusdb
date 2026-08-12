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
# Output: the constructs whose `supported`-with-no-evidence status is NEW on
# this branch. Empty output (a `false` result under `jq -e`) is the pass.

def debt:
  [ .entries[]
    | select(.status == "supported" and ((.evidence // []) | length) == 0)
    | .construct ];

($old[0] | debt) as $main
| (debt - $main)
| sort
| .[]
