# M1 follow-up: late label hydration for broad selectors with small limits

> **Verdict: `not_material` at Tier-1/CI scale — closable, with a mandatory
> Tier-2 re-evaluation pointer (see "Tier-2 mandate" below).** This report
> reflects the **v7 validity-gate redesign** (architect plan v7, PASSed
> review): the two prior growth-curve shape gates (v6, keyed on
> `cpu_micros`; v5/v4, keyed on `read_bytes`) both resolved `inconclusive`
> because they encoded a-priori guesses about eager's/late's CPU growth
> curves that the measured data contradicted **in both directions for the
> same structural cause** (`log_streams`' sparse index not pruning
> granules at these breadths — see "Why growth-curve gates were the wrong
> instrument" below). v7 replaces them with three **direction-neutral
> measurement-trustworthiness gates** — correctness/identity, rep-stability,
> and cross-path storage-equality — none of which can be satisfied or
> failed by *which* variant wins. **The 2.0x client-wall decision gate is
> unchanged since v4.** All three validity gates pass on the regenerated
> full-tier run; the decision gate does not clear 2.0x for either B
> variant (measured `1.39x`/`1.53x` at breadth 50,000) — an honest
> `not_material` at this scale, not tuned toward any outcome.

Issue: [#35](https://github.com/digitalis-io/pulsusdb/issues/35). A
benchmark-first investigation into docs/schemas.md §3.2's stage 2
(hydration): does it hydrate every selector-matched stream's `labels`
before stage 3's `LIMIT`, and if so, is a late-hydration shape (derive the
cheap `service` set first, hydrate only the `LIMIT`'d result) materially
cheaper at realistic breadth? Evidence model: docs/schemas.md §9's two-tier
model (issue #16).

## What this proves, and what it doesn't

- **Proves:** (a) the current pipeline (`eager`) and two bench-local
  late-hydration prototypes (`late_idx`, `late_proj`) return **provably
  identical** results at every breadth — each variant independently derives
  its own `service` set, all three agree; **every path's own production
  `sql::stage3` output is asserted identical — not merely equal in count —
  to the fixed 100-stream result set the corpus generator itself
  computes**; and the late variants' ≤100-fingerprint label hydration is
  byte-equal to the eager variant's hydration of the same fingerprints —
  verified live at every committed breadth. (b) storage I/O is
  **provably strategy-invariant**: `resolution`/`samples` stage
  `read_bytes`/`selected_marks` are byte-identical across all three paths,
  and `eager.hydration_full` equals each late variant's `hydration_late`
  on the same two figures, at every breadth — so any wall/CPU delta below
  isolates the hydration *strategy*, not storage I/O. (c) the hydration
  stage's own CPU cost is real and large: `hydration_late.cpu_micros` is
  **23–29x lower** than `hydration_full`'s at breadth 50,000, with
  identical `read_rows`/`read_bytes`/`selected_marks` (storage layer
  blameless, exactly as the issue's own Context predicts). (d) that
  isolated win **dilutes to `1.39x`/`1.53x` end-to-end wall** at breadth
  50,000 — below the pinned 2.0x materiality threshold — because hydration
  is only part of total query cost; see "Dilution analysis" below for the
  exact breakdown.
- **Does not prove:** a Tier-1/CI-scale wall-clock material win (measured
  `1.39x`/`1.53x < 2.0x`) — see "Decision" for the no-change rationale and
  the mandatory Tier-2 (issue #25) re-evaluation this report requires
  before the question is closed for good.

## Harness

`cargo xtask bench logs-hydration` (`xtask/src/bench/logs_hydration/`), a
sibling scenario to `logs-read`/`metrics-labels` (keeps #16's committed
Tier-1 gates and #34's committed evidence byte-stable). No product
read-path change: every stage reuses `pulsus_read::logql::{plan, sql}`
unmodified; `service_set_from_idx`/`service_set_from_streams` (the two
service-derivation builders `sql` doesn't have) are the only bench-local
SQL. The correctness gate compares each path's own production
`sql::stage3` output directly — full result envelopes (fingerprint,
timestamp_ns, body, labels), cross-path within each breadth and
cross-breadth against the first breadth's reference envelope.

- **Corpus** (`xtask/src/bench/dataset.rs::load_broad_tier`) — one
  breadth-scoped database per pass: a **fixed** 100-stream result-bearing
  set (`HYDRATION_RESULT_STREAMS`, one sample each, newest timestamp band)
  whose construction does not depend on breadth — byte-identical
  fingerprints/labels/timestamps across every breadth — plus
  `breadth - 100` filler streams. `BroadTimestampBands` gives every stream
  its own disjoint per-slot time range (result slots sized off the
  constant `HYDRATION_RESULT_STREAMS`, filler slots off the live
  `filler_streams` count), so no jitter can cross either band boundary at
  any breadth, and every timestamp in one breadth pass is globally unique.
  All `breadth` streams share the single service `svc-broad` (deliberate
  single-service isolation — the entire eager-vs-late delta is the
  `labels` column).
- **Three paths** (`paths.rs::run_variant_once`) — `eager`: resolution →
  `hydration_full` (stage2, all `breadth` fps) → `samples` (stage3, LIMIT
  100). `late_idx`/`late_proj`: resolution → `service_idx`/`service_proj` →
  `samples` → `hydration_late` (stage2, ≤100 result fps).
- **6-round Latin-square rotation** — 1 discarded warm-up round + 6
  measured rounds, each running all three variants in one of the 6
  distinct permutations of `{eager, late_idx, late_proj}` (every variant
  occupies each position exactly twice).
- **RSS — parent-side windowed sampler** — 3 fresh child processes per
  variant×breadth, a `READY`/go-signal/`DONE` handshake; the parent polls
  `/proc/<child_pid>/status` `VmRSS` every 10 ms, attributing `rss_peak -
  rss_at_ready`. Diagnostic-only, non-gating — see "RSS" below.
- **Correctness gate** (`paths.rs::correctness_gate`) — mandatory, runs
  before any perf evidence is recorded for a breadth; asserts
  fingerprint-set **identity** (not cardinality) against the corpus
  generator's fixed expected set, for every path's own production
  `sql::stage3` output (`paths.rs::assert_result_set_identity`).

## The v7 validity gates (replace the growth-curve shape gates)

All three are checked once per full-tier run, at (or, for gate (c), across)
the breadth sweep, and are **direction-neutral**: none can be satisfied or
failed by which variant wins.

- **(a) Correctness/identity.** Always `true` for any breadth present in
  committed evidence — the correctness gate above is mandatory and aborts
  the whole run before any evidence is recorded if it ever fails.
- **(b) Rep-stability.** `max/median <= 2.0` for the decision-feeding
  `client_wall_ms` `Dist` of `eager`, `late_idx`, and `late_proj`, at
  breadth 50,000. Measured this run: `eager` `1.16`, `late_idx` `1.15`,
  `late_proj` `1.24` — comfortably inside the bound.
- **(c) Cross-path storage-equality.** At every breadth in the sweep:
  `resolution.{read_bytes,selected_marks}` and
  `samples.{read_bytes,selected_marks}` byte-identical across all three
  paths, and `eager.hydration_full.{read_bytes,selected_marks} ==
  late.hydration_late.{read_bytes,selected_marks}` for each late variant.
  Measured this run: holds at every breadth (`storage_equality_ok = true`)
  — see the table below.

| breadth | stage | read_bytes (all 3 paths) |
|---|---|---|
| 1,000 | resolution | 105,556 |
| 1,000 | samples | 176,890 |
| 1,000 | hydration (full/late) | 103,556 |
| 10,000 | resolution | 460,985 |
| 10,000 | samples | 1,778,890 |
| 10,000 | hydration (full/late) | 1,045,556 |
| 50,000 | resolution | 1,639,747 |
| 50,000 | samples | 8,938,890 |
| 50,000 | hydration (full/late) | 5,272,225 |

All three gates passed on the regenerated full-tier artifact — the
measurement is trustworthy, and the decision gate is reached.

## Why growth-curve gates were the wrong instrument (v4–v6 history)

Both prior predicate versions resolved `inconclusive`, for the same root
cause. v5/v4 gated `hydration_full`/`hydration_late`'s `read_bytes`
growth; v6 re-keyed to `cpu_micros`. Both failed because `log_streams`
(`ORDER BY fingerprint`, default `index_granularity = 8192`) cannot skip
granules for a 100-value `IN (...)` predicate at this investigation's
breadths — the whole per-breadth corpus fits in 1–7 granules, so **every**
granule is touched regardless of `IN (...)` list size (confirmed live via
`EXPLAIN indexes = 1` and a controlled 2,022,000-row/247-granule probe).
`hydration_late.read_bytes` is therefore byte-identical to
`hydration_full`'s at every breadth (see the storage-equality table
above) — which the v5/v4 gate misread as "not bounded" (failing) and
which also flattens `cpu_micros`' growth curve away from either gate's
a-priori linear/bounded shape guess, in both directions, from the *same*
underlying cause. No threshold retune fixes a structurally-wrong
instrument; v7 stopped trying to shape-fit the growth curve and instead
gates on whether the *measurement itself* — correctness, dispersion,
storage isolation — is trustworthy enough to believe the one metric that
was never in question: the 2.0x client-wall decision gate.

## Comparative result

Medians over 6 measured rounds (`client_wall_ms`, `hydration_*.cpu_micros`)
at breadth 50,000; full distributions in the committed JSON.

| path | hydration cpu_micros | resolution cpu_micros | samples cpu_micros | client_wall_ms |
|---|---|---|---|---|
| eager | 172,806.5 | 21,610.5 | 105,657.0 | 437.5 |
| late_idx | 5,931.5 | 22,622.0 | 92,745.5 | 315.2 |
| late_proj | 7,517.0 | 22,054.5 | 103,747.5 | 286.2 |

`hydration_late.cpu_micros` is **29.1x** lower than `hydration_full`'s for
`late_idx`, **23.0x** lower for `late_proj` — real, large, and storage-I/O
isolated (validity gate (c)). `resolution`/`samples` CPU is effectively
unchanged across paths (as expected — they run the identical SQL text
against the identical resolved-fingerprint set regardless of hydration
strategy). `client_wall_ms`: eager/late_idx = **1.39x**, eager/late_proj =
**1.53x** — both below the 2.0x decision threshold.

## Dilution analysis: why a 23–29x hydration win is only ~1.4–1.5x wall

At breadth 50,000, eager's own total measured CPU across its three stages
is `21,610.5 (resolution) + 105,657.0 (samples) + 172,806.5 (hydration) =
300,074.0` micros. Hydration is **57.6%** of that total; `resolution` +
`samples` together are **42.4%** — and that 42.4% is **O(breadth)**,
storage-I/O-bound, and **identical across all three paths** (validity gate
(c)): a broad selector's stage-1 resolution and stage-3 sample scan cost
the same whether hydration is eager or late, because both stages already
run before/independently of the hydration strategy. Late hydration's 23–29x
reduction applies only to the 57.6% hydration share — cutting effective
total CPU to roughly `42.4% + 57.6%/25 ≈ 44.7%` of eager's, a ~2.2x
*CPU*-level improvement — but **end-to-end wall time is not CPU alone**:
connection/query-dispatch overhead, network round trips, and ClickHouse's
own per-query scheduling are shared, largely fixed costs that do not
shrink with the hydration win, further compressing the wall-visible
improvement down to the measured `1.39x`/`1.53x`. This is a direct,
quantified explanation for why a real, large, storage-isolated CPU win
(proven by validity gate (c) plus the isolated hydration figures above)
does not clear the wall-clock materiality bar at Tier-1/CI scale — not a
contradiction, and not evidence the win is illusory.

## RSS: diagnostic-only, not gating

Every `client_rss_delta_kib` cell in the committed artifacts carries
`rss_suspect = true` (the [R6] sane-band check: `rss_delta_kib * 1024`
falls outside `[0.25x, 4x]` of the ~30 KB decoded envelope payload — every
measurement here is far above that band, roughly 1–20 MB, dwarfed by
per-process overhead — connection setup, Tokio runtime allocations,
allocator fragmentation — inside the same 10 ms `/proc/<pid>/status`
sampling window). RSS never enters the v7 validity gates or the decision
gate; every RSS-backed claim renders in `render_markdown`'s per-path table
as `rss_claim = inconclusive (suspect measurement)`. No memory-inflation
claim is made or corroborated by this report.

## Decision

**Recorded verdict: `not_material`.** All three v7 validity gates pass
(`identity_ok = true`, `rep_stability_ok = true`, `storage_equality_ok =
true`) — the measurement is trustworthy and storage-isolated. Neither B
variant clears the unchanged 2.0x client-wall decision gate at breadth
50,000 (`late_idx` `1.39x`, `late_proj` `1.53x`, both `< 2.0x`).

**No-change rationale (this report is the decision record for the
Tier-1/CI-scale question; the official closeout comment is
[#35#issuecomment-4977904814](https://github.com/digitalis-io/pulsusdb/issues/35#issuecomment-4977904814),
recorded in `closeout.comment_url` of both committed
`logs-hydration-{ci,full}.json`):**
1. The hydration-stage CPU reduction is **real and evidence-backed**:
   `hydration_late.cpu_micros` is `23–29x` lower than `hydration_full`'s
   at breadth 50,000, at byte-identical `read_bytes`/`selected_marks`
   (validity gate (c) proves storage I/O is strategy-invariant, so this
   delta is attributable to hydration strategy alone) — matching the
   issue's own Context ("CPU-bound single-threaded JSON parsing… the
   storage layer blameless") precisely.
2. It is **non-decisive at Tier-1/CI scale**: total query CPU is `57.6%`
   hydration, `42.4%` O(breadth) `resolution`+`samples` that late
   hydration does not touch and that cost the same regardless of
   strategy — diluting a `23–29x` hydration-stage win to `1.39x`/`1.53x`
   end-to-end wall (see "Dilution analysis" above), below the pinned 2.0x
   bar.
3. The current eager-hydration bound (docs/schemas.md §3.2) plus the
   existing 100k-stream cap **suffice at Tier-1** — no §3.2 stage-2
   redesign is warranted by this evidence alone.
4. **Tier-2 mandate (see below): this decision is scale-scoped, not
   final.**

### Tier-2 mandate

**Issue #25 (the 1 TB/7d Tier-2 reference run) must re-evaluate this
same 2.0x client-wall decision gate at production scale, and file the
§3.2 stage-2 redesign product issue then if wall materiality emerges
there.** The dilution analysis above is itself breadth/scale-dependent:
`resolution`+`samples`' O(breadth) share, and the constant-factor
per-row-JSON-materialization cost this report isolates, may combine
differently at Tier-2's absolute scale — a larger corpus could either
further dilute the hydration share (if resolution/samples grow faster) or
let the same constant-factor CPU reduction cross into wall materiality
(if connection/dispatch overhead becomes proportionally smaller and
hydration's absolute share grows). This report does **not** predict which;
it only establishes that the CPU-level phenomenon is real, isolated, and
worth re-measuring at the scale docs/schemas.md §9's two-tier model
reserves for exactly this class of question.

**No follow-up product issue is filed by this report** (out of scope,
architect plan: "do NOT auto-file... a task-manager/human step" — that
step belongs to issue #25's own future evaluation, per the mandate above,
not to this report).

## Multi-tenant note (scoped)

The late shape bounds the **label-hydration stage** to `O(limit)` in
**result row count and CPU** (not in storage bytes scanned — validity gate
(c) proves storage I/O stays `O(breadth)` and strategy-invariant): it
transfers/decodes/materializes labels only for the `<= limit` returned
fingerprints. It does **not** make the whole query `O(limit)` — stage-1
resolution, the service-set derivation, and the stage-3 `fingerprint IN
(...)` list all remain `O(breadth)`, bounded by §3.2's existing 100k
stream cap, and (per the dilution analysis) dominate total query cost at
Tier-1 scale. The isolated hydration-stage win is real; its end-to-end
materiality is scale-dependent and unresolved pending the Tier-2 mandate
above.

## Reproduction

```text
podman run -d --rm --name pulsus-ch-hydration -p 19123:8123 -p 19000:9000 \
    clickhouse/clickhouse-server:24.8

# CI tier (record-only, no verdict — does not reach the 50,000-breadth anchor):
cargo run -p xtask -- bench logs-hydration \
    --http-url http://127.0.0.1:19123 --database pulsus_bench_hydration_ci \
    --profile ci --seed 42 \
    --out docs/benchmarks/data/logs-hydration-ci.json \
    --report-out /tmp/logs-hydration-ci.md

# Full tier (verdict-evaluation breadth [1_000, 10_000, 50_000], manual only, minutes):
cargo run -p xtask -- bench logs-hydration \
    --http-url http://127.0.0.1:19123 --database pulsus_bench_hydration_full \
    --profile full --seed 42 \
    --out docs/benchmarks/data/logs-hydration-full.json \
    --report-out /tmp/logs-hydration-full.md
```
