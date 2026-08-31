# Traces differential divergence ledger

The M4 traces differential (`e2e/src/traces.rs`, issue #60) gates every
corpus-computable TraceQL case in
`test/fixtures/traces/differential.json` against both the corpus's
by-construction expectation and the pinned reference Tempo
(`grafana/tempo:3.0.2`, digest-pinned in
`deploy/e2e/compose.single.yaml`). **The exclusion list starts empty.**

A case moves from `mode: "gated"` to `mode: "informational"` only via
the #33 triage discipline:

1. an **observed live divergence** (a failed gated run with its dumped
   repro artifact from `target/e2e-artifacts/traces-diff/`),
2. triaged **fix-our-bug vs ratify-documented-difference**, and
3. recorded here as an entry whose id the fixture case's `ledger` field
   references (a hermetic unit test in `e2e/src/traces.rs` enforces the
   fixture↔ledger link both ways).

Entries are append-only; re-gating a case removes its `ledger` reference
but keeps the entry for history.

## Entries

### 2026-07-16-negation-matches-missing-key

- **Case:** `neg_attr_missing_key` — `{ resource.run_id = "{R}" &&
  resource.env != "prod" }`, where a deterministic subset of the corpus
  (`trace_idx % 5 == 4`) carries no `env` resource attribute at all.
- **Observed divergence (live run, 2026-07-16):** gated run against
  `grafana/tempo:3.0.2` failed with
  `tempo_vs_corpus`: expected 14 traces, PulsusDB returned all 14, Tempo
  returned 10 — missing exactly the 4 traces whose resources lack the
  `env` key (repro artifact
  `traces-diff/single/search-mismatch-5e98104cd2edb55c.json`; query
  `{ resource.run_id = "e2e-traces-diff-2a977e5fd55b1e36" &&
  resource.env != "prod" }`).
- **Triage:** ratify-documented-difference, not a PulsusDB bug.
  PulsusDB's behavior is the committed contract — docs/api.md §4.2:
  "`!=`/`!~` on an attribute match spans **lacking the key entirely** as
  well as spans whose value differs" (the negation rule ratified on
  issue #57 and exercised by the frozen part-(a) golden
  `negated_attr.sql`). Tempo's TraceQL evaluates a comparison against a
  missing attribute as non-matching, so its `!=` excludes absent-key
  spans. Both stores agree on `!=`/`!~` whenever every span carries the
  key (`neg_attr_key_on_all` / `neg_regex_key_on_all` remain GATED and
  pass three-way).
- **Disposition:** `neg_attr_missing_key` moves to
  `mode: "informational"`. PulsusDB stays hard-gated against the
  corpus expectation under our documented rule (a PulsusDB regression
  on this case still fails the scenario); only the Tempo comparison is
  reported as an informational artifact.

### `traceql-validate-re2-unknown-residual` (issues #328, #336)

- **What:** the #328 semantic validator's regex check
  (`pulsus_traceql::validate` → `pulsus_re2::re2_verdict`) is
  three-valued, and `Unknown` is ACCEPTED — deciding "RE2 rejects this"
  in-process needs an RE2-syntax parser this codebase deliberately does
  not have (the root fix is **#336**; closing #336 retires this row).
- **The `Unknown` surface, enumerated from the code** (fix round 1: the
  first version of this row listed three probed classes; the honest
  enumeration is every `Unknown` return site in
  `crates/pulsus-re2/src/lib.rs`, pinned representative-by-site by
  `every_unknown_return_site_has_a_named_class_representative` and
  covered vector-by-class in `validate-vectors.json`). Per class, the
  PINNED REFERENCE's verdict
  (`grafana/tempo@sha256:aa8df8d069f77b82e978464daf55169bb8d135852ad58
  700aa96880653c3d8f7`, measured on the validation-only shadow route):
  - **`lookaround`** (`(?=`, `(?!`, `(?<=`, `(?<!`) — reference **400**
    on all four (vectors `rx-u7`–`rx-u10`). The commonest member of the
    residual by far.
  - **`unicode-property`** (`\p`/`\P`, bare or inside a class) —
    verdict varies BY MEMBER, and **#400 Stage 2 split the class along
    exactly that line**: properties in RE2's fixed 202-name table are
    200 and stay `Unknown` (`\p{L}`, `rx-u17`); the rest are 400 and are
    now DECIDED in-process, so `rx-u4` and `rx-u16`
    (`\p{Alphabetic}` bare and in class position) moved
    `pulsus: accept → reject` and left the residual. The class survives
    on its in-table half alone.
  - **`trailing-backslash`** (`a\`) — reference **400** (`rx-u11`;
    jointly rejected in fact — the Rust crate rejects it too — but the
    scan defers before compiling, by design; deciding it is #336's).
  - **`nonportable-group-head`** (`(?x`, `(?u`, `(?#`, …) — reference
    **400**. **#400 Stage 2 decided the `{u, x, R}` flag members**, so
    `rx-u18` (`(?x)a b`) and `rx-u19` (`(?u:a)`) moved
    `pulsus: accept → reject`; the class survives on `rx-u20`
    (`(?#c)a`), which is a comment group and not a flag run — the
    Rust crate refuses it too, so it was already agreement everywhere
    but here.
  - **`compiled-too-big`** (`regex::Error::CompiledTooBig` — the Rust
    crate's compiled-size budget, which is not RE2's; per-member the
    verdict depends on the two engines' budgets) — the measured member
    is a reference **400** (`rx-u23`).
  - **RETIRED by #400 Stage 2 — three classes, every member decided.**
    `over-max-repeat` (`{n}` above `kMaxRepeat = 1000`, `rx-u5`),
    `repetition-of-repetition` (`a**`, `a{2}{3}`, `a*??`,
    `rx-u6`/`rx-u21`/`rx-u22`) and `rust-only-escape` (`\u`, `\U`,
    `rx-u12`/`rx-u13`) were each a reference **400** in every member,
    and `pulsus_re2::re2_definitely_rejects` — which `re2_verdict` now
    consults before the scan — decides all of them. They are gone from
    `UNKNOWN_CLASSES` in `validate_corpus.rs`; their vectors remain, as
    reject/reject rows carrying no `unknown_class` and no `divergence`.
    **Ten vectors moved in all, every one toward parity**: each already
    recorded `tempo: reject, tempo_status: 400`, and
    `every_vector_reproduces_its_recorded_pulsus_verdict` now asserts
    that direction from a committed id list, so a move the other way is
    RED rather than merely reviewable.
  - **Agreement classes** — `Unknown` here AND accepted by the
    reference, so no consumer-visible divergence: `named-group`
    (`(?P<n>`, `(?<n>` — plan v3 wrongly listed this as rejected;
    re-measured 200, `rx-u3`), `literal-quoting` (`\Q…\E`, `rx-u1`),
    `octal-escape` (`rx-u2`), `boundary-escape` (`\<`/`\>`/`\b{…}` —
    the reference reads them as literals/boundary-plus-braces and
    accepts, `rx-u14`/`rx-u15`; the MEANING differs, which is the
    screen's original subject and precisely why they defer).
- **The residual SHRANK without this row's mechanism changing** (#400
  Stage 2). `check_regex` is untouched: it still rejects exactly when
  `re2_verdict` says `Rejects` and still accepts `Unknown`. What moved
  is `re2_verdict`, which consults a reject-only pre-check built from
  the REFERENCE's own parser before the scan. Closing #336 still retires
  this row; Stage 2 removed three of its classes and half of two more.
- **Which paths the residual reaches:** on search's `q=` and on the
  metrics query parameter the expression is planned and EXECUTED, so
  ClickHouse's RE2 rejects the diverging members at execution
  (`Code: 427` → #280's classifier → `400`) and only the error's origin
  and timing differ from the reference. On search's shadow `query=`,
  which is validated and never executed, **nothing catches them**: the
  request is a `200` where the reference is a `400` — for every
  reference-400 class above, lookarounds included.
- **Gates:** `pulsus-traceql/tests/validate_corpus.rs` asserts this row
  id and the vectors reference each other both ways and that every
  class above carries a vector;
  `tempo_differential.rs::validate_vectors_match_the_live_reference`
  re-measures every vector against the pinned digest, fail-closed.

### `traceql-validate-nil-spelling-conflation` (issue #328) — **RETIRED by #335 Stage B**

- **Retired 2026-08-03.** The row's own exit condition was "the fix rides
  whenever the AST next changes shape", and the Stage B grammar collapse
  is that change: `= nil` / `!= nil` now fold to a single `Exists { field,
  negated }` node carrying the polarity as a flag, so `{ name = nil }` and
  `{ !(name != nil) }` are DIFFERENT ASTs and the validator can tell them
  apart. Re-measured against the same pinned digest after the collapse:
  `{ name = nil }` 400 / reject, `{ !(name != nil) }` 200 / accept,
  `{ name != nil }` 200 / accept, `{ .a = nil }` 200 / accept,
  `{ !(.a != nil) }` 200 / accept,
  `{ !(resource.service.name != nil) }` 200 / accept — six for six.
  The over-rejection is gone; no divergence remains to record.
- **Gate:** `validate::tests::the_nil_spellings_are_no_longer_conflated`
  now pins the DISTINCTION (both the AST inequality and both verdicts), so
  a regression to the conflated shape fails rather than quietly restoring
  the divergence this row described. The `nil-d1`/`nil-d2` vectors were
  re-recorded with the matching verdicts in the same change.

The original entry, kept for the trail:

- **What:** the parser normalizes `x = nil` to `Not(Exists(x))` — and
  the double-negation spelling `!(x != nil)` produces the SAME node, so
  the two are indistinguishable to the AST-based validator. Measured at
  the pinned digest: `{ name = nil }` is a `400` (`intrinsics cannot be
  nil`) and `{ !(name != nil) }` is a `200`. PulsusDB rejects BOTH
  (vectors `nil-d1`/`nil-d2`: `tempo: accept`, `pulsus: reject`) — the
  canonical `= nil` rejection matches the reference, and the exotic
  double-negation spelling is over-rejected as the price.
- **Why deliberate (for now):** distinguishing the spellings needs a
  parser/AST change (a dedicated nil-comparison node), which churns the
  frozen corpus goldens and sits outside #328's file list; plan v1's
  premise that `Not(Exists(intrinsic))` "can only come from `= nil`"
  was measured false during implementation. Recorded here rather than
  silently absorbed; the fix rides whenever the AST next changes shape.
- **Gates:** the same vectors↔ledger link as above;
  `validate::tests::the_nil_double_negation_spelling_is_conflated_and_rejected`
  pins the conflation itself, so the row cannot outlive the AST that
  causes it.

### `traceql-pow-integer-operand-swap` (issue #335 Stage B; source located #351)

- **Where a reader meets it in the reference's source** (added on #351,
  after the v3.0.2 tree was read directly): `pkg/traceql/ast_execute.go`
  defines `intPow(base, exp int)` at `:940-942` and CALLS it as
  `intPow(rhsN, lhsN)` at `:486` (and again in the array-element path at
  `:741`) for `lhs ^ rhs` — the arguments are transposed at the call
  site, not in the helper. The float catch-all at `:652` is
  `math.Pow(lhs.Float(), rhs.Float())`, in the correct order, which is
  exactly why the condition below is load-bearing: the two paths
  disagree, and the literal's spelling picks the path. The measurements
  came first; this citation explains why they look the way they do and
  generalises past the inputs that were probed.

- **Reference behaviour (measured, `grafana/tempo@sha256:aa8df8d0…`,
  v3.0.2):** the `^` operator **swaps its operands on the INTEGER path**.
  Measured, with the folded constant read from the reference's own
  type-error message:

  | query | reference | `lhs^rhs` would be |
  |---|---|---|
  | `2 ^ 10` | 100 | 1024 |
  | `10 ^ 2` | 1024 | 100 |
  | `2 ^ 3` | 9 | 8 |
  | `5 ^ 0` | 0 | 1 |
  | `0 ^ 5` | 1 | 0 |
  | `2 ^ -1` | 1 | 0.5 |

- **The condition is load-bearing — this is NOT a blanket swap.** One
  float operand takes a correct path:

  | query | reference |
  |---|---|
  | `3 ^ 4` | **64** (swapped) |
  | `3.0 ^ 4` | **81.0** (correct) |
  | `2 ^ 10` | 100 (swapped) |
  | `2.0 ^ 10`, `2 ^ 10.0`, `2.0 ^ 10.0` | 1024.0 (correct) |
  | `4 ^ 0.5` | 2.0 (correct) |

  The same mathematical operands give opposite results, decided by the
  literal's spelling. Anyone writing "the reference's `^` swaps its
  operands" without the condition states something false for
  `2.0 ^ 10`, so the row would read as wrong and take the ledger's
  credibility with it.

- **PulsusDB behaviour:** `lhs ^ rhs` on every path. `2 ^ 10` is 1024.

- **Why deliberate:** this is a defect, not a convention. A result that
  depends on whether the user typed `3` or `3.0` cannot be relied on, and
  a user writing `2 ^ 10` means 1024. Under the standing mandate we match
  the reference except where it is wrong.

- **Consequence, stated plainly because it looks like a regression:**
  `{ .a = 2 ^ 3 ^ 2 }` is **512** for PulsusDB and **64** for the
  reference. Grouping AGREES — both are right-associative (established
  structurally; see `FieldOp::is_right_assoc`). Only the operator
  diverges.

  The merged #335 audit previously made PulsusDB answer 64 by making `^`
  **left**-associative with a correct operator: `(2^3)^2` = 8² = 64. The
  reference reaches 64 by right grouping with a swapped operator:
  `2^(3^2)` = `2^8` → 8² = 64. **Two independent errors cancelled for
  that one input.** Fixing the grammar without copying the defect
  necessarily breaks the agreement, and the agreement was never evidence
  of a shared model — a single value can only ever pin a value. That
  finding is not a criticism of the audit: seeing it took a three-term
  structural probe plus a characterisation of the operator, neither of
  which a single-value comparison can motivate.

### `traceql-event-link-operand-any-match` (issue #351)

**Owner ruling, 2026-08-05.** The row exists so the next reader can
re-decide from the evidence rather than re-derive it.

- **What PulsusDB does.** When a span-event or span-link intrinsic
  (`event:name`, `event:timeSinceStart`, `link:spanID`, `link:traceID`)
  is compared against another FIELD, the span matches if **any** of its
  events (or links) satisfies the comparison; `!=` matches only when
  **every** one does, so a span with no events at all satisfies it. Six
  probes: `{ .a = event:name }` and the three other intrinsics, plus the
  two reverse-order spellings.

- **What the reference does.** It answers the same queries from the
  **FIRST event only**. Measured against the pinned container
  (`grafana/tempo@sha256:aa8df8d0…`, v3.0.2) with a discriminating
  fixture — one positive example cannot tell "any" from "first" from
  "all", so the fixture varies WHICH event matches:

  | events on the span | `.a` holds | reference | PulsusDB |
  |---|---|---|---|
  | `evX, evY, evZ` | `evZ` (last) | no match | **match** |
  | `evP, evQ, evR` | `evP` (first) | **match** | **match** |
  | `ev1, evM, ev2` | `evM` (middle) | no match | **match** |
  | `ev7, ev8` | `evNope` | no match | no match |

  Each of those event names is individually queryable there
  (`{ event:name = "evZ" }` returns its span), so the data is fully
  indexed and present; the field-vs-field form simply consults the first
  entry. Stable across three runs.

  The negated form was measured on the same store and agrees with that
  reading from the other side: `{ .a != event:name }` returns the
  `evZ`-last and `evM`-middle spans there (their FIRST event differs, so
  the reference keeps them) and not the `evP`-first span. Ours excludes
  all three, because in each of them SOME event matches. Two independent
  confirmations of the same divergence, in opposite directions.

  **One edge the reference and PulsusDB already agree on:** its `!=`
  also returns spans with NO events at all (measured — the link-only
  spans in the same fixture come back), which is the empty-set rule we
  implement. So the disagreement is confined to spans that HAVE events
  and where the matching one is not the first.

- **The reference's own behaviour VARIES BY ROUTE, which is why this is
  not a contract to copy.** Three readers of the same span disagree, and
  each is reachable from user queries:

  | route | which event | citation (v3.0.2) |
  |---|---|---|
  | pushdown / fetch conditions (`{ event:name = "evZ" }`) | **any** | the parquet condition iterator matches any event row — measured: the span whose LAST event is `evZ` is returned |
  | `AttributeFor` (the field-vs-field path) | **first** | `tempodb/encoding/vparquet4/block_traceql.go:128-152` — `find` returns the first entry whose attribute matches; reached for intrinsics at `:227-241`; one entry per event is appended at `:3683-3691` |
  | `AllAttributes` (response projection) | **last** | `tempodb/encoding/vparquet4/block_traceql.go:65-104` — a `map[Attribute]Static`, so the last write wins |

  The first-event answer is a property of a linear first-match scan over
  a flat per-event list, not a designed rule. It is also indefensible on
  its own terms from the user's side: adding an OLDER event to a span
  would change whether it matches, though the event asked about did not
  change.

- **Ours is the reference's own DESIGNED multi-value rule.**
  `pkg/traceql/ast_execute.go:535-627` @ v3.0.2 compares a scalar against
  an array elementwise: `matchAll` is set for `OpNotEqual`/`OpNotRegex`
  and the result is `matchCount == elemCount`, otherwise `matchCount > 0`.
  We implement that arithmetic exactly, which is where three of our
  edge-case answers come from — an empty set satisfies `!=`
  (`0 == 0`), a cross-type element makes `!=` false rather than true,
  and an absent scalar operand never matches (issue #183's rule,
  unchanged).

- **The migration copying the reference would require.** Not
  implementable on this storage without a breaking schema change plus a
  rebuild of stored data:

  - event/link rows in `trace_attrs_idx` carry the **span's**
    `timestamp_ns`, with no event ordinal
    (`crates/pulsus-write/src/protocols/otlp_traces.rs`, the span-event
    and span-link fan-out);
  - the table is a `ReplacingMergeTree` ordered by
    `(key, val, scope, timestamp_ns, trace_id, span_id)`, so two events
    with the SAME name on one span collapse into one row — the ordering
    information is destroyed by construction, not merely unrecorded;
  - so "first event" needs a new indexed ordinal column (migration +
    write-path change + backfill of existing data), or a per-span
    payload decode in the Phase-2 hot loop.

  Paying that to reproduce a self-inconsistent artefact was judged the
  wrong trade for a query shape that is rare next to the literal form
  `{ event:name = "timeout" }`, which is common and already agrees.

- **Where it is enforced.** `filter::LeafEval::EventSetCompare` +
  `search_eval::eval_event_set_compare` (the rule), the hermetic
  `an_event_set_comparison_matches_any_event_not_the_first` /
  `an_event_set_negation_is_all_match_and_an_empty_set_satisfies_it`
  (the fixture table above, span for span), and the live
  `event_and_link_comparisons_match_any_event_over_real_clickhouse`,
  which runs the co-load against a real ClickHouse — the hermetic tests
  cannot execute the per-batch value read the semantics depend on.

- **Accept-surface effect:** the six probes move `reject → accept` on
  the wire axis. The reference's own verdict is unchanged (it always
  accepted them), so the matrix's ORACLE column is untouched; only what
  we return differs, and only for multi-event spans.

### `2026-08-05-traceql-quantile-over-time-tdigest` (issue #252)

- **What differs.** The reference computes `quantile_over_time` from its
  internal log2 histogram: `Log2QuantileWithBucket`
  (`pkg/traceql/engine_metrics.go:2058-2120 @ v3.0.2`) counts through the
  per-interval bucket tallies until it has `ceil(p × total)` samples and
  returns the **bucket label** it stopped on, interpolating exponentially
  between adjacent OCCUPIED buckets when the count lands mid-bucket. The
  answer is therefore one of at most ~64 values, and it rounds up.
  PulsusDB computes `quantilesTDigest` over the replay-deduped raw
  `duration_ns` (`traces::metrics_sql::metrics_quantile_range_sql`, the
  #173 TDigest precedent).

  `histogram_over_time` is **not** part of this divergence: as of #252 it
  matches the reference exactly — same power-of-two `__bucket` labels,
  same per-bucket tallies, only occupied buckets emitted, never
  cumulative.

- **Why ours ships** (owner ruling 2026-08-05, measured on
  `grafana/tempo:3.0.2@sha256:cda87c21…`; the capture is committed at
  `crates/pulsus-read/tests/golden/traces_metrics/log2_reference_capture.json`).
  Three corpora of 20 spans each, every span 280 ms / 300 ms / 520 ms
  respectively. All three lie in `(2^28, 2^29]`, so all three occupy the
  single bucket `2^29 ns = 0.536870912 s`, and the reference returns
  **byte-identical output for all three**:

  ```
  p=0.5   0.3796250624970063
  p=0.9   0.5009182730924541
  p=0.99  0.536870912
  p=1.0   0.536870912
  histogram_over_time(duration) -> {"__bucket": 0.536870912} = 20
  ```

  - **Their estimator is a function of the OCCUPIED BUCKET, not of the
    durations in it.** 280, 300 and 520 ms are indistinguishable at every
    `p` — even `p=0.5`, which is an interpolated value rather than a
    bucket label. This is the row's load-bearing claim; it is measured,
    and the AC3b oracle test is what keeps it checkable.
  - **Thresholds.** A true p99 of 300 ms is reported as 536.87 ms, 79%
    high, tripping a 500 ms alert the real data never crosses. The bias
    is one-directional.
  - **Trends.** 280 ms → 520 ms is an 86% rise that the reference reports
    as the same bytes on both days. Buckets double in width, so the
    slower the service the blinder it gets; the worst-case overstatement
    is ~2× and grows in absolute terms.

- **Not an internal inconsistency.** Both values are consistent with the
  same histogram — theirs is an *upper bound* within the occupied bucket,
  ours a *sharper value* inside it. Because `histogram_over_time` is
  byte-matched, a client can still reconstruct their bound from our
  buckets; what it additionally gets is a percentile that moves when the
  data moves.

- **This upholds the 2026-07-26 ruling, it does not reverse it.** What
  changed since then is only the implementation cost: the log2 tally is
  now computed anyway for `histogram_over_time`, so matching would be
  free. Cost was never that ruling's stated reason, and free is not a
  reason to report a worse number.

- **Scope.** `quantile_over_time` VALUES only. The wire shape (`p=<q>`
  label, `doubleValue`, sample encoding) is unchanged and remains
  matched; `histogram_over_time` is matched exactly.

- **Where it is enforced.** `crates/pulsus-read/tests/traces_log2_reference.rs`
  replays the committed capture through a test-only port of
  `Log2QuantileWithBucket` (a characterisation oracle, explicitly not a
  code path) and reproduces every captured reference quantile — exactly
  on the discrete branches, within a relative error of 1e-12 on the
  interpolation (observed worst case 1.8e-16); both required mutants
  (`max_samples ± 1`, neighbour `idx-1 → idx-2`) fail that assertion.
  `traces_metrics_live.rs::log2_histogram_membership_and_the_sub_two_ns_guard`
  pins our own side against real ClickHouse: over 20 identical 300 ms
  spans every quantile is exactly `0.3`, and over 520 ms spans exactly
  `0.52` — the pair the reference cannot distinguish. User-facing
  write-up: docs/api.md §4.4.1.

### `2026-08-05-traceql-histogram-series-order` (issue #252)

- **What differs.** `histogram_over_time` series ORDER, and nothing else.
  PulsusDB emits them **ascending by bucket**. The reference emits them
  in lexicographic order of a *rendering* of the bucket label.

- **The mechanism, so nobody re-derives it wrongly.** `sortResponse`
  (`modules/frontend/combiner/metrics_query_range.go:245-266 @ v3.0.2`)
  compares `Label.Value.String()`. That `Value` is a protobuf `AnyValue`
  whose `String()` is `proto.CompactTextString`
  (`pkg/tempopb/common/v1/common.pb.go:46 @ v3.0.2`); gogo's text writer
  ends at `fmt.Fprint(w, v.Interface())` for a scalar
  (`vendor/github.com/gogo/protobuf/proto/text.go`, the `default:` arm of
  `writeAny`), i.e. Go's `%v` for a `float64`, i.e.
  `strconv.FormatFloat(v, 'g', -1, 64)`. So the sort key is Go's `%g` —
  **not** the protojson text of the response body, and not the value.

- **Measured on the pinned container** (`grafana/tempo:3.0.2@sha256:cda87c21…`,
  capture corpus `mixladder`). Four spans, at 1 µs, 16 µs, 1 ms and 1 s:

  ```
  __bucket 0.001048576   (1 ms)
  __bucket 0.000001024   (1 µs)
  __bucket 1.073741824   (1 s)
  __bucket 0.000016384   (16 µs)
  ```

  Not ascending, not descending, not the order of its own JSON body
  (which renders `2^10 ns` as `0.000001024`, sorting it first). It needs
  nothing exotic to appear: the `mix16k` corpus is a 16 µs span beside a
  1 ms span — the reference returns the 1 ms bucket first, because `%g`
  writes `2^14 ns` as `1.6384e-05`.

- **Why ours ships** (owner ruling, 2026-08-05, to the question "what
  would users expect to see"). The reference's order is a **determinism
  device, not a semantic one** — it exists so two runs agree, and it
  conveys nothing a client could rely on. A histogram is drawn
  smallest-bucket-first everywhere a user has seen one, so ascending by
  bucket is the correct answer to the same question. This is the same
  ruling shape as the percentile row above: match the reference where it
  is right, be correct where it is not, record the difference.

- **Consequence: series order only.** The bucket rule, membership,
  tallies and non-cumulativity are matched to the reference (that half of
  #252 is a Tier-1 parity gate). Precisely: **label VALUES, tallies,
  counts and membership are identical; the ORDER of the array differs**,
  and — separately and independently of this row — the label TEXT differs
  for the four buckets `2^10 .. 2^13 ns`, where `serde_json`/ryu writes
  `1.024e-6` and protojson writes `0.000001024` (same `f64`, same parse;
  recorded, not filed). A client that reads the `__bucket` label — which
  is how the series is identified — is unaffected by the order; only one
  indexing the array positionally would be, and the reference's own
  positions are not meaningful to index by.

- **Where it is enforced.**
  `crates/pulsus-read/tests/traces_log2_reference.rs::we_emit_ascending_by_bucket_and_the_reference_order_is_pinned_beside_it`
  asserts our ascending order for every captured corpus AND pins the
  reference's order as an **explicit expected `bucket_ns` sequence per
  corpus** (`REFERENCE_EMITTED_ORDER`), not as a property restated from
  the capture — so a change on their side fails, which is the only thing
  that makes this row checkable. A second table
  (`IF_SORTED_ON_THE_REFERENCE_WIRE_TEXT`) pins, also as sequences, the
  order its own JSON body would have given, which differs for `mix1024`,
  `mix16k` and `mixladder` — the evidence that the sort key is
  `AnyValue.String()` and not the response text. Both tables are checked
  for membership against the capture, so a corpus cannot be added or
  dropped unpinned. Production:
  `traces::exec::sort_histogram_series_by_bucket_ascending`. User-facing
  write-up: docs/api.md §4.4.1.

### `traces-absent-trace-404-body` (issue #384)

- **What:** `GET /api/traces/v1/trace/{traceId}` for a trace that is not
  stored answers `404` with the body `trace not found` under
  `Content-Type: text/plain; charset=utf-8`. The reference answers `404`
  with **no body at all and no `Content-Type` header** — measured on the
  CI-pinned oracle (`grafana/tempo@sha256:aa8df8d069f77b82e978464daf5516
  9bb8d135852ad58700aa96880653c3d8f7`, the digest at
  `.github/workflows/ci.yml:483`), on `/api/traces/{id}` for a valid,
  absent 32-hex id. Every other §4 error body matches the reference's
  container exactly; this one row is about the body being EMPTY, not
  about the envelope #384 removed.

- **Why we do not match it.** `api_conformance`'s fetch surface has no
  other live mounting oracle. Against that suite's empty databases the
  documented outcome of a well-formed fetch IS the absent-trace `404`, so
  a mounted-but-absent response and axum's unrouted `404` are told apart
  by exactly one thing: our body is non-empty and axum's is not
  (`assert_404_empty`). Making ours empty makes a silently unmounted
  `/api/traces/v1/trace/{traceId}` indistinguishable from a working one,
  in every mode-gated spawn the suite runs.

  `route_inventory` does **not** stand in for that, and this was checked
  rather than assumed: that guard is hermetic and starts no server — no
  spawn, no socket, no request anywhere in the file. It scans the router
  **source text** for `.route(` registrations and compares the extracted
  `(method, path)` set against the manifest, pins the composition
  functions' bodies, and checks `docs/api.md` mentions each mounted path.
  So it proves the route is registered in the tree and documented; it
  cannot observe that a running `pulsusdb`, under a given
  reader/writer/compat gating, actually serves that path from the traces
  handler rather than from axum's fallback. That is the property the live
  oracle exists for.

- **Consumer impact.** A client distinguishes "absent" from "unrouted" by
  the `404` status, which is identical on both sides; ours additionally
  carries a human-readable reason. Nothing in the Grafana Tempo
  datasource's path branches on the body of a `404`.

- **Where it is enforced.** `api_conformance`'s
  `assert_traces_fetch_route` (the `documented-method-absent-404`,
  `short-16-hex-absent-404` and `absent-404-stays-plain-text` cells) and
  `traces_api_live::assert_error_body`. User-facing write-up:
  docs/api.md §4.1.

- **Retiring this row** means giving the fetch surface a live mounting
  oracle that does not read the body — the `405 Allow: GET,HEAD` cell and
  the `Vary: accept` header on the `404` are both candidates — and then
  emptying the body.

### `zipkin-backpressure-429-not-500` (issue #385)

- **What:** `POST /api/v2/spans` answers sink backpressure `429` with a plain-text
  body. The reference answers its nearest equivalent — the distributor's ingestion
  rate-limit rejection — `500`, with the 23-byte body `"Internal Server Error"`, no
  `X-Content-Type-Options` and no trailing newline. Measured on the CI-pinned oracle
  (`grafana/tempo@sha256:aa8df8d069f77b82e978464daf55169bb8d135852ad58700aa96880653c3d8f7`,
  the digest at `.github/workflows/ci.yml:483`) with
  `overrides.defaults.ingestion.rate_limit_bytes: 1000`, 3/3 identical reps.
  There is no `429` anywhere on that endpoint: its handler has exactly two error
  exits, `http.Error` for decode failures and a bare `w.Write` for consumer failures
  (`receiver/zipkinreceiver/trace_receiver.go:236` and `:255-262` @ tempo v3.0.2
  `0c4b926d`), and the consumer exit is `500` for every error Tempo produces.

- **Why we do not match it.** `500` tells a sender *we are broken*; `429` tells it
  *slow down*. Those call for different sender behaviour, and collapsing them destroys
  information the sender can act on in exchange for matching a code that misdescribes
  what happened — the FULL PARITY MANDATE's "except where they are wrong" clause.
  `429`-on-backpressure is already the documented cross-receiver PulsusDB contract
  (docs/api.md §8.2 states it for this receiver and for `/loki/api/v1/push`), so
  keeping it preserves an existing commitment rather than inventing one.

- **Consumer impact.** None for the sender we can read: the OpenTelemetry Collector's
  Zipkin exporter closes the response body without reading it and treats every
  non-2xx identically (`exporter/zipkinexporter/zipkin.go:94-97` @ tempo v3.0.2), so
  `429` and `500` are the same event to it. Other Zipkin senders (Brave,
  `zipkin-reporter-java`, `zipkin-go`'s HTTP reporter) are not checked out here and
  are not claimed.

- **Where it is enforced.** `pulsus-write`'s
  `zipkin_sink_error_container_omits_nosniff_and_terminator` (status `429` and the
  container) and `backpressure_divergence_is_recorded` (the docs copy). User-facing
  write-up: docs/api.md §8.2.

- **Retiring this row** means deciding that matching a status code matters more than
  describing the condition — which would also have to retire the identical `429` on
  `/loki/api/v1/push` and the OTLP receivers, since the contract is cross-receiver.

The user-facing statement of this divergence is CANONICAL below and
copied verbatim into docs/api.md §8.2;
`crates/pulsus-write/tests/backpressure_divergence_recorded.rs` compares
the two byte for byte, so edit it here and copy it there. Wrapping and
interior spacing are part of the comparison.

<!-- copied-rule:zipkin-backpressure:start -->
and sink backpressure is **429** plain-text — a **deliberate divergence** from the
reference, which answers its ingestion rate-limit rejection **500** with the body
`"Internal Server Error"` (measured on the pinned `grafana/tempo` v3.0.2 image; its
Zipkin receiver has no `429` path at all). `500` would tell a sender we are broken
when we are asking it to slow down, so we keep `429`; recorded as
`zipkin-backpressure-429-not-500` in docs/benchmarks/traces-differential-ledger.md.
<!-- copied-rule:zipkin-backpressure:end -->
### `traceql-spanset-by-multi-key-withdrawn` (issue #335 Stage D2) — **an accept REMOVED to restore parity**

- **What was withdrawn.** PulsusDB parsed **and served** a comma-separated
  spanset grouping stage — `{ … } | by(.b, .c)` — planning a multi-key
  `SpansetStage::By` and returning `200` with grouped results. The
  reference **parse-rejects** it: `groupOperation` is
  `BY OPEN_PARENS fieldExpression CLOSE_PARENS`
  (`pkg/traceql/expr.y:177-179` @ Tempo v3.0.2), which carries no `COMMA`,
  and `fieldExpression` has none either. Measured at the pinned digest
  (`grafana/tempo@sha256:aa8df8d0…`, `/status/version` → v3.0.2, revision
  `0c4b926d0`): `400 parse error at line 1, col 19: syntax error:
  unexpected ,`.

- **Why an accept was deleted rather than ledgered as a permissive
  divergence.** This is the only shape in the whole accept-surface audit
  where a user gets a **wrong-looking `200` here and a `400` there**. Every
  other divergence is an error on one side, which a user notices
  immediately; this one works, so the user builds on it, and the query is
  then not portable to the system PulsusDB claims compatibility with. A
  rejection is honest; an answer nobody else will give is a trap. The cost
  is zero — PulsusDB has never shipped, so no deployment depends on it.

- **Witnessed by a probe that now REJECTS**, not by one that passes: the
  accept-surface probe `{ .a = 1 } | by(.b, .c)` moved `accept → reject`
  on both the parse and the wire axis and now AGREES with the reference,
  and `reject/by_multi_key` pins the positioned parse error. That is the
  same shape as `traceql-validate-nil-spelling-conflation`'s closure —
  the check is that the divergence is gone, in the direction it was gone
  in.

- **The same change widened the production in the other direction**, which
  is why it is one stage rather than two: the single operand became a full
  field expression, so `by(.b + .c)`, `by(-.b)`, `by(!.b)`, `by((.b))` and
  `by(.b = 1)` now parse, as they always have at the reference. Four of
  those five are still a clean planner `400` here — a grouping key must
  resolve to one per-span value — recorded as class D16's `wire_status:
  "open"` with its note, and each of those probes names issue 335.

- **Nothing served moved.** `crates/pulsus-read/tests/golden_sql_freeze.rs`
  (`PINNED_SQL_CORPUS`, 69 frozen goldens) is unchanged, and
  `crates/pulsus-read/tests/traces_by_key_plan_freeze.rs` — pinned in
  Stage D0, *before* this change, over **all nineteen** served by-key
  kinds — re-derives every plan byte-identically. The frozen SQL corpus
  alone would have been corroboration over three of the nineteen.

- **Where it is enforced.** `parser::tests::the_spanset_by_takes_one_key_and_that_key_is_a_field_expression`,
  the `reject/by_multi_key` corpus case, and the accept-surface matrix's
  D16 rows. User-facing write-up: docs/api.md §4.2.

### `traceql-metrics-filter-residual-refusals` (issue #458, wave 1) — **a GAP record, not a divergence**

- **What.** The metrics filter compiler (`render_expr` in
  `crates/pulsus-read/src/traces/metrics_sql.rs`) refuses constructs the
  reference serves. Enumerated from the **reference's** behaviour rather
  than from our source — a list derived from our own refusals cannot see
  one we should have and do not — and re-verified live against
  `grafana/tempo@sha256:aa8df8d069f77b82e978464daf55169bb8d135852ad58700aa96880653c3d8f7`
  (the digest `.github/workflows/ci.yml` pins), started with
  `ci/tempo/tempo-compare.yaml` unmodified, on
  `GET /api/metrics/query_range?q=…&start=<now-3600>&end=<now>&step=60s`
  against an empty store.

- **Why the reference serves all of them.** There is no metrics-specific
  filter guard in the reference at all: `Compile`
  (`pkg/traceql/engine.go:31-48` @ v3.0.2) validates the pipeline
  identically whether or not a `MetricsPipeline` is present, and
  `CompileMetricsQueryRange` reuses `expr.Pipeline.evaluate` as its
  second-pass filter. So every refusal below is a **gap**, not a
  divergence in judgement.

- **Closed by wave 1** (this issue, still open): the `nestedSetParent`
  root/non-root family — the query a live Grafana sent us — and bare
  attribute truthiness.

- **The residual, each class with its measured reference status.** Every
  row is `we 400 / the reference 200`:

  | class id | witness query | our exact 400 body |
  |---|---|---|
  | `metrics-filter-field-vs-field-and-arithmetic` | `{ .a = .b } \| rate()`, `{ .a + 1 > 2 } \| rate()`, `{ 2 > .a } \| rate()`, `{ nestedSetParent = -1 } \| rate()`, `{ nestedSetParent != -1 } \| rate()` | `type mismatch: field-vs-field and arithmetic comparisons are not supported in metrics filters` |
  | `metrics-filter-trace-level-intrinsics` | `{ trace:duration > 1s } \| rate()` | `type mismatch: trace-level intrinsics are not supported in metrics filters` |
  | `metrics-filter-absence-checks` | `{ .a = nil } \| rate()` | `type mismatch: absence checks are not supported in metrics filters` |
  | `metrics-filter-field-negation` | `{ !.a } \| rate()` | `type mismatch: field negation is not supported in metrics filters` |
  | `metrics-filter-nested-set-numbering-range` | `{ nestedSetParent < 2 } \| rate()`, `{ nestedSetParent = 5 } \| rate()` | `type mismatch: nestedSetParent comparisons inside the numbering range are not supported in metrics filters` |
  | `metrics-filter-nested-set-left-right` | `{ nestedSetLeft > 1 } \| rate()`, `{ nestedSetLeft > 0 } \| rate()`, `{ nestedSetRight < 100 } \| rate()` | `type mismatch: nestedSetLeft and nestedSetRight are not supported in metrics filters` |
  | `metrics-filter-intrinsic-existence-shared-path` | `{ name != nil } \| rate()` | `type mismatch: existence checks are only supported on attributes` |

- **Two of those rows are not what they look like.** `{ nestedSetParent =
  -1 }` and `{ nestedSetParent != -1 }` are **not** nested-set refusals: a
  negative literal is not a literal in this grammar, so `-1` parses to
  `Unary { Neg, Number("1") }` and the query is refused by the
  operand-shape check one arm earlier, in the field-vs-field/arithmetic
  class. Spelling the same predicates without a negative literal —
  `{ nestedSetParent < 0 }` and `{ nestedSetParent >= 1 }` — is served.
  And `metrics-filter-intrinsic-existence-shared-path` is not a
  metrics-block class at all: `/api/search` refuses `{ name != nil }`
  with the identical body, so the rejection comes from a shared path. It
  is listed because it IS a divergence on this route; it is owned
  elsewhere.

- **Why each remaining class is a design, not a line.**
  `metrics-filter-field-vs-field-and-arithmetic` needs a
  `trace_attrs_idx` self-join or a correlated per-span value read;
  `metrics-filter-trace-level-intrinsics` needs a trace-wide
  `GROUP BY trace_id … HAVING` semi-join matching `search_sql`'s root
  selection; `metrics-filter-absence-checks` is three lines of lowering
  but our `= nil` **value** semantics already diverge on the search
  route, so it needs its own entry first; `metrics-filter-field-negation`
  is a value co-load whose non-boolean case is a reference **500**, which
  SQL cannot produce; the two nested-set classes need the Euler
  numbering, which is a per-trace tree walk the single-query metrics
  pushdown does not build.

- **Where it is enforced.** `crates/pulsus-read/tests/fixtures/metrics_filter_accept.json`
  carries one probe per witness with both sides' verdicts and the exact
  bodies; `crates/pulsus-read/tests/traces_metrics_filter_accept.rs`
  re-derives our column from the tree and the reference's from the pinned
  container. `crates/pulsus-read/tests/traces_metrics_ledger.rs` asserts
  that the class ids in this table and the fixture's divergent probes are
  **the same set**, both directions.

### `traceql-metrics-nestedsetparent-root-window` (issue #458) — **a temporary split between our own two routes**

- **The measurement, with route and window.** Trace `cc…cc`, service
  `orphan`: root `0300000000000001` at `T`, child `0300000000000002` at
  `T + 300s`. Route
  `GET /api/search?q={nestedSetParent<0 && resource.service.name="orphan"}&start=<T+100>&end=<T+400>&limit=10`
  — a window that **excludes the root** — against
  `grafana/tempo@sha256:aa8df8d0…` and against PulsusDB at `4193be6`.
  The reference returns `{"traces":[], …}`. Our **search** route returns
  the child, `spanID 0300000000000002`.

- **Which side is right: the reference.** Its root sentinel comes from the
  stored `ParentSpanID`
  (`tempodb/encoding/vparquet4/nested_set_model.go:11-12,57` @ v3.0.2),
  not from the query window. A span with a parent is not a root just
  because its parent fell outside the window. Our `compute_nested_set`
  (`crates/pulsus-read/src/traces/search_eval.rs`) treats a span whose
  parent is not in the hydrated window as a forest root.

- **Which of our surfaces is right: the metrics route, after issue #458.**
  Its lowering is `parent_id = <all-zero>` — the reference's own `IsRoot`
  identity — so on the metrics route the same window answers the
  reference's way. The search route is wrong here.

- **The open class this belongs to.** The window-clipping fixture class of
  `crates/pulsus-read/tests/nestedset_value_differential.rs`, which fails
  by design today and is #185-closeout work; docs/features.md already
  records that the suite is env-gated and that no workflow supplies its
  gate.

- **The split is temporary, and it closes on the SEARCH side.** It ends by
  the search route converging on the reference — **never** by the metrics
  route regressing to match search. A new surface should be correct even
  while an old one is not; the reverse rule would spread every existing
  defect to everything built after it.

- **Where it is enforced.** `crates/pulsus-read/tests/traces_metrics_ledger.rs`
  asserts each of the five facts above individually, so the entry cannot
  be satisfied by existing.

### `traceql-differential-legs-skip-green-on-a-missing-endpoint` (issue #458) — **open wiring risk, recorded not fixed**

- **What.** Three reference-facing differential suites read the URL of the
  container they compare against with a bare `std::env::var` and take a
  skip arm when it is absent:

  | suite | endpoint variables |
  |---|---|
  | `crates/pulsus-read/tests/compare_value_differential.rs` | `PULSUSDB_COMPARE_DIFF_URL`, `PULSUSDB_COMPARE_OTLP_URL` |
  | `crates/pulsus-read/tests/traces_search_grouping_differential.rs` | `PULSUSDB_GROUPING_DIFF_URL`, `PULSUSDB_GROUPING_OTLP_URL` |
  | `crates/pulsus-read/tests/nestedset_value_differential.rs` | `PULSUSDB_NESTEDSET_DIFF_URL`, `PULSUSDB_NESTEDSET_OTLP_URL` |

- **Why it matters.** Each also checks `PULSUS_TEST_CLICKHOUSE`, which IS
  fail-closed. So with the ClickHouse gate still set and only the URL
  variables dropped from a `schema-it` step, the suite prints a skip
  notice and the step reports **green having compared nothing** — the
  issue #320 failure, inside the legs whose whole purpose is to compare
  against the reference. Nothing currently detects it: the guard that
  would (`pulsus_testkit::require_live_endpoint_gate`) is not reached,
  because the bare `env::var` returns first.

- **Measured, on the suite where it was fixed.** `traces_metrics_filter_differential.rs`
  had the identical shape and now routes both URLs through
  `require_live_endpoint_gate`. With the URLs dropped and
  `PULSUS_TEST_CLICKHOUSE=1 GITHUB_JOB=schema-it` set it fails loudly
  (`PULSUSDB_METRICS_FILTER_DIFF_URL is not set, but this is CI job
  "schema-it"…`); before the change the same invocation printed a skip
  notice and exited `ok`.

- **Why it is not fixed here.** Three suites' gating is a change with its
  own review surface, none of them is currently failing, and issue #458 is
  about span durations and metrics filters. It is recorded rather than
  bundled — but it is a wiring hole, not a divergence, and the failure
  mode is silence.

- **The fix, when it is scheduled.** Two lines per suite:
  `pulsus_testkit::require_live_endpoint_gate("<VAR>")` before the
  `env::var` reads, once per endpoint variable. The endpoint kind exists
  because these gates carry a URL and the boolean helper counts a gate as
  set only when it is exactly `"1"`.

### `traceql-compare-topn-tie-order` (issue #460) — **a deliberate refinement: our tie order is deterministic where the reference's is arbitrary**

- **What.** `compare(f[, topN[, start, end]])` keeps, per attribute and per
  side, that side's `topN` values ranked by the sum of their counts over
  the window. When two values have the **same** sum, which of them
  survives is not defined by the reference: `topN.get` sorts with
  `sort.Slice` (`pkg/traceql/engine_metrics_compare.go:548-563`, the `sort.Slice` at `:557-559`, @ v3.0.2),
  which is explicitly not a stable sort, so equal-sum entries come out in
  an order that depends on the input permutation and on Go's pivot
  choices. PulsusDB breaks the tie by **ascending value string**, so our
  answer is the same on every run and on every machine.

- **Measured, twice, on the reference.** Two corpora built for issue #460
  with all counts equal:

  | corpus | `topN` | reference survivors |
  |---|---|---|
  | 14 values, 1 span each | 2 | `i0`, `i1` |
  | 12 values, 1 span each | 10 | everything except `m05` and `m09` |

  Neither set is derivable from the values or the counts. There is nothing
  here to match.

- **Why this is a refinement and not a divergence to fix.** Matching an
  unstable sort would mean reproducing Go's `pdqsort` pivot selection,
  which is an implementation detail with no specification and no wire
  contract. Determinism is strictly more useful to a dashboard: the same
  query twice gives the same bars.

- **Consumer impact.** A user comparing two systems on a corpus where
  several values tie at the topN boundary can see a different *subset* of
  equal-count values. The **cardinality** is the same on both systems, the
  totals are the same, and every value above the boundary is the same. In
  Grafana Traces Drilldown's Comparison tab, `computeHighestDifference`
  ranks attributes on the largest |Selection − Baseline| across their
  rows, so an exchanged pair of equal-count values leaves the ordering
  unchanged.

- **How it is gated.** `crates/pulsus-read/src/traces/exec.rs`'s
  `the_topn_keep_is_cardinality_only_under_a_tie` asserts
  `n_kept == min(top_n, distinct)` and that our kept set does not depend
  on input order — and asserts **nothing** about which member survives.
  The live differential
  (`crates/pulsus-read/tests/compare_arity_differential.rs`) uses a fixture
  where every value has a **distinct** count, so tie order never enters a
  parity assertion.

### `traceql-search-metrics-completed-jobs` (issue #464) — **a repurposing, not a new field**

- **What.** On `GET /api/traces/v1/search` and its `GET /api/search`
  alias, the `metrics` block carries `completedJobs`/`totalJobs` as `1`/`1`
  on a complete result and `0`/`1` on a truncated one, with the zero
  `completedJobs` **omitted** the way protojson omits a default-valued
  scalar. PulsusDB runs one search plan, so those are the only two values
  the pair ever takes. The reference reports real shard-job counts there,
  and also populates `inspectedBytes` on every response plus
  `totalBlocks`/`totalBlockBytes` once backend blocks exist. Measured on
  the pinned container (`grafana/tempo@sha256:aa8df8d069f77b82e978464daf55169bb8d135852ad58700aa96880653c3d8f7`,
  this repo's `ci/tempo/tempo-compare.yaml` unmodified), same corpus, two
  store states:

  | store state | reference `metrics`, verbatim |
  |---|---|
  | fresh push, live-store only | `{"inspectedBytes":"21443","completedJobs":3,"totalJobs":3}` |
  | later, a completed block exists | `{"inspectedBytes":"21443","totalBlocks":1,"completedJobs":1,"totalJobs":1,"totalBlockBytes":"31127"}` |

  Ours, both branches: `{"completedJobs":1,"totalJobs":1}` and
  `{"totalJobs":1}`.

- **Why.** `tempopb.SearchResponse` has **no partiality field**.
  `PartialStatus` (`pkg/tempopb/tempo.proto:383-386` @ v3.0.2) is on
  `TraceByIDResponse`, `QueryRangeResponse` and `QueryInstantResponse`, and
  deliberately not on `SearchResponse`, so the jobs pair is the only
  incompleteness signal this route carries
  (`modules/frontend/combiner/response_metrics.go:19-38`,
  `modules/frontend/combiner/search.go:126-135`). Our previous answer —
  an invented `metrics.partial` — was worse than a repurposing: Grafana's
  Tempo datasource decodes a search response with `jsonpb.Unmarshal`
  (`pkg/tempo/search.go:95` @ `v13.1.5-11-g3c7375b`), unknown fields
  rejected, so one invented key returned an error instead of results.

  The two signals are not equivalent in kind. Ours is **exact and
  deterministic**: it is `true` for exactly the three partiality sources
  the engine records. The reference's is **racy and one-sided** — measured
  over 10 repetitions each, a 12-hour window over 5 traces, `limit=1`
  (truncated) gave `completedJobs < totalJobs` in 6 of 10 reps and equality
  in 4, while `limit=20` (complete) gave equality in 10 of 10. So on the
  reference, inequality implies "stopped early" and equality implies
  nothing.

- **What a consumer sees, named to the route.** On the **default Traces
  frame** of `GET /api/search` a truncated result shows no indicator at
  all, on either system: the datasource builds that frame from
  `response.Traces` alone (`pkg/tempo/search.go:178-281`) and reads no
  field of the metrics block. The **visible** exception is raw table mode,
  which is an ordinary `/api/search` screen — `tableType: raw` is routed
  from the same HTTP response (`search.go:104-121`) into
  `transformRawSearchResponse`, whose `json.MarshalIndent(response)`
  (`:508-519`) puts the literal `{"completedJobs":1,"totalJobs":1}` or
  `{"totalJobs":1}` into the "Raw response" cell. The **streaming** search
  path renders the pair as a progress gauge (`src/streaming.ts:289-320`),
  but PulsusDB mounts no streaming search route
  (`crates/pulsus-server/src/traces_api/mod.rs:96-123`), so that reading is
  unreachable for us.

- **What a consumer loses.** A client reading the pair as "shards
  outstanding" over-reads our `{"totalJobs":1}` when the truncation was a
  single trace's spanset overflow rather than an unfinished shard. The
  divergence is one-directional: we never report completeness we do not
  have.

- **How it is gated.** `crates/pulsus-server/src/traces_api/search_response.rs`'s
  `the_metrics_block_is_the_reference_jobs_pair_on_both_partiality_branches`
  and `every_metrics_key_is_a_tempopb_search_metrics_field` pin the whole
  object and the key set per branch;
  `crates/pulsus-server/tests/traces_search_live.rs` asserts the real wire
  bytes against `test/fixtures/traces/search_metrics.json`, and
  `e2e/src/traces.rs` feeds the same two committed blocks to the
  differential's validity gate.

### `traceql-metrics-instant-empty-window-series` (issue #464, wave 2) — **an empty answer, spelled two ways, on `GET /api/metrics/query`**

- **Route.** `GET /api/metrics/query` (and its `/api/traces/v1/metrics/query`
  and `/tempo/api/metrics/query` spellings) — the **instant** form only. The
  range form is a different message and is not described by this row.

- **What.** For a TraceQL metrics query whose window matches no spans, the
  two systems disagree in two separate ways, and only one of them is
  semantic. Measured 2026-08-29 against the pinned reference
  (`grafana/tempo@sha256:aa8df8d069f77b82e978464daf55169bb8d135852ad58700aa96880653c3d8f7`,
  the digest `.github/workflows/ci.yml:648` pins, run with this repo's
  `ci/tempo/tempo-compare.yaml` unmodified) and against PulsusDB at
  `820e138`, same OTLP/JSON corpus pushed to both — 10 spans
  `resource.service.name="w2a"` and 4 `"w2b"` — with the filter
  `{resource.service.name="nope-w2"}`, which matches nothing:

  | `q` | reference | PulsusDB |
  |---|---|---|
  | `\| count_over_time()` | `{"series":[{"labels":[{"key":"__name__","value":{"stringValue":"count_over_time"}}]}],"metrics":{"completedJobs":1,"totalJobs":1}}` | **byte-identical** |
  | `\| rate()` | one `__name__="rate"` series, `value` omitted | **byte-identical** |
  | `\| count_over_time() by (resource.service.name)` | `{"metrics":{"completedJobs":1,"totalJobs":1}}` | `{"series":[],"metrics":{…}}` |
  | `\| max_over_time(duration)` | `{"metrics":{"completedJobs":1,"totalJobs":1}}` | one `__name__="max_over_time"` series, `value` omitted |
  | `\| quantile_over_time(duration, 0.5)` | `{"metrics":{"completedJobs":1,"totalJobs":1}}` | one `p=0.5` series, `value` omitted |
  | `\| histogram_over_time(duration)` | `{"metrics":{"completedJobs":1,"totalJobs":1}}` | `{"series":[],"metrics":{…}}` |

  Against a store with **no spans at all** the reference answers every one
  of the six with `{"metrics":{"completedJobs":1,"totalJobs":1}}`, including
  the ungrouped `count_over_time()`/`rate()` on which the two agree once any
  span exists; PulsusDB's six answers are unchanged from the table above.

- **Two mechanisms, deliberately recorded together and not split.**

  1. **Shape — `"series":[]` versus an absent `series` key.** protojson
     omits an empty repeated field, so the reference emits no `series` key;
     `serde` emits `"series":[]`. Both strict-decode as
     `tempopb.QueryInstantResponse` to zero series (verified with
     `jsonpb.Unmarshal` against the reference checkout's generated
     `tempopb`), so no client can tell them apart after decoding. This is
     the same mechanism as the range route's existing `"series":[]`, which
     `crates/pulsus-server/tests/api_conformance.rs` pins — it is one
     cross-route mechanism, and closing it on the instant route alone would
     create an asymmetry between our own two routes.
  2. **Semantics — a zero where the reference reports no-data.** For an
     ungrouped **aggregation** or **quantile** over an empty window we emit
     one series whose zero `value` is protojson-omitted, where the reference
     emits no series. Our `PlanKind::Agg` instant arm folds an absent row to
     `0.0` (`crates/pulsus-read/src/traces/exec.rs:1337-1349`, "an empty
     aggregate window is a 0-valued sample"), and the reference drops a
     series with no samples
     (`modules/frontend/metrics_query_handler.go:193-196` @ v3.0.2). This is
     the half a consumer can act on wrongly.

- **What a consumer sees.** An alert rule reading
  `max_over_time(duration)` through Grafana's Tempo datasource — which
  reaches this route by construction under Unified Alerting
  (`src/traceql/TempoQueryBuilderOptions.tsx:39,49-51` @ `v13.2.0`) —
  receives a series carrying a numeric zero from PulsusDB where Tempo
  returns no series. A rule written as "alert when the value drops below X"
  therefore fires on our no-data where it would stay silent on Tempo's.
  The reverse never happens: we never withhold a series the reference
  emits. `docs/api.md` §4.4 states the rule for readers in as many words —
  an absent `value` is a numeric zero, never no-data, and an empty `series`
  list is the only no-data signal — and
  `crates/pulsus-read/tests/traces_metrics_sql.rs`'s
  `shipped_metrics_shapes_and_limits_are_documented` pins that sentence.

- **Disposition.** Recorded, **not fixed**, and pre-existing: before wave 2
  the same series came back carrying a `samples[]` array, so wave 2 changed
  the envelope and nothing about which series exist or what they hold.
  Whether either half becomes an issue is deferred — mechanism 1 should be
  settled as one cross-route change alongside the range route's identical
  `"series":[]`, not on this route alone.

### `traceql-parse-error-body-differs-by-route` (issue #464, wave 2) — **a reference-side fact, recorded so the next comparison uses the right endpoint**

- **Route.** Three of them, which is the entire point of the row.

- **What.** The reference does not have *one* malformed-query body. For the
  same query `q=%7B` (a bare `{`), measured 2026-08-29 against the pinned
  container named in the entry above, all three answering `400`:

  | route | reference body, verbatim |
  |---|---|
  | `GET /api/metrics/query` | `compiling query: parse error at line 1, col 2: syntax error: unexpected $end` |
  | `GET /api/metrics/query_range` | `parse error at line 1, col 2: syntax error: unexpected $end` |
  | `GET /api/search` | `invalid TraceQL query: parse error at line 1, col 2: syntax error: unexpected $end` |

  Same parser, same message tail, three different prefixes: one, none, and a
  third.

- **Why this is a ledger row rather than a note.** A recorded claim of the
  form "the reference's parse-error body is X" is true on one of these
  routes and false on the other two, and the next person to check it will
  reach for whichever endpoint they happen to try — so the claim goes stale
  invisibly, without anything failing. The row therefore names the route in
  the row itself. It was found exactly that way: a plan cell carried the
  `/api/search` prefix against the `/api/metrics/query` row.

- **PulsusDB's side, unchanged.** We answer `400` with
  `content-type: text/plain; charset=utf-8` and our own message —
  `unexpected end of query at byte 1: expected a field, a literal, or '('`
  — on all three routes, carrying the byte offset the reference does not
  (`docs/api.md` §4.2, issue #384). Nothing here is a behaviour change and
  nothing is proposed: the row exists so that the *next* comparison is made
  against the right endpoint.

### `traceql-search-duration-ms-saturates-not-wraps` (issue #473) — **a value divergence at four wire fields, one operation**

- **Route.** `GET /api/traces/v1/search` and its `GET /api/search` alias —
  the only route that renders these fields
  (`crates/pulsus-server/src/traces_api/search.rs` is the sole caller of
  `search_response::render`). The trace-fetch route re-emits stored OTLP
  and is not described by this row.

- **What.** The response's `durationMs` is `uint32` on the wire
  (`TraceSearchMetadata.durationMs`, `pkg/tempopb/tempo.proto:139` @
  v3.0.2) and the reference fills it with an unchecked
  `uint32(spanset.DurationNanos / 1_000_000)`
  (`pkg/traceql/engine.go:295`), so a trace longer than ~49.7 days
  **wraps**. PulsusDB **saturates**: below `0` renders `0`, above
  `4294967295` renders `4294967295`. Two captured inputs, one output:

  | trace width | reference | PulsusDB |
  |---|---|---|
  | 2^53 + 1 ns (`9007199254740993`) | `417264662` | `4294967295` |
  | `i64::MAX` ns (`9223372036854775807`) | `2077252342` | `4294967295` |

  Ours is the **same number for both inputs**; the reference's two values
  differ. That is the whole discriminator: a wrapping renderer and a
  saturating one agree on every width at or below the boundary and can
  only be told apart by a pair like this one. The captured reference
  values live in
  `crates/pulsus-server/src/traces_api/search_response.rs`
  (`every_trace_width_renders_the_reference_captured_trace_object`) and
  are never edited to match us.

  The same operation covers the other three integers the response
  carries — `startTimeUnixNano` at the trace level and at the span level,
  and a span's `durationNanos`, all `uint64` on the wire
  (`pkg/tempopb/tempo.proto:138,159,160` @ v3.0.2). For those the upper
  clamp is inert (every `i64` fits a `u64`), so saturation there is
  exactly "below `0` renders `0`". A negative is reachable only from a
  write that bypassed `pulsus-write`; the projection exists so that such
  a value cannot render a minus-signed protojson string.

- **Why saturation and not the reference's wrap.** Saturation is
  monotonic and preserves a lower bound; wrapping does not. A 60-day
  trace wrapped is `889032704` ms — 10.3 days, an ordinary-looking
  duration a reader cannot distinguish from a genuinely short trace.
  Saturated it is `4294967295` ms — 49.7 days, visibly extreme, and a
  true lower bound. A consumer can act on "at least 49.7 days"; there is
  nothing to do with a plausible-looking number that is simply wrong.
  This is the same shape, for the same reason, as
  `detected-fields-limit-saturates-not-wraps` in
  `docs/benchmarks/logs-differential-ledger.md` — which is what makes it
  a rule rather than an ad-hoc call.

- **What a consumer sees, named at the client.** For a trace wider than
  ~49.7 days, the Duration column of a search result shows 49.7 days
  where the reference shows a shorter, plausible number. **Neither is the
  true duration.** For every other trace the two agree exactly. The
  reason this divergence is worth having at all is the other half of the
  change: a strict protobuf-JSON client decodes the search body with no
  per-field recovery, so a single out-of-domain integer returns an error
  instead of results and **one bad trace discards every trace of that
  response**. Before this change, a store containing one such trace made
  every search whose window matched it fail with an error where the
  results table should be — intermittent, data-dependent, and naming
  neither the field nor the trace.

- **How it is gated.**
  `crates/pulsus-server/src/traces_api/search_response.rs`:
  `duration_ms_saturates_into_the_wire_uint32_domain` (the boundary and
  the two-input identity),
  `every_trace_width_renders_the_reference_captured_trace_object` (the
  captured pair: the reference's values differ, ours are equal and equal
  to the maximum), and
  `every_integer_the_response_emits_lies_in_its_wire_domain` (the
  response-wide walk).
  `crates/pulsus-server/tests/traces_api_live.rs`:
  `search_response_integers_stay_inside_their_wire_domain_on_the_wire`
  asserts the wire bytes on the real route for the boundary width, the
  width one nanosecond below it, and an `i64::MAX` width.

- **Not referenced from `test/fixtures/traces/differential.json`, and it
  cannot be.** The search differential compares trace-ID **sets** only
  (`e2e/src/traces.rs`), so no fixture case can express a field-value
  divergence; a fixture entry that cannot carry the claim is not a record
  of it. `traceql-search-metrics-completed-jobs` above is the precedent
  for a ledger entry with no fixture case, and the fixture-to-ledger test
  (`informational_cases_are_recorded_in_the_committed_ledger`) constrains
  only the other direction. Adding a value-comparison axis to the
  differential is its own piece of work with its own measurement.

- **Also recorded here, out of scope and not fixed:**
  `crates/pulsus-read/src/traces/search_sql.rs` computes
  `max(timestamp_ns + duration_ns) AS trace_end_ns` in ClickHouse
  `Int64`, which wraps silently rather than erroring. That column feeds
  the `traceDuration` **intrinsic**, not the rendered `durationMs`, so it
  cannot reach this response — but `{ traceDuration > 1h }` would
  evaluate a negative width for a trace whose end overflows. Different
  surface, different rule; noted so the next person on that code finds it.

### `traces-v2-fetch-metrics-not-populated` (issue #474)

- **What:** `GET /api/v2/traces/{traceId}` returns an envelope whose
  field 2 (`metrics`) PulsusDB always emits present and at its default,
  encoding as the two bytes `12 00`. The reference populates it with a
  byte counter. Measured on the CI-pinned oracle
  (`grafana/tempo@sha256:aa8df8d069f77b82e978464daf55169bb8d135852ad5870
  0aa96880653c3d8f7`, the digest at `.github/workflows/ci.yml:648`),
  booted on this repository's own `ci/tempo/tempo-compare.yaml`.

- **Why this is an exclusion and not a frozen value.** The reference's
  own number is not stable, and — this is the part that matters — it does
  not vary per request either. It moves in **plateaus**: several adjacent
  fetches of the same trace, with no ingest in between, return the
  identical value, and a later fetch returns a different one. Values
  observed across three machines and three runs, all on the same single
  trace: `1204089baf03`, `120408b2de01` (twice, on two machines),
  `120408c8f906`, `120408acb60a`, `120408969b05`, `120408e4bc03`.
  **Two adjacent fetches agreeing therefore proves nothing about
  stability** — a check that sampled the value twice and saw agreement
  would have concluded, wrongly, that it could be frozen. The plateau, not
  the spread, is why the field is excluded from comparison rather than
  pinned.

- **What a consumer sees.** The Grafana Tempo datasource reads this block
  only to report bytes inspected for a trace fetch; PulsusDB has no
  equivalent read-accounting to report, so the number is zero rather than
  wrong. The block is **present and empty**, never absent — the
  datasource dereferences it without a nil check, which is the whole
  subject of issue #474.

- **How it is gated.** Every v2 comparison in the suite compares field 1
  (the trace) byte-for-byte and asserts our field 2 is exactly `12 00`;
  the reference's field 2 is **never** compared.
  `crates/pulsus-server/tests/trace_nullable_wire_differential.rs`
  (`the_committed_capture_matches_the_live_reference`) parses field 1 out
  of the reference's envelope and drops the rest;
  `crates/pulsus-server/tests/traces_api_live.rs`
  (`absent_submessages_are_materialized_present_and_empty_on_the_wire`)
  asserts our own whole body, tail included.

- **Precedent.** `traceql-search-metrics-completed-jobs` above already
  omits the same class of counter on the search surface, for the same
  reason.

### `traces-v2-fetch-get-only` (issue #474)

- **What:** `POST /api/v2/traces/{traceId}` is `405` with
  `Allow: GET,HEAD` for PulsusDB. The reference answers `200` with the
  same envelope a `GET` returns — measured on the same pinned oracle:
  `POST /api/v2/traces/<absent id>` → `200`, `Content-Length: 25`, body
  `{"trace":{},"metrics":{}}`. Its v1 twin, by contrast, answers `404` for
  the same `POST`, so the reference routes the verb on both routes and the
  difference between them is only the empty-trace status.

- **Why we do not match it.** The whole Tempo-compat alias surface is
  `GET`-only, and `api_conformance` already pins `405 Allow: GET,HEAD` on
  the v1 twin. Matching the reference here would make one route of
  fourteen answer a verb its thirteen siblings refuse, for no consumer:
  the datasource issues `GET` on trace-by-ID, and no named client sends
  `POST` to this path.

- **What a consumer sees.** Nothing, unless it sends a verb no known
  client sends. A caller that did would get `405` with an `Allow` header
  naming the verbs that work, rather than a body.

- **How it is gated.** `crates/pulsus-server/tests/api_conformance.rs`,
  `assert_traces_fetch_v2_route`'s `undocumented-method-405` cell, which
  also asserts the router-generated `405` carries no `Vary: accept`.

### `traces-fetch-json-emits-proto3-defaults` (issue #474) — **a representation difference on two named routes, recorded because nothing recorded it**

- **The routes.** `GET /api/traces/v1/trace/{traceId}`, its three aliases
  (`/api/traces/{traceId}`, `/api/traces/{traceId}/json`,
  `/tempo/api/traces/{traceId}`), and `GET /api/v2/traces/{traceId}` —
  in every case under `Accept: application/json`, and on the `/json`
  suffix route unconditionally. **The protobuf representation of all of
  these is unaffected**: it is byte-identical to the reference's, which
  is what issue #474 changed and what the wire tests gate.

- **What:** our JSON emits proto3 default values; the reference omits
  them. Measured on the CI-pinned oracle
  (`grafana/tempo@sha256:aa8df8d069f77b82e978464daf55169bb8d135852ad5870
  0aa96880653c3d8f7`, the digest at `.github/workflows/ci.yml:648`) and
  on a `pulsusdb` spawn, both fed the identical four-span fixture in
  `crates/pulsus-server/tests/fixtures/trace_nullable_wire/capture.json`.
  For the probe whose sender omitted `status` entirely, the span object
  reads:

  | | reference | PulsusDB |
  |---|---|---|
  | `status` | `{}` | `{"message":"","code":0}` |
  | `traceState`, `parentSpanId`, `flags` | omitted | `""`, `""`, `0` |
  | `attributes`, `events`, `links` | omitted | `[]`, `[]`, `[]` |
  | `droppedAttributesCount`, `droppedEventsCount`, `droppedLinksCount` | omitted | `0`, `0`, `0` |
  | `schemaUrl` (both levels) | omitted | `""` |
  | `resource`, `scope` | `{}` | every field spelled out at its default |

  Three further differences on the same bodies, measured at the same
  time and recorded here so a future JSON comparison uses the right
  expectations rather than rediscovering them: ids are **base64** on the
  reference and **hex** for us (`"uwAAAAAAAAE="` vs `"bb00000000000001"`);
  `kind` is the **enum name** on the reference and the **number** for us
  (`"SPAN_KIND_INTERNAL"` vs `1`), and likewise `status.code`
  (`"STATUS_CODE_ERROR"` vs `2` on the probe whose sender set a real
  status); and on the **v1** route only, the reference's top-level key is
  `batches` where ours is `resourceSpans` — on the **v2** route the
  reference uses `resourceSpans`, the same as ours. That last one is why
  this row names the route for every claim: the reference answers the
  same trace with two different key names depending on which of its own
  routes you ask.

- **Why we do not match it.** No consumer branches on the difference: a
  client decoding either body reads the same values, because an omitted
  proto3 field and a field present at its default are the same value by
  definition. The bar here is consumer impact, not byte identity.
  Matching would mean changing the JSON representation of the whole §4.1
  trace-fetch surface — the ids, the enums, the omission rule, and the v1
  top-level key — which moves an established response shape for every
  existing consumer of that route. That is a separate piece of work with
  its own measurement, and issue #474 put it out of scope explicitly.

- **What issue #474 did and did not change here.** Default emission is
  **pre-existing**, and that is checkable rather than asserted: the
  fields above that #474 never touches — `traceState`, `flags`,
  `droppedAttributesCount`, `schemaUrl` — are emitted at their defaults
  on the same bodies. (`kind` is deliberately NOT in that list: the
  fixture sets it to `1` and the proto3 default is `0`, so a body
  carrying `kind` shows a field with a value, not a default being
  emitted. Its numeric-versus-enum-name difference is real and is
  recorded above.) What #474 changed is three keys that
  previously serialised as JSON `null` (`resource`, `scope`, `status`,
  measured directly on an unfilled `TracesData`) and now serialise as
  default-valued objects. **`null` was further from the reference's `{}`
  than the default-valued object is**, so the change moves this
  representation toward the reference on those three keys while leaving
  the omission rule itself untouched.

- **Not documented before this row, and the prior claim was wrong.** An
  earlier draft of `docs/api.md` §8.1 justified our populated JSON body
  by citing "§4.1's OTLP protojson convention (hex ids, no default
  omission)". §4.1 documents the hex ids, the camelCase keys and the
  64-bit-integers-as-strings rule; **it has never documented a
  default-emission rule**, and the repository documents us *following*
  protojson default omission elsewhere (a zero sample `value`, a zero
  `completedJobs` — `docs/api.md` §4.4 and §4.2). The citation was
  circular: the only text in the tree saying "no default omission" was
  the §8.1 sentence doing the citing. §4.1 now states the actual
  behaviour and points here.

- **How it is gated.** Nothing gates the populated JSON body against the
  reference, and that is deliberate rather than an omission: there is no
  captured reference JSON to compare against, because this row exists to
  say the two are not comparable. What **is** gated is the part that must
  match — the absent-trace v2 JSON body, byte-exact at 25 bytes, in
  `crates/pulsus-server/tests/trace_nullable_wire_differential.rs`
  (against the live reference) and in
  `crates/pulsus-server/tests/api_conformance.rs`
  (`assert_traces_fetch_v2_route`, against a live spawn) — and our own
  populated JSON shape, in
  `crates/pulsus-server/src/traces_api/fetch_v2.rs`
  (`the_populated_json_envelope_nests_resource_spans_under_trace`), which
  asserts the `status` object this row describes so a change to the
  serializer reddens rather than passing silently.

### `traceql-intrinsic-scope-unserved-names` (issue #475) — **four intrinsics we deliberately do not offer as tags**

- **Route.** `GET /api/traces/v1/tags`, `GET /api/v2/search/tags` and
  `GET /api/search/tags` — the `intrinsic` scope's tag list. The two
  values routes are **unaffected** and the row says so explicitly:
  `GET /api/v2/search/tag/nestedSetLeft/values` (and its native twin)
  answers `200 {"tagValues":[]}`, because an intrinsic absent from the
  served NAME list is still an intrinsic for a value LOOKUP.

- **What.** `pulsus_traceql::Intrinsic` has 21 variants
  (`crates/pulsus-traceql/src/ast.rs`). Four of them carry an explicitly
  EMPTY spelling list and are therefore absent from the served scope:
  `span:childCount`, `nestedSetParent`, `nestedSetLeft`,
  `nestedSetRight`. The remaining 17 contribute 25 spellings (8 variants
  with two spellings, 9 with one), which is the served list.

- **Why they are absent.** They are query-time structural properties of a
  span, not tags a user picks from a dropdown; offering them would put
  values in an autocomplete that no value lookup can ever populate. The
  list of 25 was measured equal, byte for byte, to the pinned reference
  build's own `intrinsic` scope during planning on 2026-08-30 (issue
  #475), so this is a decision recorded against our own vocabulary rather
  than a divergence from the reference's.

- **What is gated.** `crates/pulsus-server/src/traces_api/intrinsics.rs`
  pins the 25 names as a literal and asserts the four empty slices;
  `crates/pulsus-traceql/src/ast.rs` asserts that both discovery matches
  have NO wildcard arm, so a variant added later cannot go silently
  unserved — it fails to compile until someone chooses.

### `traceql-tag-discovery-ordering` (issue #475) — **ours is deterministic where the reference's varies between requests**

- **Route.** `GET /api/v2/search/tags` (the `scopes` order) and
  `GET /api/v2/search/tag/{tag}/values` (the value order). The native
  twins are identical to the aliases by construction.

- **What was measured.** During planning on 2026-08-30 against the pinned
  reference build, three requests to the SAME process over the SAME
  corpus that select the same scope set — unscoped, `?scope=none`,
  `?scope=` — returned three different scope orders, and four consecutive
  `status` value requests returned three different value orders.

- **Ours.** The `intrinsic` scope first, then the catalog scopes in
  `(scope, key)` ascending order; values ascending for a catalog read and
  in grammar order for the two closed keyword sets. Deterministic on
  every request.

- **Disposition.** A deliberate refinement, no code proposed. A client
  that depends on the order gets a stable answer from us and an unstable
  one from the reference, so any future comparison of these bodies must
  compare SETS, not sequences.

### `traceql-intrinsic-shadows-attribute-lookup` (issue #475) — **a bare intrinsic spelling wins over an attribute of the same name**

- **Route.** `GET /api/traces/v1/tag/{tag}/values` and
  `GET /api/v2/search/tag/{tag}/values` **only** — explicitly NOT
  `GET /api/search/tag/{tag}/values`, which keeps the attribute-only
  reading (see `traceql-v1-tag-values-statics-unimplemented`).

- **What.** On those two routes a `{tag}` that is an intrinsic spelling is
  answered from the static vocabulary and the catalog is not read. So in
  a store holding a user attribute keyed `status`, `.../tag/status/values`
  answers `ok`/`error`/`unset`, not that attribute's values. The attribute
  remains reachable by its scoped spelling: `.../tag/span.status/values`
  answers `degraded` on the same store.

- **Why this is the right shape.** The alternative — union the static list
  with a catalog read — leaves the collision in place: the dropdown then
  offers both the keywords and whatever a same-named attribute happens to
  hold, which is the wrong-values case the issue exists to remove. The
  reference does not read its store for these keys at all.

- **Gated by.** `crates/pulsus-server/tests/traces_tags_live.rs`
  (`intrinsic_discovery_answers_from_the_vocabulary_and_reads_no_trace_table`)
  on a corpus that carries the colliding `status` attribute, so the two
  behaviours give different bytes.

### `traceql-v1-tag-values-statics-unimplemented` (issue #475) — **the v1 flat values route serves no static values, matching the reference**

- **Route.** `GET /api/search/tag/{tag}/values`. The row also states the
  other half: `GET /api/v2/search/tag/{tag}/values` answers the same two
  keys from our statics.

- **What was measured.** During planning on 2026-08-30 against the pinned
  reference build, on a store seeded with a user attribute keyed
  `status`: `/api/search/tag/status.code/values` →
  `{"tagValues":["error","ok","unset"]}` and
  `/api/search/tag/error/values` → `{"tagValues":["true"]}` (its own v1
  static map), while `/api/search/tag/status/values` →
  `{"tagValues":["degraded"]}` — its store read, not a static.

- **Ours.** `GET /api/search/tag/status/values` → `{"tagValues":["degraded"]}`
  — the same store-read answer. We implement none of the reference's v1
  static map (`status.code`, `error`), and no client of ours reads status
  values off the v1 route.

- **Extended by issue #478.** The same row now also covers `name`: the
  native and v2 routes answer it from `trace_spans`, and the v1 flat
  route keeps its attribute-only reading, so `GET /api/search/tag/name/values`
  answers `{"tagValues":[]}` on a corpus with no attribute keyed `name`
  where the reference answers with its span names. Measured on the
  captured corpus. The v1 route also ignores `q`, on both sides.

- **Cases.** `Q-AQ`, `Q-AR`.

- **Disposition.** No code. The split is deliberate parity: a v1 lookup
  conflates an intrinsic with an attribute of the same name, which is
  exactly the defect removed on v2.

### `traceql-intrinsic-values-empty-pending-span-names` (issue #475) — **an open-valued intrinsic answers empty where the reference answers from its store**

- **Route.** `GET /api/traces/v1/tag/{tag}/values` and
  `GET /api/v2/search/tag/{tag}/values`.

- **What.** For an intrinsic with no closed value set — `duration`,
  `rootName`, `span:id`, every one except `name`, `status` and `kind` —
  we answer `200 {"tagValues":[]}`. Measured during planning on
  2026-08-30, the pinned reference build answers
  `/api/v2/search/tag/name/values` with the three **span** names its
  store holds.

- **`name` left this row in issue #478**, which is the work the last
  bullet below anticipated: it is served from `trace_spans` on the native
  and v2 routes, through a day-grain projection, and the remaining
  open-valued intrinsics are unchanged. The row stays for them.

- **Why empty rather than a store read.** Before issue #475 that lookup
  fell through to a bare-key catalog read, which answered with span
  EVENT names (the catalog carries one `event:intrinsic`/`name` row per
  span event) — a syntactically valid query against a real intrinsic,
  populated with values from the wrong thing, and occasionally matching
  by coincidence when an event and a span share a name. An empty list is
  legible; that was not.

- **What remains.** The other open-valued intrinsics. Each would need its
  own read and its own index story — `span:id` and `duration` in
  particular enumerate values that are not a discoverable set — so this
  row is the placeholder for that decision, not a permanent one.

### `traceql-v1-flat-tag-names-order` (issue #475) — **catalog order, not sorted**

- **Route.** `GET /api/search/tags`.

- **What.** Our flat `tagNames` are the distinct keys in catalog
  `(scope, key)` order, deduplicated on first occurrence. Measured during
  planning on 2026-08-30, the pinned reference build returns the same SET
  sorted ascending. Pre-existing, unchanged by issue #475; recorded
  because the two are byte-different and a future comparison must compare
  sets.

### `traceql-v1-flat-empty-value-dropped` (issue #475) — **we emit the empty string, the reference omits the element**

- **Route.** `GET /api/search/tag/{tag}/values`.

- **What.** For an attribute whose stored value is the empty string, our
  v1 flat body is `{"tagValues":[""]}`; measured during planning on
  2026-08-30, the reference returns `{"tagValues":[]}`.

- **Note the asymmetry with the typed routes, which issue #475 changed.**
  On the native and v2 typed routes an empty value now omits the `value`
  key (`{"type":"string"}`), the canonical protobuf JSON mapping for a
  default-valued scalar. The v1 flat projection has no key to omit — the
  element is the value — so matching the reference there would mean
  dropping the element, which changes the list length. Not done, not
  proposed; recorded so the difference between the two shapes is not read
  as an oversight in one of them.

### `traceql-v2-bare-attribute-key-accepted` (issue #475) — **we accept a bare non-intrinsic key where the reference rejects it**

- **Route.** `GET /api/v2/search/tag/{tag}/values`. The native twin
  (`GET /api/traces/v1/tag/{tag}/values`) has no reference counterpart,
  and this row does not imply one.

- **What.** `GET /api/v2/search/tag/spanID/values` answers `200` with the
  matching attribute values from the five attribute scopes. Measured
  during planning on 2026-08-30, the pinned reference build answers `400`
  (`please provide a valid tagName: …`) for every bare key that is not an
  intrinsic spelling, and likewise rejects a `trace.`-prefixed key, which
  we treat as an ordinary bare key.

- **Disposition.** A documented superset, unchanged by issue #475. Our
  acceptance is strictly more permissive, so no client that works against
  the reference breaks against us.

### `traceql-tag-routes-metrics-object-absent` (issue #475) — **the reference's tag bodies carry a `metrics` object we do not emit**

- **Route.** Measured on three: `GET /api/v2/search/tags`,
  `GET /api/search/tags` and `GET /api/v2/search/tag/{tag}/values`. **Not
  captured on `GET /api/search/tag/{tag}/values`** — stated rather than
  assumed.

- **What.** Measured during planning on 2026-08-30, the pinned reference
  build's tag bodies carry a trailing `"metrics":{...}` (for example
  `{"scopes":[],"metrics":{"inspectedBytes":"18694"}}`), present even when
  the answer is empty. Ours carry no such key on any tag route.

- **Disposition.** Docs-only, no code. Recorded separately from
  `traceql-intrinsic-scope-on-empty-store`, which cross-references it, so
  the two are not read as one difference.

### `traceql-intrinsic-scope-on-empty-store` (issue #475) — **a reference-side fact, recorded because our contract would have gone wrong silently**

- **Route.** `GET /api/v2/search/tags?scope=intrinsic` and the unscoped
  `GET /api/v2/search/tags`, against a store holding no spans.

- **What was measured.** During plan review on 2026-08-30, against an
  EMPTY store, the pinned reference build returned `200` with `scopes` =
  one entry named `intrinsic` carrying the 25 names, and answered the
  unscoped route with the same intrinsic-only body; `?scope=resource` and
  `?scope=span` each returned `{"scopes":[],"metrics":{}}`, and
  `/api/v2/search/tag/name/values` returned `{"tagValues":[],"metrics":{}}`.

- **Ours.** The same 25 names in the same order, without the `metrics`
  object — whose absence is the separate row
  `traceql-tag-routes-metrics-object-absent` above.

- **Why the row exists.** Our static lists are unconditional by design, so
  an empty store still returns them. That was our own contract before this
  measurement and would have become wrong, with nothing failing to say so,
  if the reference had gated its list on store contents. It does not.
  **No code change.** The empty-database cells in
  `crates/pulsus-server/tests/api_conformance.rs` are what hold our half.

### `traceql-untyped-intrinsic-cross-type-operand` (issue #476) — **a cross-type `=`/`!=` on an untyped intrinsic**

- **Route.** `GET /api/search`, and the same leaf on
  `GET /api/metrics/query_range`.

- **Queries.** `{instrumentation:name=5}`, `{instrumentation:version=5}`
  and the `!=` forms. The reference's `impliedType` has no arm for either
  intrinsic, so its own validator accepts them.

- **Three measured answers** (captured during planning on 2026-08-31
  against the pinned reference build):
  - the reference, on a store holding a block in range: **`500`**, with a
    store-internal message;
  - the same reference, on a time range selecting no block: `200`
    `{"traces":[]}`;
  - PulsusDB: `200` with no matching span.

- **Chosen: ours.** The reference's own validator accepts the query, and a
  `500` for something a server has just validated is not behaviour to
  reproduce. It is also the answer the surrounding language already gives:
  a cross-type comparison resolves to "no match" for every other operator
  here, so our previous `400` was the outlier, not the `200`.

- **Disposition.** Ratify-documented-difference. No fixture case references
  this entry — the divergence is in a status code the differential corpus
  does not exercise, and it is recorded so the next reader does not treat
  the reference's `500` as the target.

### `traceql-tag-values-q-lenient-parse-not-reproduced` (issue #478) — **an unparseable `q` widens here and sometimes narrows there**

- **Route.** `GET /api/traces/v1/tag/{tag}/values` and
  `GET /api/v2/search/tag/{tag}/values`.

- **What.** Both sides answer `200` to a `q` that is well-formed input
  and does not parse as TraceQL — that much is parity, and it is the
  load-bearing half: the query editor sends the whole half-typed
  expression as `q` on every distinct prefix a user types through, so a
  `400` there would break autocomplete for input the client cannot avoid
  sending. (Two classes ARE rejected on our side below the interpretation
  layer, by the HTTP transport, and neither is a shape an editor emits:
  raw invalid UTF-8 in the request target is `400`, and a `q` past the
  64 KiB request-target bound is `414`. Both are bounded and measured in
  `crates/pulsus-read/src/traces/tag_narrow.rs`'s module doc and pinned
  by `the_q_tolerance_stops_at_input_that_is_not_well_formed`; the
  reference was not probed for either, so no parity claim is made about
  them.) What differs is the ANSWER. We drop the
  whole unparseable `q` and return the unnarrowed list. Measured on the
  captured corpus, the reference sometimes returns a NARROWED list
  instead — it recovers complete condition groups from the incomplete
  text and applies them, and once returned an EMPTY list for a prefix we
  answer in full.

- **Cases.** `Q-I`, `Q-J`, `Q-L`, `Q-M`. (`Q-K` and the fourteen other
  malformed shapes agree: both sides answer the full list.)

- **Disposition.** Ratify-documented-difference. Reproducing the
  reference's incomplete-matcher recovery would mean a second, lenient
  parser beside the real one, whose disagreements with the real one are
  exactly the bugs a query language cannot afford. Widening is always a
  superset of the correct answer for a prefix that is still being typed,
  which is the direction a dropdown can absorb.

- **Gated by.** `crates/pulsus-server/tests/trace_tag_values_differential.rs`
  (the oracle leg replays the reference side) and
  `crates/pulsus-server/tests/traces_tag_values_narrow_live.rs` (ours).

### `traceql-tag-values-q-partial-pushdown` (issue #478) — **we push the `&&` spine only, so an `||` widens**

- **Route.** `GET /api/traces/v1/tag/{tag}/values` and
  `GET /api/v2/search/tag/{tag}/values`.

- **What.** Only positive conjuncts on the root filter's `&&` spine are
  pushed into the value read. A `||` subtree, a `!`, a negated attribute
  condition, a structural root (`{a} >> {b}`), a pipeline stage and
  anything past the eighth term are DROPPED — every drop widens, because
  a conjunction with a conjunct removed matches a superset. The reference
  narrows on the `||` case; we return the full list.

- **Cases.** `Q-W`.

- **Disposition.** Ratify-documented-difference, with room to close. The
  rule that makes it safe is structural rather than case-by-case: a term
  is only ever taken from the `&&` spine, so no drop can narrow.

- **Gated by.** `crates/pulsus-read/src/traces/tag_narrow.rs`'s own unit
  tests for the drop rules, plus the two legs above.

### `traceql-tag-values-unscoped-attr-narrows-here` (issue #478) — **the unscoped `.attr` form narrows for us and does not there**

- **Route.** `GET /api/traces/v1/tag/{tag}/values` and
  `GET /api/v2/search/tag/{tag}/values`.

- **What.** `q={.http.method="GET"}` is an unscoped attribute condition.
  We push it as an index probe with no `scope` predicate, so it matches
  the key in every attribute scope and the answer narrows. Measured, the
  reference returns the unnarrowed list for the same query on the same
  corpus.

- **Cases.** `Q-X`.

- **Disposition.** Ratify-documented-difference. Narrowing is what the
  dropdown is for and the unscoped form is the one the editor emits when
  a user types a bare key; answering it unnarrowed would be the defect
  this issue exists to remove.

### `traceql-tag-values-requested-tag-condition-applied` (issue #478) — **a condition on the tag being listed applies here**

- **Route.** `GET /api/traces/v1/tag/{tag}/values` and
  `GET /api/v2/search/tag/{tag}/values`.

- **What.** When `q` carries a condition on the very tag whose values are
  being listed — `/tag/name/values?q={name="pay"}` — we apply it like any
  other conjunct, so the answer is the values that satisfy it (none, for
  a name no span carries). Measured, the reference ECHOES the requested
  condition's operand instead: it answered `["pay"]` for a corpus holding
  no span of that name, and answered the unnarrowed list for the regex
  form `{name=~".*charge.*"}` where we answer the two matching names.

- **Cases.** `Q-Y`, `Q-Z`.

- **Disposition.** Ratify-documented-difference. Offering a value no span
  carries is a dropdown entry that matches nothing when picked.

### `traceql-tag-values-window-is-day-granular` (issue #478) — **the window resolves to whole UTC days**

- **Route.** `GET /api/traces/v1/tag/{tag}/values` and
  `GET /api/v2/search/tag/{tag}/values`, for the reads that touch the
  span tables — the span-`name` values and any `q`-narrowed values. An
  unnarrowed attribute-value read is still the time-less catalog read and
  ignores the window entirely.

- **What.** A supplied `start`/`end` is widened to the UTC days it
  touches, and an absent one resolves to `reader.traceql_tag_lookback`
  (24 h). A sub-day window therefore answers over its whole day here. The
  sharpest instance is a zero-width range (`start == end`): the reference
  answers `[]`, we answer that day's values.

- **Why day-granular.** A sub-day `timestamp_ns` predicate prunes nothing
  on `trace_spans` — the sorting key is `(trace_id, timestamp_ns)`, so
  with `trace_id` unconstrained the second key column cannot prune — and
  it would defeat the `span_name_day` projection, whose own key is the
  day expression. So the finer predicate costs the same and buys a
  narrower answer only by accident of where a day boundary falls.

- **Cases.** `Q-AZ`.

- **Disposition.** Ratify-documented-difference. Over-reporting inside a
  day is the safe direction for a dropdown: a value that exists is
  offered.

### `traceql-tag-values-span-name-byte-cap` (issue #478) — **an over-long span name is reported capped**

- **Route.** `GET /api/traces/v1/tag/{tag}/values` and
  `GET /api/v2/search/tag/{tag}/values`, for `name`/`span:name`.

- **What.** A span name longer than 8,192 bytes is reported as its first
  2,048 code points — the same byte cap every other string column on this
  surface carries, so one cap rule covers the search results and the tag
  values. Measured on a 9,000-character name, the reference returns the
  whole name.

- **Cases.** `T-CAP`.

- **Disposition.** Ratify-documented-difference. The cap is the shipped
  rule for string columns on this surface; exempting one read would make
  the same name render two lengths in one product.

### `traceql-tag-values-range-error-text` (issue #478) — **same status and content type on a range fault, our own wording**

- **Route.** All six §4.3 routes: `GET /api/traces/v1/tags`,
  `GET /api/v2/search/tags`, `GET /api/search/tags`,
  `GET /api/traces/v1/tag/{tag}/values`,
  `GET /api/v2/search/tag/{tag}/values` and
  `GET /api/search/tag/{tag}/values`.

- **What.** A malformed timestamp, a half-supplied range and an inverted
  range are `400 text/plain; charset=utf-8` on both sides — measured, on
  every one of the reference's four tag routes. The BODY text is ours: it
  follows the §4.2 range grammar this product already ships, where the
  reference's ends in its runtime's own integer-parse error for one shape
  and names a configured maximum-window-width setting for another. The
  reference's bodies are deliberately not quoted in the fixture; the
  status and the content type are what is asserted against it.

- **Also.** We have no maximum-window-width rule, so a window wider than
  the reference's configured maximum is answered here and rejected
  there. The bound that exists instead is the reader row budget
  (`reader.traceql_scan_budget_rows`), which is a `422 query_too_broad`.

- **Cases.** `/api/search/tag/name/values|half_end`, `/api/search/tag/name/values|half_start`, `/api/search/tag/name/values|inverted`, `/api/search/tag/name/values|malformed_end`, `/api/search/tag/name/values|malformed_start`, `/api/search/tag/name/values|zero_end`, `/api/search/tag/name/values|zero_start`, `/api/search/tags|half_end`, `/api/search/tags|half_start`, `/api/search/tags|inverted`, `/api/search/tags|malformed_end`, `/api/search/tags|malformed_start`, `/api/search/tags|zero_end`, `/api/search/tags|zero_start`, `/api/v2/search/tag/name/values|half_end`, `/api/v2/search/tag/name/values|half_start`, `/api/v2/search/tag/name/values|inverted`, `/api/v2/search/tag/name/values|malformed_end`, `/api/v2/search/tag/name/values|malformed_start`, `/api/v2/search/tag/name/values|zero_end`, `/api/v2/search/tag/name/values|zero_start`, `/api/v2/search/tags|half_end`, `/api/v2/search/tags|half_start`, `/api/v2/search/tags|inverted`, `/api/v2/search/tags|malformed_end`, `/api/v2/search/tags|malformed_start`, `/api/v2/search/tags|zero_end`, `/api/v2/search/tags|zero_start`.

- **Disposition.** Ratify-documented-difference. Matching the status is
  what a client can act on; reproducing another runtime's parse-error
  text is not parity, it is imitation.

### `traceql-tag-values-narrowed-set-complete-here` (issue #478) — **the reference under-reports a narrowed value list; we return the complete set**

- **Route.** `GET /api/v2/search/tag/{tag}/values`.

- **What was measured.** On the captured corpus, with the same window:
  `/api/v2/search/tag/span.http.status_code/values?q={resource.service.name="pay"}`
  returned `[201]` from the reference, while its own
  `/api/v2/search/tag/name/values` for the SAME condition listed the span
  named `a`, and `/api/v2/search/tag/span.http.status_code/values?q={name="a"}`
  returned `[200]`. So a value the condition matches was absent from the
  narrowed list. The same shape appeared for the `checkout` service (only
  `500`, with `200` missing) and did not appear for `cart`. Reproduced on
  two independent container runs.

- **Ours.** Both values, `200` and `201`: the narrowed read is a `DISTINCT`
  over the index rows of the matching span set, so it cannot omit a value
  that set carries.

- **Cases.** `Q-AM`.

- **Disposition.** Deliberate divergence — the reference is wrong here.
  A dropdown that omits a value present on a matching span sends the user
  to a query that returns nothing. **The mechanism behind the reference's
  omission was not established**, only the observation; recorded so the
  next reader does not treat its answer as the target.
