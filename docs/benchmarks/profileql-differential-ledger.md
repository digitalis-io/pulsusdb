# Profiles differential ledger

The profiles differential (issue #822) sends one fixture through every
profiles receiver of PulsusDB and of the pinned profiles reference, under two
tenants, and compares the answers to the requests the datasource and the
exploration app send. The suite is
`crates/pulsus-server/tests/profileql_differential.rs`.

Each row below is one recorded difference: a route and a request on which
PulsusDB answers differently from the reference on purpose. A difference
that is not a row here fails the suite.

- **Rows are never deleted.** A row the reference stops differing on is
  marked retired, with the date and the reason, and keeps its heading.
- **A row is added by decision, not by the suite's author.** A new
  difference is reported with both answers; fixing PulsusDB or recording
  the difference is decided outside the change that found it, and a
  recorded difference is the owner's decision.
- **Case** names the suite's case that pins both answers, the issue whose
  part adds one, or `none` with the reason. A pinned case fails when the
  two answers agree, because then the reference has changed and the row is
  to be retired.

### `tenancy-absent-header-is-the-empty-tenant`

- **Route:** any profiles route.
- **Request:** no `X-Scope-OrgID` header.
- **PulsusDB:** the request is served as the empty tenant: `ProfileTypes`
  answers `200` with an empty body.
- **Reference:** `401`.
- **Why:** owner decision, Q13 answer; high-level design §2.1.
- **Case:** `C0` in `crates/pulsus-server/tests/profileql_differential.rs`.

### `tenancy-header-bytes-refused`

- **Route:** any profiles route.
- **Request:** `X-Scope-OrgID: a|b`, or a tenant holding any of `!*'()`.
- **PulsusDB:** `400`.
- **Reference:** reads both tenants of `a|b`, and accepts those bytes in a
  tenant.
- **Why:** owner decision, Q12 answer.
- **Case:** `none`.

### `catalogue-exact-window`

- **Route:** the four catalogue methods: profile types, label names, label
  values and series.
- **Request:** a window excluding a profile that shares a storage block with
  one inside it.
- **PulsusDB:** the profile outside the window is not listed.
- **Reference:** it is listed.
- **Why:** R7; high-level design §12.
- **Case:** `none`: it needs control of the reference's blocks.

### `selector-parse-error-text`

- **Route:** label names, label values, series.
- **Request:** an invalid label selector.
- **PulsusDB:** `400 invalid_argument`, with our selector parser's text.
- **Reference:** `400`, with its own parser's text.
- **Why:** #820 plan §2.
- **Case:** `none`.

### `otlp-unconvertible-is-400`

- **Route:** `/v1development/profiles`.
- **Request:** a profile that cannot be converted, or a request with no
  dictionary.
- **PulsusDB:** `400`, a `google.rpc.Status` body.
- **Reference:** `500`, a plain-text body.
- **Why:** owner, 2026-11-16, "400, a recorded difference" (#815).
- **Case:** `none`.

### `relative-time-adds-every-pair`

- **Route:** a route that parses relative times.
- **Request:** `from=now-1h30m`.
- **PulsusDB:** 90 minutes ago.
- **Reference:** 1 hour ago.
- **Why:** owner, 2026-11-16, "Parse correctly" (#821).
- **Case:** P17 (the render routes).

### `flamegraph-named-other-keeps-cut-values`

- **Route:** the merged flame graph.
- **Request:** a node limit that cuts a parent's small children while a kept
  child is named `other`; for example the rows `main→a` 10, `main→b` 1 and
  `main→other` 5 with a limit of 3.
- **PulsusDB:** the kept `other` takes the cut children's sum: total 16,
  `other` 6.
- **Reference:** the cut children's sum is lost: total 15.
- **Why:** `docs/reference-defects-we-do-not-copy.md` entry 41.
- **Case:** `C20` in `crates/pulsus-server/tests/profileql_differential.rs`.

### `series-average-is-the-mean`

- **Route:** `SelectSeries`.
- **Request:** `aggregation` `TIME_SERIES_AGGREGATION_TYPE_AVERAGE`.
- **PulsusDB:** each step's value is the mean of the step's profile totals.
- **Reference:** the sum, whatever the aggregation asked for.
- **Why:** `docs/reference-defects-we-do-not-copy.md` entry 42.
- **Case:** `none`: no front-end client sends `aggregation` or a call site on
  `SelectSeries`; pinned by `profiles_series_live.rs` (entry 42).

### `series-call-site-filters-the-totals`

- **Route:** `SelectSeries`.
- **Request:** a `stack_trace_selector` with a call site.
- **PulsusDB:** each profile's total counts only the samples on stacks
  beginning with the call site.
- **Reference:** the selector is dropped: every stack's total.
- **Why:** `docs/reference-defects-we-do-not-copy.md` entry 42.
- **Case:** `none`: no front-end client sends `aggregation` or a call site on
  `SelectSeries`; pinned by `profiles_series_live.rs` (entry 42).

### `ingest-folded-long-line-is-read`

- **Route:** `/ingest`, folded format.
- **Request:** a body with a line of 64 KiB or more.
- **PulsusDB:** every line is stored, however long.
- **Reference:** the lines before the long one are stored, the long line and
  every line after it are dropped, and the push answers `200`.
- **Why:** `docs/reference-defects-we-do-not-copy.md` entry 43.
- **Case:** `none`: a rare input; pinned by the unit test its defects entry names.

### `ingest-profile-id-push-is-stored`

- **Route:** `/ingest`, folded, lines, trie, tree or speedscope format.
- **Request:** a `name` whose labels hold a non-empty `profile_id`.
- **PulsusDB:** the profile is stored, and `profile_id` is a series label.
- **Reference:** nothing is stored, and the push answers `200`.
- **Why:** `docs/reference-defects-we-do-not-copy.md` entry 44.
- **Case:** `none`: a rare input; pinned by the unit test its defects entry names.

### `ingest-speedscope-values-computed-before-truncation`

- **Route:** `/ingest`, speedscope format.
- **Request:** fractional weights, open and close gaps, or the
  `nanoseconds` unit; for example `O a@0, O b@3, C b@5, C a@6` in unit
  `none`.
- **PulsusDB:** a value is multiplied before it is truncated, both gaps take
  the multiplier, and a nanosecond is stored as one: `a` 40,000,000 and `b`
  20,000,000 in the example.
- **Reference:** a weight of `0.5` stores 0, the example stores `a` as
  10,300,000, and a `nanoseconds` file stores nothing and answers `200`.
- **Why:** `docs/reference-defects-we-do-not-copy.md` entry 45.
- **Case:** `none`: a rare input; pinned by the unit test its defects entry names.

### `ingest-tree-same-name-siblings-sum`

- **Route:** `/ingest`, tree format.
- **Request:** two sibling nodes of one name under one parent.
- **PulsusDB:** their values sum.
- **Reference:** the later sibling's value replaces the earlier one's.
- **Why:** `docs/reference-defects-we-do-not-copy.md` entry 46.
- **Case:** `none`: a rare input; pinned by the unit test its defects entry names.

### `ingest-speedscope-error-text`

- **Route:** `/ingest`, speedscope format.
- **Request:** a malformed speedscope body.
- **PulsusDB:** our JSON reader's text.
- **Reference:** its JSON reader's text.
- **Why:** #818 plan §8.
- **Case:** `none`: error answers are not compared (#822 plan §10).

### `ingest-jfr-delay-in-nanoseconds`

- **Route:** `/ingest`, JFR format.
- **Request:** a recording holding `jdk.JavaMonitorEnter` or `jdk.ThreadPark`
  events on a clock other than one tick per nanosecond.
- **PulsusDB:** each delay is converted to nanoseconds with its chunk's
  ticks per second: the library's `example.jfr` stores about 8,005,192,311.
- **Reference:** the tick count is stored as nanoseconds: 18,443,760,794.
- **Why:** `docs/reference-defects-we-do-not-copy.md` entry 47.
- **Case:** `VJ` in `crates/pulsus-server/tests/profileql_differential.rs`
  (#819), which pins both totals.

### `ingest-jfr-event-count-not-carried`

- **Route:** `/ingest`, JFR format.
- **Request:** a recording holding `profiler.WallClockSample` events with a
  `samples` count above 1 and later CPU, allocation or lock events.
- **PulsusDB:** each of those later events counts once.
- **Reference:** each counts as the last wall-clock sample's count.
- **Why:** `docs/reference-defects-we-do-not-copy.md` entry 48.
- **Case:** `none` (#819): the fixture's `example.jfr` holds no wall-clock
  sample, so the two count each event once there; our side is pinned by J6
  in `crates/pulsus-write/src/protocols/profileql_legacy/jfr/mod.rs`.

### `ingest-jfr-series-stored-together`

- **Route:** `/ingest`, JFR format.
- **Request:** a recording of several profiles, one of which the checks
  refuse.
- **PulsusDB:** nothing is stored, and the refusal names index 0.
- **Reference:** each series is pushed on its own, so the others are stored,
  and the refusal names the refused series' position.
- **Why:** atomic writes; #819 plan E6 and E7.
- **Case:** `none` (#819): it needs a series the checks refuse, which the
  fixture does not push.

### `otlp-multiple-values-are-summed`

- **Route:** `/v1development/profiles`.
- **Request:** a sample with several `values`, for example `[2,2,3,3]`.
- **PulsusDB:** each value is one observation, so the sample's value is
  their sum, `10`, for every sample type.
- **Reference:** keeps one value (`2`) or refuses the profile, summing only
  for off-CPU profiles.
- **Why:** R3; high-level design §6, the mapping.
- **Case:** `none`: the fixture's OTLP samples carry one value and no link.

### `otlp-json-link-ids-are-hex`

- **Route:** `/v1development/profiles`, OTLP/JSON.
- **Request:** a sample with a link.
- **PulsusDB:** span and trace ids are read as hex, as every OTLP/JSON
  route reads them (`docs/api.md` §1.1).
- **Reference:** read as base64 under the generic JSON mapping, so other
  ids are stored.
- **Why:** high-level design §6, last paragraph.
- **Case:** `none`: the fixture's OTLP samples carry one value and no link.

### `heatmap-absent-query-type-is-400`

- **Route:** `SelectHeatmap`.
- **Request:** no `queryType`, or an unknown one.
- **PulsusDB:** `400 invalid_argument` `valid heatmap query type must be
  specified`, on every window.
- **Reference:** `500` with that text when the window holds data, and an
  empty answer when it holds none.
- **Why:** owner, 2026-11-17 (#825).
- **Case:** `none`: an error answer.

### `heatmap-series-ordered-by-labels`

- **Route:** `SelectHeatmap`.
- **Request:** a heatmap of several series, below the limit.
- **PulsusDB:** the series are ordered by their labels.
- **Reference:** by the text of internal reference numbers written in hex,
  which follows no label or value order.
- **Why:** owner, 2026-11-17 (#825).
- **Case:** `none`: not met, the cases hold one series.

### `heatmap-span-one-value-per-profile`

- **Route:** `SelectHeatmap`, `HEATMAP_QUERY_TYPE_SPAN`.
- **Request:** two profiles carrying one span.
- **PulsusDB:** one value per profile and span, whether or not their
  timestamps coincide.
- **Reference:** one value when they share a millisecond, two when they are
  a millisecond apart.
- **Why:** #825's code review note 3.
- **Case:** `none`: two profiles carrying one span in one millisecond.

### `exemplar-split-profile-per-part`

- **Route:** `SelectSeries`, `SelectHeatmap`.
- **Request:** exemplars, over a profile whose samples carry differing
  labels; for example one profile of 3, 2 and 1 in three series:
  PulsusDB's exemplar 3, the reference's 6, both with the labels the three
  share.
- **PulsusDB:** one candidate per series, each with its own total and
  labels.
- **Reference:** the parts summed into one, with the labels they share.
- **Why:** #826 plan E6: summing first needs a group per profile, about
  one key per entry.
- **Case:** `C43` in `crates/pulsus-server/tests/profileql_differential.rs`.

### `heatmap-exemplar-mismatch-is-400`

- **Route:** `SelectHeatmap`.
- **Request:** an exemplar type unlike the query type.
- **PulsusDB:** `400 invalid_argument`, on every window.
- **Reference:** `500` when the window holds data, `{}` when it holds none.
- **Why:** owner, 2026-11-17, the decision for #825's absent query type,
  applied here (#826 plan E8).
- **Case:** `none`: an error answer, and no front-end client sends an
  exemplar type unlike its query type.

### `diff-side-failure-keeps-its-code`

**Retired 2026-11-18.** The reference's served `Diff` returns each side's
own error, as PulsusDB does.

- **Route:** `Diff`.
- **Request:** one side fails, a ceiling included.
- **PulsusDB:** the side's own code, as on every profiles route: a ceiling
  reached by either side is `429 resource_exhausted`,
  `query_too_broad: <setting>`.
- **Reference:** the side's own error, as PulsusDB (its read-path router's
  `Diff`); the `400 invalid_argument` this row recorded came from a
  function the reference does not mount.
- **Why:** was defects entry 47, withdrawn by #827 part B.
- **Case:** `none`.

### `label-values-passes-the-name`

- **Route:** `/pyroscope/label-values`.
- **Request:** a label other than `__name__`.
- **PulsusDB:** `200`, the label's values over the last hour.
- **Reference:** `400`: it sends `LabelValues` with no name
  (`pkg/querier/http.go:57`).
- **Why:** defects entry 49.
- **Case:** `C37` in `crates/pulsus-server/tests/profileql_differential.rs`.

### `render-timeline-keeps-the-window-grid`

- **Route:** `/pyroscope/render`.
- **Request:** a window that starts before the tenant's first profile.
- **PulsusDB:** the timeline's points on the window's own steps.
- **Reference:** the points on steps starting at the tenant's first
  profile, so a sample can fall one slot earlier
  (`pkg/frontend/readpath/router.go:92-113`).
- **Why:** owner, 2026-11-19, "Record the difference"; defects entry 50.
- **Case:** `C38` in `crates/pulsus-server/tests/profileql_differential.rs`.

### `pprof-period-is-the-pushed-one`

- **Route:** `SelectMergeProfile`, `SelectMergeStacktraces` with
  `PROFILE_FORMAT_PPROF`, `GET /api/profiles/v1/export`.
- **Request:** any selection holding samples.
- **PulsusDB:** `period` is the largest period the selected samples were
  pushed with: 10,000,000 for the fixture's CPU profile.
- **Reference:** 1,000,000,000 for `process_cpu`, 524,288 for `memory`, else
  1, whatever was pushed.
- **Why:** owner, 2026-11-19, "Write the pushed period";
  `docs/reference-defects-we-do-not-copy.md` entry 51.
- **Case:** C44 to C49, rule R4 in
  `crates/pulsus-server/tests/profileql_differential.rs`.

### `pprof-cut-keeps-max-nodes-exactly`

- **Route:** `SelectMergeProfile`, `SelectMergeStacktraces` with
  `PROFILE_FORMAT_PPROF`, `GET /api/profiles/v1/export`.
- **Request:** a `max_nodes` above 0; for example 3 on the fixture, or the
  16,384 the exploration app sends with every export.
- **PulsusDB:** each stack is cut at its first kept function from the leaf: a
  location holding only cut functions goes, a location holding cut and kept
  lines keeps its kept lines, and a stack with no function is kept as it is.
- **Reference:** one cut frame more is kept below the last kept one, a partly
  cut location is kept whole, and every stack with no function is folded
  into `other`.
- **Why:** owner, 2026-11-19, "Cut exactly";
  `docs/reference-defects-we-do-not-copy.md` entry 52.
- **Case:** `C44` and `C45` in `crates/pulsus-server/tests/profileql_differential.rs`.

### `stats-bounds-are-hours`

- **Route:** `GetProfileStats`, `GET /api/profiles/v1/stats`.
- **Request:** a tenant holding profiles.
- **PulsusDB:** `oldestProfileTime` the start of the oldest profile's UTC
  hour and `newestProfileTime` the last millisecond of the newest profile's
  UTC hour, read from the series table.
- **Reference:** `oldestProfileTime` 0 (mechanism not located) and
  `newestProfileTime` the newest profile's millisecond.
- **Why:** issue #830: the exploration app sends it on every load and reads
  only `dataIngested`, and the exact times would read every entry the tenant
  holds: 25,921,000 rows for three days of a 1,500-series tenant, past the
  scan budget after 5.8 days.
- **Case:** `C59` in `crates/pulsus-server/tests/profileql_differential.rs`.

### `analyze-query-is-not-computed`

- **Route:** `AnalyzeQuery`.
- **Request:** a window holding data.
- **PulsusDB:** `{}`, with no statement sent.
- **Reference:** two scopes, `Short term storage` and `Long term storage`,
  every count zero.
- **Why:** owner, 2026-11-21, "I'll go with your recommendations": the
  figure would read every sample of the window, and the exploration app's
  only caller of it is disabled.
- **Case:** `C60` in `crates/pulsus-server/tests/profileql_differential.rs`.

### `recording-rules-unimplemented`

- **Route:** `settings.v1.RecordingRulesService/*`.
- **Request:** any.
- **PulsusDB:** `501`
  `{"code":"unimplemented","message":"recording rules are not served"}`.
- **Reference:** `404` `404 page not found`: the service is not mounted
  without recording rules.
- **Why:** owner decision Q15 on #772.
- **Case:** `C61` in `crates/pulsus-server/tests/profileql_differential.rs`.

### `vcs-get-file-unimplemented`

- **Route:** `vcs.v1.VCSService/GetFile`.
- **Request:** no session.
- **PulsusDB:** `501`
  `{"code":"unimplemented","message":"GitHub integration is not configured"}`,
  as every method of the service answers.
- **Reference:** `401` `{"code":"unauthenticated","message":"invalid token"}`.
- **Why:** owner decision Q16 on #772.
- **Case:** `C63` in `crates/pulsus-server/tests/profileql_differential.rs`.
