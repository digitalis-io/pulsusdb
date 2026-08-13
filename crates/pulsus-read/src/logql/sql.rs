//! Pure per-stage SQL string builders — the snapshot-testing surface
//! (`tests/sql_snapshots.rs`). Every function here is `AST-derived data →
//! String`: no `ChClient`, no I/O, no randomness. Callers (mainly
//! [`super::plan`]) are responsible for pre-escaping every user-controlled
//! fragment via [`super::escape`] before it reaches these builders — that
//! is the injection boundary, not this module.
//!
//! **Issue #286 made most of that responsibility a type, not a convention.**
//! Boolean predicate fragments arrive as [`super::predicate::CheckedFragment`],
//! string literals as [`super::predicate::CheckedLiteral`], month partition
//! literals as [`super::predicate::MonthLiteral`], and a metric read's
//! bucket/aggregate columns as a [`MetricShape`] — none of which can be built
//! from unchecked text outside `logql::predicate`. The table-name parameters
//! deliberately stay `&str`; `predicate.rs`'s module doc records that residual
//! and why neither candidate mechanism was taken.

use super::params::Direction;
use super::predicate::{CheckedFragment, CheckedLiteral, MonthLiteral};

/// A half-open-below/closed-above nanosecond time bound (`ts > start AND ts
/// <= end`, docs/schemas.md §3.2), grouped into one parameter so the stage
/// 3/metric builders below stay under clippy's argument-count lint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeWindow {
    pub start_ns: i64,
    pub end_ns: i64,
}

/// How a METRIC scan's lower time bound compares (issue #227 review
/// round 11). `Exclusive` is the ordinary half-open window predicate
/// (`timestamp_ns > start_ns` — the reference's `(t-range, t]`).
/// `Inclusive` (`timestamp_ns >= start_ns`) exists for exactly one case:
/// the planner's `start - range` scan widening UNDERFLOWED i64, so the
/// LOGICAL lower bound sits strictly below the representable timestamp
/// domain and the saturated `i64::MIN` carried in
/// [`TimeWindow::start_ns`] is a VACUOUS bound, not an exclusive one — a
/// sample stored at exactly `i64::MIN` is inside the reference's window,
/// and `>` would silently drop it. A legitimately-computed
/// (non-underflowing) `i64::MIN` bound stays `Exclusive` and keeps
/// excluding a sample at exactly that timestamp. Only
/// [`super::plan::metric_plan`]'s widening decides this; the log-path
/// stage-3 builders take client bounds verbatim (never widened) and stay
/// structurally exclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanLowerBound {
    Exclusive,
    Inclusive,
}

impl ScanLowerBound {
    /// The SQL comparison operator the lower bound renders as. Both forms
    /// are plain range predicates on the `ORDER BY` time column, so
    /// primary-key pruning is identical (`>=` at `i64::MIN` trivially
    /// covers every part, exactly like the saturated `>` did).
    fn sql_op(self) -> &'static str {
        match self {
            ScanLowerBound::Exclusive => ">",
            ScanLowerBound::Inclusive => ">=",
        }
    }
}

/// Declares [`MetricShape`], its COMPLETE variant list and both column
/// renderings from one source — the `vector_agg_ops!` precedent
/// (`crates/pulsus-logql/src/ast.rs:2460-2504`, issue #406), for the same
/// reason it gives: a hand-maintained `ALL` beside a hand-written enum is two
/// sources, and [`MetricShape::from_columns`] enumerates through `ALL`.
///
/// A fifth shape cannot be added without supplying both column literals and
/// landing in `ALL`, so the round-trip test covers it without being edited.
/// There is **no `_ =>` arm** in any [`MetricShape`] impl and no second
/// invocation is possible (`E0428`).
///
/// Per-variant docs travel through `$(#[$meta:meta])*`.
macro_rules! metric_shapes {
    ($($(#[$meta:meta])* $variant:ident => ($bucket:literal, $agg:literal),)+) => {
        /// The complete set of `(bucket_col, agg_expr)` column shapes a
        /// metric read can have: rollup vs raw × bytes vs count. Sealed so
        /// [`MetricSource`] cannot carry caller text (issue #286).
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum MetricShape { $($(#[$meta])* $variant),+ }

        impl MetricShape {
            /// Every variant, in declaration order — emitted by the same
            /// invocation that declares them, so nothing that enumerates
            /// through this slice can miss a shape the enum has.
            pub const ALL: &'static [MetricShape] = &[$(MetricShape::$variant),+];

            /// Wildcard-free by construction: the macro emits one arm per
            /// variant and there is no `_ =>` arm to add one to.
            pub const fn bucket_col(self) -> &'static str {
                match self { $(MetricShape::$variant => $bucket),+ }
            }

            /// Wildcard-free by construction, as [`MetricShape::bucket_col`].
            pub const fn agg_expr(self) -> &'static str {
                match self { $(MetricShape::$variant => $agg),+ }
            }
        }
    };
}

metric_shapes! {
    /// `log_metrics_<res>` serving a count-family reducer.
    RollupCount => ("bucket_ns", "sum(count)"),
    /// `log_metrics_<res>` serving a bytes-family reducer.
    RollupBytes => ("bucket_ns", "sum(bytes)"),
    /// The `log_samples` raw fallback, count family.
    RawCount => ("timestamp_ns", "count()"),
    /// The `log_samples` raw fallback, bytes family.
    RawBytes => ("timestamp_ns", "sum(length(body))"),
}

impl MetricShape {
    /// The rollup shape for a count/bytes reducer.
    pub const fn rollup(is_bytes: bool) -> Self {
        if is_bytes {
            MetricShape::RollupBytes
        } else {
            MetricShape::RollupCount
        }
    }

    /// The raw-fallback shape for a count/bytes reducer.
    pub const fn raw(is_bytes: bool) -> Self {
        if is_bytes {
            MetricShape::RawBytes
        } else {
            MetricShape::RawCount
        }
    }

    /// The inverse, derived from [`MetricShape::ALL`] — so a shape added to
    /// the `metric_shapes!` invocation is covered here without this function
    /// being edited, and no `_ =>` arm exists that could swallow one.
    /// `None` for any other column pair.
    pub fn from_columns(bucket_col: &str, agg_expr: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|s| s.bucket_col() == bucket_col && s.agg_expr() == agg_expr)
    }
}

/// Which physical table a metric read targets, and that table's
/// bucket/aggregate column shape — the rollup-vs-raw routing decision
/// [`super::plan::metric_plan`] makes, grouped into one parameter (same
/// clippy argument-count reason as [`TimeWindow`]). Rollup-served reads
/// `log_metrics_<res>` with `bucket_ns`/`sum(count)`|`sum(bytes)`; the raw
/// fallback reads `log_samples` with `timestamp_ns`/`count()`|`sum(length(body))`.
///
/// **Issue #286 — the fields are private and the columns are an enum.**
/// `bucket_col`/`agg_expr` were `pub &'a str` interpolated verbatim into the
/// `SELECT` list and the `WHERE`, so a caller could pass
/// `&format!("match(body, {…})", …)`. They are now a [`MetricShape`], which
/// has exactly four inhabitants. The asymmetry with
/// [`super::plan::MetricPlan`], whose equivalents are still strings, is not a
/// design preference: that struct's field set is pinned by issue #293's
/// frozen `tests/golden/plan_build_differential.txt` — see
/// [`super::plan::MetricPlan::source_shape`].
///
/// **`table` deliberately stays `&str`.** No enforced property covers it;
/// `predicate.rs`'s module doc carries that residual, its ground (an
/// observation about `PlanCtx`, of the kind review round 2 refused for
/// `services`), and the two candidate mechanisms that were measured and
/// declined.
#[derive(Debug, Clone, Copy)]
pub struct MetricSource<'a> {
    table: &'a str,
    shape: MetricShape,
}

impl<'a> MetricSource<'a> {
    /// The ONLY constructor. The column shape can only be one of
    /// [`MetricShape::ALL`]; `table` is a trusted schema name — see the
    /// residual note on the type.
    ///
    /// The pre-#286 bypass — caller text straight into the `SELECT` list —
    /// no longer compiles:
    ///
    /// ```compile_fail,E0560
    /// let _ = pulsus_read::logql::sql::MetricSource {
    ///     table: "t",
    ///     bucket_col: "b",
    ///     agg_expr: "match(body, '(')",
    /// };
    /// ```
    ///
    /// **The code above is `E0560`, not the `E0451` a reader might expect**,
    /// and it was measured rather than assumed: the two column fields no
    /// longer exist, so rustc reports `struct MetricSource<'_> has no field
    /// named bucket_col`/`agg_expr` and aborts before the privacy check on
    /// `table`. The privacy seal on its own is this, naming only real fields:
    ///
    /// ```compile_fail,E0451
    /// use pulsus_read::logql::sql::{MetricShape, MetricSource};
    /// let _ = MetricSource { table: "t", shape: MetricShape::RawCount };
    /// ```
    ///
    /// **Note on what a `compile_fail` fence does and does not check
    /// (measured, issue #286):** rustdoc runs the snippet and requires it to
    /// FAIL, but it does **not** verify the annotated error code —
    /// `compile_fail,E0999`, a code that does not exist, passes. The codes
    /// above are therefore documentation of a measurement, not a gate. What
    /// makes a fence worth something is its REMOVAL TEST, and both of these
    /// have been watched go green when the property they name is deleted:
    /// `pub table` + `pub shape` turns the second one green, and the first
    /// one goes green **only** when the entire pre-#286
    /// `{ pub table, pub bucket_col, pub agg_expr }` shape is restored —
    /// re-adding just the two column fields leaves it red on the missing
    /// `shape`, which was measured rather than assumed (`predicate.rs`'s
    /// module doc carries the whole table). The compiling twin below shares
    /// their skeleton so a typo cannot make them pass for the wrong reason.
    ///
    /// ```
    /// use pulsus_read::logql::sql::{MetricShape, MetricSource};
    /// let s = MetricSource::new("log_samples", MetricShape::RawCount);
    /// assert_eq!(s.shape().agg_expr(), "count()");
    /// ```
    pub fn new(table: &'a str, shape: MetricShape) -> Self {
        MetricSource { table, shape }
    }

    /// The physical table name.
    pub fn table(&self) -> &'a str {
        self.table
    }

    /// The sealed bucket/aggregate column shape.
    pub fn shape(&self) -> MetricShape {
        self.shape
    }
}

/// Stage 1 — single-pass stream resolution over `log_streams_idx`
/// (docs/schemas.md §3.2). `months` are pre-rendered `'YYYY-MM-01'` date
/// literals (at least one); `positive_branches`/`negative_branches` are
/// pre-rendered, already-parenthesized `(key = '...' AND ...)` OR-branches
/// (see [`super::plan::normalize_matchers`]).
///
/// **Pure-positive selectors collapse byte-for-byte to docs/schemas.md
/// §3.2's canonical `HAVING uniqExact(key, val) = n` form** (architect plan
/// amendment §1) — the `negative_branches.is_empty()` branch below is
/// load-bearing for that byte-exact requirement; changing its shape breaks
/// the snapshot contract.
pub fn stage1(
    streams_idx_table: &str,
    months: &[MonthLiteral],
    positive_branches: &[CheckedFragment],
    negative_branches: &[CheckedFragment],
) -> String {
    let month_clause = month_clause(months);

    let mut where_branches: Vec<&str> = positive_branches
        .iter()
        .map(CheckedFragment::as_sql)
        .collect();
    where_branches.extend(negative_branches.iter().map(CheckedFragment::as_sql));
    let where_or_list = where_branches.join(" OR ");

    let having = if negative_branches.is_empty() {
        format!("uniqExact(key, val) = {}", positive_branches.len())
    } else {
        let pos_or = join_fragments(positive_branches);
        let neg_or = join_fragments(negative_branches);
        format!(
            "uniqExactIf((key, val), {pos_or}) = {}\n   AND countIf({neg_or}) = 0",
            positive_branches.len()
        )
    };

    format!(
        "SELECT fingerprint\nFROM {streams_idx_table}\nWHERE {month_clause}\n  AND ({where_or_list})\nGROUP BY fingerprint\nHAVING {having}"
    )
}

/// A `count()` selectivity probe over one matcher key's index prefix
/// (docs/schemas.md §3.2: "the planner orders matchers by selectivity
/// (cheap `count()` probes on index prefixes)"). Only computed when the
/// selector contains at least one regex matcher — pure-equality selectors
/// are point ranges and skip probes entirely (architect plan: "Selectivity
/// probes").
pub fn probe(
    streams_idx_table: &str,
    months: &[MonthLiteral],
    key_literal: &CheckedLiteral,
) -> String {
    let month_clause = month_clause(months);
    let key_literal = key_literal.as_sql();
    format!(
        "SELECT count() AS n\nFROM {streams_idx_table}\nWHERE {month_clause} AND key = {key_literal}"
    )
}

/// Labels discovery (#13 `GET|POST /api/logs/v1/labels`): every distinct
/// `log_streams_idx` key of a stream ACTIVE within `window`, ascending.
/// Budget-capped like every other index scan in this module
/// (`LogQlEngine::budget_settings`).
///
/// **Issue #399:** the month partition alone is not the requested window —
/// `log_streams_idx` carries no time column at all (`month Date, key, val,
/// fingerprint`), so the window arrives as the
/// [`active_fingerprints`] semi-join over the log rollup. The month
/// predicate is kept exactly where it was: it is the partition-pruning
/// bound, and the semi-join narrows within it. **M1 scope, unchanged:**
/// `label_names` takes no fingerprints — `query=`-selector narrowing is
/// deferred (docs/api.md §2.3), so this form is always unscoped.
pub fn label_names(
    streams_idx_table: &str,
    months: &[MonthLiteral],
    rollup_table: &str,
    window: TimeWindow,
    rollup_res_ns: u64,
) -> String {
    format!(
        "SELECT DISTINCT key AS name\nFROM {streams_idx_table}\nWHERE {}\n  AND fingerprint IN ({})\nORDER BY name",
        month_clause(months),
        active_fingerprints(rollup_table, None, window, rollup_res_ns)
    )
}

/// Label-values discovery (#13 `GET /api/logs/v1/label/{{name}}/values`):
/// every distinct value of one key, over the streams ACTIVE within
/// `window`, ascending. `key_literal` is a pre-escaped ClickHouse string
/// literal (see [`super::escape::ch_string`]). **M1 scope:** returns the
/// key's full distinct-value set over that window; `query=`-selector
/// narrowing is deferred to M6 parity (docs/api.md §2.3).
///
/// **Issue #399:** same shape and same reason as [`label_names`] — the
/// month predicate prunes partitions, the [`active_fingerprints`]
/// semi-join applies the request's own window.
pub fn label_values(
    streams_idx_table: &str,
    months: &[MonthLiteral],
    key_literal: &CheckedLiteral,
    rollup_table: &str,
    window: TimeWindow,
    rollup_res_ns: u64,
) -> String {
    let key_literal = key_literal.as_sql();
    format!(
        "SELECT DISTINCT val AS value\nFROM {streams_idx_table}\nWHERE {} AND key = {key_literal}\n  AND fingerprint IN ({})\nORDER BY value",
        month_clause(months),
        active_fingerprints(rollup_table, None, window, rollup_res_ns)
    )
}

/// Detected-labels aggregation over the stream index (issue #170,
/// docs/api.md §2.6): one output row per distinct key within `months`,
/// with `uniqExact(val)` as the exact cardinality (a REGISTERED
/// divergence, not an unrecorded improvement: the reference reports a p14
/// hyperloglog estimate — `detected-cardinality-exact-not-estimated` in
/// docs/benchmarks/logs-differential-ledger.md, which carries the
/// per-family measurements and the cost that decided it) and
/// `non_id_values` counting
/// values that are neither a float (`toFloat64OrNull`) nor a UUID
/// ([`super::predicate::non_id_values_expr`], whose `UUID_RE` constant moved
/// there with it in issue #286) — the server-side half of the reference's
/// `containsAllIDTypes` relevance filter (the keep rule — static label OR
/// `non_id_values > 0` — applies client-side in `exec`). `fingerprints` =
/// `None` for the unscoped form; `Some` pushes the caller's stage-1 result
/// **into** the [`active_fingerprints`] subquery (the two `IN`s compose:
/// the subquery's result is already a subset of the list, so rendering the
/// list once inside it is equivalent and cheaper — measured ~395× fewer
/// rows read, issue #399). **Never touches `log_samples`** — it reads the
/// stream index plus the log rollup's `(fingerprint, bucket_ns)` prefix,
/// month-partition-pruned on one side and bucket-range-pruned on the
/// other, server-side aggregated (fan-in is one row per key, never per
/// value).
///
/// **Issue #399:** before this, the only time bound was `month`, so a
/// ten-minute request was answered from the whole calendar month.
pub fn detected_labels(
    streams_idx_table: &str,
    months: &[MonthLiteral],
    fingerprints: Option<&[u64]>,
    rollup_table: &str,
    window: TimeWindow,
    rollup_res_ns: u64,
) -> String {
    let month_clause = month_clause(months);
    let non_id_values = super::predicate::non_id_values_expr();
    let non_id_values = non_id_values.as_sql();
    let active = active_fingerprints(rollup_table, fingerprints, window, rollup_res_ns);
    format!(
        "SELECT key, uniqExact(val) AS cardinality, {non_id_values} AS non_id_values\nFROM {streams_idx_table}\nWHERE {month_clause}\n  AND fingerprint IN ({active})\nGROUP BY key\nORDER BY key"
    )
}

/// The rollup bucket CONTAINING `start_ns` — the activity semi-join's
/// lower bound (issue #399).
///
/// The rollup MV stores `bucket_ns = intDiv(timestamp_ns, res) * res`
/// (`crates/pulsus-schema/src/catalog.rs`, `log_metrics_{{…}}_mv`), so a
/// sample inside the request's `(start, end]` window can sit in a bucket
/// that starts at or before `start`; comparing `bucket_ns` against
/// `start_ns` itself — the half-open shape [`log_stats_rollup`] uses on
/// the SAMPLE axis, where it is correct — would silently drop that
/// stream. Flooring makes the filter conservative in the only safe
/// direction: it can over-include by at most one bucket (5s by default)
/// at each edge and can never drop a stream with a line in the window.
///
/// Floor division (`div_euclid`), not truncation, so a negative
/// `start_ns` rounds DOWN; `saturating_mul` so no input can overflow.
pub fn activity_lower_bucket_ns(start_ns: i64, rollup_res_ns: u64) -> i64 {
    let res = i64::try_from(rollup_res_ns).unwrap_or(i64::MAX).max(1);
    start_ns.div_euclid(res).saturating_mul(res)
}

/// Fingerprints with at least one log line in (approximately) `window` —
/// the stream-activity semi-join source shared by [`detected_labels`],
/// [`label_names`], [`label_values`] and `/series` (issue #399).
///
/// `log_streams_idx` has no time column, so the window has to come from
/// the one co-sharded table that records per-stream activity in time: the
/// log rollup `log_metrics_<res>` (`ORDER BY (fingerprint, bucket_ns)`,
/// `PARTITION BY toDate(fromUnixTimestamp64Nano(bucket_ns))`), already the
/// source for `/stats` and `/volume`.
///
/// `DISTINCT` on the PK prefix is a streaming distinct and is load-bearing:
/// without it the `IN` set carries one row per bucket per stream.
/// `fingerprints` = `Some` pushes the caller's stage-1 result INTO this
/// scan so it reads PK point ranges instead of the whole bucket range.
///
/// The lower bound is [`activity_lower_bucket_ns`], NOT `start_ns` — see
/// that function for why the obvious `bucket_ns > start_ns` is wrong here.
pub fn active_fingerprints(
    rollup_table: &str,
    fingerprints: Option<&[u64]>,
    window: TimeWindow,
    rollup_res_ns: u64,
) -> String {
    let TimeWindow { start_ns, end_ns } = window;
    let lower = activity_lower_bucket_ns(start_ns, rollup_res_ns);
    let mut sql = format!("SELECT DISTINCT fingerprint FROM {rollup_table} WHERE ");
    if let Some(fps) = fingerprints {
        sql.push_str(&format!("fingerprint IN ({}) AND ", fp_list(fps)));
    }
    sql.push_str(&format!("bucket_ns >= {lower} AND bucket_ns <= {end_ns}"));
    sql
}

/// The `month = '...'` / `month IN (...)` clause shared by every stage-1-
/// style `log_streams_idx` scan in this module (`months` is at least one
/// pre-rendered `'YYYY-MM-01'` date literal).
fn month_clause(months: &[MonthLiteral]) -> String {
    if months.len() == 1 {
        format!("month = {}", months[0].as_sql())
    } else {
        format!(
            "month IN ({})",
            months
                .iter()
                .map(MonthLiteral::as_sql)
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

/// `" OR "`-joins a fragment list — the one place [`stage1`]'s `HAVING`
/// conditional form unwraps its branches.
fn join_fragments(branches: &[CheckedFragment]) -> String {
    branches
        .iter()
        .map(CheckedFragment::as_sql)
        .collect::<Vec<_>>()
        .join(" OR ")
}

/// Stage 2 — hydration (docs/schemas.md §3.2 line 307), byte-exact to the
/// canonical shape: `SELECT fingerprint, service, labels FROM log_streams
/// WHERE fingerprint IN (...)`.
pub fn stage2(streams_table: &str, fingerprints: &[u64]) -> String {
    let fp_list = fp_list(fingerprints);
    format!(
        "SELECT fingerprint, service, labels FROM {streams_table} WHERE fingerprint IN ({fp_list})"
    )
}

/// Stage 3 — samples, primary-index + skip-index served (docs/schemas.md
/// §3.2). `services` are pre-escaped string literals; `line_filters` are
/// pre-rendered predicate fragments (see
/// [`super::plan::compile_line_filter`]), one per pipeline `LineFilter`
/// stage, ANDed together.
///
/// **Singleton/`IN` split (architect plan amendment §2, review finding 2):**
/// exactly one service renders the byte-exact §3.2 form `PREWHERE service =
/// 'checkout'`; more than one renders `PREWHERE service IN (...)`.
///
/// **The `ORDER BY` is a TOTAL order, and that is load-bearing (issue
/// #406, found by a CI failure).** `ORDER BY timestamp_ns` alone leaves
/// entries sharing a timestamp in whatever order the parts happened to be
/// read in, which ClickHouse does not fix across runs: measured
/// 2026-08-10 on 81 active parts holding 200 rows at one identical
/// `timestamp_ns`, **fifteen identical queries returned fifteen different
/// orderings**, and at a `LIMIT` that cut through the tie group,
/// **fifteen different result SETS** — the same query answering with
/// different log lines each time. The reference is stable over the same
/// corpus shape (229 rows, 160 separate appends): one ordering, one set,
/// every run.
///
/// So ties break on `fingerprint`, then `cityHash64(body)`, then the raw
/// `body` — **the identical key list [`stage3_keyset`] already renders**,
/// so the fast path and the fetch-until-limit path order a tie group the
/// same way and a pipeline gaining a dropping stage cannot reshuffle it.
/// All four columns follow `direction`, as there.
///
/// This costs no pruning: the table's sort key is `(service, fingerprint,
/// timestamp_ns)` and this query leads with `timestamp_ns`, so it was
/// never a read-in-order scan — the added keys change comparison cost
/// inside a sort that already had to happen, over at most the matched
/// rows. Index engagement is pinned unchanged by
/// `explain_indexes.rs`'s stage-3 cases.
///
/// **This buys determinism, NOT the reference's order.** Both stores are
/// deterministic and they settle into different sequences — ours is
/// `cityHash64(body)`, the reference's is arrival order within a stream
/// and `streamHash` across streams. At a `LIMIT` cutting a tie group that
/// means a different subset survives, so it is registered as
/// `timestamp-tie-order` in docs/benchmarks/logs-differential-ledger.md
/// with the probe, both sequences, and why matching it is separate work.
pub fn stage3(
    samples_table: &str,
    services: &[CheckedLiteral],
    fingerprints: &[u64],
    window: TimeWindow,
    line_filters: &[CheckedFragment],
    direction: Direction,
    limit: u32,
) -> String {
    let service_pred = service_predicate(services);
    let fp_list = fp_list(fingerprints);
    let order = match direction {
        Direction::Backward => "DESC",
        Direction::Forward => "ASC",
    };
    let TimeWindow { start_ns, end_ns } = window;

    let mut sql = format!(
        "SELECT fingerprint, timestamp_ns, body, structured_metadata\nFROM {samples_table}\nPREWHERE {service_pred}\nWHERE fingerprint IN ({fp_list})\n  AND timestamp_ns > {start_ns} AND timestamp_ns <= {end_ns}"
    );
    for clause in line_filters {
        sql.push_str("\n  AND ");
        sql.push_str(clause.as_sql());
    }
    sql.push_str(&format!(
        "\nORDER BY timestamp_ns {order}, fingerprint {order}, cityHash64(body) {order}, body \
         {order}\nLIMIT {limit}"
    ));
    sql
}

/// The lower-bound mode of one [`stage3_keyset`] page (issue #74 live
/// tail, generalized for issue #90 streams paging — plan v4 D1/D2 + the
/// round-4 adjudication).
///
/// - [`KeysetLower::First`] — the first page: the API window's exclusive
///   `start`/inclusive `end` bounds (`timestamp_ns > start_ns AND
///   timestamp_ns <= end_ns`, docs/schemas.md §3.2), carried by the
///   `window` argument. No keyset term. Direction-agnostic.
/// - [`KeysetLower::After`] — every later page: the occurrence-count
///   keyset. The composite predicate is **inclusive** (`>=` the boundary
///   tuple when walking Forward, `<=` when walking Backward) so a tie
///   group split by `LIMIT` is re-fetched rather than skipped, and the
///   server-side `OFFSET` skips exactly the already-delivered rows of the
///   boundary tuple (deterministic under the total `ORDER BY` below). The
///   redundant `timestamp_ns >= ts` (Forward) / `timestamp_ns <= ts`
///   (Backward) term keeps the primary index's time column engaged for
///   granule pruning (the tuple comparison alone does not prune).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeysetLower {
    /// The first page; the `window` argument carries both time bounds.
    First,
    After {
        /// The boundary `(timestamp_ns, fingerprint, cityHash64(body))`.
        tuple: (i64, u64, u64),
        /// How many rows equal to `tuple` were already delivered — the
        /// SQL `OFFSET`.
        offset: u32,
    },
}

/// Stage 3, keyset-pagination mode (issue #74 live tail; issue #90
/// streams fetch-until-limit — deliberately a reusable builder, the
/// foundation for streams pagination, not a tail-only hack). Leaves the
/// frozen [`stage3`] untouched; shares its `PREWHERE
/// service`/`fingerprint IN`/line-filter pushdown contract byte-for-byte.
///
/// The total `ORDER BY timestamp_ns, fingerprint, body_hash, body` (raw
/// `body` as final tiebreaker), all columns following `direction`, makes
/// equal-`(ts, fp, hash)` rows — including genuine CityHash collisions —
/// a stable adjacent run across queries, so [`KeysetLower::After`]'s
/// occurrence-count `OFFSET` is well-defined. `cityHash64(body)` is
/// projected by ClickHouse and captured as `body_hash` (no CH/Rust
/// divergence at the boundary).
///
/// **`direction` mirrors the whole page.** Forward (ASC, tail's only
/// mode — byte-identical to issue #74's rendering) walks oldest→newest
/// with a `>=` composite and the redundant `timestamp_ns >= ts` lower
/// bound; Backward (DESC, the query default's newest-first mode) walks
/// newest→oldest with a `<=` composite and the redundant `timestamp_ns
/// <= ts` upper bound, keeping the API's `start` bound as the fixed
/// lower.
// One arg over clippy's threshold: `window`, `lower`, and `direction`
// are each independent page coordinates that cannot fold without
// obscuring the SQL contract (the sibling `stage3` sits at the 7-arg
// limit for the same reason).
#[allow(clippy::too_many_arguments)]
pub fn stage3_keyset(
    samples_table: &str,
    services: &[CheckedLiteral],
    fingerprints: &[u64],
    window: TimeWindow,
    lower: KeysetLower,
    direction: Direction,
    line_filters: &[CheckedFragment],
    limit: u32,
) -> String {
    let service_pred = service_predicate(services);
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;

    let mut sql = format!(
        "SELECT fingerprint, timestamp_ns, body, cityHash64(body) AS body_hash, structured_metadata\nFROM {samples_table}\nPREWHERE {service_pred}\nWHERE fingerprint IN ({fp_list})"
    );
    match (direction, lower) {
        (_, KeysetLower::First) => {
            sql.push_str(&format!(
                "\n  AND timestamp_ns > {start_ns} AND timestamp_ns <= {end_ns}"
            ));
        }
        (
            Direction::Forward,
            KeysetLower::After {
                tuple: (ts, fp, hash),
                ..
            },
        ) => {
            sql.push_str(&format!(
                "\n  AND timestamp_ns >= {ts} AND timestamp_ns <= {end_ns}\n  AND (timestamp_ns, fingerprint, cityHash64(body)) >= ({ts}, {fp}, {hash})"
            ));
        }
        (
            Direction::Backward,
            KeysetLower::After {
                tuple: (ts, fp, hash),
                ..
            },
        ) => {
            sql.push_str(&format!(
                "\n  AND timestamp_ns > {start_ns} AND timestamp_ns <= {ts}\n  AND (timestamp_ns, fingerprint, cityHash64(body)) <= ({ts}, {fp}, {hash})"
            ));
        }
    }
    for clause in line_filters {
        sql.push_str("\n  AND ");
        sql.push_str(clause.as_sql());
    }
    let ord = match direction {
        Direction::Forward => "ASC",
        Direction::Backward => "DESC",
    };
    sql.push_str(&format!(
        "\nORDER BY timestamp_ns {ord}, fingerprint {ord}, body_hash {ord}, body {ord}"
    ));
    match lower {
        KeysetLower::First => sql.push_str(&format!("\nLIMIT {limit}")),
        KeysetLower::After { offset, .. } => {
            sql.push_str(&format!("\nLIMIT {limit} OFFSET {offset}"))
        }
    }
    sql
}

/// The `/api/logs/v1/stats` rollup-served aggregation (issue #74, no line
/// filter): zero body reads — `sum(count)`/`sum(bytes)` off
/// `log_metrics_<res>` (PK `(fingerprint, bucket_ns)`), `streams` as
/// `uniqExact(fingerprint)`, and `chunks` as the adjudicated
/// selector-scoped partition-count proxy (`uniqExact` of the bucket's
/// date — docs/api.md §2.5). Same half-open bucket predicate as
/// [`metric_range`].
pub fn log_stats_rollup(rollup_table: &str, fingerprints: &[u64], window: TimeWindow) -> String {
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    format!(
        "SELECT uniqExact(fingerprint) AS streams, uniqExact(toDate(fromUnixTimestamp64Nano(bucket_ns))) AS chunks, sum(count) AS entries, sum(bytes) AS bytes\nFROM {rollup_table}\nWHERE fingerprint IN ({fp_list}) AND bucket_ns > {start_ns} AND bucket_ns <= {end_ns}"
    )
}

/// The `/api/logs/v1/stats` raw fallback (issue #74, line-filtered): the
/// rollup is body-content-blind, so a line filter forces a `log_samples`
/// scan with the identical `PREWHERE service` + skip-index-prunable
/// line-filter predicates [`stage3`] emits (granule-skipped, PK-pruned). Same
/// `streams`/`chunks` shape as [`log_stats_rollup`]; `entries`/`bytes`
/// count matching lines exactly.
pub fn log_stats_raw(
    samples_table: &str,
    services: &[CheckedLiteral],
    fingerprints: &[u64],
    window: TimeWindow,
    line_filters: &[CheckedFragment],
) -> String {
    let service_pred = service_predicate(services);
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    let mut sql = format!(
        "SELECT uniqExact(fingerprint) AS streams, uniqExact(toDate(fromUnixTimestamp64Nano(timestamp_ns))) AS chunks, count() AS entries, sum(length(body)) AS bytes\nFROM {samples_table}\nPREWHERE {service_pred}\nWHERE fingerprint IN ({fp_list})\n  AND timestamp_ns > {start_ns} AND timestamp_ns <= {end_ns}"
    );
    for clause in line_filters {
        sql.push_str("\n  AND ");
        sql.push_str(clause.as_sql());
    }
    sql
}

/// The `/api/logs/v1/volume` aggregation (issue #169, docs/api.md §2.6):
/// per-fingerprint byte volume off `log_metrics_<res>` (PK `(fingerprint,
/// bucket_ns)`) — rollup-ONLY, zero body reads (the endpoint accepts a
/// matchers-only selector, so there is no raw fallback at all, unlike
/// [`log_stats_rollup`]'s line-filtered sibling). Same half-open bucket
/// predicate family as [`log_stats_rollup`]/[`metric_range`], so the
/// identical MinMax + `(fingerprint, bucket_ns)` primary-key pruning
/// applies (`tests/explain_indexes.rs`' Tier-1 gate).
pub fn log_volume_rollup(rollup_table: &str, fingerprints: &[u64], window: TimeWindow) -> String {
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    format!(
        "SELECT fingerprint, sum(bytes) AS bytes\nFROM {rollup_table}\nWHERE fingerprint IN ({fp_list}) AND bucket_ns > {start_ns} AND bucket_ns <= {end_ns}\nGROUP BY fingerprint"
    )
}

/// The maximum number of pattern series `/api/logs/v1/patterns` returns — the
/// top-`N`-by-total-count LIMIT pushed into ClickHouse (M7-C3, issue #171).
pub const MAX_PATTERNS: usize = 1000;

/// The `/api/logs/v1/patterns` aggregation (M7-C3, issue #171, docs/schemas.md
/// §3.2): stage-1 fingerprints → ONE pushed-down aggregate over `log_patterns`
/// with no hydration (the response carries no labels). The inner query
/// re-buckets `bucket_ns` to `step_ns` and sums per `(pattern, ts_ns)`; the
/// outer sums per pattern and emits the ascending `(ts_ns, cnt)` samples array,
/// ordered total-count desc then pattern asc, top-[`MAX_PATTERNS`].
///
/// **Pushdown/pruning:** `fingerprint IN` engages the `(fingerprint, bucket_ns,
/// pattern)` primary-key prefix (granule pruning), daily partitions prune the
/// window (`tests/explain_indexes.rs`' Tier-1 gate), and the aggregation +
/// top-K + LIMIT all execute in ClickHouse — the client decodes ≤ 1000
/// already-assembled series. Half-open window `[start, end)` (D4).
pub fn log_patterns_read(
    patterns_table: &str,
    fingerprints: &[u64],
    window: TimeWindow,
    step_ns: u64,
) -> String {
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    let limit = MAX_PATTERNS;
    format!(
        "SELECT pattern, sum(cnt) AS total, arraySort(x -> x.1, groupArray((ts_ns, cnt))) AS samples\nFROM (\n  SELECT pattern, intDiv(bucket_ns, {step_ns}) * {step_ns} AS ts_ns, sum(count) AS cnt\n  FROM {patterns_table}\n  WHERE fingerprint IN ({fp_list}) AND bucket_ns >= {start_ns} AND bucket_ns < {end_ns}\n  GROUP BY pattern, ts_ns\n)\nGROUP BY pattern\nORDER BY total DESC, pattern ASC\nLIMIT {limit}"
    )
}

/// A range metric query bucketed by `step_ns` (`intDiv(bucket_col, step) *
/// step`, docs/schemas.md §3.2). `extra_predicates` carries line-filter
/// pushdown for the (line-filter-forced) raw fallback.
///
/// **`PREWHERE service ...` on the raw fallback only (fix-plan amendment
/// §3, code review finding "Raw metric fallback loses the `log_samples`
/// primary-key prefix"):** when `source.table` is `log_samples`, omitting a
/// service predicate drops the leading column of `ORDER BY (service,
/// fingerprint, timestamp_ns)` — docs/schemas.md §3.2 line 285 mandates
/// injecting it "even a query that never mentions `service`" to keep the
/// primary index engaged, exactly as stage 3 already does. Pass `services =
/// &[]` for the rollup path (`log_metrics_<res>` has no `service` column,
/// `ORDER BY (fingerprint, bucket_ns)`); a non-empty `services` renders the
/// same singleton/`IN` split [`stage3`] uses.
///
/// **Structurally unreachable today** (recorded while auditing the offset
/// shift, issue #343 — a note, not a change): `plan.rs`'s `metric_plan`
/// sets `client = Some(..)` for EVERY `is_range` query (issue #227 retired
/// the range rollup fast path), so routing can never take the
/// `RouteChoice::Rollup` range arm and no caller reaches this builder
/// outside its own snapshot tests. Kept because the rollup range shape is
/// the one this file documents against docs/schemas.md §3.2.
pub fn metric_range(
    source: MetricSource<'_>,
    services: &[CheckedLiteral],
    fingerprints: &[u64],
    window: TimeWindow,
    lower: ScanLowerBound,
    step_ns: u64,
    extra_predicates: &[CheckedFragment],
) -> String {
    let MetricSource { table, shape } = source;
    let (bucket_col, agg_expr) = (shape.bucket_col(), shape.agg_expr());
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    let lower_op = lower.sql_op();
    let prewhere = metric_prewhere(services);
    let mut sql = format!(
        "SELECT fingerprint, intDiv({bucket_col}, {step_ns}) * {step_ns} AS step, {agg_expr} AS n\nFROM {table}\n{prewhere}WHERE fingerprint IN ({fp_list}) AND {bucket_col} {lower_op} {start_ns} AND {bucket_col} <= {end_ns}"
    );
    for clause in extra_predicates {
        sql.push_str(" AND ");
        sql.push_str(clause.as_sql());
    }
    sql.push_str("\nGROUP BY fingerprint, step");
    sql
}

/// An instant metric query — a single window, no bucketing
/// ([`super::params::QuerySpec::Instant`]'s structural contract: no
/// `intDiv` expression, no `step` column). See [`metric_range`]'s doc
/// comment for the `services`/`PREWHERE` contract (fix-plan amendment §3).
///
/// **Grouped by `(fingerprint, structured_metadata)`** under
/// [`ScanProjection::WithStructuredMetadata`] (issue #249): the metric path
/// merges structured metadata into the label set, so one fingerprint covers
/// N output series, and the client re-groups the returned rows by the merged
/// final label set. That re-grouping is EXACT rather than approximate
/// because every op that can reach this pushdown path is a linear sum
/// (`count()` / `sum(length(body))`), so summing the server's per-metadata
/// partial counts reproduces the client path's single accumulator bit for
/// bit.
///
/// [`ScanProjection::Lean`] exists here for the ROLLUP source
/// (`log_metrics_<res>`), which has no `structured_metadata` column at all.
/// That arm is structurally unreachable for a LogQL instant plan — a
/// `RouteChoice::Rollup` decision requires `QuerySpec::Range` (`plan.rs`'s
/// `match p.spec` on the routing decision), so an instant query is always
/// `Raw` over `log_samples`. The parameter exists so the builder CANNOT
/// render a column the named table lacks, rather than resting on that
/// reachability argument holding forever.
pub fn metric_instant(
    source: MetricSource<'_>,
    services: &[CheckedLiteral],
    fingerprints: &[u64],
    window: TimeWindow,
    lower: ScanLowerBound,
    extra_predicates: &[CheckedFragment],
    projection: ScanProjection,
) -> String {
    let MetricSource { table, shape } = source;
    let (bucket_col, agg_expr) = (shape.bucket_col(), shape.agg_expr());
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    let lower_op = lower.sql_op();
    let prewhere = metric_prewhere(services);
    let sm = projection.column_suffix();
    let mut sql = format!(
        "SELECT fingerprint, {agg_expr} AS n{sm}\nFROM {table}\n{prewhere}WHERE fingerprint IN ({fp_list}) AND {bucket_col} {lower_op} {start_ns} AND {bucket_col} <= {end_ns}"
    );
    for clause in extra_predicates {
        sql.push_str(" AND ");
        sql.push_str(clause.as_sql());
    }
    sql.push_str(match projection {
        ScanProjection::Lean => "\nGROUP BY fingerprint",
        ScanProjection::WithStructuredMetadata => "\nGROUP BY fingerprint, structured_metadata",
    });
    sql
}

/// Whether a raw metric scan projects `structured_metadata` (issue #249).
///
/// The metric path merges structured metadata into the label set, so it must
/// normally be read. [`ScanProjection::Lean`] has exactly ONE caller —
/// `absent_over_time` — and its exemption is proved rather than assumed:
/// `pkg/logql/syntax/extractor.go:46-47 @ v3.7.4` forces `noLabels = true`
/// for `OpRangeTypeAbsent`, and `pkg/logql/log/labels.go:667-668` then
/// returns `EmptyLabelsResult`, so the reducer's label set cannot depend on
/// metadata at all. Reading the column for it would be a permanent cost on
/// an unbounded scan with no observable effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanProjection {
    /// `absent_over_time` only — no `structured_metadata` column.
    Lean,
    /// Every other metric reducer.
    WithStructuredMetadata,
}

impl ScanProjection {
    /// The `SELECT` list's trailing column, or nothing.
    fn column_suffix(self) -> &'static str {
        match self {
            ScanProjection::Lean => "",
            ScanProjection::WithStructuredMetadata => ", structured_metadata",
        }
    }
}

/// The client-aggregated metric fetch (issue M6-10): a stage-3-shaped raw
/// scan of `(fingerprint, timestamp_ns, body)` over the **full** window,
/// with the line-filter prefix pushed down — and deliberately **no
/// `LIMIT`**: an aggregation must see every matching line or abort on the
/// byte scan budget (`max_bytes_to_read` → `QueryTooBroad`), never
/// silently truncate (complete-or-error, the adjudicated design). The
/// `PREWHERE service ...` contract matches [`stage3`]/[`metric_range`]
/// (the `log_samples` primary-key prefix stays engaged).
///
/// **Stable total order (review round 2, finding 2):** `ORDER BY`
/// carries `fingerprint, body` as secondary keys — the projection's only
/// other columns — so equal-timestamp rows arrive in one reproducible
/// order across runs/merges/replicas (float accumulation order, and
/// therefore bit-level sums, stay stable).
///
/// **This ordering is now load-bearing for `first`/`last`, not merely
/// stabilising** (issue #344). Those reducers take the endpoints of
/// Loki's `(timestamp, stream_hash, tie_rank)` delivery order; the
/// instant accumulator compares `(timestamp, stream_hash)` explicitly
/// and reads `tie_rank` — which separates only SAME-stream samples at
/// one nanosecond — off the arrival sequence this `ORDER BY` produces,
/// `body` ascending being exactly the sliding path's group-local
/// `tie_rank`. Dropping or reordering the `body` key would therefore
/// change answers, not just bit-level sums. (The doc line replaced here
/// claimed the first/last reducers were "additionally order-independent
/// via their own value tie-break"; that tie-break was deleted as a wrong
/// value — see `SimpleAcc::add`.)
///
/// **`structured_metadata` is in the `SELECT` list and NOWHERE else** (issue
/// #249). It never appears in `WHERE`, `PREWHERE`, `ORDER BY` or any skip
/// index: a filter on a metadata label is evaluated CLIENT-side, in the
/// compiled pipeline, over the merged label set. That is the reference's
/// shape too — a metadata filter is a pipeline stage run per entry, after
/// `builder.Add(StructuredMetadataLabel, …)` at
/// `pkg/logql/log/metrics_extraction.go:104 @ v3.7.4`, and
/// `grep -n StructuredMetadata pkg/logql/syntax/*.go` at `b318f28` returns
/// no filter, matcher or pushdown site at all. Pushing it would also be
/// useless AND wrong: the column is an opaque canonical-JSON `String` with
/// `DEFAULT ''` and no index (`pulsus-schema/src/catalog.rs`), so a
/// JSON-extract predicate could prune no granule, and the merge renames a
/// colliding key to `<k>_extracted` before any filter sees it.
pub fn metric_raw_samples(
    samples_table: &str,
    services: &[CheckedLiteral],
    fingerprints: &[u64],
    window: TimeWindow,
    lower: ScanLowerBound,
    extra_predicates: &[CheckedFragment],
    projection: ScanProjection,
) -> String {
    let service_pred = service_predicate(services);
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    let lower_op = lower.sql_op();
    let sm = projection.column_suffix();
    let mut sql = format!(
        "SELECT fingerprint, timestamp_ns, body{sm}\nFROM {samples_table}\nPREWHERE {service_pred}\nWHERE fingerprint IN ({fp_list})\n  AND timestamp_ns {lower_op} {start_ns} AND timestamp_ns <= {end_ns}"
    );
    for clause in extra_predicates {
        sql.push_str("\n  AND ");
        sql.push_str(clause.as_sql());
    }
    sql.push_str("\nORDER BY timestamp_ns ASC, fingerprint ASC, body ASC");
    sql
}

/// The sliding-window range metric fetch (issue #227): identical scan/
/// predicate shape to [`metric_raw_samples`], but ordered by the **physical
/// primary key** `(service, fingerprint, timestamp_ns)` so ClickHouse
/// streams it with `optimize_read_in_order=1` and **no server-side sort** —
/// the memory-bound streaming slide reads one fingerprint's ascending-ts run
/// at a time. Deliberately **no `body` in `ORDER BY`** (body is not in the
/// MergeTree key, so ordering by it would force an unbounded per-collision-
/// group sort — issue #227 review finding): same-`(fingerprint,
/// timestamp_ns)` rows arrive in arbitrary order and the deterministic
/// full-body `tie_rank` order is imposed in Rust at group formation. And
/// deliberately **no global `ORDER BY timestamp_ns`** (a scan-sized sort;
/// the canonical `(ts, stream_hash, tie_rank)` fold order is imposed in
/// Rust over the bounded retained window).
///
/// **And deliberately no `structured_metadata` in `ORDER BY`** (issue #249).
/// It is a projected column only — see [`metric_raw_samples`]'s note for why
/// it is never a predicate either. Adding it to the sort key would make
/// `(service, fingerprint, structured_metadata, timestamp_ns)` a
/// NON-primary-key order and force a scan-sized server-side sort, destroying
/// `optimize_read_in_order`. The design does not need it: the fan-out
/// `MutCells` representation is order-independent by construction, and rows
/// within one metadata variant still arrive timestamp-ascending because the
/// existing key order already delivers them that way.
pub fn metric_raw_samples_sliding(
    samples_table: &str,
    services: &[CheckedLiteral],
    fingerprints: &[u64],
    window: TimeWindow,
    lower: ScanLowerBound,
    extra_predicates: &[CheckedFragment],
    projection: ScanProjection,
) -> String {
    let service_pred = service_predicate(services);
    let fp_list = fp_list(fingerprints);
    let TimeWindow { start_ns, end_ns } = window;
    let lower_op = lower.sql_op();
    let sm = projection.column_suffix();
    let mut sql = format!(
        "SELECT fingerprint, timestamp_ns, body{sm}\nFROM {samples_table}\nPREWHERE {service_pred}\nWHERE fingerprint IN ({fp_list})\n  AND timestamp_ns {lower_op} {start_ns} AND timestamp_ns <= {end_ns}"
    );
    for clause in extra_predicates {
        sql.push_str("\n  AND ");
        sql.push_str(clause.as_sql());
    }
    sql.push_str("\nORDER BY service ASC, fingerprint ASC, timestamp_ns ASC");
    sql
}

/// Renders the metric-read `PREWHERE service ...\n` line, or an empty
/// string when `services` is empty (the rollup path — no `service` column
/// to filter on).
fn metric_prewhere(services: &[CheckedLiteral]) -> String {
    if services.is_empty() {
        String::new()
    } else {
        format!("PREWHERE {}\n", service_predicate(services))
    }
}

fn fp_list(fingerprints: &[u64]) -> String {
    fingerprints
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// The singleton-equality/`IN` split shared by every stage 3 style
/// predicate over a resolved value set (architect plan amendment §2).
fn service_predicate(services: &[CheckedLiteral]) -> String {
    if services.len() == 1 {
        format!("service = {}", services[0].as_sql())
    } else {
        format!(
            "service IN ({})",
            services
                .iter()
                .map(CheckedLiteral::as_sql)
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logql::predicate::{literal, month_literal};

    /// The pushed-down fragment fixture these builders' loop tests append.
    ///
    /// Issue #286 replaced the previous hand-written
    /// `positionCaseSensitive(body, 'err') > 0` string with a genuinely
    /// minted one: [`CheckedFragment`] has no constructor outside
    /// `logql::predicate`, and no mint renders `positionCaseSensitive`, so
    /// the old fixture is not expressible. `|= "err"` renders
    /// `body LIKE '%err%'` (issue #450), and the three
    /// expectations below carry that text instead. The property under test
    /// — the builder appends each fragment behind its own `AND` — is
    /// unchanged.
    fn err_line_filter() -> CheckedFragment {
        crate::logql::predicate::line_filter(&pulsus_logql::LineFilter {
            op: pulsus_logql::LineFilterOp::Contains,
            value: "err".to_string(),
            value_is_ip: false,
            or_matches: Vec::new(),
        })
        .expect("a Contains filter compiles no regex")
    }

    // -----------------------------------------------------------------
    // Issue #286: `MetricShape` — single-sourced, wildcard-free, total.
    // -----------------------------------------------------------------

    /// AC13(b): the inverse is derived from [`MetricShape::ALL`], which the
    /// declaring `metric_shapes!` invocation emits — so a fifth shape is
    /// covered here without this test being edited.
    #[test]
    fn metric_shape_round_trips_through_from_columns_for_every_variant() {
        let mut covered = 0usize;
        for &shape in MetricShape::ALL {
            assert_eq!(
                MetricShape::from_columns(shape.bucket_col(), shape.agg_expr()),
                Some(shape),
                "{shape:?} must round-trip"
            );
            covered += 1;
        }
        // Derived from the enum, NOT hand-listed: `ALL` is emitted by the
        // same `metric_shapes!` invocation that declares the variants, so a
        // shape added there is covered here without this test being edited
        // (issue #286 AC16 — demonstrated by adding a fifth line and
        // watching `covered` become 5).
        assert_eq!(covered, MetricShape::ALL.len());
        println!("metric_shape round-trip covered {covered} shapes");
    }

    /// AC13(c): caller text cannot masquerade as a column pair.
    #[test]
    fn metric_shape_from_columns_refuses_a_foreign_pair() {
        assert_eq!(
            MetricShape::from_columns("timestamp_ns", "match(body, '(')"),
            None
        );
        assert_eq!(MetricShape::from_columns("bucket_ns", "count()"), None);
        assert_eq!(MetricShape::from_columns("", ""), None);
    }

    /// AC13(d): the four pairs are, verbatim, the literals `plan.rs`'s
    /// `metric_plan` spelled by hand before issue #286 sealed them.
    #[test]
    fn metric_shape_columns_are_the_pre_286_literals_verbatim() {
        assert_eq!(MetricShape::rollup(false), MetricShape::RollupCount);
        assert_eq!(MetricShape::rollup(true), MetricShape::RollupBytes);
        assert_eq!(MetricShape::raw(false), MetricShape::RawCount);
        assert_eq!(MetricShape::raw(true), MetricShape::RawBytes);
        assert_eq!(
            (
                MetricShape::RollupCount.bucket_col(),
                MetricShape::RollupCount.agg_expr()
            ),
            ("bucket_ns", "sum(count)")
        );
        assert_eq!(
            (
                MetricShape::RollupBytes.bucket_col(),
                MetricShape::RollupBytes.agg_expr()
            ),
            ("bucket_ns", "sum(bytes)")
        );
        assert_eq!(
            (
                MetricShape::RawCount.bucket_col(),
                MetricShape::RawCount.agg_expr()
            ),
            ("timestamp_ns", "count()")
        );
        assert_eq!(
            (
                MetricShape::RawBytes.bucket_col(),
                MetricShape::RawBytes.agg_expr()
            ),
            ("timestamp_ns", "sum(length(body))")
        );
    }

    /// The sealed source hands back exactly what it was built from.
    #[test]
    fn metric_source_new_is_the_only_way_in_and_reads_back_its_shape() {
        let s = MetricSource::new("log_metrics_5s", MetricShape::RollupBytes);
        assert_eq!(s.table(), "log_metrics_5s");
        assert_eq!(s.shape(), MetricShape::RollupBytes);
        assert_eq!(s.shape().bucket_col(), "bucket_ns");
        assert_eq!(s.shape().agg_expr(), "sum(bytes)");
    }

    #[test]
    fn stage2_renders_the_canonical_hydration_shape() {
        assert_eq!(
            stage2("log_streams", &[18374, 99120]),
            "SELECT fingerprint, service, labels FROM log_streams WHERE fingerprint IN (18374, 99120)"
        );
    }

    /// The `[start, end]` window every discovery-builder test below
    /// renders — 5s-aligned so the floored lower bound equals `start_ns`
    /// and the expectation stays readable.
    const DISCOVERY_WINDOW: TimeWindow = TimeWindow {
        start_ns: 1_751_328_000_000_000_000,
        end_ns: 1_751_331_600_000_000_000,
    };
    const RES_5S: u64 = 5_000_000_000;
    const ACTIVE_ALL: &str = "SELECT DISTINCT fingerprint FROM log_metrics_5s WHERE bucket_ns >= 1751328000000000000 AND bucket_ns <= 1751331600000000000";

    #[test]
    fn label_names_renders_a_distinct_key_scan_for_one_month() {
        assert_eq!(
            label_names(
                "log_streams_idx",
                &[month_literal(2026, 7)],
                "log_metrics_5s",
                DISCOVERY_WINDOW,
                RES_5S
            ),
            format!(
                "SELECT DISTINCT key AS name\nFROM log_streams_idx\nWHERE month = '2026-07-01'\n  AND fingerprint IN ({ACTIVE_ALL})\nORDER BY name"
            )
        );
    }

    #[test]
    fn label_names_renders_a_month_in_list_for_a_boundary_spanning_window() {
        let sql = label_names(
            "log_streams_idx",
            &[month_literal(2026, 7), month_literal(2026, 8)],
            "log_metrics_5s",
            DISCOVERY_WINDOW,
            RES_5S,
        );
        assert!(sql.contains("WHERE month IN ('2026-07-01', '2026-08-01')"));
    }

    #[test]
    fn label_values_renders_a_distinct_value_scan_scoped_to_one_key() {
        assert_eq!(
            label_values(
                "log_streams_idx",
                &[month_literal(2026, 7)],
                &literal("env"),
                "log_metrics_5s",
                DISCOVERY_WINDOW,
                RES_5S
            ),
            format!(
                "SELECT DISTINCT val AS value\nFROM log_streams_idx\nWHERE month = '2026-07-01' AND key = 'env'\n  AND fingerprint IN ({ACTIVE_ALL})\nORDER BY value"
            )
        );
    }

    #[test]
    fn detected_labels_unscoped_renders_one_row_per_key_with_the_id_predicate() {
        let sql = detected_labels(
            "log_streams_idx",
            &[month_literal(2026, 7)],
            None,
            "log_metrics_5s",
            DISCOVERY_WINDOW,
            RES_5S,
        );
        assert!(sql.starts_with("SELECT key, uniqExact(val) AS cardinality, countIf(toFloat64OrNull(val) IS NULL AND NOT match(val, "));
        assert!(sql.contains("WHERE month = '2026-07-01'"));
        assert!(sql.contains(&format!("\n  AND fingerprint IN ({ACTIVE_ALL})\n")));
        assert!(sql.ends_with("GROUP BY key\nORDER BY key"));
    }

    /// Issue #399: scoping moves the stage-1 list INSIDE the activity
    /// subquery rather than adding a second outer `IN` — the two forms
    /// differ by exactly that prefix, and the list is rendered once.
    #[test]
    fn detected_labels_scoped_pushes_the_fingerprint_list_into_the_activity_subquery() {
        let scoped = detected_labels(
            "log_streams_idx",
            &[month_literal(2026, 7)],
            Some(&[7, 9]),
            "log_metrics_5s",
            DISCOVERY_WINDOW,
            RES_5S,
        );
        assert!(scoped.contains(
            "AND fingerprint IN (SELECT DISTINCT fingerprint FROM log_metrics_5s WHERE \
             fingerprint IN (7, 9) AND bucket_ns >="
        ));
        assert_eq!(
            scoped.matches("fingerprint IN (7, 9)").count(),
            1,
            "the stage-1 list must be rendered exactly once: {scoped}"
        );
        let unscoped = detected_labels(
            "log_streams_idx",
            &[month_literal(2026, 7)],
            None,
            "log_metrics_5s",
            DISCOVERY_WINDOW,
            RES_5S,
        );
        assert_eq!(
            scoped.replace("fingerprint IN (7, 9) AND ", ""),
            unscoped,
            "scoped form must be the unscoped scan plus only the pushed-down list"
        );
    }

    /// Issue #399 AC2 — the discriminator against the plausible-but-wrong
    /// `bucket_ns > start_ns` copy from [`log_stats_rollup`]: the bound is
    /// the bucket CONTAINING `start_ns`.
    #[test]
    fn activity_lower_bucket_ns_floors_to_the_containing_bucket() {
        const B: i64 = 100 * RES_5S as i64;
        assert_eq!(activity_lower_bucket_ns(B + 1_000_000_000, RES_5S), B);
        assert_eq!(activity_lower_bucket_ns(B, RES_5S), B);
        assert_eq!(activity_lower_bucket_ns(B - 1, RES_5S), B - RES_5S as i64);
        // Floor, not truncation-toward-zero: `-1 / 5e9` is `0` in Rust's
        // integer division and would round a pre-epoch bound UP.
        assert_eq!(activity_lower_bucket_ns(-1, RES_5S), -(RES_5S as i64));
        // No input can overflow or panic.
        assert_eq!(
            activity_lower_bucket_ns(i64::MIN, u64::MAX),
            i64::MIN.div_euclid(i64::MAX).saturating_mul(i64::MAX)
        );
        // A zero resolution is not representable in config, but the
        // builder must not divide by zero if one ever reaches it.
        assert_eq!(activity_lower_bucket_ns(7, 0), 7);
    }

    #[test]
    fn active_fingerprints_renders_a_streaming_distinct_over_the_rollup_pk() {
        assert_eq!(
            active_fingerprints("log_metrics_5s", None, DISCOVERY_WINDOW, RES_5S),
            ACTIVE_ALL
        );
        assert_eq!(
            active_fingerprints(
                "log_metrics_5s",
                Some(&[101, 205]),
                DISCOVERY_WINDOW,
                RES_5S
            ),
            "SELECT DISTINCT fingerprint FROM log_metrics_5s WHERE fingerprint IN (101, 205) \
             AND bucket_ns >= 1751328000000000000 AND bucket_ns <= 1751331600000000000"
        );
    }

    /// A `start_ns` one nanosecond past a bucket edge must render the
    /// bucket's own start, not `start_ns` — the whole point of #399's
    /// floor (a sample at `B + 1s` lives in bucket `B`).
    #[test]
    fn active_fingerprints_lower_bound_is_the_containing_bucket_not_the_request_start() {
        let b = 1_751_328_000_000_000_000_i64;
        let sql = active_fingerprints(
            "log_metrics_5s",
            None,
            TimeWindow {
                start_ns: b + 1_000_000_000,
                end_ns: b + 4_000_000_000,
            },
            RES_5S,
        );
        assert!(
            sql.contains(&format!("bucket_ns >= {b} AND")),
            "lower bound must floor to the containing bucket: {sql}"
        );
    }

    #[test]
    fn service_predicate_is_bare_equality_for_one_service() {
        assert_eq!(
            service_predicate(&[literal("checkout")]),
            "service = 'checkout'"
        );
    }

    #[test]
    fn service_predicate_is_in_list_for_multiple_services() {
        assert_eq!(
            service_predicate(&[literal("checkout"), literal("billing")]),
            "service IN ('checkout', 'billing')"
        );
    }

    #[test]
    fn fp_list_joins_with_comma_space() {
        assert_eq!(fp_list(&[1, 2, 3]), "1, 2, 3");
    }

    #[test]
    fn metric_range_omits_prewhere_when_services_is_empty_the_rollup_path() {
        let sql = metric_range(
            MetricSource::new("log_metrics_5s", MetricShape::RollupCount),
            &[],
            &[1, 2],
            TimeWindow {
                start_ns: 0,
                end_ns: 100,
            },
            ScanLowerBound::Exclusive,
            60,
            &[],
        );
        assert!(!sql.contains("PREWHERE"));
    }

    #[test]
    fn metric_range_renders_singleton_prewhere_for_the_raw_fallback() {
        let sql = metric_range(
            MetricSource::new("log_samples", MetricShape::RawCount),
            &[literal("checkout")],
            &[1, 2],
            TimeWindow {
                start_ns: 0,
                end_ns: 100,
            },
            ScanLowerBound::Exclusive,
            60,
            &[],
        );
        assert!(sql.contains("PREWHERE service = 'checkout'\n"));
    }

    #[test]
    fn metric_range_renders_in_list_prewhere_for_multiple_services() {
        let sql = metric_range(
            MetricSource::new("log_samples", MetricShape::RawCount),
            &[literal("checkout"), literal("billing")],
            &[1, 2],
            TimeWindow {
                start_ns: 0,
                end_ns: 100,
            },
            ScanLowerBound::Exclusive,
            60,
            &[],
        );
        assert!(sql.contains("PREWHERE service IN ('checkout', 'billing')\n"));
    }

    /// Issue #74 first-bound AC (hermetic half): the first tail page
    /// carries the explicit exclusive API `start` bound — the repo
    /// stage-3 convention — and NO keyset term, byte-exact.
    #[test]
    fn stage3_keyset_first_page_is_byte_exact_with_the_exclusive_start_bound() {
        let sql = stage3_keyset(
            "log_samples",
            &[literal("checkout")],
            &[18374],
            TimeWindow {
                start_ns: 1_000,
                end_ns: 2_000,
            },
            KeysetLower::First,
            Direction::Forward,
            &[],
            500,
        );
        assert_eq!(
            sql,
            "SELECT fingerprint, timestamp_ns, body, cityHash64(body) AS body_hash, structured_metadata\n\
             FROM log_samples\n\
             PREWHERE service = 'checkout'\n\
             WHERE fingerprint IN (18374)\n\
             \x20 AND timestamp_ns > 1000 AND timestamp_ns <= 2000\n\
             ORDER BY timestamp_ns ASC, fingerprint ASC, body_hash ASC, body ASC\n\
             LIMIT 500"
        );
    }

    /// Issue #74 (round-4 adjudication #1): the keyset page — inclusive
    /// `>=` composite predicate, redundant time lower bound for granule
    /// pruning, total ORDER BY with the raw-`body` tiebreaker, and the
    /// server-side occurrence-count `OFFSET` — byte-exact.
    #[test]
    fn stage3_keyset_later_page_is_byte_exact_with_inclusive_tuple_and_offset() {
        let sql = stage3_keyset(
            "log_samples",
            &[literal("checkout"), literal("billing")],
            &[1, 2],
            TimeWindow {
                start_ns: 1_000,
                end_ns: 2_000,
            },
            KeysetLower::After {
                tuple: (1_500, 7, 42),
                offset: 3,
            },
            Direction::Forward,
            &[err_line_filter()],
            500,
        );
        assert_eq!(
            sql,
            "SELECT fingerprint, timestamp_ns, body, cityHash64(body) AS body_hash, structured_metadata\n\
             FROM log_samples\n\
             PREWHERE service IN ('checkout', 'billing')\n\
             WHERE fingerprint IN (1, 2)\n\
             \x20 AND timestamp_ns >= 1500 AND timestamp_ns <= 2000\n\
             \x20 AND (timestamp_ns, fingerprint, cityHash64(body)) >= (1500, 7, 42)\n\
             \x20 AND body LIKE '%err%'\n\
             ORDER BY timestamp_ns ASC, fingerprint ASC, body_hash ASC, body ASC\n\
             LIMIT 500 OFFSET 3"
        );
    }

    /// Issue #90 (backward paging): the query default's newest-first
    /// keyset — DESC on every ORDER column, the exclusive API `start` as
    /// the fixed lower bound, and the mirrored redundant `timestamp_ns <=
    /// end` upper bound so the first page's granule pruning matches the
    /// forward form.
    #[test]
    fn stage3_keyset_backward_first_page_is_byte_exact_desc_with_the_window_bounds() {
        let sql = stage3_keyset(
            "log_samples",
            &[literal("checkout")],
            &[18374],
            TimeWindow {
                start_ns: 1_000,
                end_ns: 2_000,
            },
            KeysetLower::First,
            Direction::Backward,
            &[],
            500,
        );
        assert_eq!(
            sql,
            "SELECT fingerprint, timestamp_ns, body, cityHash64(body) AS body_hash, structured_metadata\n\
             FROM log_samples\n\
             PREWHERE service = 'checkout'\n\
             WHERE fingerprint IN (18374)\n\
             \x20 AND timestamp_ns > 1000 AND timestamp_ns <= 2000\n\
             ORDER BY timestamp_ns DESC, fingerprint DESC, body_hash DESC, body DESC\n\
             LIMIT 500"
        );
    }

    /// Issue #90 (backward paging): a later newest-first page — the
    /// inclusive `<=` composite tuple, the redundant `timestamp_ns <= ts`
    /// upper bound (walking down), the fixed exclusive `timestamp_ns >
    /// start` lower bound, DESC total order, and the occurrence-count
    /// `OFFSET` — byte-exact.
    #[test]
    fn stage3_keyset_backward_later_page_is_byte_exact_with_le_tuple_and_offset() {
        let sql = stage3_keyset(
            "log_samples",
            &[literal("checkout"), literal("billing")],
            &[1, 2],
            TimeWindow {
                start_ns: 1_000,
                end_ns: 2_000,
            },
            KeysetLower::After {
                tuple: (1_500, 7, 42),
                offset: 3,
            },
            Direction::Backward,
            &[err_line_filter()],
            500,
        );
        assert_eq!(
            sql,
            "SELECT fingerprint, timestamp_ns, body, cityHash64(body) AS body_hash, structured_metadata\n\
             FROM log_samples\n\
             PREWHERE service IN ('checkout', 'billing')\n\
             WHERE fingerprint IN (1, 2)\n\
             \x20 AND timestamp_ns > 1000 AND timestamp_ns <= 1500\n\
             \x20 AND (timestamp_ns, fingerprint, cityHash64(body)) <= (1500, 7, 42)\n\
             \x20 AND body LIKE '%err%'\n\
             ORDER BY timestamp_ns DESC, fingerprint DESC, body_hash DESC, body DESC\n\
             LIMIT 500 OFFSET 3"
        );
    }

    /// Issue #74 pre-query clamp AC (hermetic half): the caller-clamped
    /// fetch limit is exactly what lands in the SQL `LIMIT` — nothing in
    /// the builder can widen it back.
    #[test]
    fn stage3_keyset_limit_is_the_callers_clamped_fetch_limit_verbatim() {
        let sql = stage3_keyset(
            "log_samples",
            &[literal("checkout")],
            &[1],
            TimeWindow {
                start_ns: 0,
                end_ns: 10,
            },
            KeysetLower::First,
            Direction::Forward,
            &[],
            5_000,
        );
        assert!(sql.ends_with("LIMIT 5000"), "{sql}");
    }

    /// Issue #74 AC6 (hermetic half): the no-filter stats aggregation is
    /// rollup-served — zero body reads — byte-exact.
    #[test]
    fn log_stats_rollup_is_byte_exact() {
        let sql = log_stats_rollup(
            "log_metrics_5s",
            &[18374, 99120],
            TimeWindow {
                start_ns: 1_000,
                end_ns: 2_000,
            },
        );
        assert_eq!(
            sql,
            "SELECT uniqExact(fingerprint) AS streams, uniqExact(toDate(fromUnixTimestamp64Nano(bucket_ns))) AS chunks, sum(count) AS entries, sum(bytes) AS bytes\n\
             FROM log_metrics_5s\n\
             WHERE fingerprint IN (18374, 99120) AND bucket_ns > 1000 AND bucket_ns <= 2000"
        );
        assert!(!sql.contains("body"), "rollup stats must never read body");
    }

    /// Issue #171 (M7-C3): the `/api/logs/v1/patterns` aggregation is
    /// byte-exact — the pushed-down top-1000 with the `fingerprint IN` PK
    /// prefix, half-open `[start, end)` window, `step_ns` re-bucketing, and the
    /// `groupArray((ts_ns, cnt))` samples array. Never reads `body`.
    #[test]
    fn log_patterns_read_is_byte_exact() {
        let sql = log_patterns_read(
            "log_patterns",
            &[18374, 99120],
            TimeWindow {
                start_ns: 1_000,
                end_ns: 2_000,
            },
            10_000_000_000,
        );
        assert_eq!(
            sql,
            "SELECT pattern, sum(cnt) AS total, arraySort(x -> x.1, groupArray((ts_ns, cnt))) AS samples\n\
             FROM (\n  \
               SELECT pattern, intDiv(bucket_ns, 10000000000) * 10000000000 AS ts_ns, sum(count) AS cnt\n  \
               FROM log_patterns\n  \
               WHERE fingerprint IN (18374, 99120) AND bucket_ns >= 1000 AND bucket_ns < 2000\n  \
               GROUP BY pattern, ts_ns\n\
             )\n\
             GROUP BY pattern\n\
             ORDER BY total DESC, pattern ASC\n\
             LIMIT 1000"
        );
        assert!(!sql.contains("body"), "patterns must never read body");
    }

    /// Issue #74 AC6 (hermetic half): the line-filtered stats fallback
    /// scans `log_samples` with stage-3's exact PREWHERE + skip-index
    /// line-filter pushdown — byte-exact.
    #[test]
    fn log_stats_raw_is_byte_exact_with_line_filter_pushdown() {
        let sql = log_stats_raw(
            "log_samples",
            &[literal("checkout")],
            &[18374],
            TimeWindow {
                start_ns: 1_000,
                end_ns: 2_000,
            },
            &[err_line_filter()],
        );
        assert_eq!(
            sql,
            "SELECT uniqExact(fingerprint) AS streams, uniqExact(toDate(fromUnixTimestamp64Nano(timestamp_ns))) AS chunks, count() AS entries, sum(length(body)) AS bytes\n\
             FROM log_samples\n\
             PREWHERE service = 'checkout'\n\
             WHERE fingerprint IN (18374)\n\
             \x20 AND timestamp_ns > 1000 AND timestamp_ns <= 2000\n\
             \x20 AND body LIKE '%err%'"
        );
    }

    #[test]
    fn metric_instant_renders_the_same_prewhere_contract() {
        let sql = metric_instant(
            MetricSource::new("log_samples", MetricShape::RawCount),
            &[literal("checkout")],
            &[1],
            TimeWindow {
                start_ns: 0,
                end_ns: 100,
            },
            ScanLowerBound::Exclusive,
            &[],
            ScanProjection::WithStructuredMetadata,
        );
        assert!(sql.contains("PREWHERE service = 'checkout'\n"));
        assert!(!sql.contains("intDiv"));
    }

    /// Issue #227 review round 11: an UNDERFLOWED scan widening renders the
    /// lower bound INCLUSIVELY (`>= i64::MIN` — vacuous, so a sample stored
    /// at exactly `i64::MIN` survives the predicate, matching the
    /// reference's `(t-range, t]` whose logical bound sits below the
    /// representable domain), across all four metric builders.
    #[test]
    fn an_underflowed_scan_lower_bound_renders_inclusively() {
        let window = TimeWindow {
            start_ns: i64::MIN,
            end_ns: i64::MIN,
        };
        let raw_source = MetricSource::new("log_samples", MetricShape::RawCount);
        let svc = [literal("checkout")];
        let sliding = metric_raw_samples_sliding(
            "log_samples",
            &svc,
            &[1],
            window,
            ScanLowerBound::Inclusive,
            &[],
            ScanProjection::WithStructuredMetadata,
        );
        assert!(
            sliding.contains(
                "timestamp_ns >= -9223372036854775808 AND timestamp_ns <= -9223372036854775808"
            ),
            "sliding scan must be lower-inclusive on underflow: {sliding}"
        );
        let instant_raw = metric_raw_samples(
            "log_samples",
            &svc,
            &[1],
            window,
            ScanLowerBound::Inclusive,
            &[],
            ScanProjection::WithStructuredMetadata,
        );
        assert!(
            instant_raw.contains("timestamp_ns >= -9223372036854775808"),
            "instant raw scan must be lower-inclusive on underflow: {instant_raw}"
        );
        let instant_agg = metric_instant(
            raw_source,
            &svc,
            &[1],
            window,
            ScanLowerBound::Inclusive,
            &[],
            ScanProjection::WithStructuredMetadata,
        );
        assert!(
            instant_agg.contains("timestamp_ns >= -9223372036854775808"),
            "instant aggregate must be lower-inclusive on underflow: {instant_agg}"
        );
        let range_agg = metric_range(
            raw_source,
            &svc,
            &[1],
            window,
            ScanLowerBound::Inclusive,
            60,
            &[],
        );
        assert!(
            range_agg.contains("timestamp_ns >= -9223372036854775808"),
            "range aggregate must be lower-inclusive on underflow: {range_agg}"
        );
    }

    /// The negative control: an `Exclusive` bound at exactly `i64::MIN` (a
    /// LEGITIMATELY-computed lower bound, e.g. `start = i64::MIN + 1` with
    /// `[1ns]`) keeps the half-open `>` — a sample at exactly `i64::MIN`
    /// stays excluded, as in the reference.
    #[test]
    fn a_legitimate_i64_min_scan_lower_bound_stays_exclusive() {
        let window = TimeWindow {
            start_ns: i64::MIN,
            end_ns: i64::MIN + 1,
        };
        let svc = [literal("checkout")];
        let sliding = metric_raw_samples_sliding(
            "log_samples",
            &svc,
            &[1],
            window,
            ScanLowerBound::Exclusive,
            &[],
            ScanProjection::WithStructuredMetadata,
        );
        assert!(
            sliding.contains("timestamp_ns > -9223372036854775808 AND"),
            "a non-underflowing i64::MIN bound must stay exclusive: {sliding}"
        );
        assert!(
            !sliding.contains(">="),
            "no inclusive lower bound: {sliding}"
        );
    }
}
