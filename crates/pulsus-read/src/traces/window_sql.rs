//! The trace read path's time-window bound conventions, and the single
//! place each one's row-level and partition-level clauses are rendered
//! from (issue #525).
//!
//! **Two conventions, both live, both correct.** A trace read's window
//! ends one of two ways, and which one it is decides two things that must
//! agree:
//!
//! ```text
//!                          day D                     day D+1
//!               ...  23:59:59.999999999 | 00:00:00.000000000  ...
//!                                       ^
//!                                    end_ns  (midnight, day D+1)
//!
//!   StartOpenEndClosed   ts > start AND ts <= end
//!       last included ns = end_ns          -> end day = D+1
//!       reads partitions ... D, D+1        (D+1 holds exactly one
//!                                           selectable nanosecond, and
//!                                           a row can sit on it)
//!
//!   StartClosedEndOpen   ts >= start AND ts < end
//!       last included ns = end_ns - 1      -> end day = D
//!       reads partitions ... D            (no row in D+1 can match)
//! ```
//!
//! The row bound (`timestamp_ns` operators) and the day bound (the
//! `date` partition prune) are two renderings of ONE fact: which
//! nanosecond is the last the window contains. Before this module they
//! were written out separately in three files, so the day bound could be
//! given the other convention's rule while the row bound kept its own,
//! and nothing would have failed.
//!
//! **The two possible mismatches are not the same defect.** Which one
//! you get depends on which convention is handed the other's day rule:
//!
//! ```text
//!   inclusive window (ts <= end) given the EXCLUSIVE day rule
//!       day clause becomes `date <= D` — one day NARROWER than the row
//!       bound admits. A span stored at exactly end_ns is inside the
//!       window, but its partition is never read.
//!       => THE ANSWER LOSES ROWS. Not a slower query, a wrong one.
//!
//!   exclusive window (ts < end) given the INCLUSIVE day rule
//!       day clause becomes `date <= D+1` — one day WIDER. No row in
//!       D+1 can satisfy `ts < end_ns` anyway.
//!       => EVERY ANSWER IDENTICAL, one extra partition read.
//!          This is the silent one: there is no result to compare, so
//!          no result comparison can see it.
//! ```
//!
//! Both were measured against ClickHouse on a two-partition corpus
//! (issue #525, `trace_attrs_idx` DDL, 500 000 attribute rows per UTC
//! day, one row on the boundary nanosecond). Narrowing the inclusive
//! window returned 499 999 rows where the correct clause returned
//! 500 001. Widening the exclusive window returned the same 500 000 rows
//! either way, reading 2 partitions instead of 1.
//!
//! **The partition count is the figure that transfers: exactly one extra
//! day's partition. The row and byte counts do not** — they are
//! properties of the data. That same widening read 500 001 extra rows on
//! a corpus of 200 attribute keys, where `timestamp_ns` sits behind an
//! unconstrained prefix of the sorting key and cannot prune granules;
//! but only 33 057 extra rows on a corpus with ONE key and five distinct
//! values, where the leading columns were near-degenerate and the
//! primary key pruned inside the extra partition. Quote the partition
//! count; treat any row or byte figure as one corpus's illustration.
//!
//! That is why [`WindowSql`] has no public fields and no `new`: the only
//! ways to build one are the two constructors named after their
//! operators, and BOTH clauses come out of the one value. Changing a
//! call site's convention now changes the row bound too, which moves the
//! byte-frozen SQL goldens (`crates/pulsus-read/tests/golden/`) — the
//! mistake is loud instead of silent.
//!
//! **Who uses which:**
//!
//! **A request window is half-open; an instant set named inside the query
//! text is not.** Requirement R9 gives every window that a *request*
//! selects spans with one rule, `start <= ts < end`. What a query's own
//! operands mean is defined by the query language and follows the
//! reference: `compare()`'s `start`/`end` arguments are right-closed there
//! and stay right-closed here, as does a metrics range selector's `[5m]`.
//!
//! | caller | convention | why |
//! |---|---|---|
//! | [`super::search_sql`] | `StartClosedEndOpen` | R9, docs/api.md §4.2 `[start, end)` |
//! | [`super::graph_sql`] | `StartClosedEndOpen` | docs/api.md §4.5 `[start, end)` |
//! | [`super::metrics_sql`] evaluation window | `StartClosedEndOpen` | `metrics_plan`'s snapped `[start, end)` |
//! | [`super::metrics_sql`] `compare()` selection window | `StartOpenEndClosed` | the reference's `spanStartTime > start && spanStartTime <= end` (Tempo `pkg/traceql/engine_metrics_compare.go:98-110` @ v3.0.2) — a query operand, not a request window |
//! | [`super::search_sql`] `trace_recent` bucket clause ([`WindowSql::bucket_clause`], issue #560) | `StartClosedEndOpen` | the same search bound, read at bucket grain |
//!
//! [`super::tags_sql::DaySpan`] is deliberately NOT expressed here. The
//! tag-discovery reads carry a day bound and no `timestamp_ns` bound at
//! all (a sub-day predicate prunes nothing on `trace_spans` and defeats
//! the `span_name_day` projection — see that type's own doc), so they
//! have no row bound for a day bound to agree with, and resolving their
//! window inclusively at both ends is the deliberately wider read.

use super::search_sql::date_literal;

/// The resource table's `day` partition bound, from two nanosecond
/// EXPRESSIONS — a window's own literals, or the bound names a statement
/// that reads its own extent uses (issue #587).
///
/// **One owning text, and the explicit `'UTC'` is load-bearing.**
/// `resources.day` is written by the writer, in Rust, with no timezone in
/// the computation at all, so the bound has to say `'UTC'` to match it
/// whatever any DDL does. Measured: for `1700000000000000000` the bare
/// form returns `2023-11-15` in a non-UTC session and the explicit form
/// `2023-11-14`, and a trace near a day boundary would then select the
/// wrong partition and return spans whose resources carry no attributes.
pub fn resources_day_bound(lo: &str, hi: &str) -> String {
    format!(
        "day >= toDate(fromUnixTimestamp64Nano({lo}), 'UTC') \
         AND day <= toDate(fromUnixTimestamp64Nano({hi}), 'UTC')"
    )
}

const NS_PER_DAY: i64 = 86_400_000_000_000;

/// The recency table's bucket width in nanoseconds (issue #560). THE
/// definition — the reader owns the value, `trace_recent_mv`'s
/// `intDiv(timestamp_ns, …)` carries the same number as TEXT, and
/// `tests/traces_recency_table_literals.rs` binds the two. A whole number
/// of these makes a UTC day (288 of them), so a bucket never straddles
/// midnight and `trace_recent.date` is a function of `bucket`.
pub const RECENT_BUCKET_NS: i64 = 300_000_000_000;

/// Which nanoseconds a trace read's window contains — the ONE
/// declaration [`WindowSql`] derives both its clauses from.
///
/// Not `pub`ly constructible as a bare field anywhere: pick a
/// [`WindowSql`] constructor instead, so the choice is made once per
/// window and both clauses follow it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowBounds {
    /// `timestamp_ns > start_ns AND timestamp_ns <= end_ns`. `end_ns`
    /// itself is IN the window, so the last included nanosecond is
    /// `end_ns` and the end day is `end_ns`'s day.
    StartOpenEndClosed,
    /// `timestamp_ns >= start_ns AND timestamp_ns < end_ns`. `end_ns`
    /// itself is OUT of the window, so the last included nanosecond is
    /// `end_ns - 1` and the end day is `(end_ns - 1)`'s day — a window
    /// ending exactly at midnight never drags in the next day's
    /// partition.
    StartClosedEndOpen,
}

/// A nanosecond window together with the convention its ends follow,
/// able to render both of the clauses that convention decides.
///
/// Fields are private and there is no `new`: a value exists only by way
/// of [`WindowSql::start_open_end_closed`] or
/// [`WindowSql::start_closed_end_open`], each named for the operators it
/// emits. That is the whole point of the type — see the module doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowSql {
    start_ns: i64,
    end_ns: i64,
    bounds: WindowBounds,
}

impl WindowSql {
    /// A window rendering `timestamp_ns > start_ns AND timestamp_ns <=
    /// end_ns` — `end_ns` is IN the window.
    pub fn start_open_end_closed(start_ns: i64, end_ns: i64) -> Self {
        WindowSql {
            start_ns,
            end_ns,
            bounds: WindowBounds::StartOpenEndClosed,
        }
    }

    /// A window rendering `timestamp_ns >= start_ns AND timestamp_ns <
    /// end_ns` — `end_ns` is OUT of the window.
    pub fn start_closed_end_open(start_ns: i64, end_ns: i64) -> Self {
        WindowSql {
            start_ns,
            end_ns,
            bounds: WindowBounds::StartClosedEndOpen,
        }
    }

    /// Which convention this window was built with.
    pub fn bounds(self) -> WindowBounds {
        self.bounds
    }

    /// The last nanosecond the window contains. The end day is this
    /// nanosecond's day, never `end_ns`'s day unconditionally.
    pub fn last_included_ns(self) -> i64 {
        match self.bounds {
            WindowBounds::StartOpenEndClosed => self.end_ns,
            WindowBounds::StartClosedEndOpen => self.end_ns - 1,
        }
    }

    /// The first nanosecond the window contains — the mirror of
    /// [`WindowSql::last_included_ns`], for a clause whose operators are
    /// written out rather than rendered by [`WindowSql::time_clause`].
    ///
    /// [`super::search_sql`]'s `trace_recent` row bound is the one such
    /// clause: `ts_max >= <this> AND ts_min <= <last included>`. Taking
    /// `start_ns` there instead would leave that clause on whichever
    /// convention it was written under while every other rendering
    /// followed the constructor, which is the shape that drops a trace
    /// whose newest span sits exactly on `start_ns`.
    pub fn first_included_ns(self) -> i64 {
        match self.bounds {
            WindowBounds::StartOpenEndClosed => self.start_ns + 1,
            WindowBounds::StartClosedEndOpen => self.start_ns,
        }
    }

    /// The row-level bound on `timestamp_ns`, operators chosen by the
    /// convention.
    pub fn time_clause(self) -> String {
        self.time_clause_on("timestamp_ns")
    }

    /// [`WindowSql::time_clause`] over a named column, because the trace
    /// fetch's span table calls the same quantity `start_ns` (issue #587).
    ///
    /// **Factored, not duplicated.** A second rendering of the row bound
    /// is a second place the convention's operators can be written down,
    /// which is the defect this module exists to remove.
    pub fn time_clause_on(self, column: &str) -> String {
        let (lo, hi) = match self.bounds {
            WindowBounds::StartOpenEndClosed => (">", "<="),
            WindowBounds::StartClosedEndOpen => (">=", "<"),
        };
        format!(
            "{column} {lo} {} AND {column} {hi} {}",
            self.start_ns, self.end_ns
        )
    }

    /// The span table's row bound (issue #587) — [`time_clause_on`] over
    /// `start_ns`.
    ///
    /// [`time_clause_on`]: WindowSql::time_clause_on
    pub fn span_time_clause(self) -> String {
        self.time_clause_on("start_ns")
    }

    /// The span table's daily-partition bound (issue #588), the SECOND
    /// production delegation to [`date_clause_on`] —
    /// [`WindowSql::date_clause`] is the first.
    ///
    /// The expression is byte-identical to `spans`' own `PARTITION BY`
    /// (`schema/schema.sql`), **including the explicit `'UTC'`**: a bare
    /// `toDate(fromUnixTimestamp64Nano(start_ns))` is a different
    /// expression and the partition prune is lost. Measured with
    /// `EXPLAIN indexes = 1` on 26.3.29.7, the `Partition` stage's
    /// `Condition` reads `and((toDate(fromUnixTimestamp64Nano(start_ns),
    /// 'UTC') in (-Inf, 20718]), (… in [20718, +Inf)))` with the zone and
    /// `true` without it.
    ///
    /// **No part saving is claimed for it.** On that table
    /// `intDiv(start_ns, 300000000000)` leads the sorting key and had
    /// already cut three parts to one, so the day clause pruned no
    /// additional part. It is kept because `docs/TraceQL/sql-schema.md`
    /// puts the day bound under the one-value rule and
    /// [`super::spans::fetch`] writes the same expression out.
    ///
    /// [`date_clause_on`]: WindowSql::date_clause_on
    pub fn span_day_clause(self) -> String {
        self.date_clause_on("toDate(fromUnixTimestamp64Nano(start_ns), 'UTC')")
    }

    /// The resource table's `day` partition bound (issue #587), from this
    /// window's own two nanoseconds. [`resources_day_bound`] is the text.
    pub fn resources_day_clause(self) -> String {
        resources_day_bound(
            &self.first_included_ns().to_string(),
            &self.last_included_ns().to_string(),
        )
    }

    /// The per-trace read's `day` bound (issue #594 part 1): the day
    /// before this window's first day to the day after its last, so a
    /// trace crossing a midnight either side is read whole.
    pub fn per_trace_day_clause(self) -> String {
        resources_day_bound(
            &self
                .first_included_ns()
                .saturating_sub(NS_PER_DAY)
                .to_string(),
            &self
                .last_included_ns()
                .saturating_add(NS_PER_DAY)
                .to_string(),
        )
    }

    /// Whether the span-table bucket `var` lies outside this window's
    /// buckets (issue #594 part 1), divided server-side as
    /// [`WindowSql::span_bucket_clause`] is.
    pub fn bucket_outside(self, var: &str) -> String {
        format!(
            "NOT ({var} BETWEEN intDiv({}, {bucket}) AND intDiv({}, {bucket}))",
            self.first_included_ns(),
            self.last_included_ns(),
            bucket = RECENT_BUCKET_NS
        )
    }

    /// The span table's leading sort-key bound (issue #587).
    ///
    /// **Both sides are divided server-side**, so the reader never
    /// reproduces `intDiv`'s rounding: `intDiv` truncates toward zero
    /// where this module's own `div_euclid` floors, and the two disagree
    /// below the epoch. Handing the engine the nanoseconds and letting it
    /// divide removes the question rather than answering it.
    pub fn span_bucket_clause(self) -> String {
        format!(
            "intDiv(start_ns, {bucket}) BETWEEN intDiv({}, {bucket}) AND intDiv({}, {bucket})",
            self.first_included_ns(),
            self.last_included_ns(),
            bucket = RECENT_BUCKET_NS
        )
    }

    /// The daily-partition pruning clause on the `date` column —
    /// `date >= toDate('…') AND date <= toDate('…')`, the same
    /// expression `trace_attrs_idx`/`trace_edges` are partitioned by
    /// (docs/schemas.md §4.1), so it prunes parts rather than filtering
    /// rows.
    ///
    /// The end day comes from [`WindowSql::last_included_ns`], which is
    /// what makes it agree with [`WindowSql::time_clause`].
    ///
    /// The START day is `start_ns`'s own day under BOTH conventions,
    /// including `StartOpenEndClosed` where `start_ns` is excluded. A
    /// `start_ns` sitting on the final nanosecond of a day therefore
    /// keeps that day's partition even though no row in it can match:
    /// one partition wider than strictly needed, never narrower, so the
    /// prune stays a superset of the row bound. It is left as it is
    /// because narrowing it would move committed SQL; it is recorded
    /// here so the asymmetry reads as a decision rather than an
    /// oversight.
    pub fn date_clause(self) -> String {
        self.date_clause_on("date")
    }

    /// [`WindowSql::date_clause`] over a named column (issue #587), the
    /// same factoring as [`WindowSql::time_clause_on`] and for the same
    /// reason.
    pub fn date_clause_on(self, column: &str) -> String {
        let start_days = self.start_ns.div_euclid(NS_PER_DAY);
        let end_days = self.last_included_ns().div_euclid(NS_PER_DAY);
        format!(
            "{column} >= {} AND {column} <= {}",
            date_literal(start_days),
            date_literal(end_days)
        )
    }

    /// `bucket >= <lo> AND bucket <= <hi>` over `trace_recent`'s leading
    /// sort-key column (issue #560). `lo` from `start_ns`, `hi` from
    /// [`WindowSql::last_included_ns`], both by `div_euclid` — the floor
    /// [`WindowSql::date_clause`] uses — and rendered SIGNED with no
    /// clamp.
    ///
    /// ```text
    ///   reader   ns.div_euclid(300_000_000_000)          floor
    ///   writer   toUInt32(intDiv(timestamp_ns, 3e11))    truncate
    ///   the two agree for every ns >= 0; the first disagreement is at
    ///   ns = -1 (reader -1, writer 0), which no stored row reaches:
    ///   ingest refuses a span before 1970-01-01
    /// ```
    ///
    /// No clamp, and that is a decision: a `UInt32` column compared with
    /// a negative literal is promoted to a signed comparison and answers
    /// correctly (measured on 26.3: `bucket >= -7` and `bucket >= 0`
    /// select the same stored rows). The largest bucket any `i64`
    /// produces is 30,744,573, 0.7% of the `UInt32` ceiling, so neither
    /// side can overflow. The clause selects a superset of the rows the
    /// row-level `ts_max`/`ts_min` bound admits, for every window the
    /// request parser accepts.
    pub fn bucket_clause(self) -> String {
        format!(
            "bucket >= {} AND bucket <= {}",
            self.start_ns.div_euclid(RECENT_BUCKET_NS),
            self.last_included_ns().div_euclid(RECENT_BUCKET_NS)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2023-11-15 00:00:00 UTC — exactly a UTC day boundary, which is
    /// the ONLY input at which the two conventions' day bounds differ.
    /// Every test below that is about the difference uses it; a window
    /// ending mid-day cannot discriminate, which is why the pre-#525
    /// unit tests did not.
    const MIDNIGHT_NS: i64 = 1_700_006_400_000_000_000;
    /// 2023-11-14 00:00:00 UTC, the previous midnight.
    const PREV_MIDNIGHT_NS: i64 = 1_699_920_000_000_000_000;

    #[test]
    fn the_last_included_nanosecond_is_end_ns_when_the_end_is_closed() {
        let w = WindowSql::start_open_end_closed(PREV_MIDNIGHT_NS, MIDNIGHT_NS);
        assert_eq!(w.last_included_ns(), MIDNIGHT_NS);
        assert_eq!(w.bounds(), WindowBounds::StartOpenEndClosed);
    }

    #[test]
    fn the_last_included_nanosecond_is_one_before_end_ns_when_the_end_is_open() {
        let w = WindowSql::start_closed_end_open(PREV_MIDNIGHT_NS, MIDNIGHT_NS);
        assert_eq!(w.last_included_ns(), MIDNIGHT_NS - 1);
        assert_eq!(w.bounds(), WindowBounds::StartClosedEndOpen);
    }

    #[test]
    fn the_closed_end_convention_renders_its_operators() {
        assert_eq!(
            WindowSql::start_open_end_closed(10, 20).time_clause(),
            "timestamp_ns > 10 AND timestamp_ns <= 20"
        );
    }

    #[test]
    fn the_open_end_convention_renders_its_operators() {
        assert_eq!(
            WindowSql::start_closed_end_open(10, 20).time_clause(),
            "timestamp_ns >= 10 AND timestamp_ns < 20"
        );
    }

    /// A closed end at midnight KEEPS the next day's partition, because
    /// a row stored at exactly that nanosecond is in the window.
    #[test]
    fn a_closed_end_at_midnight_keeps_the_day_the_boundary_starts() {
        assert_eq!(
            WindowSql::start_open_end_closed(PREV_MIDNIGHT_NS, MIDNIGHT_NS).date_clause(),
            "date >= toDate('2023-11-14') AND date <= toDate('2023-11-15')"
        );
    }

    /// An open end at midnight DROPS it: no row in that day can match,
    /// so reading its partition is pure cost.
    #[test]
    fn an_open_end_at_midnight_drops_the_day_the_boundary_starts() {
        assert_eq!(
            WindowSql::start_closed_end_open(PREV_MIDNIGHT_NS, MIDNIGHT_NS).date_clause(),
            "date >= toDate('2023-11-14') AND date <= toDate('2023-11-14')"
        );
    }

    /// The relational form of the two tests above: whatever the day
    /// literals are, on a day boundary the two conventions must NOT
    /// render the same day clause. Stated as a difference so that
    /// collapsing the conventions into one fails here even if someone
    /// also updates the two expected strings above to match each other.
    #[test]
    fn the_two_conventions_disagree_about_a_window_ending_on_a_day_boundary() {
        let closed = WindowSql::start_open_end_closed(PREV_MIDNIGHT_NS, MIDNIGHT_NS);
        let open = WindowSql::start_closed_end_open(PREV_MIDNIGHT_NS, MIDNIGHT_NS);
        assert_ne!(
            closed.date_clause(),
            open.date_clause(),
            "a window ending exactly at midnight touches one more day when its end is \
             INCLUDED than when it is EXCLUDED; if these render alike, one convention has \
             been given the other's day rule — and the two directions differ: widening the \
             EXCLUSIVE window reads a whole extra partition with every answer unchanged, \
             while narrowing the INCLUSIVE one drops spans stored at exactly end_ns \
             (issue #525)"
        );
        assert_ne!(closed.last_included_ns(), open.last_included_ns());
    }

    /// Away from a day boundary the two agree, which is exactly why the
    /// mistake is invisible to any test whose window ends mid-day.
    #[test]
    fn the_two_conventions_agree_about_a_window_ending_mid_day() {
        let mid = MIDNIGHT_NS + 4_400_000_000_000;
        assert_eq!(
            WindowSql::start_open_end_closed(PREV_MIDNIGHT_NS, mid).date_clause(),
            WindowSql::start_closed_end_open(PREV_MIDNIGHT_NS, mid).date_clause()
        );
    }

    /// Pre-epoch windows: `div_euclid` floors towards negative infinity,
    /// so a negative nanosecond lands in the day that CONTAINS it rather
    /// than in the day above it.
    #[test]
    fn a_pre_epoch_open_end_at_midnight_drops_the_day_the_boundary_starts() {
        assert_eq!(
            WindowSql::start_closed_end_open(-2 * NS_PER_DAY, -NS_PER_DAY).date_clause(),
            "date >= toDate('1969-12-30') AND date <= toDate('1969-12-30')"
        );
        assert_eq!(
            WindowSql::start_open_end_closed(-2 * NS_PER_DAY, -NS_PER_DAY).date_clause(),
            "date >= toDate('1969-12-30') AND date <= toDate('1969-12-31')"
        );
    }

    /// `F-5`: **the span table's three renderings all derive from the
    /// same last-included nanosecond**, and the resource bound carries its
    /// explicit zone.
    ///
    /// The window used is the one input that can discriminate: it ends
    /// exactly at a UTC midnight, which is also exactly a five-minute
    /// bucket boundary. Under the `[start, end)` convention the last
    /// included nanosecond is the one before it, so the three clauses must
    /// render the **previous** day, the **previous** bucket and
    /// `start_ns < <midnight>` together. A clause given `end_ns` instead
    /// moves one of the three and not the others.
    #[test]
    fn the_three_span_renderings_share_one_last_included_nanosecond() {
        let w = WindowSql::start_closed_end_open(PREV_MIDNIGHT_NS, MIDNIGHT_NS);
        assert_eq!(w.last_included_ns(), MIDNIGHT_NS - 1);

        assert_eq!(
            w.span_time_clause(),
            format!("start_ns >= {PREV_MIDNIGHT_NS} AND start_ns < {MIDNIGHT_NS}"),
            "the row bound names the span table's own column and keeps the \
             convention's operators"
        );
        assert_eq!(
            w.resources_day_clause(),
            format!(
                "day >= toDate(fromUnixTimestamp64Nano({PREV_MIDNIGHT_NS}), 'UTC') \
                 AND day <= toDate(fromUnixTimestamp64Nano({}), 'UTC')",
                MIDNIGHT_NS - 1
            ),
            "the resource day bound's upper argument is the last INCLUDED nanosecond"
        );
        assert_eq!(
            w.resources_day_clause().matches(", 'UTC')").count(),
            2,
            "both conversions carry the zone explicitly"
        );
        assert_eq!(
            w.span_bucket_clause(),
            format!(
                "intDiv(start_ns, 300000000000) BETWEEN intDiv({PREV_MIDNIGHT_NS}, 300000000000) \
                 AND intDiv({}, 300000000000)",
                MIDNIGHT_NS - 1
            ),
            "both sides of the bucket bound are divided server-side"
        );

        // The discriminator: the same window under the INCLUSIVE
        // convention includes the midnight nanosecond, so all three
        // clauses move together. A clause that took `end_ns` regardless
        // would render the inclusive form under both conventions.
        let inclusive = WindowSql::start_open_end_closed(PREV_MIDNIGHT_NS, MIDNIGHT_NS);
        assert_ne!(w.span_time_clause(), inclusive.span_time_clause());
        assert_ne!(w.resources_day_clause(), inclusive.resources_day_clause());
        assert_ne!(w.span_bucket_clause(), inclusive.span_bucket_clause());
    }

    /// The row bound factored on a column name renders the same text for
    /// the old column as it did before the factoring — the refactor's own
    /// guard, so the search path's committed SQL cannot move underneath
    /// it.
    #[test]
    fn the_factored_row_and_day_bounds_still_render_the_original_columns() {
        let w = WindowSql::start_closed_end_open(PREV_MIDNIGHT_NS, MIDNIGHT_NS);
        assert_eq!(w.time_clause(), w.time_clause_on("timestamp_ns"));
        assert_eq!(w.date_clause(), w.date_clause_on("date"));
        assert!(w.time_clause().starts_with("timestamp_ns >= "));
        assert!(w.date_clause().starts_with("date >= toDate('"));
    }

    /// The resource-day bound's one owning text, called with bound NAMES
    /// — the form the two statements that read their own extent use.
    #[test]
    fn the_resource_day_bound_takes_expressions_as_well_as_literals() {
        assert_eq!(
            resources_day_bound("ext_lo", "ext_hi"),
            "day >= toDate(fromUnixTimestamp64Nano(ext_lo), 'UTC') \
             AND day <= toDate(fromUnixTimestamp64Nano(ext_hi), 'UTC')"
        );
    }
}
