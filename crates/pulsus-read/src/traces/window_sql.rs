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
//! | caller | convention | why |
//! |---|---|---|
//! | [`super::search_sql`] | `StartOpenEndClosed` | docs/schemas.md §4.2 search bound |
//! | [`super::graph_sql`] | `StartClosedEndOpen` | docs/api.md §4.5 `[start, end)` |
//! | [`super::metrics_sql`] evaluation window | `StartClosedEndOpen` | `metrics_plan`'s snapped `[start, end)` |
//! | [`super::metrics_sql`] `compare()` selection window | `StartOpenEndClosed` | the reference's `spanStartTime > start && spanStartTime <= end` (Tempo `pkg/traceql/engine_metrics_compare.go:98-110` @ v3.0.2) |
//!
//! [`super::tags_sql::DaySpan`] is deliberately NOT expressed here. The
//! tag-discovery reads carry a day bound and no `timestamp_ns` bound at
//! all (a sub-day predicate prunes nothing on `trace_spans` and defeats
//! the `span_name_day` projection — see that type's own doc), so they
//! have no row bound for a day bound to agree with, and resolving their
//! window inclusively at both ends is the deliberately wider read.

use super::search_sql::date_literal;

const NS_PER_DAY: i64 = 86_400_000_000_000;

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

    /// The row-level bound on `timestamp_ns`, operators chosen by the
    /// convention.
    pub fn time_clause(self) -> String {
        let (lo, hi) = match self.bounds {
            WindowBounds::StartOpenEndClosed => (">", "<="),
            WindowBounds::StartClosedEndOpen => (">=", "<"),
        };
        format!(
            "timestamp_ns {lo} {} AND timestamp_ns {hi} {}",
            self.start_ns, self.end_ns
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
        let start_days = self.start_ns.div_euclid(NS_PER_DAY);
        let end_days = self.last_included_ns().div_euclid(NS_PER_DAY);
        format!(
            "date >= {} AND date <= {}",
            date_literal(start_days),
            date_literal(end_days)
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
}
