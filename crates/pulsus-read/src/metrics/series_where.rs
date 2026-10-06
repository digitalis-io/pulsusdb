//! **LEAF MODULE — the only place a metrics series read's window or a
//! matcher predicate can be rendered from the sanctioned components** (issue #315, review rounds 1–3; the one boundary
//! crossing rustc does NOT police is stated under "What rustc enforces —
//! and what it does not").
//!
//! # Why the renderer lives in a leaf module
//!
//! Issue #315 has to hold an invariant across *every* builder that reads
//! the series tables: a user regex may not be rendered into SQL without the
//! constant compile probe that forces ClickHouse's RE2 to adjudicate it
//! (see [`SeriesWhere`]'s own doc for what the probe is and why it is
//! spliced into the bound). The first cut of that fix satisfied the
//! invariant at all five call sites and left it as a **hand-maintained
//! enumeration**: a sixth builder could call the private predicate
//! renderer, skip the probe, and pass every gate — the coverage test was a
//! list of builders a person had to remember to extend.
//!
//! **Private is a scope, not a restriction, and the scope must be a
//! leaf.** A private item is visible throughout the defining module's
//! subtree, so a private `fn` in `sql.rs` is reachable from every other
//! `fn` in `sql.rs`, and a private constructor in `metrics/mod.rs` is
//! reachable from every descendant — `sql.rs` included, which is how
//! review round 2 found the #240 capability token still constructible
//! from the builders' own file. Everything the hole could be rebuilt from
//! is therefore *written* HERE, in a leaf with no children:
//! `anchored_re2_literal`, `matcher_regex_literal` and `predicate` are
//! module-private with no visibility modifier at all;
//! [`SeriesWhere`]'s field is private; and [`PromqlRe2Fallback`] — the
//! token the `pub(crate)` escaper demands — has a private field and a
//! private `new`, so no other module can present one.
//!
//! **Written here is not the same as reachable only from here**, and the
//! difference is not academic: review round 9 defined a macro in a sibling
//! module, invoked it at associated-item position inside this file's own
//! `impl SeriesWhere`, and had it expand to a `pub(super)` wrapper around
//! the private `predicate`. From `sql.rs` that rendered
//! `match(JSONExtractString(labels, 'job'), '(?-s)^(?:5..)$')` with **no
//! compile probe** — the #315 hole itself — while every test in this file
//! stayed green. Rust's privacy is per-module, and a macro invoked inside
//! the module expands inside it, so the privates above are reachable from
//! any text a member of this module admits.
//!
//! That is why the rule below is stated as an obligation on authors rather
//! than a guarantee from the compiler, and why the seal is completed by
//! #328's extraction into `pulsus-re2` rather than by anything in here.
//!
//! # Boundary inventory
//!
//! Nine declarations carry a visibility modifier, pinned by the in-file
//! census test `the_boundary_inventory_is_pinned` because a
//! hand-maintained inventory here was wrong once (review round 3 found
//! it listing three of the six). That pin reads `pub`-prefixed lines and
//! `impl` headers, not the language's notion of visibility — see the
//! test's own doc for what it cannot see:
//!
//! * [`SeriesWhere`] — the type name (`pub(super)`); its fields stay
//!   private, so the type can be named but not forged. Its derived `Debug`
//!   does print them, which confers nothing beyond what the two renderings
//!   below already hand to the same audience.
//! * [`SeriesWhere::activity`] — consumes the window and the matchers and
//!   renders the window, the probe and the predicates together (issue
//!   #623).
//! * [`SeriesWhere::ids_from_where`] — statement 1's `FROM … WHERE …`,
//!   whole.
//! * [`SeriesWhere::with_labels`] — statement 2, whole. The probe is in
//!   the activity read of both, so it cannot be dropped from a regex.
//! * [`Lookup`] — `activity`'s input (`pub(super)`), with three
//!   `pub(super)` fields: a scope's predicates and the two matcher sets.
//!   It carries no rendered matcher, so it yields nothing renderable.
//! * [`MatcherTarget`] — the column a matcher reads (`pub(super)`); its
//!   variants inherit that visibility and name a column, nothing more.
//! * [`PromqlRe2Fallback`] — the TYPE only (`pub(crate)`, so the
//!   escaper's pinned signature can name it); both ways of *making* one
//!   are private to this file.
//! * [`anchored_re2_literal_for_test`] — the `#[doc(hidden)]` seam, a
//!   plain-`pub` fn returning the anchored pattern literal.
//!
//! # What rustc enforces — and what it does not
//!
//! Measured, by compiling each bypass spelling from `sql.rs` in turn
//! (review round 2; the two token rows were also compiled from `logql`,
//! same diagnostics):
//!
//! | attempted spelling | rustc |
//! |---|---|
//! | `anchored_re2_literal(..)`, `matcher_regex_literal(..)`, `predicate(..)` — unqualified | `E0425` cannot find function in this scope |
//! | `crate::metrics::series_where::predicate(..)` — fully qualified | `E0603` private function |
//! | `SeriesWhere { activity: … }` (struct literal) | `E0451` private field |
//! | `w.activity` (field access) | `E0616` private field |
//! | `PromqlRe2Fallback::new()` — including under a `use … as` alias | `E0624` private associated function |
//! | `PromqlRe2Fallback(())` (tuple constructor) | `E0423` cannot initialize a tuple struct which contains private fields |
//!
//! So what rustc enforces is exactly this much: **in safe Rust**, outside
//! this file, no spelling constructs the #240 token, calls the escaper
//! (its signature demands that token), calls the private predicate
//! helpers, forges [`SeriesWhere`] or names its fields — the *sanctioned
//! components* cannot be recombined into "a regex without its probe".
//!
//! Safe Rust is the boundary that matters here, and it is the only one on
//! offer: privacy is not checked by `unsafe` conversions, so
//! `unsafe { std::mem::zeroed::<PromqlRe2Fallback>() }` builds this
//! unit-field token anywhere in the crate and then satisfies the escaper's
//! signature (review round 4, measured). A workspace-wide
//! `forbid(unsafe_code)` would delete that class outright but is not
//! available — the allocation-ceiling suites install `GlobalAlloc` impls
//! and `pulsus-config`'s test support mutates the environment, both
//! `unsafe` by definition. What remains has the literal seam's standing
//! below: a deliberate, self-announcing act that no ordinary refactor
//! performs and that review reads at a glance, as against the accidental
//! "sixth builder" this seal exists to make impossible.
//!
//! **The literal seam is NOT sealed** (review round 3). Nothing stops
//! production code, at compile time, from calling the plain-`pub`
//! [`anchored_re2_literal_for_test`] and splicing the returned pattern
//! literal into a hand-written `match(...)` conjunct — no token, no
//! probe — which is precisely the #315 hole. The compiler's guarantee
//! ends at the sanctioned components; the seam is covered only by review
//! (a `_for_test` call in a production path is the tell), until issue
//! #328's D1 extracts this screen into the `pulsus-re2` crate, where the
//! differential lives beside the code it tests and the export goes away.
//!
//! **Nothing but the renderer may be added to this file.** Every item
//! declared here can reach those privates and could therefore rebuild the
//! hole — this file is the seal's trust base, and rustc cannot police
//! additions *inside* it. A new builder belongs in `sql.rs`, where the
//! only fragments available to it are a whole rendered read and the
//! seam's bare literal.

use crate::logql::escape::{ch_regex_anchored_promql_re2, ch_string};

use super::matcher::{DataWindow, LabelMatcher, MatchOp};

/// Capability token (issue #240). Possession proves the caller is on the
/// PromQL fallback path, where ClickHouse's RE2 — not the Rust `regex`
/// crate — is the regex authority (`labels.rs:496-506`, `:521-526`).
///
/// SEALING FORM IS LOAD-BEARING — do not "tidy" any line:
///  * the tuple field has NO visibility modifier, so the tuple constructor
///    is callable only inside this leaf module;
///  * `new` has NO visibility modifier, for the same reason;
///  * the TYPE is `pub(crate)` so `crate::logql::escape`'s pinned signature
///    can name it (via `super`'s `pub(crate) use` re-export) — naming it
///    confers nothing, every way of *making* one is private to this file.
///
/// The token lived in `metrics/mod.rs` until issue #315's review round 2,
/// with field and `new` private to `metrics` — which reads sealed but is
/// not: a private item is visible to the defining module's DESCENDANTS,
/// and `sql.rs` is one, so a builder there could construct the token and
/// reach the `pub(crate)` escaper under a `use … as` alias that no textual
/// census would catch (measured: it compiled). Defined HERE, in a leaf
/// with no children that is nobody's ancestor, the same mutant is a
/// compile error — `E0624` on `new`, `E0423` on the tuple constructor —
/// measured from both `sql.rs` and `logql`; the module-doc table carries
/// the full battery.
pub(crate) struct PromqlRe2Fallback(());

impl PromqlRe2Fallback {
    fn new() -> Self {
        PromqlRe2Fallback(())
    }
}

/// Which column a matcher is evaluated against on the lookup table.
///
/// An enum rather than a boolean (workspace rule: no boolean parameters),
/// and matched exhaustively in [`predicate`], so a third target cannot be
/// added without the build failing here — the same "adding a variant
/// breaks the build" property the `MatchOp` arms already carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MatcherTarget {
    /// Ordinary label matchers, read out of the lookup row's canonical JSON
    /// (`metric_labels`, one row per series, issue #623).
    /// `JSONExtractString` returns `''` for a missing key, which is
    /// Prometheus's absent-label rule and matches `super::labels`'
    /// in-process `""` — load-bearing for the cache-vs-SQL differential.
    Labels,
    /// `__name__` matchers, which address the lookup's **`metric_name`
    /// column** — never a stored label (docs/schemas.md §2.1).
    MetricNameColumn,
}

/// What a series read matches on the lookup table (issue #623): the
/// predicates its scope already states (`metric_name = 'up'`, `metric_name
/// IN (…)`, `fingerprint IN (…)`), its `__name__` matchers and its label
/// matchers. Every one of them runs on the lookup; the activity table
/// answers only the window.
#[derive(Debug, Clone, Copy)]
pub(super) struct Lookup<'a> {
    pub(super) scope: &'a [String],
    pub(super) name_matchers: &'a [LabelMatcher],
    pub(super) matchers: &'a [LabelMatcher],
}

/// A series read's window on the activity table and its matchers on the
/// lookup table, rendered together and inseparable (issue #623).
///
/// # The window
///
/// The activity table holds one row per series per UTC day, `hours` the
/// 24-bit mask of the hours the series had samples in. The window bounds
/// `day` by its first and last UTC day, and [`hour_mask`] gives each day
/// the window's hours: the first day from the window's first hour, the
/// last day to its last hour, a day between them whole. Unmerged rows of
/// one day are each tested, so no `FINAL` is needed: a row tests true when
/// any of its hours is in the window.
///
/// # The compile probe (issue #315)
///
/// ClickHouse compiles a `match()` pattern only when it evaluates that
/// `match()` on a row. A selector naming a metric with no rows in the
/// window therefore never reaches RE2 at all, so an RE2-rejected pattern
/// came back as an empty `200` where upstream Prometheus (the metrics API's
/// reference of record, issue #283) answers `400`. The activity read
/// therefore carries one `0 * match('', <pattern>) = 0` line per regex
/// matcher, over a **constant** subject, which ClickHouse folds during
/// query analysis, before a single part is read: a pattern RE2 refuses
/// raises `Code: 427 CANNOT_COMPILE_REGEXP` there, and [`super::dispatch`]
/// classifies it into the same 400 the row predicate would have produced.
/// The patterns themselves run on the lookup rows, inside the activity
/// read's `fingerprint IN` set, so no activity row evaluates a regex.
///
/// A matcher set with no regex renders no probe line at all. Probes are
/// emitted in matcher order — name matchers, then label matchers — so the
/// FIRST invalid pattern is the one reported, matching upstream's own
/// order and issue #316's in-process `first_invalid_regex_detail`.
#[derive(Debug)]
pub(super) struct SeriesWhere {
    /// The activity read's conditions after `WHERE`: the day range, the
    /// probe lines and the hour mask, one per line, unindented.
    activity: Vec<String>,
    /// The lookup read's conditions: the scope, then one predicate per
    /// matcher. Empty when the read has neither.
    lookup: Vec<String>,
}

impl SeriesWhere {
    /// Renders `window` as the activity read's day range and hour mask, the
    /// compile probe for every regex in `lookup`, and `lookup`'s
    /// predicates.
    pub(super) fn activity(window: DataWindow, lookup: Lookup<'_>) -> Self {
        let (first, _) = day_and_hour(window.start_ms);
        let (last, _) = day_and_hour(window.end_ms);
        let mut activity = vec![format!(
            "day BETWEEN '{}' AND '{}'",
            day_text(first),
            day_text(last)
        )];
        for literal in lookup
            .name_matchers
            .iter()
            .chain(lookup.matchers)
            .filter_map(matcher_regex_literal)
        {
            activity.push(format!("0 * match('', {literal}) = 0"));
        }
        activity.push(format!("bitAnd(hours, {}) != 0", hour_mask(window)));
        let lookup_preds = lookup
            .scope
            .iter()
            .cloned()
            .chain(
                lookup
                    .name_matchers
                    .iter()
                    .map(|m| predicate(m, MatcherTarget::MetricNameColumn)),
            )
            .chain(
                lookup
                    .matchers
                    .iter()
                    .map(|m| predicate(m, MatcherTarget::Labels)),
            )
            .collect();
        SeriesWhere {
            activity,
            lookup: lookup_preds,
        }
    }

    /// Statement 1's `FROM … WHERE …`: the activity rows in the window
    /// whose IDs the lookup predicates select. A builder prepends its own
    /// projection.
    pub(super) fn ids_from_where(&self, series_table: &str, labels_table: &str) -> String {
        self.activity_read(series_table, labels_table, "")
    }

    /// Statement 2 whole: each series the lookup predicates select whose
    /// ID the activity read finds in the window, with its own name and
    /// labels, once.
    pub(super) fn with_labels(&self, series_table: &str, labels_table: &str) -> String {
        let mut out = format!(
            "SELECT fingerprint, any(name) AS metric_name, any(label_text) AS labels\n\
             FROM (\n\
             \x20 SELECT fingerprint, metric_name AS name, labels AS label_text\n\
             \x20 FROM {labels_table}\n"
        );
        let mut keyword = "WHERE";
        for pred in &self.lookup {
            out.push_str(&format!("  {keyword} {pred}\n"));
            keyword = "  AND";
        }
        out.push_str(&format!(
            "  {keyword} fingerprint IN (\n\
             \x20     SELECT fingerprint\n\
             {}\n\
             \x20   )\n\
             )\n\
             GROUP BY fingerprint\n\
             ORDER BY metric_name, fingerprint",
            self.activity_read(series_table, labels_table, "      ")
        ));
        out
    }

    /// The activity read, every line indented by `indent`: the window, then
    /// the IDs the lookup predicates select when there are any.
    fn activity_read(&self, series_table: &str, labels_table: &str, indent: &str) -> String {
        let mut lines = vec![format!("{indent}FROM {series_table}")];
        let mut keyword = "WHERE";
        for cond in &self.activity {
            lines.push(format!("{indent}{keyword} {cond}"));
            keyword = "  AND";
        }
        if !self.lookup.is_empty() {
            lines.push(format!("{indent}  AND fingerprint IN ("));
            lines.push(format!("{indent}    SELECT fingerprint"));
            lines.push(format!("{indent}    FROM {labels_table}"));
            let mut keyword = "WHERE";
            for pred in &self.lookup {
                lines.push(format!("{indent}    {keyword} {pred}"));
                keyword = "  AND";
            }
            lines.push(format!("{indent}  )"));
        }
        lines.join("\n")
    }
}

/// A narrow, `#[doc(hidden)]` test seam, re-exported by `super` next to
/// `re2_authority`'s seam, which is its shape and precedent. Not a
/// `cfg(test)` or feature gate, for that precedent's reason: the consumer
/// is an external integration binary (`tests/re2_screen_differential.rs`)
/// that links the lib compiled *without* `cfg(test)`, and a cargo feature
/// would either drop that suite's hermetic half from the plain
/// `cargo test --workspace` lane or ship the seam in the CI-tested
/// configuration anyway. The differential must cross ClickHouse's RE2
/// over the **production** rendering, and nothing else can hand it that
/// rendering — the escaper's token is constructible only in this file.
///
/// This is the module's one UNSEALED boundary crossing (module doc,
/// "what rustc does not enforce"): plain `pub`, so production code can
/// call it and splice the returned literal into a hand-written
/// `match(...)` with no token and no probe. The export exists only
/// because the differential is an external binary; issue #328's D1 moves
/// this screen into the `pulsus-re2` crate, where the differential sits
/// beside the code it tests and this export is retired.
#[doc(hidden)]
pub fn anchored_re2_literal_for_test(pattern: &str) -> String {
    anchored_re2_literal(pattern)
}

/// The metrics path's **only** regex→SQL rendering (issue #240's PromQL
/// exemption, narrowed to one site by issue #315 so the row predicate and
/// the compile probe can never disagree about the pattern text they name).
/// Issue #324's `(?-s)` prefix lives inside the escaper, not here.
fn anchored_re2_literal(pattern: &str) -> String {
    ch_regex_anchored_promql_re2(PromqlRe2Fallback::new(), pattern)
}

/// `Some(literal)` for the two ops whose value is a regex, `None` for the
/// two whose value is a literal string and is never compiled.
fn matcher_regex_literal(m: &LabelMatcher) -> Option<String> {
    match m.op {
        MatchOp::Re | MatchOp::Nre => Some(anchored_re2_literal(&m.value)),
        MatchOp::Eq | MatchOp::Neq => None,
    }
}

/// One matcher, against whichever column `target` names. Label keys are
/// always string literals (see `super::sql`'s escaping table) — never
/// `ch_ident`, which is reserved for trusted schema identifiers.
fn predicate(m: &LabelMatcher, target: MatcherTarget) -> String {
    let column = match target {
        MatcherTarget::Labels => format!("JSONExtractString(labels, {})", ch_string(&m.key)),
        MatcherTarget::MetricNameColumn => "metric_name".to_string(),
    };
    match m.op {
        MatchOp::Eq => format!("{column} = {}", ch_string(&m.value)),
        MatchOp::Neq => format!("{column} != {}", ch_string(&m.value)),
        MatchOp::Re => format!("match({column}, {})", anchored_re2_literal(&m.value)),
        MatchOp::Nre => format!("NOT match({column}, {})", anchored_re2_literal(&m.value)),
    }
}

/// A UTC instant's day, in days since the epoch, and its hour — clamped to
/// the `Date` column's range, so a window reaching past it reads the days
/// the table can hold.
fn day_and_hour(ms: i64) -> (i64, i64) {
    const DAY_MS: i64 = 86_400_000;
    const LAST_DAY: i64 = u16::MAX as i64;
    let day = ms.div_euclid(DAY_MS);
    if day < 0 {
        (0, 0)
    } else if day > LAST_DAY {
        (LAST_DAY, 23)
    } else {
        (day, ms.rem_euclid(DAY_MS) / 3_600_000)
    }
}

/// A day since the epoch as the `YYYY-MM-DD` text a `Date` compares with.
fn day_text(day: i64) -> String {
    chrono::DateTime::from_timestamp(day * 86_400, 0)
        .map(|t| t.format("%Y-%m-%d").to_string())
        .unwrap_or_default()
}

/// Hours `first` to `last` of a day as mask bits.
fn bits(first: i64, last: i64) -> i64 {
    (1 << (last + 1)) - (1 << first)
}

/// The hours of `window`, as each day's mask: `multiIf(day = d0 AND day =
/// d1, bits(h0, h1), day = d0, bits(h0, 23), day = d1, bits(0, h1),
/// 16777215)`. On a window inside one day the first branch decides; across
/// days it cannot hold and is rendered `0`.
fn hour_mask(window: DataWindow) -> String {
    let (d0, h0) = day_and_hour(window.start_ms);
    let (d1, h1) = day_and_hour(window.end_ms);
    let (t0, t1) = (day_text(d0), day_text(d1));
    let same_day = if d0 == d1 { bits(h0, h1) } else { 0 };
    format!(
        "multiIf(day = '{t0}' AND day = '{t1}', {same_day}, day = '{t0}', {}, \
         day = '{t1}', {}, 16777215)",
        bits(h0, 23),
        bits(0, h1)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The §4 window of the design: 2026-09-07 22:30 to 23:30 UTC.
    fn evening() -> DataWindow {
        DataWindow {
            start_ms: 1_788_820_200_000,
            end_ms: 1_788_823_800_000,
        }
    }

    /// **K1 (issue #623): every name and label matcher runs on the
    /// lookup.** Each row of the design's §2 table, rendered; and a selector
    /// with a name regex and a label matcher puts both in the lookup
    /// sub-query, the regex's probe on its own line ahead of the hour mask.
    #[test]
    fn every_matcher_renders_its_lookup_predicate() {
        let name = |op, v| predicate(&m(op, "__name__", v), MatcherTarget::MetricNameColumn);
        assert_eq!(name(MatchOp::Eq, "m"), "metric_name = 'm'");
        assert_eq!(name(MatchOp::Neq, "m"), "metric_name != 'm'");
        assert_eq!(
            name(MatchOp::Re, "re"),
            "match(metric_name, '(?-s)^(?:re)$')"
        );
        assert_eq!(
            name(MatchOp::Nre, "re"),
            "NOT match(metric_name, '(?-s)^(?:re)$')"
        );
        let label = |op, v| predicate(&m(op, "k", v), LABELS);
        assert_eq!(
            label(MatchOp::Eq, "v"),
            "JSONExtractString(labels, 'k') = 'v'"
        );
        assert_eq!(
            label(MatchOp::Neq, "v"),
            "JSONExtractString(labels, 'k') != 'v'"
        );
        assert_eq!(
            label(MatchOp::Re, "re"),
            "match(JSONExtractString(labels, 'k'), '(?-s)^(?:re)$')"
        );
        assert_eq!(
            label(MatchOp::Nre, "re"),
            "NOT match(JSONExtractString(labels, 'k'), '(?-s)^(?:re)$')"
        );

        let names = [m(MatchOp::Re, "__name__", "up|down")];
        let labels = [m(MatchOp::Eq, "job", "api")];
        let w = SeriesWhere::activity(
            evening(),
            Lookup {
                scope: &[],
                name_matchers: &names,
                matchers: &labels,
            },
        );
        assert_eq!(
            w.ids_from_where("metric_series", "metric_labels"),
            "FROM metric_series\n\
             WHERE day BETWEEN '2026-09-07' AND '2026-09-07'\n\
             \x20 AND 0 * match('', '(?-s)^(?:up|down)$') = 0\n\
             \x20 AND bitAnd(hours, multiIf(day = '2026-09-07' AND day = '2026-09-07', 12582912, day = '2026-09-07', 12582912, day = '2026-09-07', 16777215, 16777215)) != 0\n\
             \x20 AND fingerprint IN (\n\
             \x20   SELECT fingerprint\n\
             \x20   FROM metric_labels\n\
             \x20   WHERE match(metric_name, '(?-s)^(?:up|down)$')\n\
             \x20     AND JSONExtractString(labels, 'job') = 'api'\n\
             \x20 )"
        );
    }

    /// **A1 (issue #623): the hour mask.** The window's UTC days bound the
    /// `day` column, and the mask gives each day the window's hours: one
    /// hour; the design's 22:30-23:30; across midnight, 23:00 on the first
    /// day and 00:00 on the next; and three days, the middle one whole.
    #[test]
    fn the_hour_mask_covers_exactly_the_windows_hours() {
        let window = |start_ms, end_ms| DataWindow { start_ms, end_ms };
        assert_eq!(
            hour_mask(window(1_788_819_000_000, 1_788_821_400_000)),
            "multiIf(day = '2026-09-07' AND day = '2026-09-07', 4194304, \
             day = '2026-09-07', 12582912, day = '2026-09-07', 8388607, 16777215)"
        );
        assert_eq!(
            hour_mask(evening()),
            "multiIf(day = '2026-09-07' AND day = '2026-09-07', 12582912, \
             day = '2026-09-07', 12582912, day = '2026-09-07', 16777215, 16777215)"
        );
        let midnight = hour_mask(window(1_788_823_800_000, 1_788_827_400_000));
        assert!(
            midnight.contains("day = '2026-09-07', 8388608, day = '2026-09-08', 1, 16777215)"),
            "{midnight}"
        );
        let three = hour_mask(window(1_788_820_200_000, 1_788_915_600_000));
        assert!(
            three.contains("day = '2026-09-07', 12582912, day = '2026-09-09', 3, 16777215)"),
            "the middle day takes every hour: {three}"
        );
    }

    fn window() -> DataWindow {
        DataWindow {
            start_ms: 1_000,
            end_ms: 3_600_001,
        }
    }

    fn m(op: MatchOp, key: &str, value: &str) -> LabelMatcher {
        LabelMatcher {
            key: key.to_string(),
            op,
            value: value.to_string(),
        }
    }

    /// The label column every `Labels` case below renders against.
    const LABELS: MatcherTarget = MatcherTarget::Labels;

    /// Statement 1's text for `matchers` over [`window`], unscoped: name
    /// matchers when they are `__name__`'s, label matchers otherwise.
    fn ids(matchers: &[LabelMatcher]) -> String {
        let (names, labels): (Vec<LabelMatcher>, Vec<LabelMatcher>) =
            matchers.iter().cloned().partition(|m| m.key == "__name__");
        SeriesWhere::activity(
            window(),
            Lookup {
                scope: &[],
                name_matchers: &names,
                matchers: &labels,
            },
        )
        .ids_from_where("metric_series", "metric_labels")
    }

    /// The module-doc boundary inventory, kept honest mechanically
    /// (review round 3: the hand-maintained list was wrong once — it
    /// said three items where six are visible). The production half of
    /// this file must contain exactly these visibility-carrying
    /// declarations, in this order; a new `pub` item, a widened
    /// visibility, or a changed signature fails here until both this pin
    /// and the module-doc inventory are updated.
    ///
    /// # What this establishes, and what it provably cannot
    ///
    /// **It establishes exactly one thing: the declarations written in
    /// this file match the module-doc inventory.** That is worth having —
    /// it is the drift this test was added for, and it caught the
    /// inventory listing three items when six were visible.
    ///
    /// **It does NOT establish that nothing else reaches `tail`, and no
    /// version of it can.** Reviews 5–8 each defeated a stricter textual
    /// form, and the last two did so with code whose text is not in this
    /// file at all:
    ///
    ///  * round 5 — a generic `impl<'a> IntoIterator for &'a SeriesWhere`
    ///    slipped an `impl `-with-a-space filter;
    ///  * round 6 — `#[rustfmt::skip]` on a declaration's own line is a
    ///    rustfmt fixpoint, so the declaration never starts a line;
    ///  * round 7 — a `macro_rules!` defined in a sibling and invoked at
    ///    item position here expanded to a leaking `impl`;
    ///  * round 8 — an indented `leak_method!()` inside the existing
    ///    `impl` block, and, in one word on an existing line,
    ///    `#[derive(Debug, serde::Serialize)]`, which was *executed* to
    ///    print `{"tail":"unix_milli >= 0 AND …"}` from a sibling module.
    ///
    /// The pattern is not that each guard was too loose. It is that the
    /// property wanted — *no declaration reaches `tail` except the listed
    /// ones* — is about expansion and reachability, and a check that reads
    /// this file's characters is downstream of neither. Escalating the
    /// textual form buys one spelling per round and keeps meeting the same
    /// wall, so the escalation stops here rather than at the next round.
    ///
    /// **What actually closes it** is making the field unreachable rather
    /// than merely unlisted, which is issue #328's D1 extraction into
    /// `pulsus-re2` — there the differential lives beside the code and the
    /// `_for_test` seam that motivates all of this goes away. Until then
    /// the compile-error table below is the real guarantee (rustc, not
    /// text), and this test is a drift alarm on the written inventory.
    ///
    /// The checks kept below are the cheap ones that demonstrably catch
    /// drift; they are not claimed to be exhaustive over anything.
    #[test]
    fn the_boundary_inventory_is_pinned() {
        let text = include_str!("series_where.rs");
        let production = &text[..text.find("mod tests {").unwrap()];
        // Item-position shape: top-level lines must be plain items or
        // attributes and carry no `!`, which keeps `macro_rules!`, an
        // item-position invocation, `include!` and `mod` out of the file.
        // Kept because it is cheap and does catch those; NOT relied on —
        // an indented invocation inside an existing `impl`, and a derive
        // added to an existing attribute, both pass it (round 8). See the
        // doc above for why no textual form closes the class.
        const ITEM_STARTS: [&str; 5] = ["use ", "pub ", "pub(", "impl ", "fn "];
        for line in production.lines() {
            if line.is_empty() || line.starts_with(' ') || line.starts_with("//") || line == "}" {
                continue;
            }
            let ok = line.starts_with("#[") || ITEM_STARTS.iter().any(|s| line.starts_with(s));
            assert!(
                ok && !line.contains('!'),
                "unexpected item-position line in this leaf: {line:?}\n\
                 Only plain `use`/`pub`/`impl`/`fn` items and `#[...]` \
                 attributes may appear at top level, and none may carry \
                 `!`. Text-importing forms (`macro_rules!`, a macro \
                 invocation, `include!`, `mod`) would put boundary-crossing \
                 code outside this file, where this test cannot see it."
            );
        }
        assert!(
            !production.contains("rustfmt::skip"),
            "`#[rustfmt::skip]` in the production half defeats this pin's \
             line-prefix matching; it is not permitted in this leaf"
        );
        let declared = |prefix: &'static str| -> Vec<&str> {
            production
                .lines()
                .map(str::trim_start)
                .filter(|line| line.starts_with(prefix))
                .collect()
        };
        assert_eq!(
            declared("pub"),
            [
                "pub(crate) struct PromqlRe2Fallback(());",
                "pub(super) enum MatcherTarget {",
                "pub(super) struct Lookup<'a> {",
                "pub(super) scope: &'a [String],",
                "pub(super) name_matchers: &'a [LabelMatcher],",
                "pub(super) matchers: &'a [LabelMatcher],",
                "pub(super) struct SeriesWhere {",
                "pub(super) fn activity(window: DataWindow, lookup: Lookup<'_>) -> Self {",
                "pub(super) fn ids_from_where(&self, series_table: &str, labels_table: &str) -> String {",
                "pub(super) fn with_labels(&self, series_table: &str, labels_table: &str) -> String {",
                "pub fn anchored_re2_literal_for_test(pattern: &str) -> String {",
            ],
            "series_where.rs declarations drifted from this test's list"
        );
        // Round 9: the assertion above compares declarations against a list
        // typed HERE, so editing the module doc alone left it green — the
        // failure message named a comparison it did not make, and the
        // unguarded direction was exactly the round-3 doc drift this test
        // exists for. Read the doc's inventory bullets and compare for real.
        // Both anchors are asserted present. The start anchor already failed
        // closed (a missing section yields an empty list, which cannot equal
        // the array below), but round 10 found the END anchor unverified:
        // renaming it let the scan run to EOF. That could not produce a wrong
        // answer — the names still came back correct and in order — so this
        // is one line closing an asymmetry, not a demonstrated defect.
        const DOC_START: &str = "//! # Boundary inventory";
        const DOC_END: &str = "//! # What rustc enforces";
        // Matched as a LINE PREFIX, not with `contains`: these constants are
        // themselves lines of this file, so `text.contains(DOC_END)` is true
        // of the declaration above and can never fail — a check satisfied by
        // its own definition. The const lines are indented, so a prefix match
        // sees only the real doc headings.
        let anchors = |a: &str| text.lines().filter(|l| l.starts_with(a)).count();
        assert!(
            anchors(DOC_START) == 1 && anchors(DOC_END) == 1,
            "the module doc's inventory section anchors moved; this parse \
             reads between {DOC_START:?} and {DOC_END:?} and cannot bound \
             the section without exactly one of each"
        );
        let doc_names: Vec<&str> = text
            .lines()
            .skip_while(|l| !l.starts_with(DOC_START))
            .take_while(|l| !l.starts_with(DOC_END))
            .filter_map(|l| l.trim_start().strip_prefix("//! * [`"))
            .filter_map(|l| l.split("`]").next())
            .collect();
        assert_eq!(
            doc_names,
            [
                "SeriesWhere",
                "SeriesWhere::activity",
                "SeriesWhere::ids_from_where",
                "SeriesWhere::with_labels",
                "Lookup",
                "MatcherTarget",
                "PromqlRe2Fallback",
                "anchored_re2_literal_for_test",
            ],
            "the module-doc boundary inventory drifted from this test's list"
        );
        let impl_headers: Vec<&str> = production
            .lines()
            .map(str::trim_start)
            .filter(|line| line.starts_with("impl") || line.starts_with("unsafe impl"))
            .collect();
        assert_eq!(
            impl_headers,
            ["impl PromqlRe2Fallback {", "impl SeriesWhere {"],
            "a new impl block can expose private state without declaring `pub`"
        );
    }

    /// What this test establishes, no more: for statement 1 rendered by
    /// [`SeriesWhere::activity`] — each `MatchOp` × each target with one
    /// matcher, at fixed representative values — a `match(` predicate on
    /// the lookup appears iff its compile probe line does, and a regex-free
    /// set renders no probe. It does NOT establish that `activity` is the
    /// only source of a predicate (module doc, "what rustc does not
    /// enforce"). Fixed values rather than a generator: the pairing is
    /// decided by `MatchOp` alone, and the injection tests in `super::sql`
    /// cover hostile values.
    #[test]
    fn a_rendered_regex_and_its_compile_probe_are_inseparable() {
        for key in ["job", "__name__"] {
            for op in [MatchOp::Re, MatchOp::Nre] {
                let rendered = ids(&[m(op, key, "5..")]);
                assert!(rendered.contains("match("), "{op:?}/{key}: {rendered}");
                assert!(
                    rendered.contains("\n  AND 0 * match('', '(?-s)^(?:5..)$') = 0\n"),
                    "{op:?}/{key} rendered a regex with no probe: {rendered}"
                );
            }
            for op in [MatchOp::Eq, MatchOp::Neq] {
                let rendered = ids(&[m(op, key, "api")]);
                assert!(!rendered.contains("match("), "{op:?}/{key}: {rendered}");
            }
        }
        // No matcher: the window alone.
        assert_eq!(
            ids(&[]),
            "FROM metric_series\n\
             WHERE day BETWEEN '1970-01-01' AND '1970-01-01'\n\
             \x20 AND bitAnd(hours, multiIf(day = '1970-01-01' AND day = '1970-01-01', 3, \
             day = '1970-01-01', 16777215, day = '1970-01-01', 3, 16777215)) != 0"
        );
    }

    /// Probes are emitted in matcher order, name matchers first (ClickHouse
    /// folds them in order, so the first invalid pattern is the one
    /// reported), and literal-valued matchers contribute none.
    #[test]
    fn probes_follow_matcher_order_and_skip_literal_matchers() {
        let rendered = ids(&[
            m(MatchOp::Re, "status", "5.."),
            m(MatchOp::Eq, "job", "api"),
            m(MatchOp::Nre, "env", "dev"),
            m(MatchOp::Re, "__name__", "up.*"),
        ]);
        let probes: Vec<&str> = rendered
            .lines()
            .filter(|l| l.starts_with("  AND 0 * match('', "))
            .collect();
        assert_eq!(
            probes,
            [
                "  AND 0 * match('', '(?-s)^(?:up.*)$') = 0",
                "  AND 0 * match('', '(?-s)^(?:5..)$') = 0",
                "  AND 0 * match('', '(?-s)^(?:dev)$') = 0",
            ],
            "{rendered}"
        );
    }

    /// The two targets address different columns; `__name__` matchers are
    /// never read out of the JSON blob (`__name__` is not a stored label,
    /// docs/schemas.md §2.1).
    #[test]
    fn the_matcher_target_selects_the_column() {
        let labels = predicate(&m(MatchOp::Re, "job", "api"), LABELS);
        assert_eq!(
            labels,
            "match(JSONExtractString(labels, 'job'), '(?-s)^(?:api)$')"
        );
        let names = predicate(
            &m(MatchOp::Re, "__name__", "up.*"),
            MatcherTarget::MetricNameColumn,
        );
        assert_eq!(names, "match(metric_name, '(?-s)^(?:up.*)$')");
    }

    /// The scope's predicates lead the lookup's, and a read with neither
    /// scope nor matcher reads the window alone in statement 2.
    #[test]
    fn the_scope_leads_the_lookup_predicates() {
        let scope = ["metric_name = 'up'".to_string()];
        let labels = [m(MatchOp::Eq, "job", "api")];
        let w = SeriesWhere::activity(
            window(),
            Lookup {
                scope: &scope,
                name_matchers: &[],
                matchers: &labels,
            },
        );
        assert!(
            w.ids_from_where("s", "l").ends_with(
                "    WHERE metric_name = 'up'\n      AND JSONExtractString(labels, 'job') = 'api'\n  )"
            ),
            "{}",
            w.ids_from_where("s", "l")
        );
        let none = SeriesWhere::activity(
            window(),
            Lookup {
                scope: &[],
                name_matchers: &[],
                matchers: &[],
            },
        );
        assert!(
            none.with_labels("s", "l").contains(
                "  FROM l\n  WHERE fingerprint IN (\n      SELECT fingerprint\n      FROM s\n"
            ),
            "{}",
            none.with_labels("s", "l")
        );
    }
}
