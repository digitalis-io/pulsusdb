//! **LEAF MODULE — the only place LogQL can render a ClickHouse `match(…)`
//! predicate, a ClickHouse string literal for a `logql::sql` builder, or a
//! month partition literal** (issue #286).
//!
//! # The guarantee
//!
//! > For any [`CheckedFragment`] value, every `match(<column>, <literal>)`
//! > occurrence in its text was rendered inside `logql::predicate`, from a
//! > literal produced by [`super::escape::ch_regex_anchored_checked`] or
//! > [`super::escape::ch_regex_unanchored_checked`] — i.e. the pattern was
//! > compiled, in the exact form emitted (modulo #331's proven
//! > compilability- and semantics-preserving transform,
//! > `escape.rs:146-158`), before the fragment existed — or from
//! > [`UUID_RE`], a module constant no caller can influence.
//!
//! That is the whole claim. It is deliberately narrower than "this fragment
//! is safe":
//!
//! - it says **nothing** about injection — that is `ch_string`'s property,
//!   separately gated by `tests/injection.rs`;
//! - it says **nothing** about the fragment being well-formed SQL or
//!   semantically right;
//! - it says **nothing** about values that never become a
//!   [`CheckedFragment`] (table names — see "Residual: table names" below);
//! - it is a property of the **value**, not of the query: a fragment minted
//!   for one request and cached could be reused for another. Nothing in
//!   scope does that.
//!
//! The consequence the guarantee buys is exactly #240's: an uncompilable
//! user regex is a `400` at plan time, never a ClickHouse `500` mid-query
//! (`escape.rs:150-155`).
//!
//! [`CheckedLiteral`] carries a second, deliberately different property:
//!
//! > Every value that reaches a `services` or `key_literal` parameter of
//! > `logql::sql` was produced by [`super::escape::ch_string`]. This is
//! > enforced by rustc — the field carries no visibility modifier, the leaf
//! > is the only module that can build one, and the single mint *is* the
//! > escaper — not observed by searching for producers.
//!
//! A **literal is a value**, a **fragment is a boolean expression**; they
//! are not interchangeable and the type system says so. The `CheckedLiteral`
//! property likewise says nothing about the value being safe, well-formed or
//! sensible in the position it lands in — only where the text came from.
//! (Issue #286 plan v4 §D6 **withdrew**, rather than narrowed, the earlier
//! producer-table argument for these two parameters: its subject was a set
//! and its evidence a subset.)
//!
//! [`MonthLiteral`] carries the strongest of the three, and by the cheapest
//! mechanism: [`month_literal`] takes **integers**, so there is no path from
//! a `String` to a `MonthLiteral` at all.
//!
//! # Why a leaf module
//!
//! **Private is a scope, not a restriction, and the scope must be a leaf.**
//! A private item is visible throughout the defining module's subtree, so a
//! private constructor in a parent module is reachable from every
//! descendant. This file is therefore flat — it declares no `mod` item other
//! than its trailing `#[cfg(test)] mod tests` — and each newtype's `sql`
//! field carries **no visibility modifier at all**. That is the
//! `metrics/series_where.rs` posture (`series_where.rs:19-31`) and its
//! reason.
//!
//! Because every mint here validates (a mint that emits `match(` takes its
//! pattern only through the `_checked` escapers; the one constant-pattern
//! mint takes no argument, and [`month_literal`] takes no string), this
//! design needs **no `_for_test` escape hatch** — unlike `series_where.rs`,
//! whose `anchored_re2_literal_for_test` is its one documented unsealed
//! crossing (`series_where.rs:113-119`).
//!
//! # What rustc enforces — and what it does not
//!
//! Measured by compiling each bypass spelling from another module in turn
//! (issue #286; the transcript is on the issue):
//!
//! | attempted spelling | rustc |
//! |---|---|
//! | `CheckedFragment { sql: … }` (struct literal, another module) | `E0451` field `sql` of struct `CheckedFragment` is private |
//! | `CheckedLiteral { sql: … }` (struct literal, another module) | `E0451` field `sql` of struct `CheckedLiteral` is private |
//! | `MonthLiteral { sql: … }` (struct literal, another module) | `E0451` field `sql` of struct `MonthLiteral` is private |
//! | `f.sql` (field access, another module) | `E0616` field `sql` of struct `CheckedLiteral` is private |
//! | a bare `String` passed to a retyped `logql::sql` parameter | `E0308` mismatched types: expected `CheckedFragment`, found `String` |
//! | `MetricSource { table: …, shape: … }` (another module) | `E0451` field `table` of struct `MetricSource` is private |
//! | `MetricSource { table: …, bucket_col: …, agg_expr: … }` — the pre-#286 bypass | `E0560` struct `MetricSource<'_>` has no field named `bucket_col` (and `agg_expr`); rustc aborts on these BEFORE the privacy check on `table`, so the code here is **not** `E0451` |
//!
//! ## What a `compile_fail` fence does and does not establish
//!
//! **It does not check its error code** (measured on this toolchain, issue
//! #286): rustdoc requires the snippet to fail, but `compile_fail,E0999` — a
//! code that does not exist — passes. Every code above and on the doctests
//! below is documentation of a measurement, not a gate.
//!
//! **So a fence is only worth what its REMOVAL TEST is worth**, and the
//! removal test is the one this issue's review round 1 caught being skipped:
//! `CheckedFragment`'s only fence used to call `sql::stage3` with a bare
//! `String`, which fails on the parameter TYPE and stays red with the seal
//! deleted. It was a real check of a real property — but not of the seal,
//! which is this module's entire mechanism. Every fence here now names ONE
//! property and has been watched go green when exactly that property is
//! removed:
//!
//! | fence | property it gates | removal that turns it GREEN — every row RUN, not reasoned |
//! |---|---|---|
//! | [`CheckedFragment`] #1 | the seal on `CheckedFragment::sql` | `pub sql: String` |
//! | [`CheckedFragment`] #2 | the builder parameter's TYPE | **none in isolation** — see below |
//! | [`CheckedLiteral`] | the seal on `CheckedLiteral::sql` | `pub sql: String` |
//! | [`MonthLiteral`] | the seal on `MonthLiteral::sql` | `pub sql: String` |
//! | [`super::sql::MetricSource::new`] #1 | the pre-#286 three-string `MetricSource` is gone | restoring that WHOLE struct shape — see below |
//! | [`super::sql::MetricSource::new`] #2 | the privacy of `MetricSource`'s fields | `pub table` + `pub shape` |
//!
//! Two rows are honest about being **over-determined**, because measuring
//! them is how that was discovered rather than assumed:
//!
//! - [`CheckedFragment`] #2 fails on the parameter type and would keep
//!   failing with the seal deleted. It is entailed by #1 — no seal, no
//!   distinct type, no `E0308` — so it documents the user-visible
//!   consequence and #1 is the gate. This is the exact shape review round 1
//!   caught: a fence whose failure had nothing to do with the property it
//!   was cited for.
//! - [`super::sql::MetricSource::new`] #1 does **not** go green when just
//!   `bucket_col`/`agg_expr` are re-added as `pub` fields (measured: it then
//!   fails on the missing/private `shape` instead). Only restoring the
//!   entire pre-#286 `{ pub table, pub bucket_col, pub agg_expr }` shape
//!   turns it green, so that — and not "either field" — is what it gates.
//!
//! Each failing fence is also paired with a **compiling twin** on the same
//! skeleton, differing only in the line that exercises the property, so a
//! typo cannot make the pair pass for the wrong reason.
//!
//! **What the mechanism does NOT close** — stated plainly, because "the
//! compiler closes this" is always true of a narrower set than it sounds:
//!
//! 1. **Adding a mint inside this file.** The file is the trust base; rustc
//!    cannot police additions to it. `tests/logqltest_provenance.rs`' check H
//!    converts that into a failing test — a review event, not a compile
//!    error.
//! 2. **A macro invoked inside this file expanding to a mint.**
//!    **Residual 1 — a sibling macro invoked here.** The census bans
//!    `macro_rules!` and `include!` *declared in this file*. It does **not**
//!    cover `foo!()`, an invocation of a macro defined elsewhere that
//!    expands to a mint inside this module: Rust's privacy is per-module and
//!    a macro invoked here expands here, so the expansion reaches the
//!    private field. **Compiled and confirmed** (issue #286 plan review
//!    round 2). This is a known limit of a text census, not an oversight,
//!    and it is the same residual `metrics/series_where.rs:34-42` records
//!    from #315's round 9.
//! 3. **`unsafe`.** `std::mem::transmute::<String, CheckedFragment>(s)`
//!    compiles anywhere in the crate. A workspace `forbid(unsafe_code)` is
//!    unavailable for the reasons recorded at `series_where.rs:100-111`
//!    (allocation-ceiling suites install `GlobalAlloc`; `pulsus-config` test
//!    support mutates the environment).
//! 4. **The unwrap points.** There is one per type: `as_sql(&self) -> &str`.
//!    They are read-only and confer no minting power, but their text can be
//!    spliced into a hand-built `String` and passed through any `logql::sql`
//!    parameter still typed `&str`. **After this change that set is,
//!    exhaustively:** the table names — `streams_idx_table`,
//!    `streams_table`, `samples_table`, `rollup_table`, `patterns_table`,
//!    `MetricSource::table`. `services`/`key_literal` (now
//!    [`CheckedLiteral`]), `months` (now [`MonthLiteral`]) and
//!    `MetricSource::{bucket_col, agg_expr}` (now
//!    [`super::sql::MetricShape`]) **were** on this list and have left it. A
//!    type-enforced dataflow property is closed at the type boundary and
//!    open wherever it is unwrapped, and this is the list.
//! 5. **Any SQL path that does not go through `logql::sql`.** `traces/` and
//!    `metrics/` build and issue their own statements; a future module that
//!    formats a whole `SELECT` itself is outside this type's reach entirely.
//!    Check G's inventory is the only instrument that spans them, and an
//!    inventory is a drift detector, not a proof.
//!
//! Two further limits of check H's census, recorded beside its domain: a
//! mint whose signature spells none of the three newtype names and no `Self`
//! still fails the **table** (the entry is new) but not the count, so only
//! the table half is load-bearing for that shape; and the scanner is
//! line/brace-oriented rather than a Rust parser, so a signature it
//! mis-joins produces a mismatch — a loud failure, never a silent pass.
//!
//! # Who can reach this module at all
//!
//! Established by `cargo metadata --no-deps`' kind-tagged reverse lookup
//! (issue #286 plan v6 §D14), not by reading manifests by eye: the complete
//! set of packages that can name `pulsus_read::logql::sql` is **`pulsus-read`,
//! `pulsus-server`, `xtask`, `pulsus-e2e`**. `pulsus-e2e` reaches it through
//! a **`[dev-dependencies]`** entry, so only its *test* targets can — a
//! `cargo check` without `--all-targets` prints zero for it and means
//! nothing.
//!
//! # The sibling languages — measured, with no mechanism behind it
//!
//! TraceQL's production `match(` renderings all route through
//! `anchored_regex_sql` → `ch_regex_anchored_checked`
//! (`traces/filter.rs:701-728`, `:1195-1199`); PromQL's sit inside #315's
//! sealed leaf (`metrics/series_where.rs:320-340`). That is a **measurement
//! taken today, with no mechanism keeping it true** — `ch_string` is `pub`,
//! so either file could acquire the same unanchored bypass this issue exists
//! to close for LogQL. Check G's inventory detects the drift for spellings
//! it can see; that is the whole of the claim, and the inventory is
//! explicitly **not** a gate.
//!
//! # Residual: table names
//!
//! `logql::sql`'s six table-name parameters (`streams_idx_table`,
//! `streams_table`, `samples_table`, `rollup_table`, `patterns_table`,
//! [`super::sql::MetricSource`]'s `table`) stay `&str`. **No enforced
//! property covers them.** Their production producers are `PlanCtx`/config
//! fields (`params.rs:61-81`) — which is *an observation of the same kind
//! issue #286's review round 2 refused for `services`*, and it is recorded
//! here as an observation, not as a ground.
//!
//! Both mechanisms that would produce an enforced property were measured and
//! declined (ruling v6), and are recorded so the next reader does not
//! re-derive them:
//!
//! - [`super::escape::ch_ident`] — backtick-quoting **changes the emitted
//!   bytes** (`FROM log_samples` becomes ``FROM `log_samples` ``), so it
//!   breaks every SQL snapshot, `explain_indexes.rs`' `index_usage`
//!   expectations and `query_log_gates.rs`' granule expectations. A wire
//!   change bought to gain a type-system property is the wrong direction.
//! - Sealing `PlanCtx`'s `pub` fields so a `TableRef` could only be minted
//!   from its accessors — genuinely the right long-term shape, and a
//!   separate change with its own census.
//!
//! A `table_ref(&str)` newtype was compiled and rejected: it accepts any
//! `&str` and therefore **enforces nothing**. It is a label, not a property,
//! and a `Checked`-shaped name on an unchecked value is worse than an honest
//! `&str`, because the next reader would trust it.

use pulsus_logql::{CompareOp, LineFilter, LineFilterOp, MatchOp, ParserStage};

use super::escape::ch_like_contains;
use super::escape::{ch_regex_anchored_checked, ch_regex_unanchored_checked, ch_string};
use super::pipeline::PipelineError;

/// The four textual forms Go's `uuid.Parse` accepts (issue #170,
/// `/detected_labels`' ID-likeness reference, grafana/loki:3.4.2
/// `containsAllIDTypes`): plain hyphenated 8-4-4-4-12 (optionally
/// `urn:uuid:`-prefixed), `{hyphenated}` (both braces required), and bare
/// 32-hex. Case-insensitive (`(?i)`), fully anchored — rendered through
/// [`super::escape::ch_string`] into [`non_id_values_expr`]'s `match(val,
/// ...)` predicate, the single implementation (SQL only, no Rust twin to
/// drift). A module constant: no caller can influence it, which is why
/// [`non_id_values_expr`] takes no argument.
const UUID_RE: &str = r"(?i)^(?:(?:urn:uuid:)?[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|\{[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\}|[0-9a-f]{32})$";

/// A ClickHouse SQL boolean fragment (or aggregate expression) rendered from
/// user-supplied values through the validated escapers. See the module doc
/// for the guarantee this carries and the five things it does not close.
///
/// # THE SEAL — this fence is the gate, and it is sensitive to the seal
///
/// The bypass this type exists to make impossible: an unvalidated,
/// uncompilable pattern wrapped **by a struct literal**. Deleting the seal —
/// putting `pub` on the `sql` field below — makes this snippet compile and
/// this doctest FAIL, which is the only reason to believe it tests anything
/// (measured, issue #286 review round 1; the annotated error code pins
/// nothing, see the module doc).
///
/// ```compile_fail,E0451
/// use pulsus_read::logql::{escape, predicate};
///
/// let sql = format!("match(body, {})", escape::ch_string("("));
/// let forged = predicate::CheckedFragment { sql };
/// assert!(forged.as_sql().starts_with("match(body, "));
/// ```
///
/// The compiling twin shares that skeleton — same imports, same
/// `format!`-built `sql`, same read-back assertion — and differs in the one
/// line that matters: the value comes from the sanctioned mint instead of
/// from a struct literal. So a typo that made the snippet above fail for
/// some unrelated reason breaks this one.
///
/// ```
/// use pulsus_logql::{CompareOp, LineFilter, LineFilterOp, MatchOp, ParserStage};
/// use pulsus_read::logql::{escape, predicate};
///
/// let sql = format!("match(body, {})", escape::ch_string("boom"));
/// let forged = predicate::line_filter(&LineFilter {
///     op: LineFilterOp::Regex,
///     value: "boom".into(),
///     value_is_ip: false,
///     or_matches: Vec::new(),
/// })
/// .expect("`boom` compiles");
/// assert!(forged.as_sql().ends_with(&sql));
/// ```
///
/// # The consequence at the builder — an illustration, not the gate
///
/// The seal is why a bare `String` cannot reach a `logql::sql` parameter.
/// This fence shows that consequence; it is **entailed by** the fence above
/// rather than independent of it, and on its own it would stay red with the
/// seal removed (it fails on the parameter TYPE — the diagnostic is
/// `expected `CheckedFragment`, found `String``, captured verbatim in the
/// issue's notes). Read it as documentation of the user-visible effect.
///
/// ```compile_fail,E0308
/// use pulsus_read::logql::{escape, params::Direction, sql};
///
/// let bypass = format!("match(body, {})", escape::ch_string("("));
/// let _ = sql::stage3(
///     "log_samples",
///     &[pulsus_read::logql::predicate::literal("checkout")],
///     &[1u64],
///     sql::TimeWindow { start_ns: 0, end_ns: 1 },
///     &[bypass],
///     Direction::Backward,
///     10,
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckedFragment {
    /// NO visibility modifier — this is the seal. See the module doc.
    sql: String,
}

impl CheckedFragment {
    /// The rendered text. Read-only; confers no minting power.
    pub fn as_sql(&self) -> &str {
        &self.sql
    }
}

/// A ClickHouse string LITERAL — `'…'`, quotes included — produced by
/// [`super::escape::ch_string`]. Distinct from [`CheckedFragment`]: this is
/// a VALUE that lands in an equality/`IN` position, not a boolean
/// expression.
///
/// # THE SEAL — sensitive to it, and demonstrated
///
/// Putting `pub` on the `sql` field below makes this snippet compile and
/// this doctest FAIL (measured, issue #286 review round 1).
///
/// ```compile_fail,E0451
/// use pulsus_read::logql::predicate;
///
/// let sql = "'x'".to_string();
/// let forged = predicate::CheckedLiteral { sql };
/// assert_eq!(forged.as_sql(), "'x'");
/// ```
///
/// The compiling twin, same skeleton, minted instead of forged:
///
/// ```
/// use pulsus_read::logql::predicate;
///
/// let sql = "'x'".to_string();
/// let forged = predicate::literal("x");
/// assert_eq!(forged.as_sql(), sql);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckedLiteral {
    /// NO visibility modifier — the same seal, the same leaf.
    sql: String,
}

impl CheckedLiteral {
    /// The rendered literal, quotes included. Read-only.
    pub fn as_sql(&self) -> &str {
        &self.sql
    }
}

/// A `log_streams_idx` month partition literal — `'YYYY-MM-01'`, quotes
/// included. Its only mint, [`month_literal`], takes **integers**: there is
/// no path from a `String` to a `MonthLiteral`, so "no caller text enters
/// the month predicate" is a property rustc holds up rather than an
/// observation about callers.
///
/// # THE SEAL — sensitive to it, and demonstrated
///
/// Putting `pub` on the `sql` field below makes this snippet compile and
/// this doctest FAIL (measured, issue #286 review round 1).
///
/// ```compile_fail,E0451
/// use pulsus_read::logql::predicate;
///
/// let sql = "'2026-07-01'".to_string();
/// let forged = predicate::MonthLiteral { sql };
/// assert_eq!(forged.as_sql(), "'2026-07-01'");
/// ```
///
/// The compiling twin, same skeleton, minted instead of forged:
///
/// ```
/// use pulsus_read::logql::predicate;
///
/// let sql = "'2026-07-01'".to_string();
/// let forged = predicate::month_literal(2026, 7);
/// assert_eq!(forged.as_sql(), sql);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonthLiteral {
    /// NO visibility modifier — the same seal, the same leaf.
    sql: String,
}

impl MonthLiteral {
    /// The rendered date literal, quotes included. Read-only.
    pub fn as_sql(&self) -> &str {
        &self.sql
    }
}

/// THE ONLY [`CheckedLiteral`] mint. Escaping IS the constructor: there is
/// no path from a `String` to a `CheckedLiteral` that does not run
/// [`super::escape::ch_string`].
pub fn literal(value: &str) -> CheckedLiteral {
    CheckedLiteral {
        sql: ch_string(value),
    }
}

/// THE ONLY [`MonthLiteral`] mint — `'{year:04}-{month:02}-01'`, the exact
/// rendering [`super::plan::months_overlapping`] has always emitted. Takes
/// integers, so no caller text can enter it.
pub fn month_literal(year: i64, month: u32) -> MonthLiteral {
    MonthLiteral {
        sql: format!("'{year:04}-{month:02}-01'"),
    }
}

/// `(key = 'k'[ AND val = 'v'][ AND match(val, '^(?:p)$')]…)` — one positive
/// `log_streams_idx` OR-branch, conditions in exactly the order
/// [`super::plan::normalize_matchers`] has always built them: the key
/// equality, then the optional value equality, then one anchored regex
/// condition per pattern in order.
pub fn index_positive_branch(
    key: &str,
    eq_value: Option<&str>,
    anchored_regexes: &[String],
) -> Result<CheckedFragment, PipelineError> {
    let mut conds = vec![format!("key = {}", ch_string(key))];
    if let Some(v) = eq_value {
        conds.push(format!("val = {}", ch_string(v)));
    }
    for pat in anchored_regexes {
        conds.push(format!("match(val, {})", ch_regex_anchored_checked(pat)?));
    }
    Ok(CheckedFragment {
        sql: format!("({})", conds.join(" AND ")),
    })
}

/// `(key = 'k' AND val = 'v')` — one `!=` negative branch. Renders no
/// `match(`, and takes no regex, so it is infallible.
pub fn index_neq_branch(key: &str, value: &str) -> CheckedFragment {
    CheckedFragment {
        sql: format!("(key = {} AND val = {})", ch_string(key), ch_string(value)),
    }
}

/// `(key = 'k' AND match(val, '^(?:p)$'))` — one `!~` negative branch.
pub fn index_nre_branch(key: &str, pattern: &str) -> Result<CheckedFragment, PipelineError> {
    Ok(CheckedFragment {
        sql: format!(
            "(key = {} AND match(val, {}))",
            ch_string(key),
            ch_regex_anchored_checked(pattern)?
        ),
    })
}

/// Compiles one pushed-down `LineFilter` stage. Positive ops (`|=`, `|~`)
/// render exactly one predicate over `body` — `body LIKE '%needle%'` or
/// `match(body, …)` — and negative ops (`!=`, `!~`) wrap it in `NOT (...)`,
/// which is the exclusion semantic directly because `body` is a non-NULL
/// `String` column and the positive predicate is total over it.
///
/// **No token prefilter is minted, in any form (issue #450).** The old
/// rendering ANDed `hasToken(body, <token>)` onto the exact predicate on
/// the claim that a bloom filter has no false negatives. That is true of
/// the `tokenbf_v1` *skip index* and false of the `hasToken()` *function*,
/// which is an exact whole-token membership test: a needle that is a
/// fragment of a longer token gives `hasToken = 0` while the text is
/// plainly present, so `|=` dropped matching lines, `!=` kept lines it
/// should have excluded, and a needle containing `_` failed the query
/// outright (`BAD_ARGUMENTS`). There is no safe subset for us to write —
/// a token prefilter is equivalence-preserving only when the needle is
/// token-aligned *in the data*, which the query cannot know. ClickHouse's
/// own `LIKE`/`match` index analysis derives the sound version of that
/// rule (a token is required only when it is separator-delimited *inside
/// the pattern*) and applies it to `tokenbf_v1` for free, and the
/// `ngrambf_v1(4, …)` body index prunes for both forms.
///
/// An `or` group (M8-LQ2 `linefilter.or`) is a disjunction of the same
/// per-alternative predicate: `((a) OR (b) …)` for positive ops,
/// `NOT ((a) OR (b) …)` for negative ops. A
/// single-value filter is left un-wrapped so its pushed-down SQL is
/// byte-identical to the pre-`or` output. Callers must gate on
/// [`super::plan::is_pushable_line_filter`], which stays in `plan.rs`: this
/// only ever sees literal/regex alternatives (`ip(…)` is served
/// client-side).
pub fn line_filter(lf: &LineFilter) -> Result<CheckedFragment, PipelineError> {
    let mut disjuncts: Vec<String> = Vec::new();
    for (value, _) in lf.alternatives() {
        disjuncts.push(match lf.op {
            LineFilterOp::Contains | LineFilterOp::NotContains => contains_predicate(value),
            LineFilterOp::Regex | LineFilterOp::NotRegex => regex_predicate(value)?,
        });
    }
    let core = if lf.or_matches.is_empty() {
        disjuncts
            .into_iter()
            .next()
            .expect("a line filter always has a head alternative")
    } else {
        disjuncts
            .iter()
            .map(|p| format!("({p})"))
            .collect::<Vec<_>>()
            .join(" OR ")
    };
    Ok(CheckedFragment {
        sql: match lf.op {
            LineFilterOp::Contains | LineFilterOp::Regex => {
                if lf.or_matches.is_empty() {
                    core
                } else {
                    format!("({core})")
                }
            }
            LineFilterOp::NotContains | LineFilterOp::NotRegex => format!("NOT ({core})"),
        },
    })
}

/// The ONLY two ways an emitted statement may NAME the
/// structured-metadata column (issue #507, condition 6).
///
/// The column name is not reachable as a string from this module's public
/// API: it lives in [`METADATA_COLUMN`], which is private, and every
/// renderer that mentions the column takes one of these variants or calls
/// the private guard mint below. An alias or a subquery cannot get around
/// that, because no function renders an extraction over the column in the
/// first place.
///
/// **No SQL this module emits reads INSIDE the column.** It may be
/// projected, grouped by, and compared whole against a literal — nothing
/// else. Two readers of a stored value disagree; one reader cannot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataTerm {
    /// `structured_metadata` in a `SELECT` list.
    Project,
    /// `structured_metadata` in a `GROUP BY`.
    Group,
}

/// The one place the column's name is written.
const METADATA_COLUMN: &str = "structured_metadata";

impl MetadataTerm {
    /// The column reference this term renders. Both variants render the
    /// bare name; they differ in where the caller puts it, which is why
    /// they are two variants and not one.
    pub fn as_sql(self) -> &'static str {
        match self {
            MetadataTerm::Project | MetadataTerm::Group => METADATA_COLUMN,
        }
    }
}

/// `structured_metadata != ''` — the whole-value guard.
///
/// **Private, and takes no argument.** A key-specific guard would be a
/// second reader of the column, and a second reader disagrees with the
/// first: our flat scanner accepts shapes a JSON parser rejects and
/// decodes escapes differently, measured in both directions. This term
/// reads no key. It is true on every row carrying any metadata at all,
/// which is a superset of every row where the two readers could differ.
///
/// **Rows it drops: none the evaluator keeps.** It only ever adds a
/// disjunct, so it can only widen the predicate it sits in.
///
/// Its cost is stated rather than hidden: on a corpus where every row
/// carries metadata the term is true everywhere, so the predicate it
/// guards keeps every row and buys nothing.
fn metadata_non_empty_guard() -> CheckedFragment {
    CheckedFragment {
        sql: format!("{METADATA_COLUMN} != ''"),
    }
}

/// Why a parsed-name label filter is not pushed down (issue #507, W3).
///
/// **Not a [`PipelineError`].** Every one of these means the query is
/// valid and the filter is evaluated after the read, exactly as it is
/// today.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParsedFilterRefusal {
    /// The operator has no specified fragment. Only `=` on the string
    /// form is served; `!=`, `=~` and `!~` are not.
    OperatorNotServed,
    /// The label name can be produced by more than one raw key of this
    /// parser's input, and no key-precise expression reads the one the
    /// evaluator resolved. See [`name_is_unambiguous`].
    AmbiguousName,
    /// This parser has no key-precise expression at all — a capture's
    /// value comes from running a regular expression over the line, which
    /// the database cannot reproduce — and the value probe does not serve
    /// this operator.
    NoKeyExpression,
    /// A non-finite threshold. `| k > 1e400` parses to an infinity here,
    /// and the database refuses the literal with a syntax error.
    ThresholdNotFinite,
    /// The label name is not one a fragment may name.
    NameNotRenderable,
}

/// Can this label name have been produced by more than one raw key?
///
/// **No, exactly when it contains no `_`.** `sanitize_label_key`
/// (`pipeline.rs:3939-3952`) does three things and no more: it prepends
/// `_` when the first character is an ASCII digit, keeps ASCII
/// alphanumerics and `_`, and replaces every other character with `_`. It
/// never deletes and never shortens. A bare `| json` additionally flattens
/// nested objects, inserting `_` between levels (`pipeline.rs:27`).
///
/// So **every transformation either preserves the string or introduces a
/// `_`.** A name with no `_` had nothing introduced, so it was not
/// transformed, so its only preimage is that same key at the top level.
///
/// The bound is tight in that direction only. A name containing `_` is
/// merely *capable* of ambiguity — it is actually ambiguous only when one
/// document carries two of its preimages, which no plan-time predicate can
/// know. Measured: `{"a":{"b":"nested"},"a_b":"top"}` gives the label
/// `a_b = nested`, because extraction skips an already-extracted name and
/// the first raw key in document order wins, while
/// `JSONExtractString(body,'a_b')` reads `top` — the one the evaluator
/// discarded.
fn name_is_unambiguous(name: &str) -> bool {
    !name.contains('_')
}

/// A label name a fragment may render. The parser cannot produce anything
/// else, and a fragment builder does not take the word of a caller that
/// did not come through it.
fn name_is_renderable(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// The expression a parser makes a name resolve to, for the two parsers
/// that have one.
fn parsed_name_expr(name: &str, parser: &ParserStage) -> Option<String> {
    let key = ch_string(name);
    match parser {
        ParserStage::Json { .. } => Some(format!("JSONExtractString(body, {key})")),
        ParserStage::Logfmt { .. } => Some(format!(
            "extractKeyValuePairs(body, '=', ' \\t\\r\\n', '\"')[{key}]"
        )),
        // A capture's value is produced by running a regular expression
        // over the line. There is no key-precise expression to write.
        ParserStage::Regexp(_) | ParserStage::Pattern(_) => None,
    }
}

/// `| <parser> | NAME = "VALUE"`, pushed down (issue #507, W3).
///
/// # The rule every cell here is built to
///
/// A `Fidelity::Wider` predicate **may emit rows the evaluator discards,
/// and may never discard a row the evaluator keeps.** Each route below
/// states which it does.
///
/// # Route A — a key-precise comparison, for a name with no `_`
///
/// ```text
/// | json    (JSONType(body,'k') != 'String' OR JSONExtractString(body,'k') = 'v' OR structured_metadata != '')
/// | logfmt  (extractKeyValuePairs(body,'=',' \t\r\n','"')['k'] IN ('', 'v') OR position(body, '\\') > 0 OR structured_metadata != '')
/// ```
///
/// **Rows it drops: none the evaluator keeps.** The name has exactly one
/// preimage ([`name_is_unambiguous`]), so the expression reads the key the
/// evaluator resolved. Every other kind of stored value reaches the type
/// guard or the empty alternative: a key that is absent, a line that is not
/// JSON at all, a line with trailing bytes after the object, a number, an
/// object, an array. Both sides decode `\uXXXX` the same way — measured,
/// `{"k":"A"}` gives the label `A` here and `JSONExtractString` gives `A`
/// there — so an escaped value is compared, not missed.
///
/// # Route B — a value probe, for a parser with no key expression
///
/// ```text
/// | regexp, | pattern    body LIKE '%v%'
/// ```
///
/// **Rows it drops: none the evaluator keeps.** A capture is a literal
/// byte slice of the line, so if the evaluator kept the row its capture
/// equalled `v` and those bytes are in the body. Rows without `v` anywhere
/// are dropped, and the evaluator drops them too.
///
/// It reaches the body skip indexes, so for a selective value it prunes
/// better than a key-precise comparison would.
///
/// **Route B is NOT offered for `| json` or `| logfmt`**, and the reason is
/// measured rather than assumed: those two decode their values, so a body
/// may hold `A` where the label is `A` and the probe would miss a row
/// the evaluator keeps. No test on the value alone can exclude that, since
/// JSON permits any character to be written as an escape.
pub fn parsed_string_filter(
    name: &str,
    op: MatchOp,
    value: &str,
    parser: &ParserStage,
) -> Result<CheckedFragment, ParsedFilterRefusal> {
    let cmp = match op {
        MatchOp::Eq => "=",
        MatchOp::Neq => "!=",
        // The two regular-expression operators are not served, and the
        // reason is not the SQL. Our label-filter regex semantics already
        // differ from the reference at a width this plan records and has
        // not repaired; pushing them down would bind that difference into
        // a statement before anyone has decided about it.
        MatchOp::Re | MatchOp::Nre => return Err(ParsedFilterRefusal::OperatorNotServed),
    };
    if !name_is_renderable(name) {
        return Err(ParsedFilterRefusal::NameNotRenderable);
    }
    let guard = metadata_non_empty_guard();
    let guard = guard.as_sql();
    let Some(expr) = parsed_name_expr(name, parser) else {
        // Route B, and it serves `=` alone.
        //
        // **Rows `=` drops: none the evaluator keeps.** A capture is a
        // literal slice of the line, so a kept row's bytes are in the
        // body.
        //
        // **`!=` has no probe form and is refused.** The evaluator keeps a
        // row whose capture differs from the value; `NOT (body LIKE
        // '%v%')` drops every row holding the value ANYWHERE, including a
        // row where it sits outside the capture and the capture differs.
        // That is the forbidden direction, and no test on the value
        // repairs it — a substring probe cannot say where in the line the
        // bytes sat.
        if matches!(op, MatchOp::Neq) {
            return Err(ParsedFilterRefusal::OperatorNotServed);
        }
        return Ok(CheckedFragment {
            sql: contains_predicate(value),
        });
    };
    if !name_is_unambiguous(name) {
        return Err(ParsedFilterRefusal::AmbiguousName);
    }
    let v = ch_string(value);
    let key = ch_string(name);
    Ok(CheckedFragment {
        sql: match parser {
            ParserStage::Json { .. } => {
                format!("(JSONType(body, {key}) != 'String' OR {expr} {cmp} {v} OR {guard})")
            }
            // Two extra alternatives, each for a case the comparison
            // alone would get wrong. The empty one is the sanitising
            // case: a raw key the database reads under a different
            // spelling renders `''` here, and the evaluator decides. The
            // backslash one is the escaping case: a quoted logfmt value
            // may carry an escape that the two decoders resolve
            // differently, so any line holding one is kept whole.
            //
            // **The empty alternative belongs to `=` and must NOT be
            // carried over to `!=`.** For `!=` it would keep a row whose
            // value EQUALS the filter's — harmless — and drop one whose
            // value is neither empty nor equal, which the evaluator
            // keeps. Checked rather than mirrored: with the label
            // `other`, `!= "error"`, the evaluator keeps and the reused
            // form drops.
            _ if matches!(op, MatchOp::Neq) => {
                format!("({expr} != {v} OR position(body, '\\\\') > 0 OR {guard})")
            }
            _ => format!("({expr} IN ('', {v}) OR position(body, '\\\\') > 0 OR {guard})"),
        },
    })
}

/// `| json | NAME <op> <number>`, pushed down (issue #507, W3).
///
/// ```text
/// (JSONType(body,'k') NOT IN ('Int64','UInt64','Double') OR JSONExtractFloat(body,'k') >= 500.0 OR structured_metadata != '')
/// ```
///
/// **Rows it drops: none the evaluator keeps.** It drops only a row whose
/// key holds a JSON number failing the comparison, and the evaluator drops
/// that row too. Every other type reaches the guard, including a
/// numeric-looking STRING — which matters, because `JSONExtractFloat` of
/// `"12abc"` is `0` while our conversion of the label text is not a number
/// at all.
///
/// # The threshold is rendered as a float literal, and that is not cosmetic
///
/// `threshold` is rendered with `{:?}` — Rust's shortest round-tripping
/// decimal, which always carries a `.` or an exponent, so the database
/// lexes it as `Float64` and both sides have taken the same rounding.
/// Written bare it would lex as `UInt64` and be compared against a
/// `Float64` **exactly**: measured, `toTypeName(9007199254740993)` is
/// `UInt64`, `toTypeName(9007199254740993.0)` is `Float64`, and
/// `9007199254740992.0 = 9007199254740993` is false — so a bare literal at
/// that magnitude returns no rows where the answer is two.
///
/// **`threshold` must be the `f64` OUR unit parser produced**, never a
/// re-parse of the source text: a duration or size literal is interpreted
/// by that parser and the database has no equivalent.
///
/// A non-finite threshold is refused. `| k > 1e400` parses to an infinity
/// here and the database refuses the literal outright, so there is no
/// fragment to emit and the filter is evaluated after the read.
pub fn parsed_numeric_filter(
    name: &str,
    op: CompareOp,
    threshold: f64,
    parser: &ParserStage,
) -> Result<CheckedFragment, ParsedFilterRefusal> {
    if !name_is_renderable(name) {
        return Err(ParsedFilterRefusal::NameNotRenderable);
    }
    if !threshold.is_finite() {
        return Err(ParsedFilterRefusal::ThresholdNotFinite);
    }
    if !matches!(parser, ParserStage::Json { .. }) {
        // The specified numeric cell reads `JSONExtractFloat`, which is a
        // JSON reader. No other parser has one, and the value probe is a
        // substring test that cannot serve an inequality.
        return Err(ParsedFilterRefusal::NoKeyExpression);
    }
    if !name_is_unambiguous(name) {
        return Err(ParsedFilterRefusal::AmbiguousName);
    }
    let key = ch_string(name);
    let guard = metadata_non_empty_guard();
    let guard = guard.as_sql();
    let cmp = match op {
        CompareOp::Eq => "=",
        CompareOp::Neq => "!=",
        CompareOp::Gt => ">",
        CompareOp::Gte => ">=",
        CompareOp::Lt => "<",
        CompareOp::Lte => "<=",
    };
    Ok(CheckedFragment {
        sql: format!(
            "(JSONType(body, {key}) NOT IN ('Int64','UInt64','Double') OR \
             JSONExtractFloat(body, {key}) {cmp} {threshold:?} OR {guard})"
        ),
    })
}

/// Why an anchored bucket grid cannot be rendered (issue #507, W2).
///
/// **Not a [`PipelineError`].** Every one of these means the query is
/// valid and the range aggregation does not lower here, so the link stays
/// residual and the evaluator answers exactly as it does today. A
/// `PipelineError` would surface as a 400 for a query nothing is wrong
/// with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketGridRefusal {
    /// `step_ns` is zero or negative, so the grid has no points.
    StepNotPositive,
    /// The anchor does not sit at or below the first row the statement can
    /// admit. `intDiv` truncates toward zero, which is a floor only for a
    /// non-negative numerator, so an anchor above the scan start would
    /// bucket the early rows upward instead of downward.
    AnchorAboveScanStart,
    /// The worst-case arithmetic is not representable in `Int64`.
    WouldOverflow,
}

/// The anchored bucket expression: the grid point a row belongs to under a
/// window that is **open below and closed above**.
///
/// ```text
/// <lo> + intDiv(<col> - <lo> + <step> - 1, <step>) * <step>
/// ```
///
/// **`lo` is `grid_start_ns - step_ns` and never the scan's own start.**
/// The scan start is widened backwards by the range selector's duration
/// (`plan.rs`'s `widen_scan_start`) so the first grid point's window is
/// complete; using it as the anchor would shift the whole grid by one
/// range. The two coincide exactly when the range equals the step, which
/// is the case that would make a wrong implementation look right.
///
/// **Why a ceiling and not the floor the shipped range renderer uses.**
/// The window for grid point `g` is `(g - range, g]`, on a grid anchored
/// at the query's start. A floor onto a grid anchored at the epoch is a
/// different function: it is closed below rather than above, and its
/// output is a multiple of the step rather than a point of the query's
/// grid. The two agree only when the grid start happens to be a multiple
/// of the step, and then only at the grid points themselves.
///
/// `bucket_col` is a `&'static str` and its only callers pass
/// [`super::sql::MetricShape::bucket_col`], which returns one of two
/// literals — no caller text can reach it.
///
/// # Refusals
///
/// Three, each with its own reason and none of them a client error. The
/// overflow bound is taken over the newest row the statement can admit,
/// which is the same `scan_end_ns` the statement's own `WHERE` enforces —
/// so "this statement's arithmetic cannot wrap" is a property of the pair,
/// not of the expression alone.
pub fn bucket_expr(
    bucket_col: &'static str,
    lo_ns: i64,
    step_ns: i64,
    scan_start_ns: i64,
    scan_end_ns: i64,
) -> Result<CheckedFragment, BucketGridRefusal> {
    if step_ns <= 0 {
        return Err(BucketGridRefusal::StepNotPositive);
    }
    if lo_ns > scan_start_ns {
        return Err(BucketGridRefusal::AnchorAboveScanStart);
    }
    // The widest numerator the statement can evaluate, and the grid point
    // it produces. Both checked: a wrapped bucket is a silently wrong
    // answer, where a refusal is today's behaviour.
    let numerator = scan_end_ns
        .checked_sub(lo_ns)
        .and_then(|d| d.checked_add(step_ns))
        .and_then(|d| d.checked_sub(1))
        .ok_or(BucketGridRefusal::WouldOverflow)?;
    (numerator / step_ns)
        .checked_mul(step_ns)
        .and_then(|t| lo_ns.checked_add(t))
        .ok_or(BucketGridRefusal::WouldOverflow)?;
    Ok(CheckedFragment {
        sql: format!(
            "{lo_ns} + intDiv({bucket_col} - {lo_ns} + {step_ns} - 1, {step_ns}) * {step_ns}"
        ),
    })
}

/// `countIf(toFloat64OrNull(val) IS NULL AND NOT match(val, '<UUID_RE>'))` —
/// `/detected_labels`' non-ID-value aggregate. **Takes no argument**, so no
/// user string can enter it; [`UUID_RE`] is a module constant.
pub(super) fn non_id_values_expr() -> CheckedFragment {
    CheckedFragment {
        sql: format!(
            "countIf(toFloat64OrNull(val) IS NULL AND NOT match(val, {}))",
            ch_string(UUID_RE)
        ),
    }
}

fn contains_predicate(phrase: &str) -> String {
    format!("body LIKE {}", ch_like_contains(phrase))
}

fn regex_predicate(pattern: &str) -> Result<String, PipelineError> {
    Ok(format!(
        "match(body, {})",
        ch_regex_unanchored_checked(pattern)?
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn regex_filter(value: &str) -> LineFilter {
        LineFilter {
            op: LineFilterOp::Regex,
            value: value.to_string(),
            value_is_ip: false,
            or_matches: Vec::new(),
        }
    }

    /// The mint VALIDATES rather than merely wrapping: `(` is an
    /// uncompilable pattern, so it is refused at the mint instead of
    /// reaching ClickHouse.
    #[test]
    fn an_uncompilable_pattern_is_refused_at_the_line_filter_mint() {
        let err = line_filter(&regex_filter("(")).expect_err("`(` cannot compile");
        assert!(
            matches!(err, PipelineError::BadRegex(_)),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn an_uncompilable_pattern_is_refused_at_the_index_regex_mints() {
        assert!(matches!(
            index_nre_branch("k", "(").expect_err("`(` cannot compile"),
            PipelineError::BadRegex(_)
        ));
        assert!(matches!(
            index_positive_branch("k", None, &["(".to_string()]).expect_err("`(` cannot compile"),
            PipelineError::BadRegex(_)
        ));
    }

    #[test]
    fn the_literal_mint_is_exactly_the_escaper() {
        for value in ["a'b", "a\\b", "a\nb", "a\tb", "a\rb", "a\0b", "plain"] {
            assert_eq!(literal(value).as_sql(), ch_string(value));
        }
    }

    #[test]
    fn the_month_mint_renders_the_committed_date_literal_shape() {
        assert_eq!(month_literal(2026, 7).as_sql(), "'2026-07-01'");
        assert_eq!(month_literal(999, 12).as_sql(), "'0999-12-01'");
    }

    /// AC9: the newtypes are a compile-time wrapper, not a runtime cost.
    #[test]
    fn the_newtypes_are_the_size_and_alignment_of_the_string_they_wrap() {
        use std::mem::{align_of, size_of};
        assert_eq!(size_of::<CheckedFragment>(), size_of::<String>());
        assert_eq!(align_of::<CheckedFragment>(), align_of::<String>());
        assert_eq!(size_of::<CheckedLiteral>(), size_of::<String>());
        assert_eq!(align_of::<CheckedLiteral>(), align_of::<String>());
        assert_eq!(size_of::<MonthLiteral>(), size_of::<String>());
        assert_eq!(align_of::<MonthLiteral>(), align_of::<String>());
    }

    #[test]
    fn the_positive_branch_renders_key_then_value_then_regexes_in_order() {
        let f = index_positive_branch("app", Some("api"), &["a.*".to_string()])
            .expect("`a.*` compiles");
        assert_eq!(
            f.as_sql(),
            "(key = 'app' AND val = 'api' AND match(val, '^(?:a.*)$'))"
        );
    }

    #[test]
    fn the_negative_branches_render_the_committed_shapes() {
        assert_eq!(
            index_neq_branch("app", "api").as_sql(),
            "(key = 'app' AND val = 'api')"
        );
        assert_eq!(
            index_nre_branch("app", "a.*")
                .expect("`a.*` compiles")
                .as_sql(),
            "(key = 'app' AND match(val, '^(?:a.*)$'))"
        );
    }

    #[test]
    fn the_non_id_values_aggregate_takes_no_caller_input() {
        let f = non_id_values_expr();
        assert_eq!(
            f.as_sql(),
            format!(
                "countIf(toFloat64OrNull(val) IS NULL AND NOT match(val, {}))",
                ch_string(UUID_RE)
            )
        );
    }

    #[test]
    fn a_regex_line_filter_renders_the_bare_unanchored_match() {
        assert_eq!(
            line_filter(&regex_filter("boom"))
                .expect("`boom` compiles")
                .as_sql(),
            "match(body, 'boom')"
        );
    }

    /// AC4 (issue #450): the correctness condition is not a comment and not
    /// a predicate we maintain — **we mint no token prefilter at all**. A
    /// `hasToken(body, …)` conjunct is only equivalence-preserving when the
    /// needle happens to be token-aligned in the data, which the query
    /// cannot know; re-introducing one in any form fails this test.
    ///
    /// Every needle here is a valid RE2 pattern as well as a literal, so
    /// all four ops can be minted from the same list.
    #[test]
    fn no_line_filter_op_mints_a_token_prefilter_for_any_shaped_needle() {
        const SHAPED: &[&str] = &[
            "connection refused",
            "user_id",
            "06Q924X3qTas",
            "a-b.c/d:e",
            "100%",
            "a_b",
            "a\\\\b",
            "café",
            "a—b",
            "🙂",
            "",
        ];
        for op in [
            LineFilterOp::Contains,
            LineFilterOp::NotContains,
            LineFilterOp::Regex,
            LineFilterOp::NotRegex,
        ] {
            for needle in SHAPED {
                let sql = line_filter(&LineFilter {
                    op,
                    value: (*needle).to_string(),
                    value_is_ip: false,
                    or_matches: Vec::new(),
                })
                .unwrap_or_else(|e| panic!("{op:?} {needle:?} must mint: {e:?}"))
                .as_sql()
                .to_string();
                assert!(
                    !sql.contains("hasToken("),
                    "{op:?} {needle:?} minted a token prefilter: {sql}"
                );
            }
        }
    }

    /// The `|=` mint is the LIKE escaper, and `%`/`_`/`\` in the needle are
    /// escaped so they match literally rather than as LIKE wildcards
    /// (issue #450: the previous rendering failed the query outright on a
    /// needle containing `_`).
    #[test]
    fn a_contains_line_filter_renders_an_escaped_like_pattern() {
        for (needle, expected) in [
            ("connection refused", "body LIKE '%connection refused%'"),
            ("user_id", "body LIKE '%user\\\\_id%'"),
            ("100%", "body LIKE '%100\\\\%%'"),
            ("a\\b", "body LIKE '%a\\\\\\\\b%'"),
            ("", "body LIKE '%%'"),
        ] {
            let f = LineFilter {
                op: LineFilterOp::Contains,
                value: needle.to_string(),
                value_is_ip: false,
                or_matches: Vec::new(),
            };
            assert_eq!(
                line_filter(&f).expect("a literal always mints").as_sql(),
                expected
            );
        }
    }

    // -----------------------------------------------------------------
    // W3 (issue #507): the parsed-name filter's fragments, its refusals,
    // and the rule every cell is built to.
    // -----------------------------------------------------------------

    /// One row of `tests/logql_parsed_filter_witnesses.tsv`.
    pub(crate) struct Witness {
        pub(crate) form: String,
        pub(crate) parser: String,
        pub(crate) arg: String,
        pub(crate) name: String,
        pub(crate) value: String,
        pub(crate) body: String,
        pub(crate) keeps: bool,
    }

    /// The witness table, read from the file both halves of the rule read.
    pub(crate) fn witnesses() -> Vec<Witness> {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/logql_parsed_filter_witnesses.tsv"
        );
        let text = std::fs::read_to_string(path).expect("the witness table is readable");
        let mut out = Vec::new();
        for line in text.lines() {
            if line.starts_with('#') || line.trim().is_empty() || line.starts_with("form\t") {
                continue;
            }
            let f: Vec<&str> = line.split('\t').collect();
            assert_eq!(f.len(), 7, "seven columns: {line:?}");
            out.push(Witness {
                form: f[0].to_string(),
                parser: f[1].to_string(),
                arg: f[2].to_string(),
                name: f[3].to_string(),
                value: f[4].to_string(),
                body: f[5].to_string(),
                keeps: f[6] == "1",
            });
        }
        assert!(!out.is_empty(), "the witness table is not empty");
        out
    }

    /// The LogQL query one witness row describes.
    pub(crate) fn witness_query(w: &Witness) -> String {
        let stage = match w.parser.as_str() {
            "json" => "| json".to_string(),
            "logfmt" => "| logfmt".to_string(),
            "regexp" => format!(r#"| regexp "{}""#, w.arg),
            "pattern" => format!(r#"| pattern "{}""#, w.arg),
            other => panic!("unknown parser {other}"),
        };
        let filter = match w.form.as_str() {
            "numeric" => format!("| {} >= {}", w.name, w.value),
            "neq" => format!(r#"| {}!="{}""#, w.name, w.value),
            _ => format!(r#"| {}="{}""#, w.name, w.value),
        };
        format!(r#"{{s="m"}} {stage} {filter}"#)
    }

    /// W3 (issue #507) — **the `keeps` column is the answer the shipped
    /// pipeline gives**, not a value someone typed and nobody ran.
    ///
    /// The column is hand-written, because it is the claim the live half
    /// checks the database against; this test is what stops a typo in it
    /// becoming a false expectation there.
    #[test]
    fn every_witness_row_states_the_answer_the_pipeline_gives() {
        for w in witnesses() {
            let query = witness_query(&w);
            let expr = pulsus_logql::parse(&query).unwrap_or_else(|e| panic!("{query}: {e}"));
            let pulsus_logql::Expr::Log(log) = expr else {
                panic!("{query} is not a log expression")
            };
            let compiled = super::super::pipeline::CompiledPipeline::compile(&log.pipeline)
                .unwrap_or_else(|e| panic!("{query}: {e}"));
            let mut out = Vec::new();
            let kept = compiled
                .run_into(&w.body, &[], 0, &mut out)
                .expect("within budget")
                .is_some();
            assert_eq!(
                kept, w.keeps,
                "`{query}` over `{}`: the table says keeps={}, the pipeline says {kept}",
                w.body, w.keeps
            );
        }
    }

    fn json_parser() -> ParserStage {
        ParserStage::Json {
            extractions: Vec::new(),
        }
    }

    fn logfmt_parser() -> ParserStage {
        ParserStage::Logfmt {
            strict: false,
            keep_empty: false,
            extractions: Vec::new(),
        }
    }

    /// W3 (issue #507) — the three served fragments, byte for byte.
    #[test]
    fn a_parsed_name_filter_renders_the_specified_fragment() {
        assert_eq!(
            parsed_string_filter("level", MatchOp::Eq, "error", &json_parser())
                .expect("served")
                .as_sql(),
            r"(JSONType(body, 'level') != 'String' OR JSONExtractString(body, 'level') = 'error' OR structured_metadata != '')"
        );
        assert_eq!(
            parsed_string_filter("level", MatchOp::Neq, "error", &json_parser())
                .expect("served")
                .as_sql(),
            r"(JSONType(body, 'level') != 'String' OR JSONExtractString(body, 'level') != 'error' OR structured_metadata != '')"
        );
        // The logfmt form BRANCHES on the operator: the empty alternative
        // belongs to `=` and would drop a kept row under `!=`.
        assert_eq!(
            parsed_string_filter("level", MatchOp::Neq, "error", &logfmt_parser())
                .expect("served")
                .as_sql(),
            concat!(
                r#"(extractKeyValuePairs(body, '=', ' \t\r\n', "#,
                r#"'"'"#,
                r#")['level'] != 'error' OR position(body, '\\') > 0 "#,
                r#"OR structured_metadata != '')"#,
            )
        );
        assert_eq!(
            parsed_string_filter(
                "a",
                MatchOp::Eq,
                "err",
                &ParserStage::Regexp("(?P<a>[a-z]+)".to_string())
            )
            .expect("served")
            .as_sql(),
            "body LIKE '%err%'"
        );
        assert_eq!(
            parsed_numeric_filter("status", CompareOp::Gte, 500.0, &json_parser())
                .expect("served")
                .as_sql(),
            r"(JSONType(body, 'status') NOT IN ('Int64','UInt64','Double') OR JSONExtractFloat(body, 'status') >= 500.0 OR structured_metadata != '')"
        );
    }

    /// W3 (issue #507) — **a numeric threshold is a float literal, and
    /// that is not cosmetic.**
    ///
    /// Written bare, `9007199254740993` lexes as `UInt64` and is compared
    /// against a `Float64` exactly. Measured on 26.3.29.7:
    /// `toTypeName(9007199254740993)` is `UInt64`,
    /// `toTypeName(9007199254740993.0)` is `Float64`, and
    /// `9007199254740992.0 = 9007199254740993` is false — so the bare form
    /// returns no rows at that magnitude where the answer is two.
    ///
    /// The four values are chosen where the two readings differ by the
    /// least, on both sides of the point where they start to differ.
    #[test]
    fn a_numeric_threshold_renders_as_a_float_literal() {
        for (threshold, want) in [
            (500.0f64, "500.0"),
            (0.25, "0.25"),
            (9007199254740992.0, "9007199254740992.0"),
            (9007199254740993.0, "9007199254740992.0"),
            (9007199254740994.0, "9007199254740994.0"),
            // The smallest positive subnormal, written as its bits so no
            // decimal spelling is involved: `{:?}` renders it `5e-324`,
            // and the database reads `5e-324` back as the same double.
            // A string cast does not — `toFloat64('4.9406564584124654e-324')`
            // is `0` on 26.3.29.7 — which is why the literal is a decimal
            // and not a cast.
            (f64::from_bits(1), "5e-324"),
        ] {
            let sql = parsed_numeric_filter("status", CompareOp::Gte, threshold, &json_parser())
                .expect("served");
            assert!(
                sql.as_sql()
                    .contains(&format!("JSONExtractFloat(body, 'status') >= {want} OR")),
                "threshold {threshold} must render as `{want}`: {}",
                sql.as_sql()
            );
        }
    }

    /// W3 (issue #507) — every refusal, each with the input that reaches
    /// it and no other.
    #[test]
    fn a_parsed_name_filter_refuses_with_the_stated_reason() {
        let json = json_parser();
        let re = ParserStage::Regexp("(?P<a>.*)".to_string());
        // The two regular-expression operators are not served, because
        // our regex label-filter semantics already differ from the
        // reference and pushing them down would bind that difference into
        // a statement.
        for op in [MatchOp::Re, MatchOp::Nre] {
            assert_eq!(
                parsed_string_filter("level", op, "error", &json),
                Err(ParsedFilterRefusal::OperatorNotServed),
                "{op:?}"
            );
        }
        // A name with `_` can be produced by more than one raw key.
        assert_eq!(
            parsed_string_filter("trace_id", MatchOp::Eq, "x", &json),
            Err(ParsedFilterRefusal::AmbiguousName)
        );
        assert_eq!(
            parsed_numeric_filter("status_code", CompareOp::Gte, 500.0, &json),
            Err(ParsedFilterRefusal::AmbiguousName)
        );
        // The value probe serves `=` alone: for `!=` it would drop every
        // row holding the value anywhere, including a row where it sits
        // outside the capture and the capture differs.
        assert_eq!(
            parsed_string_filter("a", MatchOp::Neq, "v", &re),
            Err(ParsedFilterRefusal::OperatorNotServed)
        );
        assert!(
            parsed_string_filter("a", MatchOp::Eq, "v", &re).is_ok(),
            "the control: the probe serves `=` on the same input"
        );
        // A capture has no key-precise expression, so the numeric form has
        // nothing to compare.
        assert_eq!(
            parsed_numeric_filter("a", CompareOp::Gte, 500.0, &re),
            Err(ParsedFilterRefusal::NoKeyExpression)
        );
        assert_eq!(
            parsed_numeric_filter("status", CompareOp::Gte, 500.0, &logfmt_parser()),
            Err(ParsedFilterRefusal::NoKeyExpression)
        );
        // A non-finite threshold has no literal the database accepts.
        for t in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            assert_eq!(
                parsed_numeric_filter("status", CompareOp::Gt, t, &json),
                Err(ParsedFilterRefusal::ThresholdNotFinite)
            );
        }
        // A name a fragment may not render.
        for bad in ["1abc", "a-b", "", "a b"] {
            assert_eq!(
                parsed_string_filter(bad, MatchOp::Eq, "x", &json),
                Err(ParsedFilterRefusal::NameNotRenderable),
                "{bad:?}"
            );
        }
        // The controls: the refusals must not be reachable from an
        // ordinary filter.
        assert!(parsed_string_filter("level", MatchOp::Eq, "error", &json).is_ok());
        assert!(parsed_numeric_filter("status", CompareOp::Gte, 500.0, &json).is_ok());
    }

    /// W3 (issue #507), condition 6 — the metadata column is named in one
    /// private constant, and the two public ways to name it both render
    /// the bare column.
    #[test]
    fn the_metadata_column_is_named_in_one_place() {
        assert_eq!(MetadataTerm::Project.as_sql(), "structured_metadata");
        assert_eq!(MetadataTerm::Group.as_sql(), "structured_metadata");
        assert_eq!(
            metadata_non_empty_guard().as_sql(),
            "structured_metadata != ''"
        );
    }
}
