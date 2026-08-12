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

use pulsus_logql::{LineFilter, LineFilterOp};

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
/// use pulsus_logql::{LineFilter, LineFilterOp};
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
/// render `hasToken` prefilter(s) ANDed with the exact predicate. Negative
/// ops (`!=`, `!~`) wrap the *same* compound predicate in `NOT (...)` rather
/// than negating only the exact predicate: `hasToken` never has false
/// negatives (a bloom filter can only ever say "maybe present" or
/// "definitely absent"), so `hasToken(...) AND exact(...)` is exactly
/// equivalent to `exact(...)` alone — `NOT (hasToken(...) AND exact(...))` is
/// therefore provably equivalent to `NOT exact(...)`, the correct exclusion
/// semantic, while still surfacing the prefilter for ClickHouse's optimizer
/// to exploit where it can (architect plan: "Prefilter is always paired with
/// the exact predicate").
///
/// An `or` group (M8-LQ2 `linefilter.or`) is a disjunction of the same
/// per-alternative compound predicate: `((a) OR (b) …)` for positive ops,
/// `NOT ((a) OR (b) …)` for negative ops (each disjunct's `hasToken`
/// prefilter is preserved, so the `tokenbf_v1` skip index still prunes). A
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

/// ClickHouse's `tokenbf_v1` splits on non-alphanumeric ASCII; a `hasToken`
/// prefilter must extract tokens the same way or it misses granules that
/// truly contain the phrase.
fn tokenize(literal: &str) -> Vec<String> {
    literal
        .split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

const REGEX_METACHARS: &[char] = &[
    '.', '^', '$', '*', '+', '?', '(', ')', '[', ']', '{', '}', '|', '\\',
];

/// Conservative, safe-by-construction heuristic: a pattern with zero regex
/// metacharacters is a plain literal, so its tokens can seed a `hasToken`
/// prefilter exactly like a `|=` phrase. Anything else skips the prefilter
/// (never wrong, just less pruning) rather than attempting regex analysis
/// (out of scope — see the AST's own "regex not validated" contract).
fn is_plain_literal(pattern: &str) -> bool {
    !pattern.chars().any(|c| REGEX_METACHARS.contains(&c))
}

fn contains_predicate(phrase: &str) -> String {
    let mut parts: Vec<String> = tokenize(phrase)
        .iter()
        .map(|t| format!("hasToken(body, {})", ch_string(t)))
        .collect();
    parts.push(format!("position(body, {}) > 0", ch_string(phrase)));
    parts.join(" AND ")
}

fn regex_predicate(pattern: &str) -> Result<String, PipelineError> {
    let mut parts: Vec<String> = Vec::new();
    if is_plain_literal(pattern) {
        parts.extend(
            tokenize(pattern)
                .iter()
                .map(|t| format!("hasToken(body, {})", ch_string(t))),
        );
    }
    parts.push(format!(
        "match(body, {})",
        ch_regex_unanchored_checked(pattern)?
    ));
    Ok(parts.join(" AND "))
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

    // Both moved verbatim from `plan.rs`'s `mod tests` with the functions
    // they cover (issue #286), plus one added assertion over the whole
    // `REGEX_METACHARS` set.
    #[test]
    fn tokenize_splits_on_non_alphanumeric_boundaries() {
        assert_eq!(
            tokenize("connection refused"),
            vec!["connection".to_string(), "refused".to_string()]
        );
        assert_eq!(tokenize("a_b-c"), vec!["a_b".to_string(), "c".to_string()]);
    }

    #[test]
    fn is_plain_literal_rejects_regex_metacharacters() {
        assert!(is_plain_literal("connection refused"));
        assert!(!is_plain_literal("test.*"));
        for meta in REGEX_METACHARS {
            assert!(
                !is_plain_literal(&format!("a{meta}b")),
                "metachar {meta:?} must disqualify the hasToken prefilter"
            );
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
    fn a_regex_line_filter_pairs_the_token_prefilter_with_the_exact_predicate() {
        assert_eq!(
            line_filter(&regex_filter("boom"))
                .expect("`boom` compiles")
                .as_sql(),
            "hasToken(body, 'boom') AND match(body, 'boom')"
        );
    }
}
