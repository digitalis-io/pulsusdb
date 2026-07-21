//! PromQL evaluation annotations — informational/warning messages returned
//! alongside a query result (the Prometheus API's `warnings`/`infos`
//! response arrays), ported from `util/annotations/annotations.go` (pinned
//! v3.13.0, `40af9c2`, via `git show 40af9c2:util/annotations/annotations.go`).
//!
//! **Simplification vs. the pin (documented, KISS; scope narrowed by the
//! `#124` codex review, finding 2):** upstream annotation messages carry a
//! query-source-text-derived position suffix (`(annoErr).Error()`,
//! `annotations.go:194-199`: `"%s (%s)"` with `PositionRange.
//! StartPosInput`), appended only at `AsStrings` render time via
//! `SetQuery` — `pulsus-promql`'s parser/planner tracks no position/span
//! information at all (grep-verified: no `PositionRange` type anywhere in
//! this crate), so this port never had a position to carry and omits
//! **only that suffix** (issue #128 tracks the retrofit). This does not
//! change DEDUP cardinality: upstream's dedup key (`Annotations.Add`,
//! `annotations.go:41`) is `err.Error()` computed at `Add` time, which is
//! *before* any query is set (`SetQuery` runs only inside `AsStrings`,
//! after every `Add` for the evaluation is done) — so upstream itself
//! already dedups on the pre-suffix message. Every other message BODY
//! constructor (the fixed wording, `maybeAddMetricName`'s `for metric name
//! "..."` suffix, `%g`-formatted floats via [`go_float`], and the
//! forced-monotonicity detail below) matches the pin verbatim.
//!
//! **Forced-monotonicity detail — structured, cross-firing-merged, and
//! rendered unconditionally.** Upstream's
//! `histogramQuantileForcedMonotonicityErr` carries a structured payload
//! (timestamp span, forced-bucket-bound span, max clamped diff, merge
//! counter) that (a) MERGES on every same-base-message repeat — both
//! `Add` (`annotations.go:46-51`) and `Annotations.Merge`
//! (`:66-71`, the path `rangeEval`'s per-step `warnings.Merge(ws)` takes,
//! `engine.go:1523-1525`) run `annoError.Merge`, so a range query that
//! forces monotonicity at several steps produces ONE widened info per
//! metric name, `over N samples from <minTs> to <maxTs>` — and (b) is
//! rendered only at `AsStrings` time off the merged object. This port
//! mirrors both: [`ForcedMonotonicityDetail`] is the payload,
//! [`Annotations`] merges it on key (base-message) collision, and
//! [`Annotations::as_strings`] renders the suffix. The ONE divergence:
//! upstream gates the rendering behind the same `e.Query == ""` check as
//! the position suffix (`annotations.go:333-341` — its own corpus
//! comparison, query unset, sees the base message only; the HTTP wire,
//! query set, sees the detail), while this port has no query-gated
//! rendering mode built into [`Self::as_strings`] and includes the
//! detail on every call — per the `#124` review adjudication the detail
//! is wire-visible message BODY content, and pulsus's only `as_strings`
//! consumer IS the HTTP wire. Issue #124 (M7-A6) landed the promqltest
//! `expect warn`/`expect info` harness, which needs the query-UNSET
//! comparison instead — [`Self::base_messages`] is that second accessor
//! (base message only, no cap, no detail), used exclusively by the
//! corpus runner; `as_strings`/the HTTP wire are untouched.
//!
//! **Where the position omission is and is not observable (verified at
//! the pin):**
//! - The promqltest corpus's `expect warn`/`expect info` comparisons read
//!   `err.Error()` with NO query ever set (`promql/promqltest/test.go:
//!   1223-1226` — `checkAnnotations` iterates the raw `Annotations` map;
//!   `SetQuery` never runs there), i.e. the **base message without any
//!   position suffix** — byte-identical (module note above aside) to what
//!   [`Self::base_messages`] emits, so the A6 corpus comparison needs no
//!   position tracking.
//! - The HTTP API envelope (`web/api/v1/api.go` `AsStrings(query, 10, 10)`)
//!   DOES set the query, so upstream's wire `warnings`/`infos` strings end
//!   in ` (<line>:<col>)` — pulsus's wire strings omit exactly that
//!   render-time tail. This is the one remaining place output differs
//!   from upstream; acceptable for A5b-i (no position machinery exists to
//!   thread) — see issue #128 for the retrofit scope.

use std::collections::HashMap;

/// Which of the two Prometheus API arrays (`warnings`/`infos`) an
/// annotation belongs to — mirrors upstream's `errors.Is(err, PromQLInfo)`
/// dispatch in `Annotations.AsStrings` (`annotations.go:103`): every
/// constructor is either a `…Warning` or a `…Info` kind, fixed at
/// construction (not query-content-dependent).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnotationKind {
    Warning,
    Info,
}

/// The structured payload of a forced-monotonicity info — the port of
/// `histogramQuantileForcedMonotonicityErr`'s non-message fields
/// (`annotations.go:319-326`, pinned `40af9c2`): the occurrence
/// timestamp span, the forced bucket-bound span, the largest clamped
/// decrease, and the pin's merge counter (`count` starts at 0 per
/// construction; [`Self::merge_from`] accumulates `o.count += e.count + 1`
/// exactly like the pin's `Merge`, so `count + 1` = total firings — the
/// `%d samples` the rendering shows).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ForcedMonotonicityDetail {
    pub min_ts_ms: i64,
    pub max_ts_ms: i64,
    pub min_bucket: f64,
    pub max_bucket: f64,
    pub max_diff: f64,
    pub count: u64,
}

impl ForcedMonotonicityDetail {
    /// One construction's payload (`NewHistogramQuantileForcedMonotonicityInfo`,
    /// `annotations.go:379-390`: `minTs = maxTs = ts`, `count` zero).
    pub fn single(ts_ms: i64, min_bucket: f64, max_bucket: f64, max_diff: f64) -> Self {
        Self {
            min_ts_ms: ts_ms,
            max_ts_ms: ts_ms,
            min_bucket,
            max_bucket,
            max_diff,
            count: 0,
        }
    }

    /// Folds another occurrence (`e`, the newer) into `self` (`o`, the
    /// accumulated) — the exact widening of the pin's
    /// `histogramQuantileForcedMonotonicityErr.Merge`
    /// (`annotations.go:353-376`): min/max the timestamp and bucket
    /// spans, max the diff, `o.count += e.count + 1`.
    fn merge_from(&mut self, e: &ForcedMonotonicityDetail) {
        if e.min_ts_ms < self.min_ts_ms {
            self.min_ts_ms = e.min_ts_ms;
        }
        if e.max_ts_ms > self.max_ts_ms {
            self.max_ts_ms = e.max_ts_ms;
        }
        if e.min_bucket < self.min_bucket {
            self.min_bucket = e.min_bucket;
        }
        if e.max_bucket > self.max_bucket {
            self.max_bucket = e.max_bucket;
        }
        if e.max_diff > self.max_diff {
            self.max_diff = e.max_diff;
        }
        self.count += e.count + 1;
    }

    /// The rendered detail suffix — the query-set arm of the pin's
    /// `histogramQuantileForcedMonotonicityErr.Error()`
    /// (`annotations.go:333-341`), minus the trailing position (issue
    /// #128): `", from buckets %g to %g, with a max diff of %.2g, over %d
    /// samples from %s to %s"`, with `%d` = `count + 1` and the
    /// timestamps as `time.Unix(ts/1000, 0).UTC().Format(time.RFC3339)`
    /// (Go `/` truncates toward zero — mirrored by Rust integer `/`).
    fn render_suffix(&self) -> String {
        let start = crate::eval::datetime::rfc3339_utc_seconds(self.min_ts_ms / 1000);
        let end = crate::eval::datetime::rfc3339_utc_seconds(self.max_ts_ms / 1000);
        format!(
            ", from buckets {} to {}, with a max diff of {}, over {} samples from {start} to {end}",
            go_float::format_g(self.min_bucket),
            go_float::format_g(self.max_bucket),
            go_float::format_g_precision(self.max_diff, 2),
            self.count + 1,
        )
    }
}

/// One deduplicated annotation: the BASE message (upstream's `Add`-time
/// dedup key, always query-independent) plus, for the forced-monotonicity
/// info only, the structured detail whose rendering
/// [`Annotations::as_strings`] appends — mirroring upstream, where the
/// map value is the (merged) error object and the detail string is
/// produced only at `AsStrings` render time.
#[derive(Debug, Clone, PartialEq)]
pub struct Annotation {
    pub kind: AnnotationKind,
    pub message: String,
    pub detail: Option<ForcedMonotonicityDetail>,
}

/// The accumulated warnings/infos for one evaluation — mirrors upstream
/// `Annotations` (`map[string]error`, deduplicated by message,
/// `annotations.go:29`). Insertion-ordered (a deterministic superset of
/// upstream's own Go-map-iteration-order-independent contract — see
/// [`Self::as_strings`]'s doc). A repeated add of a detail-carrying
/// annotation MERGES the structured payloads (upstream `Add`/`Merge` both
/// run `annoError.Merge` on a key collision, `annotations.go:46-51`/
/// `:66-71`) — this is how a range query's per-step forced-monotonicity
/// firings coalesce into ONE widened info, exactly like the pin's
/// `rangeEval` per-step `warnings.Merge(ws)` (`engine.go:1523-1525`).
#[derive(Debug, Clone, Default)]
pub struct Annotations {
    index: HashMap<String, usize>,
    items: Vec<Annotation>,
}

impl Annotations {
    pub fn new() -> Self {
        Self::default()
    }

    fn add_item(
        &mut self,
        kind: AnnotationKind,
        message: String,
        detail: Option<ForcedMonotonicityDetail>,
    ) {
        if let Some(&i) = self.index.get(&message) {
            // Key collision — upstream `Add` (`annotations.go:42-53`):
            // a type-specific `Merge` for the detail-carrying kind,
            // an identical-content overwrite (observable no-op) for
            // every plain message.
            if let (Some(prev), Some(new)) = (self.items[i].detail.as_mut(), detail.as_ref()) {
                prev.merge_from(new);
            }
            return;
        }
        self.index.insert(message.clone(), self.items.len());
        self.items.push(Annotation {
            kind,
            message,
            detail,
        });
    }

    /// Adds `message` under `kind`, deduplicated by the exact message text
    /// — mirrors upstream `Annotations.Add`'s idempotent `map[string]error`
    /// insert (`annotations.go:41`).
    pub fn add(&mut self, kind: AnnotationKind, message: impl Into<String>) {
        self.add_item(kind, message.into(), None);
    }

    pub fn warning(&mut self, message: impl Into<String>) {
        self.add(AnnotationKind::Warning, message);
    }

    pub fn info(&mut self, message: impl Into<String>) {
        self.add(AnnotationKind::Info, message);
    }

    /// Adds one forced-monotonicity firing (an **Info**, upstream
    /// `NewHistogramQuantileForcedMonotonicityInfo`): `base` is the
    /// query-independent message (the dedup key), `detail` the
    /// occurrence's payload. Repeat firings with the same `base` (same
    /// metric name — other steps of a range query, other bucket groups)
    /// merge per the pin (see [`ForcedMonotonicityDetail::merge_from`]).
    pub fn forced_monotonicity_info(
        &mut self,
        base: impl Into<String>,
        detail: ForcedMonotonicityDetail,
    ) {
        self.add_item(AnnotationKind::Info, base.into(), Some(detail));
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Merges `other`'s items into `self`, preserving dedup and merging
    /// detail payloads on key collision — upstream `Annotations.Merge`
    /// (`annotations.go:58-75`), which applies the same `annoError.Merge`
    /// as `Add`.
    pub fn merge(&mut self, other: Annotations) {
        for item in other.items {
            self.add_item(item.kind, item.message, item.detail);
        }
    }

    /// Splits into `(warnings, infos)` of the BASE (query-unset) message
    /// only — no cap, no detail-suffix rendering. Issue #124 (M7-A6): this
    /// is what the promqltest corpus driver checks against, mirroring
    /// upstream's `checkAnnotations` reading raw `err.Error()` with
    /// `SetQuery` never called (this module's own doc — the corpus
    /// comparison sees neither the position suffix nor the
    /// forced-monotonicity detail, unlike [`Self::as_strings`], the
    /// HTTP-wire renderer, which unconditionally appends the detail).
    pub fn base_messages(&self) -> (Vec<String>, Vec<String>) {
        let mut warnings = Vec::new();
        let mut infos = Vec::new();
        for item in &self.items {
            match item.kind {
                AnnotationKind::Info => infos.push(item.message.clone()),
                AnnotationKind::Warning => warnings.push(item.message.clone()),
            }
        }
        (warnings, infos)
    }

    /// Splits into `(warnings, infos)` with upstream's exact `AsStrings`
    /// cap/overflow contract (`annotations.go:88-123`): at most
    /// `max_warnings` warnings / `max_infos` infos are kept (post-dedup),
    /// plus one trailing `"{n} more warning/info annotations omitted"`
    /// line appended *only* when the respective remainder is non-zero — no
    /// pluralization variance, matching the pin's fixed-format
    /// `fmt.Sprintf` calls (`annotations.go:118,121`) verbatim. `0` means
    /// "no limit" (upstream's own `maxWarnings == 0` convention,
    /// `annotations.go:101`).
    ///
    /// Ordering: upstream iterates a Go map (nondeterministic) — the caps
    /// are the byte-relevant contract, not order (plan v3 finding 4). This
    /// port pins the deterministic insertion (first-`Add`) order instead, a
    /// stricter, still-conformant choice.
    pub fn as_strings(&self, max_warnings: usize, max_infos: usize) -> (Vec<String>, Vec<String>) {
        let mut warnings = Vec::new();
        let mut infos = Vec::new();
        let mut warn_skipped = 0usize;
        let mut info_skipped = 0usize;
        for item in &self.items {
            // The wire string = base message + (for forced-monotonicity)
            // the merged detail suffix — upstream renders the detail only
            // here, at `AsStrings` time, off the merged error object.
            let rendered = match &item.detail {
                Some(d) => format!("{}{}", item.message, d.render_suffix()),
                None => item.message.clone(),
            };
            match item.kind {
                AnnotationKind::Info => {
                    if max_infos == 0 || infos.len() < max_infos {
                        infos.push(rendered);
                    } else {
                        info_skipped += 1;
                    }
                }
                AnnotationKind::Warning => {
                    if max_warnings == 0 || warnings.len() < max_warnings {
                        warnings.push(rendered);
                    } else {
                        warn_skipped += 1;
                    }
                }
            }
        }
        if warn_skipped > 0 {
            warnings.push(format!("{warn_skipped} more warning annotations omitted"));
        }
        if info_skipped > 0 {
            infos.push(format!("{info_skipped} more info annotations omitted"));
        }
        (warnings, infos)
    }
}

/// Go `%g`/`%.Ng`-equivalent float formatting for annotation message
/// bodies (`#124` review finding 3): every pinned constructor that embeds
/// a float uses Go's `fmt.Sprintf("%g", …)`/`"%.2g"` — `strconv`'s
/// shortest-round-trip (or fixed-significant-digit) decimal digits, with
/// `+Inf`/`-Inf`/`NaN` spellings and a scientific-vs-fixed threshold
/// neither Rust's `Display` (`{}` never emits `+Inf`/scientific notation)
/// nor its `LowerExp` (`{:e}`, always scientific) reproduce alone.
pub(crate) mod go_float {
    /// `fmt.Sprintf("%g", v)` — shortest round-trip decimal.
    pub(crate) fn format_g(v: f64) -> String {
        format(v, None)
    }

    /// `fmt.Sprintf("%.<prec>g", v)` — `prec` significant digits, trailing
    /// zeros trimmed.
    pub(crate) fn format_g_precision(v: f64, prec: usize) -> String {
        format(v, Some(prec))
    }

    /// Ported from Go's `strconv` `ftoa.go` (`'g'`/`'G'` case, stdlib
    /// vendored via the pinned toolchain — the same conformance rule as
    /// every other Prometheus port in this crate): digit generation via
    /// [`shortest_digits`]/Rust's exact fixed-precision `{:.Ne}` (see the
    /// former's doc for the exactness argument), then Go's exact
    /// scientific-vs-fixed layout — `exp < -4 || exp >= eprec`, where
    /// `eprec` is `6` for the shortest (`prec == -1`) case (`ftoa.go:
    /// 216-218`: "if precision was the shortest possible, use precision 6
    /// for this decision" — NOT 21; verified against the toolchain:
    /// `strconv.FormatFloat(1e6, 'g', -1, 64) == "1e+06"` while `999999.5
    /// -> "999999.5"`) and otherwise the requested precision, reduced to
    /// the actual (trimmed) digit count when that count is itself an
    /// integer needing no fraction (`formatDigits`, `ftoa.go:201-234`) —
    /// and Go's `+Inf`/`-Inf`/`NaN` spelling (`genericFtoa`'s `Inf`/`NaN`
    /// branch).
    ///
    /// Differential-verified against the Go toolchain with ZERO
    /// divergences over 600k+ values — uniform-random `f64` bit patterns,
    /// random subnormals, every power of ten `1e-330..=1e308`, ±ulp
    /// neighborhoods of the `1e-5`/`1e-4`/`1e5`/`1e6`/`1e20`/`1e21`
    /// layout thresholds, `±0`/`±Inf`/`NaN`/`MAX`/`MIN_POSITIVE`/
    /// `5e-324`, and fixed-precision tie cases — for `format_g` and
    /// `format_g_precision(_, 1|2|5)`.
    fn format(v: f64, prec: Option<usize>) -> String {
        if v.is_nan() {
            return "NaN".to_string();
        }
        if v.is_infinite() {
            return if v > 0.0 {
                "+Inf".to_string()
            } else {
                "-Inf".to_string()
            };
        }
        let negative = v.is_sign_negative();
        if v == 0.0 {
            return if negative {
                "-0".to_string()
            } else {
                "0".to_string()
            };
        }
        let abs = v.abs();
        let sci = match prec {
            None => shortest_digits(abs),
            Some(p) => format!("{:.*e}", p.saturating_sub(1), abs),
        };
        let (mantissa, exp_str) = sci
            .split_once('e')
            .expect("Rust's LowerExp always emits an 'e'");
        let exp: i32 = exp_str
            .parse()
            .expect("Rust's LowerExp exponent is always a valid integer");
        let mut digits: String = mantissa.chars().filter(|c| *c != '.').collect();
        if prec.is_some() {
            // Fixed-precision mode pads to the exact digit count; `%g`
            // trims the trailing zeros `%e`/`%f` would keep.
            while digits.len() > 1 && digits.ends_with('0') {
                digits.pop();
            }
        }
        let nd = digits.len() as i32;
        let dp = exp + 1;
        let eprec = match prec {
            None => 6,
            Some(p) => {
                let mut eprec = p as i32;
                if eprec > nd && nd >= dp {
                    eprec = nd;
                }
                eprec
            }
        };
        let mut out = String::new();
        if exp < -4 || exp >= eprec {
            out.push(digits.as_bytes()[0] as char);
            if nd > 1 {
                out.push('.');
                out.push_str(&digits[1..]);
            }
            out.push('e');
            out.push(if exp >= 0 { '+' } else { '-' });
            out.push_str(&format!("{:02}", exp.abs()));
        } else if dp <= 0 {
            out.push_str("0.");
            out.push_str(&"0".repeat((-dp) as usize));
            out.push_str(&digits);
        } else if dp >= nd {
            out.push_str(&digits);
            out.push_str(&"0".repeat((dp - nd) as usize));
        } else {
            out.push_str(&digits[..dp as usize]);
            out.push('.');
            out.push_str(&digits[dp as usize..]);
        }
        if negative { format!("-{out}") } else { out }
    }

    /// The shortest round-trip decimal digits of `abs` (positive, finite,
    /// non-zero), in `{:e}` shape (`d[.ddd]e<exp>`), matching Go
    /// `strconv.FormatFloat(abs, 'e'|'g', -1, 64)`'s digit choice exactly.
    ///
    /// Rust's `{:e}` (`Display`-family shortest mode) guarantees the
    /// MINIMAL digit count that round-trips, but when two same-length
    /// candidates both round-trip it does not always pick the one closest
    /// to the true binary value — Go's Ryū does (nearest, ties-to-even on
    /// the exact value). Observed on real inputs (e.g. the f64 nearest
    /// `-887777373534812.2`: Rust `…123e14`, Go `…122e14` — both
    /// round-trip; Go's is nearer). Correction: take Rust's digit COUNT
    /// `n`, re-round `abs` to exactly `n` significant digits with Rust's
    /// exact fixed-precision `{:.n-1e}` (correctly rounded to nearest,
    /// ties-to-even, via the exact fallback path — the same rounding rule
    /// Go's Ryū applies), trim any trailing zeros the re-round exposes
    /// (a carry like `9.99e2 -> 1.00e3` trims to `1e3`; a non-carry
    /// trailing zero would contradict `n`'s minimality), and keep the
    /// result only if it still round-trips to `abs`. The fallback to
    /// Rust's original digits is provably Go's pick in the only case the
    /// re-round can fail round-trip: the rounding interval of `abs` is
    /// asymmetric (binade boundary), at most two same-length candidates
    /// lie inside it, and Go selects the in-interval candidate nearest
    /// the true value — if the globally-nearest (the re-round) is
    /// outside the interval, the only in-interval candidate left IS
    /// Rust's round-tripping original.
    fn shortest_digits(abs: f64) -> String {
        let rust_shortest = format!("{abs:e}");
        let (mantissa, _) = rust_shortest
            .split_once('e')
            .expect("Rust's LowerExp always emits an 'e'");
        let n = mantissa.chars().filter(char::is_ascii_digit).count();
        let refit = format!("{:.*e}", n - 1, abs);
        let (rm, re) = refit
            .split_once('e')
            .expect("Rust's LowerExp always emits an 'e'");
        let rexp: i32 = re
            .parse()
            .expect("Rust's LowerExp exponent is always a valid integer");
        let mut rdigits: String = rm.chars().filter(char::is_ascii_digit).collect();
        while rdigits.len() > 1 && rdigits.ends_with('0') {
            rdigits.pop();
        }
        // Round-trip guard: value = rdigits (as an integer) x 10^(rexp-k).
        let k = rdigits.len() as i32 - 1;
        let candidate = format!("{rdigits}e{}", rexp - k);
        if candidate.parse::<f64>() == Ok(abs) {
            if rdigits.len() == 1 {
                format!("{rdigits}e{rexp}")
            } else {
                format!("{}.{}e{rexp}", &rdigits[..1], &rdigits[1..])
            }
        } else {
            rust_shortest
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn format_g_special_values() {
            assert_eq!(format_g(f64::NAN), "NaN");
            assert_eq!(format_g(f64::INFINITY), "+Inf");
            assert_eq!(format_g(f64::NEG_INFINITY), "-Inf");
            assert_eq!(format_g(0.0), "0");
            assert_eq!(format_g(-0.0), "-0");
        }

        #[test]
        fn format_g_normal_values_match_the_go_pin() {
            assert_eq!(format_g(1.5), "1.5");
            assert_eq!(format_g(100.0), "100");
            assert_eq!(format_g(0.0001), "0.0001");
            assert_eq!(format_g(0.00001), "1e-05");
            assert_eq!(format_g(1_000_000.0), "1e+06");
            assert_eq!(format_g(123_456.789), "123456.789");
            assert_eq!(format_g(-42.0), "-42");
        }

        #[test]
        fn format_g_precision_rounds_and_trims_trailing_zeros() {
            assert_eq!(format_g_precision(100.0, 2), "1e+02");
            assert_eq!(format_g_precision(0.005, 2), "0.005");
            assert_eq!(format_g_precision(2.675, 2), "2.7");
            assert_eq!(format_g_precision(f64::INFINITY, 2), "+Inf");
            assert_eq!(format_g_precision(f64::NAN, 2), "NaN");
        }

        /// `#124` round-2 blocker A regression: Rust's raw `{:e}` shortest
        /// picks `…123e14` here (round-trips, but is not the nearest
        /// same-length candidate); Go's Ryū — and therefore this module —
        /// must pick `…122e14`. Also pins the shortest-mode layout
        /// threshold at decimal exponent 6 (`ftoa.go`'s "use precision 6
        /// for this decision"), verified against the Go toolchain: `1e6`
        /// is e-form while a 7-digit exp-5 value stays fixed-form.
        #[test]
        fn format_g_matches_ryu_nearest_digit_choice_and_the_exp_6_layout_threshold() {
            let v: f64 = -887777373534812.2;
            assert_eq!(
                format!("{:e}", v),
                "-8.877773735348123e14",
                "precondition: Rust's raw shortest diverges (if this fails, Rust changed its picker and the correction below is vacuous)"
            );
            assert_eq!(format_g(v), "-8.877773735348122e+14");
            // Layout threshold: exp >= 6 switches to e-form in shortest mode.
            assert_eq!(format_g(999999.5), "999999.5");
            assert_eq!(format_g(1e6), "1e+06");
            assert_eq!(format_g(100000.0), "100000");
            // Denormal extreme.
            assert_eq!(format_g(5e-324), "5e-324");
        }
    }
}

/// Annotation message constructors — one function per upstream
/// `annotations.New*` this crate populates (A5b-i's subset; A5b-ii/iii add
/// theirs alongside). Each returns the message text only; callers choose
/// [`Annotations::warning`]/[`Annotations::info`] per the constructor's
/// fixed kind (documented on each function).
pub mod messages {
    /// Upstream `maybeAddMetricName` (`annotations.go:214`): appends `for
    /// metric name "<name>"` when `metric_name` is non-empty, verbatim.
    fn maybe_add_metric_name(message: String, metric_name: &str) -> String {
        if metric_name.is_empty() {
            message
        } else {
            format!("{message} for metric name {metric_name:?}")
        }
    }

    /// **Warning.** Upstream `NewInvalidQuantileWarning` (`annotations.go:225`):
    /// `histogram_quantile`'s φ (or `histogram_quantiles`' per-quantile arg)
    /// is outside `[0, 1]` or NaN. `q` renders via Go `%g` (`fmt.Errorf("%w,
    /// got %g", …)`) — `+Inf`/`-Inf`/scientific notation for out-of-range
    /// values, not Rust's bare `Display` (`#124` review finding 3).
    pub fn invalid_quantile_warning(q: f64) -> String {
        format!(
            "PromQL warning: quantile value should be between 0 and 1, got {}",
            super::go_float::format_g(q)
        )
    }

    /// **Warning** (issue #130). Upstream `NewInvalidRatioWarning`
    /// (`annotations.go:234` at the pin, over `InvalidRatioWarning`,
    /// `:150`): `limit_ratio`'s parameter fell outside `[-1, 1]` and was
    /// capped. Both floats render via Go `%g` (`fmt.Errorf("%w, got %g,
    /// capping to %g", …)`). Emitted at most once per extrema side per
    /// query, from the evaluation-wide max/min — see
    /// `eval::flush_ratio_warnings`, never per step.
    pub fn invalid_ratio_warning(given: f64, capped: f64) -> String {
        format!(
            "PromQL warning: ratio value should be between -1 and 1, got {}, capping to {}",
            super::go_float::format_g(given),
            super::go_float::format_g(capped)
        )
    }

    /// **Info.** Upstream `NewNativeHistogramQuantileNaNResultInfo`
    /// (`annotations.go:437`): the histogram has NaN observations and the
    /// requested quantile falls above every bucket, so the result is NaN.
    pub fn native_histogram_quantile_nan_result_info(metric_name: &str) -> String {
        maybe_add_metric_name(
            "PromQL info: input to histogram_quantile has NaN observations, result is NaN"
                .to_string(),
            metric_name,
        )
    }

    /// **Info.** Upstream `NewNativeHistogramQuantileNaNSkewInfo`
    /// (`annotations.go:444`): the histogram has NaN observations and the
    /// quantile fell into an existing bucket, so the result may be skewed
    /// higher (NaNs are treated as +Inf for ranking purposes).
    pub fn native_histogram_quantile_nan_skew_info(metric_name: &str) -> String {
        maybe_add_metric_name(
            "PromQL info: input to histogram_quantile has NaN observations, result is skewed higher"
                .to_string(),
            metric_name,
        )
    }

    /// **Info.** Upstream `NewNativeHistogramFractionNaNsInfo`
    /// (`annotations.go:451`): `histogram_fraction`'s input has NaN
    /// observations, which are excluded from every fraction (so e.g.
    /// `histogram_fraction(-Inf, +Inf, v)` can read less than `1.0`).
    pub fn native_histogram_fraction_nans_info(metric_name: &str) -> String {
        maybe_add_metric_name(
            "PromQL info: input to histogram_fraction has NaN observations, which are excluded from all fractions"
                .to_string(),
            metric_name,
        )
    }

    /// **Info** (BASE message only — the dedup key). Upstream
    /// `NewHistogramQuantileForcedMonotonicityInfo` (`annotations.go:380`):
    /// `histogram_quantile`'s classic-`le` input needed its cumulative
    /// bucket counts forced monotonic (`BucketQuantile`'s
    /// `forcedMonotonic` return, `ensureMonotonicAndIgnoreSmallDeltas`,
    /// `quantile.go`). Callers pair this with a
    /// [`crate::annotations::ForcedMonotonicityDetail`] via
    /// [`crate::annotations::Annotations::forced_monotonicity_info`]; the
    /// `", from buckets … over N samples …"` detail suffix is rendered
    /// (and cross-firing-merged) by `as_strings`, mirroring upstream's
    /// render-at-`AsStrings`-time-off-the-merged-object structure. The
    /// trailing position suffix the pin also renders there is omitted
    /// (module doc, issue #128).
    pub fn histogram_quantile_forced_monotonicity_info(metric_name: &str) -> String {
        maybe_add_metric_name(
            "PromQL info: input to histogram_quantile needed to be fixed for monotonicity (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile)"
                .to_string(),
            metric_name,
        )
    }

    /// **Warning.** Upstream `NewMixedClassicNativeHistogramsWarning`
    /// (`annotations.go:271`): one identity carries BOTH classic `le`
    /// buckets and a native histogram at the same timestamp
    /// (`resetHistograms`' conflict filter, `engine.go:1354-1371`) —
    /// neither is evaluated.
    pub fn mixed_classic_native_histograms_warning(metric_name: &str) -> String {
        maybe_add_metric_name(
            "PromQL warning: vector contains a mix of classic and native histograms".to_string(),
            metric_name,
        )
    }

    /// **Warning.** Upstream `NewBadBucketLabelWarning` (`annotations.go:
    /// 232`): a classic-histogram bucket sample's `le` label is missing
    /// (`label_value == ""`, upstream `labels.Get`'s not-found value) or
    /// fails to parse as a float (`#124` review finding 4 — `resetHistograms`
    /// skips the bucket and warns rather than rejecting the whole query,
    /// `engine.go:1331-1341`).
    pub fn bad_bucket_label_warning(metric_name: &str, label_value: &str) -> String {
        maybe_add_metric_name(
            format!(
                "PromQL warning: bucket label \"le\" is missing or has a malformed value of {label_value:?}"
            ),
            metric_name,
        )
    }

    /// **Warning** (M7-A5b-ii). Upstream `NewMixedFloatsHistogramsWarning`
    /// (`annotations.go:254`): a range-vector function's window (or an
    /// `instantValue`/`irate`/`idelta` last-two-samples pair) contains
    /// BOTH float and histogram samples. Unlike
    /// [`histogram_quantile_forced_monotonicity_info`] and friends, the
    /// pin embeds the metric name unconditionally via raw `%q` (NOT
    /// `maybeAddMetricName`) — always appended, never omitted for an
    /// empty name.
    pub fn mixed_floats_histograms_warning(metric_name: &str) -> String {
        format!(
            "PromQL warning: encountered a mix of histograms and floats for metric name {metric_name:?}"
        )
    }

    /// **Warning** (M7-A5b-ii; hint-conditional since issue #125).
    /// Upstream `NewNativeHistogramNotGaugeWarning` (`annotations.go:286`):
    /// a gauge-expecting function (`delta`/`idelta` — `histogramRate`'s
    /// `!isCounter` arm, `functions.go:695-697`, and `instantValue`'s
    /// `!isRate` arm, `:850-853`) received a native histogram whose
    /// `CounterResetHint` is NOT `GaugeType`. With the hint stored
    /// (migrations 27/28) the callers now apply the pin's exact hint
    /// conditions instead of firing unconditionally. Metric name embedded
    /// via raw `%q` (`fmt.Errorf("%w %q", …)` — always appended, never
    /// omitted for an empty name).
    pub fn native_histogram_not_gauge_warning(metric_name: &str) -> String {
        format!("PromQL warning: this native histogram metric is not a gauge: {metric_name:?}")
    }

    /// **Warning** (issue #125). Upstream
    /// `NewNativeHistogramNotCounterWarning` (`annotations.go:279-286`): a
    /// counter-expecting function (`rate`/`increase` — `histogramRate`'s
    /// `isCounter` arms, `functions.go:618,651` — and `irate`,
    /// `instantValue`'s `isRate` arm, `:847-849`) received a native
    /// histogram whose `CounterResetHint` IS `GaugeType`. Reachable now
    /// that hints are stored/propagated. Metric name embedded via raw `%q`
    /// (always appended, never omitted for an empty name).
    pub fn native_histogram_not_counter_warning(metric_name: &str) -> String {
        format!("PromQL warning: this native histogram metric is not a counter: {metric_name:?}")
    }

    /// **Warning** (issue #125). Upstream
    /// `NewHistogramCounterResetCollisionWarning` (`annotations.go:485-492`
    /// over `HistogramCounterResetCollisionWarning`, `:170`): a
    /// `CounterReset` hint met a `NotCounterReset` hint during a histogram
    /// operation — `+`/`-` binops (`engine.go:3519-3538`, via
    /// `CombineOutcome::counter_reset_collision`) or a `sum`/`avg`
    /// aggregation / `sum_over_time`/`avg_over_time` fold tracking input
    /// hints (`engine.go:3939-3941`, `functions.go:1178-1196`). Renders the
    /// operation word only (`aggregation`/`addition`/`subtraction`) — no
    /// metric name, same as [`mismatched_custom_buckets_histograms_info`].
    pub fn histogram_counter_reset_collision_warning(op: HistogramOperation) -> String {
        format!(
            "PromQL warning: conflicting counter resets during histogram {}",
            op.as_str()
        )
    }

    /// **Warning** (M7-A5b-ii). Upstream
    /// `NewMixedExponentialCustomHistogramsWarning` (`annotations.go:299`):
    /// a `rate`/`increase`/`delta`/`irate`/`idelta` window mixes
    /// exponential-schema and NHCB (custom-buckets) histograms — this
    /// port's `combine` `IncompatibleSchema` outcome. (Mismatched custom
    /// bounds between two NHCB histograms are NOT this warning — they
    /// reconcile, with [`mismatched_custom_buckets_histograms_info`].)
    pub fn mixed_exponential_custom_histograms_warning(metric_name: &str) -> String {
        format!(
            "PromQL warning: vector contains a mix of histograms with exponential and custom buckets schemas for metric name {metric_name:?}"
        )
    }

    /// **Info** (M7-A5b-ii). Upstream `NewHistogramIgnoredInMixedRangeInfo`
    /// (`annotations.go:412`): a `*_over_time`/`quantile_over_time`/`deriv`
    /// window contains BOTH floats and histograms — the histograms are
    /// dropped, the float-only computation proceeds. Fires ONLY on a mixed
    /// window; a histogram-only window is silent (plan v4 residual A,
    /// `functions.go:1461,1464`) — the caller's disposition, not this
    /// constructor's concern. Always embeds the metric name (raw `%q`, not
    /// `maybeAddMetricName` — same convention as the two constructors
    /// above).
    pub fn histogram_ignored_in_mixed_range_info(metric_name: &str) -> String {
        format!(
            "PromQL info: ignored histograms in a range containing both floats and histograms for metric name {metric_name:?}"
        )
    }

    /// Which histogram operation reconciled mismatched NHCB custom bounds
    /// — upstream's `HistogramOperation` (`annotations.go:458-464`): A5b-ii
    /// ported `Add`/`Sub`; A5b-iii adds `Agg` (`sum`/`avg` aggregation AND
    /// `sum_over_time`/`avg_over_time` — both fold through `KahanAdd`,
    /// `engine.go:3608,3673`, `functions.go`'s `aggrHistOverTime` callers —
    /// share the one `HistogramAgg` operation text).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum HistogramOperation {
        Add,
        Sub,
        Agg,
    }

    impl HistogramOperation {
        fn as_str(self) -> &'static str {
            match self {
                HistogramOperation::Add => "addition",
                HistogramOperation::Sub => "subtraction",
                HistogramOperation::Agg => "aggregation",
            }
        }
    }

    /// **Info** (M7-A5b-ii/iii). Upstream
    /// `NewMismatchedCustomBucketsHistogramsInfo` (`annotations.go:486`):
    /// an `Add`/`Sub`/`Agg` between NHCB histograms with MISMATCHED custom
    /// bounds reconciled them to their intersection (`nhcbBoundsReconciled`
    /// — `functions.go:672-674,689-691,863-865`; the aggregation fold sites
    /// — `engine.go:3620,3665,3697`, `funcSumOverTime`/`funcAvgOverTime`'s
    /// `nhcbBoundsReconciledSeen` — all use `HistogramAgg`). No metric-name
    /// suffix — the pin renders only the operation.
    pub fn mismatched_custom_buckets_histograms_info(op: HistogramOperation) -> String {
        format!(
            "PromQL info: mismatched custom buckets were reconciled during {}",
            op.as_str()
        )
    }

    /// **Warning** (M7-A5b-iii). Upstream
    /// `NewMixedFloatsHistogramsAggWarning` (`annotations.go:263-269`): an
    /// aggregation group (`sum`/`avg`) — or a `sum_over_time`/
    /// `avg_over_time` window — mixes float and histogram samples; the
    /// WHOLE group/window is dropped (upstream's own doc: "used when the
    /// queried series includes both float samples and histogram samples in
    /// an aggregation"). Unlike [`mixed_floats_histograms_warning`] (the
    /// per-metric-name range-function variant), this one carries NO
    /// metric-name suffix — `base = MixedFloatsHistogramsWarning` (`"…for"`)
    /// + `" aggregation"` (`annotations.go:265-269`), never `"for metric
    /// name …"`.
    pub fn mixed_floats_histograms_agg_warning() -> String {
        "PromQL warning: encountered a mix of histograms and floats for aggregation".to_string()
    }

    /// **Info** (M7-A5b-iii). Upstream `NewHistogramIgnoredInAggregationInfo`
    /// (`annotations.go:403-410`): `min`/`max`/`stddev`/`stdvar`/`quantile`/
    /// `topk`/`bottomk` skip a histogram sample they cannot handle.
    /// `aggregation` is the aggregation-op name verbatim (e.g. `"min"`,
    /// `"stddev"`, `"topk"`), no metric name.
    pub fn histogram_ignored_in_aggregation_info(aggregation: &str) -> String {
        format!("PromQL info: ignored histogram in {aggregation} aggregation")
    }

    /// **Info** (M7-A5b-iii). Upstream `NewIncompatibleTypesInBinOpInfo`
    /// (`annotations.go:394-401`): a binary operator's operand TYPES don't
    /// support the requested op — a float/histogram or histogram/histogram
    /// combination the op has no defined semantics for (e.g.
    /// `histogram + float`, `histogram * histogram`). `operator` is the
    /// operator's canonical text (`"+"`, `"*"`, `"=="`, …, `BinOp::
    /// item_type_str`); `lhs_type`/`rhs_type` are `"float"`/`"histogram"`.
    /// Sample dropped, not a hard error.
    pub fn incompatible_types_in_binop_info(
        lhs_type: &str,
        operator: &str,
        rhs_type: &str,
    ) -> String {
        format!(
            "PromQL info: incompatible sample types encountered for binary operator {operator:?}: {lhs_type} {operator} {rhs_type}"
        )
    }

    /// **Warning** (M7-A5b-iii). Upstream
    /// `NewIncompatibleBucketLayoutInBinOpWarning` (`annotations.go:421-427`):
    /// `histogram ± histogram` where one operand is exponential-schema and
    /// the other NHCB (`FloatHistogramOpError::IncompatibleSchema` —
    /// distinct from a MISMATCHED-bounds NHCB pair, which reconciles via
    /// [`mismatched_custom_buckets_histograms_info`] instead). `operator`
    /// is the canonical operator text (`"+"`/`"-"`).
    pub fn incompatible_bucket_layout_in_binop_warning(operator: &str) -> String {
        format!(
            "PromQL warning: incompatible bucket layout encountered for binary operator {operator}"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_dedups_by_exact_message_text() {
        let mut a = Annotations::new();
        a.warning("dup");
        a.warning("dup");
        a.info("dup"); // Same text, different kind — upstream dedups by the
        // full `err.Error()`, which for different constructors differs
        // structurally (different kind text), but a hand-built identical
        // string across kinds is a contrived edge case exercised here to
        // pin the observed behavior: kind is not part of the dedup key,
        // only the message string is — the FIRST kind wins.
        let (warnings, infos) = a.as_strings(0, 0);
        assert_eq!(warnings, vec!["dup".to_string()]);
        assert!(infos.is_empty());
    }

    #[test]
    fn as_strings_splits_by_kind() {
        let mut a = Annotations::new();
        a.warning("w1");
        a.info("i1");
        let (warnings, infos) = a.as_strings(0, 0);
        assert_eq!(warnings, vec!["w1".to_string()]);
        assert_eq!(infos, vec!["i1".to_string()]);
    }

    #[test]
    fn as_strings_caps_at_max_and_appends_the_overflow_line() {
        let mut a = Annotations::new();
        for i in 0..11 {
            a.warning(format!("w{i}"));
        }
        let (warnings, _) = a.as_strings(10, 10);
        assert_eq!(warnings.len(), 11, "10 kept + 1 overflow line");
        assert_eq!(warnings[10], "1 more warning annotations omitted");
    }

    #[test]
    fn as_strings_no_overflow_line_when_at_or_under_the_cap() {
        let mut a = Annotations::new();
        for i in 0..10 {
            a.warning(format!("w{i}"));
        }
        let (warnings, _) = a.as_strings(10, 10);
        assert_eq!(warnings.len(), 10, "no overflow line at exactly the cap");
    }

    #[test]
    fn as_strings_zero_limit_means_unlimited() {
        let mut a = Annotations::new();
        for i in 0..15 {
            a.info(format!("i{i}"));
        }
        let (_, infos) = a.as_strings(0, 0);
        assert_eq!(infos.len(), 15);
    }

    #[test]
    fn as_strings_caps_infos_independently_of_warnings() {
        let mut a = Annotations::new();
        for i in 0..11 {
            a.info(format!("i{i}"));
        }
        a.warning("w0");
        let (warnings, infos) = a.as_strings(10, 10);
        assert_eq!(warnings, vec!["w0".to_string()]);
        assert_eq!(infos.len(), 11);
        assert_eq!(infos[10], "1 more info annotations omitted");
    }

    #[test]
    fn merge_preserves_dedup_across_both_sides() {
        let mut a = Annotations::new();
        a.warning("shared");
        let mut b = Annotations::new();
        b.warning("shared");
        b.warning("only-in-b");
        a.merge(b);
        let (warnings, _) = a.as_strings(0, 0);
        assert_eq!(
            warnings,
            vec!["shared".to_string(), "only-in-b".to_string()]
        );
    }

    #[test]
    fn invalid_quantile_warning_names_the_offending_value() {
        assert_eq!(
            messages::invalid_quantile_warning(1.5),
            "PromQL warning: quantile value should be between 0 and 1, got 1.5"
        );
    }

    /// `#124` review finding 3: `%g` spells non-finite quantiles
    /// `+Inf`/`-Inf`/`NaN`, not Rust's bare `Display`.
    #[test]
    fn invalid_quantile_warning_formats_non_finite_values_like_go_g() {
        assert_eq!(
            messages::invalid_quantile_warning(f64::INFINITY),
            "PromQL warning: quantile value should be between 0 and 1, got +Inf"
        );
        assert_eq!(
            messages::invalid_quantile_warning(f64::NEG_INFINITY),
            "PromQL warning: quantile value should be between 0 and 1, got -Inf"
        );
        assert_eq!(
            messages::invalid_quantile_warning(f64::NAN),
            "PromQL warning: quantile value should be between 0 and 1, got NaN"
        );
    }

    /// Issue #130: the exact `limit.test:118/:123` message texts, plus a
    /// non-finite `given` rendered per Go `%g` (`+Inf`, not Rust's `inf`).
    #[test]
    fn invalid_ratio_warning_names_the_given_and_capped_values() {
        assert_eq!(
            messages::invalid_ratio_warning(1.1, 1.0),
            "PromQL warning: ratio value should be between -1 and 1, got 1.1, capping to 1"
        );
        assert_eq!(
            messages::invalid_ratio_warning(-1.1, -1.0),
            "PromQL warning: ratio value should be between -1 and 1, got -1.1, capping to -1"
        );
        assert_eq!(
            messages::invalid_ratio_warning(f64::INFINITY, 1.0),
            "PromQL warning: ratio value should be between -1 and 1, got +Inf, capping to 1"
        );
    }

    #[test]
    fn nan_result_info_omits_the_metric_name_suffix_when_empty() {
        assert_eq!(
            messages::native_histogram_quantile_nan_result_info(""),
            "PromQL info: input to histogram_quantile has NaN observations, result is NaN"
        );
    }

    #[test]
    fn nan_result_info_appends_the_metric_name_suffix_when_present() {
        assert_eq!(
            messages::native_histogram_quantile_nan_result_info("my_metric"),
            "PromQL info: input to histogram_quantile has NaN observations, result is NaN for metric name \"my_metric\""
        );
    }

    #[test]
    fn nan_skew_info_message_text() {
        assert_eq!(
            messages::native_histogram_quantile_nan_skew_info(""),
            "PromQL info: input to histogram_quantile has NaN observations, result is skewed higher"
        );
    }

    #[test]
    fn forced_monotonicity_info_renders_the_base_plus_single_firing_detail() {
        let mut a = Annotations::new();
        a.forced_monotonicity_info(
            messages::histogram_quantile_forced_monotonicity_info(""),
            ForcedMonotonicityDetail::single(0, 0.1, 0.5, 2.0),
        );
        let (_, infos) = a.as_strings(0, 0);
        assert_eq!(
            infos,
            vec![
                "PromQL info: input to histogram_quantile needed to be fixed for monotonicity (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile), from buckets 0.1 to 0.5, with a max diff of 2, over 1 samples from 1970-01-01T00:00:00Z to 1970-01-01T00:00:00Z".to_string()
            ]
        );
        assert!(
            messages::histogram_quantile_forced_monotonicity_info("hist")
                .ends_with(" for metric name \"hist\"")
        );
    }

    /// `#124` review finding 2a: the `%.2g` `max_diff` rendering and a
    /// non-epoch, non-UTC-midnight timestamp.
    #[test]
    fn forced_monotonicity_info_formats_max_diff_with_two_significant_digits() {
        let mut a = Annotations::new();
        a.forced_monotonicity_info(
            messages::histogram_quantile_forced_monotonicity_info(""),
            ForcedMonotonicityDetail::single(1_705_314_645_000, 1.0, 100.0, 123.456),
        );
        let (_, infos) = a.as_strings(0, 0);
        let msg = &infos[0];
        assert!(
            msg.contains("with a max diff of 1.2e+02"),
            "message was: {msg}"
        );
        assert!(
            msg.contains("over 1 samples from 2024-01-15T10:30:45Z to 2024-01-15T10:30:45Z"),
            "message was: {msg}"
        );
    }

    /// `#124` round-2 blocker B: repeat firings with the same base
    /// message (other steps of a range query, other groups) MERGE into
    /// ONE widened info — the port of `histogramQuantileForcedMonotonicityErr
    /// .Merge` (`annotations.go:353-376`) as applied by both `Add`
    /// (`:46-51`) and the cross-step `Annotations.Merge` (`:66-71`).
    #[test]
    fn forced_monotonicity_infos_merge_across_firings_widening_bounds_and_counting() {
        let base = messages::histogram_quantile_forced_monotonicity_info("m");
        let mut a = Annotations::new();
        // Step 1: buckets 0.5..1, diff 2, at t=0.
        a.forced_monotonicity_info(
            base.clone(),
            ForcedMonotonicityDetail::single(0, 0.5, 1.0, 2.0),
        );
        // Step 2: buckets 0.1..0.5, diff 7, at t=60s.
        a.forced_monotonicity_info(
            base.clone(),
            ForcedMonotonicityDetail::single(60_000, 0.1, 0.5, 7.0),
        );
        // Step 3: buckets 1..2, diff 3, at t=120s.
        a.forced_monotonicity_info(
            base.clone(),
            ForcedMonotonicityDetail::single(120_000, 1.0, 2.0, 3.0),
        );
        let (warnings, infos) = a.as_strings(0, 0);
        assert!(warnings.is_empty());
        assert_eq!(infos.len(), 1, "one merged info, not one per firing");
        assert_eq!(
            infos[0],
            format!(
                "{base}, from buckets 0.1 to 2, with a max diff of 7, over 3 samples from 1970-01-01T00:00:00Z to 1970-01-01T00:02:00Z"
            )
        );
    }

    /// The cross-`Annotations` merge path (this port's analogue of the
    /// pin's `rangeEval` per-step `warnings.Merge(ws)`) applies the same
    /// detail widening as a direct repeat add — and the count survives
    /// merging two already-merged sides (`o.count += e.count + 1`).
    #[test]
    fn forced_monotonicity_detail_merges_through_annotations_merge_too() {
        let base = messages::histogram_quantile_forced_monotonicity_info("m");
        let mut a = Annotations::new();
        a.forced_monotonicity_info(
            base.clone(),
            ForcedMonotonicityDetail::single(0, 0.5, 1.0, 2.0),
        );
        a.forced_monotonicity_info(
            base.clone(),
            ForcedMonotonicityDetail::single(30_000, 0.5, 1.0, 2.0),
        );
        let mut b = Annotations::new();
        b.forced_monotonicity_info(
            base.clone(),
            ForcedMonotonicityDetail::single(60_000, 0.25, 1.0, 4.0),
        );
        a.merge(b);
        let (_, infos) = a.as_strings(0, 0);
        assert_eq!(infos.len(), 1);
        assert!(
            infos[0].contains("from buckets 0.25 to 1, with a max diff of 4, over 3 samples from 1970-01-01T00:00:00Z to 1970-01-01T00:01:00Z"),
            "got {infos:?}"
        );
    }

    /// Distinct base messages (different metric names) never merge —
    /// upstream keys the map on `err.Error()`.
    #[test]
    fn forced_monotonicity_infos_for_different_metric_names_stay_separate() {
        let mut a = Annotations::new();
        a.forced_monotonicity_info(
            messages::histogram_quantile_forced_monotonicity_info("m1"),
            ForcedMonotonicityDetail::single(0, 0.5, 1.0, 2.0),
        );
        a.forced_monotonicity_info(
            messages::histogram_quantile_forced_monotonicity_info("m2"),
            ForcedMonotonicityDetail::single(0, 0.5, 1.0, 2.0),
        );
        let (_, infos) = a.as_strings(0, 0);
        assert_eq!(infos.len(), 2);
    }

    #[test]
    fn mixed_classic_native_warning_message_text() {
        assert_eq!(
            messages::mixed_classic_native_histograms_warning("m"),
            "PromQL warning: vector contains a mix of classic and native histograms for metric name \"m\""
        );
    }

    #[test]
    fn fraction_nans_info_message_text() {
        assert_eq!(
            messages::native_histogram_fraction_nans_info(""),
            "PromQL info: input to histogram_fraction has NaN observations, which are excluded from all fractions"
        );
    }

    /// `#124` review finding 4: `NewBadBucketLabelWarning`'s text —
    /// `label_value` is the raw (possibly empty, for a missing label)
    /// `le` value, rendered `%q`-equivalent.
    #[test]
    fn bad_bucket_label_warning_message_text_with_and_without_metric_name() {
        assert_eq!(
            messages::bad_bucket_label_warning("", "notanumber"),
            "PromQL warning: bucket label \"le\" is missing or has a malformed value of \"notanumber\""
        );
        assert_eq!(
            messages::bad_bucket_label_warning("my_metric", ""),
            "PromQL warning: bucket label \"le\" is missing or has a malformed value of \"\" for metric name \"my_metric\""
        );
    }

    // -- M7-A5b-ii annotation message text --

    #[test]
    fn mixed_floats_histograms_warning_message_text() {
        assert_eq!(
            messages::mixed_floats_histograms_warning("m"),
            "PromQL warning: encountered a mix of histograms and floats for metric name \"m\""
        );
        // Always embedded, even when empty (unlike maybeAddMetricName).
        assert_eq!(
            messages::mixed_floats_histograms_warning(""),
            "PromQL warning: encountered a mix of histograms and floats for metric name \"\""
        );
    }

    /// The `native_histograms.test` corpus asserts this exact text
    /// (`expect warn msg: PromQL warning: this native histogram metric is
    /// not a gauge: "nhcb_metric"`, `:1323`).
    #[test]
    fn native_histogram_not_gauge_warning_message_text() {
        assert_eq!(
            messages::native_histogram_not_gauge_warning("nhcb_metric"),
            "PromQL warning: this native histogram metric is not a gauge: \"nhcb_metric\""
        );
        // Raw %q, always appended — an empty name renders as "".
        assert_eq!(
            messages::native_histogram_not_gauge_warning(""),
            "PromQL warning: this native histogram metric is not a gauge: \"\""
        );
    }

    #[test]
    fn mixed_exponential_custom_histograms_warning_message_text() {
        assert_eq!(
            messages::mixed_exponential_custom_histograms_warning("m"),
            "PromQL warning: vector contains a mix of histograms with exponential and custom buckets schemas for metric name \"m\""
        );
    }

    #[test]
    fn histogram_ignored_in_mixed_range_info_message_text() {
        assert_eq!(
            messages::histogram_ignored_in_mixed_range_info("m"),
            "PromQL info: ignored histograms in a range containing both floats and histograms for metric name \"m\""
        );
    }

    /// The `native_histograms.test` corpus asserts this exact text
    /// (`expect info msg: PromQL info: mismatched custom buckets were
    /// reconciled during subtraction`, `:1369`).
    #[test]
    fn mismatched_custom_buckets_histograms_info_message_text() {
        assert_eq!(
            messages::mismatched_custom_buckets_histograms_info(messages::HistogramOperation::Sub),
            "PromQL info: mismatched custom buckets were reconciled during subtraction"
        );
        assert_eq!(
            messages::mismatched_custom_buckets_histograms_info(messages::HistogramOperation::Add),
            "PromQL info: mismatched custom buckets were reconciled during addition"
        );
        assert_eq!(
            messages::mismatched_custom_buckets_histograms_info(messages::HistogramOperation::Agg),
            "PromQL info: mismatched custom buckets were reconciled during aggregation",
            "native_histograms.test:1300's sum_over_time(nhcb_metric[13m]) expect info"
        );
    }

    /// Issue #125: the `native_histograms.test` corpus asserts this exact
    /// text (`expect warn msg: PromQL warning: this native histogram
    /// metric is not a counter: "some_metric"`, `:1095`).
    #[test]
    fn native_histogram_not_counter_warning_message_text() {
        assert_eq!(
            messages::native_histogram_not_counter_warning("some_metric"),
            "PromQL warning: this native histogram metric is not a counter: \"some_metric\""
        );
        // Raw %q, always appended — an empty name renders as "".
        assert_eq!(
            messages::native_histogram_not_counter_warning(""),
            "PromQL warning: this native histogram metric is not a counter: \"\""
        );
    }

    /// Issue #125: the corpus asserts the aggregation form verbatim
    /// (`native_histograms.test:1858` etc.); the addition/subtraction
    /// forms are the binop arms' texts (`engine.go:3524,3535`).
    #[test]
    fn histogram_counter_reset_collision_warning_message_text() {
        assert_eq!(
            messages::histogram_counter_reset_collision_warning(messages::HistogramOperation::Agg),
            "PromQL warning: conflicting counter resets during histogram aggregation"
        );
        assert_eq!(
            messages::histogram_counter_reset_collision_warning(messages::HistogramOperation::Add),
            "PromQL warning: conflicting counter resets during histogram addition"
        );
        assert_eq!(
            messages::histogram_counter_reset_collision_warning(messages::HistogramOperation::Sub),
            "PromQL warning: conflicting counter resets during histogram subtraction"
        );
    }

    #[test]
    fn mixed_floats_histograms_agg_warning_message_text() {
        assert_eq!(
            messages::mixed_floats_histograms_agg_warning(),
            "PromQL warning: encountered a mix of histograms and floats for aggregation"
        );
    }

    #[test]
    fn histogram_ignored_in_aggregation_info_message_text() {
        assert_eq!(
            messages::histogram_ignored_in_aggregation_info("min"),
            "PromQL info: ignored histogram in min aggregation"
        );
        assert_eq!(
            messages::histogram_ignored_in_aggregation_info("topk"),
            "PromQL info: ignored histogram in topk aggregation"
        );
    }

    #[test]
    fn incompatible_types_in_binop_info_message_text() {
        assert_eq!(
            messages::incompatible_types_in_binop_info("histogram", "+", "float"),
            "PromQL info: incompatible sample types encountered for binary operator \"+\": histogram + float"
        );
    }

    #[test]
    fn incompatible_bucket_layout_in_binop_warning_message_text() {
        assert_eq!(
            messages::incompatible_bucket_layout_in_binop_warning("+"),
            "PromQL warning: incompatible bucket layout encountered for binary operator +"
        );
    }
}
