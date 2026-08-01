//! **The only way to retain template output on the pipeline's row path**
//! (issue #260, review rounds 2 and 3).
//!
//! Three rounds of "every retaining site now charges" each found a new
//! site — the full-engine render, then the `Simple`/`Parts` fast paths,
//! then the once-per-stage label snapshot. Site-by-site charging is an
//! open enumeration: the set of places that produce a retained value
//! grows, and nothing makes a new one declare itself.
//!
//! So the charge moves into the TYPE. [`Retained`] and [`LabelSnapshot`]
//! wrap private fields and this module exports no other constructor, so
//! outside it the only way to obtain either is a function here — and
//! every one of those charges the row's [`RenderBudget`] BEFORE it
//! allocates. The pipeline types its retained template values as
//! [`Retained`] and its `StageMap` snapshot as [`LabelSnapshot`], so a
//! fourth render shape is a `match` arm that does not compile until it
//! produces a charged value, and a second snapshot is a field that
//! admits nothing else.
//!
//! **Where this ended up, and the five places it had to be dragged
//! from.** Each round of review found the same defect one level further
//! in, so the list is written out — a reader who only gets the rule will
//! reinvent the instances:
//!
//!   1. **Pipeline sites.** The full-engine render was charged; its
//!      output was retained per `label_format` destination, so the cap
//!      bounded one buffer while the number of live buffers was bounded
//!      only by the query-text cap. *Fixed by moving the budget's
//!      lifetime from the render to the ROW.*
//!   2. **More pipeline sites.** The `Simple`/`Parts` compile-time fast
//!      paths never call the engine and retained their copies uncharged
//!      — the shape `label_format d="{{.a}}"` compiles to, i.e. most
//!      queries. *Fixed by charging them; found because the round-1
//!      fixtures only used the full engine.*
//!   3. **The snapshot clone.** A snapshot-requiring `label_format`
//!      stage deep-copies every OWNED value in the label set, including
//!      output already charged for, putting a third copy live while
//!      charging for two. *Fixed by charging it — and by concluding
//!      that site-by-site charging is an open enumeration, which is why
//!      this module exists.*
//!   4. **The seal's own API.** The leaf shipped a constructor taking a
//!      caller-supplied length AND an arbitrary fill closure, checked by
//!      a `debug_assert` that compiles out; and an adopter at
//!      `pub(super)`, so anything in `template` could adopt an arbitrary
//!      uncharged `Vec<u8>`. "Only one caller" is a convention, not a
//!      restriction. *Fixed by [`Retained::concat`] sizing the pieces
//!      itself, and by making the adopter PRIVATE with its one caller,
//!      [`render_full`], moved in beside it.*
//!   5. **The ordering inside the replacement.** `concat` reconciled the
//!      sized total against the written total AFTERWARDS — so a source
//!      that yielded more on the write walk had already made `push_str`
//!      reallocate past the charged capacity by the time the excess
//!      charge could refuse. **The mechanism that exists to enforce
//!      charge-before-allocate was itself allocating before charging.**
//!      *Fixed by charging each piece BEFORE pushing it;
//!      `tests/logql_retained_ordering_gate.rs` measures the difference
//!      under a near-exhausted budget, where correct ordering refuses
//!      without growing and the old one leaves the grown buffer on the
//!      allocator counter.*
//!
//! **So the rule this module is held to** (round 3, and the #280
//! dispatch-seal lesson): it may contain the TYPES plus their legitimate
//! constructors and nothing else; no PUBLIC constructor may take a
//! length, a writer or a buffer from its caller; and no charge may be
//! reconciled after the allocation it pays for.
//!
//! The one exception is deliberate and is the reason the rule says
//! PUBLIC: the private `from_engine` does take a `Vec<u8>`, from
//! `render_full` beside it in this module, and charges the UTF-8
//! expansion it causes. That route is unreachable from outside the leaf
//! (`E0624`), so the caller is verified rather than trusted — which is
//! the property the rule is actually protecting.
//!
//! **What this does NOT cover, stated because the previous wording was
//! faulted for overstating.** The row's label vector and its `line` also
//! hold owned values produced by PARSERS (`json`, `logfmt`, `regexp`,
//! `pattern`, `unpack`) and by the non-template line rewrites
//! (`decolorize`, `unpack`). Those are not template output, and charging
//! them against `MAX_TEMPLATE_RENDER_BYTES` would give a query with no
//! template at all a 64 MiB ceiling on parser extraction — a different
//! rejection surface and a different issue. They are bounded by the scan
//! budget instead. The guarantee here is exactly: *every template-derived
//! retained value, and every copy of the row's label set, is charged*.

use std::borrow::Cow;

use super::{BudgetExhausted, RenderBudget};

/// A template-derived string the row RETAINS, whose bytes were charged
/// against the row's [`RenderBudget`] BEFORE they were allocated.
///
/// The field is private; the constructors below are the only ones. Every
/// pipeline site that keeps template output is typed as this, which is
/// what makes the enumeration the COMPILER's rather than a reviewer's.
#[derive(Debug)]
pub struct Retained(String);

impl Retained {
    /// A verbatim copy of an already-resolved value — the
    /// `Template::Simple` fast path, whose output is exactly `src`.
    ///
    /// Charge-before-allocate holds trivially: the final length is
    /// `src.len()`, known before the copy exists.
    pub fn copy(budget: &RenderBudget, src: &str) -> Result<Self, BudgetExhausted> {
        budget.charge_retained(src.len())?;
        Ok(Retained(src.to_string()))
    }

    /// A CONCATENATION of pieces the constructor sizes itself — the
    /// `Template::Parts` fast path, which is a text/label piece list.
    ///
    /// There is no caller-supplied length and no caller-supplied writer
    /// (review round 3, finding 1: the previous `assemble` took both,
    /// and its size-equality check was a `debug_assert` — so a release
    /// build would let a caller write past what was charged. A charge
    /// the callee cannot verify is not a charge). `pieces` is walked
    /// TWICE by this function: once to size, once to write, so the
    /// charged number and the allocated number are the same expression
    /// and no caller can put them out of step.
    ///
    /// **The write loop charges each piece BEFORE it pushes it** (review
    /// round 4). That the two walks agree is a property of the caller's
    /// iterator, not of this constructor, so a source that yields more
    /// on the second walk must pay before the buffer can grow — the
    /// previous version reconciled the totals AFTERWARDS, by which time
    /// `push_str` had already reallocated past the charged capacity.
    /// A refusal returns with the buffer un-grown and drops it, so the
    /// breach is a clean 422 and nothing was ever allocated unpaid.
    ///
    /// (An iterator that yields LESS leaves the budget over-charged,
    /// which is the safe direction and needs no correction — this ledger
    /// has no refund by design. Sizes are content bytes, not allocator
    /// blocks, consistently with every other constructor here.)
    pub fn concat<'s, I>(budget: &RenderBudget, pieces: I) -> Result<Self, BudgetExhausted>
    where
        I: Iterator<Item = &'s str> + Clone,
    {
        let mut need = 0usize;
        for piece in pieces.clone() {
            need = need.saturating_add(piece.len());
        }
        budget.charge_retained(need)?;
        let mut out = String::with_capacity(need);
        // Bytes the ledger has already paid for. It starts at the sizing
        // walk's total and only ever grows, ahead of the write.
        let mut paid = need;
        for piece in pieces {
            // CHECKED, not saturating: a saturating total would silently
            // stop demanding payment at `usize::MAX` instead of refusing.
            let after = out.len().checked_add(piece.len()).ok_or(BudgetExhausted)?;
            if after > paid {
                budget.charge_retained(after - paid)?;
                paid = after;
            }
            out.push_str(piece);
        }
        Ok(Retained(out))
    }

    /// The full engine's output, adopted at the pipeline boundary.
    ///
    /// **PRIVATE** (review round 3, finding 2). It was `pub(super)`,
    /// which let anything in `template` adopt an arbitrary uncharged
    /// `Vec<u8>` as a `Retained`; "only `render_full` calls it" was a
    /// convention, and this project has been bitten by that exact
    /// sentence before. Its one legitimate caller now lives in this
    /// module, so the restriction is the compiler's.
    ///
    /// Adoption charges nothing for `bytes` themselves: by the time the
    /// engine returns it has charged every byte it produced into the
    /// SAME ledger, production by production, so charging again would
    /// double-count and would move the pinned single-render boundary.
    ///
    /// The UTF-8 repair IS charged here: the engine works in bytes and
    /// this is where they become a `String`, so an invalid render
    /// expands by up to two bytes per invalid sequence and that
    /// expansion was previously nobody's. Same
    /// precompute-charge-allocate-once discipline the engine's own
    /// conversions use; valid UTF-8 MOVES the buffer and charges
    /// nothing, so every pinned boundary is unchanged.
    fn from_engine(budget: &RenderBudget, bytes: Vec<u8>) -> Result<Self, BudgetExhausted> {
        match String::from_utf8(bytes) {
            Ok(s) => Ok(Retained(s)),
            Err(e) => {
                let raw = e.as_bytes();
                let need = super::funcs::lossy_repaired_len(raw);
                budget.charge_retained(need.saturating_sub(raw.len()))?;
                Ok(Retained(super::funcs::lossy_repaired(raw, need)))
            }
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Spends the value into the row state (`line`, or a `label_format`
    /// destination). Consuming, so one charge can back only one retained
    /// value.
    pub fn into_cow<'a>(self) -> Cow<'a, str> {
        Cow::Owned(self.0)
    }
}

/// Renders a FULL template. `labels` is the SNAPSHOT the caller decided
/// on (the #231 once-per-stage map rule); `err`/`err_details` are the
/// gate-resolved #238 out-of-band pair; `line`/`ts_ns` back
/// `__line__`/`__timestamp__`.
///
/// `budget` is the CALLER's (issue #260): the ledger outlives this call
/// because this call's output does — see
/// [`super::MAX_TEMPLATE_RENDER_BYTES`]. Constructing it here would have
/// bounded one render while leaving the number of simultaneously-live
/// renders unbounded.
///
/// **Lives in this module** (review round 3, finding 2) because it is
/// the engine flavour's CONSTRUCTOR, and the alternative — exporting
/// [`Retained::from_engine`] at `pub(super)` and relying on this being
/// its only caller — is a convention rather than a restriction. Here,
/// the adopter is private and the compiler holds that no other path
/// turns engine bytes into a `Retained`.
///
/// It returns a [`Retained`] rather than filling a caller's buffer:
/// the output is retained by the caller, so it is exactly the thing the
/// charging type exists to represent, and returning it is what makes
/// every retention site on the pipeline's row path share one type.
#[allow(clippy::too_many_arguments)]
pub fn render_full(
    prog: &super::Program,
    labels: &[(Cow<'_, str>, Cow<'_, str>)],
    err: Option<&str>,
    err_details: Option<&str>,
    line: &str,
    ts_ns: i64,
    env: &super::TemplateEnv,
    budget: &RenderBudget,
) -> Result<Retained, super::TemplateExecError> {
    let mut out: Vec<u8> = Vec::new();
    let input = super::eval::EvalInput {
        text: &prog.text,
        parse_name: prog.kind.parse_name(),
        root: &prog.root,
        defines: &prog.defines,
        labels,
        err,
        err_details,
        line,
        ts_ns,
        env,
        regex_cache: &prog.regex_cache,
        budget,
    };
    super::eval::render(&input, &mut out)?;
    // A breach here is the budget's, in the engine's own error class, so
    // the caller's single `budget_breach` arm keeps handling both.
    Retained::from_engine(budget, out).map_err(|BudgetExhausted| super::TemplateExecError {
        msg: format!(
            "template output exceeded the {}-byte render budget",
            super::MAX_TEMPLATE_RENDER_BYTES
        ),
        budget_breach: true,
    })
}

/// A DEEP COPY of the row's label set, charged for the bytes it copies.
///
/// The reference builds `label_format`'s data map once per stage
/// (`fmt.go:423-425`), so a snapshot-requiring stage renders every
/// destination against the labels as they were at stage entry — which
/// means copying them while the live vector keeps being mutated. Cloning
/// a `Cow::Borrowed` is a pointer copy and free; cloning a `Cow::Owned`
/// duplicates an already-retained value, so a stage that snapshots a
/// label set holding a large rendered value put a second copy of it live
/// with nothing charging for it (review round 2, finding 1).
///
/// Private field, one constructor: `StageMap`'s field is typed as this,
/// so the copy cannot be made any other way.
#[derive(Debug)]
pub struct LabelSnapshot<'a>(Vec<(Cow<'a, str>, Cow<'a, str>)>);

impl<'a> LabelSnapshot<'a> {
    /// Charges what the copy will retain, then copies.
    ///
    /// Two terms, both exact: the OWNED halves' bytes (borrowed halves
    /// cost nothing — the clone is a pointer copy into the same backing
    /// string), and the element buffer, which `to_vec` reserves exactly.
    pub fn take(
        budget: &RenderBudget,
        labels: &[(Cow<'a, str>, Cow<'a, str>)],
    ) -> Result<Self, BudgetExhausted> {
        let mut need = labels
            .len()
            .saturating_mul(size_of::<(Cow<'a, str>, Cow<'a, str>)>());
        for (k, v) in labels {
            // Only the OWNED halves cost anything: cloning a
            // `Cow::Borrowed` re-points at the same backing string.
            for half in [k, v] {
                if let Cow::Owned(s) = half {
                    need = need.saturating_add(s.len());
                }
            }
        }
        budget.charge_retained(need)?;
        Ok(LabelSnapshot(labels.to_vec()))
    }

    pub fn as_slice(&self) -> &[(Cow<'a, str>, Cow<'a, str>)] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    /// The seal, asserted as a property rather than as prose: each
    /// constructor moves the ledger by exactly what it retains, and a
    /// refusal leaves nothing behind.
    #[test]
    fn every_constructor_charges_exactly_what_it_retains() {
        let budget = RenderBudget::default();
        let before = budget.charged_bytes();
        let r = Retained::copy(&budget, "hello").expect("fits");
        assert_eq!(r.as_str(), "hello");
        assert_eq!(budget.charged_bytes() - before, 5);

        let at = budget.charged_bytes();
        let r = Retained::concat(&budget, ["abc", "", "defg"].into_iter()).expect("fits");
        assert_eq!(r.as_str(), "abcdefg");
        assert_eq!(budget.charged_bytes() - at, 7);

        // Valid UTF-8 from the engine is a MOVE — the engine already
        // charged those bytes, so adopting them charges nothing.
        let at = budget.charged_bytes();
        let r = Retained::from_engine(&budget, b"already charged".to_vec()).expect("fits");
        assert_eq!(r.as_str(), "already charged");
        assert_eq!(budget.charged_bytes() - at, 0);

        // Invalid UTF-8 grows: 0xFF becomes U+FFFD (1 byte in, 3 out),
        // so the EXPANSION — and only the expansion — is charged.
        let at = budget.charged_bytes();
        let r = Retained::from_engine(&budget, vec![b'a', 0xFF, b'b']).expect("fits");
        assert_eq!(r.as_str(), "a\u{FFFD}b");
        assert_eq!(r.as_str().len(), 5);
        assert_eq!(budget.charged_bytes() - at, 2);
    }

    /// An iterator whose SECOND walk yields more than its first — the
    /// only way left to make a `concat` write more than it sized, now
    /// that no caller supplies the length.
    ///
    /// A hostile stand-in for the shape review round 3 found real: the
    /// old `assemble` let a caller pass `presized` and an unrelated fill
    /// closure, and checked the two agreed with a `debug_assert` that
    /// vanishes in release. Nothing about "the caller is well-behaved"
    /// is this constructor's to assume, so the excess is CHARGED.
    #[derive(Clone)]
    struct GrowsOnTheSecondWalk<'a> {
        pieces: &'a [&'a str],
        /// Shared across clones, so the clone the constructor walks
        /// first and the original it walks second disagree.
        walk: &'a Cell<usize>,
        next: usize,
    }

    impl<'a> Iterator for GrowsOnTheSecondWalk<'a> {
        type Item = &'a str;
        fn next(&mut self) -> Option<&'a str> {
            // First walk: the first piece only. Later walks: everything.
            let visible = if self.walk.get() == 0 {
                1
            } else {
                self.pieces.len()
            };
            if self.next >= visible {
                self.walk.set(self.walk.get() + 1);
                self.next = 0;
                return None;
            }
            let item = self.pieces[self.next];
            self.next += 1;
            Some(item)
        }
    }

    /// **The overfill case** (review round 3, finding 1). The written
    /// length is reconciled against the charge UNCONDITIONALLY — in
    /// every build profile — so a source that produces more than it
    /// sized pays for the excess instead of slipping past a
    /// `debug_assert`.
    #[test]
    fn concat_charges_a_source_that_writes_more_than_it_sized() {
        let walk = Cell::new(0usize);
        let pieces: &[&str] = &["aaaa", "bbbbbbbb"];
        let liar = GrowsOnTheSecondWalk {
            pieces,
            walk: &walk,
            next: 0,
        };

        let budget = RenderBudget::default();
        let r = Retained::concat(&budget, liar).expect("fits");
        // The sizing walk saw 4 bytes; the writing walk wrote 12.
        assert_eq!(r.as_str(), "aaaabbbbbbbb");
        assert_eq!(
            budget.charged_bytes(),
            r.as_str().len() as u64,
            "the ledger must have moved by what was WRITTEN, not by what was sized"
        );
    }

    /// ...and the reconciliation can REFUSE: an overrun that crosses the
    /// budget is the same clean breach as any other, not a silent
    /// over-allocation.
    #[test]
    fn concat_refuses_when_the_overrun_crosses_the_budget() {
        let big = "z".repeat(super::super::MAX_TEMPLATE_RENDER_BYTES as usize / 2 + 1);
        let walk = Cell::new(0usize);
        let pieces: &[&str] = &[&big, &big];
        let liar = GrowsOnTheSecondWalk {
            pieces,
            walk: &walk,
            next: 0,
        };

        let budget = RenderBudget::default();
        assert_eq!(
            Retained::concat(&budget, liar).unwrap_err(),
            BudgetExhausted
        );
        assert!(budget.breached());
    }

    /// A snapshot charges the OWNED halves it duplicates and not the
    /// borrowed ones it merely re-points at.
    #[test]
    fn a_snapshot_charges_the_bytes_it_deep_copies() {
        let slot = size_of::<(Cow<'_, str>, Cow<'_, str>)>();
        let budget = RenderBudget::default();

        let borrowed: Vec<(Cow<'_, str>, Cow<'_, str>)> =
            vec![(Cow::Borrowed("a"), Cow::Borrowed("xxxxxxxxxx"))];
        let at = budget.charged_bytes();
        LabelSnapshot::take(&budget, &borrowed).expect("fits");
        assert_eq!(
            budget.charged_bytes() - at,
            slot as u64,
            "a borrowed pair costs only its slot"
        );

        let owned: Vec<(Cow<'_, str>, Cow<'_, str>)> =
            vec![(Cow::Borrowed("a"), Cow::Owned("y".repeat(1_000)))];
        let at = budget.charged_bytes();
        let snap = LabelSnapshot::take(&budget, &owned).expect("fits");
        assert_eq!(budget.charged_bytes() - at, slot as u64 + 1_000);
        assert_eq!(snap.as_slice()[0].1.len(), 1_000);
    }

    /// A refused charge yields nothing — there is no partially-built
    /// `Retained`, which is the point of charging in the constructor.
    #[test]
    fn a_refused_charge_produces_no_value_and_poisons_the_budget() {
        let budget = RenderBudget::default();
        let huge = "q".repeat(super::super::MAX_TEMPLATE_RENDER_BYTES as usize / 2 + 1);
        assert_eq!(
            Retained::concat(&budget, [huge.as_str(), huge.as_str()].into_iter()).unwrap_err(),
            BudgetExhausted
        );
        assert!(budget.breached());
    }

    /// **The forge case** (review round 3, finding 2), asserted where it
    /// can be: [`Retained::from_engine`] is a PRIVATE `fn`, so the only
    /// route from engine bytes to a `Retained` anywhere outside this
    /// module is [`render_full`] — which runs the engine, and the engine
    /// charges every byte it produces.
    ///
    /// The negative half ("nothing outside can call `from_engine`") is a
    /// compile-time property and cannot be a running test; it is
    /// demonstrated by mutation instead (adding an external call is a
    /// build failure). What IS asserted here is the positive half: the
    /// one public route really does leave the ledger holding the output.
    #[test]
    fn the_only_public_engine_route_charges_what_it_produces() {
        let Ok(super::super::Template::Full(prog)) =
            super::super::compile("{{ repeat 4096 \"x\" }}", super::super::TemplateKind::Line)
        else {
            panic!("the fixture must need the full engine");
        };
        let budget = RenderBudget::default();
        let rendered = render_full(
            &prog,
            &[],
            None,
            None,
            "line",
            0,
            &Default::default(),
            &budget,
        )
        .expect("well inside the budget");
        assert_eq!(rendered.as_str().len(), 4096);
        assert!(
            budget.charged_bytes() >= 4096,
            "the engine's own productions must already be on the ledger by adoption time; \
             charged {}",
            budget.charged_bytes()
        );
    }
}
