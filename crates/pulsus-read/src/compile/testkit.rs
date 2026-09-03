//! Shared apparatus for the residual-state-effect gates (issue #492).
//!
//! The design's central repair is that **a residual link still applies
//! its state effect**. Two things can go wrong with it and they need
//! different instruments:
//!
//! - the effect is **missing** — caught by comparing the real dispatcher
//!   against [`Neutered`], whose `residual_effect` is the identity. That
//!   comparison IS the break a reviewer would otherwise ask for, run
//!   in-process for every row on every run: no source edit, no rebuild,
//!   and no row that can be forgotten;
//! - the effect is **wrong** — caught by two literal `Relation`s per row,
//!   written in the test from the design record's own row rather than
//!   computed by the code under test.
//!
//! **Two seeds per row, not one, and that is the part that matters.** If
//! a row claims the effect leaves the shape alone and the single seed's
//! shape is the stage's usual input shape, then an implementation that
//! *preserves* the shape and one that *assigns* it produce the same
//! relation, and a one-seed gate passes on both while looking like
//! coverage. Assertion 3 is the one a single seed cannot make.

use super::fold::{Capability, Lang, Lower, LowerCx, Relation};

/// The neutered dispatcher: `residual_effect` returns the relation
/// unchanged and everything else delegates.
pub struct Neutered<'a, L: Lang + ?Sized>(pub &'a dyn Lower<L>);

impl<L: Lang + ?Sized> Lower<L> for Neutered<'_, L> {
    fn capability(&self, s: &L::Stage, rel: &Relation<L>) -> Capability {
        self.0.capability(s, rel)
    }
    fn apply(
        &self,
        s: &L::Stage,
        rel: Relation<L>,
        cx: &LowerCx<'_, L>,
    ) -> Result<Relation<L>, L::Err> {
        self.0.apply(s, rel, cx)
    }
    fn residual_effect(&self, _s: &L::Stage, rel: Relation<L>) -> Relation<L> {
        rel
    }
}

/// One row of a residual-state-effect gate.
pub struct EffectRow<L: Lang + ?Sized> {
    /// The design record's own name for the link.
    pub name: &'static str,
    pub link: L::Stage,
    pub s1: Relation<L>,
    pub s2: Relation<L>,
    /// Whole relations written in the test from the row, never computed
    /// by the code under test.
    pub e1: Relation<L>,
    pub e2: Relation<L>,
    /// `false` for every row whose effect PRESERVES a field the two seeds
    /// differ in; `true` only where the effect genuinely resets a field
    /// to a constant.
    pub effect_is_constant: bool,
    /// `false` for the rows whose stated effect is *none* — the exemption
    /// checked rather than assumed.
    pub has_effect: bool,
}

/// Runs every assertion of the design record's §11.2b row shape.
pub fn assert_every_residual_state_effect<L: Lang + ?Sized + 'static>(
    rows: &[EffectRow<L>],
    expected_rows: usize,
) {
    assert_eq!(
        rows.len(),
        expected_rows,
        "the gate must carry one row per link with a stated residual state effect"
    );
    for row in rows {
        let real = L::lower_of(&row.link);
        let name = row.name;

        // 1. the two seeds really are different, so a row cannot be
        //    satisfied by supplying the same seed twice.
        assert_ne!(row.s1, row.s2, "{name}: the two seeds must differ");

        // 2. the effect is the one the row states — on both seeds.
        assert_eq!(
            real.residual_effect(&row.link, row.s1.clone()),
            row.e1,
            "{name}: seed 1"
        );
        assert_eq!(
            real.residual_effect(&row.link, row.s2.clone()),
            row.e2,
            "{name}: seed 2"
        );

        // 3. preserve-against-assign: the assertion a single seed cannot
        //    make.
        assert_eq!(
            row.e1 == row.e2,
            row.effect_is_constant,
            "{name}: the two literal expectations agree iff the effect resets a field to a \
             constant"
        );

        // 4/5. the neutering: a MISSING effect, and the exemption.
        let neutered = Neutered(real);
        for (i, seed) in [(1usize, &row.s1), (2usize, &row.s2)] {
            let got = real.residual_effect(&row.link, seed.clone());
            let flat = neutered.residual_effect(&row.link, seed.clone());
            if row.has_effect {
                assert_ne!(
                    got, flat,
                    "{name}: seed {i} — the effect is missing (it equals the neutered one)"
                );
            } else {
                assert_eq!(
                    got, flat,
                    "{name}: seed {i} — this row's stated effect is NONE, so it must equal the \
                     neutered one"
                );
            }
        }
    }
}
