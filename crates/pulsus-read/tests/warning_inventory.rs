//! Issue #277 AC 17 — **the scoping decision is pinned; the reference
//! inventory is a read.**
//!
//! PulsusDB emits exactly one of the reference's three warning message
//! families. This suite commits the eleven `AddWarning`/`AddWarnings` call
//! sites that inventory rests on as DATA, asserts our implemented family is
//! family 1 and nothing else, and asserts that families 2 and 3 are
//! recorded as measured non-emissions in the differential ledger's entry
//! `(d)` — parsed out of that entry, not substring-matched against the
//! whole document.
//!
//! # What this pins, and what it does not
//!
//! **It pins OUR decision, not the reference's completeness.** The bound on
//! the reference side has two halves, both stated so the next reader can
//! re-check them rather than trust this file:
//!
//! * a client's `warnings` array is populated only from `metadata.Context`'s
//!   `warnings map[string]struct{}` (`pkg/logqlmodel/metadata/context.go:34
//!   @ grafana/loki v3.7.4 b318f2829f0ae2094ab3a1e90780450e9e4b03be`), or
//!   propagated from an upstream response that was itself populated that
//!   way. That field is **unexported**, so Go's package privacy excludes
//!   every OTHER package;
//! * inside that package the bound is a **hand read**, not a compiler one —
//!   privacy does not exclude the package from itself. At `v3.7.4` the
//!   package is a single non-test file (`git ls-tree -r --name-only v3.7.4
//!   pkg/logqlmodel/metadata` lists `context.go` and `context_test.go`)
//!   carrying exactly two insertion statements, `context.go:84`
//!   (`AddWarning`) and `:140` (`AddWarnings`).
//!
//! **Residue, accepted and recorded.** [`CALL_SITES`] is a read enumeration
//! of a FROZEN tree — our reference is pinned at
//! `b318f2829f0ae2094ab3a1e90780450e9e4b03be`, so nothing can "appear"
//! without a deliberate version bump, but equally **nothing on our side
//! would catch a mis-read of that tree.** This file stores what we
//! concluded; it does not re-derive it. A check asserting the two insertion
//! statements are still the only ones would, against a pinned commit, be
//! asserting its own input.

use std::path::PathBuf;

/// Every `AddWarning`/`AddWarnings` call site under `pkg/`, tests and
/// generated files excluded, at `grafana/loki v3.7.4`
/// `b318f2829f0ae2094ab3a1e90780450e9e4b03be`. Read with:
///
/// ```text
/// git -C <loki> grep -nE '\.(AddWarning|AddWarnings)\(' v3.7.4 -- pkg/ \
///     | grep -v '_test.go' | grep -v '\.pb\.go'
/// ```
///
/// `family` is `0` for a PROPAGATION site — one that forwards an upstream
/// response's already-built array (`batch.Warnings`, `res.Warnings`) and
/// mints no string of its own.
const CALL_SITES: &[(&str, u32, u8)] = &[
    ("pkg/engine/basic_engine.go", 265, 3),
    ("pkg/engine/engine.go", 298, 3),
    ("pkg/engine/handler.go", 532, 3),
    ("pkg/iter/entry_iterator.go", 383, 0),
    ("pkg/iter/sample_iterator.go", 500, 0),
    ("pkg/logql/downstream.go", 487, 0),
    ("pkg/logql/engine.go", 506, 1),
    ("pkg/logql/engine.go", 542, 2),
    ("pkg/logql/engine.go", 582, 2),
    ("pkg/querier/queryrange/limits.go", 507, 1),
    ("pkg/querier/queryrange/limits.go", 512, 2),
];

/// The three message families, by the format string each site builds.
const FAMILIES: &[(u8, &str)] = &[
    (1, "maximum of series (%d) reached for variant (%s)"),
    (
        2,
        "maximum number of series (%d) reached for a single query; returning partial results",
    ),
    (
        3,
        "Query was executed using the new experimental query engine[ and dataobj storage.]",
    ),
];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf()
}

/// The body of the differential ledger's
/// `variants-label-collision-and-fanout-bounds` entry `(d)`: everything
/// from the `- **(d)` bullet up to the next `###` heading. Sliced so the
/// assertions below cannot be satisfied by text sitting somewhere else in
/// a 2 000-line document.
fn ledger_entry_d() -> String {
    let text =
        std::fs::read_to_string(repo_root().join("docs/benchmarks/logs-differential-ledger.md"))
            .expect("the differential ledger is readable");
    let section_at = text
        .find("### `variants-label-collision-and-fanout-bounds`")
        .expect("the variants section exists");
    let section = &text[section_at..];
    let section_end = section[1..]
        .find("\n### ")
        .map(|i| i + 1)
        .unwrap_or(section.len());
    let section = &section[..section_end];
    let d_at = section
        .find("- **(d)")
        .expect("entry (d) exists in the variants section");
    section[d_at..].to_string()
}

#[test]
fn the_reference_warning_inventory_is_three_families() {
    // The committed enumeration is exactly eleven sites, and every one is
    // classified into a family or marked as propagation.
    assert_eq!(CALL_SITES.len(), 11, "the read enumeration is eleven sites");
    for (file, line, family) in CALL_SITES {
        assert!(
            *family == 0 || FAMILIES.iter().any(|(f, _)| f == family),
            "{file}:{line} names family {family}, which is not in the inventory"
        );
    }
    // Every family is reached by at least one MINTING site — an inventory
    // row nothing produces would be a claim about nothing.
    for (family, message) in FAMILIES {
        assert!(
            CALL_SITES.iter().any(|(_, _, f)| f == family),
            "family {family} ({message}) has no call site"
        );
    }
    // Three propagation sites, which mint no string at all.
    assert_eq!(
        CALL_SITES.iter().filter(|(_, _, f)| *f == 0).count(),
        3,
        "the propagation sites forward an upstream array and mint nothing"
    );

    // OUR implemented family is family 1, and exactly family 1. The
    // formatter is the single production source of the text, so this
    // compares against what actually ships.
    let ours = pulsus_read::logql::variant_series_warning(500, 0);
    assert_eq!(ours, "maximum of series (500) reached for variant (0)");
    let family1 = FAMILIES
        .iter()
        .find(|(f, _)| *f == 1)
        .map(|(_, m)| *m)
        .expect("family 1");
    // Family 1's shape with its two verbs substituted.
    assert_eq!(
        ours,
        family1.replace("(%d)", "(500)").replace("(%s)", "(0)")
    );
    // …and specifically NOT family 2's wording, which differs by exactly
    // the word this project has already mistaken once.
    assert!(
        !ours.contains("maximum number of series"),
        "family 1 is `maximum of series`, family 2 is `maximum number of series`"
    );

    // Families 2 and 3 are recorded as measured NON-EMISSIONS in ledger
    // entry (d) — parsed out of that entry, not found anywhere in the file.
    let d = ledger_entry_d();
    for (family, key) in [
        (
            2u8,
            "maximum number of series (%d) reached for a single query;",
        ),
        (
            3,
            "Query was executed using the new experimental query engine",
        ),
    ] {
        assert!(
            d.contains(key),
            "ledger entry (d) no longer names family {family}"
        );
    }
    assert!(
        d.contains("NOT emitted"),
        "entry (d) must record the non-emissions as such"
    );
    // The gating header for family 2 — the reason it is not implementable
    // rather than merely not implemented.
    assert!(
        d.contains("X-Query-Tags") && d.contains("grafana-lokiexplore-app"),
        "entry (d) must record what gates family 2"
    );
    // The evidence that there is no stable rule to match: the dropped ids
    // differed run to run.
    for id in ["184", "396", "370", "42", "303"] {
        assert!(
            d.contains(id),
            "entry (d) must keep the measured dropped ids ({id} missing) — a future \
             reader who sees only \"not implemented\" will otherwise re-derive them"
        );
    }
    // And family 1 is recorded as the one we DO implement.
    assert!(
        d.contains("**implemented**"),
        "entry (d) must say which family PulsusDB emits"
    );
}

/// The inventory is only meaningful if the ledger entry it points at is
/// the one carrying the range over-acceptance too — the two halves were
/// adjudicated together and a split would let either be deleted alone.
///
/// **Its strength, measured rather than claimed.** Each assertion is a
/// PRESENCE test over entry `(d)`'s body, and that is the whole claim:
/// "entry (d) names X". Verified by breaking it — replacing every
/// ``!ok`` in the ledger with prose reddens the second assertion;
/// editing ONE of the entry's two mentions does not, because the entry
/// still names it. Anyone wanting a stronger property (each mention in
/// its own sentence, say) has to state which sentence, and that is a
/// claim about prose this test deliberately does not make.
#[test]
fn ledger_entry_d_carries_the_range_over_acceptance_as_well() {
    let d = ledger_entry_d();
    assert!(
        d.contains("multiVariantVectorsToSeries"),
        "entry (d) must name the defect by function"
    );
    assert!(
        d.contains("`!ok`"),
        "entry (d) must name the MISSING GUARD, not just call it an off-by-one — \
         that is what stops someone later 'correcting' us toward it"
    );
    assert!(
        d.contains("vectorsToSeriesWithLimit"),
        "entry (d) must name the sibling that has the guard"
    );
    assert!(
        d.contains("over-acceptance"),
        "entry (d) must state the direction of the divergence"
    );
}
