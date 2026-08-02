//! Issue #328 D8: the RE2-authority screen's verdicts over the WHOLE
//! committed corpus, as a hermetic before/after baseline.
//!
//! `tests/re2_screen_differential.rs` proves the screen against a real
//! RE2, but every `screened(...)` call there sits inside its live
//! (`PULSUS_TEST_CLICKHOUSE`-gated) tests — on a hermetic run that file
//! proves nothing about the wrapper. This binary is the hermetic half of
//! issue #328's bit-for-bit preservation claim: the committed
//! `screen_verdicts.txt` records `pattern_requires_re2_authority`'s
//! verdict for every corpus pattern (curated first, then generated, in
//! file order), captured on the pre-extraction tree, and this test
//! replays the screen over the corpus and asserts every verdict is
//! unchanged. The `pulsus-re2` extraction commit must not touch the
//! fixture, so a wrapper that changes ANY pattern's verdict — or a corpus
//! coverage shrink — reddens here, with no container.
//!
//! Regenerating (a deliberate screen change only — never to absorb an
//! extraction diff): `PULSUS_REGEN_RE2_SCREEN_VERDICTS=1 cargo test -p
//! pulsus-read --test re2_screen_preservation`, then review the diff.
//!
//! Deliberately duplicated from `re2_screen_differential.rs`: only
//! `fixture_dir`/`read_corpus` (the plan's D8 names them as the sole
//! sanctioned duplication). The generator/seed machinery is NOT
//! duplicated — this binary reads the committed fixtures and nothing
//! else, so it cannot drift from the generator the differential pins.

use std::path::PathBuf;

use pulsus_read::metrics::pattern_requires_re2_authority_for_test as screened;

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/re2_screen")
}

fn read_file(name: &str) -> String {
    let path = fixture_dir().join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"))
}

/// The corpus records: one pattern per line, with blank lines and `#`
/// comments dropped as prose (duplicated from
/// `re2_screen_differential.rs::read_corpus`, per D8).
fn read_corpus(name: &str) -> Vec<String> {
    read_file(name)
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_string)
        .collect()
}

/// Curated first, generated second — the same order the differential
/// replays and the order the verdict fixture is committed in.
fn full_corpus() -> Vec<String> {
    let mut all = read_corpus("curated.txt");
    all.extend(read_corpus("generated.txt"));
    all
}

/// The exact bytes `screen_verdicts.txt` must contain: a header, then one
/// `{0|1}\t{pattern}` line per corpus pattern, corpus order. Single
/// source of truth for the regeneration path and the comparison.
fn rendered_verdicts() -> String {
    let mut out = String::from(
        "# Issue #328 D8: pattern_requires_re2_authority's verdict (1 = requires the\n\
         # RE2 authority, 0 = decidable in-process) for every corpus pattern —\n\
         # curated.txt then generated.txt, in file order. Captured on the\n\
         # pre-extraction tree; the pulsus-re2 extraction must not change one bit\n\
         # of it. Regenerate ONLY for a deliberate screen change:\n\
         # PULSUS_REGEN_RE2_SCREEN_VERDICTS=1, then review the diff.\n",
    );
    for pattern in full_corpus() {
        out.push(if screened(&pattern) { '1' } else { '0' });
        out.push('\t');
        out.push_str(&pattern);
        out.push('\n');
    }
    out
}

/// The whole-corpus preservation gate: every committed verdict, byte for
/// byte. A single changed bit names the first offending pattern.
#[test]
fn every_committed_screen_verdict_is_reproduced_by_the_screen() {
    let expected = rendered_verdicts();
    let path = fixture_dir().join("screen_verdicts.txt");
    if std::env::var("PULSUS_REGEN_RE2_SCREEN_VERDICTS").as_deref() == Ok("1") {
        std::fs::write(&path, &expected).expect("write screen_verdicts.txt");
        return;
    }
    let committed = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "read {path:?}: {e} — the verdict baseline must be committed BEFORE \
             any change to the screen (issue #328 AC 12)"
        )
    });
    if committed == expected {
        return;
    }
    // Name the first divergent line rather than dumping ~4,300 lines.
    for (i, (have, want)) in committed.lines().zip(expected.lines()).enumerate() {
        assert_eq!(
            have,
            want,
            "screen_verdicts.txt:{}: the screen's verdict changed (committed {have:?}, \
             screen now produces {want:?}) — pattern_requires_re2_authority is no longer \
             bit-for-bit preserved",
            i + 1
        );
    }
    panic!(
        "screen_verdicts.txt differs from the live screen only in line count/terminators \
         ({} bytes committed, {} produced) — the corpus coverage moved",
        committed.len(),
        expected.len()
    );
}

/// The baseline's coverage cannot silently shrink: it must carry exactly
/// one verdict per corpus pattern, in corpus order.
#[test]
fn the_committed_baseline_covers_the_whole_corpus_in_order() {
    let corpus = full_corpus();
    let committed: Vec<(String, String)> = read_file("screen_verdicts.txt")
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|l| {
            let (verdict, pattern) = l
                .split_once('\t')
                .unwrap_or_else(|| panic!("malformed verdict line {l:?}"));
            assert!(
                verdict == "0" || verdict == "1",
                "verdict must be 0 or 1, got {verdict:?}"
            );
            (verdict.to_string(), pattern.to_string())
        })
        .collect();
    assert_eq!(
        committed.len(),
        corpus.len(),
        "the baseline must carry one verdict per corpus pattern"
    );
    for (i, ((_, pattern), expected)) in committed.iter().zip(&corpus).enumerate() {
        assert_eq!(
            pattern,
            expected,
            "screen_verdicts.txt entry {} names {pattern:?} where the corpus has {expected:?}",
            i + 1
        );
    }
}
