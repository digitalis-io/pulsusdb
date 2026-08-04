//! Issue #335 Stage C: the 63 SQL goldens' whole-corpus content freeze —
//! the mechanism that makes "this change edits zero SQL goldens" a fact a
//! reviewer can check **from the diff**, instead of a claim to be taken on
//! trust.
//!
//! **The problem it solves.** `golden/traces_search/*.sql` (46) and
//! `golden/traces_metrics/*.sql` (17) are the semantic witness every
//! TraceQL grammar change is measured against: they live in another crate
//! from `Display`, so they catch a meaning change our own rendering
//! cannot see. Every such change therefore reports "the 63 SQL goldens
//! take zero edits" — and until now that sentence was verifiable only by
//! trusting the author's `git status`, or by re-listing 63 paths by hand.
//! A reviewer working from a patch had no single artefact to look at.
//!
//! **The mechanism.** One digest over the whole corpus — every file's
//! relative path AND bytes, in sorted path order — pinned as a constant
//! in THIS SOURCE FILE, deliberately not beside the data (the
//! `accept_surface.rs::the_reference_column_is_frozen_against_silent_re_pinning`
//! posture): a data-only edit fails here, so any golden movement forces a
//! visible source-line change in the same diff. `PINNED_SQL_CORPUS`
//! unchanged in a diff therefore MEANS all 63 files are byte-identical —
//! one line to look at rather than 63.
//!
//! The count is asserted separately from the digest so the failure says
//! which happened: a file added or removed reads differently from a file
//! edited, and lumping them into one hash tells a reviewer neither.
//!
//! **The unit is the DIRECTORY, not the `.sql` files in it** (Stage C
//! review, [low]). The first cut walked one level and filtered on the
//! `.sql` extension, so a `README` dropped in, or a golden tucked into a
//! subdirectory, moved neither the count nor the digest — the claim
//! "these directories are frozen" was true only of part of them. The
//! walk is now recursive and digests EVERY file it finds, whatever its
//! extension, keyed by its path RELATIVE to `tests/golden/`. Two
//! consequences worth stating because they are the properties being
//! bought: a file nested one level down has a different relative path
//! and so moves the digest, and a rename — including a move between the
//! two golden directories — moves it too, because the path is fed
//! before the bytes.
//!
//! **This is not an immutability claim.** These goldens are regenerated
//! deliberately when the SQL builders change (issue #57's
//! `regenerate_goldens`); such a change updates this constant in the same
//! commit, which is the reviewable act. What the freeze denies is a
//! SILENT edit, and a claim of zero edits that nobody can check.

use std::fs;
use std::path::{Path, PathBuf};

/// `traces_search` (issue #57) and `traces_metrics` (issue #59/#182):
/// the two byte-frozen SQL corpora, with their committed sizes. The
/// count is of EVERY file in the directory tree, not of `.sql` files —
/// today the two coincide, and a file of any other kind appearing is
/// precisely the thing the count should report.
const CORPORA: [(&str, usize); 2] = [("traces_search", 46), ("traces_metrics", 17)];

/// A 64-bit rolling digest over `(relative path, 0x01, bytes, 0x00)` for
/// every `.sql` file, in sorted path order — FNV-1a's shape with the
/// same mixing constants `accept_surface.rs` uses, deliberately, so the
/// two change-detectors in this repo are the same function (its
/// multiplier is not the textbook FNV prime; for a change-detector that
/// is immaterial, and matching the existing one is worth more than the
/// name). No new dependency, and the value is regenerated from the
/// assertion message.
///
/// Verified to certify the PRE-change corpus: recomputed independently
/// over `49cff9a`'s 63 golden blobs, it is this value — so the constant
/// pins the goldens as they stood before issue #335 Stage C, which is
/// what makes "Stage C edits zero SQL goldens" a checkable statement
/// rather than a claim.
///
/// **Never update this to make a run go green.** Moving it means one
/// thing: the frozen SQL corpus was deliberately regenerated, and the
/// change says which query's output moved and why.
const PINNED_SQL_CORPUS: u64 = 0x04ac_b7f2_1762_cfb7;

fn golden_dir(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
        .join(name)
}

/// Every file under `dir`, RECURSIVELY and regardless of extension,
/// returned as `(path relative to the corpus root, absolute path)` and
/// sorted by the relative path — which is what the digest feeds, so the
/// order is deterministic and a nested file is distinguishable from a
/// top-level one of the same name.
fn corpus_files(dir: &Path) -> Vec<(String, PathBuf)> {
    fn walk(dir: &Path, prefix: &str, out: &mut Vec<(String, PathBuf)>) {
        let mut entries: Vec<PathBuf> = fs::read_dir(dir)
            .unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()))
            .map(|entry| entry.expect("dir entry").path())
            .collect();
        entries.sort();
        for path in entries {
            let name = path
                .file_name()
                .and_then(|f| f.to_str())
                .unwrap_or_else(|| panic!("non-UTF-8 golden name: {}", path.display()))
                .to_string();
            let rel = format!("{prefix}{name}");
            if path.is_dir() {
                walk(&path, &format!("{rel}/"), out);
            } else {
                out.push((rel, path));
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, "", &mut out);
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

#[test]
fn the_sql_golden_corpus_has_exactly_its_committed_membership() {
    let mut total = 0usize;
    for (name, want) in CORPORA {
        let dir = golden_dir(name);
        let files = corpus_files(&dir);
        assert_eq!(
            files.len(),
            want,
            "golden/{name}/ holds {} files, not the committed {want} — something was added or \
             removed under that directory (the walk is recursive and counts EVERY file, not \
             only `.sql`); that is a deliberate act and moves this count with it: {:?}",
            files.len(),
            files.iter().map(|(rel, _)| rel).collect::<Vec<_>>()
        );
        total += files.len();
    }
    assert_eq!(total, 63, "the frozen SQL corpus is 46 + 17 = 63 files");
}

#[test]
fn the_sql_golden_corpus_matches_its_committed_digest() {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let mut feed = |b: &[u8]| {
        for byte in b {
            h ^= u64::from(*byte);
            h = h.wrapping_mul(0x1000_0000_01b3);
        }
    };
    for (name, _) in CORPORA {
        let dir = golden_dir(name);
        for (rel, path) in corpus_files(&dir) {
            // The path is fed BEFORE the bytes, so a rename — including a
            // move between the two corpora, or into a subdirectory —
            // moves the digest even though no byte of content changed.
            feed(format!("{name}/{rel}").as_bytes());
            feed(&[0x01]);
            feed(&fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display())));
            feed(&[0x00]);
        }
    }
    assert_eq!(
        h, PINNED_SQL_CORPUS,
        "the 63 frozen SQL goldens changed. This is not a constant to refresh: it means the \
         planner's or the SQL builders' output moved. If that was deliberate, regenerate the \
         goldens, say in the notes which query's SQL changed and why, and update \
         PINNED_SQL_CORPUS to {h:#x} in the same change — that edit is what makes 'zero SQL \
         golden edits' checkable from a diff"
    );
}
