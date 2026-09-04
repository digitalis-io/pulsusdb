//! Issue #335 Stage C: the SQL goldens' whole-corpus content freeze —
//! the mechanism that makes "this change edits zero SQL goldens" a fact a
//! reviewer can check **from the diff**, instead of a claim to be taken on
//! trust.
//!
//! **The problem it solves.** `golden/traces_search/*.sql` (49) and
//! `golden/traces_metrics/*.sql` (17) are the semantic witness every
//! TraceQL grammar change is measured against: they live in another crate
//! from `Display`, so they catch a meaning change our own rendering
//! cannot see. Every such change therefore reports "the 66 SQL goldens
//! take zero edits" — and until now that sentence was verifiable only by
//! trusting the author's `git status`, or by re-listing 66 paths by hand.
//! A reviewer working from a patch had no single artefact to look at.
//!
//! **The mechanism.** One digest over the whole corpus — every file's
//! relative path AND bytes, in sorted path order — pinned as a constant
//! in THIS SOURCE FILE, deliberately not beside the data (the
//! `accept_surface.rs::the_reference_column_is_frozen_against_silent_re_pinning`
//! posture): a data-only edit fails here, so any golden movement forces a
//! visible source-line change in the same diff. `PINNED_SQL_CORPUS`
//! unchanged in a diff therefore MEANS all 66 files are byte-identical —
//! one line to look at rather than 66.
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
//! walk is now recursive and digests EVERY entry it finds, whatever its
//! extension, keyed by its path RELATIVE to `tests/golden/`. Consequences
//! worth stating because they are the properties being bought: a file
//! nested one level down has a different relative path and so moves the
//! digest; a rename — including a move between the two golden
//! directories — moves it too, because the path is fed before the bytes;
//! an EMPTY directory moves it, because directories are entries in their
//! own right (round 3 — a file-only walk digests nothing for one); and a
//! symlink FAILS rather than being followed, so the digest cannot depend
//! on bytes outside the corpus and a directory cycle cannot hang the
//! walk.
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
const CORPORA: [(&str, usize); 2] = [("traces_search", 56), ("traces_metrics", 27)];

/// A 64-bit rolling digest over every entry, in sorted path order —
/// FNV-1a's shape with the same mixing constants `accept_surface.rs`
/// uses, deliberately, so the two change-detectors in this repo are the
/// same function (its multiplier is not the textbook FNV prime; for a
/// change-detector that is immaterial, and matching the existing one is
/// worth more than the name). No new dependency, and the value is
/// regenerated from the assertion message.
///
/// **The record is LENGTH-PREFIXED, not separator-delimited** (Stage C
/// review round 4). Framing each entry as `kind · len(path) · path ·
/// len(bytes) · bytes` makes the encoding injective by construction. The
/// separator form it replaces was ambiguous: with `0x01` between path and
/// content and `0x00` after, a file named `a\x01b` holding `X` fed
/// exactly the same bytes as a file named `a` holding `b\x01X` — two
/// different corpus states, one digest. Escaping `0x01` would have moved
/// that question to the next byte; a length prefix removes it.
///
/// Verified to certify the PRE-change corpus: recomputed independently
/// over `49cff9a`'s 63 golden blobs — under this encoding — it was
/// `0xfd4a_e0c5_99df_bc38`, so the constant pinned the goldens as they
/// stood before issue #335 Stage C, which is what made "Stage C edits
/// zero SQL goldens" a checkable statement rather than a claim.
///
/// **Never update this to make a run go green.** Moving it means one
/// thing: the frozen SQL corpus was deliberately regenerated, and the
/// change says which query's output moved and why.
///
/// **Moved on issue #351** (owner ruling, 2026-08-05), for ADDITIONS
/// only: three new `traces_search` cases pin the multi-valued event/link
/// value read's SQL — `event_name_vs_attr`, `event_name_vs_name_neq`
/// (the negated form, whose generator must fall back to the time-range
/// superset because a span with NO events matches `!=`), and
/// `event_time_since_start_vs_attr` (the numeric member, read from
/// `val_num`). The corpus went 63 → 66 entries; **no PRE-EXISTING
/// golden's bytes changed**, which the separate membership assertion
/// above makes visible — a count that moves by exactly the number of new
/// files, beside a digest that moves, reads differently from a digest
/// that moves alone.
///
/// Moved a second time in the same issue, after review: the three NEW
/// goldens were regenerated when the read dropped its
/// `groupUniqArray(...) GROUP BY` aggregate for a row-per-value
/// projection (the Layer-1 residual bound in the `traces::exec` module
/// doc — an array column is row-unbounded by construction). Still only
/// those three files; the other 63 are byte-identical to `49cff9a`.
///
/// **Moved on issue #252**, for THREE separate reasons in one change —
/// the situation this test's split count/digest reporting exists for:
///   1. `histogram_over_time_duration.sql` was REGENERATED. The fixed
///      14-entry cumulative `le` ladder became the reference's log2
///      tally (`GROUP BY toUInt64(roundToExp2(val - 1)) * 2` under an
///      outer `WHERE val >= 2`). Only the OUTER select and its trailing
///      clauses moved; the inner replay-dedup subquery is byte-identical,
///      which is what keeps the projection/granule gates valid.
///   2. Two goldens were ADDED — `docs_histogram_worked_example.sql` and
///      `docs_quantile_worked_example.sql`, the plans of the two queries
///      docs/api.md §4.4.1 documents, frozen so a documented example
///      cannot drift into one that no longer plans.
///   3. One non-`.sql` file was ADDED — `log2_reference_capture.json`,
///      the byte-committed capture from the pinned Tempo container. The
///      walk digests EVERY entry, whatever its extension, so it moves
///      both numbers by design. (Re-captured during review as corpora
///      were added to hold down the series-ORDER divergence —
///      `mix1024`, `mix16k`, `mixladder` — and the label-TEXT one —
///      `expform`. Each time the count stayed at 20, because those
///      corpora live INSIDE the existing JSON, and only the digest
///      moved: the split this test's two assertions exist to make
///      readable.) Its `_provenance.config` sentence was then CORRECTED
///      in the same issue — it claimed the reference needs a
///      `metrics_generator` block to answer TraceQL metrics at all,
///      which an A/B recorded on issue #252 refuted. Its
///      `_provenance.note` followed in the next review round — it
///      blamed metrics visibility lag on block completion, where the
///      same A/B shows the lag is the step's right edge. Digest-only
///      both times: no captured value moved.
///
/// Corpus 66 -> 69 entries; `quantile_over_time_multi.sql` and the other
/// 65 pre-existing goldens are byte-identical.
///
/// 69 -> 75 (issue #458): six ADDED `traces_metrics` goldens —
/// `nested_set_root_rate`, `nested_set_nonroot_rate`,
/// `nested_set_constant_true`, `nested_set_constant_false`,
/// `service_and_nested_set_root` and `bare_attr_truthiness` — for the two
/// metrics-filter lowerings that issue adds. **No pre-existing golden
/// moved:** the new lowerings are reached only where the old code
/// returned `Err`, so `git diff --stat crates/pulsus-read/tests/golden/`
/// shows additions and nothing else. The digest moves because the corpus
/// grew, not because any frozen byte changed.
///
/// 75 -> 76 (issue #460): ONE added `traces_metrics` golden,
/// `compare_status_window.sql` — the four-argument
/// `compare(f, topN, start, end)` the Traces Drilldown Comparison tab
/// generates. **No pre-existing golden moved, and `compare_status.sql`
/// specifically did not**: the `(start, end]` conjunct renders only when
/// the query carries a window, so the one-argument form's `is_sel` string
/// is byte-identical to before. That byte-identity is the check, not a
/// hope — `git diff --stat crates/pulsus-read/tests/golden/` shows one
/// addition and no modification, and this digest would move if it did
/// not. The digest moves because the corpus grew.
///
/// 76 -> 77 (issue #476 Wave B): ONE added `traces_search` golden,
/// `service_name_cross_type_eq.sql` — the cross-type `=` on
/// `resource.service.name` that Wave B turns from a planner `400` into an
/// accepted query matching no span. **No pre-existing golden moved:** the
/// new lowering is reached only where the old code returned `Err`, so
/// `git diff --stat crates/pulsus-read/tests/golden/` shows one addition
/// and no modification. The digest moves because the corpus grew.
///
/// 77 -> 77 (issue #479): **7 modified, none added.** The matched-span
/// projection fuses the matched `val` into the membership read of every
/// probe whose value a projection needs, so
/// `SELECT DISTINCT trace_id, span_id` becomes
/// `SELECT DISTINCT trace_id, span_id, <byte-capped val> AS v` in exactly
/// the seven `traces_search` goldens whose `phase2 membership` predicate
/// is not a plain `val = '…'` string equality:
/// `arith_attr_pushdown.sql`, `arith_fold.sql`,
/// `clustered_worked_example.sql`, `event_time_since_start_gt.sql`,
/// `existence_present.sql`, `val_num_range.sql`, `worked_example.sql`.
/// `existence_absent.sql` (`{ .a = nil }`) is the control and does NOT
/// move: its membership SQL is byte-identical to `existence_present`'s,
/// so the SQL alone cannot discriminate them — the negation does, and a
/// negated leaf projects nothing. `git diff --name-only -- /// crates/pulsus-read/tests/golden/traces_search/` shows exactly those
/// seven, and `CORPORA`'s count stays `50`.
///
/// **Digest-only on issue #477. The corpus stays at 77 entries: all 26
/// `traces_metrics` goldens were REGENERATED, 0 added, 0 removed**, and
/// the 50 `traces_search` goldens are byte-identical to the row above
/// this one (issue #477 moves no search golden; #479 moved seven, and
/// this row is stacked on that). Three things moved
/// in every regenerated file at once, which is why the whole corpus half
/// moves together.
///
/// (1) The range bucket label became RIGHT-CLOSED — `toStartOfInterval(
/// fromUnixTimestamp64Nano(timestamp_ns - 1), INTERVAL step_ns
/// NANOSECOND) + step_ms` in place of the left-edge millisecond form — so
/// a span landing on a grid point belongs to that point. The unit moved to
/// nanoseconds with the shift, because a millisecond interval rounds a
/// one-nanosecond shift away before it floors (measured on 26.3.17.110;
/// `metrics_sql::range_bucket_expr` carries the figures).
///
/// (2) The range window widened to `(aS - step, aE]`, rendered as
/// `>= 1699999920000000001 AND < 1700010840000000001`, one whole step
/// earlier and one nanosecond later than the instant window.
///
/// (3) Two sections were ADDED per file: a `range series probe` (or
/// `compare range series probe`) beside the frozen instant one, and an
/// `exemplars` section, since exemplars are now collected by default
/// rather than only under a `with()` hint.
///
/// The `instant (query)`, `series probe` and `compare series probe`
/// sections are byte-identical to `2f78c53` in all 26 files — the instant
/// route is #503's and this change moves none of its bytes. **The
/// committed base copy lives in `tests/golden/traces_metrics_base/`,
/// which is a SIBLING of the two walked roots and therefore outside this
/// corpus**: `CORPORA` is a fixed two-name list, so neither the count
/// (still 50 + 27 = 77) nor this digest can see it. Do not "fix" either
/// by adding it — that would freeze the base copy and defeat the
/// section-wise inverse the base copy exists for.
///
/// **Digest-only again on issue #477 wave 2. Still 77 entries; two of
/// the 26 `traces_metrics` goldens moved and no other file did** —
/// `rate_by_service.sql` and `sum_over_time_by_service.sql`, the two
/// grouped cases. Their `exemplars` section gained the group column the
/// grouped range query already groups by (`, service AS g0` in the
/// SELECT list, `GROUP BY t, g0`, `ORDER BY t ASC, g0`) so an exemplar
/// row says which series it belongs to. Nothing else in either file
/// moves, and the other 24 metrics goldens and all 50 search goldens are
/// byte-identical to the wave-1 corpus.
///
/// **Digest-only a third time, on the wave-2 review's ruling. Still 77
/// entries; six more of the 26 `traces_metrics` goldens moved and no
/// other file did** — `quantile_over_time_multi.sql`,
/// `docs_quantile_worked_example.sql`, `histogram_over_time_duration.sql`,
/// `docs_histogram_worked_example.sql`, `compare_status.sql` and
/// `compare_status_window.sql` (six files, three shapes). Only their
/// `exemplars` section moves, and for one reason: those three shapes
/// frame MANY series per bucket — one per `p`, one per `__bucket`, one
/// per `(__meta_type, attribute key)` — while their exemplar statement
/// returned only the time bucket, so the engine had nothing to join on
/// and attached every sample to the first series. Each statement now
/// returns its shape's own identity: the sampled span's duration for
/// quantile, the log2 bucket bound for histogram, `is_sel` + `akey` for
/// comparison. The other 20 metrics goldens and all 50 search goldens are
/// byte-identical to the wave-1-fix corpus.
///
/// **Digest-only a fourth time, and this one is a REVERT plus a rebase.
/// Still 77 entries.** Two things moved the number and they are
/// independent:
///
/// (a) Issue #477's own change: the wave-3 pooled placement domain was
/// withdrawn (the reference compares an exemplar against a distribution
/// it never draws — ledger row
/// `traceql-metrics-quantile-exemplar-placement-domain`), so
/// `quantile_over_time_multi.sql` and `docs_quantile_worked_example.sql`
/// went BACK to their wave-2-fix bytes: no `quantilesTDigestState` and no
/// `quantilesTDigestMerge(…) OVER ()`, just the sampled tuple. Those two
/// files are byte-identical to the wave-2-fix corpus and the other 24
/// metrics goldens never moved in wave 3 or 4 at all.
///
/// (b) The rebase onto issue #479, whose row above this one moved SEVEN
/// `traces_search` goldens. Issue #477 moves none of them, so the search
/// half of the corpus is exactly #479's.
///
/// Both together are what this constant is over: with (a) alone it would
/// be the wave-2-fix value, and with (b) alone #479's.
/// **Moved on issue #492, for ADDITIONS only. 77 -> 83 entries: six
/// ADDED `traces_search` goldens, and no pre-existing golden moved by one
/// byte** — `git diff --stat -- crates/pulsus-read/tests/golden/` shows
/// six additions and no modification, which is what the separate
/// membership assertion above makes visible: a count that moves by
/// exactly the number of new files, beside a digest that moves, reads
/// differently from a digest that moves alone.
///
/// The six are the queries issue #492 names, committed so that "wave 1
/// moves no SQL" is a claim about text a reviewer can diff rather than a
/// sentence to take on trust: `issue492_attr_eq`,
/// `issue492_attr_eq_with_max_duration`, `issue492_by_then_count`,
/// `issue492_count_then_by`, `issue492_select_span_attr` and
/// `issue492_mixed_source_or`. Two of them are PAIRS whose SQL is
/// byte-identical below the header — an aggregate that contributes no
/// SQL, and a pipeline order the SQL cannot see — and
/// `traces_search_sql.rs`'s
/// `the_aggregate_and_the_ordering_pairs_send_byte_identical_sql`
/// asserts that on the rendered composite rather than on the files.
///
/// **Adding a golden is not moving SQL, but it does move this digest**,
/// and moving the digest constant is the reviewable act.
///
/// **Digest-only on issue #510. The corpus stays at 83 entries: 13
/// `traces_search` goldens were regenerated, 0 added, 0 removed**, and
/// every `traces_metrics` golden is byte-identical to the row above.
/// `git diff --name-only -- crates/pulsus-read/tests/golden/` lists
/// exactly those 13 files, and
/// `git diff -- crates/pulsus-read/tests/golden/traces_search/ | grep -E
/// '^[-+][^-+]' | sort | uniq -c` shows SIX distinct lines and no others:
/// three `SELECT` projections, each in its before and after form.
///
/// One column was added to each of the three attribute VALUE projections
/// so the response can render a value in the arm the sender stored it as:
///
/// ```text
/// SELECT trace_id, span_id, any(val_num) AS v                     -> …, any(val_type) AS t   (6 occurrences)
/// SELECT trace_id, span_id, any(<byte-capped val>) AS v           -> …, any(val_type) AS t   (7 occurrences)
/// SELECT DISTINCT trace_id, span_id, <byte-capped val> AS v       -> …, val_type AS t        (7 occurrences)
/// ```
///
/// **The hot no-value membership read did NOT move.** `SELECT DISTINCT
/// trace_id, span_id` — the statement every string-equality attribute
/// condition issues — appears in the corpus unchanged, which is why no
/// `-SELECT DISTINCT trace_id, span_id` line is in that diff at all. Only
/// the `with_value` arm of `membership_sql` grew a column, and
/// `existence_absent.sql` stays byte-identical while
/// `existence_present.sql` moves, which is the pair that shows the split
/// is the projection and not the predicate.
///
/// No `WHERE` clause, index prefix, date/time clause or `trace_id IN`
/// restriction moved in any of the 13, so part and granule selection
/// cannot have moved either —
/// `traces_search_explain.rs::attr_value_reads_keep_their_index_selection`
/// gates that as an identity rather than leaving it as this sentence.
const PINNED_SQL_CORPUS: u64 = 0x5b8b_80d7_38cb_049b;

fn golden_dir(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
        .join(name)
}

/// One entry in the frozen tree: a file whose bytes are digested, or a
/// directory, which is digested as a NAME ONLY.
///
/// Directories are represented (Stage C review round 3) so that an EMPTY
/// one is not invisible: a walk that only yields files digests nothing
/// for `traces_search/scratch/`, and a directory appearing inside a
/// frozen corpus is a change like any other. The two kinds carry
/// different KIND bytes, so a directory `foo` and a file `foo` cannot
/// collide.
enum Entry {
    File { rel: String, path: PathBuf },
    Dir { rel: String },
}

impl Entry {
    fn rel(&self) -> &str {
        match self {
            Entry::File { rel, .. } | Entry::Dir { rel } => rel,
        }
    }
}

/// Everything under `dir`, RECURSIVELY, sorted by the path relative to
/// the corpus root — which is what the digest feeds, so the order is
/// deterministic and a nested entry is distinguishable from a top-level
/// one of the same name.
///
/// **Symlinks are rejected outright, not followed.** `is_dir()` resolves
/// through a symlink, so the previous walk would have recursed forever
/// on a directory-symlink cycle and would have digested bytes from
/// outside the repository on an external target — a non-hermetic freeze.
/// `symlink_metadata` never follows, and a symlink appearing inside a
/// directory that is supposed to be frozen is itself a change worth
/// failing on, which is why this rejects rather than resolves.
///
/// **The ROOT is held to the same rule as its children** (Stage C review
/// round 4). Checking every entry `read_dir` yields while never checking
/// the directory `read_dir` was called on left the whole corpus
/// substitutable: replace `traces_search/` itself with a symlink and the
/// walk followed it, digesting a tree from anywhere. The root is checked
/// before it is read.
///
/// **Stated edge, deliberately not closed** (the review classified it a
/// stated edge, not a demonstrated bypass; the cap on this apparatus is
/// deliberate): a FIFO or a device node is neither a symlink nor a
/// directory, so it is walked as a file and `fs::read` is called on it —
/// a FIFO would block the suite until something writes, and a character
/// device would feed bytes that are not corpus content. Reaching it
/// requires `mknod`/`mkfifo` inside `tests/golden/`, which is not
/// something a patch can do and not something git can carry: git stores
/// only regular files, symlinks and gitlinks, so neither can arrive
/// through a PR. Closing it would be one `kind.is_file()` assertion if
/// that ever changes.
fn corpus_entries(dir: &Path) -> Vec<Entry> {
    fn walk(dir: &Path, prefix: &str, out: &mut Vec<Entry>) {
        let mut paths: Vec<PathBuf> = fs::read_dir(dir)
            .unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()))
            .map(|entry| entry.expect("dir entry").path())
            .collect();
        paths.sort();
        for path in paths {
            let name = path
                .file_name()
                .and_then(|f| f.to_str())
                .unwrap_or_else(|| panic!("non-UTF-8 golden name: {}", path.display()))
                .to_string();
            let rel = format!("{prefix}{name}");
            let meta = fs::symlink_metadata(&path)
                .unwrap_or_else(|e| panic!("symlink_metadata {}: {e}", path.display()));
            let kind = meta.file_type();
            assert!(
                !kind.is_symlink(),
                "{} is a symlink. A frozen corpus holds real files: a symlink makes the digest \
                 depend on something outside it (and a directory cycle would make this walk \
                 never terminate). Remove it, or freeze what it points at.",
                path.display()
            );
            if kind.is_dir() {
                out.push(Entry::Dir { rel: rel.clone() });
                walk(&path, &format!("{rel}/"), out);
            } else {
                out.push(Entry::File { rel, path });
            }
        }
    }
    let root = fs::symlink_metadata(dir)
        .unwrap_or_else(|e| panic!("symlink_metadata {}: {e}", dir.display()));
    assert!(
        !root.file_type().is_symlink(),
        "the corpus root {} is a symlink. The whole frozen directory would be substitutable: \
         the walk would digest whatever tree it points at. Remove it, or freeze what it points \
         at.",
        dir.display()
    );
    assert!(
        root.file_type().is_dir(),
        "the corpus root {} is not a directory",
        dir.display()
    );
    let mut out = Vec::new();
    walk(dir, "", &mut out);
    out.sort_by(|a, b| a.rel().cmp(b.rel()));
    out
}

#[test]
fn the_sql_golden_corpus_has_exactly_its_committed_membership() {
    let mut total = 0usize;
    for (name, want) in CORPORA {
        let dir = golden_dir(name);
        let entries = corpus_entries(&dir);
        assert_eq!(
            entries.len(),
            want,
            "golden/{name}/ holds {} entries, not the committed {want} — something was added or \
             removed under that directory (the walk is recursive and counts EVERY entry: files \
             of any extension AND directories); that is a deliberate act and moves this count \
             with it: {:?}",
            entries.len(),
            entries.iter().map(Entry::rel).collect::<Vec<_>>()
        );
        total += entries.len();
    }
    assert_eq!(total, 83, "the frozen SQL corpus is 56 + 27 = 83 entries");
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
    // One record per entry: KIND · len(path) · path · len(bytes) · bytes,
    // every length an 8-byte big-endian count. Length-prefixed rather
    // than separator-delimited, so the encoding is injective: no two
    // corpus states can produce the same byte stream, whatever bytes
    // appear inside a name or a file. The path is still fed before the
    // content, so a rename — including a move between the two corpora or
    // into a subdirectory — moves the digest with no byte of content
    // changed; a directory carries its own KIND and an empty content
    // field, so an empty directory is visible and cannot be confused
    // with a file of the same name.
    const KIND_FILE: u8 = 0x01;
    const KIND_DIR: u8 = 0x02;
    for (name, _) in CORPORA {
        let dir = golden_dir(name);
        for entry in corpus_entries(&dir) {
            let (kind, rel, content) = match entry {
                Entry::File { rel, path } => (
                    KIND_FILE,
                    rel,
                    fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display())),
                ),
                Entry::Dir { rel } => (KIND_DIR, rel, Vec::new()),
            };
            let key = format!("{name}/{rel}");
            feed(&[kind]);
            feed(&(key.len() as u64).to_be_bytes());
            feed(key.as_bytes());
            feed(&(content.len() as u64).to_be_bytes());
            feed(&content);
        }
    }
    assert_eq!(
        h, PINNED_SQL_CORPUS,
        "the 83 frozen SQL goldens changed. This is not a constant to refresh: it means the \
         planner's or the SQL builders' output moved. If that was deliberate, regenerate the \
         goldens, say in the notes which query's SQL changed and why, and update \
         PINNED_SQL_CORPUS to {h:#x} in the same change — that edit is what makes 'zero SQL \
         golden edits' checkable from a diff"
    );
}

/// ADR 0008 D2: **no emitted SQL contains a `WITH` clause.**
///
/// The common-table form was rejected on measurement, not on taste, and
/// the decision needs a check that fails when a wave writes one — the
/// alternative is a rule nobody can see being broken.
///
/// **This is vacuous today and says so.** At base the corpus contains no
/// lowered SQL at all, so the assertion is green over a population
/// holding none of the case it exists for. It becomes a real check the
/// moment a wave emits a wrapped statement into the corpus (ADR 0008 D1),
/// which is why it is written now rather than then: a gate added
/// alongside the first violation is a gate nobody ever saw fail.
///
/// The word is matched case-insensitively and only where it stands as a
/// whole token, so `WITH`, `with` and a leading `\nWITH` all trip it
/// while `subqueries_with_x` and a `with(...)` hint inside a `-- q:`
/// header line do not.
#[test]
fn the_golden_sql_corpus_contains_no_with_clause() {
    let mut scanned = 0usize;
    let mut entries = 0usize;
    let mut statements = 0usize;
    for (name, _) in CORPORA {
        let dir = golden_dir(name);
        for entry in corpus_entries(&dir) {
            let Entry::File { rel, path } = entry else {
                continue;
            };
            entries += 1;
            if !rel.ends_with(".sql") {
                continue;
            }
            scanned += 1;
            let text = fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
            for (i, line) in text.lines().enumerate() {
                // The two header lines carry the case name and the query
                // text, not SQL; a `with(...)` search hint lives there.
                if line.starts_with("-- ") {
                    continue;
                }
                if line.starts_with("== ") {
                    statements += 1;
                    continue;
                }
                let has_with = line
                    .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                    .any(|tok| tok.eq_ignore_ascii_case("with"));
                assert!(
                    !has_with,
                    "{name}/{rel}:{}: ADR 0008 D2 bans the common-table form; the measured \
                     alternative is a materialised value list. Line: {line}",
                    i + 1
                );
            }
        }
    }
    // The scan is not vacuous in the OTHER direction: it really did read
    // the corpus. (It IS vacuous in the direction that matters until a
    // wave emits a wrapped statement, which the doc comment states.)
    assert_eq!(
        entries, 83,
        "every committed corpus entry is walked (the same 83 the membership gate counts)"
    );
    assert_eq!(
        scanned, 82,
        "every SQL golden is scanned; the one entry that is not a statement is          `traces_metrics/log2_reference_capture.json`"
    );
    assert!(
        statements > 100,
        "the corpus holds the statements this rule is about: {statements}"
    );
}
