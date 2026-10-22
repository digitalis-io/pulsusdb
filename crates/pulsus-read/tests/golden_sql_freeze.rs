//! Issue #335 Stage C: the SQL goldens' whole-corpus content freeze —
//! the mechanism that makes "this change edits zero SQL goldens" a fact a
//! reviewer can check **from the diff**, instead of a claim to be taken on
//! trust.
//!
//! **What it covers.** The two statement corpora: `golden/traces_spans_search/`
//! (the search statement, issue #590) and `golden/traces_spans_metrics/`
//! (the metrics statement, issue #596). The previous metrics SQL's corpus,
//! `golden/traces_metrics/`, and its digest went with that SQL (#602 part
//! 3a), as the previous search engine's corpus went with it (#602 part
//! 2c-2b-1).
//!
//! **The mechanism.** One digest per corpus — every entry's path relative
//! to `tests/golden/` AND bytes, in sorted path order — pinned as a
//! constant in THIS SOURCE FILE, deliberately not beside the data (the
//! `accept_surface.rs::the_reference_column_is_frozen_against_silent_re_pinning`
//! posture): a data-only edit fails here, so any golden movement forces a
//! visible source-line change in the same diff. Each corpus has its own
//! digest, so no digest moves when only another corpus does.
//!
//! The count is asserted separately from the digests so the failure says
//! which happened: a file added or removed reads differently from a file
//! edited, and lumping them into one hash tells a reviewer neither.
//!
//! **The unit is the DIRECTORY, not the `.sql` files in it** (Stage C
//! review, [low]). The walk is recursive and digests EVERY entry it finds,
//! whatever its extension. A file nested one level down has a different
//! relative path and so moves the digest; a rename moves it too, because
//! the path is fed before the bytes; an EMPTY directory moves it, because
//! directories are entries in their own right; and a symlink FAILS rather
//! than being followed, so the digest cannot depend on bytes outside the
//! corpus and a directory cycle cannot hang the walk.
//!
//! **This is not an immutability claim.** These goldens are regenerated
//! deliberately when the statement builders change; such a change updates
//! the corpus's constant in the same commit, which is the reviewable act.
//! What the freeze denies is a SILENT edit, and a claim of zero edits that
//! nobody can check.

use std::fs;
use std::path::{Path, PathBuf};

/// `traces_spans_search` (#590) and `traces_spans_metrics` (#596 part 1):
/// the two byte-frozen SQL corpora, with their committed sizes. The count
/// is of EVERY file in the directory tree, not of `.sql` files — today the
/// two coincide, and a file of any other kind appearing is precisely the
/// thing the count should report.
///
/// Each is pinned by its own digest, so no digest moves when only
/// another's does.
const CORPORA: [(&str, usize); 2] = [("traces_spans_search", 18), ("traces_spans_metrics", 23)];

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
    assert_eq!(total, 41, "the frozen SQL corpus is 18 + 23 = 41 entries");
}

/// The search statement's eighteen goldens: issue #590's seven —
/// `match_all`, `service`, `span_attribute`, `resource_attribute`,
/// `event_intrinsic`, `demand` and `limits` — regenerated by issue #591
/// part 1, and its six — `spanset_and`, `spanset_or`, `spanset_nested`,
/// `name_and_kind`, `literal_and_stored` and `instrumentation`; part 2
/// regenerated `resource_attribute` and `event_intrinsic`, whose
/// projections it serves, and added four — `unscoped_span`,
/// `unscoped_resource`, `event_element` and `link_element`; issue #602
/// part 1 regenerated twelve, whose presence tests read only the types the
/// reference holds and whose `service.name` projection reads only a
/// string; issue #602 part 2a regenerated `spanset_and`, `spanset_or` and
/// `spanset_nested`, whose spanset tree is now the fold's selector; part
/// 2b added `structural_child`; issue #600 part 3 regenerated
/// `unscoped_span`, `unscoped_resource`, `event_element` and
/// `link_element`, whose projection reads the last element; issue #601
/// part 4a regenerated every one, whose span reads keep one row per span
/// identity and whose child counts count each span once; part 4c
/// regenerated every one, whose projection, fold columns and selector
/// flags are computed inside that read. Each
/// is one statement of `crates/pulsus-read/src/traces/spans/search.rs`'s
/// `search_sql`, under [`corpus_digest`]'s encoding.
const PINNED_SPANS_SEARCH_CORPUS: u64 = 0x12e3_5061_8645_68f2;

/// The metrics statement's twenty-three goldens (issue #596 parts 1 and 2,
/// issue #594 part 5; five regenerated by issue #602 part 1, whose presence
/// tests read only the types the reference holds; three by issue #600
/// part 3, whose `*_over_time` exemplar carries its span's own value;
/// thirteen by issue #600 part 1b-3, whose series key folds a value with no
/// TraceQL type into the missing series; all twenty-three by issue #601
/// part 2, whose instant form reads the requested window unsnapped — only
/// the instant statements' bounds and `rate` divisor move; twenty-one by
/// issue #601 part 4a, whose span reads keep one row per span identity, a
/// count and a rate counting each identity once — the two that read the
/// numbering keep `final` and do not move; one, `sum_errors`, by issue #601
/// part 4b, whose `{ status = error }` filter reads `error_spans`; three by
/// part 4c, whose key array or attribute target is computed inside the read
/// that keeps one row per span identity — a count and a rate have no such
/// read, and a statement with neither reads no value through it), each the
/// range and instant forms of one request's statement, rendered by
/// `crates/pulsus-read/src/traces/spans/metrics.rs`'s
/// `plan_trace_metrics_routed`, under [`corpus_digest`]'s encoding.
const PINNED_SPANS_METRICS_CORPUS: u64 = 0x3032_bb57_79ec_d2c5;

#[test]
fn the_search_statement_golden_corpus_matches_its_committed_digest() {
    let h = corpus_digest(&["traces_spans_search"]);
    assert_eq!(
        h, PINNED_SPANS_SEARCH_CORPUS,
        "the 18 search statement goldens changed: `search_sql`'s output moved. If that was \
         deliberate, regenerate them, say which statement changed and why, and update \
         PINNED_SPANS_SEARCH_CORPUS to {h:#x} in the same change"
    );
}

#[test]
fn the_metrics_statement_golden_corpus_matches_its_committed_digest() {
    let h = corpus_digest(&["traces_spans_metrics"]);
    assert_eq!(
        h, PINNED_SPANS_METRICS_CORPUS,
        "the 23 metrics statement goldens changed: the metrics statement's output moved. If that \
         was deliberate, regenerate them, say which statement changed and why, and update \
         PINNED_SPANS_METRICS_CORPUS to {h:#x} in the same change"
    );
}

/// The digest of the named corpora, in the order given, each entry in
/// sorted path order.
fn corpus_digest(names: &[&str]) -> u64 {
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
    for name in names {
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
    h
}

// ADR 0008 D2 — **the check that enforces it lives in
// `tests/live_sql_corpus_ast.rs`** (issue #549), not here.
//
// A text scan for the word `WITH` stood in this file until then. It was
// the seventh such rule and, like the six before it, it asserted a
// property of the TEXT; the rule is about what the SQL *is*, so the
// check now asks ClickHouse's own parser for the AST and asserts that no
// statement in the committed corpus carries a `WithElement` node. That
// needs a server, so it moved into the live-gated set.
//
// Two consequences worth stating rather than leaving implicit:
//
// * **This file's digest is unchanged by the move.** The freeze still
//   pins the corpus bytes; it simply no longer claims anything about
//   what those bytes mean.
// * **The hermetic half cannot see the binding property at all.** That
//   is a stated limit, not a gate that implies otherwise.

/// The root of the golden tree — every committed golden, not only the
/// two byte-frozen corpora in [`CORPORA`].
fn golden_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
}

/// The committed goldens that carry a join today, none of which the
/// compile core plans. The list is asserted as an EQUALITY, not used as
/// a skip list: a join in any file it does not name fails this test.
const JOINS_OUTSIDE_THE_COMPILED_SEARCH_CORPUS: [&str; 43] = [
    // Issue #599: the service graph's statement pairs a raw region's
    // spans by `ARRAY JOIN` over each key's server halves, an array of the
    // grouped row itself; built by `spans::graph::spans_graph_sql`, which
    // the compile core does not plan.
    "traces_spans_graph/clustered.sql",
    "traces_spans_graph/single_node.sql",
    // Issue #596 part 1: the metrics statement's `ARRAY JOIN` expands a
    // series' cells, or a bucket's quantiles, into rows — an array of the
    // row itself, not a second table. It is the design's own statement,
    // built by `spans::metrics::plan_trace_metrics_routed`, which the compile core
    // does not plan; every one of its goldens carries one, and part
    // 2's per-trace, numbering and child-count reads are `LEFT JOIN`s.
    "traces_spans_metrics/arithmetic.sql",
    "traces_spans_metrics/avg_attribute.sql",
    "traces_spans_metrics/avg_unscoped.sql",
    "traces_spans_metrics/by_child_count.sql",
    "traces_spans_metrics/by_nested_parent.sql",
    "traces_spans_metrics/by_root_service.sql",
    "traces_spans_metrics/by_route.sql",
    "traces_spans_metrics/count_compared.sql",
    "traces_spans_metrics/count_no_exemplars.sql",
    "traces_spans_metrics/explore_root.sql",
    "traces_spans_metrics/field_vs_field.sql",
    "traces_spans_metrics/filter_child_count.sql",
    "traces_spans_metrics/filter_nested_left.sql",
    "traces_spans_metrics/filter_trace_duration.sql",
    "traces_spans_metrics/histogram.sql",
    "traces_spans_metrics/histogram_by_service.sql",
    "traces_spans_metrics/quantile_by_route.sql",
    "traces_spans_metrics/quantile_three.sql",
    "traces_spans_metrics/rate_by_service.sql",
    "traces_spans_metrics/rate_by_service_topk.sql",
    "traces_spans_metrics/rate_match_all.sql",
    "traces_spans_metrics/sum_errors.sql",
    "traces_spans_metrics/two_keys.sql",
    // Issue #590's decision 5: the search statement's `LEFT JOIN traces`
    // takes each returned trace's root, extent and duration, and keeps a
    // trace the per-trace table has not indexed. It is the design's own
    // statement (`docs/TraceQL/sql-schema.md` §5.2), built by
    // `spans::search::search_sql`, which the old compile core does not
    // plan.
    "traces_spans_search/demand.sql",
    "traces_spans_search/event_element.sql",
    "traces_spans_search/event_intrinsic.sql",
    "traces_spans_search/instrumentation.sql",
    "traces_spans_search/limits.sql",
    "traces_spans_search/link_element.sql",
    "traces_spans_search/literal_and_stored.sql",
    "traces_spans_search/match_all.sql",
    "traces_spans_search/name_and_kind.sql",
    "traces_spans_search/resource_attribute.sql",
    "traces_spans_search/service.sql",
    "traces_spans_search/span_attribute.sql",
    "traces_spans_search/spanset_and.sql",
    "traces_spans_search/spanset_nested.sql",
    "traces_spans_search/spanset_or.sql",
    "traces_spans_search/structural_child.sql",
    "traces_spans_search/unscoped_resource.sql",
    "traces_spans_search/unscoped_span.sql",
];

/// ADR 0008: **no statement the compile core plans may contain a join**,
/// and the committed goldens that carry one, pinned by name.
///
/// **Issue #596 part 1 adds twelve, part 2 eight, and issue #594 part 5
/// three**: the
/// `traces_spans_metrics/` goldens, whose `ARRAY JOIN` expands an array of
/// the row itself and whose `LEFT JOIN`s read the per-trace values, the
/// numbering and the child counts, built by `spans::metrics`, which the
/// compile core does not plan.
///
/// **Issue #590 adds seven, issue #591 part 1 six more, part 2 four, and issue #602 part 2b one**: the `traces_spans_search/` goldens, whose
/// statement's `LEFT JOIN traces` the design names (`docs/TraceQL/sql-
/// schema.md` §5.2). That builder is not the compile core, which plans
/// only `traces_search/`, so the rule below does not reach it; ADR 0008's
/// decision record says the same.
///
/// # The ADR's rule sentence is wider than the decision it records, and this test does not change it
///
/// ADR 0008 is titled "SQL composition for lowered query pipelines" and
/// its three rules govern how a lowered pipeline composes. The rule
/// added on 2026-09-02 is written without that qualifier — "no emitted
/// SQL may contain a join until this ADR is amended to name the clause"
/// (`docs/decisions/0008-sql-composition-for-lowered-pipelines.md:201`,
/// the same claim in the summary at line 11). The committed goldens named
/// above carry a join and **none is planned by the compile core**: each
/// is a statement of `spans/graph.rs`, `spans/metrics.rs` or
/// `spans/search.rs`, hand-written builders that are not lowered
/// pipelines, so the decision never reached them. The previous metrics and
/// graph SQL's goldens, which carried joins too, went with that SQL (#602
/// part 3a). The sentence reaches further than the decision it records: a
/// drafting fault in the record, not shipped code breaking a rule. **The
/// wording belongs to the amendment round ADR 0008 already reserves.**
/// This test enforced the rule the ADR can mean — zero joins in the
/// compiled search corpus — until #602 part 2c-2b-1 deleted that corpus;
/// it now pins the join files by name, so a join in any other file fails;
/// `docs/query-lowering.md` §9.8 carries the same record.
///
/// # Why this walks the golden root and not `CORPORA`
///
/// Written over `CORPORA` this gate could not see two of the join files:
/// `traces_spans_graph/` is not in `CORPORA`, so the digest gates above
/// do not reach it either.
///
/// # Where this stops
///
/// It freezes the CORPUS, not the builders. A builder could emit a join
/// on an input no golden covers, and nothing here would see it.
#[test]
fn every_join_in_the_golden_corpus_is_named() {
    let root = golden_root();
    let mut scanned = 0usize;
    let mut with_a_join: Vec<String> = Vec::new();

    for entry in corpus_entries(&root) {
        let Entry::File { rel, path } = entry else {
            continue;
        };
        if !rel.ends_with(".sql") {
            continue;
        }
        scanned += 1;
        let text =
            fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let mut this_file_joins = false;
        for line in text.lines() {
            // The two header lines carry the case name and the query
            // text, not SQL — and one case is literally named
            // `attr_semi_join`, whose body renders `IN (SELECT …)` and
            // holds no join at all.
            if line.starts_with("-- ") {
                continue;
            }
            let has_join = line
                .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                .any(|tok| tok.eq_ignore_ascii_case("join"));
            if !has_join {
                continue;
            }
            this_file_joins = true;
        }
        if this_file_joins {
            with_a_join.push(rel);
        }
    }

    with_a_join.sort();
    assert_eq!(
        with_a_join, JOINS_OUTSIDE_THE_COMPILED_SEARCH_CORPUS,
        "the joins outside the compiled search corpus are exactly these; a join in \
         any other file means a builder grew one and nobody said so"
    );
    assert_eq!(
        scanned, 43,
        "every committed SQL golden in the tree is scanned, not only the two frozen corpora"
    );
}
