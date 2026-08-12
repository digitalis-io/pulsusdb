//! Issue #261. The `/detected_labels` half of the registered cardinality
//! divergence: PulsusDB reports the EXACT distinct-value count where the
//! reference reports a p14 HyperLogLog estimate.
//!
//! Nothing here changes behaviour, and nothing here proves the endpoint
//! works — the live gates in `crates/pulsus-server/tests/logs_detected_live.rs`
//! and `crates/pulsus-read/tests/query_log_gates.rs` do that. Neither of
//! those discriminates this change: #261 edited no `src/`, so the code
//! they exercise IS the pre-#261 code and they pass against it by
//! construction. What this file gates is the pair of DOCUMENTED claims
//! #261 corrected:
//!
//! 1. the retracted universal agreement threshold ("the estimate equals
//!    the exact count up to 5327") must not come back, in the ledger or
//!    on the public API surface; and
//! 2. the capture that replaced it must keep discriminating — every row
//!    of the artifact disagrees with us, and the artifact contains the
//!    two facts that make the universal reading impossible (a family
//!    that diverges BELOW 5328, and one `N` at which three families give
//!    three different reference answers).
//!
//! Hermetic: reads committed bytes and renders production SQL. No
//! container, no network.

use std::collections::{HashMap, HashSet};

use pulsus_read::logql::predicate::month_literal;
use pulsus_read::logql::sql;

/// The `/detected_labels` capture. Deliberately a NEW artifact rather
/// than a column added to `golden/detected_cardinality/reference_divergence.tsv`,
/// whose 5-column shape is frozen by `detected_fields_witness.rs`'s AC 19.
const TSV: &str = include_str!("golden/detected_labels_cardinality/reference_divergence.tsv");

fn repo_file(rel: &str) -> String {
    let path = format!("{}/../../{rel}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

fn crate_file(rel: &str) -> String {
    let path = format!("{}/{rel}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

/// One row of the capture.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Row {
    family: String,
    n_distinct: u64,
    reference_value: u64,
    pulsus_exact: u64,
    observed_by: String,
    ledger_id: String,
    note: String,
}

/// The `#`-comment header, verbatim, minus the leading `# `.
fn header(tsv: &str) -> String {
    tsv.lines()
        .take_while(|l| l.starts_with('#'))
        .map(|l| l.trim_start_matches('#').trim_start())
        .collect::<Vec<_>>()
        .join("\n")
}

fn parse_rows(tsv: &str) -> Vec<Row> {
    let mut rows = Vec::new();
    for line in tsv.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let cols: Vec<&str> = line.split('\t').collect();
        assert_eq!(cols.len(), 7, "row must have 7 columns: {line:?}");
        for (i, c) in cols.iter().enumerate() {
            assert!(!c.trim().is_empty(), "column {i} is empty in {line:?}");
        }
        rows.push(Row {
            family: cols[0].to_string(),
            n_distinct: cols[1].parse().expect("n_distinct"),
            reference_value: cols[2].parse().expect("reference_value"),
            pulsus_exact: cols[3].parse().expect("pulsus_exact"),
            observed_by: cols[4].to_string(),
            ledger_id: cols[5].to_string(),
            note: cols[6].to_string(),
        });
    }
    rows
}

/// Every column of every row carries a rule — including `note` and
/// `observed_by`, which look like free text and are the two places a
/// silently-unprovenanced row would hide:
///
/// (a) our side is EXACT (`pulsus_exact == n_distinct`);
/// (b) the row DISAGREES (`reference_value != pulsus_exact`) — an
///     agreeing point discriminates nothing and belongs in the header as
///     a last-agreeing witness, not in the body;
/// (c) `observed_by` is one of the two accepted instruments;
/// (d) `ledger_id` occurs verbatim in the ledger, which names #261;
/// (e) `note` is present and `(family, n_distinct)` is unique;
/// (f) the two mandatory CONTAINER rows are present with their measured
///     numbers;
/// (g) a mandatory row sits BELOW 5328 — the standing guard against the
///     retracted universal threshold being re-derived from this file;
/// (h) at least two families share one `n_distinct` and answer
///     DIFFERENTLY — the artifact's own proof that the threshold is not
///     a function of `N`;
/// (i) the header pins the capture conditions (image tag + reference
///     SHA) and records the last-agreeing witnesses, which must NOT also
///     appear as rows.
#[test]
fn detected_labels_divergence_rows_hold_and_the_ledger_names_them() {
    let rows = parse_rows(TSV);
    let ledger = repo_file("docs/benchmarks/logs-differential-ledger.md");
    assert!(ledger.contains("#261"), "the ledger must name issue #261");

    let mut seen: HashSet<(String, u64)> = HashSet::new();
    let mut by_n: HashMap<u64, HashSet<u64>> = HashMap::new();
    for r in &rows {
        assert_eq!(r.pulsus_exact, r.n_distinct, "(a) we are exact: {r:?}");
        assert_ne!(
            r.reference_value, r.pulsus_exact,
            "(b) a kept row must DISAGREE: {r:?}"
        );
        assert!(
            r.observed_by == "container" || r.observed_by == "library",
            "(c) observed_by must be `container` or `library`: {r:?}"
        );
        assert!(
            ledger.contains(&r.ledger_id),
            "(d) the ledger is missing {:?}",
            r.ledger_id
        );
        assert!(!r.note.trim().is_empty(), "(e) every row carries a note");
        assert!(
            seen.insert((r.family.clone(), r.n_distinct)),
            "(e) duplicate (family, n_distinct): {r:?}"
        );
        by_n.entry(r.n_distinct)
            .or_default()
            .insert(r.reference_value);
    }

    // (f) + (g): the two container-observed points, one of them below
    // the retracted 5328.
    for (family, n, reference) in [("svc-", 4533u64, 4532u64), ("pod-", 7708, 7640)] {
        let row = rows
            .iter()
            .find(|r| r.family == family && r.n_distinct == n)
            .unwrap_or_else(|| panic!("(f) the mandatory {family}{n} row is missing"));
        assert_eq!(row.reference_value, reference, "(f) {family}{n}");
        assert_eq!(
            row.observed_by, "container",
            "(f) {family}{n} must be container-observed, not derived"
        );
    }
    assert!(
        rows.iter()
            .any(|r| r.observed_by == "container" && r.n_distinct < 5328),
        "(g) a container-observed divergence BELOW 5328 must stay in the artifact — \
         it is the counterexample to the retracted universal threshold"
    );

    // (h) The ledger asserts "one `N`, three reference answers". That
    // sentence is a figure describing THIS artifact, so it is recomputed
    // here and matched against the prose instead of being trusted: the
    // widest disagreement in the file is counted, and the ledger must
    // spell out that same number. Deleting a row therefore cannot leave
    // the sentence standing — the count drops and the words no longer
    // match.
    let widest = by_n.values().map(HashSet::len).max().unwrap_or(0);
    assert!(
        widest >= 3,
        "(h) the artifact must keep at least one `n_distinct` at which THREE \
         families give three different reference answers (widest is {widest}); \
         two would still read as a table of thresholds indexed by N, and the \
         ledger's own sentence claims three"
    );
    let spelled = ["zero", "one", "two", "three", "four", "five", "six"]
        .get(widest)
        .copied()
        .unwrap_or_else(|| panic!("(h) extend the number words past {widest}"));
    let ledger_norm = normalize(&ledger);
    assert!(
        ledger_norm.contains(&format!("one n, {spelled} reference answers")),
        "(h) the ledger must state the widest disagreement this artifact \
         actually carries — {widest} answers at one N, i.e. \
         \"one `N`, {spelled} reference answers\""
    );

    // (i) capture conditions and the agreeing witnesses.
    let head = header(TSV);
    for needle in [
        "grafana/loki:3.7.4",
        "b318f2829f0ae2094ab3a1e90780450e9e4b03be",
        "hyperloglog v0.2.6",
    ] {
        assert!(head.contains(needle), "(i) the header must pin {needle:?}");
    }
    for (family, n) in [("pod-", 7707u64), ("svc-", 4532)] {
        assert!(
            head.contains(&format!("n = {n} -> {n}")),
            "(i) the header must record the last-agreeing witness {family}{n}"
        );
        assert!(
            !rows.iter().any(|r| r.family == family && r.n_distinct == n),
            "(i) an agreeing witness must not also be a row: {family}{n}"
        );
    }
}

/// Lowercases, folds `≤` and dashes, and drops the markup characters
/// that could sit inside a phrase — WITHOUT collapsing whitespace.
///
/// [`normalize`] is exactly this plus the collapse, and the two must stay
/// in that relationship: it is what makes the pre-filter sound. A token
/// containing no whitespace survives into this output if and only if it
/// survives into `normalize`'s, so a file with no raw hit for such a
/// token cannot contain the phrase however it is wrapped.
fn flatten(text: &str) -> String {
    flatten_with(text, Quotes::Strip)
}

/// Whether [`flatten_with`] keeps `"`. Phrase matching strips it —
/// emphasis must not hide a banned sentence — while the frozen artifact's
/// scope anchor needs it, because the quotes are what stop
/// `"v0".."v{n-1}"` arising from unrelated prose.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Quotes {
    Strip,
    Keep,
}

fn flatten_with(text: &str, quotes: Quotes) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '"' if quotes == Quotes::Keep => out.push('"'),
            '`' | '*' | '_' | '"' => {}
            '≤' => out.push_str("<="),
            '—' | '–' => out.push('-'),
            c if c.is_uppercase() => out.extend(c.to_lowercase()),
            c => out.push(c),
        }
    }
    out
}

/// Every value-family range form `"<a>".."<b>"` in `text`, in order.
///
/// Hand-scanned rather than pattern-matched: the shape is four literal
/// characters between two quoted tokens, and this test adds no dependency
/// to find them.
fn value_family_ranges(text: &str) -> Vec<String> {
    const SEP: &str = "\"..\"";
    let mut out = Vec::new();
    for (i, _) in text.match_indices(SEP) {
        let Some(open) = text[..i].rfind('"') else {
            continue;
        };
        let rest = i + SEP.len();
        let Some(close) = text[rest..].find('"') else {
            continue;
        };
        out.push(format!(
            "\"{}\"..\"{}\"",
            &text[open + 1..i],
            &text[rest..rest + close]
        ));
    }
    out
}

/// [`flatten`] plus whitespace collapse — the form phrases are matched
/// against. The collapse is why a banned sentence cannot return by
/// falling across two lines; every caller must therefore normalize the
/// whole SPAN it is checking, never line by line (issue #261 review,
/// round three: the per-line variant let a phrase through by wrapping).
fn normalize(text: &str) -> String {
    collapse(&flatten(text))
}

/// The whitespace-collapsing half of [`normalize`], split out so a caller
/// that has already paid for [`flatten`] does not pay for it twice.
///
/// It also drops each line's LEADING comment marker before joining. That
/// is not cosmetic: in a `#`-commented file the continuation line of a
/// wrapped sentence carries a `#` that lands INSIDE the phrase —
/// `only while # all three` — so collapsing whitespace alone does not
/// rejoin it, and the wrap-collapse this whole gate rests on silently
/// stops working. It was found in the one file with its own rule (issue
/// #261 review, round three) but the hole was never confined to it: the
/// main sweep reads `.test`, `.tsv`, YAML and Rust sources, and a phrase
/// wrapped across two `#` or `//` lines in any of them would have walked
/// through the same way.
fn collapse(flat: &str) -> String {
    let mut out = String::with_capacity(flat.len());
    for line in flat.lines() {
        // One leading run of comment punctuation, after any indent.
        //
        // `--` is included and a lone `-` is not, and the difference is
        // real rather than fussy: `--` at the start of a line is only
        // ever a SQL comment — this tree tracks 70 `.sql` files and the
        // `golden/traces_*` corpora — whereas `- ` is a markdown bullet,
        // which STARTS a sentence instead of continuing one, so eating it
        // would join unrelated prose. Hence the `--` test below rather
        // than adding `-` to the character class.
        //
        // Because the indent is trimmed first, this covers the 126
        // tracked files with an indented-or-not `--` line, not the 105
        // with one in column zero. Say which set is meant.
        //
        // Known residuals, recorded with their DIRECTION rather than
        // fixed, since both need contrived input and one is merely
        // noisy:
        //  - `trim_start_matches('-')` eats the whole run, not just the
        //    `--` introducer, so a `---` rule or YAML front matter
        //    between two halves of a phrase joins them. Fails CLOSED: a
        //    spurious failure, and `---` is common enough to meet.
        //  - `-->` closing an HTML comment mid-phrase is stripped and
        //    the halves join. Fails OPEN, but needs a comment ended
        //    mid-sentence and then continued.
        //
        // Two neighbours read text slightly differently and are left
        // alone on purpose, because both fail CLOSED: `header_len`
        // requires a `#` in column zero where this accepts an indented
        // one, and `header()` matches raw substrings for the
        // capture-condition pins. Unifying them would buy nothing and
        // risk the fail-open direction that has already cost a round.
        let mut body = line.trim_start().trim_start_matches(['#', '/', ';', '>']);
        if body.starts_with("--") {
            body = body.trim_start_matches('-');
        }
        for ch in body.chars() {
            if ch.is_whitespace() {
                if !out.ends_with(' ') {
                    out.push(' ');
                }
            } else {
                out.push(ch);
            }
        }
        if !out.ends_with(' ') {
            out.push(' ');
        }
    }
    out
}

/// The claim that #261 exists to retract must not return to the ledger
/// or to the public API surface, and the statement that replaced it must
/// still be there.
///
/// **Domain: every git-TRACKED file, with exactly one file under a
/// different rule and none skipped.** "None skipped" is enforced, not
/// asserted in prose: a tracked path the filesystem will not open is
/// collected and the collection is asserted empty, so the domain in this
/// sentence is the domain the code walks. Not a hand-listed set — a hand
/// list cannot see the copy nobody remembered, and the claim was in four
/// separate files before #261, each found by grep rather than by
/// recollection. Line wraps are collapsed first, so the sentence cannot
/// return by being re-flowed or re-emphasised.
///
/// `golden/detected_cardinality/reference_divergence.tsv` is the one
/// exception, and it is a STRICTER rule rather than a pass: the phrase
/// may appear there only inside a `#` comment line, and only while the
/// header also names the value family it was measured on (`"v0"` …
/// `"v{n-1}"`). That file is a frozen #244 artifact whose wording is
/// true for its own family — re-verified 2026-08-08 by scanning
/// n = 1…5400 fresh-sketch-per-n against the vendored v0.2.6 library,
/// first `Estimate() != n` at n = 5328. It needs its own rule because
/// the AC-19 gate that otherwise freezes it
/// (`detected_fields_witness.rs`) `continue`s on `#` lines, so nothing
/// else in the tree reads that header at all.
///
/// **Where this instrument stops — three limits, stated because a gate
/// that looks complete is worse than one whose edges are known.**
///
/// 1. It bans the enumerated FORMS. A universal claim worded differently
///    — a new number, a new phrasing — is not detectable by any textual
///    rule, and this one does not pretend otherwise.
/// 2. The pre-filter is sound only for those forms. Every banned phrase
///    is asserted to contain a pre-filter token, so skipping a file that
///    holds none is safe for THIS ban list and only for it.
/// 3. **The positive assertions below only prevent DELETION.** They do
///    not stop a contradictory sentence being ADDED alongside them: a
///    reviewer added a universal claim without removing anything and
///    this test stayed green. Reviewing a diff that adds prose here is
///    still a human job.
#[test]
fn the_universal_agreement_threshold_claim_stays_retracted() {
    // The `v{i}`-scoped #244 capture, excluded with its reason in this
    // test's doc comment.
    const FROZEN_244_ARTIFACT: &str = "golden/detected_cardinality/reference_divergence.tsv";
    // Every one of these is a form that WAS committed somewhere in the
    // tree before #261, or the phrase that presupposed it. Each is
    // assembled from halves so the literal never appears in any file —
    // which lets the sweep below include its OWN definition site rather
    // than carve out an exemption for it. The one exemption in this test
    // is the frozen #244 artifact, and it has a reason.
    // Each entry is (phrase, required pre-filter tokens). The phrase is
    // assembled from halves so the literal never appears in any file,
    // which lets the sweep read its OWN definition site instead of
    // exempting it. The tokens are what makes skipping a file sound: a
    // file is only inspected when ALL of some phrase's tokens appear in
    // it, and both properties that argument needs — every token is
    // whitespace-free, every token is inside its phrase — are asserted
    // below rather than asserted in a comment.
    let banned: Vec<(String, &[&str])> = vec![
        (
            ["for every ", "distinct-value count"].concat(),
            &["distinct-value"][..],
        ),
        (["for every ", "n <= 5327"].concat(), &["5327"][..]),
        (["first divergence ", "at n = 5328"].concat(), &["5328"][..]),
        (["first divergence ", "is n = 5328"].concat(), &["5328"][..]),
        (
            ["far inside ", "the agreeing range"].concat(),
            &["agreeing"][..],
        ),
        // Round two: sufficient conditions written as necessary ones.
        // Two tokens, because its rarest single word (`only`) is in
        // almost every file and would make the pre-filter do nothing.
        (
            ["only while ", "all three"].concat(),
            &["only", "three"][..],
        ),
    ];
    // Every TRACKED file, from git rather than from a directory walk: a
    // walk would also read a developer's untracked scratch files, and a
    // gate that fails on those is a gate people learn to ignore. A
    // failure to list is a hard failure, never a quiet empty sweep.
    let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let listing = std::process::Command::new("git")
        .arg("-C")
        .arg(&repo)
        .args(["ls-files", "-z"])
        .output()
        .expect("git ls-files must run: this gate's domain is the tracked tree");
    assert!(
        listing.status.success(),
        "git ls-files failed: {}",
        String::from_utf8_lossy(&listing.stderr)
    );
    // Cheap pre-filter so the sweep does not normalize every megabyte of
    // committed benchmark JSON. Each token is a single unsplittable word
    // or number, so no line wrap can hide it — and the loop below ASSERTS
    // that every banned phrase contains at least one of them, which is
    // what makes skipping a file that contains none provably safe rather
    // than probably safe.
    // The soundness argument has two halves and BOTH are checked, because
    // an earlier revision's comment claimed unsplittability while its
    // assertion only checked containment — a gate proving something
    // weaker than the sentence above it (issue #261 review, round three).
    for (phrase, tokens) in &banned {
        assert!(
            !tokens.is_empty(),
            "banned phrase {phrase:?} has no pre-filter token, so every file would \
             be skipped for it"
        );
        for token in *tokens {
            assert!(
                !token.chars().any(char::is_whitespace),
                "pre-filter token {token:?} contains whitespace, so a line wrap inside \
                 it would give zero hits and the file would be skipped — which is \
                 exactly how `all three` let a wrapped claim through"
            );
            assert!(
                phrase.contains(token),
                "pre-filter token {token:?} is not inside its phrase {phrase:?}, so \
                 skipping on it proves nothing"
            );
        }
    }
    let mut scanned = 0usize;
    let mut inspected = 0usize;
    let mut saw_frozen = false;
    let mut unreadable: Vec<String> = Vec::new();
    for rel in String::from_utf8_lossy(&listing.stdout).split('\0') {
        if rel.is_empty() {
            continue;
        }
        if rel.ends_with(FROZEN_244_ARTIFACT) {
            saw_frozen = true;
            check_frozen_244_header(&repo.join(rel), &banned);
            continue;
        }
        let path = repo.join(rel);
        let Ok(bytes) = std::fs::read(&path) else {
            // A path git lists that the filesystem will not open
            // (submodule, sparse checkout). Counted, never silently
            // dropped: the domain sentence above says none is skipped, so
            // this is asserted at zero below rather than tolerated.
            unreadable.push(rel.to_string());
            continue;
        };
        scanned += 1;
        // `flatten`, not the raw bytes: markup INSIDE a token (`dis*tinct*`)
        // would otherwise hide it from the pre-filter while `normalize`
        // still saw the phrase. Same strip set, one collapse apart.
        let flat = flatten(&String::from_utf8_lossy(&bytes));
        if !banned
            .iter()
            .any(|(_, tokens)| tokens.iter().all(|t| flat.contains(t)))
        {
            continue;
        }
        let norm = collapse(&flat);
        inspected += 1;
        for (banned, _) in &banned {
            assert!(
                !norm.contains(banned),
                "{rel} states a retracted over-claim about when the reference's \
                 estimate is exact: {banned:?}. Two shapes were retracted under \
                 #261 — a threshold stated without the value family it was \
                 measured on, and sufficient conditions written as necessary \
                 ones. Name the family, or state the condition as sufficient."
            );
        }
    }
    assert!(
        scanned > 500,
        "the sweep must actually have covered the tracked tree (scanned {scanned} files); \
         a sweep that silently covers nothing passes every ban"
    );
    assert!(
        inspected > 0,
        "the pre-filter matched nothing at all, which would make the ban vacuous — \
         the ledger alone should have carried a matching token"
    );
    assert!(
        unreadable.is_empty(),
        "git lists these tracked paths but they could not be read, so they went \
         unswept: {unreadable:?}"
    );
    assert!(
        saw_frozen,
        "the frozen #244 artifact was not seen in the tracked listing, so its \
         header went unchecked — the exception must be exercised, not assumed"
    );

    // The positive half — the sentences that must survive.
    let ledger = normalize(&repo_file("docs/benchmarks/logs-differential-ledger.md"));
    assert!(
        ledger.contains("the agreement threshold is a property of the value strings"),
        "the ledger must keep saying what the threshold depends on"
    );
    assert!(
        ledger.contains("sufficient conditions, not necessary ones"),
        "the ledger must keep the conditions labelled SUFFICIENT: a dense sketch \
         still answers exactly N at plenty of N, so writing them as necessary \
         conditions is the same false-universal shape one layer in (issue #261)"
    );
    assert!(
        ledger.contains("4533"),
        "the ledger must keep the svc-{{i}} counterexample cardinality 4533, which is \
         BELOW the number the retracted claim named"
    );
    // The one-N-three-answers sentence is NOT asserted here: it is a
    // figure describing the artifact, so it is recomputed from the
    // artifact and matched against this prose by check (h) of
    // `detected_labels_divergence_rows_hold_and_the_ledger_names_them`.
    // One owner per number.
    let api = normalize(&repo_file("docs/api.md"));
    assert!(
        api.contains("from two values upward there is no useful bound"),
        "docs/api.md §2.6.3 must keep the statement that replaced the bound — and it \
         must keep saying `no USEFUL bound` rather than `no bound`, because a \
         single value cannot collide and so always agrees"
    );
}

/// The frozen #244 artifact's rule: a banned phrase may appear only in a
/// `#` comment line, and only while the header still names the value
/// family that phrase was measured on. Coverage, not an exemption — see
/// the caller's doc comment for why this file needs its own rule.
fn check_frozen_244_header(path: &std::path::Path, banned: &[(String, &[&str])]) {
    let text = std::fs::read_to_string(path).expect("the frozen #244 artifact must exist");
    let lines: Vec<&str> = text.lines().collect();
    // The LEADING contiguous comment block — the same span `header()`
    // reads, and the only place the wording is scoped by the family
    // sentence.
    let header_len = lines.iter().take_while(|l| l.starts_with('#')).count();
    // Both spans are normalized WHOLE, never line by line. Two earlier
    // revisions of this function were evaded through that gap: first by
    // appending a `#` comment after the data rows (inside a `starts_with`
    // check, outside the scope check), then by splitting a phrase across
    // two `#` lines, which a per-line normalize cannot see and the
    // main sweep — which normalizes whole files — always could. This is
    // the only file the main sweep does not cover, so it is the only
    // place that gap was reachable (issue #261 review, rounds two/three).
    let header_span = normalize(&lines[..header_len].join("\n"));
    let body_span = normalize(&lines[header_len..].join("\n"));
    for (phrase, _) in banned {
        assert!(
            !body_span.contains(phrase),
            "{}: the retracted claim {phrase:?} appears at or after line {} — outside \
             the leading header block, where the family-scoping sentence cannot reach \
             it (issue #261).",
            path.display(),
            header_len + 1
        );
    }
    if banned.iter().any(|(p, _)| header_span.contains(p)) {
        // Pins the whole scoping DECLARATION, not just the family name
        // inside it.
        //
        // Why not just the name: coverage turned out to be a property of
        // SPELLING, not of intent. A uniqueness test on the quoted range
        // `"v0".."v{n-1}"` only sees a re-capture that writes the new
        // family the same way — and that is not this repo's notation.
        // Measured in this tree: the ledger names families in the `{i}`
        // form 16 times (`v{i}`, `svc-{i}`, `pod-{i}`, `instance-{i}`,
        // `10.42.0.{i}`); the sibling artifact this issue added names
        // them bare (`pod-`, `svc-`) in a `family` column and as "three
        // families" in prose; and a quoted range form appears in only 4
        // tracked files (`git grep -lE '"[^"]+"\.\."[^"]+"'`). So a
        // maintainer re-capturing on `pod-` writes `the `pod-{i}`
        // family` or `family: pod-`, leaves the stale quoted range in a
        // superseded-capture aside, and a uniqueness check sees nothing
        // wrong while the header now asserts the retracted threshold
        // about a family that diverges at 7708. Both of those were
        // green before this assertion existed (issue #261 review, round
        // six).
        //
        // WHAT THIS PROVES, exactly: that one sentence is present,
        // verbatim, in the leading header block. It does NOT prove that
        // sentence governs the threshold sentence two lines below it —
        // no presence test can, and a sentence-relation parser is out of
        // proportion to a comment in a frozen, non-executing artifact
        // whose exactness contract is carried by the live gates.
        //
        // SO, IF YOU ARE EDITING THIS HEADER: this gate will tell you
        // the declaration changed. It will not tell you the threshold
        // sentence has become false. Re-read that sentence against the
        // family this declaration names, or delete it.
        const SCOPE_DECLARATION: &str = "inserted per row: \"v0\"..\"v{n-1}\", fresh sketch per n";
        // Read through the SAME span pipeline as the phrase check, only
        // with quotes preserved, so strengthening one reader cannot
        // silently weaken the other the way it did in round four. It is
        // tolerant of the wraps that actually occur: markdown reflow
        // breaks at spaces, and this is checked after collapse.
        let anchor_span = collapse(&flatten_with(&lines[..header_len].join("\n"), Quotes::Keep));
        assert!(
            anchor_span.contains(SCOPE_DECLARATION),
            "{}: the header carries the retracted threshold wording but no longer \
             carries the declaration that scopes it, verbatim: {SCOPE_DECLARATION:?}. \
             Any re-capture rewrites that sentence — and the threshold wording is \
             true ONLY for that family, so it must be re-read or deleted at the same \
             time (issue #261).",
            path.display()
        );
        // Kept as a second, fail-closed constraint: a stale quoted range
        // left behind in an aside is still a second family named in the
        // same header.
        let ranges = value_family_ranges(&anchor_span);
        assert_eq!(
            ranges.len(),
            1,
            "{}: the header names {n} value family ranges ({ranges:?}); exactly one \
             is allowed, so a superseded capture's name cannot linger beside the \
             live one (issue #261).",
            path.display(),
            n = ranges.len()
        );
    }
}

/// `docs/api.md` §2.6.2 must not list the absent `sketch` key as a
/// divergence again. It is parity: the reference strips the field on the
/// HTTP path in every deployment shape, and the section has to carry the
/// two citations that let a reader re-check that without re-measuring.
///
/// A false divergence entry is worse than a missing one, because the
/// next reader defends it.
#[test]
fn the_absent_sketch_key_is_documented_as_parity_not_as_a_divergence() {
    let api = repo_file("docs/api.md");
    let norm = normalize(&api);
    assert!(
        !norm.contains("no sketch key is emitted"),
        "docs/api.md §2.6.2 lists the absent `sketch` key as a deliberate divergence \
         again; the reference never emits it on the HTTP surface (issue #261)"
    );
    assert!(
        norm.contains("parity, not a divergence - no sketch key"),
        "docs/api.md §2.6.2 must state the absent `sketch` key as parity"
    );
    for citation in [
        "newdetectedlabelscardinalityfilter",
        "roundtrip.go:347-370",
        "modules.go:1368",
    ] {
        assert!(
            norm.contains(citation),
            "the parity note must keep its source citation {citation:?}"
        );
    }
}

/// The committed capture transcript in PROVENANCE.md and the committed
/// artifact must not drift apart: every disagreeing transcript row is a
/// TSV row with the same numbers and the same instrument, and every
/// agreeing transcript row is a header witness rather than a row.
///
/// Without this, the transcript is prose — the class of claim that never
/// fails on its own.
#[test]
fn the_provenance_transcript_and_the_artifact_agree() {
    let provenance = crate_file("tests/logqltest/PROVENANCE.md");
    let start = provenance
        .find("```pulsus-261-detected-labels-capture")
        .expect("the #261 capture block must exist in PROVENANCE.md");
    let body = &provenance[start..];
    let end = body[3..].find("```").expect("closing fence") + 3;
    let block = &body[..end];

    let rows = parse_rows(TSV);
    let head = header(TSV);
    let mut transcribed = 0usize;
    for line in block.lines().skip(1) {
        if line.trim().is_empty() {
            continue;
        }
        let cols: Vec<&str> = line.split('\t').collect();
        assert!(cols.len() >= 4, "malformed transcript row: {line:?}");
        let family = cols[0];
        let n: u64 = cols[1].parse().expect("transcript n");
        let value: u64 = cols[2].parse().expect("transcript cardinality");
        let observed_by = cols[3];
        transcribed += 1;
        if value == n {
            assert!(
                !rows.iter().any(|r| r.family == family && r.n_distinct == n),
                "{family}{n} agrees, so it must not be an artifact row"
            );
            assert!(
                head.contains(&format!("n = {n} -> {n}")),
                "the agreeing transcript row {family}{n} must be a header witness"
            );
        } else {
            let row = rows
                .iter()
                .find(|r| r.family == family && r.n_distinct == n)
                .unwrap_or_else(|| panic!("transcript row {family}{n} is missing from the TSV"));
            assert_eq!(row.reference_value, value, "{family}{n} value drift");
            assert_eq!(row.observed_by, observed_by, "{family}{n} instrument drift");
        }
    }
    assert_eq!(
        transcribed,
        rows.len() + 2,
        "the transcript must carry every artifact row plus the two last-agreeing witnesses"
    );
}

/// The production aggregate still computes the EXACT count.
///
/// Weak on its own, and said so: it reads rendered SQL text, so it only
/// stops a silent estimator swap in a text-level refactor. The value
/// claim is carried by
/// `detected_labels_cardinality_is_exact_at_the_reference_divergence_points`
/// (`crates/pulsus-server/tests/logs_detected_live.rs`), which asserts
/// the number on the HTTP response body.
#[test]
fn the_detected_labels_aggregate_is_still_an_exact_count() {
    for fingerprints in [None, Some(&[7u64, 9][..])] {
        let rendered = sql::detected_labels(
            "log_streams_idx",
            &[month_literal(2026, 8)],
            fingerprints,
            "log_metrics_5s",
            sql::TimeWindow {
                start_ns: 1_754_000_000_000_000_000,
                end_ns: 1_754_003_600_000_000_000,
            },
            5_000_000_000,
        );
        assert!(
            rendered.contains("uniqExact(val) AS cardinality"),
            "the exact aggregate is the contract: {rendered}"
        );
        // A deny list of approximate distinct-count entry points. It is
        // NOT exhaustive and must not be described as such — `uniqIf`
        // and any future spelling are absent. What actually carries this
        // test is the POSITIVE assertion above: an estimator swap has to
        // remove `uniqExact(val) AS cardinality` to take effect, and
        // that fires first whatever the replacement is called. The list
        // is belt-and-braces, and it makes the failure message name the
        // offending function.
        for estimator in [
            "uniq(",
            "uniqIf",
            "uniqCombined",
            "uniqCombined64",
            "uniqHLL12",
            "uniqTheta",
            "uniqUpTo",
            "uniqExactIf",
        ] {
            assert!(
                !rendered.contains(estimator),
                "an approximate distinct-count ({estimator}) reached the \
                 /detected_labels aggregate: {rendered}"
            );
        }
    }
}
