//! Issue #510 — the documentation says what the code does, checked BY
//! COMMAND rather than by reading.
//!
//! Four claims, each scoped to the REGION it is about and asserted as a
//! COUNT in that region, following
//! `traces_projection_ledger.rs`'s posture: a claim about a RELATION
//! ("this row names both routes", "§4.2 states this rule") cannot be
//! tested by asking whether a substring occurs somewhere in a file.
//!
//! 1. every live ledger id this issue and issue #492 item 2 add exists
//!    exactly once, and its `- **Route.**` bullet names BOTH mounted
//!    routes;
//! 2. BOTH withdrawn rows are recorded as withdrawals with the
//!    measurement that retired each, not silently dropped;
//! 3. `docs/reference-defects-we-do-not-copy.md` carries entry 24 — index
//!    row, section heading, matching anchor, and an Evidence section
//!    naming the ledger id and BOTH push-order fixtures;
//! 4. `docs/api.md` §4.2 no longer states the three claims measurement
//!    contradicted, and does state their corrections, including the two
//!    rules issue #492 item 2's ordered fold added.
//!
//! What this does NOT establish: that the sentences are true of the code.
//! Each is pinned separately — by the hermetic response tests, by the
//! live differential fixtures named beside it, and by the stored-type
//! seal — and the point of this file is that the documents cannot drift
//! away from those without a red test.
//!
//! Hermetic: reads three committed files, runs no query, needs no
//! container.

use std::path::PathBuf;

/// The LIVE ledger ids issue #510 and issue #492 item 2 add. The count is
/// in the type: a row added without a clause here, or removed while a
/// clause survives, moves this array.
///
/// `traceql-spanset-aggregate-precedes-grouping` is deliberately NOT
/// here. It was a live row on the issue #510 branch and the ordered fold
/// retired it; it now appears in [`WITHDRAWN`] instead, and moving it
/// between the two arrays is the edit that records the retirement.
const LEDGER_IDS: [&str; 7] = [
    "traceql-spanset-aggregate-double-lexical-form",
    "traceql-spanset-aggregate-mixed-type-attribute",
    "traceql-spanset-aggregate-string-attribute-contributes",
    "traceql-attribute-aggregate-float64-precision",
    "traceql-nested-by-composite-series-cap",
    "traceql-select-before-by-nil-group-key",
    "traceql-midpipeline-spanset-filter-unsupported",
];

/// The rows WITHDRAWN, each paired with phrases from the measurement that
/// retired it.
///
/// The phrases are what stops a deletion passing as a withdrawal: a
/// section that exists and says nothing measured satisfies a
/// heading-only check, and deleting the row outright satisfies nothing at
/// all — measured before this array existed, a silent deletion of the
/// second row plus its two entries here passed every one of the 6970
/// tests in a full hermetic workspace run.
///
/// At least one phrase per row is a NUMBER the measurement moved, so a
/// withdrawal written from memory rather than from the run fails.
const WITHDRAWN: [(&str, &[&str]); 2] = [
    (
        "traceql-spanset-stacked-by-last-key-wins",
        &[
            "**three** span sets",
            "**two** here",
            "by(name) | count() > 0 | by(status)",
        ],
    ),
    (
        "traceql-spanset-aggregate-precedes-grouping",
        &[
            "by(name)=stringValue=alpha,count()=intValue=2",
            "`4`/`4`/`4` here and are now `3`/`3`/`1`",
            "by(name) | count() > 0 | by(status)",
        ],
    ),
];

/// Both mounted routes. The search handler is one function behind two
/// paths, so a divergence recorded for one is a divergence on both.
const ROUTES: [&str; 2] = ["/api/traces/v1/search", "/api/search"];

const LEDGER: &str = "docs/benchmarks/traces-differential-ledger.md";
const DEFECTS: &str = "docs/reference-defects-we-do-not-copy.md";
const API: &str = "docs/api.md";

/// The §4.2 heading whose body carries both rules this issue corrects.
const API_SECTION: &str = "### 4.2 `GET /api/traces/v1/search`";

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

fn read(rel: &str) -> String {
    let path = workspace_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"))
}

/// Collapses every run of whitespace to one space, so a needle is a
/// property of the sentence rather than of the hard wrap.
fn squash(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn count(hay: &str, needle: &str) -> usize {
    hay.matches(needle).count()
}

/// The lines of a markdown document OUTSIDE every fenced code block. A
/// heading-shaped line inside a fence is an example, not a heading.
fn lines_outside_fences(doc: &str) -> Vec<(usize, &str)> {
    let mut fenced = false;
    let mut out = Vec::new();
    for (i, line) in doc.lines().enumerate() {
        if line.trim_start().starts_with("```") {
            fenced = !fenced;
            continue;
        }
        if !fenced {
            out.push((i, line));
        }
    }
    out
}

/// The body under the UNIQUE heading line beginning with `marker`, up to
/// the next heading AT `stop_at`'s LEVEL OR ABOVE. Uniqueness is asserted
/// rather than assumed.
///
/// The level matters: an entry in the defects document is a `##` section
/// whose body carries `###` sub-headings, so stopping at the next heading
/// of any level would truncate it to its first line — and an Evidence
/// check over that fragment would fail for the wrong reason, or (worse,
/// with a different needle) pass over almost nothing.
fn unique_section_to(doc: &str, marker: &str, stop_at: &str, what: &str) -> String {
    let lines = lines_outside_fences(doc);
    let heads: Vec<usize> = lines
        .iter()
        .filter(|(_, l)| l.starts_with(marker))
        .map(|(i, _)| *i)
        .collect();
    assert_eq!(
        heads.len(),
        1,
        "{what} must contain exactly one heading line starting {marker:?}; found {} at lines \
         {heads:?}",
        heads.len()
    );
    let start = heads[0];
    let end = lines
        .iter()
        .find(|(i, l)| *i > start && is_heading_at_or_above(l, stop_at))
        .map_or(usize::MAX, |(i, _)| *i);
    lines
        .iter()
        .filter(|(i, _)| *i > start && *i < end)
        .map(|(_, l)| *l)
        .collect::<Vec<_>>()
        .join("\n")
}

/// A markdown heading whose level is `stop_at`'s or shallower — `"## "`
/// stops at `## ` and `# `, and passes over `### `.
fn is_heading_at_or_above(line: &str, stop_at: &str) -> bool {
    let hashes = |s: &str| s.chars().take_while(|c| *c == '#').count();
    let level = hashes(line);
    level > 0 && line[level..].starts_with(' ') && level <= hashes(stop_at)
}

/// The body under a UNIQUE heading, up to the next heading of ANY level —
/// the shape a `###` ledger row wants.
fn unique_section(doc: &str, marker: &str, what: &str) -> String {
    unique_section_to(doc, marker, "###### ", what)
}

/// A ledger row's `- **Route.**` bullet, up to the next sibling bullet —
/// the only place a row states which routes it is about. A route named in
/// passing elsewhere in the row does not scope it.
fn route_bullet(body: &str, id: &str) -> String {
    let lines: Vec<&str> = body.lines().collect();
    let starts: Vec<usize> = lines
        .iter()
        .enumerate()
        .filter(|(_, l)| l.starts_with("- **Route.**"))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        starts.len(),
        1,
        "the ledger row {id} must have exactly one `- **Route.**` bullet; found {}",
        starts.len()
    );
    let start = starts[0];
    let end = lines
        .iter()
        .enumerate()
        .find(|(i, l)| *i > start && l.starts_with("- **"))
        .map_or(lines.len(), |(i, _)| i);
    squash(&lines[start..end].join("\n"))
}

/// The §4.2 body — the region every API claim below is scoped to. A claim
/// checked over the whole document is satisfied by an unrelated sentence
/// in another section.
fn api_section() -> String {
    squash(&unique_section(&read(API), API_SECTION, "docs/api.md §4.2"))
}

#[test]
fn every_spanset_attribute_ledger_row_names_both_mounted_routes() {
    let ledger = read(LEDGER);
    for id in LEDGER_IDS {
        let body = unique_section(
            &ledger,
            &format!("### `{id}`"),
            &format!("the ledger row {id}"),
        );
        let scope = route_bullet(&body, id);
        for route in ROUTES {
            assert_eq!(
                count(&scope, route),
                1,
                "the ledger row {id} must name {route} exactly once in its Route bullet, or the \
                 row goes stale invisibly when the two routes answer differently:\n{scope}"
            );
        }
    }
}

/// Each withdrawal is a record, not a deletion — with the measurement
/// that retired it.
///
/// *RED when:* a withdrawn row is dropped from the ledger, or its section
/// stops carrying the numbers that turned it from a live divergence into
/// a retired one. Deleting the row and its entry here together is the
/// only way to lose it silently, and that is a visible edit to this
/// array — the array length is in the type.
#[test]
fn every_withdrawn_row_is_recorded_as_a_withdrawal_with_its_measurement() {
    let ledger = read(LEDGER);
    for (id, needles) in WITHDRAWN {
        let body = squash(&unique_section(
            &ledger,
            &format!("### `{id}`"),
            "the withdrawn row",
        ));
        assert!(
            body.contains("WITHDRAWN")
                || body.contains("Withdrawn")
                || body.contains("withdrawn")
                || body.contains("withdrawal"),
            "the {id} section must say it is a withdrawal:\n{body}"
        );
        for needle in needles {
            assert!(
                body.contains(needle),
                "the {id} withdrawal must carry the measurement that retired it; missing \
                 {needle:?}:\n{body}"
            );
        }
        // …and it is NOT a live row: no other section may carry the id,
        // which is what would let a reader take it for a standing
        // divergence.
        assert_eq!(
            count(&ledger, id),
            1,
            "the withdrawn id {id} must appear exactly once in the ledger, in its own \
             withdrawal section"
        );
        assert!(
            !LEDGER_IDS.contains(&id),
            "{id} cannot be both a live row and a withdrawal"
        );
    }
}

/// Entry 24 exists in the reference-defects index AND as a section, its
/// anchor resolves, and its Evidence names the ledger row and BOTH
/// push-order fixtures.
///
/// It was entry 23 on the issue #510 branch. Issue #492 item 2 landed
/// first with its own entry 23, so this one is 24 — both branches wrote
/// 23, and this test plus the entry-count check below is what makes the
/// collision visible rather than silent.
///
/// *RED when:* the entry is added to one of the two places and not the
/// other (the index row and the section are separate edits), the anchor
/// is mistyped, or one push order's fixture is dropped from the Evidence
/// — which is the pairing the whole defect rests on.
#[test]
fn the_mixed_type_defect_is_indexed_and_explained_in_the_defects_document() {
    let doc = read(DEFECTS);
    const ANCHOR: &str = "#24-a-groups-sum-and-avg-over-one-attribute-give-different-answers-\
                          depending-on-which-span-arrived-first";
    let anchor = ANCHOR.split_whitespace().collect::<String>();
    let index_row = format!("| [24]({anchor}) | Tempo | queries | C, D |");
    assert_eq!(
        count(&doc, &index_row),
        1,
        "the index table must carry entry 24 exactly once, with its anchor and its test codes: \
         {index_row}"
    );
    let body = squash(&unique_section_to(
        &doc,
        "## 24. A group's `sum` and `avg`",
        "## ",
        "reference-defects entry 24",
    ));
    // The anchor GitHub derives from the heading is the one the index
    // links to — checked by deriving it here rather than by eye.
    let heading = doc
        .lines()
        .find(|l| l.starts_with("## 24."))
        .expect("entry 24's heading");
    assert_eq!(
        format!("#{}", github_anchor(heading)),
        anchor,
        "entry 24's index link must resolve to its own heading"
    );
    for needle in [
        "traceql-spanset-aggregate-mixed-type-attribute",
        "mixed_type_int_first_sum",
        "mixed_type_float_first_sum",
        "mixed_type_int_first_avg",
        "mixed_type_float_first_avg",
        "traces_search_grouping_differential.rs",
    ] {
        assert_eq!(
            count(&body, needle),
            1,
            "entry 24's Evidence must name {needle:?} exactly once:\n{body}"
        );
    }
}

/// The defects document numbers its entries once each, contiguously from
/// 1, and holds 24 — the count a merge that renumbered one entry has to
/// produce.
///
/// *RED when:* two entries carry the same number, which is exactly what
/// both branches of this merge wrote (each added its own entry 23), or an
/// entry is added or dropped without the count moving. The section
/// headings and the index rows are checked SEPARATELY and then against
/// each other: they are two edits, and a merge can leave one behind.
#[test]
fn the_defects_document_numbers_its_entries_once_each_and_holds_twenty_four() {
    let doc = read(DEFECTS);
    let numbers = |take: fn(&str) -> Option<&str>| -> Vec<u32> {
        lines_outside_fences(&doc)
            .iter()
            .filter_map(|(_, l)| take(l))
            .filter_map(|n| n.parse::<u32>().ok())
            .collect()
    };
    let headings = numbers(|l| l.strip_prefix("## ")?.split('.').next());
    let index_rows = numbers(|l| l.strip_prefix("| [")?.split(']').next());
    let want: Vec<u32> = (1..=24).collect();
    assert_eq!(
        headings, want,
        "the defects document's `## <n>.` headings must be 1..=24, once each and in order — \
         a duplicated number is what a merge of two branches that each wrote entry 23 \
         produces"
    );
    assert_eq!(
        index_rows, want,
        "the index table's rows must be 1..=24, once each and in order — the heading and the \
         index row are two separate edits"
    );
}

/// GitHub's heading anchor: lowercase, punctuation dropped, spaces to
/// dashes.
fn github_anchor(heading: &str) -> String {
    heading
        .trim_start_matches('#')
        .trim()
        .to_lowercase()
        .chars()
        .filter_map(|c| match c {
            'a'..='z' | '0'..='9' => Some(c),
            ' ' | '-' => Some('-'),
            _ => None,
        })
        .collect()
}

/// §4.2 no longer states the three claims measurement contradicted, and
/// does state their corrections — each exactly once, in §4.2 and nowhere
/// else in the document.
///
/// The retired sentences are the reason this test exists rather than a
/// nicety: `docs/api.md` said a numeric attribute by-key renders
/// `{"doubleValue":<f>}` and claimed "verified live", while the live
/// gate it pointed at had no attribute by-key in its fixture list at all
/// and compared only the first attribute of a span set.
#[test]
fn the_api_section_states_the_measured_rules_and_not_the_retired_ones() {
    let doc = squash(&read(API));
    let section = api_section();
    for retired in [
        r#"a numeric attribute → `{"doubleValue":<f>}`"#,
        "a span lacking the key groups under a null value",
        "`-0.0` folds into `+0.0` and every NaN into one group — matching the reference",
    ] {
        assert_eq!(
            count(&doc, retired),
            0,
            "docs/api.md still states {retired:?}, which measurement contradicts"
        );
    }
    for stated in [
        // the by-key typing rule
        r#"An ATTRIBUTE by-key renders in the arm the SENDER STORED it as"#,
        r#"A span lacking the key groups under `{"stringValue":"nil"}`"#,
        // signed zero, and the NaN fold recorded as unobservable
        "Float group keys separate `-0.0` from `+0.0`",
        "that fold is unobservable end to end",
        // the grouping rule the stacked-`by()` fix establishes
        "A `by()` stage EXTENDS the active grouping key list; a `coalesce()` clears it",
        // the two rules issue #492 item 2's ordered fold established, and
        // which no test on either branch could see: reverting either one
        // to main's paragraph passed a full hermetic workspace run
        "The pipeline is one ordered fold, and every stage's written position decides what it sees",
        "The backstop counts the ACCUMULATED key tuple",
        // the aggregate attribute
        "Every aggregate stage contributes one `attributes` entry too",
        "The entry is per STAGE, not per aggregate",
        // the projection's typing
        "A projected attribute's value carries the type the SENDER STORED",
        // the empty-list omission
        "the `attributes` key is **omitted** when the list is empty, never emitted as `[]`",
    ] {
        assert_eq!(
            count(&doc, stated),
            1,
            "docs/api.md must state {stated:?} exactly once"
        );
        // …and in §4.2, except for the series-cap rule, whose home is
        // §4.6's cap paragraph. Asserting §4.2 for that one would move a
        // sentence away from the rule it belongs to just to satisfy a
        // check.
        if stated != "The backstop counts the ACCUMULATED key tuple" {
            assert_eq!(
                count(&section, stated),
                1,
                "docs/api.md §4.2 must state {stated:?} exactly once in that section"
            );
        }
    }
}

/// Every ledger id is referenced from the code or the test that owns it,
/// so a row cannot outlive the behaviour it records.
///
/// *RED when:* a row is added without an owner, or an owner is deleted
/// while the row stays. The owner is named per id rather than by a
/// whole-tree grep, because a grep that finds the id in the ledger itself
/// would pass on every row.
#[test]
fn every_ledger_row_is_referenced_from_the_artefact_that_owns_it() {
    const OWNERS: [(&str, &str); 7] = [
        (
            "traceql-spanset-aggregate-double-lexical-form",
            "crates/pulsus-read/src/traces/search_eval.rs",
        ),
        (
            "traceql-spanset-aggregate-mixed-type-attribute",
            "crates/pulsus-read/src/traces/search_eval.rs",
        ),
        (
            "traceql-spanset-aggregate-string-attribute-contributes",
            "docs/benchmarks/traces-differential-ledger.md",
        ),
        (
            "traceql-attribute-aggregate-float64-precision",
            "crates/pulsus-read/tests/traces_search_grouping_differential.rs",
        ),
        (
            "traceql-nested-by-composite-series-cap",
            "crates/pulsus-read/src/traces/search_eval.rs",
        ),
        (
            "traceql-select-before-by-nil-group-key",
            "docs/reference-defects-we-do-not-copy.md",
        ),
        (
            "traceql-midpipeline-spanset-filter-unsupported",
            "crates/pulsus-traceql/src/parser.rs",
        ),
    ];
    assert_eq!(
        OWNERS.map(|(id, _)| id).to_vec(),
        LEDGER_IDS.to_vec(),
        "every id must have an owner, in the same order"
    );
    for (id, owner) in OWNERS {
        let text = read(owner);
        assert!(
            text.contains(id),
            "{owner} must reference {id}, or the row has nothing that retires it"
        );
    }
}
