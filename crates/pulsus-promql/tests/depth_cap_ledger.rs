//! Issue #262 (AC 10) — the `promql-expression-depth-cap` divergence row
//! cannot drift from the constant.
//!
//! **Why this test exists rather than riding an existing gate.**
//! `pulsus-server`'s `route_inventory.rs` proves every *mounted route's
//! path* appears in `docs/api.md`; it has no concept of sections, and it
//! passes on a tree with no divergence row at all. A green run there says
//! nothing about this row — so the row gets its own gate, and
//! `route_inventory.rs` staying green is kept as the separate, weaker
//! claim it always was.
//!
//! **Issue #461 moved the row.** `docs/api.md` §3.5 promised that a second
//! metrics divergence would graduate it to its own file; #461 landed
//! several, so the row now lives in
//! `docs/benchmarks/metrics-differential-ledger.md` and this gate reads
//! the ledger's own table. The old assertion that §3.5 *states* the
//! graduation rule was asserting a promise that has since been kept, so it
//! is replaced by one that keeps the pointer from rotting: §3.5 must name
//! the ledger file, and must still be non-empty.

use std::path::Path;

use pulsus_promql::MAX_EXPR_DEPTH;

const HEADING: &str = "## Divergences";
const ROW_KEY: &str = "`promql-expression-depth-cap`";
const LEDGER: &str = "docs/benchmarks/metrics-differential-ledger.md";
const API_SECTION: &str = "### 3.5 Limits and accepted divergences";

fn repo_root() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn ledger_md() -> String {
    std::fs::read_to_string(repo_root().join(LEDGER)).unwrap_or_else(|e| panic!("{LEDGER}: {e}"))
}

fn api_md() -> String {
    std::fs::read_to_string(repo_root().join("docs/api.md")).expect("docs/api.md")
}

/// A section's body: its heading line up to the next `## `/`### ` heading.
fn section_body(doc: &str, heading: &str) -> String {
    let start = doc
        .find(heading)
        .unwrap_or_else(|| panic!("a heading line exactly {heading:?} must exist"))
        + heading.len();
    let rest = &doc[start..];
    let end = rest
        .match_indices('\n')
        .map(|(i, _)| i + 1)
        .find(|&i| rest[i..].starts_with("## ") || rest[i..].starts_with("### "))
        .unwrap_or(rest.len());
    rest[..end].to_string()
}

/// **AC 10 checks 1, 2, 4 and 5**, against the ledger the row moved into.
///
/// Check 5 matches on **normalized** whitespace: neither file is
/// hard-wrapped, so a `contains` against a multi-word phrase can be broken
/// by a reformat — and a prose check a reformat can break gets "fixed" by
/// reformatting.
#[test]
fn the_divergence_row_is_present_and_carries_its_required_prose() {
    let ledger = ledger_md();
    let body = section_body(&ledger, HEADING);

    assert!(
        !body.trim().is_empty(),
        "the ledger's {HEADING} section must not be empty"
    );
    assert!(
        body.contains(ROW_KEY),
        "the ledger must carry the {ROW_KEY} row"
    );

    let normalized = body.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(
        normalized.contains("bad_data"),
        "the ledger must state the errorType a client sees"
    );
}

/// The pointer left behind cannot rot: `docs/api.md` §3.5 must still exist,
/// must still be non-empty (it keeps the residual measurements the ledger
/// table cannot hold), and must name the file the row moved into.
#[test]
fn api_md_section_3_5_points_at_the_ledger() {
    let api = api_md();
    let body = section_body(&api, API_SECTION);
    assert!(
        !body.trim().is_empty(),
        "§3.5 must not be an empty section — it keeps the residual measurements"
    );
    assert!(
        body.contains("metrics-differential-ledger.md"),
        "§3.5 must name the ledger the divergence rows moved into"
    );
}

/// **AC 10 check 3 — a CELL equality on the row's own line.**
///
/// A bare `contains` of the constant is satisfied by digits appearing
/// anywhere in the section. Equality on a cell cannot be satisfied by
/// digits appearing elsewhere, and it does not depend on which column the
/// constant lands in.
///
/// Measured discrimination (issue #262, the ruling's required table):
/// `250` passes; `251`, `100`, `349`, `400`, `524` and a deleted row all
/// redden. Re-measured after the #461 move against the ledger's own row.
#[test]
fn the_row_pins_the_constant_in_a_cell_of_its_own() {
    let ledger = ledger_md();
    let body = section_body(&ledger, HEADING);

    let row = body
        .lines()
        .find(|line| {
            line.starts_with('|')
                && line
                    .split('|')
                    .nth(1)
                    .is_some_and(|cell| cell.trim() == ROW_KEY)
        })
        .unwrap_or_else(|| {
            panic!("the ledger must carry a table row whose FIRST cell is exactly {ROW_KEY}")
        });

    let want = format!("`{MAX_EXPR_DEPTH}`");
    let cells: Vec<&str> = row.split('|').map(str::trim).collect();
    assert!(
        cells.contains(&want.as_str()),
        "the {ROW_KEY} row must carry a cell equal to exactly {want} \
         (MAX_EXPR_DEPTH is {MAX_EXPR_DEPTH}); cells were {cells:?}"
    );
}
