//! Issue #262 (AC 10) — the `promql-expression-depth-cap` divergence row
//! cannot drift from the constant.
//!
//! **Why this test exists rather than riding an existing gate.**
//! `pulsus-server`'s `route_inventory.rs` proves every *mounted route's
//! path* appears in `docs/api.md`; it has no concept of sections, and it
//! passes on a tree with no §3.5 at all. A green run there says nothing
//! about this row — so the row gets its own gate, and
//! `route_inventory.rs` staying green is kept as the separate, weaker
//! claim it always was.

use std::path::Path;

use pulsus_promql::MAX_EXPR_DEPTH;

const HEADING: &str = "### 3.5 Limits and accepted divergences";
const ROW_KEY: &str = "`promql-expression-depth-cap`";

fn api_md() -> String {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    std::fs::read_to_string(root.join("docs/api.md")).expect("docs/api.md")
}

/// §3.5's body: the heading line up to the next `## `/`### ` heading.
fn section_body(api: &str) -> String {
    let start = api
        .find(HEADING)
        .unwrap_or_else(|| panic!("docs/api.md must carry a heading line exactly {HEADING:?}"))
        + HEADING.len();
    let rest = &api[start..];
    let end = rest
        .match_indices('\n')
        .map(|(i, _)| i + 1)
        .find(|&i| rest[i..].starts_with("## ") || rest[i..].starts_with("### "))
        .unwrap_or(rest.len());
    rest[..end].to_string()
}

/// **AC 10 checks 1, 2, 4 and 5.**
///
/// Check 5 matches on **normalized** whitespace: `docs/api.md` is not
/// hard-wrapped (214 of its 1,403 lines exceeded 200 characters before
/// this section landed), so a `contains` against a multi-word phrase can
/// be broken by a reformat — and a prose check a reformat can break gets
/// "fixed" by reformatting.
#[test]
fn the_divergence_row_is_present_and_carries_its_required_prose() {
    let api = api_md();
    let body = section_body(&api);

    assert!(!body.trim().is_empty(), "§3.5 must not be an empty section");
    assert!(body.contains(ROW_KEY), "§3.5 must carry the {ROW_KEY} row");
    assert!(
        body.contains("400") && body.contains("bad_data"),
        "§3.5 must state the status and errorType a client sees"
    );

    let normalized = body.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(
        normalized.contains("graduates this row to its own file"),
        "§3.5 must state the rule that a SECOND metrics divergence \
         graduates this row to its own file — the binding ruling requires \
         the next person to inherit it"
    );
}

/// **AC 10 check 3 — a CELL equality on the row's own line.**
///
/// A bare `contains` of the constant is satisfied by digits appearing
/// anywhere in the section: the body carries `100,000`, `349,525` and
/// `524,287`, so `100`, `349` and `524` all pass it. Boundary-aware
/// matching fixes those three and still passes **`400`**, which collides
/// with the row's own status. Equality on a cell cannot be satisfied by
/// digits appearing elsewhere, and it does not depend on which column the
/// constant lands in.
///
/// Measured discrimination (issue #262, the ruling's required table):
/// `250` passes; `251`, `100`, `349`, `400`, `524` and a deleted row all
/// redden.
#[test]
fn the_row_pins_the_constant_in_a_cell_of_its_own() {
    let api = api_md();
    let body = section_body(&api);

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
            panic!("§3.5 must carry a table row whose FIRST cell is exactly {ROW_KEY}")
        });

    let want = format!("`{MAX_EXPR_DEPTH}`");
    let cells: Vec<&str> = row.split('|').map(str::trim).collect();
    assert!(
        cells.contains(&want.as_str()),
        "the {ROW_KEY} row must carry a cell equal to exactly {want} \
         (MAX_EXPR_DEPTH is {MAX_EXPR_DEPTH}); cells were {cells:?}"
    );
}
