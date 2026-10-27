//! `schema/schema.sql`, rendered.
//!
//! The binary does not create schema — `schema/schema.sh` does, and it reads
//! the same file. What this module exists for at run time is the one
//! question the serving process asks the file: a table's declared column
//! list, which the trace fetch derives its projection from. A materialized
//! view's own projection is read out of it too, by the schema cases in
//! `tests/schema_file.rs`.
//!
//! It also renders the whole file, which is how the test toolkit builds a
//! schema without paying for a subprocess per call site, and how
//! `tests/schema_file.rs` holds the script and this crate to the same text.

use crate::render::{self, RenderCtx};

/// The DDL, as committed. One file, two variants, selected by the
/// `--@single` / `--@cluster` line prefixes.
pub const SCHEMA_SQL: &str = include_str!("../../../schema/schema.sql");

const SINGLE: &str = "--@single";
const CLUSTER: &str = "--@cluster";

/// The move clause of a table of each family (issue #743 part A): kept when
/// that family moves, dropped otherwise.
const COLD_METRICS: &str = "--@cold-metrics";
const COLD_LOGS: &str = "--@cold-logs";
const COLD_TRACES: &str = "--@cold-traces";

/// The file with its level block expanded for the context's rollup levels
/// (issue #18), the other variant's lines dropped, this variant's markers
/// stripped, comments removed and every `{{token}}` resolved.
///
/// `schema.sh` does exactly this with an `awk` step, three `sed`
/// expressions and the same token list;
/// `the_script_renders_exactly_what_this_crate_renders` holds the two
/// together.
pub fn rendered(ctx: &RenderCtx) -> String {
    let (mine, theirs) = match ctx.cluster {
        Some(_) => (CLUSTER, SINGLE),
        None => (SINGLE, CLUSTER),
    };
    let expanded = render::expand_levels(
        SCHEMA_SQL,
        (
            &ctx.rollup_levels,
            &ctx.rollup_ranges,
            ctx.rollup_counter_buckets,
        ),
    );
    let mut out = String::with_capacity(expanded.len());
    for line in expanded.lines() {
        let line = match marked(line, mine) {
            Some(rest) => rest,
            None => {
                if marked(line, theirs).is_some() || line.starts_with("--") {
                    continue;
                }
                line
            }
        };
        out.push_str(line);
        out.push('\n');
    }
    render::substitute_tokens(&out, ctx)
}

/// `schema/schema.sql` with its level block expanded for `levels` (issue
/// #18), before any other step: what a test reads to see the level tables
/// under their names. The switch points and the counter bucket count are
/// the defaults.
#[doc(hidden)]
pub fn expand_levels_for_tests(levels: &[u32]) -> String {
    render::expand_levels(
        SCHEMA_SQL,
        (
            levels,
            render::DEFAULT_ROLLUP_RANGES,
            render::DEFAULT_ROLLUP_COUNTER_BUCKETS,
        ),
    )
}

/// `line` with `marker` and the spaces after it removed, or `None` when the
/// line does not carry that marker. The space after the marker is part of
/// the match, so a longer name cannot be mistaken for a marker.
fn marked<'a>(line: &'a str, marker: &str) -> Option<&'a str> {
    let rest = line.strip_prefix(marker)?;
    if !rest.starts_with(' ') {
        return None;
    }
    Some(rest.trim_start_matches(' '))
}

/// [`rendered`], split into the statements that are sent one per request.
///
/// A statement ends at a line whose last character is a semicolon. That is
/// exact for this file, and `a_semicolon_ends_a_line_and_occurs_nowhere_else`
/// is what keeps it so.
pub fn rendered_statements(ctx: &RenderCtx) -> Vec<String> {
    let text = rendered(ctx);
    let mut out = Vec::new();
    let mut current = String::new();
    for line in text.lines() {
        current.push_str(line);
        current.push('\n');
        if line.ends_with(';') {
            out.push(current.trim().to_string());
            current.clear();
        }
    }
    out
}

/// The projection one materialized view applies, read out of that view's own
/// statement so the two cannot drift.
///
/// `None` when the file carries no view of that name.
pub fn mv_projection(mv_name: &str, ctx: &RenderCtx) -> Option<String> {
    let head = format!(
        "CREATE MATERIALIZED VIEW {}.{} ",
        ctx.db,
        render::render_name(mv_name, ctx)
    );
    let stmt = rendered_statements(ctx)
        .into_iter()
        .find(|s| s.starts_with(&head))?;
    let (_, body) = stmt.split_once("\nAS ")?;
    Some(body.trim_end().trim_end_matches(';').trim_end().to_string())
}

/// The column names one table declares, in the order the file declares them.
///
/// Issue #587: the trace fetch's three statements project an explicit subset
/// of `spans`' and `resources`' columns, and the case that holds them
/// (`F-14`) compares each projection against **this** list minus the columns
/// it names as omitted — so a column added to either table shows up in the
/// derived list and the case fails until the new column is either projected
/// or named as an omission. A second hand-written list in the read crate
/// would drift silently.
///
/// `None` for a name the file does not carry. The parse is deliberately
/// narrow and is not a general SQL parser: the text between the first `(`
/// and its matching `)`, split on top-level commas, each entry's first
/// whitespace-free token, with `INDEX`, `CONSTRAINT` and `PROJECTION`
/// declarations left out — they share the list but are not columns.
pub fn table_column_names(table: &str) -> Option<Vec<&'static str>> {
    let head = format!("\nCREATE TABLE IF NOT EXISTS {{{{db}}}}.{table}{{{{on_cluster}}}}\n");
    let at = SCHEMA_SQL.find(&head)?;
    let body = &SCHEMA_SQL[at + head.len()..];
    let open = body.find('(')?;
    let mut depth = 0usize;
    let mut close = None;
    for (i, c) in body[open..].char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    close = Some(open + i);
                    break;
                }
            }
            _ => {}
        }
    }
    let list = &body[open + 1..close?];

    let mut names = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    let cut = |slice: &'static str, names: &mut Vec<&'static str>| {
        let Some(name) = slice.split_whitespace().next() else {
            return;
        };
        if matches!(name, "INDEX" | "CONSTRAINT" | "PROJECTION") {
            return;
        }
        names.push(name);
    };
    for (i, c) in list.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                cut(&list[start..i], &mut names);
                start = i + 1;
            }
            _ => {}
        }
    }
    cut(&list[start..], &mut names);
    Some(names)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A marker is only a marker when a space follows it, so a longer name
    /// cannot be eaten by a shorter one.
    #[test]
    fn a_marker_needs_the_space_after_it() {
        assert_eq!(marked("--@single  ENGINE = x", SINGLE), Some("ENGINE = x"));
        assert_eq!(marked("--@singleton ENGINE = x", SINGLE), None);
        assert_eq!(marked("ENGINE = x", SINGLE), None);
    }

    /// Only the two mode markers, the four level markers (issue #18) and the
    /// three cold markers (issue #743 part A) may prefix a line with `--@`:
    /// another would reach the server as text.
    #[test]
    fn the_file_carries_no_marker_but_the_two_modes_and_the_level_and_the_cold_markers() {
        let mut level_lines = 0;
        let mut cold_lines = [0usize; 3];
        for line in SCHEMA_SQL.lines() {
            if !line.starts_with("--@") {
                continue;
            }
            let level = [
                "--@level ",
                "--@level-first ",
                "--@level-next ",
                "--@level-feeds ",
            ]
            .iter()
            .any(|m| line.starts_with(m));
            level_lines += usize::from(level);
            let cold = [COLD_METRICS, COLD_LOGS, COLD_TRACES].map(|m| marked(line, m).is_some());
            for (n, c) in cold_lines.iter_mut().zip(cold) {
                *n += usize::from(c);
            }
            assert!(
                level
                    || cold.contains(&true)
                    || marked(line, SINGLE).is_some()
                    || marked(line, CLUSTER).is_some(),
                "unknown marker: {line}"
            );
        }
        assert!(level_lines > 0, "the file carries the level block");
        for (marker, n) in [COLD_METRICS, COLD_LOGS, COLD_TRACES]
            .iter()
            .zip(cold_lines)
        {
            assert!(n > 0, "the file carries a {marker} line");
        }
    }
}
