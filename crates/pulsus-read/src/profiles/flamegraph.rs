//! Issue #823: the merged tree's trim, a port of the reference's three
//! passes over the rows S5 returns (`super::tree`), and the flame graph it
//! answers. Pure: no I/O.
//!
//! Step 1 is the reference's `TreeFromStacktraceTree`, step 2 its
//! `MarshalTruncate` followed by `UnmarshalTree`, step 3 its
//! `NewFlameGraph`. Children are kept sorted by name, in byte order, at
//! every step, as the reference's `insert` keeps them.
//!
//! One deliberate difference from the reference
//! (`docs/reference-defects-we-do-not-copy.md` §41): in step 1, a kept child
//! named `other` takes the cut siblings' sum, its self and its total each
//! growing by it. The reference overwrites that sum.

use super::engine::ProfilesReadError;
use super::tree::TreeRow;

/// The name the reference gives a node that holds cut siblings' values.
pub const OTHER: &str = "other";

/// A flame graph: the names, and per level the nodes as
/// `[x offset delta, total, self, name index]`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FlameGraph {
    pub names: Vec<String>,
    pub levels: Vec<Vec<i64>>,
    pub total: i64,
    pub max_self: i64,
}

/// One node of a trimmed tree.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TreeNode {
    pub name: String,
    pub self_value: i64,
    pub total: i64,
    /// Indices into [`Tree::nodes`], sorted by name.
    pub children: Vec<usize>,
}

/// A tree of named nodes: every node in [`Tree::nodes`], the roots'
/// indices in [`Tree::roots`], sorted by name.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Tree {
    pub nodes: Vec<TreeNode>,
    pub roots: Vec<usize>,
}

/// Steps 0 and 1: S5's rows as a tree, every child whose total is below
/// the `m`-th largest total cut into a child `other`.
pub fn merged_tree(rows: &[TreeRow], m: i64) -> Result<Tree, ProfilesReadError> {
    let _ = (rows, m);
    Ok(Tree::default())
}

/// Step 2: the reference's marshal with truncation, then its unmarshal.
pub fn truncated(tree: Tree, m: i64) -> Tree {
    let _ = m;
    tree
}

/// The flame graph of S5's rows: steps 1 and 2 with `m` (the resolved
/// `max_nodes`), step 3 with `raw` (the request's value, 0 when absent).
pub fn flame_graph(rows: &[TreeRow], m: i64, raw: i64) -> Result<FlameGraph, ProfilesReadError> {
    let _ = (rows, m, raw);
    Ok(FlameGraph::default())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A row of the fixture: `path` names the node from the root down; ids
    /// are the path's position in `paths`, plus one.
    fn rows(spec: &[(&str, i64, i64)]) -> Vec<TreeRow> {
        let paths: Vec<&str> = spec.iter().map(|(p, _, _)| *p).collect();
        let id = |p: &str| -> u128 {
            paths
                .iter()
                .position(|q| *q == p)
                .map(|i| i as u128 + 1)
                .unwrap_or_else(|| panic!("no row for {p}"))
        };
        spec.iter()
            .map(|(path, total, self_value)| {
                let (parent, name) = match path.rsplit_once('/') {
                    Some((parent, name)) => (id(parent), name),
                    None => (0, *path),
                };
                TreeRow {
                    node: id(path),
                    parent,
                    name: name.to_string(),
                    total: *total,
                    self_value: *self_value,
                }
            })
            .collect()
    }

    /// Low-level §7.2's fixture after S5 (plan §3.1).
    fn fixture() -> Vec<TreeRow> {
        rows(&[
            ("main", 32, 0),
            ("main/a", 24, 2),
            ("main/a/b", 17, 17),
            ("main/a/c", 5, 5),
            ("main/d", 3, 3),
            ("main/e", 1, 0),
            ("main/e/f", 1, 1),
            ("main/other", 4, 0),
            ("main/other/x", 4, 4),
        ])
    }

    fn graph(names: &[&str], levels: &[&[i64]], total: i64, max_self: i64) -> FlameGraph {
        FlameGraph {
            names: names.iter().map(|s| s.to_string()).collect(),
            levels: levels.iter().map(|l| l.to_vec()).collect(),
            total,
            max_self,
        }
    }

    /// F1: no cut. The `other` named in the data is folded in step 3, so
    /// `x` is absent.
    #[test]
    fn f1_no_cut() {
        assert_eq!(
            flame_graph(&fixture(), 8192, 0),
            Ok(graph(
                &["total", "main", "other", "e", "f", "d", "a", "c", "b"],
                &[
                    &[0, 32, 0, 0],
                    &[0, 32, 0, 1],
                    &[0, 24, 2, 6, 0, 3, 3, 5, 0, 1, 0, 3, 0, 4, 4, 2],
                    &[2, 17, 17, 8, 0, 5, 5, 7, 3, 1, 1, 4],
                ],
                32,
                17,
            ))
        );
    }

    /// F2: `max_nodes` 4 — `min1` 5 (`c` kept, the comparison is `≥`),
    /// `min2` 8, `min3` 8.
    #[test]
    fn f2_four_nodes() {
        assert_eq!(
            flame_graph(&fixture(), 4, 4),
            Ok(graph(
                &["total", "main", "other", "a", "b"],
                &[
                    &[0, 32, 0, 0],
                    &[0, 32, 0, 1],
                    &[0, 24, 2, 3, 0, 8, 8, 2],
                    &[2, 17, 17, 4, 0, 5, 5, 2],
                ],
                32,
                17,
            ))
        );
    }

    /// F3: no rows is the reference's empty flame graph.
    #[test]
    fn f3_no_rows() {
        assert_eq!(
            flame_graph(&[], 8192, 0),
            Ok(graph(&["total"], &[&[0, 0, 0, 0]], 0, 0))
        );
    }

    /// F4: a row whose parent is neither 0 nor another row's node.
    #[test]
    fn f4_a_dangling_parent_is_internal() {
        let mut r = rows(&[("main", 1, 0), ("main/a", 1, 1)]);
        r[1].parent = 99;
        assert_eq!(
            flame_graph(&r, 8192, 0),
            Err(ProfilesReadError::Internal(
                "malformed merged tree".to_string()
            ))
        );
    }

    /// F5 (D11): a kept child named `other` takes its cut siblings' sum, so
    /// the total stays the samples' sum. The reference answers total 15,
    /// with `other` 5.
    #[test]
    fn f5_a_named_other_keeps_its_cut_siblings() {
        let r = rows(&[
            ("main", 16, 0),
            ("main/a", 10, 10),
            ("main/b", 1, 1),
            ("main/other", 5, 5),
        ]);
        assert_eq!(
            flame_graph(&r, 3, 3),
            Ok(graph(
                &["total", "main", "other", "a"],
                &[&[0, 16, 0, 0], &[0, 16, 0, 1], &[0, 10, 10, 3, 0, 6, 6, 2]],
                16,
                10,
            ))
        );
    }
}
