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

use std::collections::{HashMap, HashSet};

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
/// the `m`-th largest total cut into a child `other`
/// (`TreeFromStacktraceTree`, `pkg/model/stacktraces.go`).
pub fn merged_tree(rows: &[TreeRow], m: i64) -> Result<Tree, ProfilesReadError> {
    let mut children: HashMap<u128, Vec<usize>> = HashMap::with_capacity(rows.len());
    let ids: HashSet<u128> = rows.iter().map(|r| r.node).collect();
    for (i, r) in rows.iter().enumerate() {
        if r.parent != 0 && !ids.contains(&r.parent) {
            return Err(ProfilesReadError::Internal(
                "malformed merged tree".to_string(),
            ));
        }
        children.entry(r.parent).or_default().push(i);
    }
    // The reference's tree has a root of total 0 besides the N nodes, so
    // `m ≥ N + 1` keeps every node.
    let n = rows.len() as i64;
    let min1 = if m < 1 || m > n {
        0
    } else {
        kth(rows.iter().map(|r| r.total).chain([0]), m)
    };
    let mut tree = Tree::default();
    // (the row's node id, the tree node it becomes; `None` for the root)
    let mut stack: Vec<(u128, Option<usize>)> = vec![(0, None)];
    while let Some((id, at)) = stack.pop() {
        let mut kept = Vec::new();
        let mut cut: i64 = 0;
        for &i in children.get(&id).map(Vec::as_slice).unwrap_or(&[]) {
            let r = &rows[i];
            if r.total >= min1 {
                let index = tree.push(TreeNode {
                    name: r.name.clone(),
                    self_value: r.self_value,
                    total: r.total,
                    children: Vec::new(),
                });
                kept.push(index);
                stack.push((r.node, Some(index)));
            } else {
                cut += r.total;
            }
        }
        if cut > 0 {
            match kept.iter().find(|&&k| tree.nodes[k].name == OTHER) {
                // D11: the named node keeps the cut siblings' values.
                Some(&k) => {
                    tree.nodes[k].self_value += cut;
                    tree.nodes[k].total += cut;
                }
                None => kept.push(tree.push(TreeNode {
                    name: OTHER.to_string(),
                    self_value: cut,
                    total: cut,
                    children: Vec::new(),
                })),
            }
        }
        tree.sort_by_name(&mut kept);
        *tree.children_mut(at) = kept;
    }
    Ok(tree)
}

/// Step 2: the reference's `MarshalTruncate` then `UnmarshalTree`
/// (`pkg/model/tree.go`). With more than `m` nodes, every child whose total
/// is below the `m`-th largest total, and is not named `other`, is cut into
/// its parent's child `other`; then every total is rebuilt from the selves.
pub fn truncated(mut tree: Tree, m: i64) -> Tree {
    let reachable = tree.reachable();
    let size = reachable.len() as i64;
    let min2 = if m < 1 || size <= m {
        0
    } else {
        kth(reachable.iter().map(|&i| tree.nodes[i].total), m)
    };
    let mut stack: Vec<Option<usize>> = vec![None];
    while let Some(at) = stack.pop() {
        let mut kept = Vec::new();
        let mut cut: i64 = 0;
        for c in tree.children(at).to_vec() {
            let node = &tree.nodes[c];
            if node.total >= min2 || node.name == OTHER {
                kept.push(c);
            } else {
                cut += node.total;
            }
        }
        if cut > 0 {
            match kept.iter().find(|&&k| tree.nodes[k].name == OTHER) {
                Some(&k) => {
                    tree.nodes[k].self_value += cut;
                    tree.nodes[k].total += cut;
                }
                None => kept.push(tree.push(TreeNode {
                    name: OTHER.to_string(),
                    self_value: cut,
                    total: cut,
                    children: Vec::new(),
                })),
            }
        }
        tree.sort_by_name(&mut kept);
        stack.extend(kept.iter().map(|&k| Some(k)));
        *tree.children_mut(at) = kept;
    }
    tree.rebuild_totals();
    tree.compacted()
}

/// The flame graph of S5's rows: steps 1 and 2 with `m` (the resolved
/// `max_nodes`), step 3 with `raw` (the request's value, 0 when absent).
pub fn flame_graph(rows: &[TreeRow], m: i64, raw: i64) -> Result<FlameGraph, ProfilesReadError> {
    let tree = truncated(merged_tree(rows, m)?, m);
    Ok(flame_graph_of(&tree, raw))
}

/// Step 3: the reference's `NewFlameGraph` (`pkg/model/flamegraph.go`). A
/// depth-first walk from a virtual root, last pushed first; each node
/// appends `[name index, self, total, x]` to its level, and every child
/// below the `raw`-th largest total, or named `other`, is summed into one
/// generated `other`, pushed last. Each level is then reversed and its x
/// offsets delta-encoded.
fn flame_graph_of(tree: &Tree, raw: i64) -> FlameGraph {
    enum Item {
        Root,
        Node(usize),
        Other(i64),
    }
    let reachable = tree.reachable();
    let size = reachable.len() as i64;
    let min3 = if raw < 1 || size <= raw {
        0
    } else {
        kth(reachable.iter().map(|&i| tree.nodes[i].total), raw)
    };
    let total: i64 = tree.roots.iter().map(|&r| tree.nodes[r].total).sum();
    let mut names: Vec<String> = Vec::new();
    let mut cache: HashMap<String, i64> = HashMap::new();
    let mut levels: Vec<Vec<i64>> = Vec::new();
    let mut max_self: i64 = 0;
    let mut stack: Vec<(Item, i64, usize)> = vec![(Item::Root, 0, 0)];
    while let Some((item, mut x, level)) = stack.pop() {
        let (name, self_value, node_total, children): (&str, i64, i64, &[usize]) = match item {
            Item::Root => ("", 0, total, &tree.roots),
            Item::Node(i) => {
                let n = &tree.nodes[i];
                (&n.name, n.self_value, n.total, &n.children)
            }
            Item::Other(v) => (OTHER, v, v, &[]),
        };
        max_self = max_self.max(self_value);
        let index = match cache.get(name) {
            Some(&i) => i,
            None => {
                let i = names.len() as i64;
                // The virtual root's name, and its cache key, is `total`.
                let key = if i == 0 { "total" } else { name };
                cache.insert(key.to_string(), i);
                names.push(key.to_string());
                i
            }
        };
        if level == levels.len() {
            levels.push(Vec::new());
        }
        levels[level].extend([index, self_value, node_total, x]);
        x += self_value;
        let mut other: i64 = 0;
        for &c in children {
            let child = &tree.nodes[c];
            if child.total >= min3 && child.name != OTHER {
                stack.push((Item::Node(c), x, level + 1));
                x += child.total;
            } else {
                other += child.total;
            }
        }
        if other != 0 {
            stack.push((Item::Other(other), x, level + 1));
        }
    }
    for l in &mut levels {
        l.reverse();
        let mut prev: i64 = 0;
        for i in (0..l.len()).step_by(4) {
            l[i] -= prev;
            prev += l[i] + l[i + 1];
        }
    }
    FlameGraph {
        names,
        levels,
        total,
        max_self,
    }
}

/// The `k`-th largest of `values`, `k` from 1; 0 when there are fewer.
fn kth(values: impl Iterator<Item = i64>, k: i64) -> i64 {
    let mut v: Vec<i64> = values.collect();
    v.sort_unstable_by(|a, b| b.cmp(a));
    usize::try_from(k - 1)
        .ok()
        .and_then(|i| v.get(i).copied())
        .unwrap_or(0)
}

impl Tree {
    fn push(&mut self, node: TreeNode) -> usize {
        self.nodes.push(node);
        self.nodes.len() - 1
    }

    /// The children of `at`, or the roots when `at` is `None`.
    fn children(&self, at: Option<usize>) -> &[usize] {
        match at {
            Some(i) => &self.nodes[i].children,
            None => &self.roots,
        }
    }

    fn children_mut(&mut self, at: Option<usize>) -> &mut Vec<usize> {
        match at {
            Some(i) => &mut self.nodes[i].children,
            None => &mut self.roots,
        }
    }

    /// Sorts `nodes` by name, in byte order.
    fn sort_by_name(&self, nodes: &mut [usize]) {
        nodes.sort_by(|a, b| self.nodes[*a].name.cmp(&self.nodes[*b].name));
    }

    /// Every node reachable from the roots, parents before children.
    pub fn reachable(&self) -> Vec<usize> {
        let mut out = Vec::with_capacity(self.nodes.len());
        let mut stack: Vec<usize> = self.roots.clone();
        while let Some(i) = stack.pop() {
            out.push(i);
            stack.extend(&self.nodes[i].children);
        }
        out
    }

    /// The reachable nodes alone, renumbered.
    fn compacted(&self) -> Tree {
        let order = self.reachable();
        let mut renumbered: HashMap<usize, usize> = HashMap::with_capacity(order.len());
        for (new, &old) in order.iter().enumerate() {
            renumbered.insert(old, new);
        }
        let nodes = order
            .iter()
            .map(|&old| {
                let n = &self.nodes[old];
                TreeNode {
                    name: n.name.clone(),
                    self_value: n.self_value,
                    total: n.total,
                    children: n.children.iter().map(|c| renumbered[c]).collect(),
                }
            })
            .collect();
        Tree {
            nodes,
            roots: self.roots.iter().map(|r| renumbered[r]).collect(),
        }
    }

    /// Every total rebuilt as the node's self plus its children's totals,
    /// as the reference's `UnmarshalTree` rebuilds them.
    fn rebuild_totals(&mut self) {
        for &i in self.reachable().iter().rev() {
            let children: i64 = self.nodes[i]
                .children
                .iter()
                .map(|&c| self.nodes[c].total)
                .sum();
            self.nodes[i].total = self.nodes[i].self_value + children;
        }
    }
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
        // Step 2 cuts `c` whichever way step 1 compares, so step 1's tree
        // is read directly: `c` (5, equal to `min1`) is kept, and `d`, `e`
        // and `other` are cut into a stub of 8.
        let tree = merged_tree(&fixture(), 4).expect("a tree");
        let shape = |at: &[usize]| -> Vec<(String, i64, i64)> {
            at.iter()
                .map(|&i| {
                    let n = &tree.nodes[i];
                    (n.name.clone(), n.total, n.self_value)
                })
                .collect()
        };
        let main = &tree.nodes[tree.roots[0]];
        assert_eq!(
            shape(&main.children),
            [("a".to_string(), 24, 2), ("other".to_string(), 8, 8)]
        );
        let a = &tree.nodes[main.children[0]];
        assert_eq!(
            shape(&a.children),
            [("b".to_string(), 17, 17), ("c".to_string(), 5, 5)]
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
