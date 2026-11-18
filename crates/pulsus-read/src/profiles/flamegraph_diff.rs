//! Issue #827: the diff of two merged trees, a port of the reference's
//! `NewFlamegraphDiff` (`pkg/model/flamegraph_diff.go`), run on the rows S7
//! returns (`super::diff`). Pure: no I/O.
//!
//! Each side is trimmed with its own `max_nodes` as the flame graph's tree
//! is (steps 1 and 2 of `super::flamegraph`, #823's D11 included). The two
//! trees are then aligned under virtual roots and walked into one graph,
//! not cut again: the reference's mounted `Diff` runs its diff with no
//! limit. The reference's check for a negative
//! self is not ported: ingest stores no value of 0 or below.

use std::collections::HashMap;

use super::diff::DiffRow;
use super::engine::ProfilesReadError;
use super::flamegraph::{OTHER, Tree, merged_tree, truncated};
use super::tree::TreeRow;

/// A diff flame graph: the names, and per level the node pairs as
/// `[left x offset delta, left total, left self, right x offset delta,
/// right total, right self, name index]`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FlameGraphDiff {
    pub names: Vec<String>,
    pub levels: Vec<Vec<i64>>,
    pub total: i64,
    pub max_self: i64,
    pub left_ticks: i64,
    pub right_ticks: i64,
}

/// The diff of S7's rows: steps 1 and 2 on each side with its own resolved
/// `max_nodes`, then [`diff_of`] with no cut.
pub fn flame_graph_diff(
    rows: &[DiffRow],
    left_m: i64,
    right_m: i64,
) -> Result<FlameGraphDiff, ProfilesReadError> {
    let side = |total: fn(&DiffRow) -> (i64, i64)| -> Vec<TreeRow> {
        rows.iter()
            .filter(|r| total(r).0 > 0)
            .map(|r| TreeRow {
                node: r.node,
                parent: r.parent,
                name: r.name.clone(),
                total: total(r).0,
                self_value: total(r).1,
            })
            .collect()
    };
    let left = truncated(
        merged_tree(&side(|r| (r.left_total, r.left_self)), left_m)?,
        left_m,
    );
    let right = truncated(
        merged_tree(&side(|r| (r.right_total, r.right_self)), right_m)?,
        right_m,
    );
    Ok(diff_of(&left, &right, 0))
}

/// One node of the two aligned trees: each side's total and self, 0 on a
/// side that does not hold it.
struct Pair {
    name: String,
    left: (i64, i64),
    right: (i64, i64),
    children: Vec<usize>,
}

/// Steps 3 to 6: the reference's `combineTree`, `combineMinValues` and
/// `NewFlamegraphDiff` walk over two trimmed trees, with `m`.
pub fn diff_of(left: &Tree, right: &Tree, m: i64) -> FlameGraphDiff {
    // Step 3: the virtual roots, then the children aligned by name.
    let ticks = |t: &Tree| t.roots.iter().map(|&r| t.nodes[r].total).sum::<i64>();
    let (left_ticks, right_ticks) = (ticks(left), ticks(right));
    let mut pairs = vec![Pair {
        name: String::new(),
        left: (left_ticks, 0),
        right: (right_ticks, 0),
        children: Vec::new(),
    }];
    // (the pair, its left node's children, its right node's children)
    let mut stack: Vec<(usize, &[usize], &[usize])> = vec![(0, &left.roots, &right.roots)];
    while let Some((at, l, r)) = stack.pop() {
        let (mut i, mut j) = (0, 0);
        while i < l.len() || j < r.len() {
            let ln = l.get(i).map(|&x| &left.nodes[x]);
            let rn = r.get(j).map(|&x| &right.nodes[x]);
            let (ln, rn) = match (ln, rn) {
                (Some(a), Some(b)) => match a.name.as_bytes().cmp(b.name.as_bytes()) {
                    std::cmp::Ordering::Equal => (Some(a), Some(b)),
                    std::cmp::Ordering::Less => (Some(a), None),
                    std::cmp::Ordering::Greater => (None, Some(b)),
                },
                other => other,
            };
            i += usize::from(ln.is_some());
            j += usize::from(rn.is_some());
            let name = ln.or(rn).map(|n| n.name.clone()).unwrap_or_default();
            let index = pairs.len();
            pairs.push(Pair {
                name,
                left: ln.map(|n| (n.total, n.self_value)).unwrap_or((0, 0)),
                right: rn.map(|n| (n.total, n.self_value)).unwrap_or((0, 0)),
                children: Vec::new(),
            });
            pairs[at].children.push(index);
            stack.push((
                index,
                ln.map(|n| n.children.as_slice()).unwrap_or(&[]),
                rn.map(|n| n.children.as_slice()).unwrap_or(&[]),
            ));
        }
    }

    // Step 4: with more than `m` pairs, the `m`-th largest of each pair's
    // larger total.
    let min = if m < 1 || pairs.len() as i64 <= m {
        0
    } else {
        let mut v: Vec<i64> = pairs.iter().map(|p| p.left.0.max(p.right.0)).collect();
        v.sort_unstable_by(|a, b| b.cmp(a));
        usize::try_from(m - 1)
            .ok()
            .and_then(|i| v.get(i).copied())
            .unwrap_or(0)
    };

    // Step 5: the walk, last pushed first.
    enum Item {
        Pair(usize),
        Other(i64, i64),
    }
    let mut names: Vec<String> = Vec::new();
    let mut cache: HashMap<String, i64> = HashMap::new();
    let mut levels: Vec<Vec<i64>> = Vec::new();
    let mut max_self: i64 = 0;
    let mut stack: Vec<(Item, i64, i64, usize)> = vec![(Item::Pair(0), 0, 0, 0)];
    while let Some((item, mut xl, mut xr, level)) = stack.pop() {
        let (name, (lt, ls), (rt, rs), children): (&str, _, _, &[usize]) = match item {
            Item::Pair(i) => {
                let p = &pairs[i];
                (&p.name, p.left, p.right, &p.children)
            }
            Item::Other(l, r) => (OTHER, (l, l), (r, r), &[]),
        };
        if !(lt >= min || rt >= min || name == OTHER) {
            continue;
        }
        let index = match cache.get(name) {
            Some(&i) => i,
            None => {
                let i = names.len() as i64;
                cache.insert(name.to_string(), i);
                names.push(if i == 0 { "total" } else { name }.to_string());
                i
            }
        };
        if level == levels.len() {
            levels.push(Vec::new());
        }
        max_self = max_self.max(ls).max(rs);
        levels[level].extend([xl, lt, ls, xr, rt, rs, index]);
        xl += ls;
        xr += rs;
        let (mut ol, mut or) = (0i64, 0i64);
        for &c in children {
            let child = &pairs[c];
            if child.left.0 >= min || child.right.0 >= min {
                stack.push((Item::Pair(c), xl, xr, level + 1));
                xl += child.left.0;
                xr += child.right.0;
            } else {
                ol += child.left.0;
                or += child.right.0;
            }
        }
        if ol != 0 || or != 0 {
            stack.push((Item::Other(ol, or), xl, xr, level + 1));
        }
    }

    // Step 6: each level's pairs reversed, both x offsets delta-encoded.
    for l in &mut levels {
        let chunks: Vec<&[i64]> = l.chunks(7).rev().collect();
        *l = chunks.concat();
        for start in [0, 3] {
            let mut prev: i64 = 0;
            for i in (start..l.len()).step_by(7) {
                l[i] -= prev;
                prev += l[i] + l[i + 1];
            }
        }
    }
    FlameGraphDiff {
        names,
        levels,
        total: left_ticks + right_ticks,
        max_self,
        left_ticks,
        right_ticks,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// A node id: a hash of the path from the root, so a shared prefix
    /// shares its ids, as ingest's ids do.
    fn node_id(path: &[&str]) -> u128 {
        let mut h: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
        for name in path {
            for b in name.bytes().chain([0xff]) {
                h ^= u128::from(b);
                h = h.wrapping_mul(0x0000_0000_0100_0000_0000_0000_0000_013b);
            }
        }
        h
    }

    /// S7's rows for two sides given as stacks (`"main,a"`, value): one row
    /// per node of the union, each side's total and self beside it.
    fn rows(left: &[(&str, i64)], right: &[(&str, i64)]) -> Vec<DiffRow> {
        // path → (parent, name, [left total, left self, right total, right self])
        let mut nodes: BTreeMap<Vec<String>, (u128, String, [i64; 4])> = BTreeMap::new();
        for (k, stacks) in [(0, left), (2, right)] {
            for (stack, value) in stacks {
                let names: Vec<&str> = stack.split(',').collect();
                for n in 1..=names.len() {
                    let path: Vec<String> = names[..n].iter().map(|s| s.to_string()).collect();
                    let parent = if n == 1 { 0 } else { node_id(&names[..n - 1]) };
                    let e = nodes
                        .entry(path)
                        .or_insert((parent, names[n - 1].to_string(), [0; 4]));
                    e.2[k] += value;
                    if n == names.len() {
                        e.2[k + 1] += value;
                    }
                }
            }
        }
        nodes
            .into_iter()
            .map(|(path, (parent, name, v))| {
                let path: Vec<&str> = path.iter().map(String::as_str).collect();
                DiffRow {
                    node: node_id(&path),
                    parent,
                    name,
                    left_total: v[0],
                    left_self: v[1],
                    right_total: v[2],
                    right_self: v[3],
                }
            })
            .collect()
    }

    fn graph(
        names: &[&str],
        levels: &[&[i64]],
        (total, max_self, left_ticks, right_ticks): (i64, i64, i64, i64),
    ) -> FlameGraphDiff {
        FlameGraphDiff {
            names: names.iter().map(|s| s.to_string()).collect(),
            levels: levels.iter().map(|l| l.to_vec()).collect(),
            total,
            max_self,
            left_ticks,
            right_ticks,
        }
    }

    /// G1: two trees sharing a root, one leaf each.
    #[test]
    fn g1() {
        assert_eq!(
            flame_graph_diff(&rows(&[("main,a", 3)], &[("main,b", 5)]), 8192, 8192),
            Ok(graph(
                &["total", "main", "b", "a"],
                &[
                    &[0, 3, 0, 0, 5, 0, 0],
                    &[0, 3, 0, 0, 5, 0, 1],
                    &[0, 3, 3, 0, 0, 0, 3, 0, 0, 0, 0, 5, 5, 2],
                ],
                (8, 5, 3, 5),
            ))
        );
    }

    /// G2: an empty right side answers ticks 0 on that side.
    #[test]
    fn g2() {
        assert_eq!(
            flame_graph_diff(&rows(&[("main,a", 3)], &[]), 8192, 8192),
            Ok(graph(
                &["total", "main", "a"],
                &[
                    &[0, 3, 0, 0, 0, 0, 0],
                    &[0, 3, 0, 0, 0, 0, 1],
                    &[0, 3, 3, 0, 0, 0, 2],
                ],
                (3, 3, 3, 0),
            ))
        );
    }

    /// G3: no rows is the virtual root alone.
    #[test]
    fn g3() {
        assert_eq!(
            flame_graph_diff(&[], 8192, 8192),
            Ok(graph(&["total"], &[&[0, 0, 0, 0, 0, 0, 0]], (0, 0, 0, 0)))
        );
    }

    /// G4: each side trimmed with its own limit, the combined graph not
    /// cut: the left's `c` and the right's `a` go to each side's `other`
    /// (C27 of #827 part B).
    #[test]
    fn g4() {
        assert_eq!(
            flame_graph_diff(
                &rows(
                    &[("main,a", 4), ("main,b", 3), ("main,c", 2)],
                    &[("main,a", 1), ("main,b", 3), ("main,c", 6)],
                ),
                3,
                3,
            ),
            Ok(graph(
                &["total", "main", "other", "c", "b", "a"],
                &[
                    &[0, 9, 0, 0, 10, 0, 0],
                    &[0, 9, 0, 0, 10, 0, 1],
                    &[
                        0, 4, 4, 0, 0, 0, 5, 0, 3, 3, 0, 3, 3, 4, 0, 0, 0, 0, 6, 6, 3, 0, 2, 2, 0,
                        1, 1, 2,
                    ],
                ],
                (19, 6, 9, 10),
            ))
        );
    }

    /// G5: a named `other` and its child stay (Y1), unlike the flame graph.
    #[test]
    fn g5() {
        assert_eq!(
            flame_graph_diff(
                &rows(
                    &[("main,other,x", 4), ("main,a", 2)],
                    &[("main,other,x", 1)],
                ),
                8192,
                8192,
            ),
            Ok(graph(
                &["total", "main", "other", "x", "a"],
                &[
                    &[0, 6, 0, 0, 1, 0, 0],
                    &[0, 6, 0, 0, 1, 0, 1],
                    &[0, 2, 2, 0, 0, 0, 4, 0, 4, 0, 0, 1, 0, 2],
                    &[2, 4, 4, 0, 1, 1, 3],
                ],
                (7, 4, 6, 1),
            ))
        );
    }

    /// G6: the left side's named `other` keeps its cut sibling (D11 of
    /// #823), so the left ticks are 16; the reference answers 15.
    #[test]
    fn g6() {
        assert_eq!(
            flame_graph_diff(
                &rows(
                    &[("main,a", 10), ("main,b", 1), ("main,other", 5)],
                    &[("main,a", 2)],
                ),
                3,
                3,
            ),
            Ok(graph(
                &["total", "main", "other", "a"],
                &[
                    &[0, 16, 0, 0, 2, 0, 0],
                    &[0, 16, 0, 0, 2, 0, 1],
                    &[0, 10, 10, 0, 2, 2, 3, 0, 6, 6, 0, 0, 0, 2],
                ],
                (18, 10, 16, 2),
            ))
        );
    }

    /// G7: `main → a` 10 and `main → other → x` 1 on both sides at
    /// `max_nodes` 2: each side keeps `a` and folds `other → x` into its
    /// `other`, and the combined graph is not cut (C28 of #827 part B).
    #[test]
    fn g7() {
        let stacks = [("main,a", 10), ("main,other,x", 1)];
        assert_eq!(
            flame_graph_diff(&rows(&stacks, &stacks), 2, 2),
            Ok(graph(
                &["total", "main", "other", "a"],
                &[
                    &[0, 11, 0, 0, 11, 0, 0],
                    &[0, 11, 0, 0, 11, 0, 1],
                    &[0, 10, 10, 0, 10, 10, 3, 0, 1, 1, 0, 1, 1, 2],
                ],
                (22, 10, 11, 11),
            ))
        );
    }
}
