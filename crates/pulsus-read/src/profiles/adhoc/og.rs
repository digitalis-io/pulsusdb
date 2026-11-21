// Ported from the reference at tag v2.4.3: pkg/og/storage/tree/flamebearer.go, pkg/og/storage/tree/tree.go, pkg/og/storage/tree/minval.go and pkg/og/structs/cappedarr/cappedarr.go, under the GNU Affero General Public License v3.0, the licence of this repository.
// Changed for PulsusDB, issue #831: ported from Go to Rust; the capped array replaced by the `m`-th largest total, which keeps the same nodes.

//! Issue #831: the flame graph of a collapsed, perf script or JSON upload
//! (plan §4.4): the tree of S14's rows, children in byte order of name,
//! under a root of the roots' total; every node whose total is below the
//! `m`-th largest total, root included, is cut into its parent's `other`.
//! Pure: no I/O.

use std::collections::HashMap;

use crate::profiles::flamegraph::{FlameGraph, kth};
use crate::profiles::tree::TreeRow;

/// The name of a node that holds cut siblings' totals.
const OTHER: &str = "other";

/// One node of the reference's tree.
struct Node {
    name: String,
    total: i64,
    self_value: i64,
    children: Vec<usize>,
}

/// The flame graph of `rows` with `m` nodes: the reference's
/// `FlamebearerStruct` (`pkg/og/storage/tree/flamebearer.go:26-130`).
pub fn flame_graph(rows: &[TreeRow], m: i64) -> FlameGraph {
    // The tree: node 0 is the root, `""`, of the roots' total.
    let mut nodes: Vec<Node> = vec![Node {
        name: String::new(),
        total: 0,
        self_value: 0,
        children: Vec::new(),
    }];
    let mut at: HashMap<u128, usize> = HashMap::with_capacity(rows.len() + 1);
    at.insert(0, 0);
    for r in rows {
        at.insert(r.node, nodes.len());
        nodes.push(Node {
            name: r.name.clone(),
            total: r.total,
            self_value: r.self_value,
            children: Vec::new(),
        });
    }
    for (i, r) in rows.iter().enumerate() {
        let parent = at.get(&r.parent).copied().unwrap_or(0);
        nodes[parent].children.push(i + 1);
    }
    nodes[0].total = nodes[0].children.iter().map(|&c| nodes[c].total).sum();
    for i in 0..nodes.len() {
        let mut children = std::mem::take(&mut nodes[i].children);
        children.sort_by(|a, b| nodes[*a].name.as_bytes().cmp(nodes[*b].name.as_bytes()));
        nodes[i].children = children;
    }

    // `minValue`: the `m`-th largest total, root included; 0 with fewer.
    let min = if m < 1 {
        0
    } else {
        kth(nodes.iter().map(|n| n.total), m)
    };

    enum Item {
        Node(usize),
        Other(i64),
    }
    let mut names: Vec<String> = Vec::new();
    let mut cache: HashMap<String, i64> = HashMap::new();
    let mut levels: Vec<Vec<i64>> = Vec::new();
    let mut max_self: i64 = 0;
    // Last pushed first; each node prepended to its level, so a level
    // reads left to right.
    let mut stack: Vec<(Item, i64, usize)> = vec![(Item::Node(0), 0, 0)];
    while let Some((item, mut x, level)) = stack.pop() {
        let (name, total, self_value, children): (&str, i64, i64, &[usize]) = match &item {
            Item::Node(i) => {
                let n = &nodes[*i];
                (n.name.as_str(), n.total, n.self_value, &n.children)
            }
            Item::Other(v) => (OTHER, *v, *v, &[]),
        };
        if total < min && name != OTHER {
            continue;
        }
        let index = match cache.get(name) {
            Some(&i) => i,
            None => {
                let i = names.len() as i64;
                let key = if i == 0 { "total" } else { name };
                cache.insert(key.to_string(), i);
                names.push(key.to_string());
                i
            }
        };
        if level == levels.len() {
            levels.push(Vec::new());
        }
        max_self = max_self.max(self_value);
        levels[level].push(index);
        levels[level].push(self_value);
        levels[level].push(total);
        levels[level].push(x);
        x += self_value;
        let mut other: i64 = 0;
        for &c in children {
            let child = &nodes[c];
            if child.name == OTHER {
                other += child.total;
                continue;
            }
            if child.total >= min {
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
    // Each level was built back to front, four values a node reversed:
    // reversed whole it reads left to right as `x, total, self, name`.
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
        total: nodes[0].total,
        max_self,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profiles::adhoc::convert::{Format, convert};
    use crate::profiles::adhoc::tree_rows;

    use crate::profiles::adhoc::fx;

    fn graph(format: Format, name: &str, bytes: &[u8], m: i64) -> FlameGraph {
        let c = convert(format, name, bytes).expect("converts");
        flame_graph(&tree_rows(&c.types[0].stacks), m)
    }

    fn want(names: &[&str], levels: &[&[i64]], total: i64, max_self: i64) -> FlameGraph {
        FlameGraph {
            names: names.iter().map(|n| n.to_string()).collect(),
            levels: levels.iter().map(|l| l.to_vec()).collect(),
            total,
            max_self,
        }
    }

    /// u5: the reference's answers for F-X, F-C, F-J and F-S.
    #[test]
    fn u5_og() {
        assert_eq!(
            graph(Format::Collapsed, "x.txt", fx::F_X, 8192),
            want(
                &["total", "main", "other", "y"],
                &[&[0, 8, 0, 0], &[0, 8, 0, 1], &[0, 3, 3, 3, 0, 5, 5, 2]],
                8,
                5
            )
        );
        assert_eq!(
            graph(Format::Collapsed, "x.txt", fx::F_X, 1),
            want(
                &["total", "main", "other"],
                &[&[0, 8, 0, 0], &[0, 8, 0, 1], &[0, 8, 8, 2]],
                8,
                8
            )
        );
        assert_eq!(
            graph(Format::Collapsed, "c_1.txt", fx::F_C, 2),
            want(
                &["total", "main", "other"],
                &[&[0, 11, 0, 0], &[0, 11, 0, 1], &[0, 11, 11, 2]],
                11,
                11
            )
        );
        assert_eq!(
            graph(Format::Json, "a.json", fx::F_J, 8192),
            want(
                &["total", "main", "b", "a"],
                &[&[0, 11, 0, 0], &[0, 11, 1, 1], &[1, 7, 7, 3, 0, 3, 3, 2]],
                11,
                7
            )
        );
        assert_eq!(
            graph(Format::Json, "a.json", fx::F_J, 1),
            want(
                &["total", "main", "other"],
                &[&[0, 11, 0, 0], &[0, 11, 1, 1], &[1, 10, 10, 2]],
                11,
                10
            )
        );
        assert_eq!(
            graph(Format::PerfScript, "perf.txt", fx::F_S, 8192),
            want(
                &["total", "java", "foo", "bar"],
                &[
                    &[0, 2, 0, 0],
                    &[0, 2, 0, 1],
                    &[0, 1, 0, 3, 0, 1, 1, 2],
                    &[0, 1, 1, 2]
                ],
                2,
                1
            )
        );
    }
}
