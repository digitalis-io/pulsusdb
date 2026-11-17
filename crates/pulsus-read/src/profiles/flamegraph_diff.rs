//! Issue #827: the diff of two merged trees, a port of the reference's
//! `NewFlamegraphDiff` (`pkg/model/flamegraph_diff.go`), run on the rows S7
//! returns (`super::diff`). Pure: no I/O.

use super::diff::DiffRow;
use super::engine::ProfilesReadError;
use super::flamegraph::Tree;

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

/// The diff of S7's rows with the resolved `max_nodes` `m`.
pub fn flame_graph_diff(_rows: &[DiffRow], _m: i64) -> Result<FlameGraphDiff, ProfilesReadError> {
    Ok(FlameGraphDiff::default())
}

/// The diff of two trimmed trees with `m`.
pub fn diff_of(_left: &Tree, _right: &Tree, _m: i64) -> FlameGraphDiff {
    FlameGraphDiff::default()
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
            flame_graph_diff(&rows(&[("main,a", 3)], &[("main,b", 5)]), 8192),
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
            flame_graph_diff(&rows(&[("main,a", 3)], &[]), 8192),
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
            flame_graph_diff(&[], 8192),
            Ok(graph(&["total"], &[&[0, 0, 0, 0, 0, 0, 0]], (0, 0, 0, 0)))
        );
    }

    /// G4: the combined cut sends `a`, `b` and the named `other` to a
    /// generated `other` of (9, 4).
    #[test]
    fn g4() {
        assert_eq!(
            flame_graph_diff(
                &rows(
                    &[("main,a", 4), ("main,b", 3), ("main,c", 2)],
                    &[("main,a", 1), ("main,b", 3), ("main,c", 6)],
                ),
                3,
            ),
            Ok(graph(
                &["total", "main", "other", "c"],
                &[
                    &[0, 9, 0, 0, 10, 0, 0],
                    &[0, 9, 0, 0, 10, 0, 1],
                    &[0, 0, 0, 0, 6, 6, 3, 0, 9, 9, 0, 4, 4, 2],
                ],
                (19, 9, 9, 10),
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
    /// `max_nodes` 2: both children fall below the cut of 11.
    #[test]
    fn g7() {
        let stacks = [("main,a", 10), ("main,other,x", 1)];
        assert_eq!(
            flame_graph_diff(&rows(&stacks, &stacks), 2),
            Ok(graph(
                &["total", "main", "other"],
                &[
                    &[0, 11, 0, 0, 11, 0, 0],
                    &[0, 11, 0, 0, 11, 0, 1],
                    &[0, 11, 11, 0, 11, 11, 2],
                ],
                (22, 11, 11, 11),
            ))
        );
    }
}
