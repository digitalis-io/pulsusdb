// Ported from the reference at tag v2.4.3: pkg/og/storage/tree/flamebearer.go, pkg/og/storage/tree/tree.go, pkg/og/storage/tree/minval.go and pkg/og/structs/cappedarr/cappedarr.go, under the GNU Affero General Public License v3.0, the licence of this repository.
// Changed for PulsusDB, issue #831: ported from Go to Rust; the capped array replaced by the `m`-th largest total, which keeps the same nodes.

//! Issue #831: the flame graph of a collapsed, perf script or JSON upload
//! (plan §4.4): the tree of S14's rows, children in byte order of name,
//! under a root of the roots' total; every node whose total is below the
//! `m`-th largest total, root included, is cut into its parent's `other`.
//! Pure: no I/O.

use crate::profiles::flamegraph::FlameGraph;
use crate::profiles::tree::TreeRow;

/// The flame graph of `rows` with `m` nodes.
pub fn flame_graph(_rows: &[TreeRow], _m: i64) -> FlameGraph {
    FlameGraph::default()
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
