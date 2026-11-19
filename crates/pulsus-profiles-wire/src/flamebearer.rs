//! Issue #829: the flamebearer — the JSON answer of the render routes —
//! written as the reference's `json.Encoder` writes it
//! (`pkg/og/structs/flamebearer/flamebearer.go:18-103`,
//! `pkg/model/flamegraph.go:124-177`): its field order, strings escaped by
//! [`crate::connect::go_string`], no whitespace and a trailing `\n`.

use std::collections::BTreeMap;

/// A flame graph, or a diff's (`double`), with its metadata's type id, its
/// timeline and its groups.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Flamebearer {
    pub names: Vec<String>,
    /// Per level the nodes' values: 4 per node, or 7 when `double`.
    pub levels: Vec<Vec<i64>>,
    pub num_ticks: i64,
    pub max_self: i64,
    /// A diff's: `format` `double`.
    pub double: bool,
    /// The request's type id, `name:sample_type:sample_unit:…`.
    pub type_id: String,
    /// `null` when `None`.
    pub timeline: Option<Timeline>,
    /// `null` when `None`; keys in byte order.
    pub groups: Option<BTreeMap<String, Timeline>>,
    /// Written only when not 0.
    pub left_ticks: i64,
    /// Written only when not 0.
    pub right_ticks: i64,
}

/// A timeline: `samples` one per `duration_delta` seconds from
/// `start_time` (Unix seconds).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Timeline {
    pub start_time: i64,
    pub samples: Vec<u64>,
    pub duration_delta: i64,
}

/// The metadata's `units`, `sampleRate` and `name` for a type id
/// (`pkg/model/flamegraph.go:128-138`).
pub fn metadata(type_id: &str) -> (&str, u32, &str) {
    let _ = type_id;
    ("", 0, "")
}

impl Flamebearer {
    /// The answer's bytes, as the reference writes them, ending `\n`.
    pub fn to_json(&self) -> String {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

    fn strings(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    /// F1: the field order, the escaping of `<`, `>` and `&` in a name,
    /// and the trailing newline.
    #[test]
    fn f1_single() {
        let fb = Flamebearer {
            names: strings(&["total", "main", "std::vec::Vec<T>", "<init>", "a&b"]),
            levels: vec![
                vec![0, 20, 0, 0],
                vec![0, 20, 0, 1],
                vec![0, 14, 0, 3, 0, 6, 6, 2],
                vec![0, 14, 14, 4],
            ],
            num_ticks: 20,
            max_self: 14,
            type_id: CPU.to_string(),
            timeline: Some(Timeline {
                start_time: 1_000_005,
                samples: vec![0, 20],
                duration_delta: 15,
            }),
            ..Flamebearer::default()
        };
        assert_eq!(
            fb.to_json(),
            concat!(
                r#"{"version":1,"flamebearer":{"names":["total","main","std::vec::Vec\u003cT\u003e","\u003cinit\u003e","a\u0026b"],"levels":[[0,20,0,0],[0,20,0,1],[0,14,0,3,0,6,6,2],[0,14,14,4]],"numTicks":20,"maxSelf":14},"metadata":{"format":"single","spyName":"","sampleRate":1000000000,"units":"samples","name":"cpu"},"timeline":{"startTime":1000005,"samples":[0,20],"durationDelta":15,"watermarks":null},"groups":null,"heatmap":null}"#,
                "\n"
            )
        );
    }

    /// F2: the units table.
    #[test]
    fn f2_metadata() {
        for (id, want) in [
            (CPU, ("samples", 1_000_000_000, "cpu")),
            (
                "process_cpu:samples:count:cpu:nanoseconds",
                ("objects", 100, "samples"),
            ),
            (
                "memory:alloc_objects:count:space:bytes",
                ("objects", 100, "alloc_objects"),
            ),
            (
                "goroutines:goroutine:count:goroutine:count",
                ("objects", 100, "goroutine"),
            ),
            (
                "memory:alloc_space:bytes:space:bytes",
                ("bytes", 100, "alloc_space"),
            ),
            (
                "wall:wall:nanoseconds:wall:nanoseconds",
                ("nanoseconds", 100, "wall"),
            ),
        ] {
            assert_eq!(metadata(id), want, "{id}");
        }
    }

    /// F3: a diff is `double` with no timeline; the ticks are written only
    /// when not 0; groups are written in key order whatever order they were
    /// given in.
    #[test]
    fn f3_double() {
        let diff = Flamebearer {
            names: strings(&["total"]),
            levels: vec![vec![0, 8, 0, 0, 8, 0, 0]],
            num_ticks: 16,
            max_self: 0,
            double: true,
            type_id: CPU.to_string(),
            left_ticks: 8,
            right_ticks: 8,
            ..Flamebearer::default()
        };
        let json = diff.to_json();
        assert!(json.contains(r#""format":"double""#), "{json}");
        assert!(
            json.ends_with(
                "\"timeline\":null,\"groups\":null,\"heatmap\":null,\"leftTicks\":8,\"rightTicks\":8}\n"
            ),
            "{json}"
        );
        let zero = Flamebearer {
            left_ticks: 0,
            right_ticks: 0,
            ..diff
        }
        .to_json();
        assert!(
            zero.ends_with("\"heatmap\":null}\n")
                && !zero.contains("leftTicks")
                && !zero.contains("rightTicks"),
            "{zero}"
        );
        let t = |n: u64| Timeline {
            start_time: 1,
            samples: vec![n],
            duration_delta: 15,
        };
        let mut groups = BTreeMap::new();
        groups.insert("p2".to_string(), t(2));
        groups.insert("p1".to_string(), t(1));
        let grouped = Flamebearer {
            names: strings(&["total"]),
            levels: vec![vec![0, 0, 0, 0]],
            type_id: CPU.to_string(),
            groups: Some(groups),
            ..Flamebearer::default()
        }
        .to_json();
        assert!(
            grouped.contains(concat!(
                r#""groups":{"p1":{"startTime":1,"samples":[1],"durationDelta":15,"watermarks":null},"#,
                r#""p2":{"startTime":1,"samples":[2],"durationDelta":15,"watermarks":null}}"#
            )),
            "{grouped}"
        );
    }
}
