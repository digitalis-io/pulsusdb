//! Issue #829: the flamebearer — the JSON answer of the render routes —
//! written as the reference's `json.Encoder` writes it
//! (`pkg/og/structs/flamebearer/flamebearer.go:18-103`,
//! `pkg/model/flamegraph.go:124-177`): its field order, strings escaped by
//! [`crate::connect::go_string`], no whitespace and a trailing `\n`.
//! Issue #831: the metadata of an ad hoc upload ([`Meta`]), and an
//! uploaded flamebearer file read back into its stacks ([`parse_file`],
//! [`stacks_of_levels`]).

use std::collections::BTreeMap;
use std::fmt::Write as _;

use crate::connect::go_string;

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
    /// Issue #831: an ad hoc upload's metadata, written in place of
    /// [`metadata`]'s of `type_id` when set.
    pub meta: Option<Meta>,
}

/// Issue #831: the metadata an ad hoc upload's flame graph is written
/// with.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Meta {
    pub spy_name: String,
    pub sample_rate: u32,
    pub units: String,
    pub name: String,
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
    let mut parts = type_id.split(':');
    let sample_type = parts.nth(1).unwrap_or_default();
    let sample_unit = parts.next().unwrap_or_default();
    match sample_type {
        "inuse_objects" | "alloc_objects" | "goroutine" | "samples" => {
            ("objects", 100, sample_type)
        }
        "cpu" => ("samples", 1_000_000_000, sample_type),
        _ => (sample_unit, 100, sample_type),
    }
}

impl Flamebearer {
    /// The answer's bytes, as the reference writes them, ending `\n`.
    pub fn to_json(&self) -> String {
        let values: usize = self.levels.iter().map(Vec::len).sum();
        let mut out = String::with_capacity(256 + 12 * values + 24 * self.names.len());
        out.push_str(r#"{"version":1,"flamebearer":{"names":["#);
        for (i, name) in self.names.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            quoted(&mut out, name);
        }
        out.push_str(r#"],"levels":["#);
        for (i, level) in self.levels.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push('[');
            join(&mut out, level);
            out.push(']');
        }
        let (spy_name, units, sample_rate, name) = match &self.meta {
            Some(m) => (
                m.spy_name.as_str(),
                m.units.as_str(),
                m.sample_rate,
                m.name.as_str(),
            ),
            None => {
                let (units, sample_rate, name) = metadata(&self.type_id);
                ("", units, sample_rate, name)
            }
        };
        let format = if self.double { "double" } else { "single" };
        let _ = write!(
            out,
            r#"],"numTicks":{},"maxSelf":{}}},"metadata":{{"format":"{format}","spyName":"#,
            self.num_ticks, self.max_self
        );
        quoted(&mut out, spy_name);
        let _ = write!(out, r#","sampleRate":{sample_rate},"units":"#);
        quoted(&mut out, units);
        out.push_str(r#","name":"#);
        quoted(&mut out, name);
        out.push_str(r#"},"timeline":"#);
        match &self.timeline {
            Some(t) => timeline(&mut out, t),
            None => out.push_str("null"),
        }
        out.push_str(r#","groups":"#);
        match &self.groups {
            Some(groups) => {
                out.push('{');
                for (i, (key, t)) in groups.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    quoted(&mut out, key);
                    out.push(':');
                    timeline(&mut out, t);
                }
                out.push('}');
            }
            None => out.push_str("null"),
        }
        out.push_str(r#","heatmap":null"#);
        if self.left_ticks != 0 {
            let _ = write!(out, r#","leftTicks":{}"#, self.left_ticks);
        }
        if self.right_ticks != 0 {
            let _ = write!(out, r#","rightTicks":{}"#, self.right_ticks);
        }
        out.push_str("}\n");
        out
    }
}

/// `s` as a JSON string, escaped as the reference escapes it.
fn quoted(out: &mut String, s: &str) {
    out.push('"');
    go_string(out, s);
    out.push('"');
}

/// `values` written comma-separated.
fn join<T: std::fmt::Display>(out: &mut String, values: &[T]) {
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let _ = write!(out, "{v}");
    }
}

fn timeline(out: &mut String, t: &Timeline) {
    let _ = write!(out, r#"{{"startTime":{},"samples":["#, t.start_time);
    join(out, &t.samples);
    let _ = write!(
        out,
        r#"],"durationDelta":{},"watermarks":null}}"#,
        t.duration_delta
    );
}

/// Issue #831: an uploaded flamebearer file, checked as the reference
/// checks one.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FileProfile {
    pub names: Vec<String>,
    /// Per level the nodes' values, delta-encoded, 4 per node.
    pub levels: Vec<Vec<i64>>,
    pub spy_name: String,
    pub sample_rate: u32,
    pub units: String,
    pub name: String,
}

// Ported from the reference at tag v2.4.3: pkg/og/structs/flamebearer/flamebearer.go, under the GNU Affero General Public License v3.0, the licence of this repository.
// Changed for PulsusDB, issue #831: ported from Go to Rust; an index out of range refused instead of a panic.

/// An uploaded file as the reference's `FlamebearerProfile` reads it;
/// unknown fields are ignored.
#[derive(serde::Deserialize, Default)]
#[serde(default)]
struct FileJson {
    version: u64,
    flamebearer: FileFlamebearer,
    metadata: FileMetadata,
}

#[derive(serde::Deserialize, Default)]
#[serde(default)]
struct FileFlamebearer {
    names: Vec<String>,
    levels: Vec<Vec<i64>>,
}

#[derive(serde::Deserialize, Default)]
#[serde(default, rename_all = "camelCase")]
struct FileMetadata {
    format: String,
    spy_name: String,
    sample_rate: u32,
    units: String,
    name: String,
}

/// Issue #831: reads an uploaded flamebearer file: the reference's
/// `Validate` (`flamebearer.go:212-260`), then `ProfileToTree`'s checks
/// (`:262-269`). Each refusal is the reference's text, prefixed as its JSON
/// converter prefixes it.
pub fn parse_file(bytes: &[u8]) -> Result<FileProfile, String> {
    let f: FileJson =
        serde_json::from_slice(bytes).map_err(|e| format!("unable to unmarshall JSON: {e}"))?;
    validate(&f).map_err(|e| format!("invalid profile: {e}"))?;
    let tree_error = |e: String| format!("could not convert flameabearer to tree: {e}");
    if f.metadata.format != "single" {
        return Err(tree_error(format!(
            "unsupported flamebearer format {}",
            f.metadata.format
        )));
    }
    if f.version != 1 {
        return Err(tree_error(format!(
            "unsupported flamebearer version {}",
            f.version
        )));
    }
    Ok(FileProfile {
        names: f.flamebearer.names,
        levels: f.flamebearer.levels,
        spy_name: f.metadata.spy_name,
        sample_rate: f.metadata.sample_rate,
        units: f.metadata.units,
        name: f.metadata.name,
    })
}

/// The reference's `FlamebearerProfile.Validate`, its texts included.
fn validate(f: &FileJson) -> Result<(), String> {
    if f.version > 1 {
        return Err(format!("unsupported version {}", f.version));
    }
    let format = f.metadata.format.as_str();
    let modulus = match format {
        "single" => 4,
        "double" => 7,
        other => return Err(format!("unsupported format {other}")),
    };
    let fb = &f.flamebearer;
    if fb.names.is_empty() {
        return Err("a profile must have at least one symbol name".to_string());
    }
    if fb.levels.is_empty() {
        return Err("a profile must have at least one profiling level".to_string());
    }
    for l in &fb.levels {
        if l.len() % modulus != 0 {
            return Err(format!(
                "a profile level should have a multiple of {modulus} values, but there's a level with {} values",
                l.len()
            ));
        }
        for &i in l.iter().skip(modulus - 1).step_by(modulus) {
            if i >= fb.names.len() as i64 {
                return Err(format!(
                    "invalid name index {i}, it should be smaller than {}",
                    fb.levels.len()
                ));
            }
            if i < 0 {
                return Err(format!(
                    "invalid name index {i}, it should be a non-negative value"
                ));
            }
        }
    }
    Ok(())
}

/// Issue #831: the stacks of a single flamebearer's levels, root first,
/// each node of self above 0 with its self: the reference's
/// `flamebearerV1ToTree` (`deltaDecoding`, then `buildStack` per node,
/// `flamebearer.go:271-312`), level 0, the root, skipped.
pub fn stacks_of_levels(
    names: &[String],
    levels: &[Vec<i64>],
) -> Result<Vec<(Vec<String>, i64)>, String> {
    let out_of_range = || "a level's node is out of range".to_string();
    let mut decoded: Vec<Vec<i64>> = levels.to_vec();
    for l in &mut decoded {
        let mut prev = 0i64;
        for i in (0..l.len()).step_by(4) {
            let (x, total) = (l[i], *l.get(i + 1).ok_or_else(out_of_range)?);
            l[i] = x + prev;
            prev += x + total;
        }
    }
    let name = |i: i64| -> Result<String, String> {
        usize::try_from(i)
            .ok()
            .and_then(|i| names.get(i))
            .cloned()
            .ok_or_else(out_of_range)
    };
    let mut out = Vec::new();
    for (level, l) in decoded.iter().enumerate().skip(1) {
        for j in (0..l.len()).step_by(4) {
            let self_value = *l.get(j + 2).ok_or_else(out_of_range)?;
            if self_value <= 0 {
                continue;
            }
            let mut stack = vec![String::new(); level];
            stack[level - 1] = name(*l.get(j + 3).ok_or_else(out_of_range)?)?;
            let mut x = l[j];
            for i in (1..level).rev() {
                let above = &decoded[i];
                // The last node of level `i` starting at or before `x`.
                let (mut lo, mut hi) = (0, above.len() / 4);
                while lo < hi {
                    let mid = (lo + hi) / 2;
                    if above[mid * 4] > x {
                        hi = mid;
                    } else {
                        lo = mid + 1;
                    }
                }
                let k = lo.checked_sub(1).ok_or_else(out_of_range)?;
                stack[i - 1] = name(above[k * 4 + 3])?;
                x = above[k * 4];
            }
            out.push((stack, self_value));
        }
    }
    Ok(out)
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

    /// F3 (issue #831): an upload's metadata is written in place of the
    /// type id's, and an empty group map as `{}`.
    #[test]
    fn f3_meta() {
        let fb = Flamebearer {
            names: strings(&["total", "main", "other"]),
            levels: vec![vec![0, 11, 0, 0], vec![0, 11, 0, 1], vec![0, 11, 11, 2]],
            num_ticks: 11,
            max_self: 11,
            groups: Some(BTreeMap::new()),
            meta: Some(Meta {
                spy_name: "unknown".to_string(),
                sample_rate: 100,
                units: String::new(),
                name: "c_1.txt".to_string(),
            }),
            ..Flamebearer::default()
        };
        assert_eq!(
            fb.to_json(),
            concat!(
                r#"{"version":1,"flamebearer":{"names":["total","main","other"],"levels":[[0,11,0,0],[0,11,0,1],[0,11,11,2]],"numTicks":11,"maxSelf":11},"metadata":{"format":"single","spyName":"unknown","sampleRate":100,"units":"","name":"c_1.txt"},"timeline":null,"groups":{},"heatmap":null}"#,
                "\n"
            )
        );
    }

    /// F4 (issue #831): F-J parses with its metadata, and its stacks are
    /// its nodes of self above 0; a double file with 4-value levels and a
    /// version-0 file are refused with the reference's texts.
    #[test]
    fn f4_parse_file() {
        let fj = br#"{"version":1,"flamebearer":{"names":["total","main","a","b"],"levels":[[0,11,0,0],[0,11,1,1],[0,7,7,2,0,3,3,3]],"numTicks":11,"maxSelf":7},"metadata":{"format":"single","spyName":"gospy","sampleRate":250,"units":"bytes","name":"b"}}"#;
        let f = parse_file(fj).expect("F-J parses");
        assert_eq!(
            (
                f.spy_name.as_str(),
                f.sample_rate,
                f.units.as_str(),
                f.name.as_str()
            ),
            ("gospy", 250, "bytes", "b")
        );
        assert_eq!(f.names, strings(&["total", "main", "a", "b"]));
        assert_eq!(
            stacks_of_levels(&f.names, &f.levels).expect("stacks"),
            vec![
                (strings(&["main"]), 1),
                (strings(&["main", "a"]), 7),
                (strings(&["main", "b"]), 3),
            ]
        );
        let double = br#"{"version":1,"flamebearer":{"names":["total"],"levels":[[0,1,1,0]]},"metadata":{"format":"double"}}"#;
        assert_eq!(
            parse_file(double),
            Err("invalid profile: a profile level should have a multiple of 7 values, but there's a level with 4 values".to_string())
        );
        let v0 = br#"{"version":0,"flamebearer":{"names":["total"],"levels":[[0,1,1,0]]},"metadata":{"format":"single"}}"#;
        assert_eq!(
            parse_file(v0),
            Err(
                "could not convert flameabearer to tree: unsupported flamebearer version 0"
                    .to_string()
            )
        );
        let bad = parse_file(b"{").expect_err("not JSON");
        assert!(bad.starts_with("unable to unmarshall JSON: "), "{bad}");
    }
}
