// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Changed for PulsusDB, issue #829: ported from Go to Rust, cut to the
// parts the merged profile reaches, and the legend's mapping chosen by the
// main-binary test instead of the table's first entry.

//! Issue #829 part B: the merged profile as a DOT graph — `render` with
//! `format=dot` and `SelectMergeStacktraces` with `PROFILE_FORMAT_DOT`.
//!
//! The profile is S9's (`super::pprof`), cut to `source` function nodes;
//! [`write`] draws it with at most `dot` nodes, as the reference's DOT
//! writer draws it (plan §3).

mod graph;
mod measurement;

use pulsus_profiles_wire::google_v1::{Mapping, Profile};
use regex::Regex;
use std::sync::LazyLock;

use graph::{Graph, NodeInfo};
use measurement::{percentage, scaled};

/// The graph's node count when the request sets none.
pub const DOT_MAX_NODES_DEFAULT: i64 = 100;
/// The least number of function nodes the profile is cut to.
pub const SOURCE_MAX_NODES: i64 = 512;

/// `(dot, source)` for a request's `max_nodes` (D2): the graph's node
/// count, the request's when above 0, else 100; and the profile's cut,
/// at least 512.
pub fn limits(max_nodes: Option<i64>) -> (i64, i64) {
    let dot = max_nodes
        .filter(|n| *n > 0)
        .unwrap_or(DOT_MAX_NODES_DEFAULT);
    (dot, dot.max(SOURCE_MAX_NODES))
}

/// The string of index `i`, empty when out of range.
fn string(p: &Profile, i: i64) -> &str {
    usize::try_from(i)
        .ok()
        .and_then(|i| p.string_table.get(i))
        .map_or("", String::as_str)
}

static LIBRARY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"([.]so$|[.]so[._][0-9]+)").expect("a pattern"));

/// D6: the legend's mapping, the first whose file is not a library or a
/// bracketed name, else the first.
fn main_mapping(p: &Profile) -> Option<&Mapping> {
    p.mapping
        .iter()
        .find(|m| {
            let file = string(p, m.filename).replace("(deleted)", "");
            let file = file.trim();
            !file.is_empty() && !LIBRARY.is_match(file) && !file.starts_with('[')
        })
        .or_else(|| p.mapping.first())
}

/// `\` → `\\`, `"` → `\"`, newline → `\l`.
fn escape(s: &str) -> String {
    s.replace('\\', r"\\")
        .replace('"', r#"\""#)
        .replace('\n', r"\l")
}

/// A colour for `score` in `[-1, 1]`: green, grey, red.
fn dot_color(score: f64, background: bool) -> String {
    const SHIFT: f64 = 0.7;
    let (mut saturation, value) = if background { (0.1, 0.93) } else { (1.0, 0.7) };
    let mut score = score.clamp(-1.0, 1.0);
    if score.abs() < 0.2 {
        saturation *= score.abs() / 0.2;
    }
    if score > 0.0 {
        score = score.powf(1.0 - SHIFT);
    }
    if score < 0.0 {
        score = -(-score).powf(1.0 - SHIFT);
    }
    let (r, g) = if score < 0.0 {
        (value * (1.0 + saturation * score), value)
    } else {
        (value, value * (1.0 - saturation * score))
    };
    let b = value * (1.0 - saturation);
    format!(
        "#{:02x}{:02x}{:02x}",
        (r * 255.0) as u8,
        (g * 255.0) as u8,
        (b * 255.0) as u8
    )
}

/// A node's label: its name components on lines, the name shortened.
fn multiline_name(info: &NodeInfo) -> String {
    let name = escape(&graph::shorten(&info.name))
        .replace("::", r"\n")
        .replace("[...]", "[…]")
        .replace('.', r"\n");
    let file = if info.file.is_empty() {
        String::new()
    } else {
        graph::go_base(&info.file)
    };
    let shown = NodeInfo {
        name,
        file,
        ..info.clone()
    };
    graph::name_components(&shown).join(r"\n") + r"\n"
}

/// The DOT text of `p` with at most `max_nodes` nodes (0: every node), or
/// `None` when `p` holds no sample.
pub fn write(p: &Profile, max_nodes: i64) -> Option<String> {
    if p.sample.is_empty() {
        return None;
    }
    let (sample_type, unit) = p.sample_type.last().map_or(("", String::new()), |t| {
        (string(p, t.r#type), string(p, t.unit).to_lowercase())
    });
    let value = |v: i64| scaled(v, &unit);
    let total: i64 = p
        .sample
        .iter()
        .map(|s| s.value.last().copied().unwrap_or(0).wrapping_abs())
        .fold(0, i64::wrapping_add);

    let mut g: Graph = graph::build(p, None);
    graph::sort_entropy(&mut g);
    let orig = g.order.len();
    if let Some(kept) = graph::top(&g, max_nodes) {
        g = graph::build(p, Some(&kept));
        graph::sort_entropy(&mut g);
    }
    graph::remove_redundant_edges(&mut g);

    let mapping = main_mapping(p);
    let title = mapping
        .map(|m| string(p, m.filename))
        .filter(|f| !f.is_empty())
        .map(graph::go_base)
        .unwrap_or_default();
    let mut labels: Vec<String> = Vec::new();
    if let Some(m) = mapping {
        let file = string(p, m.filename);
        if !file.is_empty() {
            labels.push(format!("File: {}", graph::go_base(file)));
        }
        let build_id = string(p, m.build_id);
        if !build_id.is_empty() {
            labels.push(format!("Build ID: {build_id}"));
        }
    }
    labels.push(format!("Type: {sample_type}"));
    if p.time_nanos != 0 {
        let t = chrono::DateTime::from_timestamp_nanos(p.time_nanos);
        labels.push(format!(
            "Time: {} (UTC)",
            t.format("%b %-d, %Y at %-I:%M%P")
        ));
    }
    let flat_sum: i64 = g
        .order
        .iter()
        .map(|&n| g.nodes[n].flat)
        .fold(0, i64::wrapping_add);
    labels.push(format!(
        "Showing nodes accounting for {}, {} of {} total",
        value(flat_sum),
        percentage(flat_sum, total),
        value(total)
    ));
    let k = g.order.len();
    if total != 0 && k > 0 && k < orig {
        labels.push(format!("Showing top {k} nodes out of {orig}"));
    }
    labels.push("\nSee https://git.io/JfYMW for how to read the graph".to_string());

    let mut out = String::new();
    let graph_name = if title.is_empty() { "unnamed" } else { &title };
    out.push_str(&format!("digraph \"{graph_name}\" {{\n"));
    out.push_str("node [style=filled fillcolor=\"#f8f8f8\"]\n");
    let escaped: Vec<String> = labels.iter().map(|l| escape(l)).collect();
    out.push_str(&format!(
        "subgraph cluster_L {{ \"{}\" [shape=box fontsize=16 label=\"{}\\l\"",
        escape(&labels[0]),
        escaped.join(r"\l")
    ));
    if !title.is_empty() {
        out.push_str(&format!(" tooltip=\"{title}\""));
    }
    out.push_str("] }\n");

    if !g.order.is_empty() {
        let ids: std::collections::HashMap<usize, usize> = g
            .order
            .iter()
            .enumerate()
            .map(|(i, &n)| (n, i + 1))
            .collect();
        let id = |n: usize| ids.get(&n).copied().unwrap_or(0);
        let max_flat = g
            .order
            .iter()
            .map(|&n| g.nodes[n].flat.wrapping_abs() as f64)
            .fold(0.0, f64::max);
        let share = |v: i64| v as f64 / total.wrapping_abs() as f64;
        for &n in &g.order {
            let node = &g.nodes[n];
            let (flat, cum) = (node.flat, node.cum);
            let mut label = multiline_name(&node.info);
            let flat_value = value(flat);
            if flat != 0 {
                label.push_str(&format!("{flat_value} ({})", percentage(flat, total)));
            } else {
                label.push('0');
            }
            let mut cum_value = flat_value;
            if cum != flat {
                label.push_str(if flat != 0 { r"\n" } else { " " });
                cum_value = value(cum);
                label.push_str(&format!("of {cum_value} ({})", percentage(cum, total)));
            }
            let mut font_size = 8;
            let f = flat.wrapping_abs() as f64;
            if max_flat != 0.0 && flat != 0 && f <= max_flat {
                font_size += (16.0 * (f / max_flat).sqrt()).ceil() as i64;
            }
            out.push_str(&format!(
                "N{i} [label=\"{label}\" id=\"node{i}\" fontsize={font_size} shape=box \
                 tooltip=\"{} ({cum_value})\" color=\"{}\" fillcolor=\"{}\"]\n",
                escape(&graph::printable_name(&node.info)),
                dot_color(share(cum), false),
                dot_color(share(cum), true),
                i = id(n),
            ));
        }
        let edges: Vec<usize> = g
            .order
            .iter()
            .flat_map(|&n| g.nodes[n].outs.iter().copied())
            .collect();
        for e in graph::sorted_edges(&g, edges) {
            let edge = &g.edges[e];
            let w = value(edge.weight);
            let mut attr = format!(
                "label=\" {w}{}\"",
                if edge.inline { r"\n (inline)" } else { "" }
            );
            if total != 0 {
                let weight = 1
                    + (edge.weight.wrapping_mul(100) / total)
                        .wrapping_abs()
                        .min(100);
                if weight > 1 {
                    attr.push_str(&format!(" weight={weight}"));
                }
                let width = 1 + (edge.weight.wrapping_mul(5) / total).wrapping_abs().min(5);
                if width > 1 {
                    attr.push_str(&format!(" penwidth={width}"));
                }
                attr.push_str(&format!(
                    " color=\"{}\"",
                    dot_color(share(edge.weight), false)
                ));
            }
            let arrow = if edge.residual { "..." } else { "->" };
            let tooltip = format!(
                "\"{} {arrow} {} ({w})\"",
                escape(&graph::printable_name(&g.nodes[edge.src].info)),
                escape(&graph::printable_name(&g.nodes[edge.dst].info)),
            );
            attr.push_str(&format!(" tooltip={tooltip} labeltooltip={tooltip}"));
            if edge.residual {
                attr.push_str(" style=\"dotted\"");
            }
            out.push_str(&format!(
                "N{} -> N{} [{attr}]\n",
                id(edge.src),
                id(edge.dst)
            ));
        }
    }
    out.push_str("}\n");
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulsus_profiles_wire::google_v1::{Function, Line, Location, Sample, ValueType};

    /// 2026-11-20 19:53:50 UTC, the reference captures' profile time.
    const TIME_NANOS: i64 = 1_795_204_430_000_000_000;

    /// A profile's tables under construction.
    #[derive(Default)]
    struct Tables {
        strings: Vec<String>,
        functions: Vec<Function>,
    }

    impl Tables {
        fn string(&mut self, s: &str) -> i64 {
            if let Some(i) = self.strings.iter().position(|x| x == s) {
                return i as i64;
            }
            self.strings.push(s.to_string());
            self.strings.len() as i64 - 1
        }

        /// The function `(name, file, start line)`, its system name its
        /// name.
        fn function(&mut self, name: &str, file: &str, start: i64) -> u64 {
            let (n, f) = (self.string(name), self.string(file));
            if let Some(x) = self
                .functions
                .iter()
                .find(|x| x.name == n && x.filename == f && x.start_line == start)
            {
                return x.id;
            }
            let id = self.functions.len() as u64 + 1;
            self.functions.push(Function {
                id,
                name: n,
                system_name: n,
                filename: f,
                start_line: start,
            });
            id
        }
    }

    fn profile(
        t: Tables,
        ty: (i64, i64),
        mapping: Vec<Mapping>,
        location: Vec<Location>,
        sample: Vec<Sample>,
    ) -> Profile {
        Profile {
            sample_type: vec![ValueType {
                r#type: ty.0,
                unit: ty.1,
            }],
            sample,
            mapping,
            location,
            function: t.functions,
            string_table: t.strings,
            time_nanos: TIME_NANOS,
            ..Profile::default()
        }
    }

    /// One location per distinct frame name, no mapping, address 0, one
    /// line `{function, 0}`; values `count × unit`.
    fn folded(stacks: &[(&str, i64)], unit: i64, ty: (&str, &str)) -> Profile {
        let mut t = Tables::default();
        t.string("");
        let ty = (t.string(ty.0), t.string(ty.1));
        let mut names: Vec<String> = Vec::new();
        let mut location = Vec::new();
        let mut sample = Vec::new();
        for (stack, count) in stacks {
            let mut ids = Vec::new();
            for frame in stack.split(';') {
                let id = match names.iter().position(|n| n == frame) {
                    Some(i) => i as u64 + 1,
                    None => {
                        names.push(frame.to_string());
                        let id = names.len() as u64;
                        let f = t.function(frame, "", 0);
                        location.push(Location {
                            id,
                            line: vec![Line {
                                function_id: f,
                                line: 0,
                            }],
                            ..Location::default()
                        });
                        id
                    }
                };
                ids.push(id);
            }
            ids.reverse();
            sample.push(Sample {
                location_id: ids,
                value: vec![count * unit],
                label: Vec::new(),
            });
        }
        profile(t, ty, Vec::new(), location, sample)
    }

    const TRIM: [(&str, i64); 6] = [
        ("main;a;b", 17),
        ("main;a;c", 5),
        ("main;d", 3),
        ("main;a", 2),
        ("main;e;f", 1),
        ("main;other;x", 4),
    ];
    const CPU: (&str, &str) = ("cpu", "nanoseconds");
    const TEN_MS: i64 = 10_000_000;

    fn assert_text(got: Option<String>, want: &str, ctx: &str) {
        let got = got.unwrap_or_else(|| panic!("{ctx}: no text"));
        assert!(
            got == want,
            "{ctx}:\n--- got ---\n{got}--- want ---\n{want}"
        );
    }

    #[test]
    fn d1_folded() {
        let p = folded(&TRIM, TEN_MS, CPU);
        assert_text(write(&p, 100), include_str!("testdata/a1.dot"), "d1");
    }

    #[test]
    fn d2_top_four() {
        let p = folded(&TRIM, TEN_MS, CPU);
        assert_text(write(&p, 4), include_str!("testdata/a2.dot"), "d2");
    }

    #[test]
    fn d3_names() {
        let p = folded(
            &[
                (
                    "main;github.com/x/y/v2/pkg.(*T).Run;com.example.Foo.bar;\
                     ns::Widget::draw<int>;(anonymous namespace)::helper",
                    99_999,
                ),
                ("main;tiny", 1),
                ("main;mid", 300),
            ],
            TEN_MS,
            CPU,
        );
        assert_text(write(&p, 100), include_str!("testdata/a3.dot"), "d3");
    }

    #[test]
    fn d4_bytes() {
        let p = folded(
            &[
                ("main;alloc", 1536),
                ("main;small", 3),
                ("main;big", 3_221_225_472),
            ],
            1,
            ("alloc_space", "bytes"),
        );
        assert_text(write(&p, 100), include_str!("testdata/a4.dot"), "d4");
    }

    /// The legend takes `app`, the second mapping, over `libx.so`, the
    /// first; nodes carry `file:line`; the cut leaves a residual edge.
    #[test]
    fn d5_mapping_lines_residual() {
        let mut t = Tables::default();
        t.string("");
        let ty = (t.string("cpu"), t.string("nanoseconds"));
        let mapping = vec![
            Mapping {
                id: 1,
                filename: t.string("libx.so"),
                ..Mapping::default()
            },
            Mapping {
                id: 2,
                filename: t.string("app"),
                build_id: t.string("bid"),
                ..Mapping::default()
            },
        ];
        let lines: [(&str, i64, i64); 8] = [
            ("main", 10, 1),
            ("a", 20, 2),
            ("a", 21, 2),
            ("b", 30, 3),
            ("c", 40, 4),
            ("d", 50, 5),
            ("e", 60, 6),
            ("zz", 70, 7),
        ];
        let mut location: Vec<Location> = lines
            .iter()
            .enumerate()
            .map(|(i, (name, line, start))| {
                let f = t.function(name, "main.go", *start);
                Location {
                    id: i as u64 + 1,
                    mapping_id: 2,
                    line: vec![Line {
                        function_id: f,
                        line: *line,
                    }],
                    ..Location::default()
                }
            })
            .collect();
        let other = t.function("other", "", 0);
        location.push(Location {
            id: 9,
            line: vec![Line {
                function_id: other,
                line: 0,
            }],
            ..Location::default()
        });
        let samples: [(&[u64], i64); 7] = [
            (&[2, 1], 4),
            (&[3, 1], 6),
            (&[4, 2, 1], 20),
            (&[5, 3, 1], 2),
            (&[7, 6, 1], 8),
            (&[8], 2),
            (&[9], 12),
        ];
        let sample = samples
            .iter()
            .map(|(ids, count)| Sample {
                location_id: ids.to_vec(),
                value: vec![count * TEN_MS],
                label: Vec::new(),
            })
            .collect();
        let p = profile(t, ty, mapping, location, sample);
        assert_text(write(&p, 3), include_str!("testdata/a5.dot"), "d5");
    }

    #[test]
    fn d6_limits() {
        for n in [None, Some(0), Some(-5)] {
            assert_eq!(limits(n), (100, 512), "{n:?}");
        }
        assert_eq!(limits(Some(4)), (4, 512));
        assert_eq!(limits(Some(1000)), (1000, 1000));
    }

    #[test]
    fn d7_no_samples() {
        assert_eq!(write(&Profile::default(), 100), None);
    }
}
