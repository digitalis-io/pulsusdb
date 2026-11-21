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
// Changed for PulsusDB, issue #829: ported from Go to Rust, and cut to
// the parts the DOT writer reaches with the merged profile's samples (no
// tags, no filters, entropy order only).

//! Issue #829 part B (plan §3.1): the call graph of a profile — one node
//! per location line, the edges between them, the entropy order, the top
//! `n` nodes and the redundant residual edges removed.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::LazyLock;

use pulsus_profiles_wire::google_v1::{Function, Profile};
use regex::Regex;

/// A node's identity.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct NodeInfo {
    pub name: String,
    pub address: u64,
    pub file: String,
    pub start_line: i64,
    pub lineno: i64,
    pub objfile: String,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub info: NodeInfo,
    pub flat: i64,
    pub cum: i64,
    /// The node's in-edges, as indices into [`Graph::edges`], in insertion
    /// order.
    pub ins: Vec<usize>,
    pub outs: Vec<usize>,
}

#[derive(Debug, Clone)]
pub struct Edge {
    pub src: usize,
    pub dst: usize,
    pub weight: i64,
    pub residual: bool,
    pub inline: bool,
}

/// The graph: every node and edge built, and `order`, the nodes kept, in
/// their order.
#[derive(Debug, Clone, Default)]
pub struct Graph {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
    pub order: Vec<usize>,
}

/// The string of index `i`, empty when out of range.
fn string(p: &Profile, i: i64) -> &str {
    usize::try_from(i)
        .ok()
        .and_then(|i| p.string_table.get(i))
        .map_or("", String::as_str)
}

/// A function's file as the report reads it: a leading
/// `/proc/self/cwd/./` or `/proc/self/cwd/` removed.
fn trim_path(f: &str) -> &str {
    f.strip_prefix("/proc/self/cwd/./")
        .or_else(|| f.strip_prefix("/proc/self/cwd/"))
        .unwrap_or(f)
}

/// The node of one location line, or of a location with no function.
fn node_info(
    p: &Profile,
    address: u64,
    objfile: &str,
    function: Option<&Function>,
    line: i64,
) -> NodeInfo {
    let Some(f) = function else {
        return NodeInfo {
            address,
            objfile: objfile.to_string(),
            ..NodeInfo::default()
        };
    };
    let name = string(p, f.name).to_string();
    let file = trim_path(string(p, f.filename));
    let mut info = NodeInfo {
        address,
        lineno: line,
        file: if file.is_empty() {
            String::new()
        } else {
            go_clean(file)
        },
        name,
        ..NodeInfo::default()
    };
    if info.name.is_empty() {
        info.objfile = objfile.to_string();
        info.start_line = f.start_line;
    }
    info
}

/// The graph of `p`'s samples, its last value each; with `kept`, only those
/// nodes, the others counted as skipped. Nodes with no weight are left out
/// of `order`, which is in creation order.
pub fn build(p: &Profile, kept: Option<&HashSet<NodeInfo>>) -> Graph {
    let functions: HashMap<u64, &Function> = p.function.iter().map(|f| (f.id, f)).collect();
    let mappings: HashMap<u64, &str> = p
        .mapping
        .iter()
        .map(|m| (m.id, string(p, m.filename)))
        .collect();
    let mut g = Graph::default();
    let mut index: HashMap<NodeInfo, usize> = HashMap::new();
    let mut locations: HashMap<u64, Vec<Option<usize>>> = HashMap::new();
    for l in &p.location {
        let objfile = mappings.get(&l.mapping_id).copied().unwrap_or("");
        let infos: Vec<NodeInfo> = if l.line.is_empty() {
            vec![node_info(p, l.address, objfile, None, 0)]
        } else {
            l.line
                .iter()
                .map(|line| {
                    let f = functions.get(&line.function_id).copied();
                    node_info(p, l.address, objfile, f, line.line)
                })
                .collect()
        };
        let nodes = infos
            .into_iter()
            .map(|info| {
                if kept.is_some_and(|k| !k.contains(&info)) {
                    return None;
                }
                Some(*index.entry(info.clone()).or_insert_with(|| {
                    g.nodes.push(Node {
                        info,
                        flat: 0,
                        cum: 0,
                        ins: Vec::new(),
                        outs: Vec::new(),
                    });
                    g.nodes.len() - 1
                }))
            })
            .collect();
        locations.insert(l.id, nodes);
    }
    let mut edge_ids: HashMap<(usize, usize), usize> = HashMap::new();
    for sample in &p.sample {
        let w = sample.value.last().copied().unwrap_or(0);
        if w == 0 {
            continue;
        }
        let mut seen: HashSet<usize> = HashSet::new();
        let mut seen_edges: HashSet<(usize, usize)> = HashSet::new();
        let mut parent: Option<usize> = None;
        let mut residual = false;
        for id in sample.location_id.iter().rev() {
            let Some(nodes) = locations.get(id) else {
                continue;
            };
            for (i, node) in nodes.iter().enumerate().rev() {
                let Some(n) = *node else {
                    residual = true;
                    continue;
                };
                if seen.insert(n) {
                    g.nodes[n].cum = g.nodes[n].cum.wrapping_add(w);
                }
                if let Some(from) = parent
                    && from != n
                    && seen_edges.insert((n, from))
                {
                    let inline = i != nodes.len() - 1;
                    match edge_ids.get(&(from, n)) {
                        Some(&e) => {
                            let edge = &mut g.edges[e];
                            edge.weight = edge.weight.wrapping_add(w);
                            edge.residual |= residual;
                            edge.inline &= inline;
                        }
                        None => {
                            let e = g.edges.len();
                            g.edges.push(Edge {
                                src: from,
                                dst: n,
                                weight: w,
                                residual,
                                inline,
                            });
                            edge_ids.insert((from, n), e);
                            g.nodes[from].outs.push(e);
                            g.nodes[n].ins.push(e);
                        }
                    }
                }
                parent = Some(n);
                residual = false;
            }
        }
        if let Some(leaf) = parent
            && !residual
        {
            g.nodes[leaf].flat = g.nodes[leaf].flat.wrapping_add(w);
        }
    }
    g.order = (0..g.nodes.len())
        .filter(|&n| g.nodes[n].cum != 0 || g.nodes[n].flat != 0)
        .collect();
    g
}

/// The entropy of a set of edges, and of `self_weight` when above 0.
fn edge_entropy(g: &Graph, edges: &[usize], self_weight: i64) -> f64 {
    let mut total = self_weight;
    for &e in edges {
        if g.edges[e].weight > 0 {
            total = total.wrapping_add(g.edges[e].weight.wrapping_abs());
        }
    }
    let mut score = 0.0;
    if total != 0 {
        for &e in edges {
            let f = g.edges[e].weight.wrapping_abs() as f64 / total as f64;
            score += -f * f.log2();
        }
        if self_weight > 0 {
            let f = self_weight.wrapping_abs() as f64 / total as f64;
            score += -f * f.log2();
        }
    }
    score
}

fn entropy_score(g: &Graph, n: usize) -> i64 {
    let node = &g.nodes[n];
    let mut s = 0.0;
    s += if node.ins.is_empty() {
        1.0
    } else {
        edge_entropy(g, &node.ins, 0)
    };
    s += if node.outs.is_empty() {
        1.0
    } else {
        edge_entropy(g, &node.outs, node.flat)
    };
    ((s * node.cum as f64) as i64).wrapping_add(node.flat)
}

/// A node's identity as the reference prints the struct, the last key of
/// the order.
fn sprint(i: &NodeInfo) -> String {
    format!(
        "{{{} {} {} {} {} {} {}}}",
        i.name, "", i.address, i.file, i.start_line, i.lineno, i.objfile
    )
}

/// Sorts `order` by entropy score, then printable name, then flat.
pub fn sort_entropy(g: &mut Graph) {
    let scores: HashMap<usize, i64> = g.order.iter().map(|&n| (n, entropy_score(g, n))).collect();
    let nodes = &g.nodes;
    g.order.sort_by(|&a, &b| {
        let (na, nb) = (&nodes[a], &nodes[b]);
        scores[&b]
            .wrapping_abs()
            .cmp(&scores[&a].wrapping_abs())
            .then_with(|| printable_name(&na.info).cmp(&printable_name(&nb.info)))
            .then_with(|| nb.flat.wrapping_abs().cmp(&na.flat.wrapping_abs()))
            .then_with(|| sprint(&na.info).cmp(&sprint(&nb.info)))
    });
}

/// The first `n` nodes of `order` when `n` is above 0, as a set; `None`
/// when that is every node.
pub fn top(g: &Graph, n: i64) -> Option<HashSet<NodeInfo>> {
    if n <= 0 {
        return None;
    }
    let k = usize::try_from(n).unwrap_or(usize::MAX).min(g.order.len());
    (k != g.order.len()).then(|| {
        g.order[..k]
            .iter()
            .map(|&i| g.nodes[i].info.clone())
            .collect()
    })
}

/// The edge order: weight, largest first, then the source's and the
/// target's printable names.
fn edge_cmp(g: &Graph, a: usize, b: usize) -> Ordering {
    let (ea, eb) = (&g.edges[a], &g.edges[b]);
    if ea.weight != eb.weight {
        return if ea.weight.wrapping_abs() > eb.weight.wrapping_abs() {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    let name = |n: usize| printable_name(&g.nodes[n].info);
    name(ea.src)
        .cmp(&name(eb.src))
        .then_with(|| name(ea.dst).cmp(&name(eb.dst)))
}

/// `edges` in the edge order.
pub fn sorted_edges(g: &Graph, mut edges: Vec<usize>) -> Vec<usize> {
    edges.sort_by(|&a, &b| edge_cmp(g, a, b));
    edges
}

/// Whether `e`'s source reaches its target without it.
fn is_redundant(g: &Graph, e: usize) -> bool {
    let (src, dst) = (g.edges[e].src, g.edges[e].dst);
    let mut seen: HashSet<usize> = HashSet::from([dst]);
    let mut queue: VecDeque<usize> = VecDeque::from([dst]);
    while let Some(n) = queue.pop_front() {
        for &ie in &g.nodes[n].ins {
            let from = g.edges[ie].src;
            if ie == e || seen.contains(&from) {
                continue;
            }
            if from == src {
                return true;
            }
            seen.insert(from);
            queue.push_back(from);
        }
    }
    false
}

/// Drops the residual edges whose source reaches their target another way,
/// the lightest first, stopping at each node's first non-residual edge.
pub fn remove_redundant_edges(g: &mut Graph) {
    for i in (0..g.order.len()).rev() {
        let n = g.order[i];
        let ins = sorted_edges(g, g.nodes[n].ins.clone());
        for &e in ins.iter().rev() {
            if !g.edges[e].residual {
                break;
            }
            if is_redundant(g, e) {
                let (src, dst) = (g.edges[e].src, g.edges[e].dst);
                g.nodes[src].outs.retain(|&x| x != e);
                g.nodes[dst].ins.retain(|&x| x != e);
            }
        }
    }
}

/// A node's name components: the address, the name, then the file and
/// line, the file, nothing, the object file or `<unknown>`.
pub fn name_components(i: &NodeInfo) -> Vec<String> {
    let mut out = Vec::new();
    if i.address != 0 {
        out.push(format!("{:016x}", i.address));
    }
    if !i.name.is_empty() {
        out.push(i.name.clone());
    }
    if i.lineno != 0 {
        out.push(format!("{}:{}", i.file, i.lineno));
    } else if !i.file.is_empty() {
        out.push(i.file.clone());
    } else if !i.name.is_empty() {
    } else if !i.objfile.is_empty() {
        out.push(format!("[{}]", go_base(&i.objfile)));
    } else {
        out.push("<unknown>".to_string());
    }
    out
}

pub fn printable_name(i: &NodeInfo) -> String {
    name_components(i).join(" ")
}

/// `\w` in the reference's patterns: ASCII only.
const W: &str = "[0-9A-Za-z_]";

static CPP_ANONYMOUS_PREFIX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\(anonymous namespace\)::").expect("a pattern"));
static GO_VER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(.*?)/v(?:[2-9]|[1-9][0-9]+)([./].*)$").expect("a pattern"));
static GO: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(?:[0-9A-Za-z_\-\.]+/)+([^.]+\..+)").expect("a pattern"));
static JAVA: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(
        r"^(?:[a-z]{W}*\.)*([A-Z](?:{W}|\$)*\.(?:<init>|[a-z](?:{W}|\$)*(?:\$[0-9]+)?))(?:\(|$)"
    ))
    .expect("a pattern")
});
static CPP: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(
        r"^(?:[_a-zA-Z]{W}*::)+(_*[A-Z]{W}*::~?[_a-zA-Z]{W}*(?:<.*>)?)"
    ))
    .expect("a pattern")
});

/// A function name shortened: a C++ anonymous namespace and a Go major
/// version removed, then the Go, Java or C++ form's last parts.
pub fn shorten(f: &str) -> String {
    let f = CPP_ANONYMOUS_PREFIX.replace(f, "");
    let f = GO_VER.replace(&f, "${1}${2}");
    for re in [&*GO, &*JAVA, &*CPP] {
        if let Some(c) = re.captures(&f)
            && c.len() >= 2
        {
            return c
                .iter()
                .skip(1)
                .map(|m| m.map_or("", |m| m.as_str()))
                .collect();
        }
    }
    f.into_owned()
}

/// Go's lexical `filepath.Clean`.
pub fn go_clean(p: &str) -> String {
    if p.is_empty() {
        return ".".to_string();
    }
    let rooted = p.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for seg in p.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                if parts.last().is_some_and(|l| *l != "..") {
                    parts.pop();
                } else if !rooted {
                    parts.push("..");
                }
            }
            s => parts.push(s),
        }
    }
    let joined = parts.join("/");
    match (rooted, joined.is_empty()) {
        (true, _) => format!("/{joined}"),
        (false, true) => ".".to_string(),
        (false, false) => joined,
    }
}

/// Go's `filepath.Base`.
pub fn go_base(p: &str) -> String {
    if p.is_empty() {
        return ".".to_string();
    }
    let p = p.trim_end_matches('/');
    if p.is_empty() {
        return "/".to_string();
    }
    p.rsplit('/').next().unwrap_or(p).to_string()
}
