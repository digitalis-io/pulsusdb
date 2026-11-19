//! Issue #828: the merged profile as a pprof `Profile` — `SelectMergeProfile`,
//! `SelectMergeStacktraces` with `PROFILE_FORMAT_PPROF`, and
//! `GET /api/profiles/v1/export` — planned as one statement (S9).
//!
//! The statement merges the selected samples per stack and reads the
//! locations those stacks name, in one pass over the samples: each merged
//! stack is expanded into itself and its location ids, and the locations
//! are the join's probe side. `go_pgo`'s slice and `aggregate_callees`'
//! re-merge run in the statement too. [`build`] cuts the stacks to
//! `max_nodes` and writes the message.

use std::collections::HashMap;

use pulsus_model::Tenant;
use pulsus_profiles_wire::google_v1::{
    Function, Line, Location, Mapping, Profile, Sample, ValueType,
};
use serde::{Deserialize, Serialize};

use crate::logql::escape::ch_string;
use crate::traces::tenant_scope::TenantTerm;

use super::engine::{ProfilesReadConfig, ProfilesReadError};
use super::tree::{self, TreeRequest, call_site_predicate};

/// What a pprof request selects: [`super::tree::TreeRequest`]'s fields,
/// with `go_pgo` as `(keep_locations, aggregate_callees)`.
#[derive(Debug, Clone, Copy, Default)]
pub struct PprofRequest<'a> {
    pub profile_type: &'a str,
    pub label_selector: &'a str,
    pub start_ms: i64,
    pub end_ms: i64,
    /// The request's `max_nodes`, `None` when absent.
    pub max_nodes: Option<i64>,
    /// `stack_trace_selector.call_site[].name`, root first.
    pub call_site: &'a [String],
    pub span_selector: &'a [String],
    pub trace_id_selector: &'a [String],
    pub profile_id_selector: &'a [String],
    pub go_pgo: Option<(u32, bool)>,
}

/// A planned request: S9's text and what the build needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Planned {
    pub sql: String,
    pub window_end_ms: i64,
    /// The five-part type id.
    pub profile_type: String,
    /// The request's `max_nodes`, 0 when absent.
    pub max_nodes: i64,
    /// Whether the statement filters by a call site: one was sent and
    /// `go_pgo` was not.
    pub call_site: bool,
    pub go_pgo: Option<(u32, bool)>,
}

/// One row of S9: a merged stack (`kind` 0) or a location it names
/// (`kind` 1). `period` is the largest period of the selected samples, on
/// every row.
#[derive(Debug, Clone, Default, PartialEq, Eq, clickhouse::Row, Serialize, Deserialize)]
pub struct PprofRow {
    pub period: i64,
    pub kind: u8,
    pub k: u128,
    pub location_ids: Vec<u128>,
    pub v: i64,
    pub mapping_file: String,
    pub mapping_build_id: String,
    pub mapping_start: u64,
    pub mapping_limit: u64,
    pub mapping_offset: u64,
    pub mapping_flags: u8,
    pub address: u64,
    pub line_functions: Vec<String>,
    pub line_system_names: Vec<String>,
    pub line_files: Vec<String>,
    pub line_start_lines: Vec<i64>,
    pub line_numbers: Vec<i64>,
}

/// The location columns, as the location read selects them.
const LOCATION_COLUMNS: &str = "SELECT location_id, mapping_file, mapping_build_id, mapping_start, mapping_limit, mapping_offset,
         mapping_flags, address, line_functions, line_system_names, line_files, line_start_lines, line_numbers";

/// The sample read's `AND` lines one level deeper than S5 nests them.
fn deeper(lines: &str) -> String {
    lines.replace("\n      AND ", "\n        AND ")
}

/// Plans `req` for `tenant`: `Ok(None)` when its window ends before the
/// lookback floor, so nothing is read. The selection is checked by
/// [`tree::side`]; `max_nodes` is not checked (D5).
pub fn plan(
    tenant: &Tenant,
    config: &ProfilesReadConfig,
    req: &PprofRequest<'_>,
    now_ms: i64,
) -> Result<Option<Planned>, ProfilesReadError> {
    let tree_req = TreeRequest {
        profile_type: req.profile_type,
        label_selector: req.label_selector,
        start_ms: req.start_ms,
        end_ms: req.end_ms,
        max_nodes: req.max_nodes,
        call_site: req.call_site,
        span_selector: req.span_selector,
        trace_id_selector: req.trace_id_selector,
        profile_id_selector: req.profile_id_selector,
    };
    let Some(side) = tree::side(tenant, config, &tree_req, now_ms)? else {
        return Ok(None);
    };
    let tenant_term = TenantTerm::of(tenant);
    let (d0, d1) = side.days();
    // `go_pgo` wins over a call site, as the reference's resolver chooses.
    let call_site = req.go_pgo.is_none() && !req.call_site.is_empty();
    let call_site_line = if call_site {
        format!(
            "\n          AND {}",
            call_site_predicate("names", req.call_site)
        )
    } else {
        String::new()
    };
    let merged = format!(
        "SELECT any(st.location_ids) AS location_ids, any(agg.v) AS v, any(agg.p) AS p
    FROM (
      SELECT stack_id, location_ids
      FROM profile_stacks
      WHERE {tenant_term} AND day BETWEEN '{d0}' AND '{d1}'{call_site_line}
    ) AS st
    INNER JOIN (
      SELECT stack_id, sum(value) AS v, max(period) AS p
      FROM {samples}
      WHERE {tenant_term} AND type_id = {type_id}{service_terms}
        AND fingerprint IN ({series})
        AND timestamp_ns >= {start_ns} AND timestamp_ns <= {end_ns}{filters}
      GROUP BY stack_id
    ) AS agg ON st.stack_id = agg.stack_id
    GROUP BY st.stack_id",
        samples = config.samples_table,
        type_id = ch_string(side.type_id()),
        service_terms = deeper(&crate::metrics::profile_service_terms(&side.matchers)),
        series = side.series,
        start_ns = side.window.start_ms.saturating_mul(1_000_000),
        end_ns = side.window.end_ms.saturating_mul(1_000_000),
        filters = deeper(&side.filters),
    );
    let stacks = match req.go_pgo {
        Some((keep, _)) if keep > 0 => format!(
            "SELECT arraySlice(location_ids, 1, {keep}) AS location_ids, sum(v) AS v, max(p) AS p
    FROM (
    {merged}
    )
    GROUP BY location_ids"
        ),
        _ => merged,
    };
    let locations = format!(
        "{LOCATION_COLUMNS}
  FROM profile_locations
  WHERE {tenant_term} AND day BETWEEN '{d0}' AND '{d1}'"
    );
    let sql = if matches!(req.go_pgo, Some((_, true))) {
        aggregate_callees(&locations, &stacks)
    } else {
        format!(
            "SELECT max(max(o.pv)) OVER () AS period,
       o.kind AS kind, o.k AS k, any(o.locs) AS location_ids, any(o.val) AS v,
       any(l.mapping_file) AS mapping_file, any(l.mapping_build_id) AS mapping_build_id,
       any(l.mapping_start) AS mapping_start, any(l.mapping_limit) AS mapping_limit,
       any(l.mapping_offset) AS mapping_offset, any(l.mapping_flags) AS mapping_flags,
       any(l.address) AS address, any(l.line_functions) AS line_functions,
       any(l.line_system_names) AS line_system_names, any(l.line_files) AS line_files,
       any(l.line_start_lines) AS line_start_lines, any(l.line_numbers) AS line_numbers
FROM (
  {locations}
) AS l
RIGHT JOIN (
  SELECT tupleElement(t, 1) AS kind, tupleElement(t, 2) AS k, tupleElement(t, 3) AS locs, tupleElement(t, 4) AS val, tupleElement(t, 5) AS pv
  FROM (
    {stacks}
  )
  ARRAY JOIN arrayConcat([(toUInt8(0), reinterpretAsUInt128(sipHash128(location_ids)), location_ids, v, p)],
                         arrayMap(x -> (toUInt8(1), x, CAST([], 'Array(UInt128)'), toInt64(0), toInt64(0)), location_ids)) AS t
) AS o ON l.location_id = o.k
GROUP BY kind, k"
        )
    };
    Ok(Some(Planned {
        sql,
        window_end_ms: side.window.end_ms,
        profile_type: side.type_id().to_string(),
        max_nodes: req.max_nodes.unwrap_or(0),
        call_site,
        go_pgo: req.go_pgo,
    }))
}

/// S9 with `go_pgo.aggregate_callees`: each stack's leaf occurrence with
/// lines takes a variant of its location whose first line is at line 0,
/// each stack's path is rebuilt from the effective ids, and the stacks
/// whose rebuilt paths are equal are summed. A location stored more than
/// once joins every occurrence once per copy; the copies are equal, so the
/// path keeps each `(position, id)` once.
fn aggregate_callees(locations: &str, stacks: &str) -> String {
    format!(
        "SELECT max(max(a.p)) OVER () AS period, if(a.sk = 0, toUInt8(1), toUInt8(0)) AS kind,
       if(kind = 1, a.eid, reinterpretAsUInt128(sipHash128(a.path))) AS k,
       any(a.path) AS location_ids, sum(a.val) AS v,
       any(a.mapping_file) AS mapping_file, any(a.mapping_build_id) AS mapping_build_id,
       any(a.mapping_start) AS mapping_start, any(a.mapping_limit) AS mapping_limit,
       any(a.mapping_offset) AS mapping_offset, any(a.mapping_flags) AS mapping_flags,
       any(a.address) AS address, any(a.line_functions) AS line_functions,
       any(a.line_system_names) AS line_system_names, any(a.line_files) AS line_files,
       any(a.line_start_lines) AS line_start_lines, any(a.line_numbers) AS line_numbers
FROM (
  SELECT j.sk AS sk, j.eid AS eid, arrayMap(x -> x.2, arraySort(groupUniqArray((j.pos, j.eid)))) AS path, any(j.val) AS val, max(j.p) AS p,
         any(j.mapping_file) AS mapping_file, any(j.mapping_build_id) AS mapping_build_id,
         any(j.mapping_start) AS mapping_start, any(j.mapping_limit) AS mapping_limit,
         any(j.mapping_offset) AS mapping_offset, any(j.mapping_flags) AS mapping_flags,
         any(j.address) AS address, any(j.line_functions) AS line_functions,
         any(j.line_system_names) AS line_system_names, any(j.line_files) AS line_files,
         any(j.line_start_lines) AS line_start_lines, any(j.line_numbers) AS line_numbers
  FROM (
    SELECT o.sk AS sk, o.pos AS pos, o.val AS val, o.p AS p, l.mapping_file AS mapping_file,
           l.mapping_build_id AS mapping_build_id, l.mapping_start AS mapping_start, l.mapping_limit AS mapping_limit,
           l.mapping_offset AS mapping_offset, l.mapping_flags AS mapping_flags, l.address AS address,
           l.line_functions AS line_functions, l.line_system_names AS line_system_names, l.line_files AS line_files,
           l.line_start_lines AS line_start_lines,
           if(o.pos = 1 AND notEmpty(l.line_numbers), arrayConcat([toInt64(0)], arraySlice(l.line_numbers, 2)), l.line_numbers) AS line_numbers,
           if(o.pos = 1 AND notEmpty(l.line_numbers),
              reinterpretAsUInt128(sipHash128(mapping_file, mapping_build_id, mapping_start, mapping_limit, mapping_offset,
                                              mapping_flags, address, line_functions, line_system_names, line_files,
                                              line_start_lines, line_numbers)),
              o.lid) AS eid
    FROM (
      {locations}
    ) AS l
    RIGHT JOIN (
      SELECT reinterpretAsUInt128(sipHash128(location_ids)) AS sk, pos, location_ids[pos] AS lid, v AS val, p
      FROM (
        {stacks}
      )
      ARRAY JOIN arrayEnumerate(location_ids) AS pos
    ) AS o ON l.location_id = o.lid
  ) AS j
  GROUP BY GROUPING SETS ((j.sk), (j.eid))
) AS a
GROUP BY kind, k"
    )
}

// ---------------------------------------------------------------------
// The build
// ---------------------------------------------------------------------

/// A function's identity: name, system name, file, start line (D7).
type FnKey<'a> = (&'a str, &'a str, &'a str, i64);

/// A location's lines as `(function, line)`, in stored order (the innermost
/// first).
fn lines_of(r: &PprofRow) -> Vec<(FnKey<'_>, i64)> {
    let n = r
        .line_functions
        .len()
        .min(r.line_system_names.len())
        .min(r.line_files.len())
        .min(r.line_start_lines.len())
        .min(r.line_numbers.len());
    (0..n)
        .map(|i| {
            (
                (
                    r.line_functions[i].as_str(),
                    r.line_system_names[i].as_str(),
                    r.line_files[i].as_str(),
                    r.line_start_lines[i],
                ),
                r.line_numbers[i],
            )
        })
        .collect()
}

/// One location of a built sample's path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Frame {
    /// A stored location, by its id.
    Stored(u128),
    /// A stored location with its first `n` lines cut (§3.2 step 6).
    Partial(u128, usize),
    /// The cut's stub, `other` (§3.2 step 8).
    Stub,
}

/// The `Profile` of S9's rows (§3): a full profile, the cut to `max_nodes`
/// when one is set or a call site is, or `go_pgo`'s profile.
pub fn build(planned: &Planned, rows: Vec<PprofRow>) -> Profile {
    if rows.is_empty() {
        return Profile {
            sample_type: vec![ValueType::default()],
            string_table: vec![String::new()],
            period_type: Some(ValueType::default()),
            ..Profile::default()
        };
    }
    let period = rows.iter().map(|r| r.period).max().unwrap_or(0);
    let mut stacks: Vec<(&[u128], i64)> = rows
        .iter()
        .filter(|r| r.kind == 0)
        .map(|r| (r.location_ids.as_slice(), r.v))
        .collect();
    stacks.sort();
    let locations: HashMap<u128, &PprofRow> = rows
        .iter()
        .filter(|r| r.kind == 1)
        .map(|r| (r.k, r))
        .collect();
    let stored = |ids: &[u128]| ids.iter().map(|&k| Frame::Stored(k)).collect::<Vec<_>>();
    let samples: Vec<(Vec<Frame>, i64)> = if planned.go_pgo.is_some() {
        stacks
            .iter()
            .filter(|(ids, _)| !ids.is_empty())
            .map(|(ids, v)| (stored(ids), *v))
            .collect()
    } else if planned.max_nodes > 0 || planned.call_site {
        cut(&stacks, &locations, planned.max_nodes)
    } else {
        stacks.iter().map(|(ids, v)| (stored(ids), *v)).collect()
    };
    let mut e = Encoder::default();
    e.string("");
    let empty = PprofRow::default();
    let row_of = |k: u128| locations.get(&k).copied().unwrap_or(&empty);
    let mut location_ids: HashMap<Frame, u64> = HashMap::new();
    for (path, _) in &samples {
        for frame in path {
            let (k, skip) = match *frame {
                Frame::Stored(k) => (k, 0),
                Frame::Partial(k, n) => (k, n),
                Frame::Stub => continue,
            };
            if !location_ids.contains_key(frame) {
                let id = e.location(row_of(k), skip);
                location_ids.insert(*frame, id);
            }
        }
    }
    if samples.iter().any(|(path, _)| path.contains(&Frame::Stub)) {
        let id = e.stub();
        location_ids.insert(Frame::Stub, id);
    }
    let sample = samples
        .iter()
        .map(|(path, v)| Sample {
            location_id: path.iter().map(|f| location_ids[f]).collect(),
            value: vec![*v],
            label: Vec::new(),
        })
        .collect();
    let parts: Vec<&str> = planned.profile_type.split(':').collect();
    let part = |i: usize| parts.get(i).copied().unwrap_or("");
    let unit = e.string(part(2));
    let sample_kind = e.string(part(1));
    let period_kind = e.string(part(3));
    let period_unit = e.string(part(4));
    Profile {
        sample_type: vec![ValueType {
            r#type: sample_kind,
            unit,
        }],
        sample,
        mapping: e.mappings,
        location: e.locations,
        function: e.functions,
        string_table: e.strings,
        time_nanos: planned.window_end_ms.saturating_mul(1_000_000),
        period_type: Some(ValueType {
            r#type: period_kind,
            unit: period_unit,
        }),
        period,
        default_sample_type: sample_kind,
        ..Profile::default()
    }
}

/// §3.2: the merged stacks cut to exactly `max_nodes` function nodes.
fn cut(
    stacks: &[(&[u128], i64)],
    locations: &HashMap<u128, &PprofRow>,
    max_nodes: i64,
) -> Vec<(Vec<Frame>, i64)> {
    // Steps 1 and 2: each stack's functions, leaf first, inserted root
    // first into one tree.
    let lines: HashMap<u128, Vec<(FnKey<'_>, i64)>> =
        locations.iter().map(|(&k, r)| (k, lines_of(r))).collect();
    let line_count = |k: u128| lines.get(&k).map_or(0, Vec::len);
    let mut index: HashMap<(usize, FnKey<'_>), usize> = HashMap::new();
    let mut totals: Vec<i64> = vec![0];
    let mut paths: Vec<Vec<usize>> = Vec::with_capacity(stacks.len());
    for (ids, v) in stacks {
        let functions: Vec<FnKey<'_>> = ids
            .iter()
            .flat_map(|k| lines.get(k).into_iter().flatten().map(|(f, _)| *f))
            .collect();
        let mut parent = 0usize;
        let mut nodes = Vec::with_capacity(functions.len());
        for f in functions.iter().rev() {
            let next = totals.len();
            let node = *index.entry((parent, *f)).or_insert(next);
            if node == next {
                totals.push(0);
            }
            totals[node] = totals[node].saturating_add(*v);
            nodes.push(node);
            parent = node;
        }
        nodes.reverse();
        paths.push(nodes);
    }
    // Step 3: the `max_nodes`-th largest total, the root's 0 counted.
    let n = totals.len() - 1;
    let min = if max_nodes < 1 || max_nodes as u128 > n as u128 {
        0
    } else {
        let mut all: Vec<i64> = totals[1..].to_vec();
        all.push(0);
        all.sort_unstable_by(|a, b| b.cmp(a));
        all[max_nodes as usize - 1]
    };
    // Step 4.
    let marked = |node: usize| min > 0 && totals[node] < min;

    let mut out: Vec<(Vec<Frame>, i64)> = Vec::new();
    let mut at: HashMap<Vec<Frame>, usize> = HashMap::new();
    let mut full: i64 = 0;
    for ((ids, v), nodes) in stacks.iter().zip(&paths) {
        // Step 5.
        let off = nodes.iter().take_while(|&&node| marked(node)).count();
        if !nodes.is_empty() && off == nodes.len() {
            full = full.saturating_add(*v);
            continue;
        }
        // Step 6.
        let path: Vec<Frame> = if off == 0 {
            ids.iter().map(|&k| Frame::Stored(k)).collect()
        } else {
            let (mut f, mut l) = (0usize, 0usize);
            while l < ids.len() && f + line_count(ids[l]) <= off {
                f += line_count(ids[l]);
                l += 1;
            }
            let r = off - f;
            let mut path = vec![Frame::Stub];
            if r == 0 {
                path.extend(ids[l..].iter().map(|&k| Frame::Stored(k)));
            } else {
                path.push(Frame::Partial(ids[l], r));
                path.extend(ids[l + 1..].iter().map(|&k| Frame::Stored(k)));
            }
            path
        };
        // Step 7.
        match at.get(&path) {
            Some(&i) => out[i].1 = out[i].1.saturating_add(*v),
            None => {
                at.insert(path.clone(), out.len());
                out.push((path, *v));
            }
        }
    }
    if full > 0 {
        out.push((vec![Frame::Stub], full));
    }
    out
}

/// A mapping's content: file, build id, start, limit, offset, flags.
type MappingKey = (String, String, u64, u64, u64, u8);
/// A location's content: its mapping, address and lines.
type LocationKey = (u64, u64, Vec<(u64, i64)>);

/// The message's tables, each interned by content in first-use order
/// (§3.5).
#[derive(Default)]
struct Encoder {
    strings: Vec<String>,
    string_ids: HashMap<String, i64>,
    mappings: Vec<Mapping>,
    mapping_ids: HashMap<MappingKey, u64>,
    functions: Vec<Function>,
    function_ids: HashMap<(String, String, String, i64), u64>,
    locations: Vec<Location>,
    location_ids: HashMap<LocationKey, u64>,
}

impl Encoder {
    fn string(&mut self, s: &str) -> i64 {
        if let Some(&i) = self.string_ids.get(s) {
            return i;
        }
        let i = self.strings.len() as i64;
        self.strings.push(s.to_string());
        self.string_ids.insert(s.to_string(), i);
        i
    }

    /// A location row's mapping id; 0 when its six mapping fields are all
    /// empty.
    fn mapping(&mut self, r: &PprofRow) -> u64 {
        let key: MappingKey = (
            r.mapping_file.clone(),
            r.mapping_build_id.clone(),
            r.mapping_start,
            r.mapping_limit,
            r.mapping_offset,
            r.mapping_flags,
        );
        if key == MappingKey::default() {
            return 0;
        }
        if let Some(&id) = self.mapping_ids.get(&key) {
            return id;
        }
        let filename = self.string(&r.mapping_file);
        let build_id = self.string(&r.mapping_build_id);
        let id = self.mappings.len() as u64 + 1;
        let flag = |bit: u8| r.mapping_flags & (1 << bit) != 0;
        self.mappings.push(Mapping {
            id,
            memory_start: r.mapping_start,
            memory_limit: r.mapping_limit,
            file_offset: r.mapping_offset,
            filename,
            build_id,
            has_functions: flag(0),
            has_filenames: flag(1),
            has_line_numbers: flag(2),
            has_inline_frames: flag(3),
        });
        self.mapping_ids.insert(key, id);
        id
    }

    fn function(&mut self, (name, system_name, file, start_line): FnKey<'_>) -> u64 {
        let key = (
            name.to_string(),
            system_name.to_string(),
            file.to_string(),
            start_line,
        );
        if let Some(&id) = self.function_ids.get(&key) {
            return id;
        }
        let name = self.string(name);
        let filename = self.string(file);
        let system_name = self.string(system_name);
        let id = self.functions.len() as u64 + 1;
        self.functions.push(Function {
            id,
            name,
            system_name,
            filename,
            start_line,
        });
        self.function_ids.insert(key, id);
        id
    }

    /// The id of location row `r` without its first `skip` lines.
    fn location(&mut self, r: &PprofRow, skip: usize) -> u64 {
        let mapping_id = self.mapping(r);
        let lines: Vec<(u64, i64)> = lines_of(r)
            .into_iter()
            .skip(skip)
            .map(|(f, line)| (self.function(f), line))
            .collect();
        self.intern_location(mapping_id, r.address, lines)
    }

    fn intern_location(&mut self, mapping_id: u64, address: u64, lines: Vec<(u64, i64)>) -> u64 {
        let key: LocationKey = (mapping_id, address, lines);
        if let Some(&id) = self.location_ids.get(&key) {
            return id;
        }
        let id = self.locations.len() as u64 + 1;
        self.locations.push(Location {
            id,
            mapping_id,
            address,
            line: key
                .2
                .iter()
                .map(|&(function_id, line)| Line { function_id, line })
                .collect(),
            is_folded: false,
        });
        self.location_ids.insert(key, id);
        id
    }

    /// The cut's stub: a function `other`, and a location of mapping 1
    /// with one line of it at line 0; mapping 1 is created when there is
    /// none.
    fn stub(&mut self) -> u64 {
        let other = self.function(("other", "other", "", 0));
        if self.mappings.is_empty() {
            self.mappings.push(Mapping {
                id: 1,
                ..Mapping::default()
            });
        }
        let id = self.locations.len() as u64 + 1;
        self.locations.push(Location {
            id,
            mapping_id: 1,
            address: 0,
            line: vec![Line {
                function_id: other,
                line: 0,
            }],
            is_folded: false,
        });
        id
    }
}

#[cfg(test)]
#[path = "../../tests/profileql_pprof_nf/mod.rs"]
mod nf;

#[cfg(test)]
mod tests {
    use super::nf::{self, Nf, P1, P2, P4, P6, PERIOD, UNIT};
    use super::*;
    use crate::profiles::selector::WindowLimits;
    use crate::profiles::tenant_scope;
    use crate::traces::tenant_scope::TenantTerm;
    use prost::Message;

    const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
    /// 2026-11-16T09:00Z and 10:00Z.
    const START: i64 = 1_794_819_600_000;
    const END: i64 = 1_794_823_200_000;

    /// Plan §0's statement.
    const EXAMPLE: &str = "SELECT max(max(o.pv)) OVER () AS period,
       o.kind AS kind, o.k AS k, any(o.locs) AS location_ids, any(o.val) AS v,
       any(l.mapping_file) AS mapping_file, any(l.mapping_build_id) AS mapping_build_id,
       any(l.mapping_start) AS mapping_start, any(l.mapping_limit) AS mapping_limit,
       any(l.mapping_offset) AS mapping_offset, any(l.mapping_flags) AS mapping_flags,
       any(l.address) AS address, any(l.line_functions) AS line_functions,
       any(l.line_system_names) AS line_system_names, any(l.line_files) AS line_files,
       any(l.line_start_lines) AS line_start_lines, any(l.line_numbers) AS line_numbers
FROM (
  SELECT location_id, mapping_file, mapping_build_id, mapping_start, mapping_limit, mapping_offset,
         mapping_flags, address, line_functions, line_system_names, line_files, line_start_lines, line_numbers
  FROM profile_locations
  WHERE org_id = 't1' AND day BETWEEN '2026-11-16' AND '2026-11-16'
) AS l
RIGHT JOIN (
  SELECT tupleElement(t, 1) AS kind, tupleElement(t, 2) AS k, tupleElement(t, 3) AS locs, tupleElement(t, 4) AS val, tupleElement(t, 5) AS pv
  FROM (
    SELECT any(st.location_ids) AS location_ids, any(agg.v) AS v, any(agg.p) AS p
    FROM (
      SELECT stack_id, location_ids
      FROM profile_stacks
      WHERE org_id = 't1' AND day BETWEEN '2026-11-16' AND '2026-11-16'
    ) AS st
    INNER JOIN (
      SELECT stack_id, sum(value) AS v, max(period) AS p
      FROM profile_samples
      WHERE org_id = 't1' AND type_id = 'process_cpu:cpu:nanoseconds:cpu:nanoseconds'
        AND service = 's0'
        AND fingerprint IN (SELECT fingerprint
FROM profile_series
WHERE org_id = 't1' AND day BETWEEN '2026-11-16' AND '2026-11-16'
  AND bitAnd(hours, multiIf(day = '2026-11-16' AND day = '2026-11-16', 1536, day = '2026-11-16', 16776704, day = '2026-11-16', 2047, 16777215)) != 0
  AND service = 's0'
  AND type_id = 'process_cpu:cpu:nanoseconds:cpu:nanoseconds')
        AND timestamp_ns >= 1794819600000000000 AND timestamp_ns <= 1794823200000000000
      GROUP BY stack_id
    ) AS agg ON st.stack_id = agg.stack_id
    GROUP BY st.stack_id
  )
  ARRAY JOIN arrayConcat([(toUInt8(0), reinterpretAsUInt128(sipHash128(location_ids)), location_ids, v, p)],
                         arrayMap(x -> (toUInt8(1), x, CAST([], 'Array(UInt128)'), toInt64(0), toInt64(0)), location_ids)) AS t
) AS o ON l.location_id = o.k
GROUP BY kind, k";

    fn tenant(name: &str) -> Tenant {
        let value = http::HeaderValue::from_str(name).expect("a header value");
        Tenant::from_header(Some(&value), false).expect("a valid tenant")
    }

    fn single() -> ProfilesReadConfig {
        ProfilesReadConfig {
            series_table: "profile_series".to_string(),
            samples_table: "profile_samples".to_string(),
            entries_table: "profile_entries".to_string(),
            dist_suffix: String::new(),
            limits: WindowLimits::default(),
            scan_budget_rows: 50_000_000,
            max_tree_nodes: 4_194_304,
            max_result_bytes: 268_435_456,
            query_memory_bytes: 4_294_967_296,
            distributed: false,
            skip_unavailable_shards: false,
            max_nodes_default: 8_192,
            max_nodes_max: 1_048_576,
        }
    }

    fn clustered() -> ProfilesReadConfig {
        ProfilesReadConfig {
            series_table: "profile_series_dist".to_string(),
            samples_table: "profile_samples_dist".to_string(),
            entries_table: "profile_entries_dist".to_string(),
            dist_suffix: "_dist".to_string(),
            distributed: true,
            ..single()
        }
    }

    /// Plan §0's request.
    fn example() -> PprofRequest<'static> {
        PprofRequest {
            profile_type: CPU,
            label_selector: r#"{service_name="s0"}"#,
            start_ms: START,
            end_ms: END,
            max_nodes: Some(16_384),
            ..PprofRequest::default()
        }
    }

    fn planned_as(org: &str, config: &ProfilesReadConfig, req: &PprofRequest<'_>) -> Planned {
        match plan(&tenant(org), config, req, END) {
            Ok(Some(p)) => p,
            other => panic!("expected a plan, got {other:?}"),
        }
    }

    fn planned(config: &ProfilesReadConfig, req: &PprofRequest<'_>) -> Planned {
        planned_as("t1", config, req)
    }

    fn strings(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    // -----------------------------------------------------------------
    // The build, on fixture P's rows
    // -----------------------------------------------------------------

    /// A location row of kind 1.
    fn location_row(
        k: u128,
        mapping: (&str, &str, u64, u64, u8),
        address: u64,
        lines: &[(&str, i64, i64)],
    ) -> PprofRow {
        PprofRow {
            period: PERIOD,
            kind: 1,
            k,
            mapping_file: mapping.0.to_string(),
            mapping_build_id: mapping.1.to_string(),
            mapping_start: mapping.2,
            mapping_limit: mapping.3,
            mapping_flags: mapping.4,
            address,
            line_functions: lines.iter().map(|l| l.0.to_string()).collect(),
            line_system_names: lines.iter().map(|l| l.0.to_string()).collect(),
            line_files: lines.iter().map(|_| "main.go".to_string()).collect(),
            line_start_lines: lines.iter().map(|l| l.1).collect(),
            line_numbers: lines.iter().map(|l| l.2).collect(),
            ..PprofRow::default()
        }
    }

    fn stack_row(location_ids: &[u128], v: i64) -> PprofRow {
        PprofRow {
            period: PERIOD,
            kind: 0,
            k: location_ids
                .iter()
                .fold(7u128, |h, x| h.wrapping_mul(31) ^ x),
            location_ids: location_ids.to_vec(),
            v,
            ..PprofRow::default()
        }
    }

    /// P's nine location rows, `k` = 1–9.
    fn p_locations() -> Vec<PprofRow> {
        let mut out: Vec<PprofRow> = nf::LINES
            .iter()
            .enumerate()
            .map(|(i, (name, line))| {
                let start = nf::function(name).3;
                location_row(
                    i as u128 + 1,
                    ("app", "bid", 0, 0, 15),
                    0,
                    &[(name, start, *line)],
                )
            })
            .collect();
        out.push(location_row(
            9,
            ("libx.so", "", nf::LIBX_START, nf::LIBX_LIMIT, 0),
            nf::L9_ADDRESS,
            &[],
        ));
        out
    }

    /// P's rows as S9 returns them for a request with no go_pgo: the
    /// stacks `samples` (P's indices, counts) and the locations they name.
    fn p_rows(samples: &[(&[u128], i64)]) -> Vec<PprofRow> {
        let mut rows: Vec<PprofRow> = samples
            .iter()
            .map(|(ids, count)| stack_row(ids, count * UNIT))
            .collect();
        let named: std::collections::BTreeSet<u128> = samples
            .iter()
            .flat_map(|(ids, _)| ids.iter().copied())
            .collect();
        rows.extend(p_locations().into_iter().filter(|l| named.contains(&l.k)));
        rows
    }

    fn p_all() -> Vec<PprofRow> {
        p_rows(&[
            (&[2, 1], 2),
            (&[3, 1], 3),
            (&[4, 2, 1], 10),
            (&[5, 3, 1], 1),
            (&[7, 6, 1], 4),
            (&[8], 1),
            (&[9], 6),
        ])
    }

    fn planned_for(max_nodes: i64, call_site: bool, go_pgo: Option<(u32, bool)>) -> Planned {
        Planned {
            sql: String::new(),
            window_end_ms: END,
            profile_type: CPU.to_string(),
            max_nodes,
            call_site,
            go_pgo,
        }
    }

    fn built(
        max_nodes: i64,
        call_site: bool,
        go_pgo: Option<(u32, bool)>,
        rows: Vec<PprofRow>,
    ) -> Nf {
        let p = build(&planned_for(max_nodes, call_site, go_pgo), rows);
        let bytes = p.encode_to_vec();
        nf::nf(&Profile::decode(&bytes[..]).expect("the built profile decodes"))
    }

    fn want(samples: &[(&str, i64)]) -> Nf {
        nf::expected(samples, UNIT, PERIOD, END * 1_000_000)
    }

    /// P1: no `max_nodes` → every stack as stored.
    #[test]
    fn p1_full() {
        assert_eq!(built(0, false, None, p_all()), want(&P1));
    }

    /// P2: `max_nodes` 3 → `min` 10: `c`, `d`, `e`, `zz` cut exactly, the
    /// address-only `L9` kept.
    #[test]
    fn p2_cut_three() {
        assert_eq!(built(3, false, None, p_all()), want(&P2));
    }

    /// P3: `max_nodes` 16384 → `min` 0: P1, `L9` kept.
    #[test]
    fn p3_cut_16384() {
        assert_eq!(built(16_384, false, None, p_all()), want(&P1));
    }

    /// P4: the call site `[main, a]`'s rows with `max_nodes` 500.
    #[test]
    fn p4_call_site() {
        let rows = p_rows(&[
            (&[2, 1], 2),
            (&[3, 1], 3),
            (&[4, 2, 1], 10),
            (&[5, 3, 1], 1),
        ]);
        assert_eq!(built(500, true, None, rows), want(&P4));
    }

    /// P6: `go_pgo{keep 1}`: the statement's sliced rows, one sample each.
    #[test]
    fn p6_pgo_keep_one() {
        let rows = p_rows(&[
            (&[2], 2),
            (&[3], 3),
            (&[4], 10),
            (&[5], 1),
            (&[7], 4),
            (&[8], 1),
            (&[9], 6),
        ]);
        assert_eq!(built(0, false, Some((1, false)), rows), want(&P6));
    }

    /// P11: a location's inlined lines are frames of the cut: a location
    /// whose lines are all marked goes whole, one partly marked keeps only
    /// its unmarked lines.
    #[test]
    fn p11_inlined_lines_count_as_frames() {
        let main = location_row(1, ("app", "bid", 0, 0, 15), 0, &[("main", 1, 10)]);
        let x = location_row(20, ("app", "bid", 0, 0, 15), 0, &[("g", 8, 1), ("f", 9, 2)]);
        let h = location_row(21, ("app", "bid", 0, 0, 15), 0, &[("h", 10, 3)]);
        let rows = vec![
            stack_row(&[20, 1], UNIT),
            stack_row(&[21, 1], 10 * UNIT),
            main.clone(),
            x,
            h,
        ];
        let line = |name: &str, start: i64, n: i64| {
            (
                (
                    name.to_string(),
                    name.to_string(),
                    "main.go".to_string(),
                    start,
                ),
                n,
            )
        };
        let loc = |lines: Vec<((String, String, String, i64), i64)>| nf::NfLocation {
            mapping: Some(nf::mapping1()),
            address: 0,
            is_folded: false,
            lines,
        };
        let l1 = loc(vec![line("main", 1, 10)]);
        let mut expect = nf::header(PERIOD, END * 1_000_000);
        expect
            .samples
            .insert((vec![nf::stub(), l1.clone()], vec![UNIT]), 1);
        expect.samples.insert(
            (
                vec![loc(vec![line("h", 10, 3)]), l1.clone()],
                vec![10 * UNIT],
            ),
            1,
        );
        assert_eq!(built(2, false, None, rows), expect, "Lx taken whole");

        let y = location_row(22, ("app", "bid", 0, 0, 15), 0, &[("g", 8, 1), ("f", 9, 2)]);
        let z = location_row(
            23,
            ("app", "bid", 0, 0, 15),
            0,
            &[("h", 10, 3), ("f", 9, 2)],
        );
        let rows = vec![
            stack_row(&[22, 1], UNIT),
            stack_row(&[23, 1], 10 * UNIT),
            main,
            y,
            z,
        ];
        let mut expect = nf::header(PERIOD, END * 1_000_000);
        expect.samples.insert(
            (
                vec![nf::stub(), loc(vec![line("f", 9, 2)]), l1.clone()],
                vec![UNIT],
            ),
            1,
        );
        expect.samples.insert(
            (
                vec![loc(vec![line("h", 10, 3), line("f", 9, 2)]), l1],
                vec![10 * UNIT],
            ),
            1,
        );
        assert_eq!(built(3, false, None, rows), expect, "Ly split");
    }

    /// P8: no rows → the reference's six bytes.
    #[test]
    fn p8_no_rows() {
        let bytes = build(&planned_for(3, false, None), Vec::new()).encode_to_vec();
        assert_eq!(bytes, [0x0a, 0x00, 0x32, 0x00, 0x5a, 0x00]);
    }

    /// P9: the pushed period, absent when 0; `time_nanos` the window's end.
    #[test]
    fn p9_period() {
        let p = build(&planned_for(0, false, None), p_all());
        assert_eq!((p.period, p.time_nanos), (PERIOD, END * 1_000_000));
        let rows: Vec<PprofRow> = p_all()
            .into_iter()
            .map(|r| PprofRow { period: 0, ..r })
            .collect();
        let p = build(&planned_for(0, false, None), rows);
        assert_eq!((p.period, p.time_nanos), (0, END * 1_000_000));
        assert!(!p.sample.is_empty(), "{p:?}");
    }

    /// P10: one request's bytes do not depend on the rows' order.
    #[test]
    fn p10_same_bytes() {
        let a = build(&planned_for(3, false, None), p_all()).encode_to_vec();
        let mut rows = p_all();
        rows.reverse();
        let b = build(&planned_for(3, false, None), rows).encode_to_vec();
        assert!(!a.is_empty());
        assert_eq!(a, b);
    }

    // -----------------------------------------------------------------
    // The statement
    // -----------------------------------------------------------------

    /// S1: plan §0's request renders plan §0's statement, byte for byte.
    #[test]
    fn s1_the_example_renders_the_example_statement() {
        let p = planned(&single(), &example());
        assert_eq!(p.sql, EXAMPLE);
        assert_eq!(
            (p.window_end_ms, p.profile_type.as_str(), p.max_nodes),
            (END, CPU, 16_384)
        );
    }

    /// S2: clustered, the samples and the series through their wrappers; the
    /// stacks and the locations, on every node, read locally.
    #[test]
    fn s2_clustered() {
        let sql = planned(&clustered(), &example()).sql;
        for expected in [
            "FROM profile_samples_dist\n",
            "\nFROM profile_series_dist\n",
            "FROM profile_stacks\n",
            "FROM profile_locations\n",
        ] {
            assert!(sql.contains(expected), "{expected:?} missing from\n{sql}");
        }
        assert!(!sql.contains("profile_stacks_dist"), "{sql}");
        assert!(!sql.contains("profile_locations_dist"), "{sql}");
    }

    /// S3: `go_pgo` takes the slice and wins over a call site; a call site
    /// alone filters the stacks by name; `aggregate_callees` takes the
    /// second template.
    #[test]
    fn s3_pgo_and_call_site() {
        let call_site = strings(&["main", "a"]);
        let both = planned(
            &single(),
            &PprofRequest {
                call_site: &call_site,
                go_pgo: Some((5, false)),
                ..example()
            },
        );
        assert!(
            both.sql.contains(
                "SELECT arraySlice(location_ids, 1, 5) AS location_ids, sum(v) AS v, max(p) AS p"
            ),
            "{}",
            both.sql
        );
        assert!(!both.sql.contains("arraySlice(names"), "{}", both.sql);
        assert!(!both.sql.contains("GROUPING SETS"), "{}", both.sql);
        assert!(!both.call_site, "go_pgo wins over the call site");

        let site = planned(
            &single(),
            &PprofRequest {
                call_site: &call_site,
                ..example()
            },
        );
        assert!(
            site.sql
                .contains("AND arraySlice(names, 1, 2) = ['main', 'a']"),
            "{}",
            site.sql
        );
        assert!(site.call_site);

        let aggregate = planned(
            &single(),
            &PprofRequest {
                go_pgo: Some((5, true)),
                ..example()
            },
        );
        for expected in [
            "GROUP BY GROUPING SETS ((j.sk), (j.eid))",
            "arrayMap(x -> x.2, arraySort(groupUniqArray((j.pos, j.eid)))) AS path",
            "SELECT arraySlice(location_ids, 1, 5) AS location_ids, sum(v) AS v, max(p) AS p",
            "ARRAY JOIN arrayEnumerate(location_ids) AS pos",
            "SELECT max(max(a.p)) OVER () AS period",
        ] {
            assert!(
                aggregate.sql.contains(expected),
                "{expected:?} missing from\n{}",
                aggregate.sql
            );
        }
        assert_eq!(aggregate.go_pgo, Some((5, true)));
    }

    /// S4: the tenant check accepts every rendering, under each tenant.
    #[test]
    fn s4_tenant_check() {
        for org in ["t1", "t2"] {
            let term = TenantTerm::of(&tenant(org));
            for go_pgo in [None, Some((5, false)), Some((0, true)), Some((5, true))] {
                let req = PprofRequest {
                    go_pgo,
                    ..example()
                };
                let one = planned_as(org, &single(), &req).sql;
                assert_eq!(
                    tenant_scope::check(&one, &term, &tenant_scope::tables("")),
                    Ok(()),
                    "{org} {go_pgo:?}"
                );
                let two = planned_as(org, &clustered(), &req).sql;
                assert_eq!(
                    tenant_scope::check(&two, &term, &tenant_scope::tables("_dist")),
                    Ok(()),
                    "{org} {go_pgo:?} clustered"
                );
            }
        }
    }

    /// S5: `max_nodes` is not validated: negative, 0 and above the flame
    /// graph's largest all plan.
    #[test]
    fn s5_max_nodes_is_not_validated() {
        for n in [-1, 0, 2_000_000] {
            let p = planned(
                &single(),
                &PprofRequest {
                    max_nodes: Some(n),
                    ..example()
                },
            );
            assert_eq!(p.max_nodes, n);
        }
        let p = planned(
            &single(),
            &PprofRequest {
                max_nodes: None,
                ..example()
            },
        );
        assert_eq!(p.max_nodes, 0);
    }

    /// S6: the period is read from the samples the statement sums, and
    /// nothing reads the entries.
    #[test]
    fn s6_period_rides_with_the_samples() {
        let sql = planned(&single(), &example()).sql;
        for expected in [
            "SELECT stack_id, sum(value) AS v, max(period) AS p",
            "SELECT max(max(o.pv)) OVER () AS period",
        ] {
            assert!(sql.contains(expected), "{expected:?} missing from\n{sql}");
        }
        assert!(!sql.contains("profile_entries"), "{sql}");
    }

    /// A window ending before the lookback floor plans nothing.
    #[test]
    fn a_window_before_the_floor_plans_nothing() {
        let now = END + 8 * 86_400_000;
        assert_eq!(plan(&tenant("t1"), &single(), &example(), now), Ok(None));
    }
}
