//! Issue #831: ad hoc profile uploads. An upload is converted in process
//! (`convert`), stored as one landing block of kinds 5 and 6, which
//! `profile_adhoc_mv` copies into `profile_adhoc` (plan §2, §4.3), and read
//! back by S14 (`Get`, `Diff`) and S15 (`List`), one statement each (§3).
//! Its flame graph is drawn as the reference draws the upload's format:
//! a pprof by the push path's renderer, every other format by `og` (§4.4).

pub mod convert;
pub mod og;

use std::collections::{BTreeMap, HashMap, HashSet};

use pulsus_clickhouse::Row;
use pulsus_model::Tenant;
use pulsus_profiles_wire::flamebearer::{Flamebearer, Meta, metadata, stacks_of_levels};
use serde::{Deserialize, Serialize};

use super::engine::{ProfilesReadConfig, ProfilesReadError};
use super::flamegraph::Tree;
use super::tree::TreeRow;
use crate::logql::escape::ch_string;
use crate::traces::tenant_scope::TenantTerm;
use convert::Converted;

/// The landing kind of an upload's type and sample rows.
pub const KIND_ROWS: u8 = 5;
/// The landing kind of an upload's stack rows.
pub const KIND_STACKS: u8 = 6;

/// The largest `profile` text an upload may carry.
pub const MAX_PROFILE_BYTES: usize = 4_194_304;

/// One landing row of an upload: every column of the landing table, in
/// its order, as the client checks an insert's columns against it. An
/// upload sets `org_id`, `received_ms`, `kind`, `stack_id`, `value`,
/// `node_ids`, `names` and the ten upload columns; every other column is
/// its type's default.
#[derive(Debug, Clone, PartialEq, Eq, Default, Row, Serialize, Deserialize)]
pub struct AdhocLandingRow {
    pub org_id: String,
    pub received_ms: i64,
    pub kind: u8,
    pub type_id: String,
    pub service: String,
    pub fingerprint: u128,
    pub timestamp_ns: i64,
    pub stack_id: u128,
    pub value: i64,
    pub span_id: u64,
    pub trace_id: [u8; 16],
    pub node_ids: Vec<u128>,
    pub names: Vec<String>,
    pub location_ids: Vec<u128>,
    pub location_id: u128,
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
    pub labels: String,
    /// The `UUID` columns as two big-endian `UInt64` halves.
    pub profile_id: (u64, u64),
    pub upload_id: (u64, u64),
    pub upload_name: String,
    pub uploaded_at: i64,
    pub type_ordinal: u16,
    pub is_type: u8,
    pub upload_format: String,
    pub display_name: String,
    pub spy_name: String,
    pub sample_rate: u32,
    pub units: String,
    pub duration_ns: i64,
    pub period: i64,
    pub annotation_keys: Vec<String>,
    pub annotation_values: Vec<String>,
}

/// One row of S14: a tree node of the selected type (`part` 0) or a type
/// row (`part` 1), tagged by the upload's id.
#[derive(Debug, Clone, PartialEq, Eq, Default, Row, Serialize, Deserialize)]
pub struct AdhocRow {
    pub part: u8,
    pub uid: String,
    pub node: u128,
    pub parent: u128,
    pub name: String,
    pub total: i64,
    pub self_value: i64,
    pub type_ordinal: u16,
    /// `Nullable`: the selection is a scalar subquery.
    pub selected: Option<u8>,
    pub upload_name: String,
    pub uploaded_at: i64,
    pub format: String,
    pub display_name: String,
    pub spy_name: String,
    pub sample_rate: u32,
    pub units: String,
}

/// One row of S15.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct UploadRow {
    pub id: String,
    pub name: String,
    pub uploaded_at: i64,
}

/// One upload's part of S14's answer.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Selected<'a> {
    /// Its type rows, by ordinal.
    pub types: Vec<&'a AdhocRow>,
    /// The type S14 selected.
    pub selected: Option<&'a AdhocRow>,
    /// The selected type's tree.
    pub tree: Vec<TreeRow>,
}

/// A new upload's id, a UUIDv7 of `now_ms`, in canonical text.
pub fn mint_id(now_ms: i64) -> String {
    let ms = u64::try_from(now_ms).unwrap_or(0);
    let ts =
        uuid::Timestamp::from_unix(uuid::NoContext, ms / 1000, ((ms % 1000) * 1_000_000) as u32);
    uuid::Uuid::new_v7(ts).hyphenated().to_string()
}

fn valid_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_')
}

/// An upload's name, every character outside `[A-Za-z0-9._-]` as `_`.
pub fn sanitize_name(name: &str) -> String {
    name.chars()
        .map(|c| if valid_char(c) { c } else { '_' })
        .collect()
}

/// Whether `id` is a valid id: not empty, every character in
/// `[A-Za-z0-9._-]`.
pub fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.chars().all(valid_char)
}

/// Whether `id` is a UUID in canonical text, the only form an upload's id
/// takes (D8).
pub fn is_upload_id(id: &str) -> bool {
    id.len() == 36 && uuid::Uuid::parse_str(id).is_ok()
}

/// The uploaded file of `profile`, as Go's `base64.StdEncoding` decodes
/// it: `\r` and `\n` skipped, a refusal naming its offset.
pub fn decode_base64(profile: &str) -> Result<Vec<u8>, String> {
    crate::logql::template::funcs::b64_decode_go(profile.as_bytes())
}

/// `raw` resolved as the reference resolves `max_nodes`; a refusal is its
/// text.
pub fn max_nodes(raw: Option<i64>, config: &ProfilesReadConfig) -> Result<i64, String> {
    super::tree::resolve_max_nodes(raw.unwrap_or(0), config).map_err(|e| match e {
        ProfilesReadError::InvalidArgument(m) => m,
        other => format!("{other:?}"),
    })
}

/// A node's id: the push path's `id128(parent_le ‖ name)`
/// (`pulsus_model::compose128`'s composition).
fn node_id(parent: u128, name: &str) -> u128 {
    let mut buf = Vec::with_capacity(16 + name.len());
    buf.extend_from_slice(&parent.to_le_bytes());
    buf.extend_from_slice(name.as_bytes());
    ((pulsus_model::raw_cityhash64(&buf) as u128) << 64)
        | (xxhash_rust::xxh64::xxh64(&buf, 0) as u128)
}

/// A stack's node ids, root first.
fn node_ids(frames: &[String]) -> Vec<u128> {
    let mut parent = 0u128;
    frames
        .iter()
        .map(|f| {
            parent = node_id(parent, f);
            parent
        })
        .collect()
}

/// An id's `UUID` column halves.
fn uuid_halves(id: &str) -> (u64, u64) {
    let v = uuid::Uuid::parse_str(id).map(|u| u.as_u128()).unwrap_or(0);
    ((v >> 64) as u64, v as u64)
}

/// The landing rows of `c`, uploaded by `tenant` as `name` (§4.3): one type
/// row per type, one sample row per type and distinct stack, zero values
/// kept, and one stack row per distinct stack over every type.
pub fn rows(
    tenant: &Tenant,
    id: &str,
    name: &str,
    uploaded_at: i64,
    c: &Converted,
) -> Vec<AdhocLandingRow> {
    let base = AdhocLandingRow {
        org_id: tenant.as_str().to_string(),
        received_ms: uploaded_at,
        kind: KIND_ROWS,
        upload_id: uuid_halves(id),
        uploaded_at,
        ..AdhocLandingRow::default()
    };
    let mut out = Vec::new();
    let mut stacks: Vec<(u128, Vec<u128>, &[String])> = Vec::new();
    let mut seen: HashSet<u128> = HashSet::new();
    for (k, t) in c.types.iter().enumerate() {
        let ordinal = k as u16;
        out.push(AdhocLandingRow {
            is_type: 1,
            type_ordinal: ordinal,
            upload_name: name.to_string(),
            upload_format: c.format.as_str().to_string(),
            display_name: t.display_name.clone(),
            spy_name: t.spy_name.clone(),
            sample_rate: t.sample_rate,
            units: t.units.clone(),
            ..base.clone()
        });
        let mut sums: Vec<(u128, i64)> = Vec::new();
        let mut at: HashMap<u128, usize> = HashMap::new();
        for (frames, v) in &t.stacks {
            let ids = node_ids(frames);
            let Some(&leaf) = ids.last() else { continue };
            match at.get(&leaf) {
                Some(&i) => sums[i].1 += v,
                None => {
                    at.insert(leaf, sums.len());
                    sums.push((leaf, *v));
                }
            }
            if seen.insert(leaf) {
                stacks.push((leaf, ids, frames));
            }
        }
        out.extend(sums.into_iter().map(|(stack_id, value)| AdhocLandingRow {
            type_ordinal: ordinal,
            stack_id,
            value,
            ..base.clone()
        }));
    }
    out.extend(
        stacks
            .into_iter()
            .map(|(stack_id, ids, frames)| AdhocLandingRow {
                kind: KIND_STACKS,
                stack_id,
                node_ids: ids,
                names: frames.to_vec(),
                ..base.clone()
            }),
    );
    out
}

/// The tree rows of `stacks`, as S14 answers them: per node path its total
/// and self, node ids chained as the push path's, in the order each node
/// is first met.
pub fn tree_rows(stacks: &[(Vec<String>, i64)]) -> Vec<TreeRow> {
    let mut out: Vec<TreeRow> = Vec::new();
    let mut at: HashMap<u128, usize> = HashMap::new();
    for (frames, v) in stacks {
        let ids = node_ids(frames);
        let mut parent = 0u128;
        for (i, (node, name)) in ids.iter().zip(frames).enumerate() {
            let k = *at.entry(*node).or_insert_with(|| {
                out.push(TreeRow {
                    node: *node,
                    parent,
                    name: name.clone(),
                    total: 0,
                    self_value: 0,
                });
                out.len() - 1
            });
            out[k].total += v;
            if i + 1 == ids.len() {
                out[k].self_value += v;
            }
            parent = *node;
        }
    }
    out
}

/// S14 over `ids` (one, or two for a `Diff`), the type named `name` or the
/// first (§3.1).
pub fn trees_sql(
    tenant: &Tenant,
    config: &ProfilesReadConfig,
    ids: &[&str],
    name: Option<&str>,
) -> String {
    let term = TenantTerm::of(tenant);
    let adhoc = &config.adhoc_table;
    let mut ids: Vec<&str> = ids.to_vec();
    ids.dedup();
    let list: Vec<String> = ids.iter().map(|id| ch_string(id)).collect();
    let list = list.join(", ");
    let order = match name {
        Some(n) => {
            let n = ch_string(n);
            format!(
                "ORDER BY display_name = {n} DESC, if(display_name = {n}, -toInt32(type_ordinal), toInt32(type_ordinal))"
            )
        }
        None => "ORDER BY type_ordinal".to_string(),
    };
    let sel_of = |id: &str| {
        format!(
            "(SELECT type_ordinal FROM {adhoc} WHERE {term} AND upload_id = {} AND row_kind = 1 {order} LIMIT 1)",
            ch_string(id)
        )
    };
    let sel = match ids.as_slice() {
        [l, r] => format!(
            "if(upload_id = {}, {}, {})",
            ch_string(l),
            sel_of(l),
            sel_of(r)
        ),
        [l, ..] => sel_of(l),
        [] => sel_of(""),
    };
    format!(
        "SELECT 0 AS part, toString(upload_id) AS uid, node_ids[i] AS node,
       any(if(i = 1, toUInt128(0), node_ids[i - 1])) AS parent,
       any(names[i]) AS name,
       sum(v) AS total,
       sumIf(v, i = length(node_ids)) AS self_value,
       toUInt16(0) AS type_ordinal, toUInt8(0) AS selected, '' AS upload_name, toInt64(0) AS uploaded_at,
       '' AS format, '' AS display_name, '' AS spy_name, toUInt32(0) AS sample_rate, '' AS units
FROM (
  SELECT st.upload_id AS upload_id, st.stack_id AS stack_id, any(st.node_ids) AS node_ids, any(st.names) AS names, any(agg.v) AS v
  FROM (
    SELECT upload_id, stack_id, node_ids, names
    FROM {adhoc}
    WHERE {term} AND upload_id IN ({list}) AND row_kind = 2
  ) AS st
  INNER JOIN (
    SELECT upload_id, stack_id, sum(value) AS v
    FROM {adhoc}
    WHERE {term} AND upload_id IN ({list}) AND row_kind = 0
      AND type_ordinal = {sel}
    GROUP BY upload_id, stack_id
  ) AS agg ON st.upload_id = agg.upload_id AND st.stack_id = agg.stack_id
  GROUP BY st.upload_id, st.stack_id
)
ARRAY JOIN arrayEnumerate(node_ids) AS i
GROUP BY upload_id, node
UNION ALL
SELECT 1, toString(upload_id), toUInt128(0), toUInt128(0), '', toInt64(0), toInt64(0), type_ordinal,
       toUInt8(type_ordinal = {sel}), upload_name, uploaded_at, format, display_name, spy_name, sample_rate, units
FROM {adhoc}
WHERE {term} AND upload_id IN ({list}) AND row_kind = 1"
    )
}

/// S15: the tenant's uploads, newest first (§3.2).
pub fn list_sql(tenant: &Tenant, config: &ProfilesReadConfig) -> String {
    let term = TenantTerm::of(tenant);
    let adhoc = &config.adhoc_table;
    format!(
        "SELECT toString(upload_id) AS id, any(upload_name) AS name, any(uploaded_at) AS uploaded_at
FROM {adhoc}
WHERE {term} AND row_kind = 1
GROUP BY upload_id
ORDER BY uploaded_at DESC, id DESC"
    )
}

/// Upload `uid`'s type rows by ordinal, its selected type and that type's
/// tree.
pub fn select<'a>(rows: &'a [AdhocRow], uid: &str) -> Selected<'a> {
    let mut types: Vec<&AdhocRow> = rows
        .iter()
        .filter(|r| r.part == 1 && r.uid == uid)
        .collect();
    types.sort_by_key(|r| r.type_ordinal);
    let selected = types.iter().copied().find(|r| r.selected == Some(1));
    let tree = rows
        .iter()
        .filter(|r| r.part == 0 && r.uid == uid)
        .map(|r| TreeRow {
            node: r.node,
            parent: r.parent,
            name: r.name.clone(),
            total: r.total,
            self_value: r.self_value,
        })
        .collect();
    Selected {
        types,
        selected,
        tree,
    }
}

/// The flame graph of `rows`, the tree of the type `type_row`, with `m`
/// nodes, by the upload's format's renderer (§4.4).
pub fn render(
    type_row: &AdhocRow,
    rows: &[TreeRow],
    m: i64,
) -> Result<Flamebearer, ProfilesReadError> {
    let meta = Meta {
        spy_name: type_row.spy_name.clone(),
        sample_rate: type_row.sample_rate,
        units: type_row.units.clone(),
        name: type_row.display_name.clone(),
    };
    let (graph, groups) = if type_row.format == convert::Format::Pprof.as_str() {
        let tree = super::flamegraph::merged_tree(rows, m)?;
        (super::flamegraph::flame_graph_of(&tree, m), None)
    } else {
        (og::flame_graph(rows, m), Some(BTreeMap::new()))
    };
    Ok(Flamebearer {
        names: graph.names,
        levels: graph.levels,
        num_ticks: graph.total,
        max_self: graph.max_self,
        groups,
        meta: Some(meta),
        ..Flamebearer::default()
    })
}

/// The diff of two rendered sides with `m` nodes (AA1): each side's flame
/// graph read back into a tree (`adhocprofiles.go:418-436`), then the
/// diff (`:276-393`), named for the left side's type.
pub fn diff(
    left: &Flamebearer,
    right: &Flamebearer,
    m: i64,
) -> Result<Flamebearer, ProfilesReadError> {
    let tree = |fb: &Flamebearer| -> Result<Tree, ProfilesReadError> {
        stacks_of_levels(&fb.names, &fb.levels)
            .map(|s| Tree::from_stacks(&s))
            .map_err(|e| {
                ProfilesReadError::Internal(format!("failed to convert profile to tree: {e}"))
            })
    };
    let d = super::flamegraph_diff::diff_of(&tree(left)?, &tree(right)?, m);
    let lm = left.meta.clone().unwrap_or_default();
    let id = format!(":{}:{}", lm.name, lm.units);
    let (units, sample_rate, name) = metadata(&id);
    Ok(Flamebearer {
        names: d.names,
        levels: d.levels,
        num_ticks: d.total,
        max_self: d.max_self,
        double: true,
        left_ticks: d.left_ticks,
        right_ticks: d.right_ticks,
        meta: Some(Meta {
            spy_name: String::new(),
            sample_rate,
            units: units.to_string(),
            name: name.to_string(),
        }),
        ..Flamebearer::default()
    })
}

/// The answer's text: the flamebearer without its final newline.
pub fn answer_text(fb: &Flamebearer) -> String {
    let mut s = fb.to_json();
    if s.ends_with('\n') {
        s.pop();
    }
    s
}

#[cfg(test)]
#[path = "../../../tests/profileql_adhoc_fixtures/mod.rs"]
pub(crate) mod fx;

#[cfg(test)]
mod tests;
