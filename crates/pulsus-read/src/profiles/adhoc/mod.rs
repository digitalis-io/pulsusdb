//! Issue #831: ad hoc profile uploads. An upload is converted in process
//! (`convert`), stored as one landing block of kinds 5 and 6, which
//! `profile_adhoc_mv` copies into `profile_adhoc` (plan §2, §4.3), and read
//! back by S14 (`Get`, `Diff`) and S15 (`List`), one statement each (§3).
//! Its flame graph is drawn as the reference draws the upload's format:
//! a pprof by the push path's renderer, every other format by `og` (§4.4).

pub mod convert;
pub mod og;

use pulsus_clickhouse::Row;
use pulsus_model::Tenant;
use pulsus_profiles_wire::flamebearer::Flamebearer;
use serde::{Deserialize, Serialize};

use super::engine::{ProfilesReadConfig, ProfilesReadError};
use super::tree::TreeRow;
use convert::Converted;

/// The landing kind of an upload's type and sample rows.
pub const KIND_ROWS: u8 = 5;
/// The landing kind of an upload's stack rows.
pub const KIND_STACKS: u8 = 6;

/// The largest `profile` text an upload may carry.
pub const MAX_PROFILE_BYTES: usize = 4_194_304;

/// One landing row of an upload: the landing table's columns an upload
/// sets, in the table's order; every other column takes its type default.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct AdhocLandingRow {
    pub org_id: String,
    pub received_ms: i64,
    pub kind: u8,
    pub stack_id: u128,
    pub value: i64,
    pub node_ids: Vec<u128>,
    pub names: Vec<String>,
    /// The `UUID` column as two big-endian `UInt64` halves.
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
    pub selected: u8,
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
pub fn mint_id(_now_ms: i64) -> String {
    String::new()
}

/// An upload's name, every character outside `[A-Za-z0-9._-]` as `_`.
pub fn sanitize_name(name: &str) -> String {
    name.to_string()
}

/// Whether `id` is a valid id: not empty, every character in
/// `[A-Za-z0-9._-]`.
pub fn valid_id(_id: &str) -> bool {
    true
}

/// `raw` resolved as the reference resolves `max_nodes`; a refusal is its
/// text.
pub fn max_nodes(raw: Option<i64>, config: &ProfilesReadConfig) -> Result<i64, String> {
    super::tree::resolve_max_nodes(raw.unwrap_or(0), config).map_err(|e| match e {
        ProfilesReadError::InvalidArgument(m) => m,
        other => format!("{other:?}"),
    })
}

/// The landing rows of `c`, uploaded by `tenant` as `name` (§4.3).
pub fn rows(
    _tenant: &Tenant,
    _id: &str,
    _name: &str,
    _uploaded_at: i64,
    _c: &Converted,
) -> Vec<AdhocLandingRow> {
    Vec::new()
}

/// The tree rows of `stacks`, as S14 answers them: per node path its total
/// and self, node ids chained as the push path's.
pub fn tree_rows(_stacks: &[(Vec<String>, i64)]) -> Vec<TreeRow> {
    Vec::new()
}

/// S14 over `ids` (one, or two for a `Diff`), the type named `name` or the
/// first.
pub fn trees_sql(
    _tenant: &Tenant,
    _config: &ProfilesReadConfig,
    _ids: &[&str],
    _name: Option<&str>,
) -> String {
    String::new()
}

/// S15: the tenant's uploads, newest first.
pub fn list_sql(_tenant: &Tenant, _config: &ProfilesReadConfig) -> String {
    String::new()
}

/// Upload `uid`'s type rows, its selected type and that type's tree.
pub fn select<'a>(_rows: &'a [AdhocRow], _uid: &str) -> Selected<'a> {
    Selected::default()
}

/// The flame graph of `rows`, the tree of the type `type_row`, with `m`
/// nodes.
pub fn render(
    _type_row: &AdhocRow,
    _rows: &[TreeRow],
    _m: i64,
) -> Result<Flamebearer, ProfilesReadError> {
    Ok(Flamebearer::default())
}

/// The diff of two rendered sides with `m` nodes (AA1).
pub fn diff(
    _left: &Flamebearer,
    _right: &Flamebearer,
    _m: i64,
) -> Result<Flamebearer, ProfilesReadError> {
    Ok(Flamebearer::default())
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
