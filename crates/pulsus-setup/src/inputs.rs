//! The form's fields, and the rules the form adds (W1-W13).

use std::collections::BTreeMap;

use pulsus_schema::RenderCtx;
use serde::Serialize;

/// The form's fields by name. A field given twice holds both values, one
/// per line; an absent field is blank.
#[derive(Debug, Clone, Default)]
pub struct Inputs {
    fields: BTreeMap<String, String>,
}

impl Inputs {
    pub fn from_pairs<I, K, V>(_pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Inputs::default()
    }

    /// The field's value, `""` when absent.
    pub fn get(&self, name: &str) -> &str {
        self.fields.get(name).map(String::as_str).unwrap_or("")
    }
}

/// A refusal: the form field (or group: `raw`, `levels`, `tls`) and the
/// message shown beside it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FieldError {
    pub field: String,
    pub message: String,
}

/// The certificate, key and CA paths, the same on every host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tls {
    pub cert: String,
    pub key: String,
    pub ca: String,
}

/// One rollup level as the form gives it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LevelRow {
    pub width: String,
    /// The switch point; `None` for the first level.
    pub from: Option<String>,
    pub local_days: u64,
    pub remote_days: u64,
}

/// Each total `T`: local + object-storage days.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Totals {
    pub raw: u64,
    pub levels: Vec<u64>,
}

/// Inputs every rule accepted, and what the schema made of them.
#[derive(Debug, Clone)]
pub struct Validated {
    pub hosts: Vec<String>,
    pub keepers: Vec<String>,
    pub shards: u32,
    pub replicas: u32,
    pub cluster: String,
    pub tls: Option<Tls>,
    pub local_path: String,
    pub remote_url: Option<String>,
    pub cores: u32,
    pub memory_gib: u32,
    pub disk_gib: u32,
    pub raw_local_days: u64,
    pub raw_remote_days: u64,
    pub levels: Vec<LevelRow>,
    /// The schema variables, `schema.env`'s lines.
    pub vars: BTreeMap<&'static str, String>,
    pub ctx: RenderCtx,
}

impl Validated {
    pub fn totals(&self) -> Totals {
        Totals {
            raw: 0,
            levels: Vec::new(),
        }
    }
}

/// W1-W13, then `ctx_from_vars`; every failure, in field order.
pub fn validate(_inputs: &Inputs) -> Result<Validated, Vec<FieldError>> {
    Err(Vec::new())
}
