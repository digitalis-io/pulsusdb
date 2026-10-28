//! The form's fields, and the rules the form adds (W1-W13). Every rule the
//! schema decides is `pulsus_schema::ctx_from_vars`'s.

use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use pulsus_schema::{MAX_LEVELS, RenderCtx, SchemaMode, ctx_from_vars};
use serde::Serialize;

/// The form's fields by name. A field given twice holds both values, one
/// per line; an absent field is blank.
#[derive(Debug, Clone, Default)]
pub struct Inputs {
    fields: BTreeMap<String, String>,
}

impl Inputs {
    pub fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let mut fields: BTreeMap<String, String> = BTreeMap::new();
        for (k, v) in pairs {
            let v = v.into();
            fields
                .entry(k.into())
                .and_modify(|old| {
                    old.push('\n');
                    old.push_str(&v);
                })
                .or_insert(v);
        }
        Inputs { fields }
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

impl LevelRow {
    pub fn total(&self) -> u64 {
        self.local_days + self.remote_days
    }
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
            raw: self.raw_local_days + self.raw_remote_days,
            levels: self.levels.iter().map(LevelRow::total).collect(),
        }
    }

    /// Host `i`'s shard, `i` from 1 (D9).
    pub fn shard_of(&self, i: usize) -> usize {
        (i - 1) / self.replicas as usize + 1
    }
}

const NOT_A_HOST: &str = "is not an IPv4 address or a host name";
const DAYS_MAX: u64 = 4_294_967_295;

/// `text` as digits only, at most `digits` of them, from `lo` to `hi`.
fn whole(text: &str, digits: usize, lo: u64, hi: u64) -> Option<u64> {
    if text.is_empty() || text.len() > digits || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse::<u64>().ok().filter(|v| (lo..=hi).contains(v))
}

/// W1: an IPv4 address, or a host name of dot-separated labels.
fn is_host(host: &str) -> bool {
    if host.parse::<Ipv4Addr>().is_ok() {
        return true;
    }
    (1..=253).contains(&host.len())
        && host.split('.').all(|label| {
            (1..=63).contains(&label.len())
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
                && !label.starts_with('-')
                && !label.ends_with('-')
        })
}

/// One host per line, lowercased; blank lines skipped.
fn host_lines(text: &str) -> Vec<String> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

/// An absolute path of `[A-Za-z0-9._/-]`.
fn is_path(p: &str) -> bool {
    p.starts_with('/')
        && p.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'/' | b'-'))
}

/// W6.
fn is_remote_url(url: &str) -> bool {
    if url.contains(['?', '#', '@']) || !url.ends_with('/') {
        return false;
    }
    let Ok(uri) = url.parse::<http::Uri>() else {
        return false;
    };
    matches!(uri.scheme_str(), Some("http" | "https"))
        && uri.host().is_some_and(|h| !h.is_empty())
        && uri.path().ends_with('/')
}

/// The form field a `ctx_from_vars` message belongs to (§5.2).
fn schema_field(message: &str) -> &'static str {
    let starts = |prefixes: &[&str]| prefixes.iter().any(|p| message.starts_with(p));
    if starts(&["database "]) {
        "database"
    } else if starts(&["cluster ", "--mode cluster", "PULSUS_CLUSTER"]) {
        "cluster"
    } else if starts(&["PULSUS_ROLLUP_"]) {
        "levels"
    } else if starts(&[
        "PULSUS_RETENTION_DAYS",
        "PULSUS_METRICS_HOT_DAYS",
        "PULSUS_LOGS_HOT_DAYS",
        "PULSUS_TRACES_HOT_DAYS",
    ]) {
        "raw"
    } else if starts(&[
        "storage_policy ",
        "cold_volume ",
        "PULSUS_STORAGE_POLICY",
        "PULSUS_COLD_VOLUME",
    ]) {
        "remote_url"
    } else {
        "schema"
    }
}

fn refuse(errors: &mut Vec<FieldError>, field: &str, message: String) {
    errors.push(FieldError {
        field: field.to_string(),
        message,
    });
}

/// W12: one pair of day fields, as (local, object storage); `0` for a
/// refused one.
fn days(
    inputs: &Inputs,
    e: &mut Vec<FieldError>,
    remote_url: &str,
    local_field: &str,
    remote_field: &str,
) -> (u64, u64) {
    let local = whole(inputs.get(local_field), 10, 1, DAYS_MAX);
    if local.is_none() {
        refuse(
            e,
            local_field,
            "must be a whole number from 1 to 4294967295".to_string(),
        );
    }
    let remote_text = inputs.get(remote_field);
    let remote = if remote_text.is_empty() {
        Some(0)
    } else {
        whole(remote_text, 10, 0, DAYS_MAX)
    };
    match remote {
        None => refuse(
            e,
            remote_field,
            "must be a whole number from 0 to 4294967295".to_string(),
        ),
        Some(r) if r > 0 && remote_url.is_empty() => refuse(
            e,
            remote_field,
            "object storage days need a remote storage URL".to_string(),
        ),
        Some(_) => {}
    }
    (local.unwrap_or(0), remote.unwrap_or(0))
}

/// W1-W13, then `ctx_from_vars`; every form failure, or the schema's one.
pub fn validate(inputs: &Inputs) -> Result<Validated, Vec<FieldError>> {
    let mut errors: Vec<FieldError> = Vec::new();
    let e = &mut errors;

    // W1, W2.
    let hosts = host_lines(inputs.get("hosts"));
    let mut seen: Vec<&str> = Vec::new();
    for host in &hosts {
        if !is_host(host) {
            refuse(e, "hosts", format!("{host} {NOT_A_HOST}"));
        } else if seen.contains(&host.as_str()) {
            refuse(e, "hosts", format!("{host} is listed twice"));
        }
        seen.push(host);
    }

    // W3, W4.
    let shards = whole(inputs.get("shards"), 3, 1, 100);
    let replicas = whole(inputs.get("replicas"), 3, 1, 100);
    for (field, value) in [("shards", shards), ("replicas", replicas)] {
        if value.is_none() {
            refuse(e, field, "must be a whole number from 1 to 100".to_string());
        }
    }
    if let (Some(s), Some(r)) = (shards, replicas) {
        match s.checked_mul(r).filter(|h| *h <= 100) {
            None => refuse(e, "hosts", format!("{} hosts is more than 100", s * r)),
            Some(h) if h != hosts.len() as u64 => refuse(
                e,
                "hosts",
                format!(
                    "{} hosts for {s} shards of {r} replicas: give {h}",
                    hosts.len()
                ),
            ),
            Some(_) => {}
        }
    }

    // W10, W11.
    let keepers = host_lines(inputs.get("keepers"));
    let mut seen: Vec<&str> = Vec::new();
    for keeper in &keepers {
        if !is_host(keeper) {
            refuse(e, "keepers", format!("{keeper} {NOT_A_HOST}"));
        } else if seen.contains(&keeper.as_str()) {
            refuse(e, "keepers", format!("{keeper} is listed twice"));
        } else if hosts.contains(keeper) {
            refuse(
                e,
                "keepers",
                format!("{keeper} is also a ClickHouse host: Keeper runs on its own hosts"),
            );
        }
        seen.push(keeper);
    }
    if !(3..=9).contains(&keepers.len()) {
        refuse(e, "keepers", "give from 3 to 9 Keeper hosts".to_string());
    }

    // W13.
    let cluster = inputs.get("cluster");
    if !cluster.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') {
        refuse(e, "cluster", "must start with a letter or _".to_string());
    }

    // W7.
    let tls_paths = [
        ("tls_cert", inputs.get("tls_cert")),
        ("tls_key", inputs.get("tls_key")),
        ("tls_ca", inputs.get("tls_ca")),
    ];
    let given = tls_paths.iter().filter(|(_, p)| !p.is_empty()).count();
    let tls = if given == 0 {
        None
    } else if given < 3 {
        refuse(
            e,
            "tls",
            "give the certificate, the key and the CA, or none".to_string(),
        );
        None
    } else {
        for (field, path) in tls_paths {
            if !is_path(path) {
                refuse(
                    e,
                    field,
                    "must be an absolute path of letters, digits, . _ / -".to_string(),
                );
            }
        }
        Some(Tls {
            cert: tls_paths[0].1.to_string(),
            key: tls_paths[1].1.to_string(),
            ca: tls_paths[2].1.to_string(),
        })
    };

    // W5.
    let local_path = inputs.get("local_path");
    if !(is_path(local_path) && local_path.ends_with('/')) {
        refuse(
            e,
            "local_path",
            "must be an absolute directory ending in /, of letters, digits, . _ / -".to_string(),
        );
    }

    // W6.
    let remote_url = inputs.get("remote_url");
    if !remote_url.is_empty() && !is_remote_url(remote_url) {
        refuse(
            e,
            "remote_url",
            "must be an http(s) URL with a host, ending in /, without a user, query or fragment"
                .to_string(),
        );
    }

    // W8.
    let mut sizes = [0u32; 3];
    for (slot, field) in sizes.iter_mut().zip(["cores", "memory_gib", "disk_gib"]) {
        match whole(inputs.get(field), 7, 1, 1_000_000) {
            Some(v) => *slot = v as u32,
            None => refuse(
                e,
                field,
                "must be a whole number from 1 to 1000000".to_string(),
            ),
        }
    }

    // W12.
    let (raw_local_days, raw_remote_days) =
        days(inputs, e, remote_url, "raw_local_days", "raw_remote_days");

    // W9, W12 per level.
    let mut levels: Vec<LevelRow> = Vec::new();
    let mut empty_row = false;
    for k in 1..=MAX_LEVELS {
        let width = inputs.get(&format!("l{k}_width"));
        if width.is_empty() {
            empty_row = true;
            continue;
        }
        if empty_row {
            refuse(
                e,
                &format!("l{k}_width"),
                format!("level {k} is set after an empty row"),
            );
            continue;
        }
        let (local_days, remote_days) = days(
            inputs,
            e,
            remote_url,
            &format!("l{k}_local"),
            &format!("l{k}_remote"),
        );
        levels.push(LevelRow {
            width: width.to_string(),
            from: (k > 1).then(|| inputs.get(&format!("l{k}_from")).to_string()),
            local_days,
            remote_days,
        });
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    let remote_url = (!remote_url.is_empty()).then(|| remote_url.to_string());
    let vars = schema_vars(
        inputs.get("database"),
        cluster,
        remote_url.is_some(),
        raw_local_days,
        raw_remote_days,
        &levels,
    );
    let map: BTreeMap<&str, &str> = vars.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let ctx = ctx_from_vars(&map, Some(SchemaMode::Cluster)).map_err(|message| {
        vec![FieldError {
            field: schema_field(&message).to_string(),
            message,
        }]
    })?;

    Ok(Validated {
        hosts,
        keepers,
        shards: shards.unwrap_or(1) as u32,
        replicas: replicas.unwrap_or(1) as u32,
        cluster: cluster.to_string(),
        tls,
        local_path: local_path.to_string(),
        remote_url,
        cores: sizes[0],
        memory_gib: sizes[1],
        disk_gib: sizes[2],
        raw_local_days,
        raw_remote_days,
        levels,
        vars,
        ctx,
    })
}

/// §5.3: the schema variables from the form's values.
fn schema_vars(
    database: &str,
    cluster: &str,
    remote: bool,
    raw_local_days: u64,
    raw_remote_days: u64,
    levels: &[LevelRow],
) -> BTreeMap<&'static str, String> {
    let join = |items: Vec<String>| items.join(",");
    let mut vars = BTreeMap::new();
    vars.insert("CLICKHOUSE_DB", database.to_string());
    vars.insert("PULSUS_CLUSTER", cluster.to_string());
    vars.insert(
        "PULSUS_RETENTION_DAYS",
        (raw_local_days + raw_remote_days).to_string(),
    );
    if remote {
        vars.insert("PULSUS_STORAGE_POLICY", "pulsus_tiered".to_string());
        vars.insert("PULSUS_COLD_VOLUME", "cold".to_string());
        for name in [
            "PULSUS_METRICS_HOT_DAYS",
            "PULSUS_LOGS_HOT_DAYS",
            "PULSUS_TRACES_HOT_DAYS",
        ] {
            vars.insert(name, raw_local_days.to_string());
        }
    }
    vars.insert(
        "PULSUS_ROLLUP_LEVELS",
        join(levels.iter().map(|l| l.width.clone()).collect()),
    );
    vars.insert(
        "PULSUS_ROLLUP_RANGES",
        join(levels.iter().filter_map(|l| l.from.clone()).collect()),
    );
    if !levels.is_empty() {
        vars.insert(
            "PULSUS_ROLLUP_RETENTION_DAYS",
            join(levels.iter().map(|l| l.total().to_string()).collect()),
        );
    }
    if levels.iter().any(|l| l.remote_days > 0) {
        vars.insert(
            "PULSUS_ROLLUP_HOT_DAYS",
            join(levels.iter().map(|l| l.local_days.to_string()).collect()),
        );
    }
    vars
}
