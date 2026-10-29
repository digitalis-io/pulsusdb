//! The setup form's generator (issue #746 part C).
//!
//! [`Inputs::from_pairs`] reads the form's fields, [`validate`] applies the
//! form's own rules and then `pulsus_schema::ctx_from_vars` for every rule
//! the schema decides, and [`bundle`] writes the files. Nothing here reads
//! a file, opens a connection or holds a secret.

use std::path::Path;

use serde::Serialize;

mod inputs;
mod places;
mod server_xml;
mod steps;
mod vars;

pub use inputs::{FieldError, FieldWarning, Inputs, LevelRow, Tls, Totals, Validated, validate};

/// One generated file: its path inside the bundle, and its text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct File {
    pub path: String,
    pub content: String,
}

/// Every file of the bundle, in the order the page lists them: each
/// host's, each Keeper host's, then the schema, `pulsusdb.env` and
/// `setup.md`.
pub fn bundle(v: &Validated) -> Vec<File> {
    let mut files = Vec::new();
    let mut add = |path: String, content: String| files.push(File { path, content });
    let cluster = server_xml::cluster(v);
    let storage = server_xml::storage(v);
    let tls = server_xml::tls(v);
    for i in 1..=v.hosts.len() {
        let dir = format!("nodes/{i}");
        add(
            format!("{dir}/config.d/pulsus-cluster.xml"),
            cluster.clone(),
        );
        add(
            format!("{dir}/config.d/pulsus-node.xml"),
            server_xml::node(v, i),
        );
        add(
            format!("{dir}/config.d/pulsus-storage.xml"),
            storage.clone(),
        );
        if let Some(tls) = &tls {
            add(format!("{dir}/config.d/pulsus-tls.xml"), tls.clone());
        }
        add(format!("{dir}/users.d/pulsus-user.xml"), server_xml::user());
    }
    for j in 1..=v.keepers.len() {
        add(
            format!("keepers/{j}/keeper_config.xml"),
            server_xml::keeper(v, j),
        );
    }
    add("schema.env".to_string(), vars::schema_env(v));
    add(
        "schema-rendered.sql".to_string(),
        pulsus_schema::rendered(&v.ctx),
    );
    add(
        "schema/schema.sh".to_string(),
        pulsus_schema::SCHEMA_SH.to_string(),
    );
    add(
        "schema/schema.sql".to_string(),
        pulsus_schema::SCHEMA_SQL.to_string(),
    );
    add("pulsusdb.env".to_string(), vars::pulsusdb_env(v));
    let statements = pulsus_schema::rendered_statements(&v.ctx).len();
    let paths: Vec<String> = files.iter().map(|f| f.path.clone()).collect();
    files.push(File {
        path: "setup.md".to_string(),
        content: steps::setup_md(v, statements, &paths),
    });
    files
}

/// Writes `files` as the new directory `dir`, creating its parents. They
/// are written into a temporary directory beside it, which is then renamed
/// to `dir`, so `dir` holds one whole bundle or nothing. A `dir` that already exists is
/// refused, and an error reading it is returned; two writers to one `dir`
/// get one bundle and one refusal. The schema script is made executable.
pub fn write_bundle(dir: &Path, files: &[File]) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};
    let exists = || {
        Error::new(
            ErrorKind::AlreadyExists,
            format!(
                "{} already exists: write the bundle to a new directory",
                dir.display()
            ),
        )
    };
    match std::fs::symlink_metadata(dir) {
        Ok(_) => return Err(exists()),
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let name = dir
        .file_name()
        .ok_or_else(|| Error::other(format!("{} names no directory", dir.display())))?
        .to_string_lossy()
        .to_string();
    let parent = match dir.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    std::fs::create_dir_all(parent)?;
    let mut n = 0u32;
    let tmp = loop {
        let candidate = parent.join(format!(".{name}.{}-{n}", std::process::id()));
        match std::fs::create_dir(&candidate) {
            Ok(()) => break candidate,
            Err(e) if e.kind() == ErrorKind::AlreadyExists => n += 1,
            Err(e) => return Err(e),
        }
    };
    // A rename onto a directory that holds anything fails, so of two
    // writers only the first lands.
    let result = write_files(&tmp, files).and_then(|()| {
        std::fs::rename(&tmp, dir).map_err(|e| if dir.exists() { exists() } else { e })
    });
    if result.is_err() {
        let _ = std::fs::remove_dir_all(&tmp);
    }
    result
}

fn write_files(dir: &Path, files: &[File]) -> std::io::Result<()> {
    for f in files {
        let path = dir.join(&f.path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, &f.content)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = places::bundle_mode(&f.path);
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode))?;
        }
    }
    Ok(())
}

/// The page's lines for what the schema keeps: raw data, each level, then
/// the due-time ceiling.
pub fn total_lines(v: &Validated) -> Vec<String> {
    steps::total_lines(v)
}

/// Where bundle file `path` goes, as the page and the API show it:
/// `host:/path` for a file placed on a host, else where it is used.
pub fn destination_label(v: &Validated, path: &str) -> String {
    places::label(v, path)
}

/// "1 Keeper host", or "N Keeper hosts".
pub fn keeper_hosts(v: &Validated) -> String {
    steps::keeper_hosts(v)
}

/// The figures the server derives from the hardware, shown and not
/// written: (label, value).
pub fn sizing(v: &Validated) -> Vec<(&'static str, String)> {
    steps::sizing(v)
}

/// Every form field, in the form's order: the scalar fields, then each
/// level row's, each with its switch point.
pub fn field_names() -> Vec<String> {
    let mut names: Vec<String> = [
        "hosts",
        "keepers",
        "shards",
        "replicas",
        "cluster",
        "database",
        "tls_cert",
        "tls_key",
        "tls_ca",
        "local_path",
        "remote_url",
        "cores",
        "memory_gib",
        "disk_gib",
        "raw_local_days",
        "raw_remote_days",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    for k in 1..=pulsus_schema::MAX_LEVELS {
        names.push(format!("l{k}_width"));
        names.push(format!("l{k}_from"));
        names.push(format!("l{k}_local"));
        names.push(format!("l{k}_remote"));
    }
    names
}

/// The form's starting values: the chart's cluster name, the database
/// default, the shipped data path, and the default levels with their
/// default switch points.
pub fn prefilled() -> Vec<(String, String)> {
    let mut pairs = vec![
        ("cluster".to_string(), "pulsus_cluster".to_string()),
        ("database".to_string(), "pulsus".to_string()),
        ("local_path".to_string(), "/var/lib/clickhouse/".to_string()),
    ];
    for (i, (width, from)) in pulsus_schema::DEFAULT_ROLLUP_LEVELS
        .iter()
        .zip(pulsus_schema::DEFAULT_ROLLUP_RANGES)
        .enumerate()
    {
        let k = i + 1;
        pairs.push((format!("l{k}_width"), width.to_string()));
        pairs.push((format!("l{k}_from"), from.to_string()));
    }
    pairs
}

/// The issue's example as form pairs: six hosts in two shards of three,
/// three Keeper hosts, TLS, object storage, raw data 7 days local and 90 on
/// object storage, four levels kept 365 days on local disk.
pub fn example_pairs() -> Vec<(String, String)> {
    let mut pairs = vec![
        ("hosts", "ch1\nch2\nch3\nch4\nch5\nch6"),
        ("keepers", "kp1\nkp2\nkp3"),
        ("shards", "2"),
        ("replicas", "3"),
        ("cluster", "pulsus_cluster"),
        ("database", "pulsus"),
        ("tls_cert", "/etc/ch/server.crt"),
        ("tls_key", "/etc/ch/server.key"),
        ("tls_ca", "/etc/ch/ca.crt"),
        ("local_path", "/var/lib/clickhouse/"),
        ("remote_url", "https://bucket.example/pulsus/"),
        ("cores", "32"),
        ("memory_gib", "128"),
        ("disk_gib", "2048"),
        ("raw_local_days", "7"),
        ("raw_remote_days", "90"),
        ("l1_width", "60"),
        ("l1_from", "3600"),
        ("l1_local", "365"),
        ("l2_width", "720"),
        ("l2_from", "86400"),
        ("l2_local", "365"),
        ("l3_width", "7200"),
        ("l3_from", "604800"),
        ("l3_local", "365"),
        ("l4_width", "86400"),
        ("l4_from", "7776000"),
        ("l4_local", "365"),
    ];
    pairs.sort_by_key(|(k, _)| *k);
    pairs
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}
