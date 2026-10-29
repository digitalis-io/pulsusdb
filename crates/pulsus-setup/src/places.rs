//! Where each bundle file goes on its host, and who owns it there. Each
//! path is the one the repository already uses; the source is beside it.

use crate::Validated;

/// The bundle's directory name in `setup.md`.
pub(crate) const BUNDLE_DIR: &str = "pulsus-bundle";

/// The server's include directories. `ci/setup-cluster/run.sh`
/// (`start_server`) and `ci/bench-cluster/compose.yaml` mount the files
/// there.
pub(crate) const CH_CONFIG_D: &str = "/etc/clickhouse-server/config.d/";
pub(crate) const CH_USERS_D: &str = "/etc/clickhouse-server/users.d/";

/// Keeper's config file: `ci/setup-cluster/run.sh` (K1) and
/// `ci/bench-cluster/compose.yaml` mount it there, and it is the
/// `--config` of the `clickhouse-keeper` unit in the Keeper image
/// `ci/setup-cluster/run.sh` pins.
pub(crate) const KEEPER_CONFIG: &str = "/etc/clickhouse-keeper/keeper_config.xml";

/// Keeper's data: the directory above `server_xml`'s log and snapshot
/// paths, which the generated `keeper_config.xml` names.
pub(crate) const KEEPER_DATA: &str = "/var/lib/clickhouse-keeper";

/// The user both packaged services run as (`User=clickhouse` in the
/// `clickhouse-server` and `clickhouse-keeper` units of the images
/// `ci/setup-cluster/run.sh` pins), and the owner of every file they read.
pub(crate) const CH_OWNER: &str = "clickhouse";

/// The unit names in those images' `/lib/systemd/system`.
pub(crate) const SERVER_UNIT: &str = "clickhouse-server";
pub(crate) const KEEPER_UNIT: &str = "clickhouse-keeper";

/// PulsusDB's env file on a packaged install, with its owner and mode:
/// `packaging/nfpm.yaml` installs it there as `root:pulsusdb` 0640, and
/// `packaging/systemd/pulsusdb.service` reads it with `EnvironmentFile=`.
pub(crate) const PULSUSDB_ENV: &str = "/etc/pulsusdb/pulsusdb.env";
pub(crate) const PULSUSDB_ENV_OWNER: &str = "root:pulsusdb";
/// The packaged unit's name (`packaging/nfpm.yaml`).
pub(crate) const PULSUSDB_UNIT: &str = "pulsusdb";

/// The mode of bundle file `path` where it is fetched to: `0755` for the
/// schema script, which step 4 runs, `0644` for the rest.
pub(crate) fn bundle_mode(path: &str) -> u32 {
    if path.ends_with(".sh") { 0o755 } else { 0o644 }
}

/// Which machine a bundle file goes to.
pub(crate) enum Host {
    /// A ClickHouse or Keeper host, by its name on the form.
    Named(String),
    /// The host PulsusDB runs on.
    Pulsusdb,
    /// The machine `setup.md`'s step 4 runs from: the file stays in the
    /// bundle.
    Step4,
}

/// Where bundle file `path` goes: the host, the destination, and the owner
/// and mode there. `setup.md`'s table and [`label`] both read this.
pub(crate) fn destination(v: &Validated, path: &str) -> (Host, String, String) {
    let ch = format!("`{CH_OWNER}:{CH_OWNER}`, `640`");
    let host = |rest: &str, hosts: &[String]| -> Option<(String, String)> {
        let (n, rest) = rest.split_once('/')?;
        let name = hosts.get(n.parse::<usize>().ok()?.checked_sub(1)?)?;
        Some((name.clone(), rest.to_string()))
    };
    if let Some((name, rest)) = path.strip_prefix("nodes/").and_then(|r| host(r, &v.hosts)) {
        let dir = if let Some(f) = rest.strip_prefix("config.d/") {
            format!("{CH_CONFIG_D}{f}")
        } else if let Some(f) = rest.strip_prefix("users.d/") {
            format!("{CH_USERS_D}{f}")
        } else {
            format!("/etc/clickhouse-server/{rest}")
        };
        return (Host::Named(name), dir, ch);
    }
    if let Some((name, _)) = path
        .strip_prefix("keepers/")
        .and_then(|r| host(r, &v.keepers))
    {
        return (Host::Named(name), KEEPER_CONFIG.to_string(), ch);
    }
    if path == "pulsusdb.env" {
        return (
            Host::Pulsusdb,
            PULSUSDB_ENV.to_string(),
            format!("`{PULSUSDB_ENV_OWNER}`, `640`"),
        );
    }
    let mode = format!("yours, `{:04o}`", bundle_mode(path));
    (
        Host::Step4,
        format!("{BUNDLE_DIR}/{path}"),
        mode.to_string(),
    )
}

/// The host column of `setup.md`'s table.
pub(crate) fn host_cell(host: &Host) -> String {
    match host {
        Host::Named(name) => name.clone(),
        Host::Pulsusdb => "the PulsusDB host".to_string(),
        Host::Step4 => "the machine that runs step 4".to_string(),
    }
}

/// Where `path` goes as the page and the API show it: `host:/path`, or
/// where the file is used.
pub(crate) fn label(v: &Validated, path: &str) -> String {
    let (host, dest, _) = destination(v, path);
    match host {
        Host::Named(name) => format!("{name}:{dest}"),
        Host::Pulsusdb => format!("<pulsusdb host>:{dest}"),
        Host::Step4 => format!("on the machine you run step 4 from: {dest}"),
    }
}
