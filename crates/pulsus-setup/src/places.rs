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

/// One row of "Where each file goes": the host, the destination and the
/// owner and mode, for bundle file `path`.
pub(crate) fn destination(v: &Validated, path: &str) -> (String, String, String) {
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
        return (name, dir, ch);
    }
    if let Some((name, _)) = path
        .strip_prefix("keepers/")
        .and_then(|r| host(r, &v.keepers))
    {
        return (name, KEEPER_CONFIG.to_string(), ch);
    }
    if path == "pulsusdb.env" {
        return (
            "the PulsusDB host".to_string(),
            PULSUSDB_ENV.to_string(),
            format!("`{PULSUSDB_ENV_OWNER}`, `640`"),
        );
    }
    let mode = if path.ends_with(".sh") {
        "yours, executable"
    } else {
        "yours"
    };
    (
        "the machine that runs step 4".to_string(),
        format!("{BUNDLE_DIR}/{path}"),
        mode.to_string(),
    )
}
