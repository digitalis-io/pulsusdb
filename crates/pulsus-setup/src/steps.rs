//! `setup.md` (§5.6), the lines for what the schema keeps (§5.1), and the
//! sizing the server derives (§5.5).

use std::fmt::Write;

use crate::Validated;
use crate::places::{
    BUNDLE_DIR, CH_CONFIG_D, CH_OWNER, CH_USERS_D, KEEPER_CONFIG, KEEPER_DATA, KEEPER_UNIT,
    PULSUSDB_ENV, PULSUSDB_ENV_OWNER, PULSUSDB_UNIT, SERVER_UNIT, destination, host_cell,
};
use crate::server_xml::{KEEPER_LOG, KEEPER_SNAPSHOTS};
use crate::vars::http_port;

/// The ceiling of every due time (docs/configuration.md §7).
pub(crate) const CEILING: &str = "no row is due after 2106-02-07 06:28:15 UTC";

/// Raw data's line, each level's, then the ceiling.
pub(crate) fn total_lines(v: &Validated) -> Vec<String> {
    let raw_total = v.raw_local_days + v.raw_remote_days;
    // On the tiered policy with no move rule, a part can still go to the
    // cold volume when the local one is full.
    let mut lines = vec![if v.remote_url.is_some() && v.raw_remote_days == 0 {
        format!(
            "Raw data: kept at least {raw_total} days; no move rule; on local disk while the local disk has room"
        )
    } else {
        format!(
            "Raw data: kept at least {raw_total} days; at least {} on local disk",
            v.raw_local_days
        )
    }];
    for (i, level) in v.levels.iter().enumerate() {
        let head = format!(
            "Level {} ({} s): kept at least {} days;",
            i + 1,
            level.width,
            level.total()
        );
        lines.push(if level.remote_days == 0 {
            format!("{head} no move rule; on local disk while the local disk has room")
        } else {
            format!("{head} at least {} on local disk", level.local_days)
        });
    }
    lines.push(CEILING.to_string());
    lines
}

/// `tenths / 10` with at most one decimal place.
fn tenths(tenths: u64) -> String {
    if tenths.is_multiple_of(10) {
        (tenths / 10).to_string()
    } else {
        format!("{}.{}", tenths / 10, tenths % 10)
    }
}

/// §5.5: each figure the server derives, with its rule.
pub(crate) fn sizing(v: &Validated) -> Vec<(&'static str, String)> {
    let memory = u64::from(v.memory_gib);
    vec![
        (
            "Server memory limit (0.9 × memory)",
            format!("{} GiB", tenths(memory * 9)),
        ),
        (
            "Merge memory soft limit (0.5 × memory)",
            format!("{} GiB", tenths(memory * 5)),
        ),
        ("Threads per query (hardware threads)", v.cores.to_string()),
        ("Disk, as entered", format!("{} GiB", v.disk_gib)),
    ]
}

/// "1 Keeper host", or "N Keeper hosts".
pub(crate) fn keeper_hosts(v: &Validated) -> String {
    match v.keepers.len() {
        1 => "1 Keeper host".to_string(),
        n => format!("{n} Keeper hosts"),
    }
}

/// TLS: the form's three paths on each `kind` host, readable by the
/// service's user; empty without TLS.
fn tls_install(v: &Validated, kind: &str) -> String {
    let Some(t) = &v.tls else {
        return String::new();
    };
    let own = format!("-o {CH_OWNER} -g {CH_OWNER}");
    format!(
        " Then put the TLS files at the paths the form names, readable by `{CH_OWNER}`: on each {kind} host, with that host's own certificate and key,\n\n```sh\nsudo install -D -m 644 {own} <this host's certificate> {}\nsudo install -D -m 600 {own} <this host's private key> {}\nsudo install -D -m 644 {own} <the CA certificate> {}\n```",
        t.cert, t.key, t.ca
    )
}

/// The bundle, where each of its files goes, and the mount points in
/// containers.
fn bundle_part(out: &mut String, v: &Validated, paths: &[String]) {
    let _ = write!(
        out,
        "\n## The bundle\n\nThe bundle is a directory: this file and every file in the table below, each at its path under `{BUNDLE_DIR}/`. Put it on the machine you run the steps from; it needs `ssh` and `scp` to every host, and step 4 runs there.\n\n\
         - **From the setup page:** press each file's **Download** and save it at its path under `{BUNDLE_DIR}/` (the download is named with each `/` of the path written `_`).\n\
         - **From the API:** `POST /admin/api/v1/setup/bundle` with the form's fields as a JSON object (`inputs.json` here) answers with `files`, each a `path` and its `content`. With `curl` and `jq`:\n\n\
         ```sh\nmkdir {BUNDLE_DIR} && cd {BUNDLE_DIR}\n\
         curl -s --fail-with-body -u \"admin:<admin password>\" -H 'Content-Type: application/json' --data-binary @../inputs.json \"<PulsusDB URL>/admin/api/v1/setup/bundle\" > ../bundle.json\n\
         jq -r '.files[].path' ../bundle.json | while read -r p; do mkdir -p \"$(dirname \"$p\")\"; jq -j --arg p \"$p\" '.files[] | select(.path == $p) | .content' ../bundle.json > \"$p\"; done\n\
         chmod +x schema/schema.sh\n```\n"
    );
    out.push_str(
        "\n## Where each file goes\n\n| File | Host | Destination | Owner and mode |\n|---|---|---|---|\n",
    );
    for path in paths.iter().map(String::as_str).chain(["setup.md"]) {
        let (host, dest, mode) = destination(v, path);
        let host = host_cell(&host);
        let _ = writeln!(out, "| `{path}` | {host} | `{dest}` | {mode} |");
    }
    let tls = v
        .tls
        .as_ref()
        .map(|t| {
            format!(
                " On both, the certificate at `{}`, the key at `{}` and the CA at `{}`.",
                t.cert, t.key, t.ca
            )
        })
        .unwrap_or_default();
    let _ = writeln!(
        out,
        "\n## In containers\n\nIn Docker or Kubernetes, mount each file read-only at its destination above instead of copying it. ClickHouse server: each file of `nodes/<position>/config.d/` in `{CH_CONFIG_D}` and of `nodes/<position>/users.d/` in `{CH_USERS_D}`, and a volume at `{}` for its data. Keeper: `keepers/<position>/keeper_config.xml` at `{KEEPER_CONFIG}`, and a volume at `{KEEPER_DATA}` for its log and snapshots.{tls} PulsusDB: no file is mounted; pass `--env-file pulsusdb.env` (compose `env_file:`). On Kubernetes, put each host's files in a ConfigMap and the TLS files in a Secret, mount each file with `subPath` at its path, and read the variables of steps 2 and 5 from their Secrets with `envFrom`.",
        v.local_path
    );
}

/// Where one service's variables go: its systemd unit, the root-only file
/// that holds them, the bundle's own env file read beside it, if any, and
/// the Kubernetes Secret's name.
struct EnvTarget<'a> {
    unit: &'a str,
    file: &'a str,
    also: Option<&'a str>,
    secret: &'a str,
    /// Each variable and the placeholder for its value.
    vars: &'a [(&'a str, &'a str)],
}

/// "How to set them": the same variables by systemd, Docker and
/// Kubernetes, each value a placeholder.
fn how_to_set(out: &mut String, t: &EnvTarget) {
    let env_lines: String = t
        .vars
        .iter()
        .map(|(name, value)| format!("{name}={value}\n"))
        .collect();
    let yaml_lines: String = t
        .vars
        .iter()
        .map(|(name, value)| format!("  {name}: \"{value}\"\n"))
        .collect();
    let base = t.file.rsplit('/').next().unwrap_or(t.file);
    let (unit, file, secret) = (t.unit, t.file, t.secret);
    let dir = file.rsplit_once('/').map(|(d, _)| d).unwrap_or("/");
    let _ = write!(
        out,
        "\nHow to set them, with your own values in place of each `<…>`. Keep the values in your secret store, not in the bundle.\n\n\
         **systemd:** a file only root can read, named in an override of the unit, then a restart:\n\n\
         ```sh\nsudo install -d {dir}\nsudo install -m 600 -o root -g root /dev/null {file}\nsudo tee {file} > /dev/null <<'EOF'\n{env_lines}EOF\nsudo systemctl edit {unit}\n```\n\n\
         In the editor `systemctl edit` opens, add:\n\n```ini\n[Service]\n"
    );
    let _ = write!(
        out,
        "EnvironmentFile={file}\n```\n\nThen:\n\n```sh\nsudo systemctl restart {unit}\n```\n\n\
         **Docker:** the same lines in `{base}`, readable only by you (`chmod 600 {base}`), passed with `--env-file`:\n\n```sh\n"
    );
    let env_files: String = t.also.iter().map(|a| format!("--env-file {a} ")).collect();
    let _ = write!(
        out,
        "docker run {env_files}--env-file {base} … # your usual options and image\n```\n\nor, with compose:\n\n```yaml\nservices:\n  {unit}:\n    env_file:\n"
    );
    if let Some(also) = t.also {
        let _ = writeln!(out, "      - {also}");
    }
    let _ = write!(
        out,
        "      - {base}\n```\n\n**Kubernetes:** a Secret, read into the container with `envFrom`:\n\n\
         ```yaml\napiVersion: v1\nkind: Secret\nmetadata:\n  name: {secret}\ntype: Opaque\nstringData:\n{yaml_lines}```\n\n\
         and in the container's spec:\n\n```yaml\nenvFrom:\n"
    );
    if let Some(also) = t.also {
        let name = also.trim_end_matches(".env");
        let _ = write!(
            out,
            "  - configMapRef:\n      name: {name} # kubectl create configmap {name} --from-env-file={also}\n"
        );
    }
    let _ = writeln!(out, "  - secretRef:\n      name: {secret}\n```");
}

fn sql_block(out: &mut String, id: &str, title: &str, sql: &str, expected: &str) {
    let _ = write!(
        out,
        "\n### {id}\n\n{title}\n\n```sql\n{sql}\n```\n\nExpected:\n\n```text\n{expected}```\n"
    );
}

/// `setup.md`: §5.6 with every value filled in. `statements` is the
/// number of statements the schema script sends; `paths` is every other
/// file of the bundle, in its order.
pub(crate) fn setup_md(v: &Validated, statements: usize, paths: &[String]) -> String {
    let c = &v.cluster;
    let d = &v.ctx.db;
    let h = v.hosts.len();
    let n = v.levels.len();
    let p = http_port(v);
    let mut out = String::new();

    let _ = writeln!(
        out,
        "# PulsusDB cluster setup\n\nCluster `{c}`, database `{d}`: {h} ClickHouse hosts in {} shards of {} replicas, {}, TLS {}, object storage {}.",
        v.shards,
        v.replicas,
        keeper_hosts(v),
        if v.tls.is_some() { "on" } else { "off" },
        v.remote_url
            .as_deref()
            .map(|u| format!("at `{u}`"))
            .unwrap_or_else(|| "off".to_string()),
    );

    for w in &v.warnings {
        let _ = writeln!(out, "\n**Warning:** {}", w.message);
    }

    out.push_str("\n## Hosts\n\n| Position | Host | Shard | Files |\n|---|---|---|---|\n");
    for (i, host) in v.hosts.iter().enumerate() {
        let _ = writeln!(
            out,
            "| {} | {host} | {} | `nodes/{}/` |",
            i + 1,
            v.shard_of(i + 1),
            i + 1
        );
    }
    out.push_str("\n| Position | Keeper host | File |\n|---|---|---|\n");
    for (j, keeper) in v.keepers.iter().enumerate() {
        let _ = writeln!(
            out,
            "| {} | {keeper} | `keepers/{}/keeper_config.xml` |",
            j + 1,
            j + 1
        );
    }

    out.push_str("\n## What the schema keeps\n\n");
    for line in total_lines(v) {
        let _ = writeln!(out, "- {line}");
    }

    out.push_str(
        "\n## Sizing\n\nNot written to any file: the server derives these from the host.\n\n| Figure | Value |\n|---|---|\n",
    );
    for (label, value) in sizing(v) {
        let _ = writeln!(out, "| {label} | {value} |");
    }

    bundle_part(&mut out, v, paths);

    out.push_str("\n## Steps\n");
    let own = format!("-o {CH_OWNER} -g {CH_OWNER}");
    let _ = writeln!(
        out,
        "\n### K1\n\nOn each Keeper host, install the ClickHouse Keeper package. 4 GB of memory is generally enough for each Keeper server; keep its data directory on a low-latency disk.{}",
        tls_install(v, "Keeper")
    );
    let _ = writeln!(
        out,
        "\nFrom `{BUNDLE_DIR}/`, put each host's `keeper_config.xml` at `{KEEPER_CONFIG}`, owned by `{CH_OWNER}`; make the data directories its config names, `{KEEPER_LOG}` and `{KEEPER_SNAPSHOTS}`, owned by `{CH_OWNER}`; then start Keeper and enable it at boot:\n\n```sh"
    );
    for (j, kp) in v.keepers.iter().enumerate() {
        let _ = writeln!(
            out,
            "scp keepers/{}/keeper_config.xml {kp}:keeper_config.xml\nssh {kp} 'sudo install -m 640 {own} keeper_config.xml {KEEPER_CONFIG} && sudo install -d -m 750 {own} {KEEPER_LOG} {KEEPER_SNAPSHOTS} && rm keeper_config.xml && sudo systemctl enable --now {KEEPER_UNIT}'",
            j + 1
        );
    }
    out.push_str("```\n");

    let local = &v.local_path;
    let _ = writeln!(
        out,
        "\n### 1\n\nOn each ClickHouse host, install the ClickHouse server package.{}",
        tls_install(v, "ClickHouse")
    );
    let _ = writeln!(
        out,
        "\nFrom `{BUNDLE_DIR}/`, copy each host's `nodes/<position>/config.d/*` into `{CH_CONFIG_D}` and `nodes/<position>/users.d/*` into `{CH_USERS_D}`, owned by `{CH_OWNER}:{CH_OWNER}`, mode 640; and make the local storage path `{local}`, owned by `{CH_OWNER}`:\n\n```sh"
    );
    for (i, host) in v.hosts.iter().enumerate() {
        let _ = writeln!(
            out,
            "scp -r nodes/{} {host}:pulsus-node\nssh {host} 'sudo install -d -m 750 {own} {local} && sudo install -m 640 {own} pulsus-node/config.d/* {CH_CONFIG_D} && sudo install -m 640 {own} pulsus-node/users.d/* {CH_USERS_D} && rm -r pulsus-node'",
            i + 1
        );
    }
    out.push_str("```\n");
    let mut env = vec![
        "`CLICKHOUSE_INTERSERVER_SECRET` (the same value on every host)",
        "`PULSUS_CLICKHOUSE_PASSWORD`",
    ];
    let mut vars = vec![
        ("CLICKHOUSE_INTERSERVER_SECRET", "<inter-server secret>"),
        ("PULSUS_CLICKHOUSE_PASSWORD", "<pulsus password>"),
    ];
    if v.remote_url.is_some() {
        env.push("`PULSUS_S3_ACCESS_KEY_ID`");
        env.push("`PULSUS_S3_SECRET_ACCESS_KEY`");
        vars.push(("PULSUS_S3_ACCESS_KEY_ID", "<S3 access key id>"));
        vars.push(("PULSUS_S3_SECRET_ACCESS_KEY", "<S3 secret access key>"));
    }
    let _ = writeln!(
        out,
        "\n### 2\n\nOn every ClickHouse host, set in the server's environment: {}.",
        env.join(", "),
    );
    how_to_set(
        &mut out,
        &EnvTarget {
            unit: SERVER_UNIT,
            file: "/etc/clickhouse-server/pulsus.env",
            also: None,
            secret: "pulsus-clickhouse",
            vars: &vars,
        },
    );
    out.push_str("\n### 3\n\nRestart the server on every ClickHouse host.\n");

    let on_host_1 = format!("On `{}`:", v.hosts[0]);
    sql_block(
        &mut out,
        "V1",
        &on_host_1,
        &format!("SELECT count() FROM system.clusters WHERE cluster = '{c}'"),
        &format!("{h}\n"),
    );
    let mut macros: Vec<(String, String)> = Vec::new();
    for (i, host) in v.hosts.iter().enumerate() {
        macros.push(("replica".to_string(), host.clone()));
        macros.push(("shard".to_string(), v.shard_of(i + 1).to_string()));
    }
    macros.sort();
    let rows: String = macros.iter().map(|(m, s)| format!("{m}\t{s}\n")).collect();
    sql_block(
        &mut out,
        "V2",
        &on_host_1,
        &format!(
            "SELECT macro, substitution FROM clusterAllReplicas('{c}', system.macros) WHERE macro IN ('shard', 'replica') ORDER BY macro, substitution"
        ),
        &rows,
    );
    sql_block(
        &mut out,
        "V0",
        &format!(
            "{on_host_1} anything but `0` means database `{d}` exists, and step 4 drops it: stop."
        ),
        &format!(
            "SELECT count() FROM clusterAllReplicas('{c}', system.databases) WHERE name = '{d}'"
        ),
        "0\n",
    );

    let tls_env = v
        .tls
        .as_ref()
        .map(|t| format!(" CLICKHOUSE_PROTO=https CLICKHOUSE_TLS_CA_FILE={}", t.ca))
        .unwrap_or_default();
    let _ = write!(
        out,
        "\n### 4\n\nSet `PULSUS_CLICKHOUSE_PASSWORD` in the shell from your secret store, as on the servers in step 2: the command stops without it. In the bundle directory, `{BUNDLE_DIR}/` (see *The bundle* above), on a machine with `sh`, `awk`, `cat`, `curl`, `cut`, `dirname`, `grep`, `mktemp`, `rm`, `sed`, `seq` and `sleep`{}:\n\n```sh\n: \"${{PULSUS_CLICKHOUSE_PASSWORD:?set it in this shell from your secret store: it is the pulsus user password of step 2}}\"\nset -a; . ./schema.env; set +a\nCLICKHOUSE_SERVER={} CLICKHOUSE_HTTP_PORT={p} CLICKHOUSE_AUTH=\"pulsus:$PULSUS_CLICKHOUSE_PASSWORD\"{tls_env} sh schema/schema.sh\n```\n\nExpected:\n\n```text\nschema.sh: all {h} hosts in cluster '{c}' answered\nschema.sh: {statements} statements applied to {d}\n```\n",
        v.tls
            .as_ref()
            .map(|t| format!(", and the CA at `{}`", t.ca))
            .unwrap_or_default(),
        v.hosts[0],
    );
    sql_block(
        &mut out,
        "V3",
        &on_host_1,
        &format!(
            "SELECT count() FROM clusterAllReplicas('{c}', system.databases) WHERE name = '{d}' AND comment = 'retention_days={}'",
            v.raw_local_days + v.raw_remote_days
        ),
        &format!("{h}\n"),
    );
    sql_block(
        &mut out,
        "V4",
        &on_host_1,
        &format!(
            "SELECT count() FROM clusterAllReplicas('{c}', system.tables) WHERE database = '{d}' AND startsWith(comment, 'rollup_res_ms=') AND engine != 'Distributed'"
        ),
        &format!("{}\n", h * n),
    );

    let _ = write!(
        out,
        "\n### 5\n\nOn the PulsusDB host{}, start PulsusDB with the variables of `pulsusdb.env` and, from your secret store, `CLICKHOUSE_AUTH=pulsus:<password>` and `PULSUS_ADMIN_PASSWORD`. The admin pair is `admin` / `admin` until you set it: set `PULSUS_ADMIN_PASSWORD`, and `PULSUS_ADMIN_USER` for another user.\n\nOn a packaged install the `{PULSUSDB_UNIT}` unit reads `{PULSUSDB_ENV}`: from `{BUNDLE_DIR}/`, put the bundle's `pulsusdb.env` there{}:\n\n```sh\nsudo install -m 640 -o {} -g {} pulsusdb.env {PULSUSDB_ENV}\n{}```\n\nIn a container, pass it with `--env-file pulsusdb.env` (compose `env_file:`, Kubernetes a ConfigMap read with `envFrom`), as below.\n",
        v.tls
            .as_ref()
            .map(|t| format!(", put the CA at `{}`", t.ca))
            .unwrap_or_default(),
        if v.tls.is_some() {
            ", and the CA where it is named, readable by every user"
        } else {
            ""
        },
        PULSUSDB_ENV_OWNER.split(':').next().unwrap_or("root"),
        PULSUSDB_ENV_OWNER.split(':').nth(1).unwrap_or("pulsusdb"),
        v.tls
            .as_ref()
            .map(|t| format!("sudo install -D -m 644 <the CA certificate> {}\n", t.ca))
            .unwrap_or_default(),
    );
    how_to_set(
        &mut out,
        &EnvTarget {
            unit: PULSUSDB_UNIT,
            file: "/etc/pulsusdb/secrets.env",
            also: Some("pulsusdb.env"),
            secret: "pulsusdb",
            vars: &[
                ("CLICKHOUSE_AUTH", "pulsus:<pulsus password>"),
                ("PULSUS_ADMIN_PASSWORD", "<admin password>"),
            ],
        },
    );
    out.push_str("\nThen `GET /ready`.\n\nExpected: `200`.\n");

    let _ = write!(
        out,
        "\n### V5\n\nWith `PULSUS_URL` set to PulsusDB's address (for example `http://pulsusdb:3100`) and the admin pair in the environment (`admin` / `admin` when unset), run this with `sh`. It waits for PulsusDB to answer with its list of {n} levels: discovery runs at start and then every 60 s, and the list is `null` until the first one.\n\n```sh\n# V5 begin\ntries=${{V5_TRIES:-24}}; pause=${{V5_PAUSE:-5}}; i=0\nwhile [ \"$i\" -lt \"$tries\" ]; do\n  if body=$(curl -s --fail-with-body -u \"${{PULSUS_ADMIN_USER:-admin}}:${{PULSUS_ADMIN_PASSWORD:-admin}}\" \"$PULSUS_URL/admin/api/v1/database\"); then\n    case \"$body\" in\n      *'\"levels\":['*) got=$(printf '%s' \"$body\" | grep -o '\"table\":' | wc -l | tr -d ' ') ;;\n      *) got=null ;;\n    esac\n  else\n    got=\"an error\"\n  fi\n  if [ \"$got\" = {n} ]; then echo \"V5: {n} levels\"; exit 0; fi\n  i=$((i + 1)); sleep \"$pause\"\ndone\necho \"V5: got $got, want {n} levels\" >&2; exit 1\n# V5 end\n```\n\nExpected: exit 0, printing `V5: {n} levels`.\n"
    );
    out
}
