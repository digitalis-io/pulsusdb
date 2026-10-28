//! `setup.md` (§5.6), the lines for what the schema keeps (§5.1), and the
//! sizing the server derives (§5.5).

use std::fmt::Write;

use crate::Validated;
use crate::vars::http_port;

/// The ceiling of every due time (docs/configuration.md §7).
pub(crate) const CEILING: &str = "no row is due after 2106-02-07 06:28:15 UTC";

/// Raw data's line, each level's, then the ceiling.
pub(crate) fn total_lines(v: &Validated) -> Vec<String> {
    let mut lines = vec![format!(
        "Raw data: kept at least {} days; at least {} on local disk",
        v.raw_local_days + v.raw_remote_days,
        v.raw_local_days
    )];
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

fn sql_block(out: &mut String, id: &str, title: &str, sql: &str, expected: &str) {
    let _ = write!(
        out,
        "\n### {id}\n\n{title}\n\n```sql\n{sql}\n```\n\nExpected:\n\n```text\n{expected}```\n"
    );
}

/// `setup.md`: §5.6 with every value filled in. `statements` is the
/// number of statements the schema script sends.
pub(crate) fn setup_md(v: &Validated, statements: usize) -> String {
    let c = &v.cluster;
    let d = &v.ctx.db;
    let h = v.hosts.len();
    let n = v.levels.len();
    let p = http_port(v);
    let mut out = String::new();

    let _ = writeln!(
        out,
        "# PulsusDB cluster setup\n\nCluster `{c}`, database `{d}`: {h} ClickHouse hosts in {} shards of {} replicas, {} Keeper hosts, TLS {}, object storage {}.",
        v.shards,
        v.replicas,
        v.keepers.len(),
        if v.tls.is_some() { "on" } else { "off" },
        v.remote_url
            .as_deref()
            .map(|u| format!("at `{u}`"))
            .unwrap_or_else(|| "off".to_string()),
    );

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

    out.push_str("\n## Steps\n");
    let tls_files = v
        .tls
        .as_ref()
        .map(|t| {
            format!(
                "the certificate at `{}`, the key at `{}` and the CA at `{}`",
                t.cert, t.key, t.ca
            )
        })
        .unwrap_or_default();

    let _ = writeln!(
        out,
        "\n### K1\n\nOn each Keeper host, install ClickHouse Keeper, replace its `keeper_config.xml` with `keepers/<position>/keeper_config.xml`{}, and start it. 4 GB of memory is generally enough for each Keeper server; keep its data directory on a low-latency disk.",
        if v.tls.is_some() {
            format!(", put {tls_files}")
        } else {
            String::new()
        }
    );
    out.push_str(
        "\n### 1\n\nOn each ClickHouse host, copy `nodes/<position>/config.d/*` into the server's `config.d/` directory and `nodes/<position>/users.d/*` into its `users.d/` directory.\n",
    );
    let mut env = vec![
        "`CLICKHOUSE_INTERSERVER_SECRET` (the same value on every host)",
        "`PULSUS_CLICKHOUSE_PASSWORD`",
    ];
    if v.remote_url.is_some() {
        env.push("`PULSUS_S3_ACCESS_KEY_ID`");
        env.push("`PULSUS_S3_SECRET_ACCESS_KEY`");
    }
    let _ = writeln!(
        out,
        "\n### 2\n\nOn every ClickHouse host, set in the server's environment: {}.{}",
        env.join(", "),
        if v.tls.is_some() {
            format!(" Put {tls_files}.")
        } else {
            String::new()
        }
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
        "\n### 4\n\nIn the bundle's directory, on a machine with `sh`, `awk`, `cat`, `curl`, `cut`, `dirname`, `grep`, `mktemp`, `rm`, `sed`, `seq` and `sleep`{}:\n\n```sh\nset -a; . ./schema.env; set +a\nCLICKHOUSE_SERVER={} CLICKHOUSE_HTTP_PORT={p} CLICKHOUSE_AUTH=\"pulsus:$PULSUS_CLICKHOUSE_PASSWORD\"{tls_env} sh schema/schema.sh\n```\n\nExpected:\n\n```text\nschema.sh: all {h} hosts in cluster '{c}' answered\nschema.sh: {statements} statements applied to {d}\n```\n",
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
        "\n### 5\n\nOn the PulsusDB host{}, start PulsusDB with the variables of `pulsusdb.env` and, from your secret store, `CLICKHOUSE_AUTH=pulsus:<password>`, `PULSUS_ADMIN_USER` and `PULSUS_ADMIN_PASSWORD`. Then `GET /ready`.\n\nExpected: `200`.\n",
        v.tls
            .as_ref()
            .map(|t| format!(", put the CA at `{}`", t.ca))
            .unwrap_or_default()
    );

    let _ = write!(
        out,
        "\n### V5\n\nWith `PULSUS_URL` set to PulsusDB's address (for example `http://pulsusdb:3100`) and the admin pair in the environment, run this with `sh`. It waits for PulsusDB to find the {n} levels: discovery runs at start and then every 60 s.\n\n```sh\n# V5 begin\ntries=${{V5_TRIES:-24}}; pause=${{V5_PAUSE:-5}}; i=0\nwhile [ \"$i\" -lt \"$tries\" ]; do\n  got=$(curl -s --fail-with-body -u \"$PULSUS_ADMIN_USER:$PULSUS_ADMIN_PASSWORD\" \"$PULSUS_URL/admin/api/v1/database\" | grep -o '\"table\":' | wc -l)\n  if [ \"$got\" -eq {n} ]; then echo \"V5: {n} levels\"; exit 0; fi\n  i=$((i + 1)); sleep \"$pause\"\ndone\necho \"V5: $got levels, not {n}\" >&2; exit 1\n# V5 end\n```\n\nExpected: exit 0, printing `V5: {n} levels`.\n"
    );
    out
}
