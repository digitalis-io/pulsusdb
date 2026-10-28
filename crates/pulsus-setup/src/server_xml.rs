//! The ClickHouse and Keeper configuration files (§5.4). Every value placed
//! in XML goes through [`xml_text`].

use std::fmt::Write;

use crate::Validated;

/// ClickHouse's ports, from the shipped `config.xml`.
pub(crate) const TCP_PORT: u16 = 9000;
pub(crate) const TCP_PORT_SECURE: u16 = 9440;
pub(crate) const HTTP_PORT: u16 = 8123;
pub(crate) const HTTPS_PORT: u16 = 8443;
/// Keeper's client and raft ports.
const KEEPER_PORT: u16 = 9181;
const RAFT_PORT: u16 = 9234;

/// `s` as XML text: `&`, `<`, `>`, `"` and `'` escaped.
pub(crate) fn xml_text(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            other => out.push(other),
        }
    }
    out
}

const LISTEN: &str = "    <listen_host>::</listen_host>
    <listen_host>0.0.0.0</listen_host>
    <listen_try>1</listen_try>
";

/// `nodes/<i>/config.d/pulsus-cluster.xml`, the same on every host.
pub(crate) fn cluster(v: &Validated) -> String {
    let tls = v.tls.is_some();
    let name = xml_text(&v.cluster);
    let mut out = String::from("<clickhouse>\n");
    out.push_str(LISTEN);
    let _ = writeln!(out, "    <remote_servers>\n        <{name}>");
    out.push_str("            <secret from_env=\"CLICKHOUSE_INTERSERVER_SECRET\"/>\n");
    for shard in v.hosts.chunks(v.replicas as usize) {
        out.push_str(
            "            <shard>\n                <internal_replication>true</internal_replication>\n",
        );
        for host in shard {
            let _ = writeln!(
                out,
                "                <replica>\n                    <host>{}</host>\n                    <port>{}</port>",
                xml_text(host),
                if tls { TCP_PORT_SECURE } else { TCP_PORT }
            );
            if tls {
                out.push_str("                    <secure>1</secure>\n");
            }
            out.push_str("                </replica>\n");
        }
        out.push_str("            </shard>\n");
    }
    let _ = writeln!(out, "        </{name}>\n    </remote_servers>");
    out.push_str("    <zookeeper>\n");
    for keeper in &v.keepers {
        let _ = writeln!(
            out,
            "        <node>\n            <host>{}</host>\n            <port>{KEEPER_PORT}</port>",
            xml_text(keeper)
        );
        if tls {
            out.push_str("            <secure>1</secure>\n");
        }
        out.push_str("        </node>\n");
    }
    out.push_str("    </zookeeper>\n");
    out.push_str(
        "    <interserver_http_credentials>
        <user>interserver</user>
        <password from_env=\"CLICKHOUSE_INTERSERVER_SECRET\"/>
    </interserver_http_credentials>
</clickhouse>
",
    );
    out
}

/// `nodes/<i>/config.d/pulsus-node.xml` for host `i` (from 1).
pub(crate) fn node(v: &Validated, i: usize) -> String {
    let host = xml_text(&v.hosts[i - 1]);
    format!(
        "<clickhouse>
    <macros>
        <shard>{}</shard>
        <replica>{host}</replica>
        <cluster>{}</cluster>
    </macros>
    <interserver_http_host>{host}</interserver_http_host>
</clickhouse>
",
        v.shard_of(i),
        xml_text(&v.cluster)
    )
}

/// `nodes/<i>/config.d/pulsus-storage.xml`, the same on every host.
pub(crate) fn storage(v: &Validated) -> String {
    let mut out = format!(
        "<clickhouse>\n    <path>{}</path>\n",
        xml_text(&v.local_path)
    );
    if let Some(url) = &v.remote_url {
        // `{shard}` and `{replica}` are left for the server to fill in.
        let _ = write!(
            out,
            "    <storage_configuration>
        <disks>
            <pulsus_remote>
                <type>s3</type>
                <endpoint>{}{{shard}}/{{replica}}/</endpoint>
                <access_key_id from_env=\"PULSUS_S3_ACCESS_KEY_ID\"/>
                <secret_access_key from_env=\"PULSUS_S3_SECRET_ACCESS_KEY\"/>
            </pulsus_remote>
        </disks>
        <policies>
            <pulsus_tiered>
                <volumes>
                    <local><disk>default</disk></local>
                    <cold><disk>pulsus_remote</disk></cold>
                </volumes>
                <move_factor>0</move_factor>
            </pulsus_tiered>
        </policies>
    </storage_configuration>
",
            xml_text(url)
        );
    }
    out.push_str("</clickhouse>\n");
    out
}

/// The `openSSL` block, for ClickHouse and Keeper alike.
fn open_ssl(v: &Validated) -> String {
    let Some(tls) = &v.tls else {
        return String::new();
    };
    format!(
        "    <openSSL>
        <server>
            <certificateFile>{}</certificateFile>
            <privateKeyFile>{}</privateKeyFile>
            <caConfig>{}</caConfig>
            <verificationMode>none</verificationMode>
        </server>
        <client>
            <caConfig>{}</caConfig>
            <verificationMode>relaxed</verificationMode>
        </client>
    </openSSL>
",
        xml_text(&tls.cert),
        xml_text(&tls.key),
        xml_text(&tls.ca),
        xml_text(&tls.ca)
    )
}

/// `nodes/<i>/config.d/pulsus-tls.xml`: every plaintext listener closed,
/// the TLS ones open. `None` without TLS.
pub(crate) fn tls(v: &Validated) -> Option<String> {
    v.tls.as_ref()?;
    Some(format!(
        "<clickhouse>
    <http_port remove=\"1\"/>
    <tcp_port remove=\"1\"/>
    <interserver_http_port remove=\"1\"/>
    <mysql_port remove=\"1\"/>
    <postgresql_port remove=\"1\"/>
    <https_port>{HTTPS_PORT}</https_port>
    <tcp_port_secure>{TCP_PORT_SECURE}</tcp_port_secure>
    <interserver_https_port>9010</interserver_https_port>
{}</clickhouse>
",
        open_ssl(v)
    ))
}

/// `nodes/<i>/users.d/pulsus-user.xml`: the user PulsusDB and the schema
/// script connect as.
pub(crate) fn user() -> String {
    "<clickhouse>
    <users>
        <pulsus>
            <password from_env=\"PULSUS_CLICKHOUSE_PASSWORD\"/>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
        </pulsus>
    </users>
</clickhouse>
"
    .to_string()
}

/// `keepers/<j>/keeper_config.xml` for Keeper host `j` (from 1): the whole
/// file, replacing the package's.
pub(crate) fn keeper(v: &Validated, j: usize) -> String {
    let tls = v.tls.is_some();
    let mut out = String::from("<clickhouse>\n");
    out.push_str(LISTEN);
    out.push_str("    <keeper_server>\n");
    let port_key = if tls { "tcp_port_secure" } else { "tcp_port" };
    let _ = writeln!(out, "        <{port_key}>{KEEPER_PORT}</{port_key}>");
    let _ = writeln!(out, "        <server_id>{j}</server_id>");
    out.push_str(
        "        <log_storage_path>/var/lib/clickhouse-keeper/coordination/log</log_storage_path>
        <snapshot_storage_path>/var/lib/clickhouse-keeper/coordination/snapshots</snapshot_storage_path>
        <raft_configuration>
",
    );
    if tls {
        out.push_str("            <secure>true</secure>\n");
    }
    for (n, keeper) in v.keepers.iter().enumerate() {
        let _ = writeln!(
            out,
            "            <server>
                <id>{}</id>
                <hostname>{}</hostname>
                <port>{RAFT_PORT}</port>
            </server>",
            n + 1,
            xml_text(keeper)
        );
    }
    out.push_str("        </raft_configuration>\n    </keeper_server>\n");
    out.push_str(&open_ssl(v));
    out.push_str("</clickhouse>\n");
    out
}
