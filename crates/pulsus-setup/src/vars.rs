//! `schema.env` and `pulsusdb.env`: `NAME=value` lines.

use crate::Validated;
use crate::server_xml::{HTTP_PORT, HTTPS_PORT};

/// The HTTP port PulsusDB and the schema script reach ClickHouse on.
pub(crate) fn http_port(v: &Validated) -> u16 {
    if v.tls.is_some() {
        HTTPS_PORT
    } else {
        HTTP_PORT
    }
}

/// `schema.env`: the §5.3 variables, sorted by name.
pub(crate) fn schema_env(v: &Validated) -> String {
    v.vars
        .iter()
        .map(|(k, val)| format!("{k}={val}\n"))
        .collect()
}

/// `pulsusdb.env`: how PulsusDB reaches the cluster. No secret.
pub(crate) fn pulsusdb_env(v: &Validated) -> String {
    let port = http_port(v);
    let servers: Vec<String> = v.hosts.iter().map(|h| format!("{h}:{port}")).collect();
    let mut out = format!(
        "CLICKHOUSE_SERVERS={}\nCLICKHOUSE_HTTP_PORT={port}\nCLICKHOUSE_PROTO={}\n",
        servers.join(","),
        if v.tls.is_some() { "https" } else { "http" }
    );
    if let Some(tls) = &v.tls {
        out.push_str(&format!("CLICKHOUSE_TLS_CA_FILE={}\n", tls.ca));
    }
    out.push_str(&format!(
        "CLICKHOUSE_DB={}\nPULSUS_CLUSTER={}\n",
        v.ctx.db, v.cluster
    ));
    if v.remote_url.is_some() {
        out.push_str("PULSUS_STORAGE_POLICY=pulsus_tiered\n");
    }
    out
}
