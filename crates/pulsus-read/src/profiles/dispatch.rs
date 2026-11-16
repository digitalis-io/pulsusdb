//! The profiles read path's single route to ClickHouse (issue #821), on the
//! shape of `traces/dispatch.rs`: [`ProfileDispatch`] owns the only
//! [`ChClient`] this path can reach, and its field is private to this
//! module, so every statement passes the three steps below.

use futures::StreamExt;
use pulsus_clickhouse::{ChClient, ChRow, QuerySettings};

use crate::logql::exec::escape_query_placeholders;
use crate::traces::tenant_scope::TenantTerm;

use super::engine::{ProfilesReadError, map_read_error};

/// The profiles read path's ClickHouse handle, one per engine.
pub(super) struct ProfileDispatch {
    client: ChClient,
    /// The request's tenant term, which every read of a profile table must
    /// carry.
    term: TenantTerm,
    /// The profile tables [`Self::term`] is checked on.
    tables: Vec<String>,
}

impl ProfileDispatch {
    pub(super) fn new(client: ChClient, term: TenantTerm, tables: Vec<String>) -> Self {
        Self {
            client,
            term,
            tables,
        }
    }

    /// The first server failure this dispatcher's reads met.
    pub(super) fn server_failure(&self) -> Option<pulsus_clickhouse::ServerFailure> {
        self.client.server_failure()
    }

    /// Executes one rendered statement and collects its rows. Here and
    /// nowhere else: every literal `?` is doubled, the query-text guard
    /// runs on the final text, and a statement reading a profile table
    /// without the tenant term is refused and never sent.
    pub(super) async fn rows<R: ChRow>(
        &self,
        sql: &str,
        settings: &QuerySettings,
    ) -> Result<Vec<R>, ProfilesReadError> {
        let sql = escape_query_placeholders(sql);
        crate::querytext::ensure_query_text_fits(&sql)
            .map_err(|_| ProfilesReadError::TooBroad("query_too_broad: query text".to_string()))?;
        super::tenant_scope::check(&sql, &self.term, &self.tables)
            .map_err(ProfilesReadError::Internal)?;
        let mut stream = self
            .client
            .query_stream::<R>(&sql, settings)
            .await
            .map_err(map_read_error)?;
        let mut out = Vec::new();
        while let Some(row) = stream.next().await {
            out.push(row.map_err(map_read_error)?);
        }
        Ok(out)
    }
}
