//! Result-row shapes for the trace SEARCH path, deserialized straight off
//! `ChClient::query_stream` (`pulsus_clickhouse::Row` derive, RowBinary —
//! same convention as [`crate::logql::rows`]).
//!
//! **The trace FETCH's rows are not here** (issue #587): they are
//! [`crate::traces::spans::rows`]'s, because the fetch reads three
//! different tables through three different statements. Every `kind` and
//! `status_code` in this file is `i8` and must stay so — they decode the
//! old span table, whose columns are `Int8`, and they move when #602
//! moves search off it.

use pulsus_clickhouse::Row;
use serde::{Deserialize, Serialize};

/// One service-graph edge row ([`crate::traces::spans::graph::spans_graph_sql`],
/// issue #599): the aggregated `(client, server, conn_type)` edge with its
/// call count, failed-instance count, and TDigest latency quantiles.
/// `quantiles_ns` is the SQL-pinned `Array(Float64)` (`CAST(quantilesTDigest
/// (...) AS Array(Float64))`, issue #173 Fix 2 — no f32 on the decode path),
/// ordered `[p50, p95, p99]`. Field order matches the SELECT list exactly
/// (RowBinary is positional).
#[derive(Debug, Clone, PartialEq, Row, Serialize, Deserialize)]
pub struct GraphEdgeRow {
    pub client: String,
    pub server: String,
    pub conn_type: String,
    pub calls: u64,
    pub failed: u64,
    pub quantiles_ns: Vec<f64>,
}

/// The routing table's definition, the one column of
/// [`crate::traces::spans::graph::graph_target_sql`] (issue #599).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct GraphTargetRow {
    pub engine_full: String,
}

/// One `tag_names` row of the §4.3 tag-names read
/// (`spans::tags::tag_names_sql` — `SELECT DISTINCT scope, key`, issue #58).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct TagNameRow {
    pub scope: String,
    pub key: String,
}

/// One `spans` row of the §4.3 span-NAME values read
/// (the span-name statement — `SELECT DISTINCT <capped name> AS
/// val`, issue #478). One column: the type is not read from the store,
/// because a span name is a `String` column and is always reported
/// `string`.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct SpanNameRow {
    pub val: String,
}

/// The one row of the service-narrowed values read on the bucket catalogue
/// (`tags::catalogue_values_sql`, issue #601 part 4b): whether an interior
/// group the request reads is overflowed, and the pairs in today's order.
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct CatalogValuesRow {
    pub overflowed: u8,
    pub vals: Vec<(String, String)>,
}

/// One `tag_values` row of the §4.3 tag-values read
/// (`spans::tags::tag_values_sql` — `SELECT DISTINCT val, val_type`, issues
/// #58 and #476). Field order matches the SELECT list exactly (RowBinary
/// is positional).
#[derive(Debug, Clone, PartialEq, Eq, Row, Serialize, Deserialize)]
pub struct TagValueRow {
    pub val: String,
    /// The stored OTLP type (`string`/`int`/`float`/`bool`), which the
    /// writer always sets (`TraceJsonScalar::val_type`). The renderer
    /// reports it verbatim.
    pub val_type: String,
}
