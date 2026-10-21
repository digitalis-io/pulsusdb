//! The service-narrowed values read on the bucket catalogue (issue #601
//! part 4b): the window's whole 5-minute buckets from `tag_bucket_values`,
//! its partial first and last bucket from today's narrowed read.

use futures::StreamExt;
use pulsus_clickhouse::ChError;

use super::{
    PredicateCtx, TAG_VALUES_MAX, TagNarrowing, TagValue, TagValues, TagValuesRequest, TraceEngine,
    WindowSql, tag_read_error, tag_store_settings, tag_values_statement,
};
use crate::logql::error::ReadError;
use crate::traces::rows::CatalogValuesRow;
use crate::traces::spans::tags::catalogue_values_sql;

impl TraceEngine {
    /// The service-narrowed values of span-scoped `key` over `req`'s
    /// window (issue #601 part 4b): its whole 5-minute buckets from
    /// `tag_bucket_values`, its partial first and last bucket from today's
    /// narrowed read with that edge as its window, in one statement.
    /// `None` when the window holds no whole bucket, or when a whole bucket
    /// is overflowed: the caller then runs today's statement.
    pub(super) async fn catalogue_tag_values(
        &self,
        service: &str,
        key: &str,
        req: TagValuesRequest<'_>,
    ) -> Result<Option<TagValues>, ReadError> {
        const BUCKET_NS: i64 = 300_000_000_000;
        let (start_ns, end_ns) = (req.start_ns, req.end_ns);
        let lo = start_ns.div_euclid(BUCKET_NS) + i64::from(start_ns.rem_euclid(BUCKET_NS) != 0);
        let hi = end_ns.div_euclid(BUCKET_NS) - 1;
        if lo > hi {
            return Ok(None);
        }
        let mut edges = Vec::new();
        for (s, e) in [(start_ns, lo * BUCKET_NS), ((hi + 1) * BUCKET_NS, end_ns)] {
            if s < e {
                let w = WindowSql::start_closed_end_open(s, e);
                let ctx = PredicateCtx {
                    window: w,
                    spans_table: &self.config.spans_v2_table,
                    resources_table: &self.config.resources_table,
                    indexed: &[],
                };
                let edge_n = req.narrowing(&ctx);
                edges.push(tag_values_statement(
                    &self.config,
                    self.tag_read_rows,
                    Some("span"),
                    key,
                    w,
                    &edge_n,
                ));
            }
        }
        let sql = catalogue_values_sql(service, key, lo, hi, &edges, TAG_VALUES_MAX + 1);
        let n = TagNarrowing::default();
        let mut stream = self
            .dispatch
            .query_stream::<CatalogValuesRow, _>(&sql, &tag_store_settings(&self.config), |e| {
                tag_read_error(e, &n, &self.config)
            })
            .await?;
        let mut row = None;
        while let Some(next) = stream.next().await {
            row = Some(next.map_err(|e| tag_read_error(e, &n, &self.config))?);
        }
        let row = row.ok_or_else(|| {
            ReadError::Clickhouse(ChError::Decode(
                "the catalogue values statement returned no row; it returns exactly one"
                    .to_string(),
            ))
        })?;
        if row.overflowed != 0 {
            return Ok(None);
        }
        let mut values: Vec<TagValue> = row
            .vals
            .into_iter()
            .map(|(val, val_type)| TagValue { val, val_type })
            .collect();
        let truncated = values.len() > TAG_VALUES_MAX;
        values.truncate(TAG_VALUES_MAX);
        Ok(Some(TagValues { values, truncated }))
    }
}
