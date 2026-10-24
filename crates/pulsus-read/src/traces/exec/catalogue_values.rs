//! The service-narrowed values reads on the bucket catalogues: a span key's
//! (issue #601 part 4b), the window's whole 5-minute buckets from
//! `tag_bucket_values` and its partial first and last bucket from today's
//! narrowed read; and a resource key's (part 4c), the window's whole
//! buckets from `service_bucket_resources` and its partial ones from
//! `spans`.

use futures::StreamExt;
use pulsus_clickhouse::ChError;

use super::{
    PredicateCtx, TAG_VALUES_MAX, TagNarrowing, TagValue, TagValues, TagValuesRequest, TraceEngine,
    WindowSql, tag_read_error, tag_store_settings, tag_values_statement,
};
use crate::logql::error::ReadError;
use crate::traces::rows::CatalogValuesRow;
use crate::traces::spans::tags::{
    ARRAY_SERVICE_MARK, catalogue_buckets, catalogue_resource_values_sql, catalogue_values_sql,
    resource_edge_sql,
};

impl TraceEngine {
    /// The service-narrowed values read of `scope.key` on its bucket
    /// catalogue (issue #601 parts 4b and 4c): a span key's, `None` when a
    /// whole bucket is overflowed; a resource key's other than
    /// `service.name`; `None` for any other key, which the caller reads as
    /// today.
    pub(super) async fn catalogue_values(
        &self,
        scope: Option<&str>,
        service: &str,
        key: &str,
        req: TagValuesRequest<'_>,
    ) -> Result<Option<TagValues>, ReadError> {
        match scope {
            Some("span") => self.catalogue_tag_values(service, key, req).await,
            Some("resource") if key != "service.name" => {
                self.catalogue_resource_values(service, key, req).await
            }
            _ => Ok(None),
        }
    }

    /// The service-narrowed values of span-scoped `key` over `req`'s
    /// window (issue #601 part 4b): its whole 5-minute buckets from
    /// `tag_bucket_values`, its partial first and last bucket from today's
    /// narrowed read with that edge as its window, in one statement.
    /// `None` when a whole bucket is overflowed: the caller then runs
    /// today's statement.
    pub(super) async fn catalogue_tag_values(
        &self,
        service: &str,
        key: &str,
        req: TagValuesRequest<'_>,
    ) -> Result<Option<TagValues>, ReadError> {
        let (lo, hi, buckets) = catalogue_buckets(req.start_ns, req.end_ns);
        let mut edges = Vec::new();
        for (s, e) in buckets {
            let w = WindowSql::start_closed_end_open(s, e);
            let ctx = PredicateCtx {
                window: w,
                spans_table: &self.config.spans_table,
                resources_table: &self.config.resources_table,
                indexed: &[],
                tenant: &self.tenant,
            };
            let edge_n = req.narrowing(&ctx);
            edges.push(tag_values_statement(
                &self.tenant,
                &self.config,
                self.tag_read_rows,
                Some("span"),
                key,
                w,
                &edge_n,
            ));
        }
        let sql = catalogue_values_sql(
            &self.tenant,
            service,
            key,
            lo,
            hi,
            &edges,
            TAG_VALUES_MAX + 1,
            &self.config.resources_table,
            &req.window().resources_day_clause(),
        );
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

    /// The service-narrowed values of resource-scoped `key` over `req`'s
    /// whole window (issue #601 part 4c): the resources `service`'s spans
    /// used in its whole 5-minute buckets from `service_bucket_resources`,
    /// and in its partial first and last bucket from `spans`, in one
    /// statement with no cut.
    ///
    /// `None` where a resource in the window's days holds `service.name` as
    /// a string array containing `service` (issue #493): the catalogue keys
    /// a string `service` only, so the caller runs today's statement.
    pub(super) async fn catalogue_resource_values(
        &self,
        service: &str,
        key: &str,
        req: TagValuesRequest<'_>,
    ) -> Result<Option<TagValues>, ReadError> {
        let (lo, hi, buckets) = catalogue_buckets(req.start_ns, req.end_ns);
        let mut edges = Vec::new();
        for (s, e) in buckets {
            let w = WindowSql::start_closed_end_open(s, e);
            let ctx = PredicateCtx {
                window: w,
                spans_table: &self.config.spans_table,
                resources_table: &self.config.resources_table,
                indexed: &[],
                tenant: &self.tenant,
            };
            let edge_n = req.narrowing(&ctx);
            edges.push(resource_edge_sql(
                &self.tenant,
                &self.config.spans_table,
                w,
                edge_n.predicate(),
            ));
        }
        let sql = catalogue_resource_values_sql(
            &self.tenant,
            service,
            key,
            lo,
            hi,
            &edges,
            TAG_VALUES_MAX + 1,
            &self.config.resources_table,
            &req.window().resources_day_clause(),
        );
        let values = self
            .stream_tag_values(
                &sql,
                &tag_store_settings(&self.config),
                &TagNarrowing::default(),
            )
            .await?;
        if values
            .values
            .first()
            .is_some_and(|v| v.val_type == ARRAY_SERVICE_MARK)
        {
            return Ok(None);
        }
        Ok(Some(values))
    }
}
