//! The lowered `| unwrap` reducers' fold (issue #624, part 4): the rows of
//! [`super::sql::metric_range_reduced`], one per `(key, grid point)`
//! window, become the answer's series.
//!
//! ```text
//! row (c, kv, e, bucket_ns, n, r, t1, t2, v1)
//!   -> key labels  the class's labels, the key's metadata, x = "0" (or "x" when e = 1)
//!   -> the stages  once per key, today's route's pipeline less the filters the
//!                  statement applied, under its grouping and step rules
//!        dropped                         -> the key's rows are skipped
//!        kept with an error, or e = 1    -> today's route
//!        kept, labels another key has    -> today's route
//!        kept                            -> the point (bucket_ns, r), finished per reducer
//! ```

use std::collections::HashMap;

use pulsus_model::Fingerprint;

use super::agg::LabelSet;
use super::charge::AggCaps;
use super::error::ReadError;
use super::exec::MatrixSeries;
use super::rows::MetricReducedRow;
use super::sql::ReducedUnwrap;

/// The grid and divisors one reduced fold emits on (issue #624, part 4).
#[derive(Debug, Clone, Copy)]
pub(in crate::logql) struct ReducedGrid {
    pub grid_start_ns: i64,
    pub end_ns: i64,
    pub step_ns: i64,
    pub grid_points: u64,
    /// The `[range]` width, which `rate_counter` extrapolates over.
    pub range_ns: i64,
    /// The divisor `rate` and `rate_counter` take at the range step: `None`
    /// for a `rate` under a parent `sum`, which divides after the sum.
    pub rate_window_ns: Option<u64>,
}

/// What one row did to the fold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::logql) enum ReducedPush {
    /// Folded, or skipped.
    Folded,
    /// The fold cannot answer: today's route runs.
    Stop,
}

/// The lowered `| unwrap` fold (issue #624, part 4).
pub(in crate::logql) struct ReducedFold {
    grid: ReducedGrid,
}

impl ReducedFold {
    /// `class_labels` maps each class id (a fingerprint when every stream is
    /// its own class) to the labels its keys start from.
    pub(in crate::logql) fn new(
        value: &ReducedUnwrap,
        class_labels: HashMap<Fingerprint, LabelSet>,
        caps: AggCaps,
        grid: ReducedGrid,
        vector_aggs: &[super::plan::VectorAggSpec],
    ) -> Result<Self, ReadError> {
        let _ = (value, class_labels, caps, vector_aggs);
        Ok(ReducedFold { grid })
    }

    /// Folds one row.
    pub(in crate::logql) fn push(
        &mut self,
        row: &MetricReducedRow,
    ) -> Result<ReducedPush, ReadError> {
        let _ = (row, self.grid);
        Ok(ReducedPush::Folded)
    }

    /// The series, in label order.
    pub(in crate::logql) fn finish(self) -> Vec<MatrixSeries> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logql::pipeline::RangeGrouping;
    use crate::logql::plan::{ClientAgg, ClientValue};
    use crate::logql::sql::{KeyMode, UnwrapConversion};
    use pulsus_logql::{RangeAggOp, Stage};

    fn stages(query: &str) -> Vec<Stage> {
        let pulsus_logql::Expr::Log(le) = pulsus_logql::parse(query).expect("parse") else {
            panic!("expected a log expression");
        };
        le.pipeline
    }

    fn value(mode: KeyMode, grouping: Option<RangeGrouping>, query: &str) -> ReducedUnwrap {
        let pipeline = stages(query);
        ReducedUnwrap {
            reducer: RangeAggOp::AvgOverTime,
            quantile: None,
            label: "v".to_string(),
            conversion: UnwrapConversion::Number,
            mode,
            key_stages: pipeline.clone(),
            todays_route: ClientAgg {
                pipeline,
                value: ClientValue::Unwrap,
                range_op: RangeAggOp::AvgOverTime,
                param: None,
                absent_labels: Vec::new(),
                grouping: grouping.map(Box::new),
            },
            instant: false,
        }
    }

    const GRID: ReducedGrid = ReducedGrid {
        grid_start_ns: 60,
        end_ns: 120,
        step_ns: 60,
        grid_points: 2,
        range_ns: 60,
        rate_window_ns: Some(60),
    };

    fn row(c: u128, kv: &[(&str, &str)], e: u8) -> MetricReducedRow {
        MetricReducedRow {
            c,
            kv: kv
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            e,
            bucket_ns: 60,
            n: 1,
            r: 1.0,
            t1: 0,
            t2: 0,
            v1: 0.0,
        }
    }

    fn fold(value: &ReducedUnwrap) -> ReducedFold {
        ReducedFold::new(
            value,
            HashMap::from([(Fingerprint::from_raw(0), LabelSet::new())]),
            AggCaps::DEFAULT,
            GRID,
            &[],
        )
        .expect("a fold")
    }

    /// **T9 (issue #624, part 4): the fold hands the query to today's route
    /// when two keys reach one label set, or a key whose values failed to
    /// convert is kept; a filter that drops the failed key is no stop.**
    #[test]
    fn the_fold_hands_back_on_a_collision_or_a_kept_error() {
        // Two keys, one answer series: their samples would be one window.
        let keyed = value(
            KeyMode::Keep(vec!["f".to_string(), "pod".to_string()]),
            Some(RangeGrouping::By(vec!["pod".to_string()])),
            r#"{a="b"} | unwrap v"#,
        );
        let mut f = fold(&keyed);
        assert_eq!(
            f.push(&row(0, &[("f", "2"), ("pod", "a")], 0))
                .expect("push"),
            ReducedPush::Folded
        );
        assert_eq!(
            f.push(&row(0, &[("f", "3"), ("pod", "a")], 0))
                .expect("push"),
            ReducedPush::Stop,
            "a second key with the first key's labels"
        );

        // A failed key, kept: its error is today's route's to report.
        let bare = value(
            KeyMode::Drop(vec!["v".to_string()]),
            None,
            r#"{a="b"} | unwrap v"#,
        );
        let mut f = fold(&bare);
        assert_eq!(
            f.push(&row(0, &[], 1)).expect("push"),
            ReducedPush::Stop,
            "a kept e = 1 key"
        );

        // The same key under `| __error__=""`: dropped, and the fold goes on.
        let filtered = value(
            KeyMode::Drop(vec!["v".to_string()]),
            None,
            r#"{a="b"} | unwrap v | __error__="""#,
        );
        let mut f = fold(&filtered);
        assert_eq!(
            f.push(&row(0, &[], 1)).expect("push"),
            ReducedPush::Folded,
            "a dropped e = 1 key"
        );
        assert_eq!(f.push(&row(0, &[], 0)).expect("push"), ReducedPush::Folded);
        let series = f.finish();
        assert_eq!(series.len(), 1, "the e = 0 key's series: {series:?}");
        assert_eq!(series[0].points, vec![(60, 1.0)]);
    }
}
