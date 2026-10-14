//! The metrics statement's engine side (issue #596 part 1): issue the one
//! statement a [`RoutedMetricsPlan`] carries and frame its rows, or hand a
//! plan without one to today's engine.

use super::TraceEngine;
use crate::logql::error::ReadError;
use crate::traces::metrics_result::TraceMetricsResult;
use crate::traces::spans::metrics::RoutedMetricsPlan;

impl TraceEngine {
    /// The range answer to `routed`.
    pub async fn metrics_range_routed(
        &self,
        routed: &RoutedMetricsPlan,
    ) -> Result<TraceMetricsResult, ReadError> {
        self.metrics_range(routed.plan()).await
    }

    /// The instant answer to `routed`.
    pub async fn metrics_instant_routed(
        &self,
        routed: &RoutedMetricsPlan,
    ) -> Result<TraceMetricsResult, ReadError> {
        self.metrics_instant(routed.plan()).await
    }
}
