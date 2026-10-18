//! The service-graph statement over the per-minute edge totals and the span
//! table (issue #599). Stub: the tests land first.

use crate::traces::exec::GraphTarget;
use crate::traces::graph_sql::GraphWindow;

/// The service-graph statement (issue #599). Stub.
pub fn spans_graph_sql(_w: GraphWindow, _target: Option<&GraphTarget>, _max_edges: u64) -> String {
    String::new()
}
