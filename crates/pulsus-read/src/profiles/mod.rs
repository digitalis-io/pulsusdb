//! The profiles read path (issue #772). Issue #820: the label selector's
//! compiler and the tenant check over the profile tables. Issue #821: the
//! catalogue reads — profile types, label names, label values and series —
//! and the engine that sends them. Issue #823: the merged flame graph, its
//! statement (`tree`) and its trim (`flamegraph`). Issue #824: the time
//! series of profile totals (`series`). Issue #827: the diff of two
//! selections, its statement (`diff`) and its trim (`flamegraph_diff`).
//! Issue #825: the heatmap of profile and span totals (`heatmap`). Issue
//! #829: the render route's one statement and its timeline (`render`).

pub mod catalogue;
pub mod diff;
mod dispatch;
pub mod engine;
pub mod flamegraph;
pub mod flamegraph_diff;
pub mod heatmap;
pub mod render;
pub mod selector;
pub mod series;
pub mod tenant_scope;
pub mod tree;

pub use engine::{Answer, ProfilesEngine, ProfilesReadConfig, ProfilesReadError};
