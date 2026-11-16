//! The profiles read path (issue #772). Issue #820: the label selector's
//! compiler and the tenant check over the profile tables. Issue #821: the
//! catalogue reads — profile types, label names, label values and series —
//! and the engine that sends them. Issue #823: the merged flame graph, its
//! statement (`tree`) and its trim (`flamegraph`).

pub mod catalogue;
mod dispatch;
pub mod engine;
pub mod flamegraph;
pub mod selector;
pub mod tenant_scope;
pub mod tree;

pub use engine::{Answer, ProfilesEngine, ProfilesReadConfig, ProfilesReadError};
