//! The profiles read path (issue #772). Issue #820: the label selector's
//! compiler and the tenant check over the profile tables. Issue #821: the
//! catalogue reads — profile types, label names, label values and series —
//! and the engine that sends them.

pub mod catalogue;
pub mod engine;
pub mod selector;
pub mod tenant_scope;

pub use engine::{ProfilesReadConfig, ProfilesReadError};
