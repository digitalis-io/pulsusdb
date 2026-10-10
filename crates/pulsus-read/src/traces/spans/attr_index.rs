//! Operator-named attribute indexes (issue #595 part 2). Stubs: the tests
//! land first.

use pulsus_config::IndexedScope;
use pulsus_traceql::FieldExpr;

/// One indexed attribute: its scope and its key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedAttr {
    pub scope: IndexedScope,
    pub key: String,
}

impl IndexedAttr {
    /// Stub.
    pub fn from_config(_items: &[String]) -> Vec<IndexedAttr> {
        Vec::new()
    }

    /// Stub.
    pub fn expr(&self) -> String {
        String::new()
    }
}

/// Stub.
pub fn index_hints(_body: &FieldExpr, _indexed: &[IndexedAttr]) -> Vec<String> {
    Vec::new()
}
