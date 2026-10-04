//! The DDL file, rendered. Stub: the body lands with the deletion.

use crate::render::RenderCtx;

/// `schema/schema.sql`, as committed.
pub const SCHEMA_SQL: &str = include_str!("../../../schema/schema.sql");

/// The file with the other mode's lines dropped and every token resolved.
pub fn rendered(_ctx: &RenderCtx) -> String {
    String::new()
}

/// [`rendered`], split into the statements the script sends one per request.
pub fn rendered_statements(_ctx: &RenderCtx) -> Vec<String> {
    Vec::new()
}
