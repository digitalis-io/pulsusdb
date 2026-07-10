//! ClickHouse client pool (native protocol) and columnar insert buffers,
//! plus streaming reads. See docs/architecture.md §1.2 and §3. Concrete
//! client selection is deferred to issue #3.

#[cfg(test)]
mod tests {
    #[test]
    fn crate_compiles() {}
}
