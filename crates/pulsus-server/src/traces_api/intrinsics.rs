//! The static intrinsic vocabulary tag discovery serves (issue #475):
//! the `intrinsic` scope's tag list, and the closed value set for the
//! two intrinsics that have one.
//!
//! Every answer here is a compile-time list derived from
//! `pulsus_traceql`'s own enums — no state, no I/O, and in particular
//! **no ClickHouse query**. That is the point of the module rather than
//! an incidental property: `trace_tag_catalog` has no row for a value
//! that exists by definition rather than by observation, so a store read
//! for `status` or `kind` answers with whatever attribute happens to
//! carry that key. Bypassing the catalog is what makes the answer right;
//! adding the static list on top of a catalog read would leave the
//! collision in place.

use std::sync::OnceLock;

use pulsus_traceql::Intrinsic;

/// The wire `type` every static intrinsic value carries.
///
/// The Grafana Tempo datasource quotes an ad-hoc filter value only when
/// its type is `string`; any other type is emitted bare. So a `keyword`
/// value yields `{status=error}` rather than `{status="error"}`, which is
/// what parses — the type also selects the operator pair offered beside
/// the field.
pub(crate) const KEYWORD_TYPE: &str = "keyword";

/// The `intrinsic` scope's tag list, ascending byte order, built once
/// from `Intrinsic::ALL` × `discovery_spellings()`.
pub(crate) fn intrinsic_scope_tags() -> &'static [&'static str] {
    static TAGS: OnceLock<Vec<&'static str>> = OnceLock::new();
    TAGS.get_or_init(|| {
        let mut tags: Vec<&'static str> = Intrinsic::ALL
            .iter()
            .flat_map(|i| i.discovery_spellings().iter().copied())
            .collect();
        tags.sort_unstable();
        tags
    })
}

/// The static answer for a resolved intrinsic — never a store read. An
/// intrinsic with no closed value set answers an EMPTY list rather than
/// falling through to the catalog, which is what stops a bare `name`
/// lookup returning span-event names.
pub(crate) fn intrinsic_tag_values(intrinsic: Intrinsic) -> &'static [&'static str] {
    intrinsic.discovery_values().unwrap_or(&[])
}

#[cfg(test)]
mod tests {
    use super::*;

    /// AC4: the served list, as a literal typed into the test. This is
    /// the body the pinned reference build returned for its `intrinsic`
    /// scope, not a list assembled from our own code, so the code under
    /// test cannot produce it by construction.
    #[test]
    fn the_intrinsic_scope_serves_exactly_these_twenty_five_names() {
        assert_eq!(
            intrinsic_scope_tags(),
            [
                "duration",
                "event:name",
                "event:timeSinceStart",
                "instrumentation:name",
                "instrumentation:version",
                "kind",
                "link:spanID",
                "link:traceID",
                "name",
                "rootName",
                "rootServiceName",
                "span:duration",
                "span:id",
                "span:kind",
                "span:name",
                "span:parentID",
                "span:status",
                "span:statusMessage",
                "status",
                "statusMessage",
                "trace:duration",
                "trace:id",
                "trace:rootName",
                "trace:rootService",
                "traceDuration",
            ]
        );
        assert_eq!(intrinsic_scope_tags().len(), 25);
    }

    /// The list is ascending, and free of duplicates — two variants
    /// offering the same spelling would otherwise be invisible here.
    #[test]
    fn the_served_list_is_ascending_and_duplicate_free() {
        let tags = intrinsic_scope_tags();
        for pair in tags.windows(2) {
            assert!(pair[0] < pair[1], "not ascending at {pair:?}");
        }
    }

    /// AC1/AC2: the two closed value sets, in the order they are served.
    /// Literals typed here, so a `Status`/`Kind` swap fails even though
    /// the union of the two lists is unchanged.
    #[test]
    fn the_static_value_lists_are_the_closed_keyword_sets() {
        assert_eq!(
            intrinsic_tag_values(Intrinsic::Status),
            ["ok", "error", "unset"]
        );
        assert_eq!(
            intrinsic_tag_values(Intrinsic::Kind),
            [
                "unspecified",
                "internal",
                "server",
                "client",
                "producer",
                "consumer"
            ]
        );
    }

    /// An intrinsic with no closed value set answers empty — including
    /// one that is not offered in the scope list at all.
    #[test]
    fn an_open_valued_intrinsic_answers_an_empty_list() {
        for intrinsic in [
            Intrinsic::Name,
            Intrinsic::Duration,
            Intrinsic::NestedSetLeft,
            Intrinsic::EventName,
            Intrinsic::LinkSpanId,
        ] {
            assert!(intrinsic_tag_values(intrinsic).is_empty(), "{intrinsic:?}");
        }
    }
}
