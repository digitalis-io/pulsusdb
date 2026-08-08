//! `service_name` discovery: the label the reference synthesizes for a stream
//! that does not already carry one (issue #379).
//!
//! Reference: `pkg/loghttp/push/push.go:441-456 @ v3.7.4` on the Loki-push
//! path and `pkg/loghttp/push/otlp.go:174-220 @ v3.7.4` on the OTLP logs path,
//! with the thirteen default names from
//! `pkg/validation/limits.go:329-343 @ v3.7.4`. Synthesis runs **after** the
//! empty-value strip (`syntax.ParseLabels` -> `ls.WithoutEmpty()`,
//! `pkg/logql/syntax/parser.go:279-296 @ v3.7.4`) and **before** the four
//! per-stream label bounds ([`super::log_label_limits`]), because the
//! distributor re-parses the rendered literal and only then validates
//! (`pkg/distributor/distributor.go:1370-1387 @ v3.7.4`). The label set
//! interpolated into all four bound messages is therefore the post-synthesis
//! one — pinned by `the_discovered_label_is_inside_the_bound_message` in
//! `loki_push.rs`.
//!
//! # Two receivers, two algorithms — deliberately not one function
//!
//! The reference discovers a service name twice, differently, and the two
//! disagree on inputs a client can send. This module reproduces both rather
//! than picking one:
//!
//! | | Loki push (`push.go:442-453`) | OTLP logs (`otlp.go:174-220`) |
//! |---|---|---|
//! | scanned in | DISCOVERY-LIST order | WIRE order |
//! | scanned over | every stream label | only attributes the reference indexes as stream labels (the eighteen of [`super::log_label_limits::is_otlp_index_attribute`]) |
//! | empty values | skipped (`labelVal != ""`) | **not** skipped — an empty value wins the slot |
//! | no candidate | `unknown_service` | `unknown_service` |
//!
//! Measured on stock `grafana/loki:3.7.4` (revision `b318f282`), read back
//! through `/loki/api/v1/series`:
//!
//! | pushed / resource attributes | stored stream |
//! |---|---|
//! | push `{name=nn, app=aa}` | `{app=aa, name=nn, service_name=aa}` — list order, not wire or alphabetical order |
//! | push `{workload=ww, job=jj}` | `{job=jj, service_name=ww, workload=ww}` |
//! | push `{component=cc, container=kk}` | `{component=cc, container=kk, service_name=kk}` |
//! | push `{service_name="", app=aa}` | `{app=aa, service_name=aa}` — strip, then discovery |
//! | push `{__aggregated_metric__=1, app=aa}` | `{__aggregated_metric__=1, app=aa}` — no synthesis |
//! | OTLP `app=x` | `{service_name=unknown_service}` — `app` is not an index attribute, so it never reaches discovery |
//! | OTLP `k8s.pod.name=p` | `{k8s_pod_name=p, service_name=unknown_service}` — indexed, but not in the discovery list |
//! | OTLP `k8s.container.name=kc, container.name=c` | `{container_name=c, k8s_container_name=kc, service_name=kc}` |
//! | OTLP `container.name=c2, k8s.container.name=kc2` | `{container_name=c2, k8s_container_name=kc2, service_name=c2}` — **wire order decides** |
//! | OTLP `container.name=c4, service.name=""` | `{container_name=c4}` — **no `service_name` at all** |
//! | OTLP `container.name=""` | `400 error at least one label pair is required per stream` |
//!
//! The two rows in bold are why unifying the resolvers is not an option: the
//! push resolver answers `container_name`'s value for both orderings, and it
//! never returns an empty string. `otlp_discovery_is_wire_ordered_not_list_ordered`
//! asserts the disagreement directly, so a later unification fails a test
//! rather than silently flattening a difference the reference has.
//!
//! Only three of the eighteen indexed OTel attributes canonicalize onto a name
//! in the discovery list — `container.name`, `k8s.container.name`,
//! `k8s.job.name` — which is what makes the OTLP path's answer
//! `unknown_service` so often (pinned by
//! `exactly_three_index_attributes_can_be_discovered`).
//!
//! # No configurability
//!
//! `validation.discover-service-name` is a per-tenant limit upstream and
//! PulsusDB has no limits surface, so the thirteen defaults are the values —
//! the same reasoning [`super::log_label_limits`] gives for the four bounds.
//! There is deliberately no "empty list disables it" path.

use pulsus_model::{SERVICE_NAME_LABEL, canonicalize_label_key};

use super::log_label_limits::{AGGREGATED_METRIC_LABEL, PATTERN_LABEL, is_otlp_index_attribute};

/// `Limits.RegisterFlags`' default `DiscoverServiceName`
/// (`pkg/validation/limits.go:329-343 @ v3.7.4`), in the reference's order —
/// on the Loki-push path the order IS the rule, so this is a list and not a
/// set. Pinned by `the_discovery_list_matches_the_reference_defaults`.
pub const DISCOVER_SERVICE_NAME: [&str; 13] = [
    "service",
    "app",
    "application",
    "app_name",
    "name",
    "app_kubernetes_io_name",
    "container",
    "container_name",
    "k8s_container_name",
    "component",
    "workload",
    "job",
    "k8s_job_name",
];

/// `push.ServiceUnknown` (`pkg/loghttp/push/push.go:81 @ v3.7.4`).
pub const UNKNOWN_SERVICE: &str = "unknown_service";

/// `attrServiceName` (`pkg/loghttp/push/otlp.go:40 @ v3.7.4`) — the RAW OTel
/// spelling the OTLP path seeds `hasServiceName` from. Not the canonicalized
/// [`SERVICE_NAME_LABEL`]: a resource attribute literally keyed `service_name`
/// is not an index attribute upstream and neither seeds nor writes the slot.
const OTLP_SERVICE_NAME_ATTRIBUTE: &str = "service.name";

/// The Loki-push rule (`pkg/loghttp/push/push.go:442-453 @ v3.7.4`).
///
/// `stripped` is one stream's labels AFTER `WithoutEmpty`, i.e. exactly what
/// `syntax.ParseLabels` hands the reference. Returns the value to set, or
/// `None` where the reference sets nothing: the set already carries
/// `service_name`, or it is an internal stream
/// (`push.go:430-435,442 @ v3.7.4`).
///
/// Scans [`DISCOVER_SERVICE_NAME`] in LIST order and takes the first name
/// present with a non-empty value, [`UNKNOWN_SERVICE`] when none is. One
/// borrowed pass over `stripped` keeping the lowest matching list index — not
/// thirteen passes — and no allocation at all: the caller owns the copy.
///
/// Duplicate names resolve to the first in wire order, as `Labels.Get`'s
/// linear scan does (`vendor/github.com/prometheus/prometheus/model/labels/labels_slicelabels.go:203-210
/// @ v3.7.4`), which is why a strictly-lower rank is required to displace an
/// incumbent.
pub fn push_service_name(stripped: &[(String, String)]) -> Option<&str> {
    let mut best: Option<(usize, &str)> = None;
    for (name, value) in stripped {
        // `lbs.Has(LabelServiceName) || isInternalStream` — the whole `if` is
        // skipped, so nothing is set and nothing is overwritten.
        if name == SERVICE_NAME_LABEL || name == AGGREGATED_METRIC_LABEL || name == PATTERN_LABEL {
            return None;
        }
        // `if labelVal := lbs.Get(labelName); labelVal != ""` — redundant on a
        // stripped set, kept because it is the reference's guard and this
        // function's contract must not depend on the caller having stripped.
        if value.is_empty() {
            continue;
        }
        let Some(rank) = DISCOVER_SERVICE_NAME.iter().position(|c| c == name) else {
            continue;
        };
        if best.is_none_or(|(incumbent, _)| rank < incumbent) {
            best = Some((rank, value.as_str()));
        }
    }
    Some(best.map_or(UNKNOWN_SERVICE, |(_, value)| value))
}

/// The OTLP rule (`pkg/loghttp/push/otlp.go:174-220 @ v3.7.4`) — a different
/// algorithm, not a reuse of [`push_service_name`], because the reference's is
/// different and the two disagree observably (see the module doc's table).
///
/// `raw` is the resource's attributes in WIRE order, before any strip. Models
/// the reference's single `streamLabels["service_name"]` map slot:
///
/// - the `service.name` index attribute writes it, every time it appears,
///   because `streamLabels[lbl.Name] = lbl.Value` is a plain map assignment
///   (`otlp.go:193`);
/// - a discovery hit writes it — with NO non-empty guard, and only while
///   `hasServiceName` is false (`otlp.go:198-206`);
/// - `hasServiceName` is seeded from the FIRST `service.name` attribute being
///   present and non-empty (`otlp.go:176`, `pcommon.Map.Get`'s first-match
///   scan, `vendor/go.opentelemetry.io/collector/pdata/pcommon/map.go:65-73`);
/// - an unset `hasServiceName` overwrites whatever the slot holds with
///   [`UNKNOWN_SERVICE`] after the range (`otlp.go:218-220`) — which is why a
///   lone `service.name=""` stores `unknown_service` and not nothing.
///
/// The returned value may be `""`, which the caller's `WithoutEmpty` then
/// removes: that is how the reference stores a stream carrying no
/// `service_name` at all (`container.name=c4, service.name=""` above), and how
/// `container.name=""` empties a stream into a `400`.
pub fn otlp_service_name(raw: &[(String, String)]) -> String {
    let mut has_service_name = raw
        .iter()
        .find(|(key, _)| key == OTLP_SERVICE_NAME_ATTRIBUTE)
        .is_some_and(|(_, value)| !value.is_empty());
    let mut slot: Option<&str> = None;
    for (key, value) in raw {
        if !is_otlp_index_attribute(key) {
            continue;
        }
        // `attributeToLabels` canonicalizes AFTER the index-label decision was
        // taken on the raw key (`otlp.go:193,610-614 @ v3.7.4`); the discovery
        // comparison is then against the derived name.
        let derived = canonicalize_label_key(key);
        if derived == SERVICE_NAME_LABEL {
            slot = Some(value.as_str());
        }
        if !has_service_name && DISCOVER_SERVICE_NAME.contains(&derived.as_str()) {
            slot = Some(value.as_str());
            has_service_name = true;
        }
    }
    if has_service_name {
        // Unreachable as `None`: every path that sets `has_service_name` has
        // written the slot, and the seed is only true when a non-empty
        // `service.name` attribute exists — which the range then writes.
        slot.unwrap_or(UNKNOWN_SERVICE).to_string()
    } else {
        UNKNOWN_SERVICE.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(input: &[(&str, &str)]) -> Vec<(String, String)> {
        input
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    /// AC1: the list is the reference's list, in the reference's order.
    #[test]
    fn the_discovery_list_matches_the_reference_defaults() {
        assert_eq!(
            DISCOVER_SERVICE_NAME,
            [
                "service",
                "app",
                "application",
                "app_name",
                "name",
                "app_kubernetes_io_name",
                "container",
                "container_name",
                "k8s_container_name",
                "component",
                "workload",
                "job",
                "k8s_job_name",
            ],
            "`pkg/validation/limits.go:329-343 @ v3.7.4`, order included"
        );
        assert_eq!(UNKNOWN_SERVICE, "unknown_service");
        assert_eq!(OTLP_SERVICE_NAME_ATTRIBUTE, "service.name");
    }

    /// AC1: three pairs whose alphabetical order AND whose wire order both
    /// disagree with the list order, so this discriminates the rule rather
    /// than pinning an answer several rules would give.
    #[test]
    fn list_order_decides_on_the_push_path() {
        // `app` (rank 1) beats `name` (rank 4); alphabetically and in wire
        // order `app` would win too, so the wire order is reversed here.
        assert_eq!(
            push_service_name(&pairs(&[("name", "nn"), ("app", "aa")])),
            Some("aa")
        );
        // `workload` (rank 10) beats `job` (rank 11) — alphabetically `job`
        // wins, and in this wire order `job` would too.
        assert_eq!(
            push_service_name(&pairs(&[("job", "jj"), ("workload", "ww")])),
            Some("ww")
        );
        // `container` (rank 6) beats `component` (rank 9) — alphabetically and
        // in this wire order `component` would win.
        assert_eq!(
            push_service_name(&pairs(&[("component", "cc"), ("container", "kk")])),
            Some("kk")
        );
    }

    /// A repeated name resolves to the first in wire order, as `Labels.Get`
    /// does; equal rank does not displace the incumbent.
    #[test]
    fn a_repeated_discovery_name_resolves_to_the_first_in_wire_order() {
        assert_eq!(
            push_service_name(&pairs(&[("app", "first"), ("app", "second")])),
            Some("first")
        );
    }

    /// AC2: synthesis is skipped exactly where the reference skips it.
    #[test]
    fn push_synthesis_is_skipped_for_an_explicit_service_name_and_for_internal_streams() {
        assert_eq!(
            push_service_name(&pairs(&[("service_name", "given"), ("app", "aa")])),
            None
        );
        assert_eq!(
            push_service_name(&pairs(&[("__aggregated_metric__", "1"), ("app", "aa")])),
            None
        );
        assert_eq!(
            push_service_name(&pairs(&[("__pattern__", "1"), ("app", "aa")])),
            None
        );
        // Not skipped, and no candidate: the fallback.
        assert_eq!(
            push_service_name(&pairs(&[("zzz", "q")])),
            Some(UNKNOWN_SERVICE)
        );
        // The empty label set is the same fallback — this is the input that
        // makes `MissingLabelsErrorMsg` unreachable on the push path.
        assert_eq!(push_service_name(&[]), Some(UNKNOWN_SERVICE));
    }

    /// AC2: the strip runs first upstream, so an empty `service_name` does not
    /// suppress discovery — measured `{service_name:"", app:aa}` ->
    /// `{app=aa, service_name=aa}`.
    #[test]
    fn an_empty_service_name_is_stripped_before_discovery_runs() {
        // The caller hands a stripped set, which is what the receivers do.
        assert_eq!(
            push_service_name(&pairs(&[("app", "aa")])),
            Some("aa"),
            "the stripped form of `{{service_name:\"\", app:aa}}`"
        );
        // And the guard holds even if a caller does not strip: an empty
        // candidate value is skipped, exactly as `labelVal != ""` skips it.
        assert_eq!(
            push_service_name(&pairs(&[("app", ""), ("job", "jj")])),
            Some("jj")
        );
    }

    /// AC3: the OTLP resolver is wire-ordered, and the push resolver is not.
    /// Both halves are asserted side by side, so unifying the two resolvers
    /// fails this test instead of quietly flattening the difference.
    #[test]
    fn otlp_discovery_is_wire_ordered_not_list_ordered() {
        let kc_first = pairs(&[("k8s.container.name", "kc"), ("container.name", "c")]);
        let c_first = pairs(&[("container.name", "c2"), ("k8s.container.name", "kc2")]);
        assert_eq!(otlp_service_name(&kc_first), "kc");
        assert_eq!(otlp_service_name(&c_first), "c2");

        // The same names on the push path answer `container_name`'s value in
        // BOTH orders, because `container_name` (rank 7) precedes
        // `k8s_container_name` (rank 8).
        let push_kc_first = pairs(&[("k8s_container_name", "kc"), ("container_name", "c")]);
        let push_c_first = pairs(&[("container_name", "c2"), ("k8s_container_name", "kc2")]);
        assert_eq!(push_service_name(&push_kc_first), Some("c"));
        assert_eq!(push_service_name(&push_c_first), Some("c2"));
    }

    /// AC3: only the eighteen indexed attributes reach discovery at all.
    #[test]
    fn otlp_discovery_ignores_non_index_attributes() {
        // `app` is in the discovery list but is not an index attribute.
        assert_eq!(otlp_service_name(&pairs(&[("app", "x")])), UNKNOWN_SERVICE);
        // Indexed, but not in the discovery list.
        assert_eq!(
            otlp_service_name(&pairs(&[("k8s.pod.name", "p")])),
            UNKNOWN_SERVICE
        );
        // Indexed and in the list.
        assert_eq!(otlp_service_name(&pairs(&[("k8s.job.name", "j")])), "j");
    }

    /// AC3: the intersection is exactly three names, which is why the OTLP
    /// path answers `unknown_service` for most resources. Enumerated from the
    /// reference's own two lists rather than restated.
    #[test]
    fn exactly_three_index_attributes_can_be_discovered() {
        let discoverable: Vec<&str> = super::super::log_label_limits::otlp_index_attributes()
            .iter()
            .copied()
            .filter(|raw| DISCOVER_SERVICE_NAME.contains(&canonicalize_label_key(raw).as_str()))
            .collect();
        assert_eq!(
            discoverable,
            ["container.name", "k8s.container.name", "k8s.job.name"]
        );
    }

    /// AC4: the slot has no non-empty guard and `service.name`'s precedence is
    /// positional, not absolute. Every case is a measured container row.
    #[test]
    fn otlp_slot_reproduces_the_references_overwrite_order() {
        // A discovery hit writes the slot, and a LATER `service.name` — empty
        // — overwrites it, because the map assignment is unconditional.
        assert_eq!(
            otlp_service_name(&pairs(&[("container.name", "c4"), ("service.name", "")])),
            "",
            "the reference stores this stream with no `service_name` at all"
        );
        // Reversed, the empty `service.name` neither seeds `hasServiceName`
        // nor matches the discovery list, so the later hit wins.
        assert_eq!(
            otlp_service_name(&pairs(&[("service.name", ""), ("container.name", "c3")])),
            "c3"
        );
        // A non-empty `service.name` seeds `hasServiceName`, so no discovery
        // hit can displace it...
        assert_eq!(
            otlp_service_name(&pairs(&[("service.name", "a"), ("container.name", "c")])),
            "a"
        );
        assert_eq!(
            otlp_service_name(&pairs(&[("container.name", "c"), ("service.name", "a")])),
            "a"
        );
        // ...and a raw `service_name` attribute is not indexed, so it neither
        // seeds nor writes.
        assert_eq!(
            otlp_service_name(&pairs(&[("service.name", "a"), ("service_name", "b")])),
            "a"
        );
        assert_eq!(
            otlp_service_name(&pairs(&[("service_name", "x")])),
            UNKNOWN_SERVICE
        );
        // A lone empty `service.name`: the slot is written empty and then
        // overwritten by the post-range fallback, because `hasServiceName` was
        // never set (`otlp.go:218-220`).
        assert_eq!(
            otlp_service_name(&pairs(&[("service.name", "")])),
            UNKNOWN_SERVICE
        );
        // An empty discovery hit DOES set `hasServiceName`, so the fallback is
        // suppressed and the slot stays empty — the input that makes
        // `MissingLabelsErrorMsg` reachable.
        assert_eq!(otlp_service_name(&pairs(&[("container.name", "")])), "");
        // No attributes at all.
        assert_eq!(otlp_service_name(&[]), UNKNOWN_SERVICE);
    }

    /// A repeated `service.name` seeds from the FIRST occurrence
    /// (`pcommon.Map.Get`) but the slot is written by the LAST, because every
    /// index attribute assigns into the map. All three rows measured on stock
    /// `grafana/loki@sha256:87f0a067…` via `/loki/api/v1/series`.
    #[test]
    fn a_repeated_service_name_attribute_seeds_from_the_first_and_stores_the_last() {
        // The case that discriminates first-match seeding from last-match:
        // the first copy is non-empty, so `hasServiceName` is set before the
        // range and the `container.name` hit is suppressed; the last copy
        // then empties the slot. Seeding from the LAST copy instead would let
        // the hit through and store `service_name="c"`. Measured:
        // `{service.name=z, service.name="", container.name=cA-379}` stores
        // `{container_name="cA-379"}` — no `service_name` at all.
        assert_eq!(
            otlp_service_name(&pairs(&[
                ("service.name", "z"),
                ("service.name", ""),
                ("container.name", "c"),
            ])),
            ""
        );
        assert_eq!(
            otlp_service_name(&pairs(&[("service.name", "x"), ("service.name", "y")])),
            "y"
        );
        // Seeded false by the first (empty) copy, so a discovery hit between
        // them wins the slot — and the second copy then overwrites it anyway.
        assert_eq!(
            otlp_service_name(&pairs(&[
                ("service.name", ""),
                ("container.name", "c"),
                ("service.name", "z"),
            ])),
            "z"
        );
    }
}
