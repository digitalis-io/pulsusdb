//! The template timezone setting (`reader.template_timezone` /
//! `PULSUS_TEMPLATE_TIMEZONE`, issue #311).
//!
//! LogQL `line_format`/`label_format` templates expose Go's local-time
//! formatting (`toDate`, `date`, `now`, …). Loki resolves that zone from
//! the *process* — `$TZ`, falling back to `/etc/localtime` — so the same
//! query can render different text on two hosts in one cluster. PulsusDB
//! resolves it from **configuration** instead: declared once, identical on
//! every node, defaulting to `UTC`. The host's `$TZ`/`/etc/localtime` are
//! never read (see docs/benchmarks/logs-differential-ledger.md,
//! `template-timezone-configured`).
//!
//! The zone is parsed here, at config load, so [`crate::Config`] cannot
//! hold an unknown zone name: a typo is a startup error naming the
//! offending value, never a server silently running in the wrong zone.

use std::fmt;
use std::str::FromStr;

use chrono_tz::Tz;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A resolved IANA timezone for LogQL template time functions. Parsed at
/// config load (YAML *and* environment), so every value that reaches the
/// read path names a zone `chrono-tz` knows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TemplateTimezone(Tz);

impl TemplateTimezone {
    /// The documented default (docs/configuration.md §6).
    pub const UTC: TemplateTimezone = TemplateTimezone(Tz::UTC);

    /// The resolved zone.
    pub fn tz(self) -> Tz {
        self.0
    }

    /// The canonical IANA name (`"UTC"`, `"Europe/London"`, …) — what the
    /// redacted `/config` dump renders and what round-trips through
    /// [`FromStr`].
    pub fn name(self) -> &'static str {
        self.0.name()
    }
}

impl Default for TemplateTimezone {
    fn default() -> Self {
        TemplateTimezone::UTC
    }
}

/// The zone name did not match any IANA zone in the bundled tzdata.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
#[error(
    "{0:?} is not a known IANA timezone name (expected e.g. UTC, Europe/London, America/New_York)"
)]
pub struct UnknownTimezone(pub String);

impl FromStr for TemplateTimezone {
    type Err = UnknownTimezone;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Tz::from_str(s)
            .map(TemplateTimezone)
            .map_err(|_| UnknownTimezone(s.to_string()))
    }
}

impl fmt::Display for TemplateTimezone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl Serialize for TemplateTimezone {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.name())
    }
}

impl<'de> Deserialize<'de> for TemplateTimezone {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        TemplateTimezone::from_str(&raw).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_is_utc() {
        assert_eq!(TemplateTimezone::default(), TemplateTimezone::UTC);
        assert_eq!(TemplateTimezone::default().name(), "UTC");
    }

    #[test]
    fn a_known_iana_name_parses_and_round_trips_through_its_canonical_name() {
        let tz: TemplateTimezone = "Europe/London".parse().expect("known zone");
        assert_eq!(tz.name(), "Europe/London");
        assert_eq!(
            tz.name().parse::<TemplateTimezone>().expect("round trip"),
            tz
        );
    }

    #[test]
    fn an_unknown_zone_name_is_rejected_with_the_offending_value() {
        let err = "Europe/Lundon"
            .parse::<TemplateTimezone>()
            .expect_err("unknown zone must be rejected");
        assert_eq!(
            err.to_string(),
            "\"Europe/Lundon\" is not a known IANA timezone name (expected e.g. UTC, \
             Europe/London, America/New_York)"
        );
    }

    #[test]
    fn an_empty_zone_name_is_rejected_rather_than_defaulted() {
        assert!("".parse::<TemplateTimezone>().is_err());
    }

    /// `Etc/UTC` is a distinct zone from `UTC` in tzdata (same offset, a
    /// different name), and the name is what Go's `Location.String()`
    /// renders — so the two must not be collapsed.
    #[test]
    fn etc_utc_keeps_its_own_name() {
        let tz: TemplateTimezone = "Etc/UTC".parse().expect("known zone");
        assert_eq!(tz.name(), "Etc/UTC");
        assert_ne!(tz, TemplateTimezone::UTC);
    }

    #[test]
    fn serde_renders_and_reads_the_canonical_name() {
        let tz: TemplateTimezone = "America/New_York".parse().expect("known zone");
        let yaml = serde_norway::to_string(&tz).expect("serialize");
        assert_eq!(yaml.trim(), "America/New_York");
        let back: TemplateTimezone = serde_norway::from_str(&yaml).expect("deserialize");
        assert_eq!(back, tz);
    }

    #[test]
    fn serde_rejects_an_unknown_zone_name_naming_it() {
        let err = serde_norway::from_str::<TemplateTimezone>("Mars/Olympus\n")
            .expect_err("unknown zone must be rejected");
        assert!(
            err.to_string().contains("Mars/Olympus"),
            "message names the offending value: {err}"
        );
    }
}
