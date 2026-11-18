//! The JFR reader (issue #819). Stub until the change: reads no event.

use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::BuildHasherDefault;

pub(crate) type IdMap<K, V> = HashMap<K, V, BuildHasherDefault<DefaultHasher>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Frame {
    pub(crate) method: u64,
    pub(crate) line: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Method {
    pub(crate) class: u64,
    pub(crate) name: u64,
}

#[derive(Debug, Default)]
pub(crate) struct Pools<'a> {
    pub(crate) stack_traces: IdMap<u64, Vec<Frame>>,
    pub(crate) methods: IdMap<u64, Method>,
    pub(crate) classes: IdMap<u64, u64>,
    pub(crate) symbols: IdMap<u64, Cow<'a, str>>,
    pub(crate) thread_states: IdMap<u64, Cow<'a, str>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub(crate) struct Correlation {
    pub(crate) context_id: u64,
    pub(crate) span_id: u64,
    pub(crate) span_name: u64,
    pub(crate) trace_id_hi: u64,
    pub(crate) trace_id_lo: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Event<'a> {
    ExecutionSample {
        stack: u64,
        state: u64,
        correlation: Correlation,
    },
    WallClockSample {
        stack: u64,
        state: u64,
        samples: u32,
        correlation: Correlation,
    },
    AllocInNewTlab {
        stack: u64,
        tlab_size: u64,
        correlation: Correlation,
    },
    AllocOutsideTlab {
        stack: u64,
        allocation_size: u64,
        correlation: Correlation,
    },
    MonitorEnter {
        stack: u64,
        duration: u64,
        correlation: Correlation,
    },
    ThreadPark {
        stack: u64,
        duration: u64,
    },
    LiveObject {
        stack: u64,
    },
    AllocSample {
        stack: u64,
        weight: u64,
    },
    Malloc {
        stack: u64,
        size: u64,
    },
    ActiveSetting {
        name: Cow<'a, str>,
        value: Cow<'a, str>,
    },
}

pub(crate) fn merge_generated_class(symbol: Cow<'_, str>) -> Cow<'_, str> {
    symbol
}

pub(crate) struct Reader<'a> {
    pools: Pools<'a>,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(_body: &'a [u8]) -> Self {
        Reader {
            pools: Pools::default(),
        }
    }

    pub(crate) fn pools(&self) -> &Pools<'a> {
        &self.pools
    }

    pub(crate) fn ticks_per_second(&self) -> u64 {
        0
    }

    pub(crate) fn next_event(&mut self) -> Result<Option<Event<'a>>, String> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::super::test_support::unzipped;
    use super::super::{LabelsSnapshot, parse_jfr};
    use super::merge_generated_class;

    fn profiles(body: &[u8]) -> Result<usize, String> {
        parse_jfr(body, &LabelsSnapshot::default(), 0, 0, 100).map(|o| o.profiles.len())
    }

    #[test]
    fn j4_a_broken_recording_is_refused_with_the_librarys_text() {
        let prefix = "jfr parser ParseEvent error: ";
        assert_eq!(
            profiles(b"JFR"),
            Err(format!(
                "{prefix}error reading chunk header: unexpected EOF"
            ))
        );
        let example = unzipped("example.jfr.gz");
        assert_eq!(
            profiles(&example[..1000]),
            Err(format!("{prefix}error reading metadata: unexpected EOF"))
        );
        let mut zeroed = example.clone();
        zeroed[0] = 0;
        assert_eq!(
            profiles(&zeroed),
            Err(format!(
                "{prefix}error reading chunk header: invalid chunk magic: 4c5200"
            ))
        );
        let e = profiles(&example[..120_000]).expect_err("cut inside the constant pool");
        assert!(
            e.starts_with(&format!("{prefix}error reading CP: ")) && e.ends_with(" @ 55825"),
            "{e}"
        );
        assert_eq!(profiles(b""), Ok(0));
    }

    #[test]
    fn j7_generated_class_names_merge_as_the_library_merges_them() {
        let cases = [
            (
                "org/example/rideshare/EnclosingClass$$Lambda$4/1283928880",
                "org/example/rideshare/EnclosingClass$$Lambda_",
            ),
            (
                "org/example/rideshare/EnclosingClass$$Lambda$8/0x0000000800c01220",
                "org/example/rideshare/EnclosingClass$$Lambda_",
            ),
            ("Fib$$Lambda.0x00007ffa600c4da0", "Fib$$Lambda_"),
            (
                "io/opentelemetry/context/Context$$Lambda$lambda$wrap$2$2630016632.0x00000000000100dd",
                "io/opentelemetry/context/Context$$Lambda$lambda$wrap$2_",
            ),
            (
                "org/example/rideshare/EnclosingClass$$Lambda$lambda$doWork$1$1283928880",
                "org/example/rideshare/EnclosingClass$$Lambda$lambda$doWork$1_",
            ),
            (
                "org/example/rideshare/EnclosingClass$$Lambda",
                "org/example/rideshare/EnclosingClass$$Lambda",
            ),
            (
                "org/example/rideshare/EnclosingClass$$LambdaHelper",
                "org/example/rideshare/EnclosingClass$$LambdaHelper",
            ),
            (
                "java/util/concurrent/Executors$RunnableAdapter",
                "java/util/concurrent/Executors$RunnableAdapter",
            ),
            (
                "jdk/internal/reflect/GeneratedMethodAccessor31",
                "jdk/internal/reflect/GeneratedMethodAccessor_",
            ),
            (
                "foo/bar/Baz$$EnhancerBySpringCGLIB$$1234567890",
                "foo/bar/Baz$$EnhancerBySpringCGLIB$$_",
            ),
        ];
        for (src, want) in cases {
            assert_eq!(merge_generated_class(Cow::Borrowed(src)), want, "{src}");
        }
    }
}
