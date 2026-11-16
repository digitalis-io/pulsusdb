//! `ProfileWriter`: one profiles push is one `INSERT` of one block into
//! `profile_landing`. Not built yet.

use std::sync::Arc;
use std::time::Duration;

use pulsus_clickhouse::ChClient;
use pulsus_config::WriterConfig;
use pulsus_model::Tenant;

use crate::ingest::profiles::{ParsedProfileLanding, ProfileSink};
use crate::ingest::{AdmitRefusal, FlushWait, PushHeaders};
use crate::writer::config::WriterRuntime;
use crate::writer::metrics::TraceWriterMetricsSnapshot;
use crate::writer::push_dedup::PushDedup;
use crate::writer::rows::ProfileLandingRow;
use crate::writer::table::{BlockInserter, ChBlockInserter};
use crate::writer::{AdmitMode, Admitted};

/// The one table a [`ProfileWriter`] inserts into.
#[derive(Debug, Clone)]
pub struct ProfileWriterTables {
    pub landing: Arc<str>,
}

impl ProfileWriterTables {
    /// The bare landing table's name.
    pub fn profiles_default() -> Self {
        ProfileWriterTables {
            landing: Arc::from(""),
        }
    }

    /// The landing table for a deployment.
    pub fn landing_for(_dist_suffix: Option<&str>) -> Self {
        Self::profiles_default()
    }
}

/// Implements [`ProfileSink`] over one landing table.
pub struct ProfileWriter {
    _runtime: Arc<WriterRuntime>,
}

impl ProfileWriter {
    /// Production constructor against the landing table.
    pub fn new(client: Arc<ChClient>, cfg: &WriterConfig) -> Self {
        Self::new_with_tables(client, cfg, ProfileWriterTables::profiles_default())
    }

    /// [`Self::new`], but against `tables`.
    pub fn new_with_tables(
        client: Arc<ChClient>,
        cfg: &WriterConfig,
        tables: ProfileWriterTables,
    ) -> Self {
        Self::with_inserters_with_tables(Arc::new(ChBlockInserter::new(client)), cfg, tables)
    }

    /// Test/mock constructor: any landing [`BlockInserter`].
    pub fn with_inserters_with_tables(
        landing_inserter: Arc<dyn BlockInserter<ProfileLandingRow>>,
        cfg: &WriterConfig,
        tables: ProfileWriterTables,
    ) -> Self {
        Self::with_inserters_and_runtime(landing_inserter, WriterRuntime::from_config(cfg), tables)
    }

    /// [`Self::with_inserters_with_tables`], but against a caller-supplied
    /// [`WriterRuntime`].
    #[doc(hidden)]
    pub fn with_inserters_and_runtime(
        _landing_inserter: Arc<dyn BlockInserter<ProfileLandingRow>>,
        runtime: WriterRuntime,
        _tables: ProfileWriterTables,
    ) -> Self {
        ProfileWriter {
            _runtime: Arc::new(runtime),
        }
    }

    fn admit_batch(
        &self,
        _tenant: &Tenant,
        _landing: ParsedProfileLanding,
        _mode: AdmitMode,
        _push: PushHeaders,
    ) -> Result<Admitted, AdmitRefusal> {
        Err(AdmitRefusal::Backpressure)
    }

    /// A point-in-time metrics snapshot.
    pub fn metrics(&self) -> TraceWriterMetricsSnapshot {
        TraceWriterMetricsSnapshot::default()
    }

    /// This writer's push-suppression index.
    pub fn dedup(&self) -> Option<&Arc<PushDedup>> {
        None
    }

    /// Graceful shutdown.
    pub async fn shutdown(&self, _deadline: Duration) {}
}

impl ProfileSink for ProfileWriter {
    fn admit(
        &self,
        tenant: &Tenant,
        landing: ParsedProfileLanding,
        push: PushHeaders,
    ) -> Result<(), AdmitRefusal> {
        self.admit_batch(tenant, landing, AdmitMode::Async, push)
            .map(|_| ())
    }

    fn admit_flush(
        &self,
        tenant: &Tenant,
        landing: ParsedProfileLanding,
        push: PushHeaders,
    ) -> Result<FlushWait, AdmitRefusal> {
        match self.admit_batch(tenant, landing, AdmitMode::Sync, push)? {
            Admitted::Stored(_) | Admitted::Suppressed(_) => Ok(FlushWait::new(async { Ok(()) })),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::path::Path;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use pulsus_clickhouse::{ChError, QuerySettings};
    use pulsus_profiles_wire::google_v1::{Function, Line, Location, Profile, Sample, ValueType};

    use super::*;
    use crate::ingest::profiles::{
        LandingEntry, LandingLocation, LandingSample, LandingSeries, LandingStack, ProfileInput,
    };
    use crate::writer::teardown_probe::spool_root;

    // -- the mock inserter --------------------------------------------

    /// What one insert call does.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Act {
        Ok,
        /// A non-retryable error: provably not committed.
        Poison,
        /// A failure after the block was sent: its fate is unknown.
        Uncertain,
    }

    /// A scripted mock [`BlockInserter`], `tests/trace_landing.rs`'s: the
    /// script is consumed front to back and its last step repeats. Records
    /// every call's table, rows and settings.
    struct MockInserter {
        script: Vec<Act>,
        calls: AtomicUsize,
        rows: Mutex<Vec<Vec<ProfileLandingRow>>>,
        settings: Mutex<Vec<Vec<(String, String)>>>,
        tables: Mutex<Vec<String>>,
        empty: QuerySettings,
    }

    impl MockInserter {
        fn new(script: Vec<Act>) -> Arc<Self> {
            Arc::new(MockInserter {
                script,
                calls: AtomicUsize::new(0),
                rows: Mutex::new(Vec::new()),
                settings: Mutex::new(Vec::new()),
                tables: Mutex::new(Vec::new()),
                empty: QuerySettings::new(),
            })
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        fn rows_of(&self, call: usize) -> Vec<ProfileLandingRow> {
            self.rows.lock().expect("mock mutex")[call].clone()
        }

        fn settings_of(&self, call: usize) -> Vec<(String, String)> {
            self.settings.lock().expect("mock mutex")[call].clone()
        }

        fn tables(&self) -> Vec<String> {
            self.tables.lock().expect("mock mutex").clone()
        }
    }

    impl BlockInserter<ProfileLandingRow> for MockInserter {
        fn insert<'a>(
            &'a self,
            table: &'a str,
            rows: &'a [ProfileLandingRow],
        ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
            self.insert_with(table, rows, &self.empty)
        }

        fn insert_with<'a>(
            &'a self,
            table: &'a str,
            rows: &'a [ProfileLandingRow],
            extra: &'a QuerySettings,
        ) -> Pin<Box<dyn Future<Output = Result<(), ChError>> + Send + 'a>> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            self.tables
                .lock()
                .expect("mock mutex")
                .push(table.to_string());
            self.rows.lock().expect("mock mutex").push(rows.to_vec());
            self.settings.lock().expect("mock mutex").push(
                extra
                    .entries()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            );
            let act = *self
                .script
                .get(call)
                .or(self.script.last())
                .expect("a non-empty script");
            Box::pin(async move {
                match act {
                    Act::Ok => Ok(()),
                    Act::Poison => Err(ChError::Decode("mock poison".to_string())),
                    Act::Uncertain => Err(ChError::InsertUncertain("mock uncertain".to_string())),
                }
            })
        }
    }

    // -- the fixture --------------------------------------------------

    fn now_ms() -> i64 {
        i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .expect("a representable stamp")
    }

    /// Part A §0's profile: two sample types, locations `main`, `a`, `b`,
    /// stacks `main→a` and `main→b`, each value multiplied by `scale`.
    fn example_profile(time_nanos: i64, scale: i64) -> Profile {
        let st = [
            "",
            "samples",
            "count",
            "cpu",
            "nanoseconds",
            "main",
            "a",
            "b",
            "main.go",
        ];
        Profile {
            sample_type: vec![
                ValueType { r#type: 1, unit: 2 },
                ValueType { r#type: 3, unit: 4 },
            ],
            sample: vec![
                Sample {
                    location_id: vec![2, 1],
                    value: vec![3 * scale, 30_000_000 * scale],
                    label: Vec::new(),
                },
                Sample {
                    location_id: vec![3, 1],
                    value: vec![scale, 10_000_000 * scale],
                    label: Vec::new(),
                },
            ],
            function: (1..=3u64)
                .map(|id| Function {
                    id,
                    name: 4 + id as i64,
                    system_name: 0,
                    filename: 8,
                    start_line: 1,
                })
                .collect(),
            location: (1..=3u64)
                .map(|id| Location {
                    id,
                    mapping_id: 0,
                    address: 0,
                    line: vec![Line {
                        function_id: id,
                        line: 10,
                    }],
                    is_folded: false,
                })
                .collect(),
            string_table: st.iter().map(|s| (*s).to_owned()).collect(),
            time_nanos,
            duration_nanos: 10_000_000_000,
            period_type: Some(ValueType { r#type: 3, unit: 4 }),
            period: 10_000_000,
            ..Profile::default()
        }
    }

    fn example_input(scale: i64) -> ProfileInput {
        ProfileInput {
            labels: vec![
                ("__name__".to_owned(), "process_cpu".to_owned()),
                ("service_name".to_owned(), "checkout".to_owned()),
            ],
            profile: example_profile(now_ms() * 1_000_000, scale),
            decompressed_bytes: 1_000,
            id: None,
            annotations: Vec::new(),
        }
    }

    /// §0's push P, parsed by part A; `scale` 2 is P′.
    fn push(scale: i64) -> ParsedProfileLanding {
        crate::parse_profiles(vec![example_input(scale)], now_ms()).expect("the push is accepted")
    }

    fn tenant(name: &str) -> Tenant {
        Tenant::from_header(
            Some(&axum::http::HeaderValue::from_str(name).expect("a header value")),
            false,
        )
        .expect("a valid tenant")
    }

    fn config() -> WriterConfig {
        WriterConfig {
            landing_batch_ms: 0,
            ingest_dedup: false,
            profile_landing_retries: 0,
            ..WriterConfig::default()
        }
    }

    fn writer(cfg: &WriterConfig, root: &Path, inserter: Arc<MockInserter>) -> ProfileWriter {
        let mut runtime = WriterRuntime::from_config(cfg);
        runtime.spool_dir = root.to_path_buf();
        ProfileWriter::with_inserters_and_runtime(
            inserter,
            runtime,
            ProfileWriterTables::profiles_default(),
        )
    }

    async fn sync_push(
        writer: &ProfileWriter,
        who: &Tenant,
        landing: ParsedProfileLanding,
    ) -> Result<(), crate::error::LogsIngestError> {
        writer
            .admit_flush(who, landing, PushHeaders::default())
            .expect("the push is admitted")
            .await
    }

    fn count_kind(rows: &[ProfileLandingRow], kind: u8) -> usize {
        rows.iter().filter(|r| r.kind == kind).count()
    }

    fn spool_files(root: &Path, kind: &str) -> usize {
        std::fs::read_dir(root.join(kind).join("profile_landing"))
            .map(|d| {
                d.flatten()
                    .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("json"))
                    .count()
            })
            .unwrap_or(0)
    }

    /// **W1.** The stack and location cache is keyed by tenant and is
    /// emptied by a failed insert, `tests/log_landing.rs`'s
    /// forget-on-failure case for the profile writer. Both failures: an
    /// unknown fate and a refused insert.
    #[tokio::test(start_paused = true)]
    async fn the_stack_cache_is_per_tenant_and_forgets_on_failure() {
        for (failure, spool_kind) in [(Act::Uncertain, "uncertain"), (Act::Poison, "poison")] {
            let root = spool_root("profile-cache");
            let inserter = MockInserter::new(vec![Act::Ok, Act::Ok, Act::Ok, failure, Act::Ok]);
            let writer = writer(&config(), &root, inserter.clone());
            let (t1, t2) = (tenant("t1"), tenant("t2"));

            sync_push(&writer, &t1, push(1))
                .await
                .expect("P as t1 commits");
            sync_push(&writer, &t1, push(2))
                .await
                .expect("P′ as t1 commits");
            sync_push(&writer, &t2, push(1))
                .await
                .expect("P as t2 commits");
            sync_push(&writer, &t1, push(2))
                .await
                .expect_err("the fourth insert fails");
            sync_push(&writer, &t1, push(2))
                .await
                .expect("P′ as t1 commits again");

            assert_eq!(inserter.call_count(), 5, "{failure:?}: one insert per push");
            let per_call: Vec<(usize, usize)> = (0..5)
                .map(|call| {
                    let rows = inserter.rows_of(call);
                    (
                        count_kind(&rows, ProfileLandingRow::KIND_STACK),
                        count_kind(&rows, ProfileLandingRow::KIND_LOCATION),
                    )
                })
                .collect();
            assert_eq!(
                per_call,
                vec![(2, 3), (0, 0), (2, 3), (0, 0), (2, 3)],
                "{failure:?}: stack and location rows per insert"
            );
            assert_eq!(
                spool_files(&root, spool_kind),
                1,
                "{failure:?}: the failed block was spooled"
            );

            writer.shutdown(Duration::from_secs(2)).await;
            std::fs::remove_dir_all(&root).ok();
        }
    }

    /// **W2.** One push is one insert into the landing table, carrying every
    /// kind, the tenant on every row, and one minted version-7 id shared by
    /// the input's entries and their samples.
    #[tokio::test(start_paused = true)]
    async fn one_push_is_one_insert_into_the_landing_table() {
        let root = spool_root("profile-one-insert");
        let inserter = MockInserter::new(vec![Act::Ok]);
        let writer = writer(&config(), &root, inserter.clone());

        sync_push(&writer, &tenant("t1"), push(1))
            .await
            .expect("the push commits");

        assert_eq!(inserter.call_count(), 1, "one insert");
        assert_eq!(inserter.tables(), vec!["profile_landing".to_string()]);
        let rows = inserter.rows_of(0);
        let kinds: Vec<usize> = (0..=4u8).map(|k| count_kind(&rows, k)).collect();
        assert_eq!(kinds, vec![4, 2, 2, 2, 3], "rows by kind 0 to 4");
        assert!(
            rows.iter().all(|r| &*r.org_id == "t1"),
            "every row carries the tenant"
        );
        let ids: Vec<(u64, u64)> = rows
            .iter()
            .filter(|r| r.kind == ProfileLandingRow::KIND_ENTRY)
            .map(|r| r.profile_id)
            .collect();
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], ids[1], "one input, one id");
        assert_ne!(ids[0], (0, 0), "the id is minted");
        assert_eq!((ids[0].0 >> 12) & 0xF, 7, "a version-7 UUID");
        assert_eq!(ids[0].1 >> 62, 0b10, "the UUID variant");
        assert!(
            rows.iter()
                .filter(|r| r.kind == ProfileLandingRow::KIND_SAMPLE)
                .all(|r| r.profile_id == ids[0]),
            "every sample carries its entry's id"
        );

        writer.shutdown(Duration::from_secs(2)).await;
        std::fs::remove_dir_all(&root).ok();
    }

    /// **W3.** The bare name without a suffix, the wrapper with one.
    #[test]
    fn landing_for_names_the_wrapper_only_when_given_a_suffix() {
        assert_eq!(
            &*ProfileWriterTables::landing_for(None).landing,
            "profile_landing"
        );
        assert_eq!(
            &*ProfileWriterTables::landing_for(Some("_d")).landing,
            "profile_landing_d"
        );
        assert_eq!(
            &*ProfileWriterTables::profiles_default().landing,
            "profile_landing"
        );
    }

    /// **W4.** The insert carries the landing pins, and a resend carries the
    /// very token the first attempt did.
    #[tokio::test(start_paused = true)]
    async fn the_insert_carries_the_landing_pins_and_one_token_across_resends() {
        let root = spool_root("profile-token");
        let inserter = MockInserter::new(vec![Act::Uncertain, Act::Ok]);
        let cfg = WriterConfig {
            profile_landing_retries: 1,
            ..config()
        };
        let writer = writer(&cfg, &root, inserter.clone());

        sync_push(&writer, &tenant("t1"), push(1))
            .await
            .expect("the resend commits");

        assert_eq!(inserter.call_count(), 2, "one attempt and one resend");
        let value = |call: usize, key: &str| {
            inserter
                .settings_of(call)
                .into_iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v)
                .unwrap_or_else(|| panic!("call {call} carries no {key}"))
        };
        let token = value(0, "insert_deduplication_token");
        assert!(!token.is_empty());
        assert_eq!(value(1, "insert_deduplication_token"), token);
        assert_eq!(
            value(0, "max_insert_block_size"),
            cfg.profile_landing_max_rows.to_string()
        );
        let pins = QuerySettings::landing_insert(&token, cfg.profile_landing_max_rows);
        let want: Vec<(String, String)> = pins
            .entries()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(inserter.settings_of(0), want, "the landing pins, whole");

        writer.shutdown(Duration::from_secs(2)).await;
        std::fs::remove_dir_all(&root).ok();
    }

    /// **W5.** A push with no entries is answered `Ok`, makes no insert and
    /// is charged nothing.
    #[tokio::test(start_paused = true)]
    async fn an_empty_push_is_a_success_and_is_charged_for_nothing() {
        let root = spool_root("profile-empty");
        let inserter = MockInserter::new(vec![Act::Ok]);
        let writer = writer(&config(), &root, inserter.clone());

        let wait = writer.admit_flush(
            &tenant("t1"),
            ParsedProfileLanding::default(),
            PushHeaders::default(),
        );
        assert!(wait.is_ok(), "the empty push is admitted: {wait:?}");
        assert_eq!(writer.metrics().queue_bytes, 0, "nothing is charged");
        wait.expect("admitted").await.expect("answered Ok");
        assert_eq!(inserter.call_count(), 0, "no insert");

        writer.shutdown(Duration::from_secs(2)).await;
        std::fs::remove_dir_all(&root).ok();
    }

    /// A hand-built push of 2 series, 2 entries, `samples` samples, 3 stacks
    /// and 3 locations.
    fn sized_push(samples: usize) -> ParsedProfileLanding {
        let series = |t: &str| LandingSeries {
            type_id: Arc::from(t),
            service: Arc::from("checkout"),
            fingerprint: pulsus_model::Fingerprint::from_raw(7),
            timestamp_ns: now_ms() * 1_000_000,
            time_defaulted: false,
            labels: "{}".to_owned(),
        };
        let entry = |s: u32| LandingEntry {
            series: s,
            input: 0,
            profile_id: None,
            total: 1,
            duration_ns: 1,
            annotation_keys: Vec::new(),
            annotation_values: Vec::new(),
        };
        ParsedProfileLanding {
            series: vec![series("a"), series("b")],
            entries: vec![entry(0), entry(1)],
            samples: (0..samples)
                .map(|i| LandingSample {
                    entry: (i % 2) as u32,
                    stack_id: (i % 3) as u128 + 1,
                    value: 1,
                    span_id: i as u64,
                    trace_id: [0; 16],
                })
                .collect(),
            stacks: (1..=3u128)
                .map(|id| LandingStack {
                    stack_id: id,
                    node_ids: vec![id],
                    names: vec![Arc::from("f")],
                    location_ids: vec![id],
                })
                .collect(),
            locations: (1..=3u128)
                .map(|id| LandingLocation {
                    location_id: id,
                    mapping_file: Arc::from(""),
                    mapping_build_id: Arc::from(""),
                    mapping_start: 0,
                    mapping_limit: 0,
                    mapping_offset: 0,
                    address: 0,
                    line_functions: vec![Arc::from("f")],
                    line_system_names: vec![Arc::from("f")],
                    line_files: vec![Arc::from("x.go")],
                    line_start_lines: vec![1],
                    line_numbers: vec![1],
                })
                .collect(),
        }
    }

    /// **W6.** A push whose rows reach the row ceiling is refused whole and
    /// makes no insert.
    #[tokio::test(start_paused = true)]
    async fn a_push_at_the_row_ceiling_is_refused_whole() {
        let root = spool_root("profile-ceiling");
        let inserter = MockInserter::new(vec![Act::Ok]);
        let cfg = WriterConfig {
            profile_landing_max_rows: 1_000,
            ..config()
        };
        let writer = writer(&cfg, &root, inserter.clone());

        let landing = sized_push(990);
        assert_eq!(landing.total_rows(), 1_000);
        match writer.admit_flush(&tenant("t1"), landing, PushHeaders::default()) {
            Err(AdmitRefusal::PushTooLarge {
                rows: 1_000,
                row_limit: 1_000,
                ..
            }) => {}
            other => panic!("expected PushTooLarge at 1000 of 1000 rows, got {other:?}"),
        }
        // One row fewer fits.
        writer
            .admit_flush(&tenant("t1"), sized_push(989), PushHeaders::default())
            .expect("999 rows are admitted")
            .await
            .expect("and commit");
        assert_eq!(inserter.call_count(), 1, "only the push under the ceiling");

        writer.shutdown(Duration::from_secs(2)).await;
        std::fs::remove_dir_all(&root).ok();
    }

    /// **W7.** The landing counter is subtracted nowhere in this module:
    /// `writer::landing`'s `LandingBlock::release` is the one place, as for
    /// traces.
    #[test]
    fn the_profile_landing_reservation_is_released_in_one_place() {
        const SOURCE: &str = include_str!("profile.rs");
        let body = SOURCE
            .split_once("#[cfg(test)]")
            .map(|(before, _)| before)
            .unwrap_or(SOURCE);
        let subtractions: Vec<&str> = body
            .lines()
            .filter(|line| line.contains("fetch_sub") || line.contains("saturating_sub"))
            .collect();
        assert!(
            subtractions.is_empty(),
            "this module must subtract the landing counter nowhere: {subtractions:?}"
        );
    }

    /// **W10.** One id is minted per input profile: a push of two inputs,
    /// each with two sample types and no id, has four entries under two
    /// distinct ids, and each sample carries its entry's.
    #[tokio::test(start_paused = true)]
    async fn one_id_is_minted_per_input_profile() {
        let root = spool_root("profile-ids");
        let inserter = MockInserter::new(vec![Act::Ok]);
        let writer = writer(&config(), &root, inserter.clone());

        let landing = crate::parse_profiles(vec![example_input(1), example_input(1)], now_ms())
            .expect("the push is accepted");
        let inputs: Vec<u32> = landing.entries.iter().map(|e| e.input).collect();
        assert_eq!(inputs, vec![0, 0, 1, 1], "two entries per input");
        sync_push(&writer, &tenant("t1"), landing)
            .await
            .expect("the push commits");

        let rows = inserter.rows_of(0);
        let entries: Vec<&ProfileLandingRow> = rows
            .iter()
            .filter(|r| r.kind == ProfileLandingRow::KIND_ENTRY)
            .collect();
        assert_eq!(entries.len(), 4);
        let (a, b) = (entries[0].profile_id, entries[2].profile_id);
        assert_eq!(entries[1].profile_id, a, "input 0's entries share one id");
        assert_eq!(entries[3].profile_id, b, "input 1's entries share one id");
        assert_ne!(a, b, "two inputs, two ids");
        assert_ne!(a, (0, 0));
        assert_ne!(b, (0, 0));
        for id in [a, b] {
            let types: Vec<&str> = entries
                .iter()
                .filter(|e| e.profile_id == id)
                .map(|e| &*e.type_id)
                .collect();
            let samples: Vec<&ProfileLandingRow> = rows
                .iter()
                .filter(|r| r.kind == ProfileLandingRow::KIND_SAMPLE && r.profile_id == id)
                .collect();
            assert_eq!(samples.len(), 4, "two types by two stacks under each id");
            assert!(
                samples.iter().all(|s| types.contains(&&*s.type_id)),
                "a sample's id is its own entry's"
            );
        }
        assert_eq!(
            count_kind(&rows, ProfileLandingRow::KIND_SAMPLE),
            8,
            "no sample is left without an id"
        );

        writer.shutdown(Duration::from_secs(2)).await;
        std::fs::remove_dir_all(&root).ok();
    }
}
