//! Test-only: a hook a writer's `Shared` runs from the middle of its own
//! teardown, and the pieces the cases around it share.
//!
//! A `Shared` drops its fields in declaration order, and the landing queue's
//! sender comes before the drain boundary. [`TeardownProbe`] is declared
//! between the two, so its hook runs after the sender is gone and before the
//! boundary is: the window in which only the early cancel in `Shared`'s own
//! `Drop` stops an insert worker on another thread from starting an insert.

use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pulsus_clickhouse::{ChError, ChRow};

use crate::writer::table::BlockInserter;

type Hook = Box<dyn FnOnce() + Send>;

/// Runs its hook, if one was set, when it is dropped.
#[derive(Default)]
pub(crate) struct TeardownProbe(Mutex<Option<Hook>>);

impl TeardownProbe {
    /// Sets the hook the drop runs.
    pub(crate) fn set(&self, hook: impl FnOnce() + Send + 'static) {
        *self.0.lock().expect("probe mutex poisoned") = Some(Box::new(hook));
    }
}

impl Drop for TeardownProbe {
    fn drop(&mut self) {
        let hook = self.0.get_mut().expect("probe mutex poisoned").take();
        if let Some(hook) = hook {
            hook();
        }
    }
}

/// A landing inserter that reports each call and then parks until the case
/// releases it.
pub(crate) struct ParkingInserter {
    /// One permit per call, added as the insert begins.
    pub(crate) entered: tokio::sync::Semaphore,
    /// The case adds the permit that lets an insert return `Ok`.
    pub(crate) release: tokio::sync::Semaphore,
}

impl ParkingInserter {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(ParkingInserter {
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        })
    }
}

impl<R: ChRow> BlockInserter<R> for ParkingInserter {
    fn insert<'a>(
        &'a self,
        _table: &'a str,
        _rows: &'a [R],
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), ChError>> + Send + 'a>> {
        Box::pin(async move {
            self.entered.add_permits(1);
            self.release
                .acquire()
                .await
                .expect("the release semaphore is never closed")
                .forget();
            Ok(())
        })
    }
}

/// An inserter that answers every call at once, for the tables a case does
/// not watch.
pub(crate) struct OkInserter;

impl<R: ChRow> BlockInserter<R> for OkInserter {
    fn insert<'a>(
        &'a self,
        _table: &'a str,
        _rows: &'a [R],
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), ChError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

/// A fresh spool root for one case.
pub(crate) fn spool_root(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pulsus-teardown-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create the spool root");
    dir
}

/// The spool records under `poison/<table>`, parsed.
pub(crate) fn poison_records(root: &Path, table: &str) -> Vec<serde_json::Value> {
    let Ok(dir) = std::fs::read_dir(root.join("poison").join(table)) else {
        return Vec::new();
    };
    dir.filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|path| path.extension().and_then(|e| e.to_str()) == Some("json"))
        .filter_map(|path| std::fs::read(path).ok())
        .filter_map(|bytes| serde_json::from_slice(&bytes).ok())
        .collect()
}

/// The hook for a writer whose one insert worker is parked inside block A's
/// insert, with block B queued behind it and the case's own permit for A's
/// entry already taken.
///
/// It lets every insert return, then waits, on the dropping thread, until the
/// worker has either started an insert for B or filed B never sent. Without
/// the early cancel no deadline exists yet, so A commits, the worker takes B
/// and its attempt is authorized; with it, A is abandoned and B refused.
pub(crate) fn release_then_wait_for_b(
    inserter: Arc<ParkingInserter>,
    root: PathBuf,
    table: &'static str,
) -> impl FnOnce() + Send + 'static {
    move || {
        inserter.release.add_permits(2);
        let limit = std::time::Instant::now() + Duration::from_secs(10);
        while inserter.entered.available_permits() == 0 && poison_records(&root, table).is_empty() {
            assert!(
                std::time::Instant::now() < limit,
                "B was neither sent nor filed inside the teardown"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

/// Waits, on the wall clock, until B is filed, then asserts that no insert started
/// for it and that it was filed never sent.
pub(crate) async fn assert_b_filed_never_sent(
    inserter: &ParkingInserter,
    root: &Path,
    table: &str,
) {
    let limit = std::time::Instant::now() + Duration::from_secs(10);
    while poison_records(root, table).is_empty() && inserter.entered.available_permits() == 0 {
        assert!(std::time::Instant::now() < limit, "B was never filed");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(
        inserter.entered.available_permits(),
        0,
        "no insert was started for B once the teardown had begun"
    );
    let poison = poison_records(root, table);
    assert_eq!(poison.len(), 1, "B is filed never sent: {poison:?}");
    assert_eq!(
        poison[0]["error"].as_str(),
        Some(crate::writer::landing::MSG_SHUTDOWN_QUEUED)
    );
}
