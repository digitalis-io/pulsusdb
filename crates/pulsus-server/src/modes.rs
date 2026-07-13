//! `PULSUS_MODE` → mounted-subsystem matrix (docs/architecture.md §1's mode
//! table), the single source of truth [`mounted`] is unit-tested against
//! directly (no socket needed) and [`mount_subsystems`] is the only caller
//! that turns the matrix into actual routes.

use std::collections::BTreeSet;

use axum::Router;
use pulsus_config::{Config, Mode};

use crate::app::AppState;
use crate::subsystems::{reader_router, ruler_router, writer_router};

/// A mountable subsystem (docs/architecture.md §1). `init` never reaches
/// this router at all — `main.rs` dispatches `Mode::Init` to
/// `schema_init::run` and exits before `serve::run` (and therefore
/// `build_router`) is ever called, so there is no `Subsystem` variant for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Subsystem {
    Writer,
    Reader,
    Ruler,
}

/// The subsystems `cfg.mode` mounts:
/// - `all`    → Writer + Reader (+ Ruler iff `cfg.ruler.enabled`)
/// - `writer` → Writer only
/// - `reader` → Reader only
/// - `init`   → none (unreachable in practice, see [`Subsystem`]'s docs)
pub(crate) fn mounted(cfg: &Config) -> BTreeSet<Subsystem> {
    let mut set = BTreeSet::new();
    match cfg.mode {
        Mode::All => {
            set.insert(Subsystem::Writer);
            set.insert(Subsystem::Reader);
            if cfg.ruler.enabled {
                set.insert(Subsystem::Ruler);
            }
        }
        Mode::Writer => {
            set.insert(Subsystem::Writer);
        }
        Mode::Reader => {
            set.insert(Subsystem::Reader);
        }
        Mode::Init => {}
    }
    set
}

/// Merges each subsystem in `mounted(cfg)` onto `router`. Contract for later
/// issues (see `subsystems.rs`): the stub routers become the real
/// pulsus-{write,read,ruler} routers without this function changing.
pub(crate) fn mount_subsystems(router: Router<AppState>, cfg: &Config) -> Router<AppState> {
    let mut router = router;
    for subsystem in mounted(cfg) {
        router = match subsystem {
            Subsystem::Writer => router.merge(writer_router()),
            Subsystem::Reader => router.merge(reader_router()),
            Subsystem::Ruler => router.merge(ruler_router()),
        };
    }
    router
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_with_mode(mode: Mode) -> Config {
        Config {
            mode,
            ..Config::default()
        }
    }

    #[test]
    fn all_mode_mounts_writer_and_reader_but_not_ruler_by_default() {
        let cfg = cfg_with_mode(Mode::All);
        assert_eq!(
            mounted(&cfg),
            BTreeSet::from([Subsystem::Writer, Subsystem::Reader])
        );
    }

    #[test]
    fn all_mode_mounts_ruler_too_when_enabled() {
        let mut cfg = cfg_with_mode(Mode::All);
        cfg.ruler.enabled = true;
        assert_eq!(
            mounted(&cfg),
            BTreeSet::from([Subsystem::Writer, Subsystem::Reader, Subsystem::Ruler])
        );
    }

    #[test]
    fn writer_mode_mounts_only_writer() {
        let cfg = cfg_with_mode(Mode::Writer);
        assert_eq!(mounted(&cfg), BTreeSet::from([Subsystem::Writer]));
    }

    #[test]
    fn writer_mode_ignores_ruler_enabled() {
        let mut cfg = cfg_with_mode(Mode::Writer);
        cfg.ruler.enabled = true;
        assert_eq!(mounted(&cfg), BTreeSet::from([Subsystem::Writer]));
    }

    #[test]
    fn reader_mode_mounts_only_reader() {
        let cfg = cfg_with_mode(Mode::Reader);
        assert_eq!(mounted(&cfg), BTreeSet::from([Subsystem::Reader]));
    }

    #[test]
    fn init_mode_mounts_nothing() {
        let cfg = cfg_with_mode(Mode::Init);
        assert_eq!(mounted(&cfg), BTreeSet::new());
    }
}
