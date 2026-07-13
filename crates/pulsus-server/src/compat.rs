//! `PULSUS_COMPAT_ENDPOINTS` mounting contract (docs/api.md §8). No alias
//! ships in this issue's scope — [`apply_aliases`] is the documented
//! extension point later issues push `(alias, native)` pairs into, gated on
//! `cfg.compat_endpoints`.

use axum::Router;
use pulsus_config::Config;

use crate::app::AppState;

/// Mounts every enabled compatibility alias onto `router`. A no-op today:
/// every alias in docs/api.md §8's table ships in a later milestone (M1+).
pub(crate) fn apply_aliases(router: Router<AppState>, cfg: &Config) -> Router<AppState> {
    if !cfg.compat_endpoints {
        return router;
    }
    // Future issues add `.route(alias_path, native_handler)` calls here,
    // one per docs/api.md §8 row, once the native handler exists.
    router
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_aliases_is_a_no_op_regardless_of_the_flag() {
        // No alias exists yet, so both settings of `compat_endpoints` must
        // leave the router's route set unchanged (only its *presence* is
        // gated for future aliases).
        let disabled = Config {
            compat_endpoints: false,
            ..Config::default()
        };
        let enabled = Config {
            compat_endpoints: true,
            ..Config::default()
        };
        let _ = apply_aliases(Router::new(), &disabled);
        let _ = apply_aliases(Router::new(), &enabled);
    }
}
