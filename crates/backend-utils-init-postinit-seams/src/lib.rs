//! Seam declarations for the `backend-utils-init-postinit` unit
//! (`utils/init/postinit.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use types_error::PgResult;

seam_core::seam!(
    /// `InitializeMaxBackends()` (postinit.c): compute and set `MaxBackends`
    /// from the configured connection/worker limits; `ereport(ERROR)` when the
    /// total exceeds `MAX_BACKENDS`.
    pub fn initialize_max_backends() -> PgResult<()>
);

seam_core::seam!(
    /// `InitializeFastPathLocks()` (postinit.c): size the per-backend
    /// fast-path lock arrays from `max_locks_per_xact`. Infallible.
    pub fn initialize_fast_path_locks()
);

seam_core::seam!(
    /// `BaseInit()` (postinit.c): early per-backend initialization shared by
    /// all backend kinds. `ereport(ERROR)` on a setup failure.
    pub fn base_init() -> PgResult<()>
);

seam_core::seam!(
    /// `InitPostgres(NULL, InvalidOid, NULL, InvalidOid, 0, NULL)` (postinit.c)
    /// as called from bootstrap mode: complete backend initialization without
    /// selecting a database. `ereport(ERROR/FATAL)` on failure.
    pub fn init_postgres_bootstrap() -> PgResult<()>
);
