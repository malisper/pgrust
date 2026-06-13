//! Seam declarations for the `backend-utils-init-postinit` unit
//! (`utils/init/postinit.c`): per-backend initialization shared by all
//! backend types (`BaseInit`) and the database/role attach (`InitPostgres`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `BaseInit()` (postinit.c) — early per-backend initialization (smgr,
    /// pgstat, snapshot/portal state, file access) shared by every backend
    /// type. `ereport(ERROR)`/`FATAL` paths give the `PgResult`.
    pub fn base_init() -> PgResult<()>
);

seam_core::seam!(
    /// `InitPostgres(dbname, InvalidOid, username, InvalidOid, init_flags,
    /// NULL)` (postinit.c) — attach a background worker to a database by name,
    /// using the role named by `username` (`None` = bootstrap superuser /
    /// no specific database). `init_flags` is the `INIT_PG_*` bitmask. The
    /// connection setup `ereport(FATAL/ERROR)`s on many paths.
    pub fn init_postgres_by_name(
        dbname: Option<&str>,
        username: Option<&str>,
        init_flags: u32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `InitPostgres(NULL, dboid, NULL, useroid, init_flags, NULL)`
    /// (postinit.c) — attach a background worker to a database by OID using
    /// the role OID `useroid`.
    pub fn init_postgres_by_oid(dboid: Oid, useroid: Oid, init_flags: u32) -> PgResult<()>
);
