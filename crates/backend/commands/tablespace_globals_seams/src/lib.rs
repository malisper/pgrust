//! Seam declarations for the ambient per-backend globals, GUC string
//! readers, `src/port` path helpers, and the `TablespaceCreateLock` LWLock
//! that `commands/tablespace.c` reaches but whose owners are either unported
//! or carry no by-name accessor yet.
//!
//! Grouped here (rather than scattered into each owner's `-seams` crate) so a
//! single future provider — the init/GUC/lock substrate — can install them.
//! Until then a call panics loudly.

#![allow(non_snake_case)]

use ::seam_core::seam;
use ::types_core::primitive::Oid;
use ::types_error::PgResult;

/* --- src/port path helpers (src/port/path.c) --- */

seam!(
    /// `pstrdup(location); canonicalize_path(location)` (path.c) — Unix-ify and
    /// strip trailing slashes, returning the canonicalized copy.
    pub fn canonicalize_path(path: &str) -> PgResult<String>
);

seam!(
    /// `is_absolute_path(path)` (path.h).
    pub fn is_absolute_path(path: &str) -> PgResult<bool>
);

seam!(
    /// `path_is_prefix_of_path(parent, path)` (path.c).
    pub fn path_is_prefix_of_path(parent: &str, path: &str) -> PgResult<bool>
);

seam!(
    /// `pstrdup(p); get_parent_directory(p)` (path.c) — strip the last path
    /// component, returning the parent directory.
    pub fn get_parent_directory(path: &str) -> PgResult<String>
);

/* --- ambient per-backend session globals (miscinit.c / xlog.c) --- */

seam!(
    /// `MyDatabaseId` (the connected database's OID).
    pub fn MyDatabaseId() -> PgResult<Oid>
);

seam!(
    /// `MyDatabaseTableSpace` (the database's default tablespace OID).
    pub fn MyDatabaseTableSpace() -> PgResult<Oid>
);

seam!(
    /// `InRecovery` (xlog.c) — true during WAL replay / standby.
    pub fn InRecovery() -> PgResult<bool>
);

seam!(
    /// `allowSystemTableMods` (miscinit.c GUC).
    pub fn allowSystemTableMods() -> PgResult<bool>
);

seam!(
    /// `IsBinaryUpgrade` (the `--binary-upgrade` startup flag).
    pub fn IsBinaryUpgrade() -> PgResult<bool>
);

seam!(
    /// Read-and-clear `binary_upgrade_next_pg_tablespace_oid`
    /// (catalog/binary_upgrade.h): returns the override OID, then resets it to
    /// `InvalidOid` (mirrors the C `oid = global; global = InvalidOid;`).
    pub fn take_binary_upgrade_next_oid() -> PgResult<Oid>
);

/* --- GUC string variable readers (utils/guc_tables) --- */

seam!(
    /// The current value of the `default_tablespace` GUC (`""` when unset).
    pub fn default_tablespace() -> PgResult<String>
);

seam!(
    /// The current value of the `temp_tablespaces` GUC (`""` when unset).
    pub fn temp_tablespaces() -> PgResult<String>
);

seam!(
    /// The `allow_in_place_tablespaces` developer GUC.
    pub fn allow_in_place_tablespaces() -> PgResult<bool>
);

/* --- TablespaceCreateLock (storage/lmgr/lwlock.c built-in lock) --- */

seam!(
    /// `LWLockAcquire(TablespaceCreateLock, LW_EXCLUSIVE)` — serialize
    /// `TablespaceCreateDbspace` against `DROP TABLESPACE`.
    pub fn lwlock_acquire_tablespace_create() -> PgResult<()>
);

seam!(
    /// `LWLockRelease(TablespaceCreateLock)`.
    pub fn lwlock_release_tablespace_create() -> PgResult<()>
);
