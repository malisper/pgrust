//! Seam declarations for the `backend-utils-init-small` unit
//! (`utils/init/globals.c`, `utils/init/usercontext.c`): backend-global
//! variable reads.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `work_mem` (globals.c): the `work_mem` GUC — per-operation memory
    /// budget in kilobytes.
    pub fn work_mem() -> i32
);

seam_core::seam!(
    /// `MyDatabaseId` (globals.c): the OID of the database this backend is
    /// connected to (`InvalidOid` before `InitPostgres` selects one).
    pub fn my_database_id() -> types_core::primitive::Oid
);

seam_core::seam!(
    /// `MyDatabaseTableSpace` (globals.c): the default tablespace of the
    /// connected database.
    pub fn my_database_tablespace() -> types_core::primitive::Oid
);
