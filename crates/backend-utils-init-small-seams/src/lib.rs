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
    /// `MyDatabaseId` (globals.c): the connected database's OID. Pure
    /// global read.
    pub fn my_database_id() -> types_core::Oid
);

seam_core::seam!(
    /// `MyProcNumber` (globals.c): this backend's proc number (used to form
    /// `pg_temp_%d`). Pure global read.
    pub fn my_proc_number() -> types_core::ProcNumber
);
