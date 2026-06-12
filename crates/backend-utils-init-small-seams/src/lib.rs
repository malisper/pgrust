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
    /// `MyProcNumber` (globals.c).
    pub fn my_proc_number() -> types_core::ProcNumber
);

seam_core::seam!(
    /// `MyDatabaseId` (globals.c).
    pub fn my_database_id() -> types_core::Oid
);

seam_core::seam!(
    /// `MyDatabaseTableSpace` (globals.c).
    pub fn my_database_table_space() -> types_core::Oid
);
