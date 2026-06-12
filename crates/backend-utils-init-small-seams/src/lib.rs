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
    /// Read `MyDatabaseId` (globals.c).
    pub fn my_database_id() -> types_core::Oid
);

seam_core::seam!(
    /// Read `MyDatabaseTableSpace` (globals.c).
    pub fn my_database_table_space() -> types_core::Oid
);

seam_core::seam!(
    /// Read `MyProcNumber` (globals.c).
    pub fn my_proc_number() -> types_core::ProcNumber
);

seam_core::seam!(
    /// `HOLD_INTERRUPTS()` — increment `InterruptHoldoffCount` (globals.c).
    pub fn hold_interrupts()
);

seam_core::seam!(
    /// `RESUME_INTERRUPTS()` — decrement `InterruptHoldoffCount`.
    pub fn resume_interrupts()
);

seam_core::seam!(
    /// `START_CRIT_SECTION()` — increment `CritSectionCount` (globals.c);
    /// while non-zero any ERROR escalates to PANIC.
    pub fn start_critical_section()
);

seam_core::seam!(
    /// `END_CRIT_SECTION()` — decrement `CritSectionCount`.
    pub fn end_critical_section()
);

seam_core::seam!(
    /// Read `ExitOnAnyError` (globals.c).
    pub fn exit_on_any_error() -> bool
);

seam_core::seam!(
    /// Write `ExitOnAnyError` (BeginInternalSubTransaction forces FATAL exit
    /// on error around its body).
    pub fn set_exit_on_any_error(value: bool)
);
