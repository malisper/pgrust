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
    /// `IsUnderPostmaster` (globals.c) — false in the postmaster itself, true
    /// in a forked backend.
    pub fn is_under_postmaster() -> bool
);

seam_core::seam!(
    /// `MyProcNumber` (globals.c) — the pgprocno of the current backend, or
    /// `INVALID_PROC_NUMBER` when no `PGPROC` is attached (`MyProc == NULL`,
    /// i.e. during bootstrap / shared-memory initialization).
    pub fn my_proc_number() -> types_core::ProcNumber
);

seam_core::seam!(
    /// `HOLD_INTERRUPTS()` (miscadmin.h) — increment `InterruptHoldoffCount`
    /// (globals.c), deferring cancel/die interrupts until the matching
    /// `RESUME_INTERRUPTS`.
    pub fn hold_interrupts()
);

seam_core::seam!(
    /// `RESUME_INTERRUPTS()` (miscadmin.h) — decrement `InterruptHoldoffCount`
    /// (globals.c).
    pub fn resume_interrupts()
);
