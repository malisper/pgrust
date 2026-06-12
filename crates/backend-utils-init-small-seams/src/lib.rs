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
    /// `MyProcPid` (globals.c): this backend's PID, set at process start.
    pub fn my_proc_pid() -> i32
);

seam_core::seam!(
    /// `MaxBackends` (globals.c): the computed backend-slot count, fixed at
    /// postmaster startup.
    pub fn max_backends() -> i32
);

seam_core::seam!(
    /// `InterruptPending = value` (globals.c).
    pub fn set_interrupt_pending(value: bool)
);

seam_core::seam!(
    /// `ProcDiePending = value` (globals.c).
    pub fn set_proc_die_pending(value: bool)
);

seam_core::seam!(
    /// `QueryCancelPending = value` (globals.c).
    pub fn set_query_cancel_pending(value: bool)
);

seam_core::seam!(
    /// `InterruptHoldoffCount = value` (globals.c).
    pub fn set_interrupt_holdoff_count(value: u32)
);

seam_core::seam!(
    /// `HOLD_INTERRUPTS()` (miscadmin.h): `InterruptHoldoffCount++`.
    pub fn hold_interrupts()
);

seam_core::seam!(
    /// `RESUME_INTERRUPTS()` (miscadmin.h): `InterruptHoldoffCount--` (with
    /// the underflow Assert).
    pub fn resume_interrupts()
);
