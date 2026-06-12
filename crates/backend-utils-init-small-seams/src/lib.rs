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
    /// `MaxBackends` (globals.c): the computed backend-slot count, fixed at
    /// postmaster startup.
    pub fn max_backends() -> i32
);

seam_core::seam!(
    /// `MyProcNumber` (globals.c): this backend's PGPROC/ProcSignal slot
    /// index; `INVALID_PROC_NUMBER` (-1) until assigned.
    pub fn my_proc_number() -> types_core::ProcNumber
);

seam_core::seam!(
    /// `MyProcPid` (globals.c): this backend's process ID.
    pub fn my_proc_pid() -> i32
);

seam_core::seam!(
    /// Write `InterruptPending` (globals.c), the per-backend
    /// `volatile sig_atomic_t` master interrupt flag checked by
    /// `CHECK_FOR_INTERRUPTS()`.
    pub fn set_interrupt_pending(value: bool)
);
