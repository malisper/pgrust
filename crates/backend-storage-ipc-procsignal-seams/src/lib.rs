//! Seam declarations for the `backend-storage-ipc-procsignal` unit
//! (`storage/ipc/procsignal.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// Read `ProcSignalBarrierPending` (procsignal.c), the per-backend
    /// `volatile sig_atomic_t` set by the SIGUSR1 handler when a barrier
    /// must be absorbed.
    pub fn proc_signal_barrier_pending() -> bool
);

seam_core::seam!(
    /// `ProcessProcSignalBarrier()` (procsignal.c): absorb pending global
    /// barriers. Barrier-processing functions run under PG_TRY and re-throw,
    /// so an `ereport(ERROR)` propagates to the caller.
    pub fn process_proc_signal_barrier() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `int SendProcSignal(pid_t pid, ProcSignalReason reason,
    /// ProcNumber procNumber)` (procsignal.c). Returns the `kill()` result
    /// (0 success, -1 on failure). `procNumber == INVALID_PROC_NUMBER` makes
    /// it search the proc array for `pid`.
    pub fn send_proc_signal(
        pid: i32,
        reason: types_storage::ProcSignalReason,
        proc_number: types_core::ProcNumber,
    ) -> i32
);
