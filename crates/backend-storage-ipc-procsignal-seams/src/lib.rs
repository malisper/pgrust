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
