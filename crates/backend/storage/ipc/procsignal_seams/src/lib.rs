seam_core::seam!(
    // ProcSignalBarrierPending — per-backend volatile flag owned by procsignal.c.
    pub fn proc_signal_barrier_pending() -> bool
);

seam_core::seam!(
    // ProcessProcSignalBarrier() — barrier processors can ereport(ERROR).
    pub fn process_proc_signal_barrier() -> types_error::PgResult<()>
);
