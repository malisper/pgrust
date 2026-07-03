seam_core::seam!(
    // ProcSignalBarrierPending — per-backend volatile flag owned by procsignal.c.
    pub fn proc_signal_barrier_pending() -> bool
);

seam_core::seam!(
    // ProcessProcSignalBarrier() — barrier processors can ereport(ERROR).
    pub fn process_proc_signal_barrier() -> types_error::PgResult<()>
);

seam_core::seam!(
    // DrainThreadSignals — the target-side half of kill(pid,sig)'s thread
    // rendering; latch waits run it on every wake.
    pub fn drain_thread_signals() -> types_error::PgResult<()>
);
