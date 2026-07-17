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

seam_core::seam!(
    // SendThreadSignal — kill(pid, signo)'s thread rendering for seams-only
    // callers (proc.c's blocking-autovacuum cancel). Returns 0 on success,
    // errno on failure (ESRCH when no such backend, as C's kill()).
    pub fn send_thread_signal(pid: i32, signo: i32) -> i32
);
