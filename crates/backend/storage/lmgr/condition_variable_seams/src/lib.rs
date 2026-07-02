seam_core::seam!(
    pub fn condition_variable_cancel_sleep() -> bool
);

// procsignal's pss_barrierCV, keyed by ProcSignal slot index (== ProcNumber);
// the condition_variable owner allocates the per-slot CV storage.
seam_core::seam!(
    // ConditionVariableTimedSleep; true = timeout reached. Err is
    // CHECK_FOR_INTERRUPTS's ereport surface.
    pub fn proc_signal_barrier_cv_timed_sleep(
        slot: i32,
        timeout_ms: i64,
        wait_event_info: u32,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    pub fn proc_signal_barrier_cv_broadcast(slot: i32)
);
