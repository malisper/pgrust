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

// CheckpointerShmem's start_cv/done_cv; the condition_variable owner
// allocates the storage. Broadcast callers may skip when uninstalled: no
// thread can be sleeping while the unit is unported (sleep panics first).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CheckpointerCv {
    Start,
    Done,
}

seam_core::seam!(
    pub fn checkpointer_cv_broadcast(cv: CheckpointerCv)
);

seam_core::seam!(
    pub fn checkpointer_cv_prepare_to_sleep(cv: CheckpointerCv)
);

seam_core::seam!(
    // ConditionVariableSleep; Err is CHECK_FOR_INTERRUPTS's ereport surface.
    pub fn checkpointer_cv_sleep(cv: CheckpointerCv, wait_event_info: u32) -> types_error::PgResult<()>
);

// Crash-cycle re-init; uninstalled skip is safe (no sleeper ever parked).
seam_core::seam!(
    pub fn proc_signal_barrier_cvs_reset_after_crash()
);

seam_core::seam!(
    pub fn checkpointer_cvs_reset_after_crash()
);
