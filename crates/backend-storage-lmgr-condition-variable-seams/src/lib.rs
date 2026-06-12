//! Seam declarations for the `backend-storage-lmgr-condition-variable` unit
//! (`storage/lmgr/condition_variable.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly. The `ConditionVariable` data shape lives
//! in `types-condvar` so owning structures can embed it.

use types_condvar::ConditionVariable;

seam_core::seam!(
    /// `ConditionVariableTimedSleep(cv, timeout, wait_event_info)` — wait for
    /// the CV to be signaled or `timeout` (ms) to elapse; returns true on
    /// timeout. The sleep loop runs `CHECK_FOR_INTERRUPTS()`, so a
    /// query-cancel/termination `ereport(ERROR/FATAL)` surfaces as `Err`.
    pub fn condition_variable_timed_sleep(
        cv: &ConditionVariable,
        timeout: i64,
        wait_event_info: u32,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `ConditionVariableCancelSleep()` — end the current sleep protocol;
    /// returns true if we were signaled while breaking it off. Infallible.
    pub fn condition_variable_cancel_sleep() -> bool
);

seam_core::seam!(
    /// `ConditionVariableBroadcast(cv)` — wake all waiters. Infallible.
    pub fn condition_variable_broadcast(cv: &ConditionVariable)
);
