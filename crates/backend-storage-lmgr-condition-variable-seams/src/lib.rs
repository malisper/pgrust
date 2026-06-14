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
    /// `ConditionVariableInit(cv)` — initialize a condition variable in place
    /// (`SpinLockInit` + `proclist_init`). Used for CVs embedded in shmem/DSM
    /// structures that are not constructed through `ConditionVariable::new`.
    pub fn condition_variable_init(cv: &mut ConditionVariable)
);

seam_core::seam!(
    /// `ConditionVariableSleep(cv, wait_event_info)` — sleep until the CV is
    /// signaled (the no-timeout form, a thin wrapper over the timed sleep).
    /// Runs `CHECK_FOR_INTERRUPTS()`, so a cancel/terminate surfaces as `Err`.
    pub fn condition_variable_sleep(
        cv: &ConditionVariable,
        wait_event_info: u32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ConditionVariableSignal(cv)` — wake one waiter. Infallible.
    pub fn condition_variable_signal(cv: &ConditionVariable)
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

seam_core::seam!(
    /// `ConditionVariablePrepareToSleep(cv)` — enrol this backend on `cv`'s
    /// wakeup list before testing the wait condition. Infallible.
    pub fn condition_variable_prepare_to_sleep(cv: &ConditionVariable)
);

seam_core::seam!(
    /// Resolve the CV identity recorded in `cv_sleep_target` back to the live
    /// `&ConditionVariable` and run `body` over it, returning `body`'s result.
    ///
    /// `ConditionVariableCancelSleep()` takes no `cv` argument; in C it
    /// dereferences the process-local `cv_sleep_target` pointer to reach
    /// `cv->mutex`/`cv->wakeup`. This port records only the CV's *identity* (its
    /// address) in `cv_sleep_target`, never a borrow, so the single
    /// address-to-reference reconstruction is confined to this seam — exactly
    /// the role of `with_target_cv` in the src-idiomatic port. The seam adds no
    /// algorithm of its own: the spinlock-guarded
    /// contains/delete-or-mark-signaled branch runs in `body`, in-crate, over
    /// the resolved `&ConditionVariable`, just as `Signal`/`Broadcast`/`Sleep`
    /// run their branch over their own `cv` argument. `target` is the value of
    /// `CvIdentity` (the recorded address); it is only ever resolved, never
    /// arithmetic'd.
    pub fn with_target_cv(
        target: usize,
        body: &mut dyn FnMut(&ConditionVariable) -> bool,
    ) -> bool
);
