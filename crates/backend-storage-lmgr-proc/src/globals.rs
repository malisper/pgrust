//! `proc.c`'s own per-backend global state — the deadlock-timeout bookkeeping
//! `ProcSleep`/`CheckDeadLock`/`CheckDeadLockAlert` share.
//!
//! In C these are file-scope globals in `proc.c`:
//!
//! ```c
//! static volatile sig_atomic_t got_deadlock_timeout;
//! static DeadLockState deadlock_state = DS_NOT_YET_CHECKED;
//! ```
//!
//! They are backend-private, so they are `thread_local` here (per the
//! per-backend-globals rule).
//!
//! The `DeadlockTimeout` / `LockTimeout` / `log_lock_waits` GUCs (also file-scope
//! globals in `proc.c`) are NOT duplicated here: they live in their canonical,
//! SET-wired home `backend_utils_misc_guc_tables::vars` and are read straight
//! from there by `ProcSleep` and the inward `deadlock_timeout` /
//! `transaction_timeout` seams, matching C's read of its live globals. Keeping a
//! second copy here would freeze at boot defaults (no SET propagates to it).
//! Likewise `log_recovery_conflict_waits` is a GUC owned by `xlog.c`, reached
//! through that owner's seam.

use core::cell::Cell;
use types_storage::lock::DeadLockState;

// The `DeadlockTimeout`/`LockTimeout`/`log_lock_waits` GUCs are NOT kept in a
// backend-private copy here: they live in their canonical, SET-wired home
// `backend_utils_misc_guc_tables::vars` and are read straight from there (by the
// inward `deadlock_timeout`/`transaction_timeout` seams and by `ProcSleep`),
// matching C's read of its live file-scope globals. A second copy here would
// freeze at boot defaults because no SET ever propagates to it.

thread_local! {
    /// `static volatile sig_atomic_t got_deadlock_timeout;` — set by the
    /// `DEADLOCK_TIMEOUT` signal handler (`CheckDeadLockAlert`).
    static GOT_DEADLOCK_TIMEOUT: Cell<bool> = const { Cell::new(false) };

    /// `static DeadLockState deadlock_state = DS_NOT_YET_CHECKED;` — what the
    /// last `DeadLockCheck` found, communicated from `CheckDeadLock` to
    /// `ProcSleep`.
    static DEADLOCK_STATE: Cell<DeadLockState> = const { Cell::new(DeadLockState::NotYetChecked) };
}


/// `got_deadlock_timeout`.
pub fn got_deadlock_timeout() -> bool {
    GOT_DEADLOCK_TIMEOUT.with(Cell::get)
}

/// Set `got_deadlock_timeout`.
pub fn set_got_deadlock_timeout(v: bool) {
    GOT_DEADLOCK_TIMEOUT.with(|c| c.set(v));
}

/// `deadlock_state`.
pub fn deadlock_state() -> DeadLockState {
    DEADLOCK_STATE.with(Cell::get)
}

/// Set `deadlock_state`.
pub fn set_deadlock_state(v: DeadLockState) {
    DEADLOCK_STATE.with(|c| c.set(v));
}
