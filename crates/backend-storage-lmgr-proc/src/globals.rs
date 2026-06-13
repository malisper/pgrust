//! `proc.c`'s own per-backend global state — the GUCs it declares and the
//! deadlock-timeout bookkeeping `ProcSleep`/`CheckDeadLock`/`CheckDeadLockAlert`
//! share.
//!
//! In C these are file-scope globals in `proc.c`:
//!
//! ```c
//! int  DeadlockTimeout = 1000;
//! int  StatementTimeout = 0;          // (owned elsewhere; not here)
//! int  LockTimeout = 0;
//! int  IdleInTransactionSessionTimeout = 0;
//! bool log_lock_waits = false;
//! static volatile sig_atomic_t got_deadlock_timeout;
//! static DeadLockState deadlock_state = DS_NOT_YET_CHECKED;
//! ```
//!
//! They are backend-private, so they are `thread_local` here (per the
//! per-backend-globals rule). `log_recovery_conflict_waits` is a GUC owned by
//! `xlog.c`, reached through that owner's seam, not duplicated here.

use core::cell::Cell;
use types_storage::lock::DeadLockState;

thread_local! {
    /// `int DeadlockTimeout = 1000;` — the `deadlock_timeout` GUC, in ms.
    static DEADLOCK_TIMEOUT: Cell<i32> = const { Cell::new(1000) };

    /// `int LockTimeout = 0;` — the `lock_timeout` GUC, in ms.
    static LOCK_TIMEOUT: Cell<i32> = const { Cell::new(0) };

    /// `bool log_lock_waits = false;` — the `log_lock_waits` GUC.
    static LOG_LOCK_WAITS: Cell<bool> = const { Cell::new(false) };

    /// `static volatile sig_atomic_t got_deadlock_timeout;` — set by the
    /// `DEADLOCK_TIMEOUT` signal handler (`CheckDeadLockAlert`).
    static GOT_DEADLOCK_TIMEOUT: Cell<bool> = const { Cell::new(false) };

    /// `static DeadLockState deadlock_state = DS_NOT_YET_CHECKED;` — what the
    /// last `DeadLockCheck` found, communicated from `CheckDeadLock` to
    /// `ProcSleep`.
    static DEADLOCK_STATE: Cell<DeadLockState> = const { Cell::new(DeadLockState::NotYetChecked) };
}

/// `DeadlockTimeout` (ms).
pub fn deadlock_timeout() -> i32 {
    DEADLOCK_TIMEOUT.with(Cell::get)
}

/// Set `DeadlockTimeout` (GUC assign hook).
pub fn set_deadlock_timeout(v: i32) {
    DEADLOCK_TIMEOUT.with(|c| c.set(v));
}

/// `LockTimeout` (ms).
pub fn lock_timeout() -> i32 {
    LOCK_TIMEOUT.with(Cell::get)
}

/// Set `LockTimeout` (GUC assign hook).
pub fn set_lock_timeout(v: i32) {
    LOCK_TIMEOUT.with(|c| c.set(v));
}

/// `log_lock_waits`.
pub fn log_lock_waits() -> bool {
    LOG_LOCK_WAITS.with(Cell::get)
}

/// Set `log_lock_waits` (GUC assign hook).
pub fn set_log_lock_waits(v: bool) {
    LOG_LOCK_WAITS.with(|c| c.set(v));
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
