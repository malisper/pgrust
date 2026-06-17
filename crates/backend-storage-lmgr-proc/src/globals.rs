//! `proc.c`'s own per-backend global state — the deadlock-timeout bookkeeping
//! `ProcSleep`/`CheckDeadLock`/`CheckDeadLockAlert` share.
//!
//! In C these are file-scope globals in `proc.c`:
//!
//! ```c
//! static volatile sig_atomic_t got_deadlock_timeout;
//! static DeadLockState deadlock_state = DS_NOT_YET_CHECKED;
//!
//! int  DeadlockTimeout = 1000;
//! int  StatementTimeout = 0;
//! int  LockTimeout = 0;
//! int  IdleInTransactionSessionTimeout = 0;
//! int  TransactionTimeout = 0;
//! int  IdleSessionTimeout = 0;
//! bool log_lock_waits = false;
//! ```
//!
//! They are backend-private, so they are `thread_local` here (per the
//! per-backend-globals rule).
//!
//! The seven timeout/log GUCs above are the `conf->variable` backing storage
//! that `guc_tables.c` points its table entries at; proc.c owns that storage.
//! The GUC engine (`backend_utils_misc_guc_tables::vars`) reaches it through the
//! installed `GucVarAccessors { get, set }` pair (see `init_seams`), exactly as
//! the reference shmem-sizing GUCs (NBuffers et al.). The inward `deadlock_timeout`
//! / `transaction_timeout` seams and `ProcSleep` read these through the same GUC
//! slot, so a SET propagating into the slot lands in this owner storage.
//! `log_recovery_conflict_waits` is a GUC owned by `xlog.c`, reached through that
//! owner's seam.

use core::cell::Cell;
use types_storage::lock::DeadLockState;

thread_local! {
    /// `int DeadlockTimeout = 1000;` — proc.c GUC backing (milliseconds).
    static DEADLOCK_TIMEOUT: Cell<i32> = const { Cell::new(1000) };

    /// `int StatementTimeout = 0;` — proc.c GUC backing (milliseconds).
    static STATEMENT_TIMEOUT: Cell<i32> = const { Cell::new(0) };

    /// `int LockTimeout = 0;` — proc.c GUC backing (milliseconds).
    static LOCK_TIMEOUT: Cell<i32> = const { Cell::new(0) };

    /// `int IdleInTransactionSessionTimeout = 0;` — proc.c GUC backing (ms).
    static IDLE_IN_TRANSACTION_SESSION_TIMEOUT: Cell<i32> = const { Cell::new(0) };

    /// `int TransactionTimeout = 0;` — proc.c GUC backing (milliseconds).
    static TRANSACTION_TIMEOUT: Cell<i32> = const { Cell::new(0) };

    /// `int IdleSessionTimeout = 0;` — proc.c GUC backing (milliseconds).
    static IDLE_SESSION_TIMEOUT: Cell<i32> = const { Cell::new(0) };

    /// `bool log_lock_waits = false;` — proc.c GUC backing.
    static LOG_LOCK_WAITS: Cell<bool> = const { Cell::new(false) };
}

/// `DeadlockTimeout` GUC.
pub fn DeadlockTimeout() -> i32 {
    DEADLOCK_TIMEOUT.with(Cell::get)
}
/// Set `DeadlockTimeout` GUC.
pub fn set_DeadlockTimeout(v: i32) {
    DEADLOCK_TIMEOUT.with(|c| c.set(v));
}

/// `StatementTimeout` GUC.
pub fn StatementTimeout() -> i32 {
    STATEMENT_TIMEOUT.with(Cell::get)
}
/// Set `StatementTimeout` GUC.
pub fn set_StatementTimeout(v: i32) {
    STATEMENT_TIMEOUT.with(|c| c.set(v));
}

/// `LockTimeout` GUC.
pub fn LockTimeout() -> i32 {
    LOCK_TIMEOUT.with(Cell::get)
}
/// Set `LockTimeout` GUC.
pub fn set_LockTimeout(v: i32) {
    LOCK_TIMEOUT.with(|c| c.set(v));
}

/// `IdleInTransactionSessionTimeout` GUC.
pub fn IdleInTransactionSessionTimeout() -> i32 {
    IDLE_IN_TRANSACTION_SESSION_TIMEOUT.with(Cell::get)
}
/// Set `IdleInTransactionSessionTimeout` GUC.
pub fn set_IdleInTransactionSessionTimeout(v: i32) {
    IDLE_IN_TRANSACTION_SESSION_TIMEOUT.with(|c| c.set(v));
}

/// `TransactionTimeout` GUC.
pub fn TransactionTimeout() -> i32 {
    TRANSACTION_TIMEOUT.with(Cell::get)
}
/// Set `TransactionTimeout` GUC.
pub fn set_TransactionTimeout(v: i32) {
    TRANSACTION_TIMEOUT.with(|c| c.set(v));
}

/// `IdleSessionTimeout` GUC.
pub fn IdleSessionTimeout() -> i32 {
    IDLE_SESSION_TIMEOUT.with(Cell::get)
}
/// Set `IdleSessionTimeout` GUC.
pub fn set_IdleSessionTimeout(v: i32) {
    IDLE_SESSION_TIMEOUT.with(|c| c.set(v));
}

/// `log_lock_waits` GUC.
pub fn log_lock_waits() -> bool {
    LOG_LOCK_WAITS.with(Cell::get)
}
/// Set `log_lock_waits` GUC.
pub fn set_log_lock_waits(v: bool) {
    LOG_LOCK_WAITS.with(|c| c.set(v));
}

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
