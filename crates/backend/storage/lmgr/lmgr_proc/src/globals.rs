use std::cell::Cell;

// proc.c's GUC variable backing storage; per-backend thread_locals per
// AGENTS.md rule 10, installed onto guc_tables slots by init_seams().
macro_rules! guc_storage {
    ($($cell:ident, $get:ident, $set:ident, $ty:ty, $init:expr;)+) => {
        $(
            thread_local! {
                static $cell: Cell<$ty> = const {
                    assert!(!core::mem::needs_drop::<$ty>());
                    Cell::new($init)
                };
            }

            #[inline]
            pub fn $get() -> $ty {
                $cell.get()
            }

            #[inline]
            pub fn $set(value: $ty) {
                $cell.set(value);
            }
        )+
    };
}

guc_storage! {
    DEADLOCK_TIMEOUT, DeadlockTimeout, set_DeadlockTimeout, i32, 1000;
    STATEMENT_TIMEOUT, StatementTimeout, set_StatementTimeout, i32, 0;
    LOCK_TIMEOUT, LockTimeout, set_LockTimeout, i32, 0;
    IDLE_IN_TRANSACTION_SESSION_TIMEOUT, IdleInTransactionSessionTimeout,
        set_IdleInTransactionSessionTimeout, i32, 0;
    TRANSACTION_TIMEOUT, TransactionTimeout, set_TransactionTimeout, i32, 0;
    IDLE_SESSION_TIMEOUT, IdleSessionTimeout, set_IdleSessionTimeout, i32, 0;
    LOG_LOCK_WAITS, log_lock_waits, set_log_lock_waits, bool, false;
}
