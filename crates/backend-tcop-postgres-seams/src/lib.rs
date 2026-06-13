//! Seam declarations for the `backend-tcop-postgres` unit
//! (`tcop/postgres.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

extern crate alloc;

seam_core::seam!(
    /// `CHECK_FOR_INTERRUPTS()` (miscadmin.h): if an interrupt is pending,
    /// service it via `ProcessInterrupts()` (tcop/postgres.c). A query-cancel
    /// or backend-termination interrupt surfaces as the `Err` (the C
    /// `ereport(ERROR/FATAL, ...)` longjmp).
    pub fn check_for_interrupts() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `check_stack_depth()` (tcop/postgres.c): raise
    /// `ERRCODE_STATEMENT_TOO_COMPLEX` (the C `ereport(ERROR)`) when the stack
    /// is too deep. The recursive tsearch engines call this at every recursion
    /// entry.
    pub fn check_stack_depth() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `debug_query_string = NULL` (tcop/postgres.c): reset the
    /// currently-executing query string before exit-time cleanup clobbers it
    /// (`proc_exit_prepare`).
    pub fn reset_debug_query_string()
);

seam_core::seam!(
    /// `HandleRecoveryConflictInterrupt(reason)` (tcop/postgres.c) — the
    /// PROCSIG_RECOVERY_CONFLICT_* arms of `procsignal_sigusr1_handler`.
    /// Signal-handler-safe flag flipping; infallible.
    pub fn handle_recovery_conflict_interrupt(reason: types_storage::ProcSignalReason)
);

seam_core::seam!(
    /// `ProcSleep`'s `ereport(LOG, errmsg(msg), errdetail_log_plural(detail_s,
    /// detail_p, n, ...))` for the lock-wait progress messages. `detail_*` are
    /// `None` for the "acquired" case (a bare `errmsg`); when present they are
    /// the singular/plural errdetail_log forms selected by `holders_num`.
    pub fn report_lock_wait_log(
        message: alloc::string::String,
        detail_singular: Option<alloc::string::String>,
        detail_plural: Option<alloc::string::String>,
        holders_num: i32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ProcSleep`'s autovac-cancel `ereport(DEBUG1, errmsg_internal("sending
    /// cancel to blocking autovacuum PID %d", pid), errdetail_log("%s", logbuf))`.
    pub fn report_autovac_cancel(
        pid: i32,
        detail_log: alloc::string::String,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `kill(pid, SIGINT)` against a blocking autovacuum worker. `ESRCH` (the
    /// worker already exited) is ignored inside the impl; any other errno warns
    /// (`ereport(WARNING, "could not send signal to process %d: %m")`).
    pub fn signal_autovacuum_worker(pid: i32) -> types_error::PgResult<()>
);
