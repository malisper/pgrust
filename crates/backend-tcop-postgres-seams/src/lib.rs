//! Seam declarations for the `backend-tcop-postgres` unit
//! (`tcop/postgres.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

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
    /// `die(SIGNAL_ARGS)` (tcop/postgres.c) — the SIGTERM handler: set
    /// `ShutdownRequestPending`/`InterruptPending` and the latch.
    /// Async-signal-safe and infallible; installed as the SIGTERM handler.
    pub fn die(postgres_signal_arg: i32)
);

seam_core::seam!(
    /// `die(SIGNAL_ARGS)` (tcop/postgres.c) — the standard SIGTERM handler that
    /// sets `ProcDiePending`/`InterruptPending` and the latch so the next
    /// `CHECK_FOR_INTERRUPTS` exits. Returns the handler so callers can install
    /// it with `pqsignal(SIGTERM, ...)`; tcop owns the handler body, so this
    /// resolves only once tcop lands.
    pub fn die_signal_handler() -> fn(i32)
);
