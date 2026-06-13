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

// --- backend-utils-init-postinit consumers (postgres.c) ---

seam_core::seam!(
    /// `process_postgres_switches(argc, argv, ctx, NULL)` (postgres.c): apply
    /// the command-line switches from the startup packet as GUC settings under
    /// context `ctx`. `argv` is the already-split argument vector (excluding the
    /// trailing NULL). `Err` carries its `ereport(ERROR)` surface.
    pub fn process_postgres_switches(
        argv: &[String],
        ctx: types_guc::GucContext,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `PostAuthDelay` (postgres.c GUC): seconds to sleep after authentication
    /// (debugging aid).
    pub fn post_auth_delay() -> i32
);

seam_core::seam!(
    /// `TransactionTimeoutPending = value` (postgres.c interrupt flag).
    pub fn set_transaction_timeout_pending(value: bool)
);

seam_core::seam!(
    /// `IdleInTransactionSessionTimeoutPending = value` (postgres.c flag).
    pub fn set_idle_in_transaction_session_timeout_pending(value: bool)
);

seam_core::seam!(
    /// `IdleSessionTimeoutPending = value` (postgres.c flag).
    pub fn set_idle_session_timeout_pending(value: bool)
);

seam_core::seam!(
    /// `IdleStatsUpdateTimeoutPending = value` (postgres.c flag).
    pub fn set_idle_stats_update_timeout_pending(value: bool)
);

seam_core::seam!(
    /// `CheckClientConnectionPending = value` (postgres.c flag).
    pub fn set_check_client_connection_pending(value: bool)
);
