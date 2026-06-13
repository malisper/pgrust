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
    /// `PostgresMain(dbname, username)` (tcop/postgres.c) — the regular
    /// backend's main loop, entered after the startup packet is processed and
    /// the PGPROC is set up. Never returns (it exits the process through
    /// `proc_exit`). `dbname`/`username` are `MyProcPort->database_name` /
    /// `->user_name`, `None` mirroring a C NULL.
    pub fn postgres_main(dbname: Option<&str>, username: Option<&str>) -> !
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

seam_core::seam!(
    /// `pg_parse_query(query_string)` (tcop/postgres.c) — run the raw parser on
    /// a single SQL string, returning the `List *` of `RawStmt *` as their
    /// opaque handles (the raw parse trees are owned by the parser). The IMPORT
    /// FOREIGN SCHEMA loop parses each FDW-returned command this way. Can
    /// `ereport(ERROR)` on a syntax error, carried on `Err`. Allocated in
    /// `mcx`.
    pub fn pg_parse_query<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        query_string: &str,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, types_plancache::RawStmtHandle>>
);

seam_core::seam!(
    /// `pg_plan_query(querytree, query_string, cursorOptions, boundParams)`
    /// (tcop/postgres.c) — plan a single already-rewritten query, returning a
    /// `PlannedStmt` allocated in `mcx`. Runs the planner; can
    /// `ereport(ERROR)`.
    pub fn pg_plan_query<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        querytree: types_nodes::portalcmds::Query,
        query_string: std::string::String,
        cursor_options: i32,
        bound_params: types_nodes::portalcmds::ParamListInfo,
    ) -> types_error::PgResult<types_nodes::nodeindexscan::PlannedStmt<'mcx>>
);
