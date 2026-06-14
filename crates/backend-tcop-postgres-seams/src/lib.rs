//! Seam declarations for the `backend-tcop-postgres` unit
//! (`tcop/postgres.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

extern crate alloc;

seam_core::seam!(
    /// The `BackgroundWorkerMain` signal-handler block: install
    /// `StatementCancelHandler`/`procsignal_sigusr1_handler`/
    /// `FloatExceptionHandler` (database-connection workers) or `SIG_IGN`
    /// (others) for SIGINT/SIGUSR1/SIGFPE, the `bgworker_die` SIGTERM handler,
    /// `SIG_IGN` for SIGHUP/SIGPIPE/SIGUSR2, `SIG_DFL` for SIGCHLD, and run
    /// `InitializeTimeouts()`. Composite because the handler fn-pointers
    /// (`StatementCancelHandler` etc., owned by tcop/postgres.c) and the timeout
    /// manager are owned by other subsystems; `db_connection` selects the
    /// connection-handler variant. Installed by the tcop/postgres owner when it
    /// lands.
    pub fn install_bgworker_signal_handlers(db_connection: bool)
);

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

seam_core::seam!(
    /// `log_statement == LOGSTMT_ALL` (postgres.c GUC `log_statement`): whether
    /// statement logging is set to log all statements. `tcop/fastpath.c`'s
    /// `HandleFunctionRequest` reads this to decide whether to emit the
    /// "fastpath function call" `LOG` line. Pure read of the postgres.c-owned
    /// GUC.
    pub fn log_statement_is_all() -> bool
);

seam_core::seam!(
    /// `check_log_duration(msec_str, was_logged)` (postgres.c): decide whether
    /// to log the statement duration. Returns the C result code (`0` = don't
    /// log, `1` = log duration only, `2` = log duration + statement) together
    /// with the formatted milliseconds string the C writes into its
    /// `msec_str[32]` buffer (allocated in `mcx`; only meaningful for a nonzero
    /// code). Reads the duration GUCs / statement timing owned by postgres.c.
    pub fn check_log_duration<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        was_logged: bool,
    ) -> types_error::PgResult<(i32, mcx::PgString<'mcx>)>
);
