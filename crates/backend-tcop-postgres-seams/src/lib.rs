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
    /// `void ProcessClientReadInterrupt(bool blocked)` (tcop/postgres.c) —
    /// process any interrupt that arrived while waiting to read from the
    /// client. `blocked` is true when called from the blocking-wait path in
    /// `secure_read` (it then services `ProcessInterrupts` / latch-set
    /// interrupts; when false it only notes a recheck). A query-cancel /
    /// termination surfaces as `Err` (the C `ereport(ERROR/FATAL)` longjmp).
    pub fn process_client_read_interrupt(blocked: bool) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `void ProcessClientWriteInterrupt(bool blocked)` (tcop/postgres.c) —
    /// process any interrupt that arrived while waiting to write to the client
    /// (the write-side analog of [`process_client_read_interrupt`]). `Err`
    /// carries the cancel/termination `ereport`.
    pub fn process_client_write_interrupt(blocked: bool) -> types_error::PgResult<()>
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
    /// `PostgresSingleUserMain(argc, argv, username)` (tcop/postgres.c) — the
    /// standalone single-user backend entry, reached from `main()` for the
    /// `DISPATCH_SINGLE` case. Processes the command line, performs the
    /// standalone bootstrap of GUC/auth, then runs `PostgresMain`. Never
    /// returns (exits through `proc_exit`).
    pub fn postgres_single_user_main(argv: &[&str], username: &str) -> !
);

seam_core::seam!(
    /// `set_stack_base()` (tcop/postgres.c) — record the current stack frame as
    /// the reference point for `check_stack_depth()`. Returns the previous base
    /// (a `pg_stack_base_t`); `main()` ignores it. Infallible.
    pub fn set_stack_base()
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

seam_core::seam!(
    /// `log_executor_stats` (postgres.c GUC) — whether per-query executor
    /// resource-usage statistics logging is enabled. `pquery.c`'s `PortalRun` /
    /// `PortalRunMulti` read it to gate `ResetUsage` / `ShowUsage`. Pure read of
    /// the postgres.c-owned GUC.
    pub fn log_executor_stats() -> bool
);

seam_core::seam!(
    /// `ResetUsage()` (postgres.c): snapshot the current resource usage as the
    /// baseline for the next `ShowUsage`. Infallible in C.
    pub fn reset_usage()
);

seam_core::seam!(
    /// `ShowUsage(title)` (postgres.c): log the resource usage delta since the
    /// last `ResetUsage` under `title`. Infallible in C.
    pub fn show_usage(title: &str)
);

seam_core::seam!(
    /// `whereToSendOutput` (postgres.c) — the current output destination for
    /// this backend (`DestNone` / `DestDebug` / `DestRemote`). `NotifyMyFrontEnd`
    /// (async.c) reads it to decide between framing a NotificationResponse and
    /// `elog(INFO)`. Pure read of the postgres.c-owned per-backend global.
    pub fn where_to_send_output() -> types_dest::dest::CommandDest
);

seam_core::seam!(
    /// `whereToSendOutput = DestNone` (postgres.c) — reset the output
    /// destination so a subsequent `ereport` does not try to message the
    /// standby (used by `WalSndShutdown`).
    pub fn set_where_to_send_output_none()
);

seam_core::seam!(
    /// `StatementCancelHandler(SIGNAL_ARGS)` (postgres.c) — the SIGINT handler:
    /// set `QueryCancelPending` / `InterruptPending` and `SetLatch(MyLatch)`.
    /// Installed by walsender via `pqsignal(SIGINT, ...)`.
    pub fn statement_cancel_handler(postgres_signal_arg: i32)
);

// --- Cross-unit callees of `ProcessInterrupts`/`ProcessRecoveryConflictInterrupt`
// (tcop/postgres.c) whose owners are NOT yet ported. These are declared here
// (the owner consuming them is tcop/postgres.c) and remain seam-and-panic until
// their real owners land, mirroring the C call into an unported subsystem
// ("Mirror PG and panic"). They are allowlisted in the recurrence guard with a
// provider-unported note. ---

seam_core::seam!(
    /// `pgstat_report_recovery_conflict(reason)` (pgstat_relation.c /
    /// pgstat.c) — bump the per-database recovery-conflict counters for the
    /// given conflict `reason`. Called from `ProcessRecoveryConflictInterrupt`
    /// just before the conflict `ereport`. Owner (cumulative-stats recovery
    /// conflict reporting) is unported.
    pub fn pgstat_report_recovery_conflict(reason: types_storage::ProcSignalReason)
);

seam_core::seam!(
    /// `pgStatSessionEndCause = DISCONNECT_KILLED;` (pgstat.c session-stats
    /// global) — set in `die()` so the cumulative stats system records the
    /// session as terminated by an administrator. Owner (the
    /// `pgStatSessionEndCause` session-stats global) is unported.
    pub fn set_session_end_cause_killed()
);

seam_core::seam!(
    /// `IsLogicalWorker()` (logicalworker.c) — whether the current process is a
    /// logical-replication apply worker (i.e. `MyLogicalRepWorker != NULL`).
    /// `ProcessInterrupts` reads it to phrase the `ProcDiePending` FATAL.
    /// Owner (logicalworker.c) is unported.
    pub fn is_logical_worker() -> bool
);

seam_core::seam!(
    /// `MyBgworkerEntry->bgw_type` (bgworker.c) — the `bgw_type` string of the
    /// background worker the current process is running, read by
    /// `ProcessInterrupts` to phrase the background-worker termination FATAL.
    /// The owning bgworker-registration state (`MyBgworkerEntry`) is not yet
    /// exposed through an accessor.
    pub fn my_bgworker_type() -> alloc::string::String
);

seam_core::seam!(
    /// `progname` (main.c global `extern const char *progname`) — the program
    /// name set once at startup by `get_progname(argv[0])`. `process_postgres_switches`
    /// reads it only for the bad-command-line-argument FATAL hint
    /// (`errhint("Try \"%s --help\" ...")`). The repo's main.c port threads
    /// `progname` as a parameter rather than keeping the global, so there is no
    /// existing accessor; this stands in for the global read.
    pub fn progname() -> alloc::string::String
);

// ===========================================================================
// Standalone-bootstrap callees consumed by `PostgresSingleUserMain`.
//
// These are owned by units that are still CATALOG `todo` (boot gaps #4/#5/#6):
// the control-file/WAL reader (`access/transam/xlog.c`), the shared-preload /
// shmem-request / runtime-GUC machinery (`utils/init/miscinit.c`,
// `storage/ipc/ipci.c`, `utils/misc/guc_funcs.c`), and the `PgStartTime` global
// (`utils/init/globals.c`). They have no seam crate of their own yet, so the
// single-user driver declares the seams it consumes here; each panics loudly
// until the owning unit lands and installs it. This is the faithful
// "seam-and-panic into an unported dep" boundary — the driver structure above
// is real and wired; only these leaf calls are stubbed by a panic.
// ===========================================================================

seam_core::seam!(
    /// `LocalProcessControlFile(reset)` (`access/transam/xlog.c`) — read
    /// `pg_control` into the backend-local `ControlFile`, validating it and
    /// pulling the WAL-derived settings it carries. `reset` requests a fresh
    /// read. `ereport(FATAL)` on a missing/corrupt control file. Owned by the
    /// (unported) xlog unit; boot gap #5.
    pub fn local_process_control_file(reset: bool) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `process_shared_preload_libraries()` (`utils/init/miscinit.c`) — load the
    /// `shared_preload_libraries` GUC's modules so they can register hooks /
    /// request shared memory before `CreateSharedMemoryAndSemaphores`. Owned by
    /// the (unported) miscinit shared-library loader; `ereport(ERROR)` on a load
    /// failure.
    pub fn process_shared_preload_libraries() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `process_shmem_requests()` (`storage/ipc/ipci.c`) — run each preloaded
    /// module's `shmem_request_hook` so it can reserve additional shared memory
    /// before the segment is sized and created. Owned by the (unported) ipci
    /// unit; boot gap #4 (AIO/shmem sizing).
    pub fn process_shmem_requests() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `InitializeShmemGUCs()` (`utils/misc/guc_funcs.c`) — now that modules
    /// have requested shared memory, compute the runtime-computed GUCs
    /// (`shared_memory_size`, `shared_memory_size_in_huge_pages`). Owned by the
    /// (unported) GUC-funcs unit.
    pub fn initialize_shmem_gucs() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `InitializeWalConsistencyChecking()` (`access/transam/xlog.c`) — process
    /// the `wal_consistency_checking` GUC now that custom resource managers are
    /// loaded. Owned by the (unported) xlog unit; boot gap #5.
    pub fn initialize_wal_consistency_checking() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `PgStartTime = GetCurrentTimestamp()` (`utils/init/globals.c`) — record
    /// the stand-alone backend's startup time into the `PgStartTime` global, at
    /// roughly the same startup point the postmaster does. The global lives in
    /// the (unported) globals.c unit, so the write is fronted by this seam.
    pub fn set_pg_start_time(t: types_core::TimestampTz)
);
