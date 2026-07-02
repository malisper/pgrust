seam_core::seam!(
    // PostgresMain (tcop/postgres.c); never returns.
    pub fn postgres_main(dbname: &str, username: &str) -> !
);

seam_core::seam!(
    // ProcessClientReadInterrupt(blocked) (tcop/postgres.c); Err is the
    // ereport(FATAL) "terminating connection" path.
    pub fn process_client_read_interrupt(blocked: bool) -> types_error::PgResult<()>
);

seam_core::seam!(
    // ProcessClientWriteInterrupt(blocked) (tcop/postgres.c).
    pub fn process_client_write_interrupt(blocked: bool) -> types_error::PgResult<()>
);

seam_core::seam!(
    // process_postgres_switches(argc, argv, ctx, NULL) (postgres.c); the u8 is
    // a types_guc::GucContext discriminant (this decl crate stays types-lean).
    pub fn process_postgres_switches(argv: &[String], gucctx: u8) -> types_error::PgResult<()>
);

seam_core::seam!(
    // CHECK_FOR_INTERRUPTS() (miscadmin.h) -> ProcessInterrupts (postgres.c);
    // a raised cancel/die comes back as the Err.
    pub fn check_for_interrupts() -> types_error::PgResult<()>
);

seam_core::seam!(
    // die (postgres.c) — SIGTERM's thread-model rendering: flag setter run on
    // the target backend's own thread; Err is the single-user immediate
    // ProcessInterrupts leg.
    pub fn die() -> types_error::PgResult<()>
);

seam_core::seam!(
    // StatementCancelHandler (postgres.c) — SIGINT's thread-model rendering;
    // must run on the target backend's own thread (flags are thread-local).
    pub fn statement_cancel_handler()
);

seam_core::seam!(
    // quickdie (postgres.c) — SIGQUIT rendering; exits the process.
    pub fn quickdie() -> !
);

seam_core::seam!(
    // FloatExceptionHandler (postgres.c); always Err (ereport ERROR).
    pub fn float_exception_handler() -> types_error::PgResult<()>
);

seam_core::seam!(
    // HandleRecoveryConflictInterrupt(reason) (postgres.c); u32 is a
    // ProcSignalReason discriminant (this decl crate stays types-lean).
    pub fn handle_recovery_conflict_interrupt(reason: u32)
);

seam_core::seam!(
    // ResetUsage (postgres.c).
    pub fn reset_usage()
);

seam_core::seam!(
    // ShowUsage(title) (postgres.c).
    pub fn show_usage(title: &str) -> types_error::PgResult<()>
);

seam_core::seam!(
    // set_debug_options(debug_flag, ctx, source-fixed-ARGV) (postgres.c);
    // ctx u8 = GucContext discriminant.
    pub fn set_debug_options(debug_flag: i32, gucctx: u8) -> types_error::PgResult<()>
);

seam_core::seam!(
    // set_plan_disabling_options(arg, ctx, ARGV) (postgres.c).
    pub fn set_plan_disabling_options(arg: &str, gucctx: u8) -> types_error::PgResult<bool>
);

seam_core::seam!(
    // get_stats_option_name(optarg) (postgres.c); None = invalid.
    pub fn get_stats_option_name(arg: &str) -> Option<&'static str>
);
