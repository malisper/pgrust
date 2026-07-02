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
    // ResetUsage (postgres.c).
    pub fn reset_usage()
);

seam_core::seam!(
    // ShowUsage(title) (postgres.c).
    pub fn show_usage(title: &str) -> types_error::PgResult<()>
);
