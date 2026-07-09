use std::cell::Cell;

// walsender.c's am_walsender/am_db_walsender globals. Hosted here (not behind
// seam!) so readers see `false` in binaries/tests that never link walsender;
// one backend = one thread (init_small globals pattern).
thread_local! {
    static AM_WALSENDER: Cell<bool> = const { Cell::new(false) };
    static AM_DB_WALSENDER: Cell<bool> = const { Cell::new(false) };
}

pub fn am_walsender() -> bool {
    AM_WALSENDER.get()
}

pub fn am_db_walsender() -> bool {
    AM_DB_WALSENDER.get()
}

// ProcessStartupPacket's `am_walsender = true; am_db_walsender = (val == "database")`.
pub fn set_walsender_flags(db_walsender: bool) {
    AM_WALSENDER.set(true);
    AM_DB_WALSENDER.set(db_walsender);
}

seam_core::seam!(
    // exec_replication_command(cmd_string); false = not a walsender command,
    // caller falls through to exec_simple_query.
    pub fn exec_replication_command(cmd_string: &str) -> types_error::PgResult<bool>
);

seam_core::seam!(
    // InitWalSender(); PostgresMain-only caller, after pgstat_report_connect.
    pub fn init_wal_sender()
);

seam_core::seam!(
    // WalSndErrorCleanup(); PostgresMain error-recovery-only caller.
    pub fn wal_snd_error_cleanup() -> types_error::PgResult<()>
);

seam_core::seam!(
    // WalSndWakeup(physical, logical); the WAL flush/replay paths broadcast the
    // per-kind ConditionVariables in WalSndCtl. Uninstalled (walsender not
    // linked) reads as is_installed()==false so the flush tail skips it.
    pub fn wal_snd_wakeup(physical: bool, logical: bool)
);

seam_core::seam!(
    // SendBaseBackup(cmd): the BASE_BACKUP replication command, installed by the
    // basebackup crate. Off the serial path; walsender dispatches to it.
    pub fn base_backup(cmd: repl_gram::BaseBackupCmd) -> types_error::PgResult<()>
);
