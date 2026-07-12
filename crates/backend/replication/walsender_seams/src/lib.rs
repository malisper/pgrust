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

/// Marshal shape for pg_stat_get_wal_senders (walsender.c:3914): one live
/// WalSnd slot's spinlock-guarded fields plus its sync-standby classification.
/// LSNs are XLogRecPtr (u64), lags are TimeOffset (µs, -1 = unknown/null),
/// reply_time is TimestampTz (µs, 0 = null).
pub struct WalSndStatRow {
    pub pid: i32,
    pub state: &'static str,
    pub sent_ptr: u64,
    pub write: u64,
    pub flush: u64,
    pub apply: u64,
    pub write_lag: i64,
    pub flush_lag: i64,
    pub apply_lag: i64,
    pub sync_priority: i32,
    pub is_sync_standby: bool,
    pub syncrep_method_is_priority: bool,
    pub reply_time: i64,
}

seam_core::seam!(
    // pg_stat_get_wal_senders' WalSndCtl->walsnds scan (walsender.c:3914),
    // installed by walsender; the SQL function body lives in pgstatfuncs.
    pub fn pg_stat_wal_senders_snapshot() -> Vec<WalSndStatRow>
);
