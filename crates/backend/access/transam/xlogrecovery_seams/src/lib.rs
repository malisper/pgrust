use types_core::{TimeLineID, TransactionId, XLogRecPtr};
use types_error::PgResult;

seam_core::seam!(
    pub fn reached_consistency() -> bool
);

seam_core::seam!(
    pub fn get_xlog_replay_rec_ptr() -> (XLogRecPtr, TimeLineID)
);

seam_core::seam!(
    pub fn xlog_request_wal_receiver_reply()
);

// InitWalRecovery out-params (xlogrecovery.c): (wasShutdown, haveBackupLabel,
// haveTblspcMap). The starting-checkpoint side effects land in the shared
// ControlFile image owned by transam_xlog.
#[derive(Clone, Copy, Debug)]
pub struct InitWalRecoveryResult {
    pub was_shutdown: bool,
    pub have_backup_label: bool,
    pub have_tblspc_map: bool,
}

// EndOfWalRecoveryInfo (xlogrecovery.h); lastPage holds the (possibly
// partial) last block the last record spans, lastPageBeginPtr its start.
#[derive(Clone, Debug)]
pub struct EndOfWalRecoveryInfo {
    pub lastRec: XLogRecPtr,
    pub lastRecTLI: TimeLineID,
    pub endOfLog: XLogRecPtr,
    pub endOfLogTLI: TimeLineID,
    pub lastPageBeginPtr: XLogRecPtr,
    pub lastPage: Box<[u8]>,
    pub abortedRecPtr: XLogRecPtr,
    pub missingContrecPtr: XLogRecPtr,
    pub recovery_signal_file_found: bool,
    pub standby_signal_file_found: bool,
}

seam_core::seam!(
    pub fn init_wal_recovery() -> PgResult<InitWalRecoveryResult>
);

seam_core::seam!(
    pub fn perform_wal_recovery() -> PgResult<()>
);

seam_core::seam!(
    pub fn finish_wal_recovery() -> PgResult<EndOfWalRecoveryInfo>
);

seam_core::seam!(
    pub fn shutdown_wal_recovery() -> PgResult<()>
);

seam_core::seam!(
    // ArchiveRecoveryRequested (xlogrecovery.c global).
    pub fn archive_recovery_requested() -> bool
);

seam_core::seam!(
    // InArchiveRecovery (xlogrecovery.c global).
    pub fn in_archive_recovery() -> bool
);

seam_core::seam!(
    // recoveryTargetTLI (xlogrecovery.c global).
    pub fn recovery_target_tli() -> TimeLineID
);

seam_core::seam!(
    // PromoteIsTriggered() (xlogrecovery.c).
    pub fn promote_is_triggered() -> bool
);

seam_core::seam!(
    // GetCurrentReplayRecPtr(&replayTLI) (xlogrecovery.c).
    pub fn get_current_replay_rec_ptr() -> (XLogRecPtr, TimeLineID)
);

seam_core::seam!(
    // CheckRequiredParameterValues reads (xlog.c <- xlogrecovery). Standby
    // prescan of prepared xacts: PrescanPreparedTransactions(NULL, NULL) is in
    // twophase_seams; this is the recovery-time oldestActiveXid source.
    pub fn recovery_oldest_active_xid() -> TransactionId
);
