#![allow(non_snake_case)]

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
    pub recoveryStopReason: String,
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

seam_core::seam!(
    // RemovePromoteSignalFiles (xlogrecovery.c).
    pub fn remove_promote_signal_files()
);

seam_core::seam!(
    // GetXLogReceiptTime(&rtime, &fromStream) (xlogrecovery.c); receipt-time
    // tracking is unported there, so this stays loud until that lane lands.
    pub fn get_xlog_receipt_time() -> (types_core::TimestampTz, bool)
);

seam_core::seam!(
    // GetLatestXTime() (xlogrecovery.c).
    pub fn get_latest_x_time() -> types_core::TimestampTz
);

seam_core::seam!(
    // StandbyMode (xlogrecovery.c global).
    pub fn standby_mode() -> bool
);

seam_core::seam!(
    // StandbyModeRequested (xlogrecovery.c global).
    pub fn standby_mode_requested() -> bool
);

seam_core::seam!(
    // HotStandbyActive() (xlogrecovery.c).
    pub fn hot_standby_active() -> bool
);

seam_core::seam!(
    // GetRecoveryPauseState() (xlogrecovery.c).
    pub fn get_recovery_pause_state() -> i32
);

seam_core::seam!(
    // SetRecoveryPause(recoveryPause) (xlogrecovery.c).
    pub fn set_recovery_pause(recovery_pause: bool)
);

seam_core::seam!(
    // WakeupRecovery() (xlogrecovery.c): SetLatch(recoveryWakeupLatch).
    pub fn wakeup_recovery()
);

seam_core::seam!(
    // CheckPromoteSignal() (xlogrecovery.c).
    pub fn check_promote_signal() -> bool
);

seam_core::seam!(
    // RecoveryRequiresIntParameter(param, curr, min) (xlogrecovery.c).
    pub fn recovery_requires_int_parameter(
        param_name: &str,
        curr_value: i32,
        min_value: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    // StartupRequestWalReceiverRestart() (xlogrecovery.c).
    pub fn startup_request_wal_receiver_restart()
);

seam_core::seam!(
    // StartupRereadConfig's walreceiver-parameter diff (startup.c:157): with
    // process-shared string GUC backings the startup process cannot diff its
    // own pre/post-reload copies as C does; xlogrecovery compares the
    // reloaded values against what the running walreceiver was started with
    // and requests the restart itself.
    pub fn startup_reread_walrcv_config() -> ()
);

seam_core::seam!(
    // ShutDownSlotSync (slotsync.c:1586), installed by the slotsync crate:
    // the startup process shuts the slot sync machinery down during
    // promotion (xlogrecovery.c:1505 FinishWalRecovery).
    pub fn shut_down_slot_sync() -> types_error::PgResult<()>
);
