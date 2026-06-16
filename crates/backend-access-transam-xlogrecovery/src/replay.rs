//! The WAL replay driver (`PerformWalRecovery` / `ApplyWalRecord` /
//! `xlog_redo`-dispatch + consistency / timeline-switch / record-timestamp /
//! backup-page-consistency helpers).
//!
//! **Scaffold module.** Faithful signatures, honest `panic!` bodies the replay
//! family-fill lane replaces against [`crate::core::XLogRecoveryState`].
//!
//! Ported from `src/backend/access/transam/xlogrecovery.c`.

use types_core::{TimeLineID, TimestampTz, XLogRecPtr};
use types_error::PgError;

use crate::core::{RecordRef, XLogRecoveryState};

/// `void PerformWalRecovery(void)` (xlogrecovery.c) — the main redo loop: read
/// and apply WAL records until the recovery target / end of WAL is reached.
pub fn perform_wal_recovery(_st: &mut XLogRecoveryState) -> Result<(), PgError> {
    panic!(
        "blocked: xlogrecovery::replay::perform_wal_recovery — the ReadRecord->apply \
         redo loop needs the replay family (apply_wal_record + check_recovery_consistency \
         + recovery_apply_delay/recoveryStops* over the held reader); pending replay-family fill"
    )
}

/// `static void ApplyWalRecord(XLogReaderState *xlogreader, XLogRecord *record,`
/// `TimeLineID *replayTLI)` (xlogrecovery.c) — apply a single WAL record.
pub(crate) fn apply_wal_record(
    _st: &mut XLogRecoveryState,
    _record: RecordRef,
    _replay_tli: &mut TimeLineID,
) -> Result<(), PgError> {
    panic!(
        "blocked: xlogrecovery::replay::apply_wal_record — per-AM redo dispatch via the \
         rmgr redo table (GetRmgr(rmid).rm_redo over the held reader) + replay-end-state \
         bookkeeping; pending replay-family fill"
    )
}

/// `void xlog_redo(XLogReaderState *record)` (xlogrecovery.c) — the RM_XLOG_ID
/// redo handler.
pub(crate) fn xlogrecovery_redo(
    _st: &mut XLogRecoveryState,
    _record: RecordRef,
    _replay_tli: TimeLineID,
) -> Result<(), PgError> {
    panic!(
        "blocked: xlogrecovery::replay::xlogrecovery_redo — the RM_XLOG_ID redo handler \
         (checkpoint/end-of-recovery/overwrite-contrecord/restore-point record handling \
         updating ControlFile + minRecoveryPoint); pending replay-family fill"
    )
}

/// `static void CheckRecoveryConsistency(void)` (xlogrecovery.c) — check
/// whether recovery has reached a consistent state and notify the postmaster.
///
/// Cross-family sibling: called directly (intra-crate) by the replay loop.
pub(crate) fn check_recovery_consistency(_st: &mut XLogRecoveryState) -> Result<(), PgError> {
    panic!(
        "blocked: xlogrecovery::replay::check_recovery_consistency — backup-end/min-recovery-point \
         reached test + postmaster PMSIGNAL_RECOVERY_CONSISTENT/BEGIN_HOT_STANDBY signaling \
         (unported postmaster pmsignal owner); pending replay-family fill"
    )
}

/// `static void checkTimeLineSwitch(XLogRecPtr lsn, TimeLineID newTLI,`
/// `TimeLineID prevTLI, TimeLineID replayTLI)` (xlogrecovery.c) — validate a
/// timeline switch in the WAL stream.
pub(crate) fn check_time_line_switch(
    _st: &XLogRecoveryState,
    _lsn: XLogRecPtr,
    _new_tli: TimeLineID,
    _prev_tli: TimeLineID,
    _replay_tli: TimeLineID,
) -> Result<(), PgError> {
    panic!(
        "blocked: xlogrecovery::replay::check_time_line_switch — timeline-history validation \
         (tliInHistory over expected_tles + minRecoveryPoint PANIC checks); pending replay-family fill"
    )
}

/// `static bool getRecordTimestamp(XLogReaderState *record, TimestampTz *recordXtime)`
/// (xlogrecovery.c) — extract the commit/abort/restore-point timestamp from a
/// record, if it carries one.
pub(crate) fn get_record_timestamp(_record: RecordRef, _record_xtime: &mut TimestampTz) -> bool {
    panic!(
        "blocked: xlogrecovery::replay::get_record_timestamp — needs parsing the record data area \
         into xl_restore_point.rp_time / xl_xact_commit.xact_time / xl_xact_abort.xact_time \
         (xact WAL record struct decode over the held reader); pending replay-family fill"
    )
}

/// `static void verifyBackupPageConsistency(XLogReaderState *record)`
/// (xlogrecovery.c) — the `wal_consistency_checking` masked-page comparison.
pub(crate) fn verify_backup_page_consistency(
    _st: &XLogRecoveryState,
    _record: RecordRef,
) -> Result<(), PgError> {
    panic!(
        "blocked: xlogrecovery::replay::verify_backup_page_consistency — wal_consistency_checking \
         masked-page comparison (RestoreBlockImage + rm_mask + ReadBufferWithoutRelcache over \
         the held reader's block refs); pending replay-family fill"
    )
}
