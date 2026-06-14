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
    panic!("decomp: xlogrecovery::replay::perform_wal_recovery not yet filled")
}

/// `static void ApplyWalRecord(XLogReaderState *xlogreader, XLogRecord *record,`
/// `TimeLineID *replayTLI)` (xlogrecovery.c) — apply a single WAL record.
pub(crate) fn apply_wal_record(
    _st: &mut XLogRecoveryState,
    _record: RecordRef,
    _replay_tli: &mut TimeLineID,
) -> Result<(), PgError> {
    panic!("decomp: xlogrecovery::replay::apply_wal_record not yet filled")
}

/// `void xlog_redo(XLogReaderState *record)` (xlogrecovery.c) — the RM_XLOG_ID
/// redo handler.
pub(crate) fn xlogrecovery_redo(
    _st: &mut XLogRecoveryState,
    _record: RecordRef,
    _replay_tli: TimeLineID,
) -> Result<(), PgError> {
    panic!("decomp: xlogrecovery::replay::xlogrecovery_redo not yet filled")
}

/// `static void CheckRecoveryConsistency(void)` (xlogrecovery.c) — check
/// whether recovery has reached a consistent state and notify the postmaster.
///
/// Cross-family sibling: called directly (intra-crate) by the replay loop.
pub(crate) fn check_recovery_consistency(_st: &mut XLogRecoveryState) -> Result<(), PgError> {
    panic!("decomp: xlogrecovery::replay::check_recovery_consistency not yet filled")
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
    panic!("decomp: xlogrecovery::replay::check_time_line_switch not yet filled")
}

/// `static bool getRecordTimestamp(XLogReaderState *record, TimestampTz *recordXtime)`
/// (xlogrecovery.c) — extract the commit/abort/restore-point timestamp from a
/// record, if it carries one.
pub(crate) fn get_record_timestamp(_record: RecordRef, _record_xtime: &mut TimestampTz) -> bool {
    panic!("decomp: xlogrecovery::replay::get_record_timestamp not yet filled")
}

/// `static void verifyBackupPageConsistency(XLogReaderState *record)`
/// (xlogrecovery.c) — the `wal_consistency_checking` masked-page comparison.
pub(crate) fn verify_backup_page_consistency(
    _st: &XLogRecoveryState,
    _record: RecordRef,
) -> Result<(), PgError> {
    panic!("decomp: xlogrecovery::replay::verify_backup_page_consistency not yet filled")
}
