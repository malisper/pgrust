//! The WAL read-record retry loop (`ReadRecord` / `XLogPageRead` /
//! `ReadCheckpointRecord` + `EnableStandbyMode` / `emode_for_corrupt_record`).
//!
//! **Scaffold module.** The function signatures are faithful to
//! `src/backend/access/transam/xlogrecovery.c`; the bodies are honest
//! `panic!("decomp: … not yet filled")` stubs that the readrecord family-fill
//! lane replaces against [`crate::core::XLogRecoveryState`] and the prefetcher /
//! page-read seams installed into the page-read owner.
//!
//! Ported from `src/backend/access/transam/xlogrecovery.c`.

use types_core::{TimeLineID, XLogRecPtr};
use types_error::{ErrorLevel, PgError};

use crate::core::{RecordRef, XLogRecoveryState};

/// `void EnableStandbyMode(void)` (xlogrecovery.c) — enter standby mode and
/// disable startup-progress reporting.
pub(crate) fn enable_standby_mode(_st: &mut XLogRecoveryState) {
    panic!("decomp: xlogrecovery::readrecord::enable_standby_mode not yet filled")
}

/// `static int emode_for_corrupt_record(int emode, XLogRecPtr RecPtr)`
/// (xlogrecovery.c) — lower the error level for the first corrupt-record report
/// from a given source so we don't spam the log while waiting for WAL.
pub(crate) fn emode_for_corrupt_record(
    _st: &XLogRecoveryState,
    _emode: ErrorLevel,
    _rec_ptr: XLogRecPtr,
) -> ErrorLevel {
    panic!("decomp: xlogrecovery::readrecord::emode_for_corrupt_record not yet filled")
}

/// `static XLogRecord *ReadRecord(XLogPrefetcher *xlogprefetcher, int emode,`
/// `bool fetching_ckpt, TimeLineID replayTLI)` (xlogrecovery.c) — read the next
/// WAL record, retrying across WAL sources as needed.
pub(crate) fn read_record(
    _st: &mut XLogRecoveryState,
    _emode: ErrorLevel,
    _fetching_ckpt: bool,
    _replay_tli: TimeLineID,
) -> Result<RecordRef, PgError> {
    panic!("decomp: xlogrecovery::readrecord::read_record not yet filled")
}

/// `static XLogRecord *ReadCheckpointRecord(XLogPrefetcher *xlogprefetcher,`
/// `XLogRecPtr RecPtr, TimeLineID replayTLI)` (xlogrecovery.c) — read the
/// checkpoint record at a given LSN, with structured error reporting.
pub(crate) fn read_checkpoint_record(
    _st: &mut XLogRecoveryState,
    _rec_ptr: XLogRecPtr,
    _replay_tli: TimeLineID,
) -> Result<RecordRef, PgError> {
    panic!("decomp: xlogrecovery::readrecord::read_checkpoint_record not yet filled")
}
