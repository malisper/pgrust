//! `xlog_redo` — the XLOG resource manager's redo routine (xlog.c:8304).
//!
//! `xlog_redo` is the `rm_redo` callback for `RM_XLOG_ID`; the recovery loop
//! dispatches every XLOG record here. The control FLOW — the `info`-opcode
//! dispatch, the informational/no-op arms, the unknown-opcode fault — is
//! grounded 1:1 here. The genuinely-external per-arm work (record-field decode,
//! `TransamVariables`/`MultiXact`/`ControlFile`/`XLogCtl` mutation, the bufmgr
//! full-page-image restore, and the `GetCurrentReplayRecPtr` replay-timeline
//! read those arms perform) is owned by xlogreader/xlogrecovery/bufmgr/the
//! deferred control-file driver; each crosses a deferred external in [`ext`]
//! that panics loudly until the owner lands. See DESIGN_DEBT.md.
//!
//! The recovery loop owns the `XLogReaderState`; `xlog_redo` reads the masked
//! opcode (`XLogRecGetInfo(record) & ~XLR_INFO_MASK`) off the record exactly as
//! the C does. The C reads the replay timeline *inside* the arms that need it
//! (`GetCurrentReplayRecPtr`, ambient `XLogCtl` recovery state owned by
//! xlogrecovery); so does this port — the timeline is not a parameter, it lives
//! inside the deferred control-file external where xlogrecovery's
//! `GetCurrentReplayRecPtr` will be consulted when that subsystem lands.

use backend_utils_error::PgResult;
use types_wal::rmgr::XLogReaderState;
use types_wal::wal::XLR_INFO_MASK;

// XLOG resource-manager info opcodes (catalog/pg_control.h:68-82).
const XLOG_CHECKPOINT_SHUTDOWN: u8 = 0x00;
const XLOG_CHECKPOINT_ONLINE: u8 = 0x10;
const XLOG_NOOP: u8 = 0x20;
const XLOG_NEXTOID: u8 = 0x30;
const XLOG_SWITCH: u8 = 0x40;
const XLOG_BACKUP_END: u8 = 0x50;
const XLOG_PARAMETER_CHANGE: u8 = 0x60;
const XLOG_RESTORE_POINT: u8 = 0x70;
const XLOG_FPW_CHANGE: u8 = 0x80;
const XLOG_END_OF_RECOVERY: u8 = 0x90;
const XLOG_FPI_FOR_HINT: u8 = 0xA0;
const XLOG_FPI: u8 = 0xB0;
const XLOG_OVERWRITE_CONTRECORD: u8 = 0xD0;
const XLOG_CHECKPOINT_REDO: u8 = 0xE0;

/// `void xlog_redo(XLogReaderState *record)` (xlog.c:8304).
///
/// Apply one `RM_XLOG_ID` WAL record during recovery. `info` is the masked
/// opcode `XLogRecGetInfo(record) & ~XLR_INFO_MASK`, read off the record as in
/// the C. The per-arm record decode + durable-state mutation (and the
/// `GetCurrentReplayRecPtr` replay-timeline read those arms perform) are owned
/// by the record reader / control-file driver; they cross [`ext`] until those
/// land.
///
/// Returns `Err` on the C `ereport(PANIC)`/`elog(ERROR)` paths.
pub fn xlog_redo(record: &mut XLogReaderState<'_>) -> PgResult<()> {
    // uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    let info = record
        .record
        .as_ref()
        .expect("xlog_redo dispatched on a decoded record")
        .info()
        & !XLR_INFO_MASK;

    if info == XLOG_NEXTOID {
        // TransamVariables->nextOid = nextOid; oidCount = 0  (under OidGenLock)
        ext::xlog_redo_nextoid(record)
    } else if info == XLOG_CHECKPOINT_SHUTDOWN || info == XLOG_CHECKPOINT_ONLINE {
        // counters from the CheckPoint image; ControlFile + XLogCtl shmem;
        // ThisTimeLineID == replayTLI check (GetCurrentReplayRecPtr);
        // RecoveryRestartPoint; smgrdestroyall.
        ext::xlog_redo_control_file_arm(info, record)
    } else if info == XLOG_OVERWRITE_CONTRECORD {
        // nothing to do here, handled in xlogrecovery_redo()
        Ok(())
    } else if info == XLOG_END_OF_RECOVERY {
        // xl_end_of_recovery decode + the ThisTimeLineID != replayTLI PANIC.
        ext::xlog_redo_control_file_arm(info, record)
    } else if info == XLOG_NOOP {
        Ok(())
    } else if info == XLOG_SWITCH {
        Ok(())
    } else if info == XLOG_RESTORE_POINT {
        // nothing to do here, handled in xlogrecovery.c
        Ok(())
    } else if info == XLOG_FPI || info == XLOG_FPI_FOR_HINT {
        // per-block XLogReadBufferForRedo -> BLK_RESTORED -> UnlockReleaseBuffer.
        ext::xlog_redo_fpi(record, info == XLOG_FPI_FOR_HINT)
    } else if info == XLOG_BACKUP_END {
        // nothing to do here, handled in xlogrecovery_redo()
        Ok(())
    } else if info == XLOG_PARAMETER_CHANGE {
        ext::xlog_redo_control_file_arm(info, record)
    } else if info == XLOG_FPW_CHANGE {
        ext::xlog_redo_control_file_arm(info, record)
    } else if info == XLOG_CHECKPOINT_REDO {
        // nothing to do here, just for informational purposes
        Ok(())
    } else {
        // C's switch has no `default` arm: an `info` that matches none of the
        // XLOG opcodes simply falls through to the end of `xlog_redo` and the
        // function returns (a no-op). Match that — do NOT raise an error.
        Ok(())
    }
}

mod ext {
    use super::*;

    macro_rules! deferred {
        ($( $(#[$attr:meta])* pub fn $name:ident ( $($arg:ident : $argty:ty),* $(,)? ) $(-> $ret:ty)? ; )+) => {
            $(
                $(#[$attr])*
                pub fn $name ( $($arg : $argty),* ) $(-> $ret)? {
                    $( let _ = &$arg; )*
                    panic!(concat!(
                        "xlog_redo dependency not ported (xlog-redo-deps debt): ",
                        stringify!($name)
                    ))
                }
            )+
        };
    }

    deferred! {
        /// `XLOG_NEXTOID`: `memcpy(&nextOid, XLogRecGetData(record), ...)` then
        /// `TransamVariables->nextOid` under OidGenLock.
        pub fn xlog_redo_nextoid(record: &mut XLogReaderState<'_>) -> PgResult<()>;
        /// `XLOG_FPI` / `XLOG_FPI_FOR_HINT`: restore the carried full-page image.
        pub fn xlog_redo_fpi(record: &mut XLogReaderState<'_>, for_hint: bool) -> PgResult<()>;
        /// The control-file / XLogCtl-shmem / multixact arms. These read the
        /// replay timeline internally via `GetCurrentReplayRecPtr` (ambient
        /// `XLogCtl` recovery state owned by xlogrecovery), exactly as the C
        /// does — it is not a `xlog_redo` parameter.
        pub fn xlog_redo_control_file_arm(info: u8, record: &mut XLogReaderState<'_>) -> PgResult<()>;
    }
}
