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

use ::utils_error::{ereport, PgResult};
use ::control::CheckPoint;
use ::types_core::{Oid, TimeLineID, XLogRecPtr};
use ::types_error::PANIC;
use ::wal::rmgr::XLogReaderState;
use ::wal::wal::XLR_INFO_MASK;

use multixact_seams as mx_seams;
use varsup_seams as varsup_seams;

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
        // memcpy(&nextOid, XLogRecGetData(record), sizeof(Oid)); then
        // TransamVariables->nextOid = nextOid; oidCount = 0  (under OidGenLock).
        let data = record
            .record
            .as_ref()
            .expect("xlog_redo dispatched on a decoded record")
            .data();
        // `Oid` is a 4-byte unsigned; the record's main data is exactly the OID.
        let next_oid = Oid::from_ne_bytes(
            data.get(..4)
                .expect("XLOG_NEXTOID record carries a 4-byte Oid")
                .try_into()
                .expect("4-byte slice"),
        );
        varsup_seams::redo_set_next_oid::call(next_oid)
    } else if info == XLOG_CHECKPOINT_SHUTDOWN || info == XLOG_CHECKPOINT_ONLINE {
        // counters from the CheckPoint image; ControlFile + XLogCtl shmem;
        // ThisTimeLineID == replayTLI check (GetCurrentReplayRecPtr);
        // RecoveryRestartPoint; smgrdestroyall.
        redo_checkpoint(record, info == XLOG_CHECKPOINT_SHUTDOWN)
    } else if info == XLOG_OVERWRITE_CONTRECORD {
        // nothing to do here, handled in xlogrecovery_redo()
        Ok(())
    } else if info == XLOG_END_OF_RECOVERY {
        // xl_end_of_recovery decode + the ThisTimeLineID != replayTLI PANIC.
        redo_end_of_recovery(record)
    } else if info == XLOG_NOOP {
        Ok(())
    } else if info == XLOG_SWITCH {
        Ok(())
    } else if info == XLOG_RESTORE_POINT {
        // nothing to do here, handled in xlogrecovery.c
        Ok(())
    } else if info == XLOG_FPI || info == XLOG_FPI_FOR_HINT {
        // XLOG_FPI / XLOG_FPI_FOR_HINT (xlog.c:8542-8576): the record carries
        // only block references, each with a full-page image. Replay each by
        // re-reading the buffer (XLogReadBufferForRedo, which restores the FPI),
        // asserting BLK_RESTORED, then UnlockReleaseBuffer. XLOG_FPI_FOR_HINT
        // records may legally carry no image (full_page_writes was off) — in
        // that case there is nothing to do for that block.
        let max_block_id = record
            .record
            .as_ref()
            .expect("xlog_redo dispatched on a decoded record")
            .max_block_id();

        for block_id in 0..=max_block_id {
            let block_id = block_id as u8;
            let has_image = record
                .record
                .as_ref()
                .expect("decoded record")
                .has_block_image(block_id as usize);
            if !has_image {
                if info == XLOG_FPI {
                    return Err(::utils_error::PgError::new(
                        ::types_error::ERROR,
                        "XLOG_FPI record did not contain a full-page image",
                    ));
                }
                continue;
            }

            let (action, buffer) =
                xlogutils::XLogReadBufferForRedo(record, block_id)?;
            if action != ::wal::XLogRedoAction::BlkRestored {
                return Err(::utils_error::PgError::new(
                    ::types_error::ERROR,
                    "unexpected XLogReadBufferForRedo result when restoring backup block",
                ));
            }
            bufmgr_seams::unlock_release_buffer::call(buffer);
        }
        Ok(())
    } else if info == XLOG_BACKUP_END {
        // nothing to do here, handled in xlogrecovery_redo()
        Ok(())
    } else if info == XLOG_PARAMETER_CHANGE {
        redo_parameter_change(record)
    } else if info == XLOG_FPW_CHANGE {
        redo_fpw_change(record)
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

/// `XLOG_CHECKPOINT_{SHUTDOWN,ONLINE}` redo arm (xlog.c:8333-8504). Apply a
/// replayed checkpoint record: advance the cluster XID/OID/MultiXact counters
/// (exactly on shutdown, as minimums on online), refresh
/// `ControlFile->checkPointCopy.nextXid` + `XLogCtl->ckptFullXid`, verify the
/// timeline matches the replay timeline, stash the checkpoint for restartpoints,
/// and free all smgr objects.
fn redo_checkpoint(record: &mut XLogReaderState<'_>, shutdown: bool) -> PgResult<()> {
    let read_rec_ptr = record.ReadRecPtr;
    let end_rec_ptr = record.EndRecPtr;
    let data = record
        .record
        .as_ref()
        .expect("xlog_redo dispatched on a decoded record")
        .data();
    let check_point = CheckPoint::from_record_bytes(data)
        .expect("XLOG_CHECKPOINT record carries a sizeof(CheckPoint) image");

    if shutdown {
        // In a SHUTDOWN checkpoint, believe the counters exactly.
        varsup_seams::redo_set_next_xid_oid_exact::call(check_point.nextXid, check_point.nextOid)?;
        mx_seams::multi_xact_set_next_m_xact::call(
            check_point.nextMulti,
            check_point.nextMultiOffset,
        )?;
        mx_seams::multi_xact_advance_oldest::call(
            check_point.oldestMulti,
            check_point.oldestMultiDB,
        )?;
        // No need to set oldestClogXid here; an xl_clog_truncate redo handles it.
        if ::types_core::TransactionIdIsNormal(check_point.oldestXid) {
            vacuum_seams::set_transaction_id_limit::call(
                check_point.oldestXid,
                check_point.oldestXidDB,
            )?;
        }
        // (C also fakes an empty running-xacts record for Hot Standby when
        // standbyState >= STANDBY_INITIALIZED. Single-node crash recovery does
        // not initialize standby state, so that branch is skipped — same
        // documented hot-standby divergence as the checkpoint writer.)

        // ControlFile->checkPointCopy.nextXid always tracks the latest ckpt XID.
        set_control_checkpoint_next_xid(check_point.nextXid)?;
        set_ckpt_full_xid(check_point.nextXid);
        check_replay_tli(check_point.ThisTimeLineID, "shutdown")?;
        recovery_restart_point(&check_point, read_rec_ptr, end_rec_ptr);
    } else {
        // In an ONLINE checkpoint, treat the XID counter as a minimum.
        varsup_seams::redo_advance_next_xid_min::call(check_point.nextXid)?;
        // We ignore the nextOid counter in an ONLINE checkpoint (tracked via
        // XLOG_NEXTOID records).
        mx_seams::multi_xact_advance_next_m_xact::call(
            check_point.nextMulti,
            check_point.nextMultiOffset,
        )?;
        mx_seams::multi_xact_advance_oldest::call(
            check_point.oldestMulti,
            check_point.oldestMultiDB,
        )?;
        if ::types_core::TransactionIdPrecedes(
            varsup_seams::get_oldest_xid::call(),
            check_point.oldestXid,
        ) {
            vacuum_seams::set_transaction_id_limit::call(
                check_point.oldestXid,
                check_point.oldestXidDB,
            )?;
        }
        set_control_checkpoint_next_xid(check_point.nextXid)?;
        set_ckpt_full_xid(check_point.nextXid);
        check_replay_tli(check_point.ThisTimeLineID, "online")?;
        recovery_restart_point(&check_point, read_rec_ptr, end_rec_ptr);
    }

    // After replaying a checkpoint record, free all smgr objects (the startup
    // process does not process sinval / AtEOXact_SMgr).
    smgr_seams::smgrdestroyall::call()?;
    Ok(())
}

/// `XLOG_END_OF_RECOVERY` redo arm (xlog.c:8509-8531): decode the
/// `xl_end_of_recovery` record and PANIC if its `ThisTimeLineID` does not match
/// the replay timeline.
fn redo_end_of_recovery(record: &mut XLogReaderState<'_>) -> PgResult<()> {
    let data = record
        .record
        .as_ref()
        .expect("xlog_redo dispatched on a decoded record")
        .data();
    // xl_end_of_recovery: TimestampTz end_time @0; TimeLineID ThisTimeLineID @8;
    // TimeLineID PrevTimeLineID @12; int wal_level @16.
    let this_tli = TimeLineID::from_ne_bytes(
        data.get(8..12)
            .expect("xl_end_of_recovery carries ThisTimeLineID @8")
            .try_into()
            .expect("4-byte slice"),
    );
    check_replay_tli(this_tli, "end-of-recovery")
}

/// `(void) GetCurrentReplayRecPtr(&replayTLI); if (tli != replayTLI)
/// ereport(PANIC, ...)` — the per-arm timeline cross-check (xlog.c:8423/8489/8527).
fn check_replay_tli(tli: TimeLineID, kind: &str) -> PgResult<()> {
    let (_replay_ptr, replay_tli) =
        xlogrecovery_seams::get_xlog_replay_rec_ptr_tli::call();
    if tli != replay_tli {
        return ereport(PANIC)
            .errmsg(alloc::format!(
                "unexpected timeline ID {tli} (should be {replay_tli}) in {kind} checkpoint record"
            ))
            .finish(::types_error::ErrorLocation::new("xlog.c", 8425, "xlog_redo"))
            .map(|_| ());
    }
    Ok(())
}

/// `ControlFile->checkPointCopy.nextXid = nextXid` under `ControlFileLock`
/// (xlog.c:8410-8412 / 8479-8481).
fn set_control_checkpoint_next_xid(next_xid: ::types_core::FullTransactionId) -> PgResult<()> {
    let lock = lwlock::main_lock_ref(9 /* ControlFileLock */);
    lwlock::LWLockAcquire(
        lock,
        types_storage::storage::LW_EXCLUSIVE,
        init_small::globals::MyProcNumber(),
    )?;
    crate::shmem::control_file_mut().checkPointCopy.nextXid = next_xid;
    lwlock::LWLockRelease(lock)?;
    Ok(())
}

/// `XLogCtl->ckptFullXid = nextXid` under `info_lck` (xlog.c:8415-8417 /
/// 8484-8486).
fn set_ckpt_full_xid(next_xid: ::types_core::FullTransactionId) {
    // SAFETY: live shmem region.
    let ctl = unsafe { &*crate::shmem::xlog_ctl() };
    crate::shmem::spin_lock_acquire(&ctl.info_lck);
    // SAFETY: info_lck held; live shmem region.
    unsafe {
        (*crate::shmem::xlog_ctl()).ckptFullXid = next_xid;
    }
    crate::shmem::spin_lock_release(&ctl.info_lck);
}

/// `RecoveryRestartPoint(checkPoint, record)` (xlog.c:7613-7641) — stash the
/// replayed checkpoint in `XLogCtl` so the checkpointer can later choose it as a
/// restartpoint, unless there are unresolved invalid-page references.
fn recovery_restart_point(
    check_point: &CheckPoint,
    read_rec_ptr: XLogRecPtr,
    end_rec_ptr: XLogRecPtr,
) {
    if xlogutils::XLogHaveInvalidPages() {
        // Don't record a restart point while invalid-page references are pending.
        return;
    }
    // SAFETY: live shmem region.
    let ctl = unsafe { &*crate::shmem::xlog_ctl() };
    crate::shmem::spin_lock_acquire(&ctl.info_lck);
    // SAFETY: info_lck held; live shmem region.
    unsafe {
        let ctl_mut = crate::shmem::xlog_ctl();
        (*ctl_mut).lastCheckPointRecPtr = read_rec_ptr;
        (*ctl_mut).lastCheckPointEndPtr = end_rec_ptr;
        (*ctl_mut).lastCheckPoint = *check_point;
    }
    crate::shmem::spin_lock_release(&ctl.info_lck);
}

// The xlog rmgr's file-static recovery variables (xlog.c). These exist only in
// the recovery (startup) process and are touched solely from the redo path, so
// a thread-local Cell faithfully models the C file-static storage.
std::thread_local! {
    /// `static XLogRecPtr LocalMinRecoveryPoint;` (xlog.c:671) — the startup
    /// process's cached copy of `ControlFile->minRecoveryPoint`.
    static LOCAL_MIN_RECOVERY_POINT: core::cell::Cell<XLogRecPtr> =
        const { core::cell::Cell::new(0) };
    /// `static TimeLineID LocalMinRecoveryPointTLI;` (xlog.c:672).
    static LOCAL_MIN_RECOVERY_POINT_TLI: core::cell::Cell<TimeLineID> =
        const { core::cell::Cell::new(0) };
    /// `static bool lastFullPageWrites;` (xlog.c:242) — during recovery, tracks
    /// the `full_page_writes` state as last seen in the WAL.
    static LAST_FULL_PAGE_WRITES: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };

    /// `static bool updateMinRecoveryPoint = true;` (xlog.c:673) — whether this
    /// backend is allowed to update its cached `LocalMinRecoveryPoint` (set false
    /// once the startup process learns it is doing crash recovery).
    static UPDATE_MIN_RECOVERY_POINT: core::cell::Cell<bool> = const { core::cell::Cell::new(true) };
}

/// Read this backend's cached `(LocalMinRecoveryPoint, LocalMinRecoveryPointTLI)`.
pub(crate) fn local_min_recovery_point() -> (XLogRecPtr, TimeLineID) {
    (
        LOCAL_MIN_RECOVERY_POINT.with(|c| c.get()),
        LOCAL_MIN_RECOVERY_POINT_TLI.with(|c| c.get()),
    )
}

/// Set this backend's cached `(LocalMinRecoveryPoint, LocalMinRecoveryPointTLI)`.
pub(crate) fn set_local_min_recovery_point(point: XLogRecPtr, tli: TimeLineID) {
    LOCAL_MIN_RECOVERY_POINT.with(|c| c.set(point));
    LOCAL_MIN_RECOVERY_POINT_TLI.with(|c| c.set(tli));
}

/// `updateMinRecoveryPoint` read.
pub(crate) fn update_min_recovery_point() -> bool {
    UPDATE_MIN_RECOVERY_POINT.with(|c| c.get())
}

/// `updateMinRecoveryPoint` write.
pub(crate) fn set_update_min_recovery_point(v: bool) {
    UPDATE_MIN_RECOVERY_POINT.with(|c| c.set(v));
}

/// `XLOG_PARAMETER_CHANGE` redo arm (xlog.c:8580-8642). Update our copy of the
/// hot-standby parameters in `pg_control`, advance `minRecoveryPoint` while in
/// archive recovery, propagate the commit-timestamp-tracking change, flush the
/// control file, and re-check the required parameter values.
fn redo_parameter_change(record: &mut XLogReaderState<'_>) -> PgResult<()> {
    // XLogRecPtr lsn = record->EndRecPtr;
    let lsn = record.EndRecPtr;

    // memcpy(&xlrec, XLogRecGetData(record), sizeof(xl_parameter_change));
    let data = record
        .record
        .as_ref()
        .expect("xlog_redo dispatched on a decoded record")
        .data();
    let xlrec = ::wal::rmgrdesc::xl_parameter_change::from_bytes(data)
        .expect("XLOG_PARAMETER_CHANGE record carries a sizeof(xl_parameter_change) image");

    // The C hot-standby branch that invalidates obsolete logical replication
    // slots (InvalidateObsoleteReplicationSlots for RS_INVAL_WAL_LEVEL) is
    // skipped here: single-node crash recovery never enters hot standby, the
    // same documented hot-standby divergence as the other redo arms.

    // LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
    let control_file_lock = lwlock::main_lock_ref(9 /* ControlFileLock */);
    lwlock::LWLockAcquire(
        control_file_lock,
        types_storage::storage::LW_EXCLUSIVE,
        init_small::globals::MyProcNumber(),
    )?;

    let cf = crate::shmem::control_file_mut();
    cf.MaxConnections = xlrec.max_connections();
    cf.max_worker_processes = xlrec.max_worker_processes();
    cf.max_wal_senders = xlrec.max_wal_senders();
    cf.max_prepared_xacts = xlrec.max_prepared_xacts();
    cf.max_locks_per_xact = xlrec.max_locks_per_xact();
    cf.wal_level = xlrec.wal_level();
    cf.wal_log_hints = xlrec.wal_log_hints();

    // Update minRecoveryPoint to ensure that if recovery is aborted, we recover
    // back up to this point before allowing hot standby again. The local copies
    // cannot be updated as long as crash recovery is happening and we expect all
    // the WAL to be replayed.
    if xlogrecovery_seams::in_archive_recovery::call() {
        let cf = crate::shmem::control_file_mut();
        LOCAL_MIN_RECOVERY_POINT.with(|c| c.set(cf.minRecoveryPoint));
        LOCAL_MIN_RECOVERY_POINT_TLI.with(|c| c.set(cf.minRecoveryPointTLI));
    }
    let local_min = LOCAL_MIN_RECOVERY_POINT.with(|c| c.get());
    if local_min != crate::InvalidXLogRecPtr && local_min < lsn {
        let (_replay_ptr, replay_tli) =
            xlogrecovery_seams::get_xlog_replay_rec_ptr_tli::call();
        let cf = crate::shmem::control_file_mut();
        cf.minRecoveryPoint = lsn;
        cf.minRecoveryPointTLI = replay_tli;
    }

    // CommitTsParameterChange(xlrec.track_commit_timestamp,
    //                         ControlFile->track_commit_timestamp);
    let old_track = crate::shmem::control_file_mut().track_commit_timestamp;
    commit_ts_seams::commit_ts_parameter_change::call(
        xlrec.track_commit_timestamp(),
        old_track,
    )?;
    crate::shmem::control_file_mut().track_commit_timestamp = xlrec.track_commit_timestamp();

    // UpdateControlFile();
    crate::shmem::UpdateControlFile()?;

    // LWLockRelease(ControlFileLock);
    lwlock::LWLockRelease(control_file_lock)?;

    // Check to see if any parameter change gives a problem on recovery.
    crate::startup::CheckRequiredParameterValues()?;
    Ok(())
}

/// `XLOG_FPW_CHANGE` redo arm (xlog.c:8644-8666). Track the `full_page_writes`
/// state recorded in the WAL, and remember the LSN of the last record that
/// disabled it (so backup start/stop can detect that case).
fn redo_fpw_change(record: &mut XLogReaderState<'_>) -> PgResult<()> {
    // memcpy(&fpw, XLogRecGetData(record), sizeof(bool));
    let data = record
        .record
        .as_ref()
        .expect("xlog_redo dispatched on a decoded record")
        .data();
    let fpw = *data
        .first()
        .expect("XLOG_FPW_CHANGE record carries a 1-byte bool")
        != 0;

    // Update the LSN of the last replayed XLOG_FPW_CHANGE record so that
    // do_pg_backup_start()/do_pg_backup_stop() can check whether
    // full_page_writes has been disabled during online backup.
    if !fpw {
        let read_rec_ptr = record.ReadRecPtr;
        // SAFETY: live shmem region.
        let ctl = unsafe { &*crate::shmem::xlog_ctl() };
        crate::shmem::spin_lock_acquire(&ctl.info_lck);
        // SAFETY: info_lck held; live shmem region.
        unsafe {
            let ctl_mut = crate::shmem::xlog_ctl();
            if (*ctl_mut).lastFpwDisableRecPtr < read_rec_ptr {
                (*ctl_mut).lastFpwDisableRecPtr = read_rec_ptr;
            }
        }
        crate::shmem::spin_lock_release(&ctl.info_lck);
    }

    // Keep track of full_page_writes.
    LAST_FULL_PAGE_WRITES.with(|c| c.set(fpw));
    Ok(())
}
