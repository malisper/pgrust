use std::sync::atomic::Ordering::Relaxed;

use types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED, ERROR, PANIC};
use xlogreader_seams::XLogReaderState;

use crate::control_file::{control_file, control_file_update};
use crate::ctl::{ControlFileLock, XLogCtl};
use crate::*;

fn main_data(record: &XLogReaderState) -> &[u8] {
    let rec = record.record.as_ref().expect("xlog_redo with no decoded record");
    // SAFETY: main_data points into the reader's decode buffer, valid for the
    // redo callback's duration.
    unsafe { rec.main_data_bytes() }
}

/// Fetch the record's main data, validating it is at least `min_len` bytes
/// before any fixed-offset parse. C's xlog_redo memcpy's `sizeof(struct)`
/// bytes out of the (large) decode buffer without a length check, harmlessly
/// over-reading for a short-but-nonzero payload; in Rust the equivalent slice
/// indexing panics, and because xlog_redo runs on the startup/redo thread, a
/// crafted WAL record with truncated main data would turn into a process-fatal
/// panic and a persistent crash loop. Enforce the per-record minimum here and
/// surface a violation as a catchable ERRCODE_DATA_CORRUPTED error instead.
fn main_data_checked<'a>(
    record: &'a XLogReaderState,
    min_len: usize,
    what: &str,
) -> PgResult<&'a [u8]> {
    require_main_data_len(main_data(record), min_len, what)
}

/// Pure length gate for `main_data_checked`: reject a main-data slice shorter
/// than the record type's fixed-offset parse requires, as a catchable
/// ERRCODE_DATA_CORRUPTED error rather than a slice-index panic.
fn require_main_data_len<'a>(
    data: &'a [u8],
    min_len: usize,
    what: &str,
) -> PgResult<&'a [u8]> {
    if data.len() < min_len {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "invalid {what} record: main data is {} bytes, expected at least {}",
                    data.len(),
                    min_len
                ),
            )
            .with_sqlstate(ERRCODE_DATA_CORRUPTED),
        ));
    }
    Ok(data)
}

fn panic_err(msg: String) -> Box<PgError> {
    Box::new(PgError::new(PANIC, msg))
}

fn RecoveryRestartPoint(check_point: &CheckPoint, record: &XLogReaderState) {
    if xlogutils::XLogHaveInvalidPages() {
        return;
    }
    let ctl = XLogCtl();
    ctl.info_lck.with(|| {
        ctl.lastCheckPointRecPtr.store(record.ReadRecPtr, Relaxed);
        ctl.lastCheckPointEndPtr.store(record.EndRecPtr, Relaxed);
        // SAFETY: lastCheckPoint written only under info_lck.
        unsafe { *ctl.lastCheckPoint.get() = *check_point };
    });
}

pub fn xlog_redo(record: &mut XLogReaderState) -> PgResult<()> {
    let rec = record.record.as_ref().expect("xlog_redo with no decoded record");
    let info = rec.xl_info & !XLR_INFO_MASK;
    let lsn = record.EndRecPtr;

    debug_assert!(
        info == XLOG_FPI || info == XLOG_FPI_FOR_HINT || !record.has_block_ref(0)
    );

    match info {
        XLOG_NEXTOID => {
            // Believe the record exactly rather than max() against the
            // counter: max() breaks on OID wraparound, and no OID allocation
            // happens during replay anyway.
            let next_oid = u32::from_ne_bytes(
                main_data_checked(record, 4, "XLOG_NEXTOID")?[..4].try_into().unwrap(),
            );
            let oid_gen_lock = lwlock::main_lock(varsup::OID_GEN_LOCK);
            lwlock::LWLockAcquire(
                oid_gen_lock,
                lwlock::LW_EXCLUSIVE,
                init_small::globals::MyProcNumber(),
            )?;
            let tv = varsup::TransamVariables();
            tv.nextOid.store(next_oid, Relaxed);
            tv.oidCount.store(0, Relaxed);
            lwlock::LWLockRelease(oid_gen_lock)?;
        }
        XLOG_CHECKPOINT_SHUTDOWN => {
            let check_point = CheckPoint::from_bytes(main_data_checked(
                record,
                core::mem::size_of::<CheckPoint>(),
                "shutdown checkpoint",
            )?);
            procarray::TransamVariables().nextXid.store(check_point.nextXid.value, Relaxed);
            varsup::TransamVariables().nextOid.store(check_point.nextOid, Relaxed);
            varsup::TransamVariables().oidCount.store(0, Relaxed);
            if multixact_seams::multixact_set_next_mxact::is_installed() {
                multixact_seams::multixact_set_next_mxact::call(
                    check_point.nextMulti,
                    check_point.nextMultiOffset,
                );
            }
            if multixact_seams::multixact_advance_oldest::is_installed() {
                multixact_seams::multixact_advance_oldest::call(
                    check_point.oldestMulti,
                    check_point.oldestMultiDB,
                )?;
            }
            procarray::TransamVariables().oldestXid.store(check_point.oldestXid, Relaxed);

            if xlogrecovery_seams::archive_recovery_requested::call()
                && !XLogRecPtrIsInvalid(control_file().backupStartPoint)
                && XLogRecPtrIsInvalid(control_file().backupEndPoint)
            {
                return Err(panic_err(
                    "online backup was canceled, recovery cannot continue".into(),
                ));
            }

            // C xlog.c:8352: a shutdown checkpoint means nothing was running
            // on the primary. Fake up an empty running-xacts record (only
            // prepared transactions alive) and apply it now, plus recover
            // standby state for prepared transactions.
            if xlogutils::standby_state() >= xlogutils::STANDBY_INITIALIZED {
                let (oldest_active_xid, xids) =
                    if twophase_seams::prescan_prepared_transactions_xids::is_installed() {
                        twophase_seams::prescan_prepared_transactions_xids::call()?
                    } else {
                        // No twophase linked: no prepared xacts can exist.
                        (check_point.nextXid.xid(), Vec::new())
                    };
                if twophase_seams::standby_recover_prepared_transactions::is_installed() {
                    twophase_seams::standby_recover_prepared_transactions::call()?;
                }

                // TransactionIdRetreat(latestCompletedXid).
                let mut latest_completed_xid = check_point.nextXid.xid();
                loop {
                    latest_completed_xid = latest_completed_xid.wrapping_sub(1);
                    if latest_completed_xid >= types_core::FirstNormalTransactionId {
                        break;
                    }
                }

                let cx = mcx::MemoryContext::new("xlog_redo/running-xacts");
                let xid_vec = mcx::slice_in(cx.mcx(), &xids)?;
                let running = types_storage::storage::RunningTransactionsData {
                    xcnt: xids.len() as i32,
                    subxcnt: 0,
                    subxid_status: types_storage::storage::SUBXIDS_IN_SUBTRANS,
                    nextXid: check_point.nextXid.xid(),
                    oldestRunningXid: oldest_active_xid,
                    oldestDatabaseRunningXid: types_core::InvalidTransactionId,
                    latestCompletedXid: latest_completed_xid,
                    xids: xid_vec,
                };
                procarray_seams::proc_array_apply_recovery_info::call(&running)?;
            }

            // ControlFile->checkPointCopy always tracks the latest ckpt XID.
            // Hold ControlFileLock across the mutation, matching C's xlog_redo
            // (xlog.c:8386-8388), so the checkpointer's restartpoint updates
            // cannot race this write to the shared control file.
            lwlock::LWLockAcquire(
                ControlFileLock(),
                lwlock::LW_EXCLUSIVE,
                init_small::globals::MyProcNumber(),
            )?;
            control_file_update(|cf| cf.checkPointCopy.nextXid = check_point.nextXid);
            lwlock::LWLockRelease(ControlFileLock())?;
            let ctl = XLogCtl();
            ctl.info_lck.with(|| ctl.ckptFullXid.store(check_point.nextXid.value, Relaxed));

            let (_, replay_tli) = xlogrecovery_seams::get_current_replay_rec_ptr::call();
            if check_point.ThisTimeLineID != replay_tli {
                return Err(panic_err(format!(
                    "unexpected timeline ID {} (should be {}) in shutdown checkpoint record",
                    check_point.ThisTimeLineID, replay_tli
                )));
            }

            RecoveryRestartPoint(&check_point, record);

            // After replaying a checkpoint record, free all smgr objects.
            // Otherwise we would never do so for dropped relations, as the
            // startup does not process shared invalidation messages or call
            // AtEOXact_SMgr() (xlog.c:8409-8415).
            smgr_seams::smgr_destroy_all::call()?;
        }
        XLOG_CHECKPOINT_ONLINE => {
            let check_point = CheckPoint::from_bytes(main_data_checked(
                record,
                core::mem::size_of::<CheckPoint>(),
                "online checkpoint",
            )?);
            let tv = procarray::TransamVariables();
            let cur = tv.nextXid.load(Relaxed);
            if cur < check_point.nextXid.value {
                tv.nextXid.store(check_point.nextXid.value, Relaxed);
            }
            if multixact_seams::multixact_advance_next_mxact::is_installed() {
                multixact_seams::multixact_advance_next_mxact::call(
                    check_point.nextMulti,
                    check_point.nextMultiOffset,
                );
            }
            if multixact_seams::multixact_advance_oldest::is_installed() {
                multixact_seams::multixact_advance_oldest::call(
                    check_point.oldestMulti,
                    check_point.oldestMultiDB,
                )?;
            }
            // xlog.c:8452-8454: adopt the record's oldestXid iff it is
            // MODULARLY later (TransactionIdPrecedes, not an unsigned
            // compare, so a horizon frozen past the 2^32 boundary still
            // advances), and adopt it through SetTransactionIdLimit so the
            // vac/warn/stop/wrap limits and oldestXidDB follow.
            if types_core::TransactionIdPrecedes(tv.oldestXid.load(Relaxed), check_point.oldestXid)
            {
                varsup::SetTransactionIdLimit(check_point.oldestXid, check_point.oldestXidDB)?;
            }
            // ControlFile->checkPointCopy always tracks the latest ckpt XID.
            // Hold ControlFileLock across the mutation, matching C's xlog_redo
            // (xlog.c:8455-8457), so the checkpointer's restartpoint updates
            // cannot race this write to the shared control file.
            lwlock::LWLockAcquire(
                ControlFileLock(),
                lwlock::LW_EXCLUSIVE,
                init_small::globals::MyProcNumber(),
            )?;
            control_file_update(|cf| cf.checkPointCopy.nextXid = check_point.nextXid);
            lwlock::LWLockRelease(ControlFileLock())?;
            let ctl = XLogCtl();
            ctl.info_lck.with(|| ctl.ckptFullXid.store(check_point.nextXid.value, Relaxed));

            let (_, replay_tli) = xlogrecovery_seams::get_current_replay_rec_ptr::call();
            if check_point.ThisTimeLineID != replay_tli {
                return Err(panic_err(format!(
                    "unexpected timeline ID {} (should be {}) in online checkpoint record",
                    check_point.ThisTimeLineID, replay_tli
                )));
            }
            RecoveryRestartPoint(&check_point, record);

            // xlog.c:8475-8481: same smgrdestroyall() as the shutdown arm.
            smgr_seams::smgr_destroy_all::call()?;
        }
        XLOG_OVERWRITE_CONTRECORD | XLOG_BACKUP_END | XLOG_RESTORE_POINT => {}
        XLOG_END_OF_RECOVERY => {
            // xl_end_of_recovery: TimestampTz end_time; TimeLineID this/prev; int wal_level.
            let data = main_data_checked(record, 20, "end-of-recovery")?;
            let this_tli = u32::from_ne_bytes(data[8..12].try_into().unwrap());
            let (_, replay_tli) = xlogrecovery_seams::get_current_replay_rec_ptr::call();
            if this_tli != replay_tli {
                return Err(panic_err(format!(
                    "unexpected timeline ID {this_tli} (should be {replay_tli}) in end-of-recovery record"
                )));
            }
        }
        XLOG_NOOP | XLOG_SWITCH | XLOG_CHECKPOINT_REDO => {}
        XLOG_FPI | XLOG_FPI_FOR_HINT => {
            let max_block_id = record.record.as_ref().map_or(-1, |r| r.max_block_id);
            for block_id in 0..(max_block_id + 1) as u8 {
                if !record.has_block_image(block_id) {
                    if info == XLOG_FPI {
                        return Err(Box::new(PgError::error(
                            "XLOG_FPI record did not contain a full-page image",
                        )));
                    }
                    continue;
                }
                let (action, buffer) = xlogutils::XLogReadBufferForRedo(record, block_id)?;
                if action != xlogutils::BLK_RESTORED {
                    return Err(Box::new(PgError::error(
                        "unexpected XLogReadBufferForRedo result when restoring backup block",
                    )));
                }
                bufmgr_seams::lock_buffer::call(buffer, bufmgr_seams::BUFFER_LOCK_UNLOCK)?;
                bufmgr_seams::release_buffer::call(buffer)?;
            }
        }
        XLOG_PARAMETER_CHANGE => {
            // xl_parameter_change: six i32 fields (24 bytes) + two bool bytes.
            let data = main_data_checked(record, 26, "parameter-change")?;
            let max_connections = i32::from_ne_bytes(data[0..4].try_into().unwrap());
            let max_worker_processes = i32::from_ne_bytes(data[4..8].try_into().unwrap());
            let max_wal_senders = i32::from_ne_bytes(data[8..12].try_into().unwrap());
            let max_prepared_xacts = i32::from_ne_bytes(data[12..16].try_into().unwrap());
            let max_locks_per_xact = i32::from_ne_bytes(data[16..20].try_into().unwrap());
            let new_wal_level = i32::from_ne_bytes(data[20..24].try_into().unwrap());
            let wal_log_hints = data[24] != 0;
            let track_commit_timestamp = data[25] != 0;

            if commit_ts_seams::commit_ts_parameter_change::is_installed() {
                commit_ts_seams::commit_ts_parameter_change::call(
                    track_commit_timestamp,
                    control_file().track_commit_timestamp,
                )?;
            } else if track_commit_timestamp != control_file().track_commit_timestamp {
                panic!("CommitTsParameterChange replay not ported");
            }

            // Invalidate logical slots if we are in hot standby and the
            // primary no longer runs a WAL level sufficient for logical
            // decoding (xlog.c:8571). No need to search when the standby's
            // own wal_level is below logical: slot creation was disallowed
            // or existing slots already invalidated then.
            const RS_INVAL_WAL_LEVEL: u32 = 1 << 2;
            if xlogutils::InHotStandby()
                && new_wal_level < crate::WAL_LEVEL_LOGICAL
                && crate::wal_level() >= crate::WAL_LEVEL_LOGICAL
                && slot_seams::invalidate_obsolete_replication_slots::is_installed()
            {
                slot_seams::invalidate_obsolete_replication_slots::call(
                    RS_INVAL_WAL_LEVEL,
                    0,
                    types_core::InvalidOid,
                    types_core::InvalidTransactionId,
                )?;
            }

            // Hold ControlFileLock across every pg_control mutation and the
            // UpdateControlFile() flush, matching C's xlog_redo
            // (xlog.c:8580-8616), so the checkpointer/bgwriter cannot race
            // these writes (field updates and the minRecoveryPoint advance)
            // against restartpoints. The closure guarantees the lock is
            // released on every exit path, including the `?` early return out
            // of UpdateControlFile(). CheckRequiredParameterValues() runs after
            // release, as in C (xlog.c:8619).
            lwlock::LWLockAcquire(
                ControlFileLock(),
                lwlock::LW_EXCLUSIVE,
                init_small::globals::MyProcNumber(),
            )?;
            let update = (|| -> PgResult<()> {
                control_file_update(|cf| {
                    cf.MaxConnections = max_connections;
                    cf.max_worker_processes = max_worker_processes;
                    cf.max_wal_senders = max_wal_senders;
                    cf.max_prepared_xacts = max_prepared_xacts;
                    cf.max_locks_per_xact = max_locks_per_xact;
                    cf.wal_level = new_wal_level;
                    cf.wal_log_hints = wal_log_hints;
                    cf.track_commit_timestamp = track_commit_timestamp;
                });

                if xlogrecovery_seams::in_archive_recovery::call() {
                    crate::write::LOCAL_MIN_RECOVERY_POINT.set(control_file().minRecoveryPoint);
                    crate::write::LOCAL_MIN_RECOVERY_POINT_TLI
                        .set(control_file().minRecoveryPointTLI);
                }
                if crate::write::LOCAL_MIN_RECOVERY_POINT.get() != InvalidXLogRecPtr
                    && crate::write::LOCAL_MIN_RECOVERY_POINT.get() < lsn
                {
                    let (_, replay_tli) = xlogrecovery_seams::get_current_replay_rec_ptr::call();
                    control_file_update(|cf| {
                        cf.minRecoveryPoint = lsn;
                        cf.minRecoveryPointTLI = replay_tli;
                    });
                }
                UpdateControlFile()
            })();
            let release = lwlock::LWLockRelease(ControlFileLock());
            update?;
            release?;
            control_file::CheckRequiredParameterValues()?;
        }
        XLOG_FPW_CHANGE => {
            let fpw = main_data_checked(record, 1, "FPW-change")?[0] != 0;
            if !fpw {
                let ctl = XLogCtl();
                ctl.info_lck.with(|| {
                    if ctl.lastFpwDisableRecPtr.load(Relaxed) < record.ReadRecPtr {
                        ctl.lastFpwDisableRecPtr.store(record.ReadRecPtr, Relaxed);
                    }
                });
            }
            crate::startup::set_last_full_page_writes(fpw);
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::require_main_data_len;
    use types_error::ERRCODE_DATA_CORRUPTED;

    #[test]
    fn short_main_data_is_rejected_not_panicked() {
        // A crafted checkpoint record with empty/truncated main data must
        // surface as a catchable ERRCODE_DATA_CORRUPTED error, never a panic.
        let need = core::mem::size_of::<crate::CheckPoint>();
        for len in [0usize, 4, need - 1] {
            let data = vec![0u8; len];
            let err = require_main_data_len(&data, need, "online checkpoint")
                .expect_err("short main data must be rejected");
            assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
        }
    }

    #[test]
    fn sufficient_main_data_passes() {
        let need = core::mem::size_of::<crate::CheckPoint>();
        let data = vec![0u8; need];
        let ok = require_main_data_len(&data, need, "online checkpoint")
            .expect("full-length main data must be accepted");
        assert_eq!(ok.len(), need);

        // Extra trailing bytes (as legitimate records may carry, since C
        // copies only sizeof(CheckPoint)) are still accepted.
        let longer = vec![0u8; need + 8];
        assert!(require_main_data_len(&longer, need, "online checkpoint").is_ok());
    }
}
