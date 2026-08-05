//! standby.c: primary snapshot half + AEL lock export + the hot-standby
//! recovery half (conflict resolution, recovery lock tables).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

pub mod recovery;
pub use recovery::{
    CheckRecoveryConflictDeadlock, InitRecoveryTransactionEnvironment, LogRecoveryConflict,
    ResolveRecoveryConflictWithBufferPin, ResolveRecoveryConflictWithDatabase,
    ResolveRecoveryConflictWithLock, ResolveRecoveryConflictWithSnapshot,
    ResolveRecoveryConflictWithSnapshotFullXid, ResolveRecoveryConflictWithTablespace,
    ShutdownRecoveryTransactionEnvironment, StandbyAcquireAccessExclusiveLock,
    StandbyDeadLockHandler, StandbyLockTimeoutHandler, StandbyReleaseAllLocks,
    StandbyReleaseLockTree, StandbyReleaseOldLocks, StandbyTimeoutHandler,
};

use elog::elog;
use types_core::{Oid, XLogRecPtr, XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK};
use types_error::{PgResult, DEBUG2};
use types_storage::sinval::{SharedInvalidationMessage, SHARED_INVALIDATION_MESSAGE_SIZE};
use types_storage::storage::{xl_standby_lock, SUBXIDS_IN_ARRAY, SUBXIDS_MISSING};

// rmgrlist.h / standbydefs.h; RM_STANDBY_ID is test-pinned to 8 in rmgr.
const RM_STANDBY_ID: u8 = 8;
const XLOG_STANDBY_LOCK: u8 = 0x00;
const XLOG_RUNNING_XACTS: u8 = 0x10;
const XLOG_INVALIDATIONS: u8 = 0x20;
const XLR_INFO_MASK: u8 = 0x0F;
const MIN_SIZE_OF_XACT_RUNNING_XACTS: usize = 24;
// offsetof(xl_standby_locks, locks): the i32 nlocks count.
const OFFSET_OF_XL_STANDBY_LOCKS_LOCKS: usize = 4;
const SIZE_OF_XL_STANDBY_LOCK: usize = 12;
// offsetof(xl_invalidations, msgs): dbId 0, tsId 4, relcacheInitFileInval 8,
// pad, nmsgs 12.
const MIN_SIZE_OF_INVALIDATIONS: usize = 16;

#[cfg(test)]
mod tests;

pub fn LogStandbySnapshot() -> PgResult<XLogRecPtr> {
    debug_assert!(transam_xlog::XLogStandbyInfoActive());

    // standby.c:1292 (USE_INJECTION_POINTS): tests suppress the
    // xl_running_xacts record, whose replay could move a slot's xmin
    // forward during decoding and make results unpredictable.
    if injection_point::is_attached("skip-log-running-xacts") {
        return Ok(transam_xlog::GetInsertRecPtr());
    }

    let locks = lock::GetRunningTransactionLocks()?;
    if !locks.is_empty() {
        LogAccessExclusiveLocks(&locks)?;
    }

    let logical = transam_xlog::wal_level() >= transam_xlog::WAL_LEVEL_LOGICAL;
    procarray::GetRunningTransactionData(|running| {
        if !logical {
            lwlock::LWLockRelease(lwlock::main_lock(procarray::PROC_ARRAY_LOCK))?;
        }
        let recptr = LogCurrentRunningXacts(running)?;
        if logical {
            lwlock::LWLockRelease(lwlock::main_lock(procarray::PROC_ARRAY_LOCK))?;
        }
        lwlock::LWLockRelease(lwlock::main_lock(procarray::XID_GEN_LOCK))?;
        Ok(recptr)
    })
}

fn running_xacts_header(
    running: &procarray::RunningTransactions<'_>,
) -> [u8; MIN_SIZE_OF_XACT_RUNNING_XACTS] {
    let mut hdr = [0u8; MIN_SIZE_OF_XACT_RUNNING_XACTS];
    hdr[0..4].copy_from_slice(&(running.xcnt as i32).to_ne_bytes());
    hdr[4..8].copy_from_slice(&(running.subxcnt as i32).to_ne_bytes());
    hdr[8] = running.subxid_overflow as u8;
    hdr[12..16].copy_from_slice(&running.next_xid.to_ne_bytes());
    hdr[16..20].copy_from_slice(&running.oldest_running_xid.to_ne_bytes());
    hdr[20..24].copy_from_slice(&running.latest_completed_xid.to_ne_bytes());
    hdr
}

fn LogCurrentRunningXacts(running: &procarray::RunningTransactions<'_>) -> PgResult<XLogRecPtr> {
    let hdr = running_xacts_header(running);

    let xid_count = running.xcnt + running.subxcnt;
    debug_assert_eq!(running.xids.len(), xid_count);
    // SAFETY: TransactionId is u32; registered byte-for-byte as C does.
    let xids_bytes: &[u8] =
        unsafe { core::slice::from_raw_parts(running.xids.as_ptr().cast::<u8>(), xid_count * 4) };

    let main_data: &[&[u8]] = if xid_count > 0 { &[&hdr, xids_bytes] } else { &[&hdr] };
    let recptr = xloginsert::insert_record(
        RM_STANDBY_ID,
        XLOG_RUNNING_XACTS,
        transam_xlog::XLOG_MARK_UNIMPORTANT,
        main_data,
        &[],
    )?;

    if running.subxid_overflow {
        elog(
            DEBUG2,
            format!(
                "snapshot of {} running transactions overflowed (lsn {:X}/{:08X} oldest xid {} latest complete {} next xid {})",
                running.xcnt,
                recptr >> 32,
                recptr as u32,
                running.oldest_running_xid,
                running.latest_completed_xid,
                running.next_xid,
            ),
        )?;
    } else {
        elog(
            DEBUG2,
            format!(
                "snapshot of {}+{} running transaction ids (lsn {:X}/{:08X} oldest xid {} latest complete {} next xid {})",
                running.xcnt,
                running.subxcnt,
                recptr >> 32,
                recptr as u32,
                running.oldest_running_xid,
                running.latest_completed_xid,
                running.next_xid,
            ),
        )?;
    }

    transam_xlog::XLogSetAsyncXactLSN(recptr);

    Ok(recptr)
}

fn standby_locks_body(locks: &[xl_standby_lock]) -> Vec<u8> {
    let mut body = Vec::with_capacity(locks.len() * SIZE_OF_XL_STANDBY_LOCK);
    for l in locks {
        body.extend_from_slice(&l.xid.to_ne_bytes());
        body.extend_from_slice(&l.dbOid.to_ne_bytes());
        body.extend_from_slice(&l.relOid.to_ne_bytes());
    }
    body
}

// Only AccessExclusiveLocks are ever logged (lmgr/README).
fn LogAccessExclusiveLocks(locks: &[xl_standby_lock]) -> PgResult<()> {
    let hdr: [u8; OFFSET_OF_XL_STANDBY_LOCKS_LOCKS] = (locks.len() as i32).to_ne_bytes();
    let body = standby_locks_body(locks);

    xloginsert::insert_record(
        RM_STANDBY_ID,
        XLOG_STANDBY_LOCK,
        transam_xlog::XLOG_MARK_UNIMPORTANT,
        &[&hdr, &body],
        &[],
    )?;
    Ok(())
}

pub fn LogAccessExclusiveLock(dbOid: Oid, relOid: Oid) -> PgResult<()> {
    let xlrec = xl_standby_lock {
        xid: xact_seams::get_current_transaction_id::call()?,
        dbOid,
        relOid,
    };

    LogAccessExclusiveLocks(std::slice::from_ref(&xlrec))?;
    xact_seams::or_my_xact_flags::call(XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK);
    Ok(())
}

// Assign an xid before the lock reaches shared memory: standby release needs
// the completion record; GetRunningTransactionLocks must never see one invalid.
pub fn LogAccessExclusiveLockPrepare() -> PgResult<()> {
    xact_seams::get_current_transaction_id::call()?;
    Ok(())
}

fn invalidations_header(relcache_init_file_inval: bool, nmsgs: usize) -> [u8; MIN_SIZE_OF_INVALIDATIONS] {
    let mut xlrec = [0u8; MIN_SIZE_OF_INVALIDATIONS];
    xlrec[0..4].copy_from_slice(&init_small::globals::MyDatabaseId().to_ne_bytes());
    xlrec[4..8].copy_from_slice(&init_small::globals::MyDatabaseTableSpace().to_ne_bytes());
    xlrec[8] = relcache_init_file_inval as u8;
    xlrec[12..16].copy_from_slice(&(nmsgs as i32).to_ne_bytes());
    xlrec
}

pub fn LogStandbyInvalidations(
    msgs: &[SharedInvalidationMessage],
    relcache_init_file_inval: bool,
) -> PgResult<()> {
    let xlrec = invalidations_header(relcache_init_file_inval, msgs.len());
    let mut body = Vec::with_capacity(msgs.len() * SHARED_INVALIDATION_MESSAGE_SIZE);
    for msg in msgs {
        body.extend_from_slice(&msg.to_wire_bytes());
    }
    xloginsert::insert_record(RM_STANDBY_ID, XLOG_INVALIDATIONS, 0, &[&xlrec, &body], &[])?;
    Ok(())
}

pub fn standby_redo(record: &mut xlogreader_seams::XLogReaderState) -> PgResult<()> {
    let decoded = record
        .record
        .as_ref()
        .expect("standby_redo dispatched on a reader with no decoded record");
    debug_assert!(decoded.max_block_id < 0);
    let info = decoded.xl_info & !XLR_INFO_MASK;

    if xlogutils::standby_state() == xlogutils::STANDBY_DISABLED {
        return Ok(());
    }
    // SAFETY: points into the reader's decode buffer, valid for the redo call.
    let data: &[u8] = unsafe { decoded.main_data_bytes() };
    match info {
        XLOG_STANDBY_LOCK => {
            let nlocks = i32::from_ne_bytes(data[0..4].try_into().unwrap()) as usize;
            for i in 0..nlocks {
                let base = OFFSET_OF_XL_STANDBY_LOCKS_LOCKS + i * SIZE_OF_XL_STANDBY_LOCK;
                let xid = u32::from_ne_bytes(data[base..base + 4].try_into().unwrap());
                let dbOid = u32::from_ne_bytes(data[base + 4..base + 8].try_into().unwrap());
                let relOid = u32::from_ne_bytes(data[base + 8..base + 12].try_into().unwrap());
                StandbyAcquireAccessExclusiveLock(xid, dbOid, relOid)?;
            }
            Ok(())
        }
        XLOG_RUNNING_XACTS => {
            let xcnt = i32::from_ne_bytes(data[0..4].try_into().unwrap());
            let subxcnt = i32::from_ne_bytes(data[4..8].try_into().unwrap());
            let subxid_overflow = data[8] != 0;
            let next_xid = u32::from_ne_bytes(data[12..16].try_into().unwrap());
            let oldest_running_xid = u32::from_ne_bytes(data[16..20].try_into().unwrap());
            let latest_completed_xid = u32::from_ne_bytes(data[20..24].try_into().unwrap());

            let cx = mcx::MemoryContext::new("RunningXactsRedo");
            let total = (xcnt + subxcnt) as usize;
            let mut xids = mcx::vec_with_capacity_in(cx.mcx(), total)?;
            for i in 0..total {
                let base = MIN_SIZE_OF_XACT_RUNNING_XACTS + i * 4;
                xids.push(u32::from_ne_bytes(data[base..base + 4].try_into().unwrap()));
            }
            let running = procarray::RunningTransactionsData {
                xcnt,
                subxcnt,
                subxid_status: if subxid_overflow { SUBXIDS_MISSING } else { SUBXIDS_IN_ARRAY },
                nextXid: next_xid,
                oldestRunningXid: oldest_running_xid,
                // Not carried by xl_running_xacts; unused on the redo side.
                oldestDatabaseRunningXid: oldest_running_xid,
                latestCompletedXid: latest_completed_xid,
                xids,
            };
            procarray::ProcArrayApplyRecoveryInfo(&running)
        }
        XLOG_INVALIDATIONS => {
            let dbId = u32::from_ne_bytes(data[0..4].try_into().unwrap());
            let tsId = u32::from_ne_bytes(data[4..8].try_into().unwrap());
            let relcacheInitFileInval = data[8] != 0;
            let nmsgs = i32::from_ne_bytes(data[12..16].try_into().unwrap()) as usize;
            let mut msgs = Vec::with_capacity(nmsgs);
            for i in 0..nmsgs {
                let base = MIN_SIZE_OF_INVALIDATIONS + i * SHARED_INVALIDATION_MESSAGE_SIZE;
                msgs.push(
                    SharedInvalidationMessage::from_wire_bytes(
                        data[base..base + SHARED_INVALIDATION_MESSAGE_SIZE].try_into().unwrap(),
                    )
                    .expect("standby_redo: unrecognized sinval message id"),
                );
            }
            inval::eoxact::ProcessCommittedInvalidationMessages(
                &msgs,
                relcacheInitFileInval,
                dbId,
                tsId,
            )
        }
        _ => panic!("standby_redo: unknown op code {info}"),
    }
}

pub fn init_seams() {
    standby_seams::log_standby_snapshot::set(LogStandbySnapshot);
    standby_seams::log_standby_invalidations::set(LogStandbyInvalidations);
    standby_seams::log_access_exclusive_lock::set(LogAccessExclusiveLock);
    standby_seams::log_access_exclusive_lock_prepare::set(LogAccessExclusiveLockPrepare);
    standby_seams::standby_release_lock_tree::set(StandbyReleaseLockTree);
    standby_seams::init_recovery_transaction_environment::set(InitRecoveryTransactionEnvironment);
    standby_seams::shutdown_recovery_transaction_environment::set(
        ShutdownRecoveryTransactionEnvironment,
    );
    standby_seams::standby_acquire_access_exclusive_lock::set(StandbyAcquireAccessExclusiveLock);
    standby_seams::resolve_recovery_conflict_with_lock::set(ResolveRecoveryConflictWithLock);
    standby_seams::log_recovery_conflict::set(recovery::LogRecoveryConflict);
    standby_seams::resolve_recovery_conflict_with_buffer_pin::set(
        ResolveRecoveryConflictWithBufferPin,
    );
    standby_seams::check_recovery_conflict_deadlock::set(CheckRecoveryConflictDeadlock);
    standby_seams::resolve_recovery_conflict_with_snapshot::set(
        ResolveRecoveryConflictWithSnapshot,
    );
    standby_seams::resolve_recovery_conflict_with_snapshot_full_xid::set(
        ResolveRecoveryConflictWithSnapshotFullXid,
    );
    procarray::procarray_seams::standby_release_old_locks::set(StandbyReleaseOldLocks);
    recovery::install_guc_backings();
}
