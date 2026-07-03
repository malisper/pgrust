//! standby.c primary snapshot half + AEL lock export; recovery half unported.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use elog::elog;
use types_core::{Oid, XLogRecPtr, XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK};
use types_error::{PgResult, DEBUG2};
use types_storage::sinval::{SharedInvalidationMessage, SHARED_INVALIDATION_MESSAGE_SIZE};
use types_storage::storage::xl_standby_lock;

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

    // C returns before every arm unless in hot standby (recovery half unported).
    if xlogutils::standby_state() == xlogutils::STANDBY_DISABLED {
        return Ok(());
    }
    match info {
        XLOG_STANDBY_LOCK => {
            panic!("standby_redo: StandbyAcquireAccessExclusiveLock unported (hot standby)")
        }
        XLOG_RUNNING_XACTS => {
            panic!("standby_redo: ProcArrayApplyRecoveryInfo unported (hot standby)")
        }
        XLOG_INVALIDATIONS => {
            panic!("standby_redo: ProcessCommittedInvalidationMessages unported (hot standby)")
        }
        _ => panic!("standby_redo: unknown op code {info}"),
    }
}

pub fn init_seams() {
    standby_seams::log_standby_snapshot::set(LogStandbySnapshot);
    standby_seams::log_standby_invalidations::set(LogStandbyInvalidations);
    standby_seams::log_access_exclusive_lock::set(LogAccessExclusiveLock);
    standby_seams::log_access_exclusive_lock_prepare::set(LogAccessExclusiveLockPrepare);
}
