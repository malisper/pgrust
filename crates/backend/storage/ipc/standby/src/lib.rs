//! standby.c primary snapshot half; recovery half + lock export unported.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use elog::elog;
use types_core::XLogRecPtr;
use types_error::{PgResult, DEBUG2};

// rmgrlist.h / standbydefs.h; RM_STANDBY_ID is test-pinned to 8 in rmgr.
const RM_STANDBY_ID: u8 = 8;
const XLOG_RUNNING_XACTS: u8 = 0x10;
const MIN_SIZE_OF_XACT_RUNNING_XACTS: usize = 24;

#[cfg(test)]
mod tests;

pub fn LogStandbySnapshot() -> PgResult<XLogRecPtr> {
    debug_assert!(transam_xlog::XLogStandbyInfoActive());

    // GetRunningTransactionLocks reduction: a relation AEL under standby info
    // is provably absent (lock.c's prepare seam panics first); tripwire below.
    assert!(
        !standby_seams::log_access_exclusive_lock_prepare::is_installed(),
        "LogStandbySnapshot: GetRunningTransactionLocks/LogAccessExclusiveLocks unported \
         (standby.c lock export; log_access_exclusive_lock_prepare now has an installer)"
    );

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

pub fn init_seams() {
    standby_seams::log_standby_snapshot::set(LogStandbySnapshot);
}
