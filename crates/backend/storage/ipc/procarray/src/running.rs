use std::cell::RefCell;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{fence, Ordering};

use lmgr_proc::{MyProc, ProcGlobal};
use lwlock::{LWLockAcquire, LW_SHARED};
use types_core::{
    FullTransactionId, TransactionId, TransactionIdIsNormal, TransactionIdIsValid,
    TransactionIdPrecedes,
};
use types_error::PgResult;

use crate::{procArray, GetMaxSnapshotSubxidCount, ProcArrayLock, TransamVariables, XidGenLock};

pub struct RunningTransactions<'a> {
    // xcnt top-level xids followed by subxcnt subxids.
    pub xids: &'a [TransactionId],
    pub xcnt: usize,
    pub subxcnt: usize,
    pub subxid_overflow: bool,
    pub next_xid: TransactionId,
    pub oldest_running_xid: TransactionId,
    pub oldest_database_running_xid: TransactionId,
    pub latest_completed_xid: TransactionId,
}

thread_local! {
    // CurrentRunningXactsData workspace: allocated once, bgwriter-only caller.
    static RUNNING_XACTS_SCRATCH: RefCell<Vec<TransactionId>> = const { RefCell::new(Vec::new()) };
}

/// GetRunningTransactionData. ProcArrayLock and XidGenLock (both shared) are
/// HELD when `f` runs; per C's contract the caller releases them — the
/// LogStandbySnapshot closure releases ProcArrayLock before or after the WAL
/// insert per its wal_level arm and XidGenLock after it. `f` must release
/// both before returning.
#[allow(non_snake_case)]
pub fn GetRunningTransactionData<R>(
    f: impl FnOnce(&RunningTransactions<'_>) -> PgResult<R>,
) -> PgResult<R> {
    let arrayP = procArray();
    let hdr = ProcGlobal();
    let tv = TransamVariables();
    let myprocno = MyProc().expect("GetRunningTransactionData requires MyProc");
    let my_database_id = init_small::globals::MyDatabaseId();

    debug_assert!(!transam_xlog_seams::recovery_in_progress::call());

    RUNNING_XACTS_SCRATCH.with(|cell| {
        let mut xids = cell.borrow_mut();
        if xids.capacity() == 0 {
            // TOTAL_MAX_CACHED_SUBXIDS, sized before taking the locks as C.
            xids.reserve_exact(GetMaxSnapshotSubxidCount());
        }
        xids.clear();

        LWLockAcquire(ProcArrayLock(), LW_SHARED, myprocno)?;
        LWLockAcquire(XidGenLock(), LW_SHARED, myprocno)?;

        let latest_completed_xid =
            FullTransactionId::from_u64(tv.latestCompletedXid.load(Relaxed)).xid();
        let next_xid = FullTransactionId::from_u64(tv.nextXid.load(Relaxed)).xid();
        let mut oldest_running_xid = next_xid;
        let mut oldest_database_running_xid = next_xid;
        let mut suboverflowed = false;

        let num_procs = arrayP.numProcs.get() as usize;
        for index in 0..num_procs {
            // Fetch xid just once - see GetNewTransactionId.
            let xid = hdr.xids[index].read();
            if !TransactionIdIsValid(xid) {
                continue;
            }

            if TransactionIdPrecedes(xid, oldest_running_xid) {
                oldest_running_xid = xid;
            }
            if TransactionIdPrecedes(xid, oldest_database_running_xid) {
                let pgprocno = arrayP.pgprocnos[index].get();
                let proc = &hdr.allProcs[pgprocno as usize];
                if proc.databaseId.load(Relaxed) == my_database_id {
                    oldest_database_running_xid = xid;
                }
            }

            if hdr.subxidStates[index].get().overflowed {
                suboverflowed = true;
            }

            xids.push(xid);
        }
        let count = xids.len();

        let mut subcount = 0usize;
        if !suboverflowed {
            for index in 0..num_procs {
                // Owners can't add or remove entries while XidGenLock is held.
                let nsubxids = hdr.subxidStates[index].get().count as usize;
                if nsubxids > 0 {
                    let pgprocno = arrayP.pgprocnos[index].get();
                    let proc = &hdr.allProcs[pgprocno as usize];
                    fence(Ordering::Acquire); // pairs with GetNewTransactionId
                    xids.reserve(nsubxids);
                    // SAFETY: the owner only appends under XidGenLock and the
                    // count was fetched once; capacity reserved above;
                    // mirrors GetSnapshotData's locked memcpy.
                    unsafe {
                        let len = xids.len();
                        let src = (*proc.subxids.ptr()).xids.as_ptr();
                        let dst = xids.as_mut_ptr().add(len);
                        core::ptr::copy_nonoverlapping(src, dst, nsubxids);
                        xids.set_len(len + nsubxids);
                    }
                    subcount += nsubxids;
                }
            }
        }

        debug_assert!(TransactionIdIsValid(next_xid));
        debug_assert!(TransactionIdIsValid(oldest_running_xid));
        debug_assert!(TransactionIdIsNormal(latest_completed_xid));

        f(&RunningTransactions {
            xids: &xids,
            xcnt: count,
            subxcnt: subcount,
            subxid_overflow: suboverflowed,
            next_xid,
            oldest_running_xid,
            oldest_database_running_xid,
            latest_completed_xid,
        })
    })
}
