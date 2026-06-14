//! F4 — per-xid visibility tests + backend / vxid lookup + counting
//! (procarray.c).
//!
//! `TransactionIdIsInProgress`/`TransactionIdIsActive`, the PGPROC/pid lookups
//! (`ProcNumberGetProc`/`BackendPidGetProc`/`BackendXidGetPid`/`IsBackendPid`),
//! the virtual-xid scans (`GetCurrentVirtualXIDs`/`GetConflictingVirtualXIDs`/
//! the delaychkpt set / cancel / signal), the per-db/-user backend counts and
//! cancels, and `XidCacheRemoveRunningXids`. Builds on the F0 model; reuses F3
//! GlobalVis for some removability checks.

use std::cell::RefCell;

use mcx::{Mcx, PgVec};
use types_core::{
    InvalidLocalTransactionId, InvalidOid, InvalidTransactionId, Oid, ProcNumber, TransactionId,
};
use types_core::xact::TransactionIdIsValid;
use types_error::{PgResult, WARNING};
use types_storage::storage::PROC_IS_AUTOVACUUM;
use types_storage::{LWLockMode, ProcSignalReason, VirtualTransactionId};

use backend_access_transam_subtrans_seams as subtrans;
use backend_access_transam_transam_seams as transam;
use backend_access_transam_varsup_seams as varsup;
use backend_access_transam_xact_seams as xact;
use backend_access_transam_xlog_seams as xlog;
use backend_storage_ipc_procsignal_seams as procsignal;
use backend_storage_lmgr_lwlock_seams as lwlock;
use backend_storage_lmgr_proc_seams as proc;
use backend_utils_error::elog;
use backend_utils_init_miscinit_seams as miscinit;
use backend_utils_init_small::globals;
use port_pgsleep_seams as pgsleep;
use backend_utils_time_snapmgr_pc_seams as snapmgr;

use crate::knownassignedxids::{KnownAssignedXidExists, KnownAssignedXidsGet};
use crate::shmem_model::{CACHED_XID_IS_NOT_IN_PROGRESS, PROC_ARRAY};

/// `ROLE_PG_SIGNAL_BACKEND` (`catalog/pg_authid.h`).
const ROLE_PG_SIGNAL_BACKEND: Oid = 4200;

/// `MAXAUTOVACPIDS` (procarray.c) — max autovacs to SIGTERM per iteration.
const MAXAUTOVACPIDS: usize = 10;

thread_local! {
    /// The C `static TransactionId *xids` scratch workspace in
    /// `TransactionIdIsInProgress` (malloc'd once, reused). Holds main XIDs with
    /// uncached children gathered during the array scan.
    static IS_IN_PROGRESS_XIDS: RefCell<Vec<TransactionId>> = const { RefCell::new(Vec::new()) };
}

/// `TransactionIdIsInProgress(TransactionId xid)` (procarray.c) — is `xid` still
/// shown running in the ProcArray (or a still-running subxact)? Allocates a
/// scratch xids array on first use (OOM surface on `Err`).
pub fn TransactionIdIsInProgress(xid: TransactionId) -> PgResult<bool> {
    // Don't bother checking a transaction older than RecentXmin; it could not
    // possibly still be running. (Rejects InvalidTransactionId, Frozen, etc.)
    if transam::transaction_id_precedes::call(xid, snapmgr::recent_xmin::call()) {
        return Ok(false);
    }

    // If already known completed, fall out without touching shared memory.
    if CACHED_XID_IS_NOT_IN_PROGRESS.with(|c| *c.borrow()) == xid {
        return Ok(false);
    }

    // We can handle our own (sub)transactions without shared-memory access.
    if xact::transaction_id_is_current_transaction_id::call(xid) {
        return Ok(true);
    }

    let in_recovery = xlog::recovery_in_progress::call();

    // First time through, get workspace to remember main XIDs in. In hot
    // standby, reserve room for the whole known-assigned list.
    IS_IN_PROGRESS_XIDS.with(|w| {
        let mut w = w.borrow_mut();
        if w.is_empty() {
            let maxxids = if in_recovery {
                // TOTAL_MAX_CACHED_SUBXIDS (== GetMaxSnapshotSubxidCount).
                crate::shmem_model::GetMaxSnapshotSubxidCount() as usize
            } else {
                PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().maxProcs) as usize
            };
            w.reserve(maxxids);
        }
    });

    let mut nxids = 0usize;
    // The collected main-Xids workspace, cleared per call (C reuses the malloc
    // block but starts nxids = 0 each call).
    IS_IN_PROGRESS_XIDS.with(|w| w.borrow_mut().clear());

    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_SHARED)?;

    // Now that we have the lock, check latestCompletedXid; if the target Xid is
    // after that, it's surely still running.
    let latest_completed_xid = varsup::get_latest_completed_xid::call().xid();
    if transam::transaction_id_precedes::call(latest_completed_xid, xid) {
        lwlock::lwlock_release_proc_array::call()?;
        return Ok(true);
    }

    // No shortcuts, gotta grovel through the array.
    let mypgxactoff = proc::proc_pgxactoff::call(proc::my_proc_number::call());
    let num_procs = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().numProcs);

    for pgxactoff in 0..num_procs {
        // Ignore ourselves --- dealt with above.
        if pgxactoff == mypgxactoff {
            continue;
        }

        // Fetch xid just once.
        let pxid = proc::proc_array_xid::call(pgxactoff);
        if !TransactionIdIsValid(pxid) {
            continue;
        }

        // Step 1: check the main Xid.
        if pxid == xid {
            lwlock::lwlock_release_proc_array::call()?;
            return Ok(true);
        }

        // Ignore main Xids younger than the target Xid (can't be its parent).
        if transam::transaction_id_precedes::call(xid, pxid) {
            continue;
        }

        // Step 2: check the cached child-Xids arrays.
        let pgprocno =
            PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().pgprocnos[pgxactoff as usize]);
        let (pxids, subxids) = proc::proc_subxids::call(pgprocno);
        for j in (0..pxids).rev() {
            let cxid = subxids[j as usize];
            if cxid == xid {
                lwlock::lwlock_release_proc_array::call()?;
                return Ok(true);
            }
        }

        // Save the main Xid for step 4, but only if it has uncached children.
        let (_count, overflowed) = proc::proc_array_subxid_state::call(pgxactoff);
        if overflowed {
            IS_IN_PROGRESS_XIDS.with(|w| w.borrow_mut().push(pxid));
            nxids += 1;
        }
    }

    // Step 3: in hot standby mode, check the known-assigned-xids list.
    if in_recovery {
        // None of the PGPROC entries should have XIDs in hot standby mode.
        debug_assert_eq!(nxids, 0);

        if KnownAssignedXidExists(xid) {
            lwlock::lwlock_release_proc_array::call()?;
            return Ok(true);
        }

        // If the KnownAssignedXids overflowed, we must check pg_subtrans too.
        let last_overflowed =
            PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().lastOverflowedXid);
        if transam::transaction_id_precedes_or_equals::call(xid, last_overflowed) {
            let n = IS_IN_PROGRESS_XIDS.with(|w| {
                let mut w = w.borrow_mut();
                // KnownAssignedXidsGet writes into xids[]; size it to maxProcs.
                let cap = w.capacity().max(1);
                w.resize(cap, InvalidTransactionId);
                let n = KnownAssignedXidsGet(&mut w[..], xid) as usize;
                w.truncate(n);
                n
            });
            nxids = n;
        }
    }

    lwlock::lwlock_release_proc_array::call()?;

    // If none of the relevant caches overflowed, the Xid is not running.
    if nxids == 0 {
        CACHED_XID_IS_NOT_IN_PROGRESS.with(|c| *c.borrow_mut() = xid);
        return Ok(false);
    }

    // Step 4: have to check pg_subtrans. It's either a subtransaction of one of
    // the Xids in xids[], or it's not running. If it's an already-failed
    // subtransaction, say "not running" even though the parent may still run.
    if transam::transaction_id_did_abort::call(xid, snapmgr::recent_xmin::call())? {
        CACHED_XID_IS_NOT_IN_PROGRESS.with(|c| *c.borrow_mut() = xid);
        return Ok(false);
    }

    // It isn't aborted, so check whether the tree it belongs to is still
    // running (as of when we held ProcArrayLock).
    let topxid = subtrans::sub_trans_get_topmost_transaction::call(xid)?;
    debug_assert!(TransactionIdIsValid(topxid));
    if topxid != xid {
        let found = IS_IN_PROGRESS_XIDS.with(|w| w.borrow().iter().any(|&x| x == topxid));
        if found {
            return Ok(true);
        }
    }

    CACHED_XID_IS_NOT_IN_PROGRESS.with(|c| *c.borrow_mut() = xid);
    Ok(false)
}

/// `TransactionIdIsActive(TransactionId xid)` (procarray.c) — is `xid` the
/// top-level xid of a backend currently executing (stricter than InProgress)?
pub fn TransactionIdIsActive(xid: TransactionId) -> PgResult<bool> {
    let mut result = false;

    // Don't bother checking a transaction older than RecentXmin.
    if transam::transaction_id_precedes::call(xid, snapmgr::recent_xmin::call()) {
        return Ok(false);
    }

    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_SHARED)?;

    let num_procs = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().numProcs);
    for i in 0..num_procs {
        let pgprocno =
            PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().pgprocnos[i as usize]);

        // Fetch xid just once (dense ProcGlobal->xids[i]).
        let pxid = proc::proc_array_xid::call(i);
        if !TransactionIdIsValid(pxid) {
            continue;
        }

        if proc::proc_pid::call(pgprocno) == 0 {
            continue; // ignore prepared transactions
        }

        if pxid == xid {
            result = true;
            break;
        }
    }

    lwlock::lwlock_release_proc_array::call()?;

    Ok(result)
}

/// `ProcNumberGetProc(ProcNumber procNumber)` (procarray.c) — is the slot a live
/// (pid != 0) backend within `[0, allProcCount)`? Returns the slot number when
/// the PGPROC is active, mirroring the C "NULL when inactive" contract.
fn ProcNumberGetProc(proc_number: ProcNumber) -> Option<ProcNumber> {
    // if (procNumber < 0 || procNumber >= ProcGlobal->allProcCount) return NULL;
    if proc_number < 0 || proc_number as u32 >= proc::proc_all_proc_count::call() {
        return None;
    }
    // result = GetPGProcByNumber(procNumber); if (result->pid == 0) return NULL;
    if proc::proc_pid::call(proc_number) == 0 {
        return None;
    }
    Some(proc_number)
}

/// `ProcNumberGetProc(ProcNumber procNumber)->pid` (procarray.c) — pid of the
/// PGPROC in that slot, or 0 when the slot is inactive (C NULL).
pub fn ProcNumberGetProcPid(proc_number: ProcNumber) -> i32 {
    match ProcNumberGetProc(proc_number) {
        Some(p) => proc::proc_pid::call(p),
        None => 0,
    }
}

/// `ProcNumberGetProc(procNumber)` projected to `(databaseId, tempNamespaceId)`
/// (procarray.c) — `checkTempNamespaceStatus`'s reads; `None` when the slot is
/// empty.
pub fn ProcStatus(proc_number: ProcNumber) -> Option<(Oid, Oid)> {
    ProcNumberGetProc(proc_number)
        .map(|p| (proc::proc_database_id::call(p), proc::proc_temp_namespace_id::call(p)))
}

/// `ProcNumberGetTransactionIds(ProcNumber procNumber, ...)` (procarray.c) —
/// the `(xid, xmin, nsubxid, overflowed)` advertised by that slot.
pub fn ProcNumberGetTransactionIds(
    proc_number: ProcNumber,
) -> (TransactionId, TransactionId, i32, bool) {
    // *xid = InvalidTransactionId; *xmin = InvalidTransactionId;
    // *nsubxid = 0; *overflowed = false;
    let mut xid = InvalidTransactionId;
    let mut xmin = InvalidTransactionId;
    let mut nsubxid = 0;
    let mut overflowed = false;

    // if (procNumber < 0 || procNumber >= ProcGlobal->allProcCount) return;
    if proc_number < 0 || proc_number as u32 >= proc::proc_all_proc_count::call() {
        return (xid, xmin, nsubxid, overflowed);
    }

    // Need to lock out additions/removals of backends.
    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_SHARED)
        .expect("ProcNumberGetTransactionIds: ProcArrayLock acquire");

    if proc::proc_pid::call(proc_number) != 0 {
        xid = proc::proc_xid::call(proc_number);
        xmin = proc::proc_xmin::call(proc_number);
        let (count, ovf) = proc::proc_subxid_status::call(proc_number);
        nsubxid = count;
        overflowed = ovf;
    }

    lwlock::lwlock_release_proc_array::call()
        .expect("ProcNumberGetTransactionIds: ProcArrayLock release");

    (xid, xmin, nsubxid, overflowed)
}

/// `BackendPidGetProcWithLock(int pid)` (procarray.c) — like `BackendPidGetProc`
/// but the caller already holds `ProcArrayLock`; returns the slot's
/// `ProcNumber` or `None`.
pub fn BackendPidGetProcWithLock(pid: i32) -> Option<ProcNumber> {
    // if (pid == 0) return NULL; /* never match dummy PGPROCs */
    if pid == 0 {
        return None;
    }

    let num_procs = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().numProcs);
    for index in 0..num_procs {
        let pgprocno =
            PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().pgprocnos[index as usize]);
        if proc::proc_pid::call(pgprocno) == pid {
            return Some(pgprocno);
        }
    }
    None
}

/// `BackendPidGetProc(int pid)` projected to `(roleId, procNumber)`
/// (procarray.c + `GetNumberFromPGProc`) — the live backend with that pid, or
/// `None`. Takes `ProcArrayLock` shared around the scan.
pub fn BackendPidGetProcRole(pid: i32) -> Option<(Oid, ProcNumber)> {
    // if (pid == 0) return NULL; /* never match dummy PGPROCs */
    if pid == 0 {
        return None;
    }

    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_SHARED)
        .expect("BackendPidGetProc: ProcArrayLock acquire");

    let result = BackendPidGetProcWithLock(pid);

    lwlock::lwlock_release_proc_array::call().expect("BackendPidGetProc: ProcArrayLock release");

    result.map(|p| (proc::proc_role_id::call(p), p))
}

/// `BackendXidGetPid(TransactionId xid)` (procarray.c) — pid of the backend
/// running top-level `xid`, or 0. Only main transaction ids are considered.
pub fn BackendXidGetPid(xid: TransactionId) -> i32 {
    let mut result = 0;

    // if (xid == InvalidTransactionId) return 0; /* never match invalid xid */
    if xid == InvalidTransactionId {
        return 0;
    }

    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_SHARED)
        .expect("BackendXidGetPid: ProcArrayLock acquire");

    // TransactionId *other_xids = ProcGlobal->xids; scanned by dense index.
    let num_procs = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().numProcs);
    for index in 0..num_procs {
        if proc::proc_array_xid::call(index) == xid {
            let pgprocno =
                PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().pgprocnos[index as usize]);
            result = proc::proc_pid::call(pgprocno);
            break;
        }
    }

    lwlock::lwlock_release_proc_array::call().expect("BackendXidGetPid: ProcArrayLock release");

    result
}

/// `IsBackendPid(int pid)` (procarray.c) — is `pid` a live backend?
pub fn IsBackendPid(pid: i32) -> bool {
    // return (BackendPidGetProc(pid) != NULL);
    BackendPidGetProcRole(pid).is_some()
}

/// `GetCurrentVirtualXIDs(...)` (procarray.c) — the VXIDs of currently-running
/// backends matching the filter, allocated in `mcx`.
pub fn GetCurrentVirtualXIDs<'mcx>(
    mcx: Mcx<'mcx>,
    limit_xmin: TransactionId,
    exclude_xmin0: bool,
    all_dbs: bool,
    exclude_vacuum: i32,
) -> PgResult<PgVec<'mcx, VirtualTransactionId>> {
    // allocate what's certainly enough result space (maxProcs).
    let max_procs = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().maxProcs);
    let mut vxids = mcx::vec_with_capacity_in(mcx, max_procs.max(0) as usize)?;

    let my_database_id = globals::MyDatabaseId();

    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_SHARED)?;

    let num_procs = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().numProcs);
    for index in 0..num_procs {
        let pgprocno =
            PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().pgprocnos[index as usize]);
        let status_flags = proc::proc_global_status_flags::call(index);

        if proc::proc_is_my_proc::call(pgprocno) {
            continue;
        }

        if (exclude_vacuum & status_flags as i32) != 0 {
            continue;
        }

        if all_dbs || proc::proc_database_id::call(pgprocno) == my_database_id {
            // Fetch xmin just once - might change on us.
            let pxmin = proc::proc_xmin::call(pgprocno);

            if exclude_xmin0 && !TransactionIdIsValid(pxmin) {
                continue;
            }

            // InvalidTransactionId precedes all other XIDs, so a proc that
            // hasn't set xmin yet will not be rejected by this test.
            if !TransactionIdIsValid(limit_xmin)
                || transam::transaction_id_precedes_or_equals::call(pxmin, limit_xmin)
            {
                let (proc_number, lxid) = proc::proc_vxid::call(pgprocno);
                let vxid = VirtualTransactionId {
                    procNumber: proc_number,
                    localTransactionId: lxid,
                };
                if vxid.localTransactionId != InvalidLocalTransactionId {
                    vxids.push(vxid);
                }
            }
        }
    }

    lwlock::lwlock_release_proc_array::call()?;

    Ok(vxids)
}

/// `GetVirtualXIDsDelayingChkpt(int *nvxids, int type)` (procarray.c) — VXIDs of
/// backends with `delayChkptFlags & type` set, allocated in `mcx`.
pub fn GetVirtualXIDsDelayingChkpt<'mcx>(
    mcx: Mcx<'mcx>,
    delay_chkpt_type: i32,
) -> PgResult<PgVec<'mcx, VirtualTransactionId>> {
    debug_assert!(delay_chkpt_type != 0);

    // palloc enough result space; PgVec grows as the (bounded) scan pushes.
    let mut vxids = PgVec::new_in(mcx);

    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_SHARED)?;

    let num_procs = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().numProcs);
    for index in 0..num_procs {
        let pgprocno =
            PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().pgprocnos[index as usize]);

        if (proc::proc_delay_chkpt_flags::call(pgprocno) & delay_chkpt_type) != 0 {
            let (proc_number, lxid) = proc::proc_vxid::call(pgprocno);
            let vxid = VirtualTransactionId {
                procNumber: proc_number,
                localTransactionId: lxid,
            };
            // if (VirtualTransactionIdIsValid(vxid)) vxids[count++] = vxid;
            if vxid.localTransactionId != InvalidLocalTransactionId {
                vxids.push(vxid);
            }
        }
    }

    lwlock::lwlock_release_proc_array::call()?;

    Ok(vxids)
}

/// `HaveVirtualXIDsDelayingChkpt(VirtualTransactionId *vxids, int nvxids, int type)`
/// (procarray.c) — are any of `vxids` still delaying a checkpoint?
pub fn HaveVirtualXIDsDelayingChkpt(vxids: &[VirtualTransactionId], delay_chkpt_type: i32) -> bool {
    debug_assert!(delay_chkpt_type != 0);

    let mut result = false;

    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_SHARED)
        .expect("HaveVirtualXIDsDelayingChkpt: ProcArrayLock acquire");

    let num_procs = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().numProcs);
    for index in 0..num_procs {
        let pgprocno =
            PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().pgprocnos[index as usize]);

        let (proc_number, lxid) = proc::proc_vxid::call(pgprocno);
        let vxid = VirtualTransactionId {
            procNumber: proc_number,
            localTransactionId: lxid,
        };

        if (proc::proc_delay_chkpt_flags::call(pgprocno) & delay_chkpt_type) != 0
            && vxid.localTransactionId != InvalidLocalTransactionId
        {
            for other in vxids.iter() {
                // VirtualTransactionIdEquals: both fields match.
                if vxid.procNumber == other.procNumber
                    && vxid.localTransactionId == other.localTransactionId
                {
                    result = true;
                    break;
                }
            }
            if result {
                break;
            }
        }
    }

    lwlock::lwlock_release_proc_array::call()
        .expect("HaveVirtualXIDsDelayingChkpt: ProcArrayLock release");

    result
}

/// `CancelVirtualTransaction(VirtualTransactionId vxid, ProcSignalReason sigmode)`
/// (procarray.c) — pid signalled, or 0 if not found.
pub fn CancelVirtualTransaction(
    vxid: VirtualTransactionId,
    sigmode: ProcSignalReason,
) -> PgResult<i32> {
    SignalVirtualTransaction(vxid, sigmode, true)
}

/// `SignalVirtualTransaction(VirtualTransactionId vxid, ProcSignalReason sigmode,
/// bool conflictPending)` (procarray.c) — pid signalled, or 0 if not found.
pub fn SignalVirtualTransaction(
    vxid: VirtualTransactionId,
    sigmode: ProcSignalReason,
    conflict_pending: bool,
) -> PgResult<i32> {
    let mut pid = 0;

    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_SHARED)?;

    let num_procs = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().numProcs);
    for index in 0..num_procs {
        let pgprocno =
            PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().pgprocnos[index as usize]);
        let (proc_number, lxid) = proc::proc_vxid::call(pgprocno);

        if proc_number == vxid.procNumber && lxid == vxid.localTransactionId {
            proc::set_proc_recovery_conflict_pending::call(pgprocno, conflict_pending);
            pid = proc::proc_pid::call(pgprocno);
            if pid != 0 {
                // Kill the pid if it's still here. If not, that's what we
                // wanted so ignore any errors.
                let _ = procsignal::send_proc_signal::call(pid, sigmode, vxid.procNumber);
            }
            break;
        }
    }

    lwlock::lwlock_release_proc_array::call()?;

    Ok(pid)
}

/// `MinimumActiveBackends(int min)` (procarray.c) — quick check whether at least
/// `min` backends have an active transaction (no lock taken).
pub fn MinimumActiveBackends(min: i32) -> bool {
    let mut count = 0;

    // Quick short-circuit if no minimum is specified.
    if min == 0 {
        return true;
    }

    // Note: for speed, we don't acquire ProcArrayLock. This is a little bogus,
    // but since we are only testing fields for zero or nonzero, it should be OK.
    let num_procs = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().numProcs);
    for index in 0..num_procs {
        let pgprocno =
            PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().pgprocnos[index as usize]);

        if pgprocno == -1 {
            continue; // do not count deleted entries
        }
        if proc::proc_is_my_proc::call(pgprocno) {
            continue; // do not count myself
        }
        if proc::proc_xid::call(pgprocno) == InvalidTransactionId {
            continue; // do not count if no XID assigned
        }
        if proc::proc_pid::call(pgprocno) == 0 {
            continue; // do not count prepared xacts
        }
        if proc::proc_is_waiting_on_lock::call(pgprocno) {
            continue; // do not count if blocked on a lock
        }
        count += 1;
        if count >= min {
            break;
        }
    }

    count >= min
}

/// `CountDBBackends(Oid databaseid)` (procarray.c).
pub fn CountDBBackends(databaseid: Oid) -> PgResult<i32> {
    let mut count = 0;

    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_SHARED)?;

    let num_procs = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().numProcs);
    for index in 0..num_procs {
        let pgprocno =
            PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().pgprocnos[index as usize]);

        if proc::proc_pid::call(pgprocno) == 0 {
            continue; // do not count prepared xacts
        }
        if databaseid == InvalidOid || proc::proc_database_id::call(pgprocno) == databaseid {
            count += 1;
        }
    }

    lwlock::lwlock_release_proc_array::call()?;

    Ok(count)
}

/// `CountDBConnections(Oid databaseid)` (procarray.c).
pub fn CountDBConnections(databaseid: Oid) -> PgResult<i32> {
    let mut count = 0;

    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_SHARED)?;

    let num_procs = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().numProcs);
    for index in 0..num_procs {
        let pgprocno =
            PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().pgprocnos[index as usize]);

        if proc::proc_pid::call(pgprocno) == 0 {
            continue; // do not count prepared xacts
        }
        if !proc::proc_is_regular_backend::call(pgprocno) {
            continue; // count only regular backend processes
        }
        if databaseid == InvalidOid || proc::proc_database_id::call(pgprocno) == databaseid {
            count += 1;
        }
    }

    lwlock::lwlock_release_proc_array::call()?;

    Ok(count)
}

/// `CancelDBBackends(Oid databaseid, ProcSignalReason sigmode, bool conflictPending)`
/// (procarray.c).
pub fn CancelDBBackends(
    databaseid: Oid,
    sigmode: ProcSignalReason,
    conflict_pending: bool,
) -> PgResult<()> {
    // tell all backends to die
    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_EXCLUSIVE)?;

    let num_procs = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().numProcs);
    for index in 0..num_procs {
        let pgprocno =
            PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().pgprocnos[index as usize]);

        if databaseid == InvalidOid || proc::proc_database_id::call(pgprocno) == databaseid {
            let (proc_number, _lxid) = proc::proc_vxid::call(pgprocno);

            proc::set_proc_recovery_conflict_pending::call(pgprocno, conflict_pending);
            let pid = proc::proc_pid::call(pgprocno);
            if pid != 0 {
                // Kill the pid if it's still here; ignore any errors.
                let _ = procsignal::send_proc_signal::call(pid, sigmode, proc_number);
            }
        }
    }

    lwlock::lwlock_release_proc_array::call()?;

    Ok(())
}

/// `CountUserBackends(Oid roleid)` (procarray.c).
pub fn CountUserBackends(roleid: Oid) -> PgResult<i32> {
    let mut count = 0;

    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_SHARED)?;

    let num_procs = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().numProcs);
    for index in 0..num_procs {
        let pgprocno =
            PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().pgprocnos[index as usize]);

        if proc::proc_pid::call(pgprocno) == 0 {
            continue; // do not count prepared xacts
        }
        if !proc::proc_is_regular_backend::call(pgprocno) {
            continue; // count only regular backend processes
        }
        if proc::proc_role_id::call(pgprocno) == roleid {
            count += 1;
        }
    }

    lwlock::lwlock_release_proc_array::call()?;

    Ok(count)
}

/// `CountOtherDBBackends(Oid databaseId, int *nbackends, int *nprepared)`
/// (procarray.c) — used by DROP DATABASE; returns `(found_other, nbackends,
/// nprepared)`.
pub fn CountOtherDBBackends(database_id: Oid) -> PgResult<(bool, i32, i32)> {
    // The caller's *nbackends/*nprepared out-params: set each iteration and
    // left at the last iteration's values on timeout.
    let mut nbackends = 0;
    let mut nprepared = 0;

    // 50 tries with 100ms sleep between tries makes 5 sec total wait.
    for _tries in 0..50 {
        let mut nautovacs = 0usize;
        let mut autovac_pids = [0i32; MAXAUTOVACPIDS];
        let mut found = false;

        miscinit::check_for_interrupts::call()?;

        nbackends = 0;
        nprepared = 0;

        lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_SHARED)?;

        let num_procs = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().numProcs);
        for index in 0..num_procs {
            let pgprocno =
                PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().pgprocnos[index as usize]);
            let status_flags = proc::proc_global_status_flags::call(index);

            if proc::proc_database_id::call(pgprocno) != database_id {
                continue;
            }
            if proc::proc_is_my_proc::call(pgprocno) {
                continue;
            }

            found = true;

            let pid = proc::proc_pid::call(pgprocno);
            if pid == 0 {
                nprepared += 1;
            } else {
                nbackends += 1;
                if (status_flags & PROC_IS_AUTOVACUUM) != 0 && nautovacs < MAXAUTOVACPIDS {
                    autovac_pids[nautovacs] = pid;
                    nautovacs += 1;
                }
            }
        }

        lwlock::lwlock_release_proc_array::call()?;

        if !found {
            return Ok((false, nbackends, nprepared)); // no conflicting backends
        }

        // Send SIGTERM to any conflicting autovacuums before sleeping. We
        // postpone this step until after the loop because we don't want to hold
        // ProcArrayLock while issuing kill().
        for &pid in autovac_pids.iter().take(nautovacs) {
            unsafe {
                libc::kill(pid, libc::SIGTERM); // ignore any error
            }
        }

        // sleep, then try again
        pgsleep::pg_usleep::call(100 * 1000); // 100ms
    }

    // timed out, still conflicts (*nbackends/*nprepared hold the last counts).
    Ok((true, nbackends, nprepared))
}

/// `TerminateOtherDBBackends(Oid databaseId)` (procarray.c) — SIGTERM every
/// other backend connected to `databaseId` (FORCE drop). `mcx` is the transient
/// context for the database-name error string (C uses `CurrentMemoryContext`).
pub fn TerminateOtherDBBackends<'mcx>(mcx: Mcx<'mcx>, database_id: Oid) -> PgResult<()> {
    let mut pids: Vec<i32> = Vec::new();
    let mut nprepared = 0;

    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_SHARED)?;

    let num_procs = PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().numProcs);
    for i in 0..num_procs {
        let pgprocno =
            PROC_ARRAY.with(|pa| pa.borrow().as_ref().unwrap().pgprocnos[i as usize]);

        if proc::proc_database_id::call(pgprocno) != database_id {
            continue;
        }
        if proc::proc_is_my_proc::call(pgprocno) {
            continue;
        }

        let pid = proc::proc_pid::call(pgprocno);
        if pid != 0 {
            pids.push(pid);
        } else {
            nprepared += 1;
        }
    }

    lwlock::lwlock_release_proc_array::call()?;

    if nprepared > 0 {
        let dbname = backend_commands_dbcommands_seams::get_database_name::call(mcx, database_id)?
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        return backend_utils_error::ereport(types_error::ERROR)
            .errcode(types_error::ERRCODE_OBJECT_IN_USE)
            .errmsg(format!(
                "database \"{dbname}\" is being used by prepared transactions"
            ))
            .errdetail_plural(
                format!("There is {nprepared} prepared transaction using the database."),
                format!("There are {nprepared} prepared transactions using the database."),
                nprepared as u64,
            )
            .finish(types_error::ErrorLocation::new(
                "src/backend/storage/ipc/procarray.c",
                3886,
                "TerminateOtherDBBackends",
            ));
    }

    if !pids.is_empty() {
        // Permissions checks relax the pg_terminate_backend checks in two ways,
        // both by omitting the !OidIsValid(proc->roleId) check (accept autovac
        // workers + bgworkers).
        for &pid in pids.iter() {
            if let Some((role_id, _procno)) = BackendPidGetProcRole(pid) {
                if superuser_arg(role_id)? && !superuser()? {
                    return permission_denied_to_terminate(
                        "Only roles with the SUPERUSER attribute may terminate \
                         processes of roles with the SUPERUSER attribute.",
                    );
                }

                let user_id = miscinit::get_user_id::call();
                if !has_privs_of_role(user_id, role_id)?
                    && !has_privs_of_role(user_id, ROLE_PG_SIGNAL_BACKEND)?
                {
                    return permission_denied_to_terminate(
                        "Only roles with privileges of the role whose process is \
                         being terminated or with privileges of the \
                         \"pg_signal_backend\" role may terminate this process.",
                    );
                }
            }
        }

        // There's a race condition here: once we release ProcArrayLock, the
        // session might exit before we kill. Too unlikely to worry about.
        for &pid in pids.iter() {
            if BackendPidGetProcRole(pid).is_some() {
                // If we have setsid(), signal the backend's whole process group.
                unsafe {
                    libc::kill(-pid, libc::SIGTERM);
                }
            }
        }
    }

    Ok(())
}

/// `superuser_arg(roleid)` via the superuser seam.
#[inline]
fn superuser_arg(roleid: Oid) -> PgResult<bool> {
    backend_utils_misc_superuser_seams::superuser_arg::call(roleid)
}

/// `superuser()` via the superuser seam.
#[inline]
fn superuser() -> PgResult<bool> {
    backend_utils_misc_superuser_seams::superuser::call()
}

/// `has_privs_of_role(member, role)` via the acl seam.
#[inline]
fn has_privs_of_role(member: Oid, role: Oid) -> PgResult<bool> {
    backend_utils_adt_acl_seams::has_privs_of_role::call(member, role)
}

/// `ereport(ERROR, errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
/// errmsg("permission denied to terminate process"), errdetail(...))`.
fn permission_denied_to_terminate(detail: &str) -> PgResult<()> {
    backend_utils_error::ereport(types_error::ERROR)
        .errcode(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
        .errmsg("permission denied to terminate process")
        .errdetail(detail.to_string())
        .finish(types_error::ErrorLocation::new(
            "src/backend/storage/ipc/procarray.c",
            3917,
            "TerminateOtherDBBackends",
        ))
}

/// `XidCacheRemoveRunningXids(TransactionId xid, int nxids,
/// const TransactionId *xids, TransactionId latestXid)` (procarray.c) — drop
/// aborted subxids from PGPROC's subxid cache.
pub fn XidCacheRemoveRunningXids(
    xid: TransactionId,
    children: &[TransactionId],
    latest_xid: TransactionId,
) -> PgResult<()> {
    debug_assert!(TransactionIdIsValid(xid));

    // We must hold ProcArrayLock exclusively in order to remove transactions
    // from the PGPROC array. (See access/transam/README.)
    lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_EXCLUSIVE)?;

    // proc.c owns the MyProc subxid-cache layout: it performs the C
    // find-and-swap-with-last removal of each child (and `xid`), keeping the
    // ProcGlobal->subxidStates[pgxactoff] mirror in sync, and returns the xids
    // it could not find while the cache had not overflowed.
    let not_found =
        proc::remove_running_subxids_from_proc::call(children.to_vec(), xid);
    for anxid in not_found {
        // Ordinarily we should have found it, unless the cache has overflowed.
        // It's also possible to be invoked multiple times for the same subxact
        // on an error during AbortSubTransaction, so warn rather than Assert.
        let _ = elog(WARNING, format!("did not find subXID {anxid} in MyProc"));
    }

    // Also advance global latestCompletedXid while holding the lock.
    crate::membership::MaintainLatestCompletedXid(latest_xid);

    // ... and xactCompletionCount.
    varsup::increment_xact_completion_count::call();

    lwlock::lwlock_release_proc_array::call()?;

    Ok(())
}

/// Install the F4-owned inward seams: visibility / lookup / count / cancel,
/// consumed by lmgr, standby, pmsignal, commands, namespace.
pub fn init_seams() {
    use backend_storage_ipc_procarray_seams as seams;

    seams::transaction_id_is_in_progress::set(TransactionIdIsInProgress);
    seams::proc_number_get_proc_pid::set(ProcNumberGetProcPid);
    seams::proc_status::set(ProcStatus);
    seams::backend_pid_get_proc_role::set(BackendPidGetProcRole);
    seams::is_backend_pid::set(IsBackendPid);
    seams::cancel_virtual_transaction::set(CancelVirtualTransaction);
    seams::signal_virtual_transaction::set(SignalVirtualTransaction);
    seams::count_db_backends::set(CountDBBackends);
    seams::count_db_connections::set(CountDBConnections);
    seams::count_user_backends::set(CountUserBackends);
    seams::cancel_db_backends::set(CancelDBBackends);
    seams::xid_cache_remove_running_xids::set(XidCacheRemoveRunningXids);
}
