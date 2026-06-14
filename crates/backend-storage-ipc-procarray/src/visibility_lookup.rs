//! F4 — per-xid visibility tests + backend / vxid lookup + counting
//! (procarray.c).
//!
//! `TransactionIdIsInProgress`/`TransactionIdIsActive`, the PGPROC/pid lookups
//! (`ProcNumberGetProc`/`BackendPidGetProc`/`BackendXidGetPid`/`IsBackendPid`),
//! the virtual-xid scans (`GetCurrentVirtualXIDs`/`GetConflictingVirtualXIDs`/
//! the delaychkpt set / cancel / signal), the per-db/-user backend counts and
//! cancels, and `XidCacheRemoveRunningXids`. Builds on the F0 model; reuses F3
//! GlobalVis for some removability checks.

use mcx::{Mcx, PgVec};
use types_core::{
    InvalidLocalTransactionId, InvalidTransactionId, Oid, ProcNumber, TransactionId,
};
use types_error::PgResult;
use types_storage::{LWLockMode, ProcSignalReason, VirtualTransactionId};

use backend_storage_lmgr_lwlock_seams as lwlock;
use backend_storage_lmgr_proc_seams as proc;

use crate::shmem_model::PROC_ARRAY;

/// `TransactionIdIsInProgress(TransactionId xid)` (procarray.c) — is `xid` still
/// shown running in the ProcArray (or a still-running subxact)? Allocates a
/// scratch xids array on first use (OOM surface on `Err`).
pub fn TransactionIdIsInProgress(_xid: TransactionId) -> PgResult<bool> {
    panic!("decomp: TransactionIdIsInProgress not yet filled")
}

/// `TransactionIdIsActive(TransactionId xid)` (procarray.c) — is `xid` the
/// top-level xid of a backend currently executing (stricter than InProgress)?
pub fn TransactionIdIsActive(_xid: TransactionId) -> PgResult<bool> {
    panic!("decomp: TransactionIdIsActive not yet filled")
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
    _mcx: Mcx<'mcx>,
    _limit_xmin: TransactionId,
    _exclude_xmin0: bool,
    _allow_db_id: Oid,
    _exclude_vacuum: i32,
) -> PgResult<PgVec<'mcx, VirtualTransactionId>> {
    panic!("decomp: GetCurrentVirtualXIDs not yet filled")
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
    _vxid: VirtualTransactionId,
    _sigmode: ProcSignalReason,
) -> PgResult<i32> {
    panic!("decomp: CancelVirtualTransaction not yet filled")
}

/// `SignalVirtualTransaction(VirtualTransactionId vxid, ProcSignalReason sigmode,
/// bool conflictPending)` (procarray.c) — pid signalled, or 0 if not found.
pub fn SignalVirtualTransaction(
    _vxid: VirtualTransactionId,
    _sigmode: ProcSignalReason,
    _conflict_pending: bool,
) -> PgResult<i32> {
    panic!("decomp: SignalVirtualTransaction not yet filled")
}

/// `MinimumActiveBackends(int min)` (procarray.c) — quick check whether at least
/// `min` backends have an active transaction (no lock taken).
pub fn MinimumActiveBackends(_min: i32) -> bool {
    panic!("decomp: MinimumActiveBackends not yet filled")
}

/// `CountDBBackends(Oid databaseid)` (procarray.c).
pub fn CountDBBackends(_databaseid: Oid) -> PgResult<i32> {
    panic!("decomp: CountDBBackends not yet filled")
}

/// `CountDBConnections(Oid databaseid)` (procarray.c).
pub fn CountDBConnections(_databaseid: Oid) -> PgResult<i32> {
    panic!("decomp: CountDBConnections not yet filled")
}

/// `CancelDBBackends(Oid databaseid, ProcSignalReason sigmode, bool conflictPending)`
/// (procarray.c).
pub fn CancelDBBackends(
    _databaseid: Oid,
    _sigmode: ProcSignalReason,
    _conflict_pending: bool,
) -> PgResult<()> {
    panic!("decomp: CancelDBBackends not yet filled")
}

/// `CountUserBackends(Oid roleid)` (procarray.c).
pub fn CountUserBackends(_roleid: Oid) -> PgResult<i32> {
    panic!("decomp: CountUserBackends not yet filled")
}

/// `CountOtherDBBackends(Oid databaseId, int *nbackends, int *nprepared)`
/// (procarray.c) — used by DROP DATABASE; returns `(found_other, nbackends,
/// nprepared)`.
pub fn CountOtherDBBackends(_database_id: Oid) -> PgResult<(bool, i32, i32)> {
    panic!("decomp: CountOtherDBBackends not yet filled")
}

/// `TerminateOtherDBBackends(Oid databaseId)` (procarray.c) — SIGTERM every
/// other backend connected to `databaseId` (FORCE drop).
pub fn TerminateOtherDBBackends(_database_id: Oid) -> PgResult<()> {
    panic!("decomp: TerminateOtherDBBackends not yet filled")
}

/// `XidCacheRemoveRunningXids(TransactionId xid, int nxids,
/// const TransactionId *xids, TransactionId latestXid)` (procarray.c) — drop
/// aborted subxids from PGPROC's subxid cache.
pub fn XidCacheRemoveRunningXids(
    _xid: TransactionId,
    _children: &[TransactionId],
    _latest_xid: TransactionId,
) -> PgResult<()> {
    panic!("decomp: XidCacheRemoveRunningXids not yet filled")
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
