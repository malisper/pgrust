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
use types_core::{Oid, ProcNumber, TransactionId};
use types_error::PgResult;
use types_storage::{ProcSignalReason, VirtualTransactionId};

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

/// `ProcNumberGetProc(ProcNumber procNumber)->pid` (procarray.c) — pid of the
/// PGPROC in that slot, or 0 when the slot is inactive (C NULL).
pub fn ProcNumberGetProcPid(_proc_number: ProcNumber) -> i32 {
    panic!("decomp: ProcNumberGetProcPid not yet filled")
}

/// `ProcNumberGetProc(procNumber)` projected to `(databaseId, tempNamespaceId)`
/// (procarray.c) — `checkTempNamespaceStatus`'s reads; `None` when the slot is
/// empty.
pub fn ProcStatus(_proc_number: ProcNumber) -> Option<(Oid, Oid)> {
    panic!("decomp: ProcStatus not yet filled")
}

/// `ProcNumberGetTransactionIds(ProcNumber procNumber, ...)` (procarray.c) —
/// the `(xid, xmin, nsubxid, overflowed)` advertised by that slot.
pub fn ProcNumberGetTransactionIds(
    _proc_number: ProcNumber,
) -> (TransactionId, TransactionId, i32, bool) {
    panic!("decomp: ProcNumberGetTransactionIds not yet filled")
}

/// `BackendPidGetProc(int pid)` projected to `(roleId, procNumber)`
/// (procarray.c + `GetNumberFromPGProc`) — the live backend with that pid, or
/// `None`.
pub fn BackendPidGetProcRole(_pid: i32) -> Option<(Oid, ProcNumber)> {
    panic!("decomp: BackendPidGetProcRole not yet filled")
}

/// `BackendPidGetProcWithLock(int pid)` (procarray.c) — like `BackendPidGetProc`
/// but the caller already holds `ProcArrayLock`; returns the slot's
/// `ProcNumber` or `None`.
pub fn BackendPidGetProcWithLock(_pid: i32) -> Option<ProcNumber> {
    panic!("decomp: BackendPidGetProcWithLock not yet filled")
}

/// `BackendXidGetPid(TransactionId xid)` (procarray.c) — pid of the backend
/// running top-level `xid`, or 0.
pub fn BackendXidGetPid(_xid: TransactionId) -> i32 {
    panic!("decomp: BackendXidGetPid not yet filled")
}

/// `IsBackendPid(int pid)` (procarray.c) — is `pid` a live backend?
pub fn IsBackendPid(_pid: i32) -> bool {
    panic!("decomp: IsBackendPid not yet filled")
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

/// `GetVirtualXIDsDelayingChkpt(...)` (procarray.c) — VXIDs of backends with
/// `delayChkpt` set matching `type`, allocated in `mcx`.
pub fn GetVirtualXIDsDelayingChkpt<'mcx>(
    _mcx: Mcx<'mcx>,
    _delay_chkpt_type: i32,
) -> PgResult<PgVec<'mcx, VirtualTransactionId>> {
    panic!("decomp: GetVirtualXIDsDelayingChkpt not yet filled")
}

/// `HaveVirtualXIDsDelayingChkpt(VirtualTransactionId *vxids, int nvxids, int type)`
/// (procarray.c) — are any of `vxids` still delaying a checkpoint?
pub fn HaveVirtualXIDsDelayingChkpt(_vxids: &[VirtualTransactionId], _delay_chkpt_type: i32) -> bool {
    panic!("decomp: HaveVirtualXIDsDelayingChkpt not yet filled")
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
