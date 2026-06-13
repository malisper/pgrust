//! A process claiming and releasing its `PGPROC` slot (`storage/lmgr/proc.c`).
//!
//! `InitProcess` pops a free `PGPROC` from the freelist matching this
//! backend's class, links the embedded latch/semaphore, and registers
//! `RemoveProcFromArray`/`ProcKill` as shmem-exit callbacks. `ProcKill`
//! releases the slot's held LWLocks, removes it from any lock group, and
//! pushes it back onto its `procgloballist`. `InitAuxiliaryProcess` /
//! `AuxiliaryProcKill` do the same for the fixed auxiliary-proc slots.
//!
//! RECLAIMED here: the freelist `dlist` pop/push over the real `PGPROC` array.
//!
//! OUTWARD seams: procarray (`ProcArrayAdd`/`ProcArrayRemove`), lwlock
//! (`LWLockReleaseAll`), lock.c (`LockReleaseAll`), pmsignal, latch
//! (`OwnLatch`/`SetLatch`).
//!
//! SCAFFOLD STAGE: bodies are `todo!()`.

use mcx::Mcx;
use types_core::ProcNumber;
use types_datum::Datum;
use types_error::PgResult;
use types_storage::storage::PGPROC;

/// `InitProcess(void)` — claim a `PGPROC` for a regular/background backend.
pub fn InitProcess(_mcx: Mcx<'_>) -> PgResult<()> {
    todo!("proc.c:InitProcess")
}

/// `InitProcessPhase2(void)` — finish proc init once shared memory is fully
/// attached (adds `MyProc` to the procarray).
pub fn InitProcessPhase2(_mcx: Mcx<'_>) -> PgResult<()> {
    todo!("proc.c:InitProcessPhase2")
}

/// `InitAuxiliaryProcess(void)` — claim one of the fixed auxiliary-process
/// `PGPROC` slots (checkpointer, bgwriter, walwriter, ...).
pub fn InitAuxiliaryProcess(_mcx: Mcx<'_>) -> PgResult<()> {
    todo!("proc.c:InitAuxiliaryProcess")
}

/// `RemoveProcFromArray(int code, Datum arg)` — shmem-exit callback that
/// removes `MyProc` from the procarray.
pub fn RemoveProcFromArray(_code: i32, _arg: Datum) {
    todo!("proc.c:RemoveProcFromArray")
}

/// `ProcKill(int code, Datum arg)` — shmem-exit callback that releases this
/// backend's `PGPROC`: drop held LWLocks, leave any lock group, push the slot
/// back onto its freelist.
pub fn ProcKill(_code: i32, _arg: Datum) {
    todo!("proc.c:ProcKill")
}

/// `AuxiliaryProcKill(int code, Datum arg)` — shmem-exit callback releasing an
/// auxiliary-process `PGPROC` slot.
pub fn AuxiliaryProcKill(_code: i32, _arg: Datum) {
    todo!("proc.c:AuxiliaryProcKill")
}

/// `AuxiliaryPidGetProc(int pid)` — find the auxiliary-process `PGPROC` with
/// the given pid, or `None`.
pub fn AuxiliaryPidGetProc(_pid: i32) -> Option<ProcNumber> {
    todo!("proc.c:AuxiliaryPidGetProc")
}

/// `SetStartupBufferPinWaitBufId(int bufid)` — record the buffer the Startup
/// process is waiting for a pin on.
pub fn SetStartupBufferPinWaitBufId(_bufid: i32) {
    todo!("proc.c:SetStartupBufferPinWaitBufId")
}

/// `GetStartupBufferPinWaitBufId(void)` — the buffer the Startup process is
/// waiting for a pin on, or -1.
pub fn GetStartupBufferPinWaitBufId() -> i32 {
    todo!("proc.c:GetStartupBufferPinWaitBufId")
}

/// `HaveNFreeProcs(int n, int *nfree)` — true if at least `n` PGPROCs remain
/// on the regular freelist; reports the count seen via `nfree`.
pub fn HaveNFreeProcs(_n: i32, _nfree: &mut i32) -> bool {
    todo!("proc.c:HaveNFreeProcs")
}

/// `IsWaitingForLock(void)` — whether this backend is currently blocked on a
/// heavyweight lock (`lockAwaited != NULL`).
pub fn IsWaitingForLock() -> bool {
    todo!("proc.c:IsWaitingForLock")
}

/// Helper used by `InitProcess`/`ProcKill`: a borrow of this backend's claimed
/// `PGPROC` slot. (Kept here so the lifecycle module owns `MyProc` access.)
#[allow(dead_code)]
pub(crate) fn my_proc() -> &'static PGPROC {
    todo!("proc.c:MyProc")
}
