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
use types_storage::lock::LOCKMASK;
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

// ---- MyProc / PGPROC-array owner accessors --------------------------------
//
// `MyProc` and the `ProcGlobal->allProcs[]` array (and the intrusive
// `lockGroupMembers` lists threaded through it) are owned by this crate but
// are stood up by `InitProcess` / `proc_shmem::InitProcGlobal`, which have not
// landed. proc_misc's lock-group logic reaches that state through these
// accessors (the `my_proc()` convention); each panics until the owner state
// lands, exactly as the C `MyProc`/`ProcGlobal` globals are undefined before
// `InitProcGlobal`.

/// `GetNumberFromPGProc(MyProc)` (`MyProcNumber`) — this backend's slot index.
#[allow(dead_code)]
pub(crate) fn my_proc_number() -> ProcNumber {
    todo!("proc.c: GetNumberFromPGProc(MyProc)")
}

/// `GetNumberFromPGProc(proc)` — the slot index of an arbitrary `PGPROC`
/// (pointer arithmetic against `ProcGlobal->allProcs` in C).
#[allow(dead_code)]
pub(crate) fn proc_number_of(_proc: &PGPROC) -> ProcNumber {
    todo!("proc.c: GetNumberFromPGProc(proc)")
}

/// `GetPGProcByNumber(procno)->lockGroupLeader == GetPGProcByNumber(leaderno)`
/// — whether the proc in slot `procno` has slot `leaderno` as its lock-group
/// leader.
#[allow(dead_code)]
pub(crate) fn proc_lock_group_leader_is(_procno: ProcNumber, _leaderno: ProcNumber) -> bool {
    todo!("proc.c: GetPGProcByNumber(procno)->lockGroupLeader == GetPGProcByNumber(leaderno)")
}

/// `GetPGProcByNumber(procno)->lockGroupLeader == NULL`.
#[allow(dead_code)]
pub(crate) fn proc_lock_group_leader_is_none(_procno: ProcNumber) -> bool {
    todo!("proc.c: GetPGProcByNumber(procno)->lockGroupLeader == NULL")
}

/// `MyProc->lockGroupLeader = GetPGProcByNumber(leaderno)`.
#[allow(dead_code)]
pub(crate) fn set_my_proc_lock_group_leader(_leaderno: ProcNumber) {
    todo!("proc.c: MyProc->lockGroupLeader = GetPGProcByNumber(leaderno)")
}

/// `dlist_push_head(&GetPGProcByNumber(leaderno)->lockGroupMembers,
///  &GetPGProcByNumber(memberno)->lockGroupLink)`.
#[allow(dead_code)]
pub(crate) fn lock_group_members_push_head(_leaderno: ProcNumber, _memberno: ProcNumber) {
    todo!("proc.c: dlist_push_head(&leader->lockGroupMembers, &member->lockGroupLink)")
}

/// `dlist_push_tail(&GetPGProcByNumber(leaderno)->lockGroupMembers,
///  &GetPGProcByNumber(memberno)->lockGroupLink)`.
#[allow(dead_code)]
pub(crate) fn lock_group_members_push_tail(_leaderno: ProcNumber, _memberno: ProcNumber) {
    todo!("proc.c: dlist_push_tail(&leader->lockGroupMembers, &member->lockGroupLink)")
}

/// `dlist_foreach(iter, &GetPGProcByNumber(leaderno)->lockGroupMembers)` —
/// iterate the slot indices of every member of `leaderno`'s lock group
/// (`dlist_container(PGPROC, lockGroupLink, iter.cur)`).
#[allow(dead_code)]
pub(crate) fn lock_group_members_iter(_leaderno: ProcNumber) -> Vec<ProcNumber> {
    todo!("proc.c: walk GetPGProcByNumber(leaderno)->lockGroupMembers")
}

/// The `holdMask` of every `PROCLOCK` on
/// `GetPGProcByNumber(procno)->myProcLocks[partition]`.
#[allow(dead_code)]
pub(crate) fn my_proc_locks_hold_masks(
    _procno: ProcNumber,
    _partition: usize,
) -> Vec<LOCKMASK> {
    todo!("proc.c: hold masks on GetPGProcByNumber(procno)->myProcLocks[partition]")
}
