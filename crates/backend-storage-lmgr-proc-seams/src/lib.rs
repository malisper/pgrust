//! Seam declarations for the `backend-storage-lmgr-proc` unit
//! (`storage/lmgr/proc.c`): the PGPROC array fields the LWLock wait-list
//! machinery reads and writes (`GetPGProcByNumber(procno)->lwWaiting /
//! lwWaitMode / lwWaitLink`) and the per-process wait semaphore
//! (`PGSemaphoreLock` / `PGSemaphoreUnlock` on `GetPGProcByNumber(procno)->sem`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::LocalTransactionId;
use types_core::Oid;
use types_core::ProcNumber;
use types_core::TimestampTz;
use types_error::PgResult;
use types_storage::{proclist_node, LWLockMode, LWLockWaitState};

seam_core::seam!(
    /// Read `GetPGProcByNumber(procno)->lwWaiting`.
    pub fn proc_lw_waiting(procno: ProcNumber) -> LWLockWaitState
);

seam_core::seam!(
    /// Set `GetPGProcByNumber(procno)->lwWaiting`.
    pub fn set_proc_lw_waiting(procno: ProcNumber, state: LWLockWaitState)
);

seam_core::seam!(
    /// Read `GetPGProcByNumber(procno)->lwWaitMode`.
    pub fn proc_lw_wait_mode(procno: ProcNumber) -> LWLockMode
);

seam_core::seam!(
    /// Set `GetPGProcByNumber(procno)->lwWaitMode`.
    pub fn set_proc_lw_wait_mode(procno: ProcNumber, mode: LWLockMode)
);

seam_core::seam!(
    /// Read `GetPGProcByNumber(procno)->lwWaitLink` (the C
    /// `proclist_node_get(procno, offsetof(PGPROC, lwWaitLink))`).
    pub fn proc_lw_wait_link(procno: ProcNumber) -> proclist_node
);

seam_core::seam!(
    /// Write `GetPGProcByNumber(procno)->lwWaitLink`.
    pub fn set_proc_lw_wait_link(procno: ProcNumber, node: proclist_node)
);

seam_core::seam!(
    /// Read `GetPGProcByNumber(procno)->cvWaitLink` (the C
    /// `proclist_node_get(procno, offsetof(PGPROC, cvWaitLink))`).
    pub fn proc_cv_wait_link(procno: ProcNumber) -> proclist_node
);

seam_core::seam!(
    /// Write `GetPGProcByNumber(procno)->cvWaitLink`.
    pub fn set_proc_cv_wait_link(procno: ProcNumber, node: proclist_node)
);

seam_core::seam!(
    /// `SetLatch(&GetPGProcByNumber(procno)->procLatch)` — wake the given
    /// backend via its process latch. Infallible in C.
    pub fn set_proc_latch(procno: ProcNumber)
);

seam_core::seam!(
    /// `PGSemaphoreLock(GetPGProcByNumber(procno)->sem)` — block the current
    /// backend on its wait semaphore until signaled.
    pub fn pg_semaphore_lock(procno: ProcNumber)
);

seam_core::seam!(
    /// `PGSemaphoreUnlock(GetPGProcByNumber(procno)->sem)` — signal a
    /// backend's wait semaphore.
    pub fn pg_semaphore_unlock(procno: ProcNumber)
);

seam_core::seam!(
    /// `ProcWaitForSignal(wait_event_info)` — wait on the process latch until
    /// signalled. Can `ereport(ERROR)` via `CHECK_FOR_INTERRUPTS`.
    pub fn proc_wait_for_signal(wait_event_info: u32) -> PgResult<()>
);

seam_core::seam!(
    /// `DeadlockTimeout` (proc.c GUC, in milliseconds).
    pub fn deadlock_timeout() -> i32
);

seam_core::seam!(
    /// `pg_atomic_read_u64(&MyProc->waitStart)`.
    pub fn my_proc_wait_start() -> TimestampTz
);

seam_core::seam!(
    /// `pg_atomic_write_u64(&MyProc->waitStart, value)`.
    pub fn set_my_proc_wait_start(value: TimestampTz)
);

seam_core::seam!(
    /// `MyProc->vxid.procNumber = value` — stamp the proc's vxid proc number
    /// (standby.c does this for the Startup process before
    /// `VirtualXactLockTableInsert`).
    pub fn set_my_proc_vxid_proc_number(value: types_core::ProcNumber)
);

seam_core::seam!(
    /// `MyProc->tempNamespaceId = nspid` (namespace.c writes the field; the
    /// PGPROC storage belongs to proc.c). Plain shared-memory field write.
    pub fn set_my_proc_temp_namespace_id(nspid: Oid)
);

seam_core::seam!(
    /// Read `MyProc->vxid.lxid`.
    pub fn my_proc_lxid() -> LocalTransactionId
);

seam_core::seam!(
    /// Write `MyProc->vxid.lxid` (StartTransaction advertises the new local
    /// xid in the proc array).
    pub fn set_my_proc_lxid(lxid: LocalTransactionId)
);

seam_core::seam!(
    /// Read the `transaction_timeout` GUC (`int TransactionTimeout`, proc.c).
    pub fn transaction_timeout() -> i32
);

seam_core::seam!(
    /// `LockErrorCleanup()` — clean up any open wait-for-lock state.
    pub fn lock_error_cleanup()
);

seam_core::seam!(
    /// Set/clear the `DELAY_CHKPT_START` bit in `MyProc->delayChkptFlags`
    /// (the commit critical section's checkpoint interlock).
    pub fn my_proc_set_delay_chkpt_start(on: bool)
);

seam_core::seam!(
    /// `&GetPGProcByNumber(procno)->procLatch` — the process latch embedded
    /// in a backend's PGPROC entry, as a handle usable with the latch seams
    /// (`set_latch` to wake that backend). Pure array lookup; infallible.
    pub fn proc_latch(procno: ProcNumber) -> types_storage::latch::LatchHandle
);

// ---- dummy-PGPROC stand-up for prepared transactions (twophase.c) ----

seam_core::seam!(
    /// `MarkAsPreparingGuts`'s `MemSet(proc, 0, ...)` + fixed-field init of the
    /// dummy PGPROC numbered `pgprocno`: clones `MyProc`'s VXID when a valid
    /// LXID exists (else uses `xid` / `INVALID_PROC_NUMBER`), zeroes the lock
    /// lists/wait state, and stows `xid` / `owner` / `databaseid`. Plain
    /// shared-memory writes; cannot `ereport`.
    pub fn proc_init_prepared(
        pgprocno: ProcNumber,
        xid: types_core::TransactionId,
        owner: Oid,
        databaseid: Oid,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `GXactLoadSubxactData(gxact, nsubxacts, children)` — copy up to
    /// `PGPROC_MAX_CACHED_SUBXIDS` of `children` into the dummy PGPROC's
    /// `subxids`, setting the overflow flag when the count exceeds the cache.
    pub fn gxact_load_subxact_data(
        pgprocno: ProcNumber,
        children: &[types_core::TransactionId],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `MyProcNumber` (proc.c global) — this backend's proc number, stamped into
    /// `gxact->locking_backend`. Pure read of backend-local state.
    pub fn my_proc_number() -> ProcNumber
);

seam_core::seam!(
    /// `GetPGProcByNumber(pgprocno)->databaseId` — the dummy PGPROC's database,
    /// read by `LockGXact`/`pg_prepared_xact`. Plain shared-memory read.
    pub fn proc_database_id(pgprocno: ProcNumber) -> Oid
);

seam_core::seam!(
    /// `GetPGProcByNumber(pgprocno)->xid` — the dummy PGPROC's running xid, read
    /// by `pg_prepared_xact`. Plain shared-memory read.
    pub fn proc_xid(pgprocno: ProcNumber) -> types_core::TransactionId
);

seam_core::seam!(
    /// `GET_VXID_FROM_PGPROC(vxid, *GetPGProcByNumber(pgprocno))` — the dummy
    /// PGPROC's `(procNumber, localTransactionId)` pair, read by
    /// `TwoPhaseGetXidByVirtualXID`. Plain shared-memory read.
    pub fn proc_vxid(pgprocno: ProcNumber) -> (ProcNumber, u32)
);

seam_core::seam!(
    /// `GetNumberFromPGProc(&PreparedXactProcs[i])` — the proc number assigned
    /// to the i-th preallocated dummy proc by `InitProcGlobal`; used by
    /// `TwoPhaseShmemInit` to build the freelist. Pure read.
    pub fn prepared_xact_procno(i: i32) -> ProcNumber
);

seam_core::seam!(
    /// `MyProc->delayChkptFlags |= DELAY_CHKPT_START` (on=true) / `&=
    /// ~DELAY_CHKPT_START` (on=false) — the checkpoint-delay bracket around the
    /// prepare/commit WAL insert. Plain shared-memory field write.
    pub fn set_delay_chkpt_start(on: bool)
);

seam_core::seam!(
    /// `InitProcess()` (`storage/lmgr/proc.c`) — create this backend's PGPROC
    /// struct in shared memory; must run before any LWLock / shared-memory
    /// use. The "sorry, too many clients already" overflow path
    /// `ereport(FATAL)`s, which terminates the process inside the owner, so
    /// this is modeled infallible at the boundary.
    pub fn init_process()
);
