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
