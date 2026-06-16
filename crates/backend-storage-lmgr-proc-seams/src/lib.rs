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
use types_core::TransactionId;
use types_deadlock::{LockId, LockSpace};
use types_error::PgResult;
use types_storage::{proclist_node, LWLockMode, LWLockWaitState, VirtualTransactionId};

seam_core::seam!(
    /// Read `MyProc->xmin` — this backend's advertised oldest-visible xmin in
    /// shared memory. Snapmgr reads it to decide whether to advance it. Plain
    /// atomic read; cannot `ereport`.
    pub fn my_proc_xmin() -> TransactionId
);

seam_core::seam!(
    /// Write `MyProc->xmin = value` — snapmgr advances/resets the backend's
    /// advertised xmin as its registered-snapshot set changes. Shared-memory
    /// store; cannot `ereport`.
    pub fn set_my_proc_xmin(value: TransactionId)
);

seam_core::seam!(
    /// Read `MyProc->vxid` (`{procNumber, lxid}`) — snapmgr uses it to name an
    /// exported snapshot's file and label the serialized transaction. Plain
    /// shared-memory read; cannot `ereport`.
    pub fn my_proc_vxid() -> VirtualTransactionId
);

seam_core::seam!(
    /// `ProcLockWakeup(GetLocksMethodTable(lock), lock)` (proc.c) — after the
    /// deadlock detector rearranges a wait queue to resolve a soft deadlock, wake
    /// any waiters that are now grantable. Takes `&mut LockSpace` because the
    /// wakeup inspects the shared lock/proc state; the detector holds all
    /// partition locks while it runs.
    pub fn proc_lock_wakeup(space: &mut LockSpace, lock: LockId)
);

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

seam_core::seam!(
    /// Run `f` over `&ProcGlobal->allProcs[procno].procLatch` — the `Latch`
    /// embedded in a backend's `PGPROC`. The proc unit owns the `allProcs`
    /// array; this callback hands the latch unit a shared reference to the
    /// embedded latch so it can apply its `SetLatch`/`OwnLatch`/`DisownLatch`
    /// algorithm to the *real* `&proc->procLatch` (faithful to the C
    /// `Latch *`), instead of the latch unit's own registry. A callback shape
    /// (not a returned reference) keeps the shmem borrow contained
    /// (AGENTS.md: seams never return `&'static mut`).
    pub fn with_proc_latch(procno: ProcNumber, f: &mut dyn FnMut(&types_storage::latch::Latch))
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
    /// `UINT32_ACCESS_ONCE(GetPGProcByNumber(pgprocno)->xmin)` — an arbitrary
    /// proc's advertised xmin, read by `ProcArrayInstall{Imported,Restored}Xmin`
    /// and `GetConflictingVirtualXIDs`. Plain shared-memory read.
    pub fn proc_xmin(pgprocno: ProcNumber) -> types_core::TransactionId
);

seam_core::seam!(
    /// `MyProc->statusFlags = flags;` — store the calling backend's status
    /// flags, used by `ProcArrayInstallRestoredXmin` to copy `PROC_XMIN_FLAGS`
    /// from the source proc. The dense `ProcGlobal->statusFlags[]` mirror is
    /// updated separately via `set_proc_array_status_flags`.
    pub fn set_my_proc_status_flags(flags: u8)
);

seam_core::seam!(
    /// `GetPGProcByNumber(pgprocno)->roleId` — the dummy PGPROC's authenticated
    /// role, read by `BackendPidGetProc`. Plain shared-memory read.
    pub fn proc_role_id(pgprocno: ProcNumber) -> Oid
);

seam_core::seam!(
    /// `GetPGProcByNumber(pgprocno)->tempNamespaceId` — the dummy PGPROC's
    /// temp-namespace oid, read by `checkTempNamespaceStatus`. Plain
    /// shared-memory read.
    pub fn proc_temp_namespace_id(pgprocno: ProcNumber) -> Oid
);

seam_core::seam!(
    /// `ProcGlobal->allProcCount` — total number of PGPROC slots, used by
    /// `ProcNumberGetTransactionIds` / `ProcNumberGetProc` for the bounds check.
    /// Pure read.
    pub fn proc_all_proc_count() -> u32
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
    /// `MyProc->delayChkptFlags |= DELAY_CHKPT_COMPLETE` (on=true) / `&=
    /// ~DELAY_CHKPT_COMPLETE` (on=false) — the second checkpoint-delay bracket
    /// `RelationTruncate` (storage.c) sets alongside `DELAY_CHKPT_START`. Plain
    /// shared-memory field write.
    pub fn set_delay_chkpt_complete(on: bool)
);

// --- PGPROC accessors used by proc.c's own wait-queue machinery -------------
//
// These read/write the `PGPROC` array entries and `MyProc` that the sibling
// `proc_lifecycle` / `proc_shmem` families own; until `InitProcGlobal` /
// `InitProcess` land, the installed bodies panic (the wait-queue family routes
// through them rather than restructuring around the unported neighbor).

seam_core::seam!(
    /// `GetNumberFromPGProc(proc)` — the proc's index in `ProcGlobal->allProcs`.
    pub fn pgproc_number(proc: &types_storage::storage::PGPROC) -> ProcNumber
);

seam_core::seam!(
    /// `GetPGProcByNumber(procno)->lockGroupLeader` as a `ProcNumber`
    /// (`INVALID_PROC_NUMBER` if `NULL`).
    pub fn proc_lock_group_leader(procno: ProcNumber) -> ProcNumber
);

seam_core::seam!(
    /// Set `GetPGProcByNumber(procno)->heldLocks`.
    pub fn set_proc_held_locks(procno: ProcNumber, mask: types_storage::lock::LOCKMASK)
);

seam_core::seam!(
    /// Read `GetPGProcByNumber(procno)->heldLocks`.
    pub fn proc_held_locks(procno: ProcNumber) -> types_storage::lock::LOCKMASK
);

seam_core::seam!(
    /// Read `GetPGProcByNumber(procno)->waitLockMode`.
    pub fn proc_wait_lock_mode(procno: ProcNumber) -> types_storage::lock::LOCKMODE
);

seam_core::seam!(
    /// Read `GetPGProcByNumber(procno)->waitStatus`.
    pub fn proc_wait_status(procno: ProcNumber) -> types_storage::storage::ProcWaitStatus
);

seam_core::seam!(
    /// Set `MyProc->{waitLock, waitProcLock, waitLockMode}` and
    /// `waitStatus = PROC_WAIT_STATUS_WAITING` for the proc joining the queue
    /// (`lock` keyed by its LOCKTAG, `holder` the owning backend's ProcNumber).
    pub fn set_proc_wait_fields(
        procno: ProcNumber,
        lock: types_storage::lock::LOCKTAG,
        holder: ProcNumber,
        lockmode: types_storage::lock::LOCKMODE,
    )
);

seam_core::seam!(
    /// Set `pg_atomic_write_u64(&GetPGProcByNumber(procno)->waitStart, value)`.
    pub fn set_proc_wait_start(procno: ProcNumber, value: u64)
);

seam_core::seam!(
    /// `dlist_node_is_detached(&GetPGProcByNumber(procno)->links)`.
    pub fn proc_wait_link_is_detached(procno: ProcNumber) -> bool
);

seam_core::seam!(
    /// `ProcWakeup`'s state reset: clear `waitLock`/`waitProcLock`, set
    /// `waitStatus = status`, and `pg_atomic_write_u64(&MyProc->waitStart, 0)`.
    pub fn wakeup_proc_clear_wait(procno: ProcNumber, status: types_storage::storage::ProcWaitStatus)
);

seam_core::seam!(
    /// `CheckDeadLock`'s awoken test: `MyProc->links.prev == NULL ||
    /// MyProc->links.next == NULL` (we've been unlinked from the wait queue).
    pub fn proc_unlinked_from_wait_queue(procno: ProcNumber) -> bool
);

seam_core::seam!(
    /// `MyProc->waitLock != NULL` (the proc is on a lock's wait queue).
    pub fn proc_is_waiting_on_lock(procno: ProcNumber) -> bool
);

seam_core::seam!(
    /// `MyProc->waitLock->tag` — the LOCKTAG of the lock the proc awaits.
    pub fn proc_wait_lock_tag(procno: ProcNumber) -> types_storage::lock::LOCKTAG
);

seam_core::seam!(
    /// `GetPGProcByNumber(procno)->pgxactoff`.
    pub fn proc_pgxactoff(procno: ProcNumber) -> i32
);

seam_core::seam!(
    /// `ProcGlobal->statusFlags[pgxactoff]` — the dense per-proc status-flag
    /// mirror in this unit's `ProcGlobal` (protected by ProcArrayLock).
    pub fn proc_global_status_flags(pgxactoff: i32) -> u8
);

seam_core::seam!(
    /// `GetPGProcByNumber(procno)->pid`.
    pub fn proc_pid(procno: ProcNumber) -> i32
);

seam_core::seam!(
    /// `GetPGProcByNumber(procno)->isRegularBackend` — true for a regular client
    /// backend (read by `CountDBConnections`/`CountUserBackends`).
    pub fn proc_is_regular_backend(procno: ProcNumber) -> bool
);

seam_core::seam!(
    /// `&MyProc->procLatch` (`storage/proc.c`) — this backend's PGPROC shared
    /// latch, the latch `SwitchToSharedLatch` points `MyLatch` at.
    pub fn my_proc_latch() -> types_storage::latch::LatchHandle
);

seam_core::seam!(
    /// `MyProc->roleId = userid` (`storage/proc.c`) — stamp this backend's
    /// PGPROC entry with the authenticated user id (an atomic store; no lock).
    pub fn set_my_proc_role_id(userid: Oid)
);

seam_core::seam!(
    /// `InitProcess()` (proc.c): initialize the per-backend `PGPROC` entry,
    /// claiming a slot from the shared `ProcGlobal` free list. `ereport(FATAL)`
    /// when no slot is available ("sorry, too many clients already").
    pub fn init_process() -> types_error::PgResult<()>
);

// --- backend-utils-init-postinit consumers (proc.c) ---

seam_core::seam!(
    /// `InitProcessPhase2()` (proc.c): add MyProc to the ProcArray; after this
    /// the backend is visible to others. `Err` carries its `ereport` surface.
    /// Takes `Mcx` to thread the owner's memory-context-scoped body.
    pub fn init_process_phase2(mcx: mcx::Mcx<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `CheckDeadLockAlert()` (proc.c): the DEADLOCK_TIMEOUT handler body.
    pub fn check_dead_lock_alert()
);

seam_core::seam!(
    /// `HaveNFreeProcs(int n, int *nfree)` (proc.c): are at least `n` PGPROC
    /// slots free? Reports the count seen via the `nfree` out-parameter, exactly
    /// as the C signature (the owner body counts the freelist under the
    /// ProcStructLock). Infallible — no `ereport`.
    pub fn have_n_free_procs(n: i32, nfree: &mut i32) -> bool
);

seam_core::seam!(
    /// `AmRegularBackendProcess()` (miscadmin.h): is this a regular client
    /// backend (not an aux/background process)?
    pub fn am_regular_backend_process() -> bool
);

// `FastPathLockGroupsPerBackend` is a `globals.c` variable (the
// `backend-utils-init-small` unit), not a proc.c global; proc.c only reads it.
// Both the getter and the `InitializeFastPathLocks` setter are homed in
// `backend-utils-init-small-seams` (`fast_path_lock_groups_per_backend` /
// `set_fast_path_lock_groups_per_backend`); no mis-homed decl lives here.

seam_core::seam!(
    /// `MyProc->databaseId = dboid` (proc.c): mark this backend's PGPROC entry
    /// with the database OID.
    pub fn set_my_proc_database_id(dboid: types_core::Oid)
);

seam_core::seam!(
    /// `ProcGlobalSemas()` (`storage/lmgr/proc.c`) — number of semaphores the
    /// PGPROC array needs; summed into the semaphore count by ipci.c
    /// `CalculateShmemSize`. Owner unported; scaffolded slot.
    pub fn proc_global_semas() -> i32
);

seam_core::seam!(
    /// `ProcGlobalShmemSize()` (proc.c) — shared-memory bytes for the PGPROC
    /// array. `Err` carries the `add_size`/`mul_size` overflow `ereport`.
    /// Owner unported; scaffolded slot.
    pub fn proc_global_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `InitProcGlobal()` (proc.c) — allocate and initialize the PGPROC array
    /// in shared memory (postmaster/standalone only, the C `!IsUnderPostmaster`
    /// arm of `CreateOrAttachShmemStructs`). `Err` carries the out-of-shmem
    /// `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn init_proc_global() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `InitializeFastPathLocks()` (proc.c) — recompute the number of
    /// fast-path lock groups; called from the EXEC_BACKEND attach path
    /// (`AttachSharedMemoryStructs`). Owner unported; scaffolded slot.
    pub fn initialize_fast_path_locks()
);

// --- clog group-commit PGPROC / ProcGlobal accessors (proc.h) ---------------
//
// The clog group-status-update optimization (`TransactionGroupUpdateXidStatus`
// in clog.c, owned by the clog crate) reads/writes the clog-group fields of the
// PGPROC array and the `ProcGlobal->clogGroupFirst` atomic queue head, all owned
// by proc.c. These accessors expose exactly those fields; the body of the
// optimization lives in the clog crate. Until `InitProcGlobal` / `InitProcess`
// land, the installed bodies panic.

seam_core::seam!(
    /// `MyProc->xid` — this backend's top-level xid, compared against the xid
    /// whose status we are about to set (clog group-update eligibility).
    pub fn my_proc_xid() -> TransactionId
);

seam_core::seam!(
    /// `(MyProc->subxidStatus.count, MyProc->subxids.xids[0..count])` — this
    /// backend's cached subxids, compared (count + `memcmp`) against the subxids
    /// being committed (clog group-update eligibility).
    pub fn my_proc_subxids() -> (i32, Vec<TransactionId>)
);

seam_core::seam!(
    /// `(GetPGProcByNumber(procno)->subxidStatus.count,
    /// GetPGProcByNumber(procno)->subxids.xids[0..count])` — a group member's
    /// cached subxids, applied by the leader during the group clog update.
    pub fn proc_subxids(procno: ProcNumber) -> (i32, Vec<TransactionId>)
);

seam_core::seam!(
    /// Prepare `MyProc`'s clog-group fields before enqueueing
    /// (`proc->clogGroupMember = true; clogGroupMemberXid = xid;
    /// clogGroupMemberXidStatus = status; clogGroupMemberPage = pageno;
    /// clogGroupMemberLsn = lsn`).
    pub fn set_my_proc_clog_group_member_data(
        xid: TransactionId,
        status: types_core::xact::XidStatus,
        pageno: i64,
        lsn: types_core::XLogRecPtr,
    )
);

seam_core::seam!(
    /// Read `MyProc->clogGroupMember`.
    pub fn my_proc_clog_group_member() -> bool
);

seam_core::seam!(
    /// Write `MyProc->clogGroupMember = value`.
    pub fn set_my_proc_clog_group_member(value: bool)
);

seam_core::seam!(
    /// Write `GetPGProcByNumber(procno)->clogGroupMember = value`.
    pub fn set_proc_clog_group_member(procno: ProcNumber, value: bool)
);

seam_core::seam!(
    /// Read `GetPGProcByNumber(procno)->clogGroupMemberPage`.
    pub fn proc_clog_group_member_page(procno: ProcNumber) -> i64
);

seam_core::seam!(
    /// `(GetPGProcByNumber(procno)->clogGroupMemberXid,
    /// clogGroupMemberXidStatus, clogGroupMemberLsn)` — the status update a
    /// group member is requesting, applied by the leader.
    pub fn proc_clog_group_member_update(
        procno: ProcNumber,
    ) -> (TransactionId, types_core::xact::XidStatus, types_core::XLogRecPtr)
);

seam_core::seam!(
    /// `pg_atomic_read_u32(&MyProc->clogGroupNext)`.
    pub fn my_proc_clog_group_next() -> u32
);

seam_core::seam!(
    /// `pg_atomic_write_u32(&MyProc->clogGroupNext, value)`.
    pub fn set_my_proc_clog_group_next(value: u32)
);

seam_core::seam!(
    /// `pg_atomic_read_u32(&GetPGProcByNumber(procno)->clogGroupNext)`.
    pub fn proc_clog_group_next(procno: ProcNumber) -> u32
);

seam_core::seam!(
    /// `pg_atomic_write_u32(&GetPGProcByNumber(procno)->clogGroupNext, value)`.
    pub fn set_proc_clog_group_next(procno: ProcNumber, value: u32)
);

seam_core::seam!(
    /// `pg_atomic_read_u32(&ProcGlobal->clogGroupFirst)`.
    pub fn clog_group_first_read() -> u32
);

seam_core::seam!(
    /// `pg_atomic_compare_exchange_u32(&ProcGlobal->clogGroupFirst, expected,
    /// newval)` — returns `(succeeded, value_seen)` (the C updates `*expected`
    /// in place to the value seen).
    pub fn clog_group_first_compare_exchange(expected: u32, newval: u32) -> (bool, u32)
);

seam_core::seam!(
    /// `pg_atomic_exchange_u32(&ProcGlobal->clogGroupFirst, newval)` — store
    /// `newval`, returning the previous value.
    pub fn clog_group_first_exchange(newval: u32) -> u32
);

seam_core::seam!(
    /// `GetNewTransactionId` top-xid publication (varsup.c): store a freshly
    /// allocated top-level `xid` into `MyProc->xid` and
    /// `ProcGlobal->xids[MyProc->pgxactoff]` while `XidGenLock` is held (its
    /// release acts as the write barrier). proc.c owns the PGPROC/ProcGlobal
    /// layout and the `Assert(subxidStatus.count == 0)` invariants.
    pub fn store_top_xid_in_proc(xid: TransactionId)
);

// --- dense ProcGlobal array + PGPROC field accessors (procarray.c membership) -
//
// The ProcArray membership family (`ProcArrayAdd`/`Remove`/`EndTransaction*`/
// `ClearTransaction`/`GroupClearXid`) reads and writes the dense
// `ProcGlobal->{xids,subxidStates,statusFlags}` mirror arrays (indexed by
// `pgxactoff`) and the per-`PGPROC` xact fields, all owned by proc.c. These
// accessors expose exactly those reads/writes; the membership algorithm (the
// sorted insert, the `pgxactoff` fixups, the lock bracketing) lives in
// procarray. Until `InitProcGlobal` lands the bodies panic.

seam_core::seam!(
    /// `ProcGlobal->xids[idx]` (under `ProcArrayLock`).
    pub fn proc_array_xid(idx: i32) -> TransactionId
);

seam_core::seam!(
    /// `ProcGlobal->xids[idx] = xid` (under `ProcArrayLock`).
    pub fn set_proc_array_xid(idx: i32, xid: TransactionId)
);

seam_core::seam!(
    /// `(ProcGlobal->subxidStates[idx].count, .overflowed)` (under `ProcArrayLock`).
    pub fn proc_array_subxid_state(idx: i32) -> (i32, bool)
);

seam_core::seam!(
    /// `ProcGlobal->subxidStates[idx] = { count, overflowed }` (under `ProcArrayLock`).
    pub fn set_proc_array_subxid_state(idx: i32, count: i32, overflowed: bool)
);

seam_core::seam!(
    /// `ProcGlobal->statusFlags[idx] = flags` (under `ProcArrayLock`). The read
    /// is [`proc_global_status_flags`].
    pub fn set_proc_array_status_flags(idx: i32, flags: u8)
);

seam_core::seam!(
    /// `memmove(&ProcGlobal->xids[dst], &ProcGlobal->xids[src], count * sizeof)`
    /// — slide a run of dense-array entries to keep them sorted by `pgxactoff`
    /// during `ProcArrayAdd`/`Remove` (under `ProcArrayLock`+`XidGenLock`).
    pub fn proc_array_xids_memmove(dst: i32, src: i32, count: i32)
);

seam_core::seam!(
    /// `memmove(&ProcGlobal->subxidStates[dst], ..[src], count * sizeof)`.
    pub fn proc_array_subxid_states_memmove(dst: i32, src: i32, count: i32)
);

seam_core::seam!(
    /// `memmove(&ProcGlobal->statusFlags[dst], ..[src], count * sizeof)`.
    pub fn proc_array_status_flags_memmove(dst: i32, src: i32, count: i32)
);

seam_core::seam!(
    /// `GetPGProcByNumber(procno)->subxidStatus` projected to `(count, overflowed)`.
    pub fn proc_subxid_status(procno: ProcNumber) -> (i32, bool)
);

seam_core::seam!(
    /// `GetPGProcByNumber(procno)->subxidStatus = { count, overflowed }`.
    pub fn set_proc_subxid_status(procno: ProcNumber, count: i32, overflowed: bool)
);

seam_core::seam!(
    /// `GetPGProcByNumber(procno)->statusFlags`.
    pub fn proc_status_flags(procno: ProcNumber) -> u8
);

seam_core::seam!(
    /// `GetPGProcByNumber(procno)->statusFlags = flags`.
    pub fn set_proc_status_flags(procno: ProcNumber, flags: u8)
);

seam_core::seam!(
    /// `GetPGProcByNumber(procno)->xid = xid`.
    pub fn set_proc_xid(procno: ProcNumber, xid: TransactionId)
);

seam_core::seam!(
    /// `GetPGProcByNumber(procno)->xmin = xmin`.
    pub fn set_proc_xmin(procno: ProcNumber, xmin: TransactionId)
);

seam_core::seam!(
    /// `GetPGProcByNumber(procno)->vxid.lxid = lxid`.
    pub fn set_proc_lxid(procno: ProcNumber, lxid: LocalTransactionId)
);

seam_core::seam!(
    /// `GetPGProcByNumber(procno)->delayChkptFlags`.
    pub fn proc_delay_chkpt_flags(procno: ProcNumber) -> i32
);

seam_core::seam!(
    /// `GetPGProcByNumber(procno)->delayChkptFlags = flags`.
    pub fn set_proc_delay_chkpt_flags(procno: ProcNumber, flags: i32)
);

seam_core::seam!(
    /// `GetPGProcByNumber(procno)->recoveryConflictPending = value`.
    pub fn set_proc_recovery_conflict_pending(procno: ProcNumber, value: bool)
);

seam_core::seam!(
    /// `GetPGProcByNumber(procno)->pgxactoff = off`.
    pub fn set_proc_pgxactoff(procno: ProcNumber, off: i32)
);

// --- ProcArray group-clear CAS over ProcGlobal->procArrayGroupFirst + the
// per-PGPROC procArrayGroup{Next,Member,MemberXid} fields (procarray.c
// `ProcArrayGroupClearXid`). The atomic ops mirror the clog group-update set
// (`clog_group_first_*` / `*_clog_group_next`). ---

seam_core::seam!(
    /// `proc->procArrayGroupMember = member; proc->procArrayGroupMemberXid = xid`.
    pub fn set_proc_array_group_member_data(procno: ProcNumber, member: bool, xid: TransactionId)
);

seam_core::seam!(
    /// `GetPGProcByNumber(procno)->procArrayGroupMember`.
    pub fn proc_array_group_member(procno: ProcNumber) -> bool
);

seam_core::seam!(
    /// `GetPGProcByNumber(procno)->procArrayGroupMember = value`.
    pub fn set_proc_array_group_member(procno: ProcNumber, value: bool)
);

seam_core::seam!(
    /// `GetPGProcByNumber(procno)->procArrayGroupMemberXid`.
    pub fn proc_array_group_member_xid(procno: ProcNumber) -> TransactionId
);

seam_core::seam!(
    /// `pg_atomic_read_u32(&GetPGProcByNumber(procno)->procArrayGroupNext)`.
    pub fn proc_array_group_next(procno: ProcNumber) -> u32
);

seam_core::seam!(
    /// `pg_atomic_write_u32(&GetPGProcByNumber(procno)->procArrayGroupNext, value)`.
    pub fn set_proc_array_group_next(procno: ProcNumber, value: u32)
);

seam_core::seam!(
    /// `pg_atomic_read_u32(&ProcGlobal->procArrayGroupFirst)`.
    pub fn proc_array_group_first_read() -> u32
);

seam_core::seam!(
    /// `pg_atomic_compare_exchange_u32(&ProcGlobal->procArrayGroupFirst,
    /// expected, newval)` — returns `(succeeded, value_seen)` (the C updates
    /// `*expected` in place to the value seen).
    pub fn proc_array_group_first_compare_exchange(expected: u32, newval: u32) -> (bool, u32)
);

seam_core::seam!(
    /// `pg_atomic_exchange_u32(&ProcGlobal->procArrayGroupFirst, newval)` —
    /// store `newval`, returning the previous value.
    pub fn proc_array_group_first_exchange(newval: u32) -> u32
);

seam_core::seam!(
    /// `GetPGProcByNumber(procno) == MyProc` — whether `procno` is this
    /// backend's own slot (the group leader skips waking itself).
    pub fn proc_is_my_proc(procno: ProcNumber) -> bool
);

seam_core::seam!(
    /// `GetNewTransactionId` subxid publication (varsup.c): push a freshly
    /// allocated subtransaction `xid` into `MyProc->subxids.xids[]` and bump
    /// `subxidStatus.count` (with the `pg_write_barrier()`), or set the
    /// `overflowed` flag when `PGPROC_MAX_CACHED_SUBXIDS` is exceeded. proc.c
    /// owns the PGPROC/ProcGlobal subxid-cache layout.
    pub fn store_subxid_in_proc(xid: TransactionId)
);

seam_core::seam!(
    /// `XidCacheRemoveRunningXids`'s subxid-cache mutation (procarray.c): remove
    /// each of `children` (and `xid`) from `MyProc->subxids.xids[]` using the C
    /// find-and-swap-with-last logic, decrementing both
    /// `MyProc->subxidStatus.count` and the `ProcGlobal->subxidStates[pgxactoff]`
    /// mirror (with the `pg_write_barrier()`). Returns the xids that were *not*
    /// found while the cache had not overflowed, so the caller can emit the
    /// `did not find subXID %u in MyProc` WARNING. proc.c owns the PGPROC/
    /// ProcGlobal subxid-cache layout; the caller holds `ProcArrayLock`
    /// exclusively.
    pub fn remove_running_subxids_from_proc(
        children: Vec<TransactionId>,
        xid: TransactionId,
    ) -> Vec<TransactionId>
);
