//! Implementations of the inward seams this unit owns
//! (`backend-storage-lmgr-proc-seams`): the `PGPROC`-field accessors the
//! LWLock / CV / latch / twophase machinery reads and writes, plus the small
//! proc.c helpers other units call (`ProcWaitForSignal`, `LockErrorCleanup`,
//! the `DeadlockTimeout`/`TransactionTimeout` GUCs, ...).
//!
//! The accessors of this crate's OWN state — `MyProc` / `GetPGProcByNumber(
//! procno)->field` over `ProcGlobal->allProcs`, built by `InitProcGlobal` /
//! `InitProcess` — are implemented here against `proc_shmem`'s owned storage.
//! The genuinely-foreign neighbours (latch, sysv_sema, the timeout GUCs, the
//! `LatchHandle` convention, twophase's dummy-PGPROC init) remain Class-B
//! panic-through until their owners land.

use backend_storage_lmgr_proc_seams as seams;
use types_core::{LocalTransactionId, Oid, ProcNumber, TimestampTz, TransactionId};
use types_error::PgResult;
use types_storage::latch::LatchHandle;
use types_storage::storage::{LWLockWaitState, ProcWaitStatus, DELAY_CHKPT_START};
use types_storage::{proclist_node, LWLockMode};

use crate::proc_shmem::{with_my_proc, with_my_proc_ref, with_proc_by_number};

// `lwWaiting`/`lwWaitMode` are stored as the raw `uint8` C fields; these map
// them to/from the typed seam values.
fn lw_wait_state_from_u8(v: u8) -> LWLockWaitState {
    match v {
        0 => LWLockWaitState::LW_WS_NOT_WAITING,
        1 => LWLockWaitState::LW_WS_WAITING,
        2 => LWLockWaitState::LW_WS_PENDING_WAKEUP,
        other => panic!("invalid LWLockWaitState byte {other}"),
    }
}

fn lw_wait_mode_from_u8(v: u8) -> LWLockMode {
    match v {
        0 => LWLockMode::LW_EXCLUSIVE,
        1 => LWLockMode::LW_SHARED,
        2 => LWLockMode::LW_WAIT_UNTIL_FREE,
        other => panic!("invalid LWLockMode byte {other}"),
    }
}

// ---- LWLock / CV wait-list fields on a PGPROC (lwlock.c / condition_variable.c
// read & write proc.c-owned PGPROC fields) ----

fn proc_lw_waiting(procno: ProcNumber) -> LWLockWaitState {
    with_proc_by_number(procno, |p| lw_wait_state_from_u8(p.lwWaiting))
}

fn set_proc_lw_waiting(procno: ProcNumber, state: LWLockWaitState) {
    with_proc_by_number(procno, |p| p.lwWaiting = state as u8);
}

fn proc_lw_wait_mode(procno: ProcNumber) -> LWLockMode {
    with_proc_by_number(procno, |p| lw_wait_mode_from_u8(p.lwWaitMode))
}

fn set_proc_lw_wait_mode(procno: ProcNumber, mode: LWLockMode) {
    with_proc_by_number(procno, |p| p.lwWaitMode = mode as u8);
}

fn proc_lw_wait_link(procno: ProcNumber) -> proclist_node {
    with_proc_by_number(procno, |p| p.lwWaitLink)
}

fn set_proc_lw_wait_link(procno: ProcNumber, node: proclist_node) {
    with_proc_by_number(procno, |p| p.lwWaitLink = node);
}

fn proc_cv_wait_link(procno: ProcNumber) -> proclist_node {
    with_proc_by_number(procno, |p| p.cvWaitLink)
}

fn set_proc_cv_wait_link(procno: ProcNumber, node: proclist_node) {
    with_proc_by_number(procno, |p| p.cvWaitLink = node);
}

// ---- latch / semaphore (foreign: latch.c / sysv_sema) ----

fn set_proc_latch(_procno: ProcNumber) {
    // `SetLatch(&GetPGProcByNumber(procno)->procLatch)` reaches the latch
    // embedded in the PGPROC. latch.c's `SetLatch` resolves a registry
    // `LatchHandle`, which does not yet know the PGPROC-embedded `procLatch`;
    // registering it is the latch <-> proc integration step, so this aborts
    // loudly until that bridge lands.
    panic!("SetLatch(&proc->procLatch): latch <-> proc PGPROC-latch bridge not yet wired")
}

fn pg_semaphore_lock(procno: ProcNumber) {
    backend_port_pg_sema_seams::pg_semaphore_lock::call(procno);
}

fn pg_semaphore_unlock(procno: ProcNumber) {
    backend_port_pg_sema_seams::pg_semaphore_unlock::call(procno);
}

fn proc_wait_for_signal(wait_event_info: u32) -> PgResult<()> {
    crate::proc_misc::ProcWaitForSignal(wait_event_info)
}

// ---- timeout GUCs (DeadlockTimeout/TransactionTimeout live in guc_tables.c) ----

fn deadlock_timeout() -> i32 {
    backend_utils_misc_guc_tables::vars::DeadlockTimeout.read()
}

fn transaction_timeout() -> i32 {
    backend_utils_misc_guc_tables::vars::TransactionTimeout.read()
}

// ---- MyProc scalar fields (own state) ----

fn my_proc_wait_start() -> TimestampTz {
    with_my_proc_ref(|p| p.waitStart.read() as TimestampTz)
}

fn set_my_proc_wait_start(value: TimestampTz) {
    with_my_proc(|p| p.waitStart.write(value as u64));
}

fn set_my_proc_vxid_proc_number(value: ProcNumber) {
    with_my_proc(|p| p.vxid.procNumber = value);
}

fn set_my_proc_temp_namespace_id(nspid: Oid) {
    with_my_proc(|p| p.tempNamespaceId = nspid);
}

fn my_proc_lxid() -> LocalTransactionId {
    with_my_proc_ref(|p| p.vxid.lxid)
}

fn set_my_proc_lxid(lxid: LocalTransactionId) {
    with_my_proc(|p| p.vxid.lxid = lxid);
}

fn lock_error_cleanup() {
    // C `LockErrorCleanup(void)` is void: it is the lock-wait unwind run on the
    // error path and does not itself raise. The port's `LockErrorCleanup`
    // returns `PgResult<()>` only to thread its callees' (timeout-disable, lock
    // dequeue) `ereport` surface; on this cleanup path that error is fatal —
    // surface it as a panic rather than silently swallow, matching the C
    // behaviour of those callees aborting the process.
    crate::proc_waitqueue::LockErrorCleanup().expect("LockErrorCleanup failed");
}

fn my_proc_set_delay_chkpt_start(on: bool) {
    set_delay_chkpt_start(on);
}

// ---- latch handle (own: a PGPROC slot's procLatch named by its proc number) ----

fn proc_latch(procno: ProcNumber) -> LatchHandle {
    // `&GetPGProcByNumber(procno)->procLatch` as a `LatchHandle`. The latch
    // unit identifies a per-PGPROC latch by the owning slot's proc number; the
    // slot identity is this unit's own state.
    crate::proc_shmem::proc_latch_handle(procno)
}

// ---- twophase dummy-PGPROC init ----
//
// `MarkAsPreparingGuts` / `GXactLoadSubxactData` (twophase.c) initialize the
// dummy `PGPROC` slot backing a prepared transaction. The `gxact`-side writes
// stay in twophase; the `proc->...` field initialization is over this unit's
// own PGPROC array, so it is realized here against `proc_shmem`'s storage.

fn proc_init_prepared(
    pgprocno: ProcNumber,
    xid: TransactionId,
    owner: Oid,
    databaseid: Oid,
) -> PgResult<()> {
    use types_storage::storage::{LWLockWaitState, PROC_WAIT_STATUS_OK};

    // Clone the caller's VXID when it has a valid lxid (so
    // TwoPhaseGetXidByVirtualXID() can find it); otherwise the caller is the
    // startup process / a standalone backend and we wait on the XID instead.
    let my_lxid = with_my_proc_ref(|p| p.vxid.lxid);
    let my_procnumber = crate::proc_shmem::my_proc_number();

    with_proc_by_number(pgprocno, |proc| {
        // MemSet(proc, 0, sizeof(PGPROC)); dlist_node_init(&proc->links).
        *proc = types_storage::storage::PGPROC::new_zeroed();
        proc.waitStatus = PROC_WAIT_STATUS_OK;

        if my_lxid != types_core::InvalidLocalTransactionId {
            proc.vxid.lxid = my_lxid;
            proc.vxid.procNumber = my_procnumber;
        } else {
            debug_assert!(
                crate::seam::am_startup_process()
                    || !backend_utils_init_small_seams::is_postmaster_environment::call()
            );
            proc.vxid.lxid = xid;
            proc.vxid.procNumber = types_core::INVALID_PROC_NUMBER;
        }

        proc.xid = xid;
        debug_assert_eq!(proc.xmin, types_core::InvalidTransactionId);
        proc.delayChkptFlags = 0;
        proc.statusFlags = 0;
        proc.pid = 0;
        proc.databaseId = databaseid;
        proc.roleId = owner;
        proc.tempNamespaceId = types_core::InvalidOid;
        proc.isRegularBackend = false;
        proc.lwWaiting = LWLockWaitState::LW_WS_NOT_WAITING as u8;
        proc.lwWaitMode = 0;
        proc.waitLock = None;
        proc.waitProcLock = None;
        proc.waitStart.write(0);
        // dlist_init(&proc->myProcLocks[i]) for each partition — PGPROC::default
        // already leaves each partition empty.
        proc.subxidStatus.overflowed = false;
        proc.subxidStatus.count = 0;
    });

    Ok(())
}

fn gxact_load_subxact_data(pgprocno: ProcNumber, children: &[TransactionId]) -> PgResult<()> {
    use types_storage::storage::PGPROC_MAX_CACHED_SUBXIDS;

    with_proc_by_number(pgprocno, |proc| {
        let mut nsubxacts = children.len();
        if nsubxacts > PGPROC_MAX_CACHED_SUBXIDS {
            proc.subxidStatus.overflowed = true;
            nsubxacts = PGPROC_MAX_CACHED_SUBXIDS;
        }
        if nsubxacts > 0 {
            proc.subxids.xids[..nsubxacts].copy_from_slice(&children[..nsubxacts]);
            proc.subxidStatus.count = nsubxacts as u8;
        }
    });

    Ok(())
}

// ---- MyProc / PGPROC field reads used by twophase & others (own state) ----

fn my_proc_number() -> ProcNumber {
    crate::proc_shmem::my_proc_number()
}

fn proc_database_id(pgprocno: ProcNumber) -> Oid {
    with_proc_by_number(pgprocno, |p| p.databaseId)
}

fn proc_xid(pgprocno: ProcNumber) -> TransactionId {
    with_proc_by_number(pgprocno, |p| p.xid)
}

fn proc_vxid(pgprocno: ProcNumber) -> (ProcNumber, u32) {
    with_proc_by_number(pgprocno, |p| (p.vxid.procNumber, p.vxid.lxid))
}

fn prepared_xact_procno(i: i32) -> ProcNumber {
    crate::proc_shmem::prepared_xact_procno(i)
}

fn set_delay_chkpt_start(on: bool) {
    with_my_proc(|p| {
        if on {
            p.delayChkptFlags |= DELAY_CHKPT_START;
        } else {
            p.delayChkptFlags &= !DELAY_CHKPT_START;
        }
    });
}

// --- wait-queue PGPROC accessors (proc_waitqueue family; own state) ---------

fn pgproc_number(proc: &types_storage::storage::PGPROC) -> ProcNumber {
    crate::proc_shmem::proc_number_of(proc)
}

fn proc_lock_group_leader(procno: ProcNumber) -> ProcNumber {
    // `GetPGProcByNumber(procno)->lockGroupLeader` as a ProcNumber. The
    // wait-queue seam contract returns INVALID_PROC_NUMBER for a NULL leader.
    with_proc_by_number(procno, |p| {
        p.lockGroupLeader.unwrap_or(types_core::INVALID_PROC_NUMBER)
    })
}

fn set_proc_held_locks(procno: ProcNumber, mask: types_storage::lock::LOCKMASK) {
    with_proc_by_number(procno, |p| p.heldLocks = mask);
}

fn proc_held_locks(procno: ProcNumber) -> types_storage::lock::LOCKMASK {
    with_proc_by_number(procno, |p| p.heldLocks)
}

fn proc_wait_lock_mode(procno: ProcNumber) -> types_storage::lock::LOCKMODE {
    with_proc_by_number(procno, |p| p.waitLockMode)
}

fn proc_wait_status(procno: ProcNumber) -> ProcWaitStatus {
    with_proc_by_number(procno, |p| p.waitStatus)
}

fn set_proc_wait_fields(
    procno: ProcNumber,
    lock: types_storage::lock::LOCKTAG,
    holder: ProcNumber,
    lockmode: types_storage::lock::LOCKMODE,
) {
    // `MyProc->{waitLock = lock; waitProcLock = proclock; waitLockMode =
    //  lockmode; waitStatus = PROC_WAIT_STATUS_WAITING;}` — waitLock/waitProcLock
    // modeled by the lock's LOCKTAG / the holder's ProcNumber (see PGPROC).
    with_proc_by_number(procno, |p| {
        p.waitLock = Some(lock);
        p.waitProcLock = Some(holder);
        p.waitLockMode = lockmode;
        p.waitStatus = types_storage::storage::PROC_WAIT_STATUS_WAITING;
    });
}

fn set_proc_wait_start(procno: ProcNumber, value: u64) {
    with_proc_by_number(procno, |p| p.waitStart.write(value));
}

fn proc_wait_link_is_detached(procno: ProcNumber) -> bool {
    // `dlist_node_is_detached(&GetPGProcByNumber(procno)->links)`: a node is
    // detached when both links are NULL (zero).
    with_proc_by_number(procno, |p| p.links.prev.is_none() && p.links.next.is_none())
}

fn wakeup_proc_clear_wait(procno: ProcNumber, status: ProcWaitStatus) {
    // ProcWakeup's state reset: clear waitLock/waitProcLock, set waitStatus, and
    // `pg_atomic_write_u64(&proc->waitStart, 0)`.
    with_proc_by_number(procno, |p| {
        p.waitLock = None;
        p.waitProcLock = None;
        p.waitStatus = status;
        p.waitStart.write(0);
    });
}

fn proc_unlinked_from_wait_queue(procno: ProcNumber) -> bool {
    // `MyProc->links.prev == NULL || MyProc->links.next == NULL`.
    with_proc_by_number(procno, |p| p.links.prev.is_none() || p.links.next.is_none())
}

fn proc_is_waiting_on_lock(procno: ProcNumber) -> bool {
    with_proc_by_number(procno, |p| p.waitLock.is_some())
}

fn proc_wait_lock_tag(procno: ProcNumber) -> types_storage::lock::LOCKTAG {
    // `MyProc->waitLock->tag` — the LOCKTAG identifying the awaited lock (the
    // value `waitLock` is modeled by). Panics if not waiting, mirroring the C
    // deref of a NULL waitLock.
    with_proc_by_number(procno, |p| {
        p.waitLock.expect("proc_wait_lock_tag: MyProc->waitLock is NULL")
    })
}

fn proc_pgxactoff(procno: ProcNumber) -> i32 {
    with_proc_by_number(procno, |p| p.pgxactoff)
}

fn proc_global_status_flags(pgxactoff: i32) -> u8 {
    crate::proc_shmem::status_flags(pgxactoff)
}

fn proc_pid(procno: ProcNumber) -> i32 {
    with_proc_by_number(procno, |p| p.pid)
}

// ---- lifecycle / wakeup inward seams (called by other units) ----

fn init_process() -> PgResult<()> {
    // `InitProcess(void)` runs in `TopMemoryContext`; it allocates nothing in
    // the passed `Mcx` (the parameter is unused), so a throwaway context
    // satisfies the explicit-Mcx threading without changing behaviour.
    let cx = mcx::MemoryContext::new("InitProcess");
    crate::proc_lifecycle::InitProcess(cx.mcx())
}

fn proc_lock_wakeup(_space: &mut types_deadlock::LockSpace, _lock: types_deadlock::LockId) {
    // The deadlock detector's `ProcLockWakeup(space, lock)` entry: re-wake the
    // grantable waiters on `lock` after a soft-deadlock queue rearrangement.
    // This view is over a lock.c-built `LockSpace` arena (lock/proc records),
    // which proc.c cannot construct until lock.c lands — so the binding is a
    // Class-B panic-through to that unported lock.c boundary, exactly like the
    // deadlock-checker seams. (proc.c's own `ProcLockWakeup`, which operates on
    // a `&mut LOCK` + the lock.c wait-queue seams, is the faithful body and is
    // reached the other way, from `ProcSleep`/`LockReleaseAll` once lock.c
    // supplies the LOCK.)
    //
    // This panic-through is the lock.c-integration boundary: lock.c, which owns
    // the `LockSpace` arena, supplies the `&mut LOCK` adapter that lets the
    // detector reach proc.c's real `ProcLockWakeup`. Until it lands, calling
    // this aborts loudly (matching a not-yet-wired seam) rather than silently
    // doing nothing.
    panic!("ProcLockWakeup over the lock.c-built LockSpace arena: lock.c not yet ported")
}

/// Install every inward seam this unit owns.
pub(crate) fn install() {
    seams::init_process::set(init_process);
    seams::proc_lock_wakeup::set(proc_lock_wakeup);
    seams::proc_lw_waiting::set(proc_lw_waiting);
    seams::set_proc_lw_waiting::set(set_proc_lw_waiting);
    seams::proc_lw_wait_mode::set(proc_lw_wait_mode);
    seams::set_proc_lw_wait_mode::set(set_proc_lw_wait_mode);
    seams::proc_lw_wait_link::set(proc_lw_wait_link);
    seams::set_proc_lw_wait_link::set(set_proc_lw_wait_link);
    seams::proc_cv_wait_link::set(proc_cv_wait_link);
    seams::set_proc_cv_wait_link::set(set_proc_cv_wait_link);
    seams::set_proc_latch::set(set_proc_latch);
    seams::pg_semaphore_lock::set(pg_semaphore_lock);
    seams::pg_semaphore_unlock::set(pg_semaphore_unlock);
    seams::proc_wait_for_signal::set(proc_wait_for_signal);
    seams::deadlock_timeout::set(deadlock_timeout);
    seams::my_proc_wait_start::set(my_proc_wait_start);
    seams::set_my_proc_wait_start::set(set_my_proc_wait_start);
    seams::set_my_proc_vxid_proc_number::set(set_my_proc_vxid_proc_number);
    seams::set_my_proc_temp_namespace_id::set(set_my_proc_temp_namespace_id);
    seams::my_proc_lxid::set(my_proc_lxid);
    seams::set_my_proc_lxid::set(set_my_proc_lxid);
    seams::transaction_timeout::set(transaction_timeout);
    seams::lock_error_cleanup::set(lock_error_cleanup);
    seams::my_proc_set_delay_chkpt_start::set(my_proc_set_delay_chkpt_start);
    seams::proc_latch::set(proc_latch);
    seams::proc_init_prepared::set(proc_init_prepared);
    seams::gxact_load_subxact_data::set(gxact_load_subxact_data);
    seams::my_proc_number::set(my_proc_number);
    seams::proc_database_id::set(proc_database_id);
    seams::proc_xid::set(proc_xid);
    seams::proc_vxid::set(proc_vxid);
    seams::prepared_xact_procno::set(prepared_xact_procno);
    seams::set_delay_chkpt_start::set(set_delay_chkpt_start);

    // wait-queue PGPROC accessors
    seams::pgproc_number::set(pgproc_number);
    seams::proc_lock_group_leader::set(proc_lock_group_leader);
    seams::set_proc_held_locks::set(set_proc_held_locks);
    seams::proc_held_locks::set(proc_held_locks);
    seams::proc_wait_lock_mode::set(proc_wait_lock_mode);
    seams::proc_wait_status::set(proc_wait_status);
    seams::set_proc_wait_fields::set(set_proc_wait_fields);
    seams::set_proc_wait_start::set(set_proc_wait_start);
    seams::proc_wait_link_is_detached::set(proc_wait_link_is_detached);
    seams::wakeup_proc_clear_wait::set(wakeup_proc_clear_wait);
    seams::proc_unlinked_from_wait_queue::set(proc_unlinked_from_wait_queue);
    seams::proc_is_waiting_on_lock::set(proc_is_waiting_on_lock);
    seams::proc_wait_lock_tag::set(proc_wait_lock_tag);
    seams::proc_pgxactoff::set(proc_pgxactoff);
    seams::proc_global_status_flags::set(proc_global_status_flags);
    seams::proc_pid::set(proc_pid);

    // Pure-wiring install (assemble/seam-wiring-guard): the deadlock-timeout
    // signal handler is an exact match for its declared seam and is installed
    // alongside the other inward seams (keeps proc out of init_all, matching
    // its existing convention). The remaining declared proc seams either
    // diverge (extra Mcx / out-param) or are mis-homed in miscadmin/globals
    // and are tracked in DESIGN_DEBT.
    seams::check_dead_lock_alert::set(crate::proc_waitqueue::CheckDeadLockAlert);
}
