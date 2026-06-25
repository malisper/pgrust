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

use lwlock_seams as lwlock;
use lmgr_proc_seams as seams;
use ::types_core::InvalidLocalTransactionId;
use ::types_core::xact::XidStatus;
use ::types_core::{LocalTransactionId, Oid, ProcNumber, TimestampTz, TransactionId, XLogRecPtr};
use ::types_error::PgResult;
use ::types_storage::latch::LatchHandle;
use ::types_storage::storage::{
    LWLockWaitState, ProcWaitStatus, VirtualTransactionId, DELAY_CHKPT_COMPLETE, DELAY_CHKPT_START,
    PGPROC_MAX_CACHED_SUBXIDS,
};
use ::types_storage::{proclist_node, LWLockMode};

use crate::proc_shmem::{with_my_proc, with_my_proc_ref, with_proc_by_number, with_proc_global};
use ::types_storage::lock::VirtualXactExamineOutcome;

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

// `lwWaiting`/`lwWaitMode`/`lwWaitLink` are genuinely shared (not the
// COW-inherited PGPROC): an LWLock release in one process walks the wait queue a
// waiter linked itself onto in another, reading+writing that waiter's wait
// state. See `proc_shmem::lw_*` and the cvWaitLink precedent.
fn proc_lw_waiting(procno: ProcNumber) -> LWLockWaitState {
    lw_wait_state_from_u8(crate::proc_shmem::lw_waiting_read(procno))
}

fn set_proc_lw_waiting(procno: ProcNumber, state: LWLockWaitState) {
    crate::proc_shmem::lw_waiting_write(procno, state as u8);
}

fn proc_lw_wait_mode(procno: ProcNumber) -> LWLockMode {
    lw_wait_mode_from_u8(crate::proc_shmem::lw_wait_mode_read(procno))
}

fn set_proc_lw_wait_mode(procno: ProcNumber, mode: LWLockMode) {
    crate::proc_shmem::lw_wait_mode_write(procno, mode as u8);
}

fn proc_lw_wait_link(procno: ProcNumber) -> proclist_node {
    crate::proc_shmem::lw_wait_link_read(procno)
}

fn set_proc_lw_wait_link(procno: ProcNumber, node: proclist_node) {
    crate::proc_shmem::lw_wait_link_write(procno, node);
}

fn proc_cv_wait_link(procno: ProcNumber) -> proclist_node {
    // Genuinely shared (not the COW-inherited PGPROC): a ConditionVariable
    // broadcast in one process walks the wait queue a waiter linked itself onto
    // in another. See `proc_shmem::cv_wait_link_read`.
    crate::proc_shmem::cv_wait_link_read(procno)
}

fn set_proc_cv_wait_link(procno: ProcNumber, node: proclist_node) {
    crate::proc_shmem::cv_wait_link_write(procno, node);
}

// ---- latch / semaphore (foreign: latch.c / sysv_sema) ----

fn set_proc_latch(procno: ProcNumber) {
    // `SetLatch(&GetPGProcByNumber(procno)->procLatch)`: name this slot's
    // embedded latch in the proc-tagged handle space and let latch.c's
    // `SetLatch` resolve it back through `with_proc_latch` to the real
    // `&proc->procLatch`.
    latch_seams::set_latch::call(
        crate::proc_shmem::proc_latch_handle(procno),
    );
}

fn with_proc_latch(procno: ProcNumber, f: &mut dyn FnMut(&::types_storage::latch::Latch)) {
    crate::proc_shmem::with_proc_latch(procno, f);
}

fn pg_semaphore_lock(procno: ProcNumber) {
    pg_sema_seams::pg_semaphore_lock::call(procno);
}

fn pg_semaphore_unlock(procno: ProcNumber) {
    pg_sema_seams::pg_semaphore_unlock::call(procno);
}

fn proc_wait_for_signal(wait_event_info: u32) -> PgResult<()> {
    crate::proc_misc::ProcWaitForSignal(wait_event_info)
}

// ---- timeout GUCs (DeadlockTimeout/TransactionTimeout live in guc_tables.c) ----

fn deadlock_timeout() -> i32 {
    guc_tables::vars::DeadlockTimeout.read()
}

fn transaction_timeout() -> i32 {
    guc_tables::vars::TransactionTimeout.read()
}

// ---- MyProc scalar fields (own state) ----

fn my_proc_wait_start() -> TimestampTz {
    // `pg_atomic_read_u64(&MyProc->waitStart)` — waitStart lives in the shared
    // PGPROC block (read cross-process by GetLockStatusData), so its canonical
    // store is the genuinely-shared word.
    crate::proc_shmem::proc_wait_start_shared(crate::proc_shmem::my_proc_number()) as TimestampTz
}

fn set_my_proc_wait_start(value: TimestampTz) {
    // `pg_atomic_write_u64(&MyProc->waitStart, value)` — write the canonical
    // (shared) word, mirroring into the fork-private PGPROC field for same-process
    // readers.
    let procno = crate::proc_shmem::my_proc_number();
    crate::proc_shmem::set_proc_wait_start_shared(procno, value as u64);
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
    // Mirror to the genuinely-shared per-proc vxid.lxid word so other backends'
    // GetLockConflicts/GetCurrentVirtualXIDs probes see this virtual transaction.
    crate::proc_shmem::set_proc_vxid_lxid_shared(crate::proc_shmem::my_proc_number(), lxid);
}

/// The fast-path VXID-lock critical section inside
/// `VirtualXactLockTableInsert(vxid)` (lock.c). Faithful to the C body:
/// ```c
/// LWLockAcquire(&MyProc->fpInfoLock, LW_EXCLUSIVE);
/// Assert(MyProc->vxid.procNumber == vxid.procNumber);
/// Assert(MyProc->fpLocalTransactionId == InvalidLocalTransactionId);
/// Assert(MyProc->fpVXIDLock == false);
/// MyProc->fpVXIDLock = true;
/// MyProc->fpLocalTransactionId = vxid.localTransactionId;
/// LWLockRelease(&MyProc->fpInfoLock);
/// ```
/// proc.c owns the critical section because `fpInfoLock` and the `fp*` fields
/// are `MyProc`-private PGPROC storage.
fn vxid_lock_table_insert_my_proc(
    vxid_proc_number: ProcNumber,
    lxid: LocalTransactionId,
) -> PgResult<()> {
    let my_procno = crate::proc_shmem::my_proc_number();
    with_my_proc(|p| {
        // LWLockAcquire(&MyProc->fpInfoLock, LW_EXCLUSIVE); the guard releases
        // the lock when dropped at the end of this closure (LWLockRelease).
        let _guard = lwlock::lwlock_acquire::call(
            &p.fpInfoLock,
            LWLockMode::LW_EXCLUSIVE,
            my_procno,
        )?;

        debug_assert_eq!(p.vxid.procNumber, vxid_proc_number);
        debug_assert_eq!(p.fpLocalTransactionId, InvalidLocalTransactionId);
        debug_assert!(!p.fpVXIDLock);

        p.fpVXIDLock = true;
        p.fpLocalTransactionId = lxid;
        // Mirror to the genuinely-shared per-proc words (under fpInfoLock) so a
        // VirtualXactLock waiter in another backend sees this proc holds the
        // fast-path VXID lock and migrates it to the primary lock table.
        crate::proc_shmem::set_proc_fp_vxid_lock_shared(my_procno, true);
        crate::proc_shmem::set_proc_fp_local_xid_shared(my_procno, lxid);
        Ok(())
    })
}

/// `VirtualXactLockTableCleanup()` shared-memory clear (lock.c:4613). Asserts
/// `MyProc->vxid.procNumber != INVALID_PROC_NUMBER`, then under
/// `MyProc->fpInfoLock` reads and clears the fast-path VXID state, returning the
/// prior `(fpVXIDLock, fpLocalTransactionId)` so the lock.c caller can decide
/// whether the lock was transferred to the main table.
fn vxid_lock_table_cleanup_my_proc() -> PgResult<(bool, LocalTransactionId)> {
    let my_procno = crate::proc_shmem::my_proc_number();
    with_my_proc(|p| {
        debug_assert_ne!(p.vxid.procNumber, ::types_core::INVALID_PROC_NUMBER);

        // LWLockAcquire(&MyProc->fpInfoLock, LW_EXCLUSIVE); released on drop.
        let _guard = lwlock::lwlock_acquire::call(
            &p.fpInfoLock,
            LWLockMode::LW_EXCLUSIVE,
            my_procno,
        )?;

        // Read the prior state from the genuinely-shared per-proc words, NOT the
        // fork-COW-private fields: a cross-process VirtualXactLock that migrated
        // this proc's fast-path VXID lock into the primary lock table cleared the
        // SHARED `fpVXIDLock` (its write is invisible to this backend's COW image).
        // Reading the COW `p.fpVXIDLock` here would see a stale `true`, skip the
        // `LockRefindAndRelease`, and leave the transferred lock held forever — the
        // detacher/CIC waiter never wakes. The shared words are authoritative.
        let fastpath = crate::proc_shmem::proc_fp_vxid_lock_shared(my_procno);
        let lxid = crate::proc_shmem::proc_fp_local_xid_shared(my_procno);
        p.fpVXIDLock = false;
        p.fpLocalTransactionId = InvalidLocalTransactionId;
        // Mirror both clears to the genuinely-shared per-proc words (under
        // fpInfoLock), exactly as C clears both fields.
        crate::proc_shmem::set_proc_fp_vxid_lock_shared(my_procno, false);
        crate::proc_shmem::set_proc_fp_local_xid_shared(my_procno, InvalidLocalTransactionId);

        Ok((fastpath, lxid))
    })
}

/// The cross-backend `&target->fpInfoLock`-guarded critical section of lock.c's
/// `VirtualXactLock(vxid, wait)`. proc.c owns the section because `fpInfoLock` /
/// `fpVXIDLock` / `fpLocalTransactionId` / `vxid` / `xid` are PGPROC-private
/// storage. Faithful to the C body's examine-and-(optionally)-transfer steps;
/// the lock-table transfer itself is lock.c's work, run via `transfer` while
/// `fpInfoLock` is held.
fn virtual_xact_examine_proc(
    target: ProcNumber,
    vxid: VirtualTransactionId,
    wait: bool,
    transfer: &mut dyn FnMut() -> PgResult<()>,
) -> PgResult<VirtualXactExamineOutcome> {
    let my_procno = crate::proc_shmem::my_proc_number();
    with_proc_by_number(target, |p| {
        // We must acquire this lock before checking the procNumber and lxid
        // against the ones we're waiting for. The target backend will only set
        // or clear lxid while holding this lock.
        let _guard = lwlock::lwlock_acquire::call(
            &p.fpInfoLock,
            LWLockMode::LW_EXCLUSIVE,
            my_procno,
        )?;

        // The vxid (procNumber+lxid), the fast-path VXID fields
        // (`fpLocalTransactionId`/`fpVXIDLock`) and the proc's `xid` are written by
        // the *target* backend, so we must read them from the genuinely-shared
        // per-proc words — the fork-COW-private image of another backend's slot is
        // stale (e.g. `vxid.procNumber == 0` for a slot the target claimed after
        // this backend forked), which spuriously trips the "VXID ended" guard and
        // skips the wait entirely.
        let target_vxid_procno = crate::proc_shmem::proc_vxid_procno_shared(target);
        let target_fp_local_xid = crate::proc_shmem::proc_fp_local_xid_shared(target);
        if target_vxid_procno != vxid.procNumber
            || target_fp_local_xid != vxid.localTransactionId
        {
            // VXID ended.
            return Ok(VirtualXactExamineOutcome::Ended);
        }

        // If we aren't asked to wait, there's no need to set up a lock table
        // entry. The transaction is still in progress, so just return false.
        if !wait {
            return Ok(VirtualXactExamineOutcome::StillRunningNoWait);
        }

        // OK, we're going to need to sleep on the VXID. But first, we must set
        // up the primary lock table entry, if needed (ie, convert the proc's
        // fast-path lock on its VXID to a regular lock).
        if crate::proc_shmem::proc_fp_vxid_lock_shared(target) {
            // `transfer` runs lock.c's SetupLockInTable + GrantLock under the
            // partition lock (nested inside fpInfoLock, exactly as in C). On
            // out-of-shared-memory it returns Err with fpInfoLock released by
            // the guard drop.
            transfer()?;
            p.fpVXIDLock = false;
            crate::proc_shmem::set_proc_fp_vxid_lock_shared(target, false);
        }

        // If the proc has an XID now, we'll avoid a TwoPhaseGetXidByVirtualXID()
        // search. The proc might have assigned this XID but not yet locked it,
        // in which case the proc will lock this XID before releasing the VXID.
        // The fpInfoLock critical section excludes VirtualXactLockTableCleanup(),
        // so we won't save an XID of a different VXID. Read the canonical shared
        // xid word (the target backend writes it cross-process).
        let xid = crate::proc_shmem::proc_xid_shared(target);

        Ok(VirtualXactExamineOutcome::Proceed { xid })
    })
}

/// `ProcNumberGetProc(procno) != NULL` (procarray.c) — whether the proc array
/// slot is a bounds-valid, in-use backend (`result->pid != 0`). Plain
/// shared-memory read over the owned `allProcs` arena.
fn proc_number_get_proc_is_present(procno: ProcNumber) -> bool {
    let all_proc_count = with_proc_global(|pg| pg.allProcCount as ProcNumber);
    if procno < 0 || procno >= all_proc_count {
        return false;
    }
    // `result->pid != 0`: read the genuinely-shared slot-occupancy word. The
    // fork-COW-private `PGPROC.pid` field is 0 for a proc owned by a *different*
    // backend (it joined after this backend forked), so reading it here made
    // VirtualXactLock conclude the target proc was absent and skip the wait
    // entirely (the DETACH/CIC/REINDEX cross-backend wait never blocked).
    crate::proc_shmem::shared_pid(procno) != 0
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

fn my_proc_latch() -> LatchHandle {
    // `&MyProc->procLatch` as a `LatchHandle`: the procLatch of this backend's
    // own PGPROC slot, named by `MyProcNumber`. Both the slot identity and the
    // handle-minting convention are this unit's own state.
    crate::proc_shmem::proc_latch_handle(crate::proc_shmem::my_proc_number())
}

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
    use ::types_storage::storage::{LWLockWaitState, PROC_WAIT_STATUS_OK};

    // Clone the caller's VXID when it has a valid lxid (so
    // TwoPhaseGetXidByVirtualXID() can find it); otherwise the caller is the
    // startup process / a standalone backend and we wait on the XID instead.
    let my_lxid = with_my_proc_ref(|p| p.vxid.lxid);
    let my_procnumber = crate::proc_shmem::my_proc_number();

    with_proc_by_number(pgprocno, |proc| {
        // MemSet(proc, 0, sizeof(PGPROC)); dlist_node_init(&proc->links).
        *proc = ::types_storage::storage::PGPROC::new_zeroed();
        proc.waitStatus = PROC_WAIT_STATUS_OK;

        if my_lxid != ::types_core::InvalidLocalTransactionId {
            proc.vxid.lxid = my_lxid;
            proc.vxid.procNumber = my_procnumber;
        } else {
            debug_assert!(
                crate::seam::am_startup_process()
                    || !init_small_seams::is_postmaster_environment::call()
            );
            proc.vxid.lxid = xid;
            proc.vxid.procNumber = ::types_core::INVALID_PROC_NUMBER;
        }

        proc.xid = xid;
        debug_assert_eq!(proc.xmin, ::types_core::InvalidTransactionId);
        proc.delayChkptFlags = 0;
        proc.statusFlags = 0;
        proc.pid = 0;
        proc.databaseId = databaseid;
        proc.roleId = owner;
        proc.tempNamespaceId = ::types_core::InvalidOid;
        proc.isRegularBackend = false;
        proc.lwWaiting = LWLockWaitState::LW_WS_NOT_WAITING as u8;
        proc.lwWaitMode = 0;
        // The live lwWaiting/lwWaitMode/lwWaitLink are the genuinely-shared cells
        // (the COW-local fields above are dead). Reset them too so a re-attached
        // slot never reports stale wait state. lwWaitLink is left as its
        // zero-init {0,0} ("not in any list").
        crate::proc_shmem::lw_waiting_write(pgprocno, LWLockWaitState::LW_WS_NOT_WAITING as u8);
        crate::proc_shmem::lw_wait_mode_write(pgprocno, 0);
        crate::proc_shmem::lw_wait_link_write(pgprocno, proclist_node { next: 0, prev: 0 });
        proc.waitLock = None;
        proc.waitProcLock = None;
        proc.waitStart.write(0);
        // dlist_init(&proc->myProcLocks[i]) for each partition — PGPROC::default
        // already leaves each partition empty.
        proc.subxidStatus.overflowed = false;
        proc.subxidStatus.count = 0;
    });

    // Mirror the canonical xmin/databaseId/statusFlags fields into the
    // genuinely-shared per-proc arrays (these PGPROC fields live in real shmem in
    // C; GetSnapshotData/ProcArrayInstallRestoredXmin read them cross-process).
    // The closure above set proc.xmin=Invalid (asserted), statusFlags=0,
    // databaseId=databaseid.
    crate::proc_shmem::set_proc_xmin_shared(pgprocno, ::types_core::InvalidTransactionId);
    crate::proc_shmem::set_proc_database_id_shared(pgprocno, databaseid);
    crate::proc_shmem::set_proc_status_flags_shared(pgprocno, 0);
    // Mirror the dummy proc's xid + vxid.lxid into the genuinely-shared per-proc
    // words so a cross-process VirtualXactLock/GetLockConflicts probe targeting
    // this prepared transaction sees its xid/vxid. The fast-path VXID fields are
    // zeroed (MemSet on the slot): a recovered/2PC dummy holds no fast-path VXID
    // lock (its localTransactionId is a normal, locked XID).
    let (dummy_lxid, dummy_procno) = if my_lxid != ::types_core::InvalidLocalTransactionId {
        (my_lxid, my_procnumber)
    } else {
        (xid, ::types_core::INVALID_PROC_NUMBER)
    };
    crate::proc_shmem::set_proc_vxid_lxid_shared(pgprocno, dummy_lxid);
    crate::proc_shmem::set_proc_vxid_procno_shared(pgprocno, dummy_procno);
    crate::proc_shmem::set_proc_xid_shared(pgprocno, xid);
    crate::proc_shmem::set_proc_fp_vxid_lock_shared(pgprocno, false);
    crate::proc_shmem::set_proc_fp_local_xid_shared(pgprocno, ::types_core::InvalidLocalTransactionId);

    Ok(())
}

fn gxact_load_subxact_data(pgprocno: ProcNumber, children: &[TransactionId]) -> PgResult<()> {
    use ::types_storage::storage::PGPROC_MAX_CACHED_SUBXIDS;

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
    // Read from the genuinely-shared databaseId array (cross-process): a parallel
    // worker's ProcArrayInstallRestoredXmin reads the leader's databaseId.
    crate::proc_shmem::proc_database_id_shared(pgprocno)
}

fn proc_xid(pgprocno: ProcNumber) -> TransactionId {
    // Read the canonical (shared) xid word: another backend's assigned xid is
    // invisible in the fork-COW-private PGPROC image.
    crate::proc_shmem::proc_xid_shared(pgprocno)
}

fn proc_vxid(pgprocno: ProcNumber) -> (ProcNumber, u32) {
    // Both halves are written by the *target* backend, so read them from the
    // genuinely-shared per-proc words. The fork-COW-private image of another
    // backend's slot holds the fork-time value (e.g. procNumber 0 / lxid 0),
    // which is wrong for a slot reused/initialized after this backend forked.
    let proc_number = crate::proc_shmem::proc_vxid_procno_shared(pgprocno);
    let lxid = crate::proc_shmem::proc_vxid_lxid_shared(pgprocno);
    (proc_number, lxid)
}

fn proc_xmin(pgprocno: ProcNumber) -> TransactionId {
    // Read from the genuinely-shared xmin array (cross-process): a parallel
    // worker's ProcArrayInstallRestoredXmin reads the leader's advertised xmin,
    // and GetSnapshotData scans every backend's xmin.
    crate::proc_shmem::proc_xmin_shared(pgprocno)
}

fn proc_role_id(pgprocno: ProcNumber) -> Oid {
    with_proc_by_number(pgprocno, |p| p.roleId)
}

fn proc_temp_namespace_id(pgprocno: ProcNumber) -> Oid {
    with_proc_by_number(pgprocno, |p| p.tempNamespaceId)
}

fn proc_all_proc_count() -> u32 {
    crate::proc_shmem::all_proc_count()
}

fn proc_subxids(procno: ProcNumber) -> (i32, Vec<TransactionId>) {
    // Mirror C's `GetRunningTransactionData`: it reads the subxid count from the
    // dense `ProcGlobal->subxidStates[index].count` array (the caller's `nsubxids`)
    // and `memcpy`s that many entries directly out of `proc->subxids.xids`, the
    // fixed 64-slot per-PGPROC array — it never re-reads `proc->subxids.count`,
    // and there is no `nxids >= nsubxids` assertion. The dense count and the
    // per-proc `subxidStatus.count` are updated at slightly different moments
    // under concurrency, so the per-proc count can lag the dense one; copying by
    // the dense count out of the always-valid fixed array is what C relies on.
    // Return the full fixed array (and the per-proc advertised count, retained
    // for callers that still want it) so the caller can copy exactly its own
    // `nsubxids` without tripping over a transient count skew.
    with_proc_by_number(procno, |p| {
        let count = p.subxidStatus.count as i32;
        (count, p.subxids.xids.to_vec())
    })
}

fn my_proc_xmin() -> TransactionId {
    // Read from the genuinely-shared xmin array (my own slot).
    crate::proc_shmem::proc_xmin_shared(crate::proc_shmem::my_proc_number())
}

fn set_my_proc_xmin(value: TransactionId) {
    // Advertise into the genuinely-shared xmin array (cross-process visible).
    crate::proc_shmem::set_proc_xmin_shared(crate::proc_shmem::my_proc_number(), value);
}

fn set_my_proc_status_flags(flags: u8) {
    // Write the genuinely-shared per-proc statusFlags array (cross-process visible).
    crate::proc_shmem::set_proc_status_flags_shared(crate::proc_shmem::my_proc_number(), flags);
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

fn set_delay_chkpt_complete(on: bool) {
    with_my_proc(|p| {
        if on {
            p.delayChkptFlags |= DELAY_CHKPT_COMPLETE;
        } else {
            p.delayChkptFlags &= !DELAY_CHKPT_COMPLETE;
        }
    });
}

// --- wait-queue PGPROC accessors (proc_waitqueue family; own state) ---------

fn pgproc_number(proc: &::types_storage::storage::PGPROC) -> ProcNumber {
    crate::proc_shmem::proc_number_of(proc)
}

fn proc_lock_group_leader(procno: ProcNumber) -> ProcNumber {
    // `GetPGProcByNumber(procno)->lockGroupLeader` as a ProcNumber. The
    // wait-queue seam contract returns INVALID_PROC_NUMBER for a NULL leader.
    // Read from the genuinely-shared lockGroupLeader array (cross-process).
    crate::proc_shmem::proc_lock_group_leader_shared(procno)
        .unwrap_or(::types_core::INVALID_PROC_NUMBER)
}

fn proc_lock_group_members(leader: ProcNumber) -> Vec<ProcNumber> {
    // `dlist_foreach(&GetPGProcByNumber(leader)->lockGroupMembers)` — the
    // members of `leader`'s lock group, in list order. Read from the
    // genuinely-shared lockGroupMembers list (the same store every backend's
    // BecomeLockGroupLeader/Member writes), so the deadlock detector's
    // LockSpace projection sees a parallel leader's worker members.
    crate::proc_lifecycle::lock_group_members_iter(leader)
}

fn set_proc_held_locks(procno: ProcNumber, mask: ::types_storage::lock::LOCKMASK) {
    // heldLocks is read cross-process by JoinWaitQueue's wait-queue walk, so the
    // canonical store is the genuinely-shared array (the fork-private PGPROC field
    // is kept in sync for any same-process reader).
    crate::proc_shmem::set_proc_held_locks_shared(procno, mask);
    with_proc_by_number(procno, |p| p.heldLocks = mask);
}

fn proc_held_locks(procno: ProcNumber) -> ::types_storage::lock::LOCKMASK {
    crate::proc_shmem::proc_held_locks_shared(procno)
}

fn proc_wait_lock_mode(procno: ProcNumber) -> ::types_storage::lock::LOCKMODE {
    // The backend releasing a conflicting lock reads this cross-process to decide
    // whether the waiter can be granted; read the canonical shared word.
    crate::proc_shmem::proc_wait_lock_mode_shared(procno)
}

fn proc_wait_status(procno: ProcNumber) -> ProcWaitStatus {
    // Written cross-process by the waker (ProcWakeup); read the canonical shared
    // word so the blocked backend observes the grant.
    crate::proc_shmem::proc_wait_status_shared(procno)
}

fn set_proc_wait_fields(
    procno: ProcNumber,
    lock: ::types_storage::lock::LOCKTAG,
    holder: ProcNumber,
    lockmode: ::types_storage::lock::LOCKMODE,
) {
    // `MyProc->{waitLock = lock; waitProcLock = proclock; waitLockMode =
    //  lockmode; waitStatus = PROC_WAIT_STATUS_WAITING;}` — waitLock/waitProcLock
    // modeled by the lock's LOCKTAG / the holder's ProcNumber (see PGPROC).
    // waitLock, waitLockMode and waitStatus are read cross-process by the waker,
    // so their canonical store is the genuinely-shared array (the fork-private
    // fields stay in sync for same-process readers). Stamp the awaited LOCKTAG
    // last so the "queued" flag is raised only once the mode/status are published.
    crate::proc_shmem::set_proc_wait_lock_mode_shared(procno, lockmode);
    crate::proc_shmem::set_proc_wait_status_shared(
        procno,
        ::types_storage::storage::PROC_WAIT_STATUS_WAITING,
    );
    crate::proc_shmem::set_proc_wait_lock_shared(procno, Some(lock));
    with_proc_by_number(procno, |p| {
        p.waitLock = Some(lock);
        p.waitProcLock = Some(holder);
        p.waitLockMode = lockmode;
        p.waitStatus = ::types_storage::storage::PROC_WAIT_STATUS_WAITING;
    });
}

fn set_proc_wait_start(procno: ProcNumber, value: u64) {
    // `pg_atomic_write_u64(&GetPGProcByNumber(procno)->waitStart, value)`. waitStart
    // is read cross-process by GetLockStatusData (pg_locks.waitstart), so its
    // canonical store is the genuinely-shared word; mirror the fork-private field.
    crate::proc_shmem::set_proc_wait_start_shared(procno, value);
    with_proc_by_number(procno, |p| p.waitStart.write(value));
}

fn proc_wait_start(procno: ProcNumber) -> TimestampTz {
    // `pg_atomic_read_u64(&GetPGProcByNumber(procno)->waitStart)` — read the
    // canonical (shared) word (GetLockStatusData reads other backends' waitStart).
    crate::proc_shmem::proc_wait_start_shared(procno) as TimestampTz
}

fn proc_wait_link_is_detached(procno: ProcNumber) -> bool {
    // `dlist_node_is_detached(&GetPGProcByNumber(procno)->links)`: in C the lock's
    // waitProcs dclist threads through `proc->links`, so "detached" == "not queued
    // on a lock". This port models queue membership in the shared lock table; the
    // cross-process source of truth is the shared waitLock/queued flag (the
    // fork-private `links` image is stale to a remote waker).
    crate::proc_shmem::proc_wait_lock_shared(procno).is_none()
}

fn wakeup_proc_clear_wait(procno: ProcNumber, status: ProcWaitStatus) {
    // ProcWakeup's state reset: clear waitLock/waitProcLock, set waitStatus, and
    // `pg_atomic_write_u64(&proc->waitStart, 0)`. waitLock and waitStatus are read
    // by the blocked backend (and other backends) cross-process, so clear/set the
    // canonical shared words so the grant/error and dequeue become visible and the
    // waiter exits its loop.
    crate::proc_shmem::set_proc_wait_status_shared(procno, status);
    crate::proc_shmem::set_proc_wait_lock_shared(procno, None);
    // waitStart is read cross-process (pg_locks.waitstart); clear the canonical
    // shared word so the dequeued backend no longer advertises a wait start.
    crate::proc_shmem::set_proc_wait_start_shared(procno, 0);
    with_proc_by_number(procno, |p| {
        p.waitLock = None;
        p.waitProcLock = None;
        p.waitStatus = status;
        p.waitStart.write(0);
    });
}

fn proc_unlinked_from_wait_queue(procno: ProcNumber) -> bool {
    // `MyProc->links.prev == NULL || MyProc->links.next == NULL` — i.e. no longer
    // queued. Read the cross-process shared queued flag (see
    // `proc_wait_link_is_detached`).
    crate::proc_shmem::proc_wait_lock_shared(procno).is_none()
}

fn proc_is_waiting_on_lock(procno: ProcNumber) -> bool {
    crate::proc_shmem::proc_wait_lock_shared(procno).is_some()
}

fn proc_wait_lock_tag(procno: ProcNumber) -> ::types_storage::lock::LOCKTAG {
    // `MyProc->waitLock->tag` — the LOCKTAG identifying the awaited lock. Read the
    // canonical shared word (cross-process visible). Panics if not waiting,
    // mirroring the C deref of a NULL waitLock.
    crate::proc_shmem::proc_wait_lock_shared(procno).unwrap_or_else(|| {
        with_proc_by_number(procno, |p| {
            p.waitLock.expect("proc_wait_lock_tag: MyProc->waitLock is NULL")
        })
    })
}

fn proc_pgxactoff(procno: ProcNumber) -> i32 {
    // `pgxactoff` is renumbered cross-process by ProcArrayAdd/Remove, so it lives
    // in the genuinely-shared PGPROC block — read the canonical shared word, not
    // the fork-private PGPROC field.
    crate::proc_shmem::proc_pgxactoff_shared(procno)
}

fn proc_global_status_flags(pgxactoff: i32) -> u8 {
    crate::proc_shmem::status_flags(pgxactoff)
}

fn proc_pid(procno: ProcNumber) -> i32 {
    // `GetPGProcByNumber(procno)->pid` is the genuinely-shared slot-occupancy
    // word, read cross-process by consumers that inspect OTHER backends' procs:
    // the lock-wait log holder/waiter lists, GetLockStatusData, the deadlock
    // detector's LockSpace projection (the "blocked by process N" detail), and
    // pg_stat_get_activity. The fork-COW-private `PGPROC.pid` field is 0 for a
    // proc owned by a different backend, so reading it here rendered cross-proc
    // PIDs as 0 (e.g. "blocked by process 0"). Read the canonical shared word.
    crate::proc_shmem::shared_pid(procno)
}

fn proc_wait_event_info(procno: ProcNumber) -> u32 {
    // `UINT32_ACCESS_ONCE(GetPGProcByNumber(procno)->wait_event_info)`
    // (pgstatfuncs.c pg_stat_get_activity). In C `my_wait_event_info` is repointed
    // at `&MyProc->wait_event_info` in the shared PGPROC block, so the live wait id
    // is published into shmem and read here cross-process. Reading the fork-private
    // PGPROC word rendered another backend's wait event as 0 (the heavyweight-lock
    // waiter looked idle to an isolationtester `(*)` blocker poll). Read the
    // canonical shared word; the volatile access-once is the atomic load.
    crate::proc_shmem::proc_wait_event_info_shared(procno)
}

fn set_proc_wait_event_info(procno: ProcNumber, info: u32) {
    // `*my_wait_event_info = info` where `my_wait_event_info ==
    // &GetPGProcByNumber(procno)->wait_event_info`. Publish the wait id into the
    // genuinely-shared word (visible to every backend's pg_stat_get_activity), and
    // mirror the fork-private PGPROC field for same-process readers.
    crate::proc_shmem::set_proc_wait_event_info_shared(procno, info);
    with_proc_by_number(procno, |p| p.wait_event_info = info);
}

fn proc_is_regular_backend(procno: ProcNumber) -> bool {
    with_proc_by_number(procno, |p| p.isRegularBackend)
}

// ---- sync-rep PGPROC fields (syncrep.c wait queue) ----
//
// The SyncRepQueue heads live in `WalSndCtl` (walsender); these are only the
// per-proc fields C touches: `syncRepState`, `waitLSN`, and the intrusive
// `syncRepLinks` (pgprocno-indexed `proclist_node`, exactly like `lwWaitLink`).

// These per-PGPROC fields are touched cross-process by the syncrep wait queue
// (a backend enqueues itself; a walsender in another process walks the queue,
// unlinks the waiter, and sets its state), so they live in a genuine shmem
// segment rather than the COW-private `allProcs` array — see
// `proc_shmem::sync_rep_links_read` for the hang this prevents.

fn my_proc_sync_rep_state() -> i32 {
    crate::proc_shmem::sync_rep_state_read(crate::proc_shmem::my_proc_number())
}

fn set_my_proc_sync_rep_state(state: i32) {
    crate::proc_shmem::sync_rep_state_write(crate::proc_shmem::my_proc_number(), state);
}

fn set_proc_sync_rep_state(procno: ProcNumber, state: i32) {
    crate::proc_shmem::sync_rep_state_write(procno, state);
}

fn my_proc_wait_lsn() -> XLogRecPtr {
    crate::proc_shmem::sync_rep_wait_lsn_read(crate::proc_shmem::my_proc_number())
}

fn set_my_proc_wait_lsn(lsn: XLogRecPtr) {
    crate::proc_shmem::sync_rep_wait_lsn_write(crate::proc_shmem::my_proc_number(), lsn);
}

fn proc_wait_lsn(procno: ProcNumber) -> XLogRecPtr {
    crate::proc_shmem::sync_rep_wait_lsn_read(procno)
}

fn proc_sync_rep_links(procno: ProcNumber) -> proclist_node {
    crate::proc_shmem::sync_rep_links_read(procno)
}

fn set_proc_sync_rep_links(procno: ProcNumber, node: proclist_node) {
    crate::proc_shmem::sync_rep_links_write(procno, node);
}

/// `XidCacheRemoveRunningXids`'s MyProc subxid-cache mutation (procarray.c).
/// Removes each of `children` (then `xid`) from `MyProc->subxids.xids[]` via the
/// C find-and-swap-with-last scan, decrementing both `MyProc->subxidStatus.count`
/// and the `ProcGlobal->subxidStates[pgxactoff]` mirror. Returns the xids that
/// were searched for but not found while the cache had not overflowed; the
/// caller emits the `did not find subXID %u in MyProc` WARNING for each.
fn remove_running_subxids_from_proc(
    children: Vec<TransactionId>,
    xid: TransactionId,
) -> Vec<TransactionId> {
    let mut not_found = Vec::new();

    // mysubxidstat = &ProcGlobal->subxidStates[MyProc->pgxactoff];
    let pgxactoff = crate::proc_shmem::my_proc_pgxactoff();

    with_my_proc(|proc| {
        // Scan children backwards (C: for i = nxids-1; i >= 0; i--).
        for &anxid in children.iter().rev() {
            let mut found = false;
            let mut j = proc.subxidStatus.count as i32 - 1;
            while j >= 0 {
                if proc.subxids.xids[j as usize] == anxid {
                    let last = proc.subxidStatus.count as usize - 1;
                    proc.subxids.xids[j as usize] = proc.subxids.xids[last];
                    // pg_write_barrier(); — write ordering is implicit here.
                    proc.subxidStatus.count -= 1;
                    found = true;
                    break;
                }
                j -= 1;
            }
            // Ordinarily found, unless the cache overflowed; mirror C's WARNING.
            if !found && !proc.subxidStatus.overflowed {
                not_found.push(anxid);
            }
        }

        // Then remove the parent xid itself.
        {
            let mut found = false;
            let mut j = proc.subxidStatus.count as i32 - 1;
            while j >= 0 {
                if proc.subxids.xids[j as usize] == xid {
                    let last = proc.subxidStatus.count as usize - 1;
                    proc.subxids.xids[j as usize] = proc.subxids.xids[last];
                    proc.subxidStatus.count -= 1;
                    found = true;
                    break;
                }
                j -= 1;
            }
            if !found && !proc.subxidStatus.overflowed {
                not_found.push(xid);
            }
        }
    });

    // Keep the ProcGlobal->subxidStates[pgxactoff].count mirror in sync with
    // MyProc->subxidStatus.count (mysubxidstat->count-- in C).
    let new_count = with_my_proc_ref(|p| p.subxidStatus.count as i32);
    let overflowed = with_my_proc_ref(|p| p.subxidStatus.overflowed);
    crate::proc_shmem::set_proc_array_subxid_state(pgxactoff, new_count, overflowed);

    not_found
}

// --- dense ProcGlobal array + PGPROC xact-field accessors (procarray.c) ------

fn proc_array_xid(idx: i32) -> TransactionId {
    crate::proc_shmem::proc_array_xid(idx)
}

fn set_proc_array_xid(idx: i32, xid: TransactionId) {
    crate::proc_shmem::set_proc_array_xid(idx, xid);
}

fn proc_array_subxid_state(idx: i32) -> (i32, bool) {
    crate::proc_shmem::proc_array_subxid_state(idx)
}

fn set_proc_array_subxid_state(idx: i32, count: i32, overflowed: bool) {
    crate::proc_shmem::set_proc_array_subxid_state(idx, count, overflowed);
}

fn set_proc_array_status_flags(idx: i32, flags: u8) {
    crate::proc_shmem::set_proc_array_status_flags(idx, flags);
}

fn proc_array_xids_memmove(dst: i32, src: i32, count: i32) {
    crate::proc_shmem::proc_array_xids_memmove(dst, src, count);
}

fn proc_array_subxid_states_memmove(dst: i32, src: i32, count: i32) {
    crate::proc_shmem::proc_array_subxid_states_memmove(dst, src, count);
}

// --- MyProc xact-snapshot field accessors (clog group-update eligibility,
//     snapmgr exported-snapshot labelling, varsup xid/subxid publication) ------

/// `MyProc->xid` — this backend's top-level xid.
fn my_proc_xid() -> TransactionId {
    with_my_proc_ref(|p| p.xid)
}

/// `MyProc->vxid` (`{procNumber, lxid}`) as a `VirtualTransactionId`. C keeps
/// the pair as two separately-assignable fields; the read is non-atomic but
/// snapmgr only reads it inside its own transaction, so the pair is stable.
fn my_proc_vxid() -> VirtualTransactionId {
    with_my_proc_ref(|p| VirtualTransactionId {
        procNumber: p.vxid.procNumber,
        localTransactionId: p.vxid.lxid,
    })
}

/// `(MyProc->subxidStatus.count, MyProc->subxids.xids[0..count])` — this
/// backend's cached subxids (clog group-update eligibility compares these).
fn my_proc_subxids() -> (i32, Vec<TransactionId>) {
    with_my_proc_ref(|p| {
        let count = p.subxidStatus.count as i32;
        (count, p.subxids.xids[..count as usize].to_vec())
    })
}

/// `GetNewTransactionId`'s non-subxact leg (varsup.c): publish a freshly
/// allocated top-level `xid` into `MyProc->xid` and the dense
/// `ProcGlobal->xids[MyProc->pgxactoff]` mirror while `XidGenLock` is held (its
/// release acts as the write barrier). Mirrors the C `Assert`s that the subxid
/// cache is empty before a fresh top-level xid is stored.
fn store_top_xid_in_proc(xid: TransactionId) {
    with_my_proc(|p| {
        debug_assert_eq!(p.subxidStatus.count, 0);
        debug_assert!(!p.subxidStatus.overflowed);
        p.xid = xid;
    });
    let pgxactoff = crate::proc_shmem::my_proc_pgxactoff();
    crate::proc_shmem::set_proc_array_xid(pgxactoff, xid);
    // Mirror to the genuinely-shared per-proc xid word so a cross-process
    // VirtualXactLock examiner sees this backend's assigned xid (it short-circuits
    // the 2PC-by-vxid search and waits on the xid lock too).
    crate::proc_shmem::set_proc_xid_shared(crate::proc_shmem::my_proc_number(), xid);
}

/// `GetNewTransactionId`'s subxact leg (varsup.c): push a freshly allocated
/// subtransaction `xid` into `MyProc->subxids.xids[]` and bump
/// `subxidStatus.count` (plus the dense `ProcGlobal->subxidStates[pgxactoff]`
/// mirror), or set the `overflowed` flag once `PGPROC_MAX_CACHED_SUBXIDS` is
/// exceeded. The `pg_write_barrier()` between the slot store and the count bump
/// is implicit here (single-threaded model under `XidGenLock`).
fn store_subxid_in_proc(xid: TransactionId) {
    let pgxactoff = crate::proc_shmem::my_proc_pgxactoff();
    let nxids = with_my_proc_ref(|p| p.subxidStatus.count as i32);

    if (nxids as usize) < PGPROC_MAX_CACHED_SUBXIDS {
        with_my_proc(|p| {
            p.subxids.xids[nxids as usize] = xid;
            // pg_write_barrier()
            p.subxidStatus.count = (nxids + 1) as u8;
        });
        crate::proc_shmem::set_proc_array_subxid_state(pgxactoff, nxids + 1, false);
    } else {
        with_my_proc(|p| p.subxidStatus.overflowed = true);
        crate::proc_shmem::set_proc_array_subxid_state(pgxactoff, nxids, true);
    }
}

fn proc_array_status_flags_memmove(dst: i32, src: i32, count: i32) {
    crate::proc_shmem::proc_array_status_flags_memmove(dst, src, count);
}

fn proc_subxid_status(procno: ProcNumber) -> (i32, bool) {
    with_proc_by_number(procno, |p| (p.subxidStatus.count as i32, p.subxidStatus.overflowed))
}

fn set_proc_subxid_status(procno: ProcNumber, count: i32, overflowed: bool) {
    with_proc_by_number(procno, |p| {
        p.subxidStatus.count = count as u8;
        p.subxidStatus.overflowed = overflowed;
    });
}

fn proc_status_flags(procno: ProcNumber) -> u8 {
    // Read from the genuinely-shared per-proc statusFlags array (cross-process).
    crate::proc_shmem::proc_status_flags_shared(procno)
}

fn set_proc_status_flags(procno: ProcNumber, flags: u8) {
    // Write the genuinely-shared per-proc statusFlags array (cross-process).
    crate::proc_shmem::set_proc_status_flags_shared(procno, flags);
}

fn set_proc_xid(procno: ProcNumber, xid: TransactionId) {
    with_proc_by_number(procno, |p| p.xid = xid);
    // Mirror to the genuinely-shared per-proc xid word (ProcArrayEndTransaction
    // clears it cross-process; a VirtualXactLock examiner reads it).
    crate::proc_shmem::set_proc_xid_shared(procno, xid);
}

fn set_proc_xmin(procno: ProcNumber, xmin: TransactionId) {
    // Write the genuinely-shared xmin array (cross-process visible).
    crate::proc_shmem::set_proc_xmin_shared(procno, xmin);
}

fn set_proc_lxid(procno: ProcNumber, lxid: LocalTransactionId) {
    with_proc_by_number(procno, |p| p.vxid.lxid = lxid);
    // Mirror to the genuinely-shared per-proc vxid.lxid word (ProcArrayEnd-
    // Transaction clears it cross-process; GetLockConflicts/GetCurrentVirtualXIDs
    // read it).
    crate::proc_shmem::set_proc_vxid_lxid_shared(procno, lxid);
}

fn proc_delay_chkpt_flags(procno: ProcNumber) -> i32 {
    with_proc_by_number(procno, |p| p.delayChkptFlags)
}

fn set_proc_delay_chkpt_flags(procno: ProcNumber, flags: i32) {
    with_proc_by_number(procno, |p| p.delayChkptFlags = flags);
}

fn set_proc_recovery_conflict_pending(procno: ProcNumber, value: bool) {
    with_proc_by_number(procno, |p| p.recoveryConflictPending = value);
}

fn set_proc_pgxactoff(procno: ProcNumber, off: i32) {
    // Write the canonical shared word so the ProcArrayAdd/Remove renumber is
    // visible to every process (mirrors C's allProcs[procno].pgxactoff in the
    // shared PGPROC block).
    crate::proc_shmem::set_proc_pgxactoff_shared(procno, off);
}

// --- ProcArray group-clear atomics + per-PGPROC group fields ----------------

fn set_proc_array_group_member_data(procno: ProcNumber, member: bool, xid: TransactionId) {
    with_proc_by_number(procno, |p| {
        p.procArrayGroupMember = member;
        p.procArrayGroupMemberXid = xid;
    });
}

fn proc_array_group_member(procno: ProcNumber) -> bool {
    with_proc_by_number(procno, |p| p.procArrayGroupMember)
}

fn set_proc_array_group_member(procno: ProcNumber, value: bool) {
    with_proc_by_number(procno, |p| p.procArrayGroupMember = value);
}

fn proc_array_group_member_xid(procno: ProcNumber) -> TransactionId {
    with_proc_by_number(procno, |p| p.procArrayGroupMemberXid)
}

fn proc_array_group_next(procno: ProcNumber) -> u32 {
    with_proc_by_number(procno, |p| p.procArrayGroupNext.read())
}

fn set_proc_array_group_next(procno: ProcNumber, value: u32) {
    with_proc_by_number(procno, |p| {
        p.procArrayGroupNext
            .value
            .store(value, core::sync::atomic::Ordering::SeqCst)
    });
}

fn proc_array_group_first_read() -> u32 {
    crate::proc_shmem::proc_array_group_first_read()
}

fn proc_array_group_first_compare_exchange(expected: u32, newval: u32) -> (bool, u32) {
    crate::proc_shmem::proc_array_group_first_compare_exchange(expected, newval)
}

fn proc_array_group_first_exchange(newval: u32) -> u32 {
    crate::proc_shmem::proc_array_group_first_exchange(newval)
}

fn proc_is_my_proc(procno: ProcNumber) -> bool {
    crate::proc_shmem::my_proc_is_set() && crate::proc_shmem::my_proc_number() == procno
}

// --- clog.c group XID-status update: ProcGlobal->clogGroupFirst atomic + the
// per-PGPROC clogGroup{Member,Next,MemberXid,MemberXidStatus,MemberPage,
// MemberLsn} fields (clog.c `TransactionGroupUpdateXidStatus`). Mirrors the
// ProcArray group-clear set above. ---

fn set_my_proc_clog_group_member_data(
    xid: TransactionId,
    status: XidStatus,
    pageno: i64,
    lsn: XLogRecPtr,
) {
    with_my_proc(|p| {
        p.clogGroupMember = true;
        p.clogGroupMemberXid = xid;
        p.clogGroupMemberXidStatus = status;
        p.clogGroupMemberPage = pageno;
        p.clogGroupMemberLsn = lsn;
    });
}

fn my_proc_clog_group_member() -> bool {
    with_my_proc_ref(|p| p.clogGroupMember)
}

fn set_my_proc_clog_group_member(value: bool) {
    with_my_proc(|p| p.clogGroupMember = value);
}

fn set_proc_clog_group_member(procno: ProcNumber, value: bool) {
    with_proc_by_number(procno, |p| p.clogGroupMember = value);
}

fn proc_clog_group_member_page(procno: ProcNumber) -> i64 {
    with_proc_by_number(procno, |p| p.clogGroupMemberPage)
}

fn proc_clog_group_member_update(procno: ProcNumber) -> (TransactionId, XidStatus, XLogRecPtr) {
    with_proc_by_number(procno, |p| {
        (
            p.clogGroupMemberXid,
            p.clogGroupMemberXidStatus,
            p.clogGroupMemberLsn,
        )
    })
}

fn my_proc_clog_group_next() -> u32 {
    with_my_proc_ref(|p| p.clogGroupNext.read())
}

fn set_my_proc_clog_group_next(value: u32) {
    with_my_proc(|p| {
        p.clogGroupNext
            .value
            .store(value, core::sync::atomic::Ordering::SeqCst)
    });
}

fn proc_clog_group_next(procno: ProcNumber) -> u32 {
    with_proc_by_number(procno, |p| p.clogGroupNext.read())
}

fn set_proc_clog_group_next(procno: ProcNumber, value: u32) {
    with_proc_by_number(procno, |p| {
        p.clogGroupNext
            .value
            .store(value, core::sync::atomic::Ordering::SeqCst)
    });
}

fn clog_group_first_read() -> u32 {
    crate::proc_shmem::clog_group_first_read()
}

fn clog_group_first_compare_exchange(expected: u32, newval: u32) -> (bool, u32) {
    crate::proc_shmem::clog_group_first_compare_exchange(expected, newval)
}

fn clog_group_first_exchange(newval: u32) -> u32 {
    crate::proc_shmem::clog_group_first_exchange(newval)
}

// ---- lifecycle / wakeup inward seams (called by other units) ----

fn init_process() -> PgResult<()> {
    // `InitProcess(void)` runs in `TopMemoryContext`; it allocates nothing in
    // the passed `Mcx` (the parameter is unused), so a throwaway context
    // satisfies the explicit-Mcx threading without changing behaviour.
    let cx = mcx::MemoryContext::new("InitProcess");
    crate::proc_lifecycle::InitProcess(cx.mcx())
}

fn init_process_phase2(mcx: mcx::Mcx<'_>) -> PgResult<()> {
    // `InitProcessPhase2(void)` — add MyProc to the ProcArray. The owner body
    // takes the explicit `Mcx` (it allocates nothing in it; the parameter
    // threads the memory-context convention).
    crate::proc_lifecycle::InitProcessPhase2(mcx)
}

fn have_n_free_procs(n: i32, nfree: &mut i32) -> bool {
    // `HaveNFreeProcs(int n, int *nfree)` — exact C signature; the owner body
    // counts `ProcGlobal->freeProcs` under the ProcStructLock.
    crate::proc_lifecycle::HaveNFreeProcs(n, nfree)
}

fn set_my_proc_role_id(userid: Oid) {
    // `MyProc->roleId = userid` — plain shared-memory field store.
    with_my_proc(|p| p.roleId = userid);
}

fn set_my_proc_database_id(dboid: Oid) {
    // `MyProc->databaseId = dboid` — write the genuinely-shared databaseId array
    // (cross-process visible; ProcArrayInstallRestoredXmin reads the leader's).
    crate::proc_shmem::set_proc_database_id_shared(crate::proc_shmem::my_proc_number(), dboid);
}

fn proc_lock_wakeup(space: &mut types_deadlock::LockSpace, lock: types_deadlock::LockId) {
    // The deadlock detector's `ProcLockWakeup(space, lock)` entry: after a
    // soft-deadlock queue rearrangement, write the detector's new wait order for
    // `lock` back into the live shared wait queue and re-wake the now-grantable
    // waiters. The detector has already rewritten `space.lock(lock).wait_procs`
    // (in the projected arena) to the resolved order; here we map it back to the
    // real shmem queue (ProcId(n) == ProcNumber(n) by the projection's identity
    // convention) via lock.c's `apply_soft_deadlock_wait_order`, which calls
    // proc.c's faithful `ProcLockWakeup(&mut LOCK)` body after re-threading.
    let tag = space.lock(lock).tag;
    let new_order: Vec<ProcNumber> = space
        .lock(lock)
        .wait_procs
        .iter()
        .map(|p| p.0 as ProcNumber)
        .collect();
    lock_seams::apply_soft_deadlock_wait_order::call(tag, new_order);
}

/// `ProcGlobal->checkpointerProc` — the checkpointer's advertised proc number
/// (`INVALID_PROC_NUMBER` while not running). Read by `RequestCheckpoint` /
/// `ForwardSyncRequest` to wake the checkpointer.
fn checkpointer_proc() -> ProcNumber {
    // Genuinely shared (not the COW-inherited PROC_GLOBAL value): the
    // checkpointer advertises itself in its own process after fork, so a
    // process-local copy would never reach this reader (e.g. the startup
    // process' end-of-recovery RequestCheckpoint). See `proc_shmem`.
    crate::proc_shmem::checkpointer_proc_read()
}

/// `ProcGlobal->checkpointerProc = MyProcNumber` — the checkpointer advertises
/// its own proc number at startup.
fn set_checkpointer_proc_to_self() -> PgResult<()> {
    let me = crate::proc_shmem::my_proc_number();
    crate::proc_shmem::checkpointer_proc_write(me);
    Ok(())
}

/// `ProcGlobal->walwriterProc = MyProcNumber` — the walwriter advertises its
/// own proc number at startup so backends can wake it while it sleeps.
fn set_walwriter_proc_to_self() -> PgResult<()> {
    let me = crate::proc_shmem::my_proc_number();
    crate::proc_shmem::walwriter_proc_write(me);
    Ok(())
}

/// Install every inward seam this unit owns.
pub(crate) fn install() {
    seams::checkpointer_proc::set(checkpointer_proc);
    seams::set_checkpointer_proc_to_self::set(set_checkpointer_proc_to_self);
    seams::set_walwriter_proc_to_self::set(set_walwriter_proc_to_self);
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
    seams::with_proc_latch::set(with_proc_latch);
    seams::pg_semaphore_lock::set(pg_semaphore_lock);
    seams::pg_semaphore_unlock::set(pg_semaphore_unlock);
    seams::proc_wait_for_signal::set(proc_wait_for_signal);
    seams::deadlock_timeout::set(deadlock_timeout);
    seams::my_proc_wait_start::set(my_proc_wait_start);
    seams::set_my_proc_wait_start::set(set_my_proc_wait_start);
    seams::set_my_proc_vxid_proc_number::set(set_my_proc_vxid_proc_number);
    seams::vxid_lock_table_insert_my_proc::set(vxid_lock_table_insert_my_proc);
    seams::vxid_lock_table_cleanup_my_proc::set(vxid_lock_table_cleanup_my_proc);
    seams::virtual_xact_examine_proc::set(virtual_xact_examine_proc);
    seams::proc_number_get_proc_is_present::set(proc_number_get_proc_is_present);
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
    seams::proc_xmin::set(proc_xmin);
    seams::proc_role_id::set(proc_role_id);
    seams::auxiliary_pid_get_proc::set(crate::proc_lifecycle::AuxiliaryPidGetProc);
    seams::proc_temp_namespace_id::set(proc_temp_namespace_id);
    seams::proc_all_proc_count::set(proc_all_proc_count);
    seams::proc_subxids::set(proc_subxids);
    seams::my_proc_xmin::set(my_proc_xmin);
    seams::set_my_proc_xmin::set(set_my_proc_xmin);
    seams::my_proc_xid::set(my_proc_xid);
    seams::my_proc_vxid::set(my_proc_vxid);
    seams::my_proc_subxids::set(my_proc_subxids);
    seams::store_top_xid_in_proc::set(store_top_xid_in_proc);
    seams::store_subxid_in_proc::set(store_subxid_in_proc);
    seams::set_my_proc_status_flags::set(set_my_proc_status_flags);
    seams::set_indexsafe_procflags::set(crate::proc_misc::set_indexsafe_procflags);
    seams::prepared_xact_procno::set(prepared_xact_procno);
    seams::set_delay_chkpt_start::set(set_delay_chkpt_start);
    seams::set_delay_chkpt_complete::set(set_delay_chkpt_complete);

    // wait-queue PGPROC accessors
    seams::pgproc_number::set(pgproc_number);
    seams::proc_lock_group_leader::set(proc_lock_group_leader);
    seams::proc_lock_group_members::set(proc_lock_group_members);
    seams::set_proc_held_locks::set(set_proc_held_locks);
    seams::proc_held_locks::set(proc_held_locks);
    seams::proc_wait_lock_mode::set(proc_wait_lock_mode);
    seams::proc_wait_status::set(proc_wait_status);
    seams::set_proc_wait_fields::set(set_proc_wait_fields);
    seams::set_proc_wait_start::set(set_proc_wait_start);
    seams::proc_wait_start::set(proc_wait_start);
    seams::proc_wait_link_is_detached::set(proc_wait_link_is_detached);
    seams::wakeup_proc_clear_wait::set(wakeup_proc_clear_wait);
    seams::proc_unlinked_from_wait_queue::set(proc_unlinked_from_wait_queue);
    seams::proc_is_waiting_on_lock::set(proc_is_waiting_on_lock);
    seams::proc_wait_lock_tag::set(proc_wait_lock_tag);
    seams::proc_pgxactoff::set(proc_pgxactoff);
    seams::proc_global_status_flags::set(proc_global_status_flags);
    seams::proc_pid::set(proc_pid);
    seams::proc_wait_event_info::set(proc_wait_event_info);
    seams::set_proc_wait_event_info::set(set_proc_wait_event_info);
    // `GetPGProcByNumber(owner)->pid` (storage/proc.h), the AIO `pg_get_aios()`
    // SRF's owner-pid lookup; same PGPROC->pid mapping as `proc_pid`.
    funcs_seams::proc_pid_by_number::set(|owner| Ok(proc_pid(owner)));
    seams::proc_is_regular_backend::set(proc_is_regular_backend);
    seams::remove_running_subxids_from_proc::set(remove_running_subxids_from_proc);

    // dense ProcGlobal array + PGPROC xact-field accessors (procarray.c
    // membership family)
    seams::proc_array_xid::set(proc_array_xid);
    seams::set_proc_array_xid::set(set_proc_array_xid);
    seams::proc_array_subxid_state::set(proc_array_subxid_state);
    seams::set_proc_array_subxid_state::set(set_proc_array_subxid_state);
    seams::set_proc_array_status_flags::set(set_proc_array_status_flags);
    seams::proc_array_xids_memmove::set(proc_array_xids_memmove);
    seams::proc_array_subxid_states_memmove::set(proc_array_subxid_states_memmove);
    seams::proc_array_status_flags_memmove::set(proc_array_status_flags_memmove);
    seams::proc_subxid_status::set(proc_subxid_status);
    seams::set_proc_subxid_status::set(set_proc_subxid_status);
    seams::proc_status_flags::set(proc_status_flags);
    seams::set_proc_status_flags::set(set_proc_status_flags);
    seams::set_proc_xid::set(set_proc_xid);
    seams::set_proc_xmin::set(set_proc_xmin);
    seams::set_proc_lxid::set(set_proc_lxid);
    seams::proc_delay_chkpt_flags::set(proc_delay_chkpt_flags);
    seams::set_proc_delay_chkpt_flags::set(set_proc_delay_chkpt_flags);
    seams::set_proc_recovery_conflict_pending::set(set_proc_recovery_conflict_pending);
    seams::set_proc_pgxactoff::set(set_proc_pgxactoff);

    // ProcArray group-clear atomics + per-PGPROC group fields
    seams::set_proc_array_group_member_data::set(set_proc_array_group_member_data);
    seams::proc_array_group_member::set(proc_array_group_member);
    seams::set_proc_array_group_member::set(set_proc_array_group_member);
    seams::proc_array_group_member_xid::set(proc_array_group_member_xid);
    seams::proc_array_group_next::set(proc_array_group_next);
    seams::set_proc_array_group_next::set(set_proc_array_group_next);
    seams::proc_array_group_first_read::set(proc_array_group_first_read);
    seams::proc_array_group_first_compare_exchange::set(proc_array_group_first_compare_exchange);
    seams::proc_array_group_first_exchange::set(proc_array_group_first_exchange);
    seams::proc_is_my_proc::set(proc_is_my_proc);

    // clog.c group XID-status update atomics + per-PGPROC clog-group fields
    seams::set_my_proc_clog_group_member_data::set(set_my_proc_clog_group_member_data);
    seams::my_proc_clog_group_member::set(my_proc_clog_group_member);
    seams::set_my_proc_clog_group_member::set(set_my_proc_clog_group_member);
    seams::set_proc_clog_group_member::set(set_proc_clog_group_member);
    seams::proc_clog_group_member_page::set(proc_clog_group_member_page);
    seams::proc_clog_group_member_update::set(proc_clog_group_member_update);
    seams::my_proc_clog_group_next::set(my_proc_clog_group_next);
    seams::set_my_proc_clog_group_next::set(set_my_proc_clog_group_next);
    seams::proc_clog_group_next::set(proc_clog_group_next);
    seams::set_proc_clog_group_next::set(set_proc_clog_group_next);
    seams::clog_group_first_read::set(clog_group_first_read);
    seams::clog_group_first_compare_exchange::set(clog_group_first_compare_exchange);
    seams::clog_group_first_exchange::set(clog_group_first_exchange);

    // Pure-wiring install (assemble/seam-wiring-guard): the deadlock-timeout
    // signal handler is an exact match for its declared seam and is installed
    // alongside the other inward seams (keeps proc out of init_all, matching
    // its existing convention). The remaining declared proc seams either
    // diverge (extra Mcx / out-param) or are mis-homed in miscadmin/globals
    // and are tracked in DESIGN_DEBT.
    seams::check_dead_lock_alert::set(crate::proc_waitqueue::CheckDeadLockAlert);

    // Contract-reconciled installs (assemble/seam-contract-reconciles): the
    // postinit / miscinit lifecycle seams over this unit's own `MyProc` state.
    // `init_process_phase2` threads the owner's `Mcx`; `have_n_free_procs`
    // matches the C `(int, int *) -> bool` out-param shape; the role/database
    // id setters are plain `MyProc` field stores.
    seams::init_process_phase2::set(init_process_phase2);
    seams::have_n_free_procs::set(have_n_free_procs);
    seams::set_my_proc_role_id::set(set_my_proc_role_id);
    seams::set_my_proc_database_id::set(set_my_proc_database_id);

    // Contract-reconciled installs over this unit's own state: the
    // `AmRegularBackendProcess()` predicate (the owner's `crate::seam` body,
    // exact `() -> bool`) and `&MyProc->procLatch` read-side handle mint
    // (`() -> LatchHandle` over this unit's PGPROC slot).
    seams::am_regular_backend_process::set(crate::seam::am_regular_backend_process);
    seams::my_proc_latch::set(my_proc_latch);

    // PGPROC-array shmem sizing + one-time setup (proc.c). The bodies live in
    // `proc_shmem` and depend only on this unit's own state plus already-merged
    // globals/lwlock seams — no procarray/clog group-update wiring is needed
    // for them, so they install for real here (retires their
    // CONTRACT_RECONCILE_PENDING lines). ProcGlobalShmemSize's local add_size
    // does not overflow-check, so the `PgResult` is always `Ok`.
    seams::proc_global_semas::set(crate::proc_shmem::ProcGlobalSemas);
    seams::proc_global_shmem_size::set(|| Ok(crate::proc_shmem::ProcGlobalShmemSize()));
    seams::init_proc_global::set(crate::proc_shmem::InitProcGlobal);

    // Sync-rep PGPROC fields (syncrep.c wait queue).
    seams::my_proc_sync_rep_state::set(my_proc_sync_rep_state);
    seams::set_my_proc_sync_rep_state::set(set_my_proc_sync_rep_state);
    seams::set_proc_sync_rep_state::set(set_proc_sync_rep_state);
    seams::my_proc_wait_lsn::set(my_proc_wait_lsn);
    seams::set_my_proc_wait_lsn::set(set_my_proc_wait_lsn);
    seams::proc_wait_lsn::set(proc_wait_lsn);
    seams::proc_sync_rep_links::set(proc_sync_rep_links);
    seams::set_proc_sync_rep_links::set(set_proc_sync_rep_links);
}
