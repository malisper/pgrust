//! A process claiming and releasing its `PGPROC` slot (`storage/lmgr/proc.c`).
//!
//! `InitProcess` pops a free `PGPROC` from the freelist matching this
//! backend's class, links the embedded latch/semaphore, and registers
//! `RemoveProcFromArray`/`ProcKill` as shmem-exit callbacks. `ProcKill`
//! releases the slot's held LWLocks, removes it from any lock group, and
//! pushes it back onto its `procgloballist`. `InitAuxiliaryProcess` /
//! `AuxiliaryProcKill` do the same for the fixed auxiliary-proc slots.
//!
//! RECLAIMED here: the freelist `dlist` pop/push over the real `PGPROC` array
//! and the per-`PGPROC` field-init sequence that mirrors `InitProcess` /
//! `InitAuxiliaryProcess` / `ProcKill` / `AuxiliaryProcKill` one-for-one.
//!
//! OUTWARD seams: procarray (`ProcArrayAdd`/`ProcArrayRemove`), lwlock
//! (`LWLockReleaseAll`/`LWLockAcquire`/`LWLockRelease`), latch (`OwnLatch`/
//! `DisownLatch`/`SwitchToSharedLatch`/`SwitchBackToLocalLatch`), syncrep
//! (`SyncRepCleanupAtProcExit`), condition-variable
//! (`ConditionVariableCancelSleep`), pgstat wait-event, pmsignal
//! (`RegisterPostmasterChildActive`), and the deadlock checker
//! (`InitDeadLockChecking`).
//!
//! The cluster-wide `ProcGlobal` (`PROC_HDR`) / `AuxiliaryProcs` /
//! `ProcStructLock` substrate — including the freelist `dlist` storage and the
//! `GetNumberFromPGProc`/`GetPGProcByNumber` mapping — is set up by the sibling
//! [`crate::proc_shmem`] family module (`InitProcGlobal`). Until that lands its
//! per-owner accessors panic; `InitProcess`/`ProcKill` thread through them
//! exactly where the C reaches into `ProcGlobal`.

use mcx::Mcx;
use types_core::{
    InvalidLocalTransactionId, InvalidOid, InvalidTransactionId, ProcNumber, XidStatus,
    INVALID_PROC_NUMBER,
};
use types_tuple::Datum;
use types_error::PgResult;
use types_storage::lock::LOCKMASK;
use types_storage::storage::{
    LW_WS_NOT_WAITING, PGPROC, PROC_IS_AUTOVACUUM, PROC_WAIT_STATUS_OK,
};

use crate::seam;

/// `InvalidXLogRecPtr` (`access/xlogdefs.h`) — value 0; used to zero
/// `clogGroupMemberLsn`.
const INVALID_XLOG_REC_PTR: u64 = 0;
/// `SYNC_REP_NOT_WAITING` (`replication/syncrep.h`).
const SYNC_REP_NOT_WAITING: i32 = 0;
/// `TRANSACTION_STATUS_IN_PROGRESS` (`access/clog.h`).
const TRANSACTION_STATUS_IN_PROGRESS: XidStatus = 0x00;

/// Reset every field of a freshly-claimed `MyProc` exactly as `InitProcess`
/// does (the block shared by `InitProcess` after the freelist pop; the aux
/// variant differs only in `vxid.procNumber` and the syncrep/group fields it
/// omits, handled separately).
///
/// `regular` mirrors `MyProc->isRegularBackend = AmRegularBackendProcess()`.
fn init_my_proc_common(proc: &mut PGPROC, procno: ProcNumber, regular: bool) {
    // dlist_node_init(&MyProc->links);
    proc.links = Default::default();
    proc.waitStatus = PROC_WAIT_STATUS_OK;
    proc.fpVXIDLock = false;
    proc.fpLocalTransactionId = InvalidLocalTransactionId;
    proc.xid = InvalidTransactionId;
    proc.xmin = InvalidTransactionId;
    proc.pid = seam::my_proc_pid();
    proc.vxid.procNumber = procno;
    proc.vxid.lxid = InvalidLocalTransactionId;
    // databaseId and roleId will be filled in later
    proc.databaseId = InvalidOid;
    proc.roleId = InvalidOid;
    proc.tempNamespaceId = InvalidOid;
    proc.isRegularBackend = regular;
    proc.delayChkptFlags = 0;
    proc.statusFlags = 0;
    // NB -- autovac launcher intentionally does not set IS_AUTOVACUUM
    if seam::am_autovacuum_worker_process() {
        proc.statusFlags |= PROC_IS_AUTOVACUUM;
    }
    proc.lwWaiting = LW_WS_NOT_WAITING as u8;
    proc.lwWaitMode = 0;
    // Live wait state is the genuinely-shared cells; reset them too.
    crate::proc_shmem::lw_waiting_write(procno, LW_WS_NOT_WAITING as u8);
    crate::proc_shmem::lw_wait_mode_write(procno, 0);
    crate::proc_shmem::lw_wait_link_write(
        procno,
        types_storage::proclist_node { next: 0, prev: 0 },
    );
    proc.waitLock = None;
    proc.waitProcLock = None;
    proc.waitStart.write(0);
    // Clear the cross-process awaited-lock / queued flag for this (possibly
    // reused) slot, matching the fresh PGPROC the C InitProcess sees.
    crate::proc_shmem::set_proc_wait_lock_shared(procno, None);

    // USE_ASSERT_CHECKING: last process should have released all locks; the
    // myProcLocks partitions must already be empty — assertion-only, omitted.

    proc.recoveryConflictPending = false;

    // Initialize fields for sync rep
    proc.waitLSN = INVALID_XLOG_REC_PTR;
    proc.syncRepState = SYNC_REP_NOT_WAITING;
    proc.syncRepLinks = Default::default();

    // Initialize fields for group XID clearing.
    proc.procArrayGroupMember = false;
    proc.procArrayGroupMemberXid = InvalidTransactionId;
    // Assert(procArrayGroupNext == INVALID_PROC_NUMBER) — assertion only.

    // Group locking fields (lockGroupLeader / lockGroupMembers) must already
    // be in their initial state — assertion only.

    // Initialize wait event information.
    proc.wait_event_info = 0;

    // Initialize fields for group transaction status update.
    proc.clogGroupMember = false;
    proc.clogGroupMemberXid = InvalidTransactionId;
    proc.clogGroupMemberXidStatus = TRANSACTION_STATUS_IN_PROGRESS;
    proc.clogGroupMemberPage = -1;
    proc.clogGroupMemberLsn = INVALID_XLOG_REC_PTR;
    // Assert(clogGroupNext == INVALID_PROC_NUMBER) — assertion only.

    // Mirror the canonical xmin/databaseId/statusFlags fields into the
    // genuinely-shared per-proc arrays (these PGPROC fields are real shmem in C;
    // ProcArrayInstallRestoredXmin / GetSnapshotData read them cross-process).
    // statusFlags is read back from `proc` because the autovac-worker branch
    // above may have OR'd in PROC_IS_AUTOVACUUM.
    crate::proc_shmem::set_proc_xmin_shared(procno, InvalidTransactionId);
    crate::proc_shmem::set_proc_database_id_shared(procno, InvalidOid);
    crate::proc_shmem::set_proc_status_flags_shared(procno, proc.statusFlags);
}

/// `InitProcess(void)` — claim a `PGPROC` for a regular/background backend.
pub fn InitProcess(_mcx: Mcx<'_>) -> PgResult<()> {
    // ProcGlobal should be set up already.
    if !seam::proc_global_is_set() {
        seam::elog_panic("proc header uninitialized");
    }

    if seam::my_proc_is_set() {
        return seam::elog_error("you already exist");
    }

    // Mark ourselves as an active postmaster child before touching shmem.
    if seam::is_under_postmaster() {
        seam::register_postmaster_child_active();
    }

    // Decide which list should supply our PGPROC. This logic must match the
    // way the freelists were constructed in InitProcGlobal().
    let procgloballist = if seam::am_autovacuum_worker_process()
        || seam::am_special_worker_process()
    {
        seam::FreeList::Autovac
    } else if seam::am_background_worker_process() {
        seam::FreeList::Bgworker
    } else if seam::am_wal_sender_process() {
        seam::FreeList::Walsender
    } else {
        seam::FreeList::Regular
    };

    // Try to get a proc struct from the appropriate free list. While we hold
    // ProcStructLock, also copy the current shared estimate of
    // spins_per_delay to local storage.
    seam::spin_lock_acquire_proc_struct_lock();

    seam::set_spins_per_delay(seam::proc_global_spins_per_delay());

    let my_procno = match seam::freelist_pop_head(procgloballist) {
        Some(procno) => {
            seam::spin_lock_release_proc_struct_lock();
            procno
        }
        None => {
            // All the PGPROCs are in use: the standard "too many backends"
            // detection point.
            seam::spin_lock_release_proc_struct_lock();
            if seam::am_wal_sender_process() {
                return seam::ereport_fatal_too_many_wal_senders(seam::max_wal_senders());
            }
            return seam::ereport_fatal_too_many_clients();
        }
    };

    // MyProcNumber = GetNumberFromPGProc(MyProc);
    seam::set_my_proc(my_procno);
    seam::set_my_proc_number(my_procno);

    // Cross-check that the PGPROC is of the type we expect.
    debug_assert!(seam::proc_globallist_of(my_procno) == procgloballist);

    // Initialize all fields of MyProc, except for those previously
    // initialized by InitProcGlobal.
    let am_regular_backend = seam::am_regular_backend_process();
    seam::with_my_proc(|proc| init_my_proc_common(proc, my_procno, am_regular_backend));

    // Publish MyProc->pid into the genuinely-shared pid word (the cross-process
    // slot-occupancy marker; init_my_proc_common set the per-process field). C
    // writes the single shared `pid` field; here the shared write is split from
    // the closure to avoid re-borrowing ProcGlobal.
    seam::set_proc_pid(my_procno, seam::my_proc_pid());

    // Acquire ownership of the PGPROC's latch and repoint the process latch
    // (which so far points at the process-local one) to the shared one.
    seam::own_latch(my_procno);
    seam::switch_to_shared_latch();

    // Now that we have a proc, report wait events to shared memory.
    seam::pgstat_set_wait_event_storage(my_procno);

    // We might be reusing a semaphore that belonged to a failed process, so
    // reinitialize its value here.
    seam::pg_semaphore_reset(my_procno);

    // Arrange to clean up at backend exit.
    seam::on_shmem_exit(ProcKill, Datum::from_usize(0));

    // Now that we have a PGPROC, we could try to acquire locks, so initialize
    // local state needed for LWLocks, and the deadlock checker.
    seam::init_lwlock_access();
    seam::init_deadlock_checking();

    // EXEC_BACKEND: AttachSharedMemoryStructs() — not applicable.

    Ok(())
}

/// `InitProcessPhase2(void)` — finish proc init once shared memory is fully
/// attached (adds `MyProc` to the procarray).
pub fn InitProcessPhase2(_mcx: Mcx<'_>) -> PgResult<()> {
    debug_assert!(seam::my_proc_is_set());

    // Add our PGPROC to the PGPROC array in shared memory.
    seam::proc_array_add(seam::my_proc_number());

    // Arrange to clean that up at backend exit.
    seam::on_shmem_exit(RemoveProcFromArray, Datum::from_usize(0));

    Ok(())
}

/// `InitAuxiliaryProcess(void)` — claim one of the fixed auxiliary-process
/// `PGPROC` slots (checkpointer, bgwriter, walwriter, ...).
pub fn InitAuxiliaryProcess(_mcx: Mcx<'_>) -> PgResult<()> {
    // ProcGlobal and AuxiliaryProcs should be set up already.
    if !seam::proc_global_is_set() || !seam::auxiliary_procs_is_set() {
        seam::elog_panic("proc header uninitialized");
    }

    if seam::my_proc_is_set() {
        return seam::elog_error("you already exist");
    }

    if seam::is_under_postmaster() {
        seam::register_postmaster_child_active();
    }

    // We use the ProcStructLock to protect assignment and releasing of
    // AuxiliaryProcs entries. While we hold it, also copy the shared estimate
    // of spins_per_delay to local storage.
    seam::spin_lock_acquire_proc_struct_lock();

    seam::set_spins_per_delay(seam::proc_global_spins_per_delay());

    // Find a free auxproc ... *big* trouble if there isn't one ...
    let proctype = match seam::auxiliary_proc_find_free() {
        Some(proctype) => proctype,
        None => {
            seam::spin_lock_release_proc_struct_lock();
            seam::elog_fatal("all AuxiliaryProcs are in use");
        }
    };

    // Mark auxiliary proc as in use by me.
    let aux_procno = seam::auxiliary_proc_procno(proctype);
    seam::set_proc_pid(aux_procno, seam::my_proc_pid());

    seam::spin_lock_release_proc_struct_lock();

    // MyProc = auxproc; MyProcNumber = GetNumberFromPGProc(MyProc);
    seam::set_my_proc(aux_procno);
    seam::set_my_proc_number(aux_procno);

    // Initialize all fields of MyProc, except for those previously
    // initialized by InitProcGlobal. The aux variant uses
    // INVALID_PROC_NUMBER for vxid.procNumber, sets isRegularBackend = false,
    // and (unlike a regular backend) does not init the sync-rep / group-XID /
    // wait-event / clog-group fields.
    seam::with_my_proc(|proc| {
        proc.links = Default::default();
        proc.waitStatus = PROC_WAIT_STATUS_OK;
        proc.fpVXIDLock = false;
        proc.fpLocalTransactionId = InvalidLocalTransactionId;
        proc.xid = InvalidTransactionId;
        proc.xmin = InvalidTransactionId;
        proc.vxid.procNumber = INVALID_PROC_NUMBER;
        proc.vxid.lxid = InvalidLocalTransactionId;
        proc.databaseId = InvalidOid;
        proc.roleId = InvalidOid;
        proc.tempNamespaceId = InvalidOid;
        proc.isRegularBackend = false;
        proc.delayChkptFlags = 0;
        proc.statusFlags = 0;
        proc.lwWaiting = LW_WS_NOT_WAITING as u8;
        proc.lwWaitMode = 0;
        // Live wait state is the genuinely-shared cells; reset them too.
        crate::proc_shmem::lw_waiting_write(aux_procno, LW_WS_NOT_WAITING as u8);
        crate::proc_shmem::lw_wait_mode_write(aux_procno, 0);
        crate::proc_shmem::lw_wait_link_write(
            aux_procno,
            types_storage::proclist_node { next: 0, prev: 0 },
        );
        proc.waitLock = None;
        proc.waitProcLock = None;
        proc.waitStart.write(0);
        // USE_ASSERT_CHECKING: myProcLocks partitions must be empty — omitted.
    });
    // Clear the cross-process awaited-lock / queued flag for this aux slot.
    crate::proc_shmem::set_proc_wait_lock_shared(aux_procno, None);

    // Mirror the canonical xmin/databaseId/statusFlags into the genuinely-shared
    // per-proc arrays (real shmem PGPROC fields read cross-process).
    crate::proc_shmem::set_proc_xmin_shared(aux_procno, InvalidTransactionId);
    crate::proc_shmem::set_proc_database_id_shared(aux_procno, InvalidOid);
    crate::proc_shmem::set_proc_status_flags_shared(aux_procno, 0);

    // Acquire ownership of the PGPROC's latch and repoint the process latch.
    seam::own_latch(aux_procno);
    seam::switch_to_shared_latch();

    // Now that we have a proc, report wait events to shared memory.
    seam::pgstat_set_wait_event_storage(aux_procno);

    // Group locking fields must be in their initial state — assertion only.

    // Reinitialize the (possibly reused) semaphore.
    seam::pg_semaphore_reset(aux_procno);

    // Arrange to clean up at process exit.
    seam::on_shmem_exit(AuxiliaryProcKill, Datum::from_i32(proctype));

    // Now that we have a PGPROC, we could try to acquire lightweight locks.
    // (Heavyweight locks cannot be acquired in aux processes, so no deadlock
    // checker.)
    seam::init_lwlock_access();

    // EXEC_BACKEND: AttachSharedMemoryStructs() — not applicable.

    Ok(())
}

/// `RemoveProcFromArray(int code, Datum arg)` — shmem-exit callback that
/// removes `MyProc` from the procarray.
pub fn RemoveProcFromArray(_code: i32, _arg: Datum<'static>) -> PgResult<()> {
    debug_assert!(seam::my_proc_is_set());
    seam::proc_array_remove(seam::my_proc_number(), InvalidTransactionId);
    Ok(())
}

/// `ProcKill(int code, Datum arg)` — shmem-exit callback that releases this
/// backend's `PGPROC`: drop held LWLocks, leave any lock group, push the slot
/// back onto its freelist.
pub fn ProcKill(_code: i32, _arg: Datum<'static>) -> PgResult<()> {
    debug_assert!(seam::my_proc_is_set());
    let my_procno = seam::my_proc_number();

    // not safe if forked by system(), etc.
    if seam::proc_pid(my_procno) != seam::getpid() {
        seam::elog_panic("ProcKill() called in child process");
    }

    // Make sure we're out of the sync rep lists.
    seam::sync_rep_cleanup_at_proc_exit();

    // USE_ASSERT_CHECKING: myProcLocks partitions must be empty — omitted.

    // Release any LW locks I am holding (there really shouldn't be any).
    seam::lwlock_release_all();

    // Cancel any pending condition variable sleep, too.
    seam::condition_variable_cancel_sleep();

    // Detach from any lock group of which we are a member. If the leader
    // exits before all other group members, its PGPROC remains allocated
    // until the last group process exits; that process returns the leader's
    // PGPROC to the appropriate list.
    if let Some(leader) = seam::proc_lock_group_leader(my_procno) {
        let leader_lwlock = seam::lock_hash_partition_lock_by_proc(leader);

        seam::lwlock_acquire_exclusive(leader_lwlock);
        debug_assert!(!seam::proc_lock_group_members_is_empty(leader));
        seam::dlist_delete_lock_group_link(my_procno);
        if seam::proc_lock_group_members_is_empty(leader) {
            seam::set_proc_lock_group_leader(leader, None);
            if leader != my_procno {
                let procgloballist = seam::proc_globallist_of(leader);

                // Leader exited first; return its PGPROC.
                seam::spin_lock_acquire_proc_struct_lock();
                seam::freelist_push_head(procgloballist, leader);
                seam::spin_lock_release_proc_struct_lock();
            }
        } else if leader != my_procno {
            seam::set_proc_lock_group_leader(my_procno, None);
        }
        seam::lwlock_release(leader_lwlock);
    }

    // Reset MyLatch to the process-local one so that signal handlers et al can
    // keep using the latch after the shared latch isn't ours anymore.
    // Similarly stop reporting wait events to MyProc->wait_event_info. After
    // that clear MyProc and disown the shared latch.
    seam::switch_back_to_local_latch();
    seam::pgstat_reset_wait_event_storage();

    let proc = my_procno;
    seam::clear_my_proc();
    seam::set_my_proc_number(INVALID_PROC_NUMBER);
    seam::disown_latch(proc);

    // Mark the proc no longer in use.
    seam::set_proc_pid(proc, 0);
    seam::set_proc_vxid_proc_number(proc, INVALID_PROC_NUMBER);
    seam::set_proc_vxid_lxid(proc, InvalidTransactionId);

    let procgloballist = seam::proc_globallist_of(proc);
    seam::spin_lock_acquire_proc_struct_lock();

    // If we're still a member of a locking group, that means we're a leader
    // that has somehow exited before its children; the last remaining child
    // will release our PGPROC. Otherwise release it now.
    if seam::proc_lock_group_leader(proc).is_none() {
        // lockGroupMembers should be empty here — assertion only.
        // Return PGPROC structure (and semaphore) to appropriate freelist.
        seam::freelist_push_tail(procgloballist, proc);
    }

    // Update shared estimate of spins_per_delay.
    seam::set_proc_global_spins_per_delay(seam::update_spins_per_delay(
        seam::proc_global_spins_per_delay(),
    ));

    seam::spin_lock_release_proc_struct_lock();

    // Wake autovac launcher if needed -- see comments in FreeWorkerInfo.
    let avl = seam::autovacuum_launcher_pid();
    if avl != 0 {
        seam::kill_sigusr2(avl);
    }

    Ok(())
}

/// `AuxiliaryProcKill(int code, Datum arg)` — shmem-exit callback releasing an
/// auxiliary-process `PGPROC` slot.
pub fn AuxiliaryProcKill(_code: i32, arg: Datum<'static>) -> PgResult<()> {
    let proctype = arg.as_i32();
    debug_assert!(proctype >= 0 && proctype < types_storage::storage::NUM_AUXILIARY_PROCS);

    let my_procno = seam::my_proc_number();

    // not safe if forked by system(), etc.
    if seam::proc_pid(my_procno) != seam::getpid() {
        seam::elog_panic("AuxiliaryProcKill() called in child process");
    }

    let auxproc = seam::auxiliary_proc_procno(proctype);
    debug_assert!(my_procno == auxproc);

    // Release any LW locks I am holding (see notes above).
    seam::lwlock_release_all();

    // Cancel any pending condition variable sleep, too.
    seam::condition_variable_cancel_sleep();

    // look at the equivalent ProcKill() code for comments
    seam::switch_back_to_local_latch();
    seam::pgstat_reset_wait_event_storage();

    let proc = my_procno;
    seam::clear_my_proc();
    seam::set_my_proc_number(INVALID_PROC_NUMBER);
    seam::disown_latch(proc);

    seam::spin_lock_acquire_proc_struct_lock();

    // Mark auxiliary proc no longer in use.
    seam::set_proc_pid(proc, 0);
    seam::set_proc_vxid_proc_number(proc, INVALID_PROC_NUMBER);
    seam::set_proc_vxid_lxid(proc, InvalidTransactionId);

    // Update shared estimate of spins_per_delay.
    seam::set_proc_global_spins_per_delay(seam::update_spins_per_delay(
        seam::proc_global_spins_per_delay(),
    ));

    seam::spin_lock_release_proc_struct_lock();

    Ok(())
}

/// `AuxiliaryPidGetProc(int pid)` — find the auxiliary-process `PGPROC` with
/// the given pid, or `None`.
pub fn AuxiliaryPidGetProc(pid: i32) -> Option<ProcNumber> {
    // never match dummy PGPROCs
    if pid == 0 {
        return None;
    }

    let mut index = 0;
    while index < types_storage::storage::NUM_AUXILIARY_PROCS {
        let procno = seam::auxiliary_proc_procno(index);
        if seam::proc_pid(procno) == pid {
            return Some(procno);
        }
        index += 1;
    }
    None
}

/// `SetStartupBufferPinWaitBufId(int bufid)` — record the buffer the Startup
/// process is waiting for a pin on.
pub fn SetStartupBufferPinWaitBufId(bufid: i32) {
    seam::set_proc_global_startup_buffer_pin_wait_buf_id(bufid);
}

/// `GetStartupBufferPinWaitBufId(void)` — the buffer the Startup process is
/// waiting for a pin on, or -1.
pub fn GetStartupBufferPinWaitBufId() -> i32 {
    seam::proc_global_startup_buffer_pin_wait_buf_id()
}

/// `HaveNFreeProcs(int n, int *nfree)` — true if at least `n` PGPROCs remain
/// on the regular freelist; reports the count seen via `nfree`.
pub fn HaveNFreeProcs(n: i32, nfree: &mut i32) -> bool {
    debug_assert!(n > 0);

    seam::spin_lock_acquire_proc_struct_lock();

    *nfree = 0;
    for _ in seam::freelist_regular_iter() {
        *nfree += 1;
        if *nfree == n {
            break;
        }
    }

    seam::spin_lock_release_proc_struct_lock();

    *nfree == n
}

/// `IsWaitingForLock(void)` — whether this backend is currently blocked on a
/// heavyweight lock (`lockAwaited != NULL`).
pub fn IsWaitingForLock() -> bool {
    seam::lock_awaited_is_set()
}

// ---- MyProc / PGPROC-array owner accessors --------------------------------
//
// `MyProc` and the `ProcGlobal->allProcs[]` array (and the intrusive
// `lockGroupMembers` lists threaded through it) are owned by this crate and are
// stood up by `proc_shmem::InitProcGlobal` / `InitProcess`. proc_misc's
// lock-group logic reaches that owned state through these accessors, each a thin
// read/write over `proc_shmem`'s `ProcGlobal->allProcs` / `MyProc`.

/// `GetNumberFromPGProc(MyProc)` (`MyProcNumber`) — this backend's slot index.
pub(crate) fn my_proc_number() -> ProcNumber {
    seam::my_proc_number()
}

/// `GetNumberFromPGProc(proc)` — the slot index of an arbitrary `PGPROC`,
/// computed by pointer arithmetic against `ProcGlobal->allProcs` exactly as the
/// C macro does. `proc` must point into the owned arena.
#[allow(dead_code)]
pub(crate) fn proc_number_of(proc: &PGPROC) -> ProcNumber {
    crate::proc_shmem::proc_number_of(proc)
}

/// `GetPGProcByNumber(procno)->pid` — the pid published in the genuinely-shared
/// per-slot pid word (the interlock value lock-group join checks against).
pub(crate) fn proc_pid_of(procno: ProcNumber) -> i32 {
    seam::proc_pid(procno)
}

/// `GetPGProcByNumber(procno)->lockGroupLeader == GetPGProcByNumber(leaderno)`
/// — whether the proc in slot `procno` has slot `leaderno` as its lock-group
/// leader.
pub(crate) fn proc_lock_group_leader_is(procno: ProcNumber, leaderno: ProcNumber) -> bool {
    crate::proc_shmem::proc_lock_group_leader_shared(procno) == Some(leaderno)
}

/// `GetPGProcByNumber(procno)->lockGroupLeader == NULL`.
pub(crate) fn proc_lock_group_leader_is_none(procno: ProcNumber) -> bool {
    crate::proc_shmem::proc_lock_group_leader_shared(procno).is_none()
}

/// `MyProc->lockGroupLeader = GetPGProcByNumber(leaderno)`. Written to the
/// genuinely-shared lockGroupLeader array (cross-process visible).
pub(crate) fn set_my_proc_lock_group_leader(leaderno: ProcNumber) {
    let my_procno = seam::my_proc_number();
    crate::proc_shmem::set_proc_lock_group_leader_shared(my_procno, Some(leaderno));
}

/// `dlist_push_head(&GetPGProcByNumber(leaderno)->lockGroupMembers,
///  &GetPGProcByNumber(memberno)->lockGroupLink)`.
pub(crate) fn lock_group_members_push_head(leaderno: ProcNumber, memberno: ProcNumber) {
    crate::proc_shmem::lock_group_members_push_head(leaderno, memberno);
}

/// `dlist_push_tail(&GetPGProcByNumber(leaderno)->lockGroupMembers,
///  &GetPGProcByNumber(memberno)->lockGroupLink)`.
pub(crate) fn lock_group_members_push_tail(leaderno: ProcNumber, memberno: ProcNumber) {
    crate::proc_shmem::lock_group_members_push_tail(leaderno, memberno);
}

/// `dlist_foreach(iter, &GetPGProcByNumber(leaderno)->lockGroupMembers)` —
/// iterate the slot indices of every member of `leaderno`'s lock group
/// (`dlist_container(PGPROC, lockGroupLink, iter.cur)`).
#[allow(dead_code)]
pub(crate) fn lock_group_members_iter(leaderno: ProcNumber) -> Vec<ProcNumber> {
    crate::proc_shmem::lock_group_members_snapshot(leaderno)
}

/// The `holdMask` of every `PROCLOCK` on
/// `GetPGProcByNumber(procno)->myProcLocks[partition]`. The `myProcLocks`
/// partitions hold `PROCLOCK`s, which are lock.c-owned shmem records; walking
/// them belongs to lock.c, so this routes through lock.c's
/// `proc_locks_hold_masks` seam (panics until lock.c installs it). Only
/// reachable from the dead reclaimed `proc_misc::lock_group_held_locks`
/// (JoinWaitQueue uses the `lock::lock_group_held_locks` seam instead).
#[allow(dead_code)]
pub(crate) fn my_proc_locks_hold_masks(procno: ProcNumber, partition: usize) -> Vec<LOCKMASK> {
    backend_storage_lmgr_lock_seams::proc_locks_hold_masks::call(procno, partition)
}
