use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

use types_core::{ProcNumber, INVALID_PROC_NUMBER};
use types_error::{PgResult, DEBUG1, LOG, WARNING};
use types_storage::lock::{
    DeadLockState, LockMethod, LOCALLOCKTAG, LOCK, LOCKBIT_OFF, LOCKBIT_ON, LOCKMODE, PROCLOCK,
};
use types_storage::storage::{
    proclist_node, ProcWaitStatus, NUM_LOCK_PARTITIONS, PROC_ARRAY_LOCK, PROC_IS_AUTOVACUUM,
    PROC_VACUUM_FOR_WRAPAROUND, PROC_WAIT_STATUS_ERROR, PROC_WAIT_STATUS_OK,
    PROC_WAIT_STATUS_WAITING,
};
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET};

use crate::locallock::{awaited_lock, set_awaited_lock, with_local};
use crate::shared::{
    lock_check_conflicts_raw, CleanUpLock, LockHashPartitionLock, LockHashPartitionLockByIndex,
    LockTagHashCode,
};
use crate::my_procno;

const PG_WAIT_LOCK: u32 = 0x0300_0000;

// LOCK.waitProcs ops: C's dclist over PGPROC.links as ProcNumbers, [PART].

fn links_of(procno: ProcNumber) -> &'static types_storage::storage::SyncCell<proclist_node> {
    &lmgr_proc::GetPGProcByNumber(procno).links
}

unsafe fn wq_push_tail(lock: *mut LOCK, procno: ProcNumber) {
    let q = &mut (*lock).waitProcs;
    let tail = q.list.tail;
    links_of(procno).set(proclist_node {
        prev: tail,
        next: INVALID_PROC_NUMBER,
    });
    if tail == INVALID_PROC_NUMBER {
        q.list.head = procno;
    } else {
        let cell = links_of(tail);
        let mut node = cell.get();
        node.next = procno;
        cell.set(node);
    }
    q.list.tail = procno;
    q.count += 1;
}

unsafe fn wq_insert_before(lock: *mut LOCK, procno: ProcNumber, before: ProcNumber) {
    let q = &mut (*lock).waitProcs;
    let prev = links_of(before).get().prev;
    links_of(procno).set(proclist_node {
        prev,
        next: before,
    });
    if prev == INVALID_PROC_NUMBER {
        q.list.head = procno;
    } else {
        let cell = links_of(prev);
        let mut node = cell.get();
        node.next = procno;
        cell.set(node);
    }
    let cell = links_of(before);
    let mut node = cell.get();
    node.prev = procno;
    cell.set(node);
    q.count += 1;
}

// Leaves the node detached — how waiters detect they left the queue.
unsafe fn wq_delete(lock: *mut LOCK, procno: ProcNumber) {
    let q = &mut (*lock).waitProcs;
    let node = links_of(procno).get();
    if node.prev == INVALID_PROC_NUMBER {
        q.list.head = node.next;
    } else {
        let cell = links_of(node.prev);
        let mut prev = cell.get();
        prev.next = node.next;
        cell.set(prev);
    }
    if node.next == INVALID_PROC_NUMBER {
        q.list.tail = node.prev;
    } else {
        let cell = links_of(node.next);
        let mut next = cell.get();
        next.prev = node.prev;
        cell.set(next);
    }
    links_of(procno).set(proclist_node::detached());
    debug_assert!(q.count > 0);
    q.count -= 1;
}

// Deletion-safe iteration; SAFETY contract: the lock's partition LWLock held.
pub unsafe fn wq_foreach(lock: *mut LOCK, mut body: impl FnMut(ProcNumber) -> bool) {
    let mut cur = (*lock).waitProcs.list.head;
    while cur != INVALID_PROC_NUMBER {
        let next = links_of(cur).get().next;
        if !body(cur) {
            break;
        }
        cur = next;
    }
}

/// DeadLockCheck's queue rearrangement; SAFETY contract: all partition LWLocks held exclusive.
pub unsafe fn SetWaitQueueOrder(lock: *mut LOCK, procs: &[ProcNumber]) {
    debug_assert_eq!((*lock).waitProcs.count as usize, procs.len());
    (*lock).waitProcs.list.head = INVALID_PROC_NUMBER;
    (*lock).waitProcs.list.tail = INVALID_PROC_NUMBER;
    (*lock).waitProcs.count = 0;
    for &procno in procs {
        wq_push_tail(lock, procno);
    }
}

/// Join the wait queue on locallock's lock; may grant immediately instead.
/// SAFETY contract: the lock's partition LWLock held exclusive; the caller
/// releases it before ProcSleep.
pub(crate) unsafe fn JoinWaitQueue(
    localtag: &LOCALLOCKTAG,
    lockMethodTable: LockMethod,
    dontWait: bool,
) -> ProcWaitStatus {
    let lockmode = localtag.mode;
    let (lock, proclock) = with_local(|state| {
        let ll = state.table.get(localtag).expect("missing LOCALLOCK");
        (ll.lock, ll.proclock)
    });
    let procno = my_procno();
    let proc = lmgr_proc::GetPGProcByNumber(procno);
    let leader = proc.lockGroupLeader.load(Relaxed);

    let my_proc_held_locks = (*proclock).holdMask;
    let mut my_held_locks = my_proc_held_locks;
    proc.heldLocks.set(my_proc_held_locks);

    // Group members' holds give us their priority in the queue.
    if leader != INVALID_PROC_NUMBER {
        crate::shared::foreach_proclock_on_lock(lock, |other| {
            if (*other).groupLeader == leader {
                my_held_locks |= (*other).holdMask;
            }
            true
        });
    }

    // Go ahead of any waiter that must wait for locks we already hold
    // (cheap early deadlock avoidance).
    let mut insert_before = INVALID_PROC_NUMBER;
    let mut early_deadlock = false;
    let mut granted_instead = false;
    if my_held_locks != 0 && (*lock).waitProcs.count > 0 {
        let mut ahead_requests = 0;
        wq_foreach(lock, |waiter_no| {
            let waiter = lmgr_proc::GetPGProcByNumber(waiter_no);
            if leader != INVALID_PROC_NUMBER && leader == waiter.lockGroupLeader.load(Relaxed) {
                return true;
            }
            let waiter_mode = waiter.waitLockMode.get();
            if lockMethodTable.conflictTab[waiter_mode as usize] & my_held_locks != 0 {
                if lockMethodTable.conflictTab[lockmode as usize] & waiter.heldLocks.get() != 0 {
                    deadlock_seams::remember_simple_deadlock::call(
                        procno,
                        lockmode,
                        (*lock).tag,
                        waiter_no,
                    );
                    early_deadlock = true;
                    return false;
                }
                if lockMethodTable.conflictTab[lockmode as usize] & ahead_requests == 0
                    && !lock_check_conflicts_raw(
                        lockMethodTable,
                        lockmode,
                        lock,
                        proclock,
                        procno,
                    )
                {
                    crate::shared::grant_lock_raw(lock, proclock, lockmode);
                    granted_instead = true;
                    return false;
                }
                insert_before = waiter_no;
                return false;
            }
            ahead_requests |= LOCKBIT_ON(waiter_mode);
            true
        });
    }
    if granted_instead {
        return PROC_WAIT_STATUS_OK;
    }
    if early_deadlock {
        return PROC_WAIT_STATUS_ERROR;
    }
    if dontWait {
        return PROC_WAIT_STATUS_ERROR;
    }

    if insert_before != INVALID_PROC_NUMBER {
        wq_insert_before(lock, procno, insert_before);
    } else {
        wq_push_tail(lock, procno);
    }
    (*lock).waitMask |= LOCKBIT_ON(lockmode);

    proc.heldLocks.set(my_proc_held_locks);
    proc.waitLock.set(lock);
    proc.waitProcLock.set(proclock);
    proc.waitLockMode.set(lockmode);
    proc.waitStatus.store(PROC_WAIT_STATUS_WAITING, Release);

    PROC_WAIT_STATUS_WAITING
}

/// ProcSleep wrapper with tracing/awaited-lock bookkeeping (lock.c WaitOnLock).
pub(crate) fn WaitOnLock(
    localtag: &LOCALLOCKTAG,
    hashcode: u32,
    owner: types_resowner::ResourceOwner,
) -> PgResult<ProcWaitStatus> {
    ps_status_seams::set_ps_display_suffix::call("waiting");

    // LockErrorCleanup cleans up if cancel/die happens while asleep.
    set_awaited_lock(*localtag, hashcode, owner);

    match ProcSleep(localtag) {
        Ok(result) => {
            crate::locallock::ResetAwaitedLock();
            ps_status_seams::set_ps_display_remove_suffix::call();
            Ok(result)
        }
        Err(e) => {
            // awaited lock stays set until LockErrorCleanup.
            ps_status_seams::set_ps_display_remove_suffix::call();
            Err(e)
        }
    }
}

/// Sleep until the lock joined in JoinWaitQueue is granted (or deadlock).
pub fn ProcSleep(localtag: &LOCALLOCKTAG) -> PgResult<ProcWaitStatus> {
    let lockmode = localtag.mode;
    let (lock, hashcode) = with_local(|state| {
        let ll = state.table.get(localtag).expect("missing LOCALLOCK");
        (ll.lock, ll.hashcode)
    });
    let partition_lock = LockHashPartitionLock(hashcode);
    let procno = my_procno();
    let proc = lmgr_proc::GetPGProcByNumber(procno);
    let mut allow_autovacuum_cancel = true;

    debug_assert_eq!(awaited_lock().map(|(t, _)| t), Some(*localtag));
    debug_assert!(!lwlock::LWLockHeldByMe(partition_lock));

    // InHotStandby (proc.c) — standbyState >= SNAPSHOT_PENDING, a per-process
    // (here per-thread, xlogutils) state only the STARTUP process ever sets:
    // only its lock waits delegate to standby.c's
    // ResolveRecoveryConflictWithLock, with the deadlock_timeout-based
    // "recovery still waiting" reporting when log_recovery_conflict_waits.
    // NOT RecoveryInProgress(): that is true for every backend on a standby,
    // and routing a regular backend's lock wait through the startup-only
    // conflict path breaks its wakeup and deadlock detection (031 case 5:
    // the cursor session's table2 wait hung 120s and the buffer-pin deadlock
    // never got the chance to resolve).
    let in_hot_standby = xlogutils_seams::in_hot_standby::is_installed()
        && xlogutils_seams::in_hot_standby::call();
    let standby_wait_start: i64 = if in_hot_standby {
        if guc_tables::vars::log_recovery_conflict_waits.installed()
            && guc_tables::vars::log_recovery_conflict_waits.read()
        {
            timestamp_seams::get_current_timestamp::call()
        } else {
            0
        }
    } else {
        0
    };
    let mut logged_recovery_conflict = false;

    lmgr_proc::ResetDeadlockWaitState();

    let lock_timeout = lmgr_proc::globals::LockTimeout();
    if !in_hot_standby {
        if lock_timeout > 0 {
            timeout_seams::enable_timeouts::call(&[
                timeout_seams::EnableTimeoutAfterParams {
                    id: timeout_seams::DEADLOCK_TIMEOUT,
                    delay_ms: lmgr_proc::globals::DeadlockTimeout(),
                },
                timeout_seams::EnableTimeoutAfterParams {
                    id: timeout_seams::LOCK_TIMEOUT,
                    delay_ms: lock_timeout,
                },
            ])?;
        } else {
            timeout_seams::enable_timeout_after::call(
                timeout_seams::DEADLOCK_TIMEOUT,
                lmgr_proc::globals::DeadlockTimeout(),
            )?;
        }
        // Reuse the timer's start time as waitStart; written without the
        // partition lock (pg_locks may transiently see null waitstart).
        proc.waitStart
            .write(timeout_seams::get_timeout_start_time::call(
                timeout_seams::DEADLOCK_TIMEOUT,
            ) as u64);
    }

    let my_wait_status = loop {
        if in_hot_standby {
            let maybe_log_conflict = standby_wait_start != 0 && !logged_recovery_conflict;

            // Set a timer and wait for that or for the lock to be granted.
            standby_seams::resolve_recovery_conflict_with_lock::call(
                localtag.lock,
                maybe_log_conflict,
            )?;

            // Log if the startup process has waited longer than
            // deadlock_timeout for a recovery conflict on lock.
            if maybe_log_conflict {
                let now = timestamp_seams::get_current_timestamp::call();
                let deadlock_timeout = lmgr_proc::globals::DeadlockTimeout() as i64;
                if now - standby_wait_start >= deadlock_timeout * 1000 {
                    let cx = ::mcx::MemoryContext::new("ProcSleep/lock-conflicts");
                    let vxids = crate::acquire::GetLockConflicts(
                        cx.mcx(),
                        &localtag.lock,
                        ::types_storage::lock::AccessExclusiveLock,
                    )?;
                    standby_seams::log_recovery_conflict::call(
                        types_storage::storage::ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOCK,
                        standby_wait_start,
                        now,
                        if vxids.is_empty() { None } else { Some(&vxids) },
                        true,
                    )?;
                    logged_recovery_conflict = true;
                }
            }
        } else {
            latch_seams::wait_latch_my_latch::call(
                WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
                0,
                PG_WAIT_LOCK | localtag.lock.locktag_type as u32,
            );
            latch_seams::reset_latch_my_latch::call();
            if lmgr_proc::GotDeadlockTimeout() {
                CheckDeadLock()?;
                lmgr_proc::ResetGotDeadlockTimeout();
            }
            postgres_seams::check_for_interrupts::call()?;
        }

        let my_wait_status = proc.waitStatus.load(Acquire);

        if lmgr_proc::DeadlockState() == DeadLockState::BlockedByAutoVacuum
            && allow_autovacuum_cancel
        {
            cancel_blocking_autovacuum(lock, lockmode)?;
            allow_autovacuum_cancel = false;
        }

        if lmgr_proc::globals::log_lock_waits()
            && lmgr_proc::DeadlockState() != DeadLockState::NotYetChecked
        {
            log_lock_wait_progress(localtag, hashcode, partition_lock, my_wait_status)?;
            lmgr_proc::SetDeadlockState(DeadLockState::NoDeadLock);
        }

        if my_wait_status != PROC_WAIT_STATUS_WAITING {
            break my_wait_status;
        }
    };

    // Preserve the LOCK_TIMEOUT indicator so a timeout-driven cancel is
    // reported as a lock timeout, not a user cancel.
    if !in_hot_standby {
        if lock_timeout > 0 {
            timeout_seams::disable_timeouts::call(&[
                timeout_seams::DisableTimeoutParams {
                    id: timeout_seams::DEADLOCK_TIMEOUT,
                    keep_indicator: false,
                },
                timeout_seams::DisableTimeoutParams {
                    id: timeout_seams::LOCK_TIMEOUT,
                    keep_indicator: true,
                },
            ]);
        } else {
            timeout_seams::disable_timeout::call(timeout_seams::DEADLOCK_TIMEOUT, false)?;
        }
    }

    // Recovery conflict on lock resolved, but the startup process waited
    // longer than deadlock_timeout for it: close out the log pair.
    if in_hot_standby && logged_recovery_conflict {
        standby_seams::log_recovery_conflict::call(
            types_storage::storage::ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOCK,
            standby_wait_start,
            timestamp_seams::get_current_timestamp::call(),
            None,
            false,
        )?;
    }

    // The awaker did all lock-table updates; caller updates the locallock.
    Ok(my_wait_status)
}

/// proc.c's DS_BLOCKED_BY_AUTOVACUUM arm: SIGINT the autovacuum worker that
/// hard-blocks us, unless it is working to prevent Xid wraparound.
#[cold]
fn cancel_blocking_autovacuum(lock: *mut LOCK, lockmode: LOCKMODE) -> PgResult<()> {
    // libc constants inlined; the lock crate is seams-only.
    const SIGINT: i32 = 2;
    const ESRCH: i32 = 3;

    let Some(blocker) = deadlock_seams::get_blocking_autovacuum_procno::call() else {
        return Ok(());
    };
    let autovac = lmgr_proc::GetPGProcByNumber(blocker);

    // Grab info we need, then release the lock immediately. C's race note:
    // the worker can switch transactions before the signal lands; worst case
    // a for-wraparound vacuum is canceled.
    let proc_array_lock = lwlock::main_lock(PROC_ARRAY_LOCK);
    lwlock::LWLockAcquire(proc_array_lock, lwlock::LW_EXCLUSIVE, my_procno())?;
    let status_flags = lmgr_proc::ProcGlobal().statusFlags
        [autovac.pgxactoff.load(Relaxed) as usize]
        .load(Relaxed);
    // SAFETY: `lock` is the LOCK we wait on; its tag is immutable while our
    // request keeps it pinned.
    let (lockmethod_copy, locktag_copy) =
        unsafe { ((*lock).tag.locktag_lockmethodid, (*lock).tag) };
    lwlock::LWLockRelease(proc_array_lock)?;

    // Only do it if the worker is not working to protect against Xid wraparound.
    if status_flags & PROC_IS_AUTOVACUUM != 0 && status_flags & PROC_VACUUM_FOR_WRAPAROUND == 0 {
        let pid = autovac.pid.load(Relaxed);

        let modename = crate::GetLockmodeName(
            lockmethod_copy as types_storage::lock::LOCKMETHODID,
            lockmode,
        );
        let tag_desc = lmgr_seams::describe_lock_tag::call(locktag_copy);
        elog_seams::ereport_msg::call(
            DEBUG1,
            format!("sending cancel to blocking autovacuum PID {pid}"),
            Some(format!(
                "Process {} waits for {modename} on {tag_desc}.",
                init_small::globals::MyProcPid()
            )),
        )?;

        // kill(pid, SIGINT); the worker exiting first is expected (ESRCH).
        let err = procsignal_seams::send_thread_signal::call(pid, SIGINT);
        if err != 0 && err != ESRCH {
            elog_seams::ereport_msg::call(
                WARNING,
                format!("could not send signal to process {pid}: errno {err}"),
                None,
            )?;
        }
    }
    Ok(())
}

#[cold]
fn log_lock_wait_progress(
    localtag: &LOCALLOCKTAG,
    hashcode: u32,
    partition_lock: &'static lwlock::LWLock,
    my_wait_status: ProcWaitStatus,
) -> PgResult<()> {
    let modename = crate::GetLockmodeName(
        localtag.lock.locktag_lockmethodid as types_storage::lock::LOCKMETHODID,
        localtag.mode,
    );
    let tag_desc = lmgr_seams::describe_lock_tag::call(localtag.lock);
    let start = timeout_seams::get_timeout_start_time::call(timeout_seams::DEADLOCK_TIMEOUT);
    let now = timestamp_seams::get_current_timestamp::call();
    let diff_us = now - start;
    let msecs = diff_us / 1000;
    let usecs = diff_us % 1000;
    let pid = init_small::globals::MyProcPid();

    lwlock::LWLockAcquire(partition_lock, lwlock::LW_SHARED, my_procno())?;
    let (holders, waiters, holders_num) = GetLockHoldersAndWaiters(localtag, hashcode);
    lwlock::LWLockRelease(partition_lock)?;
    let noun = if holders_num == 1 {
        "Process holding the lock"
    } else {
        "Processes holding the lock"
    };
    let detail = format!("{noun}: {holders}. Wait queue: {waiters}.");

    match lmgr_proc::DeadlockState() {
        DeadLockState::SoftDeadLock => elog_seams::ereport_msg::call(
            LOG,
            format!(
                "process {pid} avoided deadlock for {modename} on {tag_desc} by rearranging queue order after {msecs}.{usecs:03} ms"
            ),
            Some(detail.clone()),
        )?,
        DeadLockState::HardDeadLock => elog_seams::ereport_msg::call(
            LOG,
            format!(
                "process {pid} detected deadlock while waiting for {modename} on {tag_desc} after {msecs}.{usecs:03} ms"
            ),
            Some(detail.clone()),
        )?,
        _ => {}
    }

    if my_wait_status == PROC_WAIT_STATUS_WAITING {
        elog_seams::ereport_msg::call(
            LOG,
            format!("process {pid} still waiting for {modename} on {tag_desc} after {msecs}.{usecs:03} ms"),
            Some(detail),
        )?;
    } else if my_wait_status == PROC_WAIT_STATUS_OK {
        elog_seams::ereport_msg::call(
            LOG,
            format!("process {pid} acquired {modename} on {tag_desc} after {msecs}.{usecs:03} ms"),
            None,
        )?;
    } else if lmgr_proc::DeadlockState() != DeadLockState::HardDeadLock {
        elog_seams::ereport_msg::call(
            LOG,
            format!("process {pid} failed to acquire {modename} on {tag_desc} after {msecs}.{usecs:03} ms"),
            Some(detail),
        )?;
    }
    Ok(())
}

/// Wake `procno` (success case only) and unlink it from the wait queue.
/// SAFETY contract: the lock's partition LWLock held exclusive.
pub unsafe fn ProcWakeup(procno: ProcNumber, waitStatus: ProcWaitStatus) {
    let proc = lmgr_proc::GetPGProcByNumber(procno);
    if proc.links.get().is_detached() {
        return;
    }
    debug_assert_eq!(proc.waitStatus.load(Relaxed), PROC_WAIT_STATUS_WAITING);

    let lock = proc.waitLock.get();
    wq_delete(lock, procno);

    proc.waitLock.set(std::ptr::null_mut());
    proc.waitProcLock.set(std::ptr::null_mut());
    proc.waitStatus.store(waitStatus, Release);
    // C clears MyProc's waitStart here (not proc's); transcribed faithfully.
    lmgr_proc::GetPGProcByNumber(my_procno()).waitStart.write(0);

    latch_seams::set_latch::call(&proc.procLatch);
}

/// Wake all waiters that are no longer blocked.
/// SAFETY contract: the lock's partition LWLock held exclusive.
pub unsafe fn ProcLockWakeup(lockMethodTable: LockMethod, lock: *mut LOCK) {
    if (*lock).waitProcs.count == 0 {
        return;
    }
    let mut ahead_requests = 0;
    wq_foreach(lock, |waiter_no| {
        let waiter = lmgr_proc::GetPGProcByNumber(waiter_no);
        let lockmode = waiter.waitLockMode.get();
        let proclock: *mut PROCLOCK = waiter.waitProcLock.get();

        // Waken if it conflicts with neither earlier waiters' requests nor
        // already-held locks.
        if lockMethodTable.conflictTab[lockmode as usize] & ahead_requests == 0
            && !lock_check_conflicts_raw(lockMethodTable, lockmode, lock, proclock, waiter_no)
        {
            crate::shared::grant_lock_raw(lock, proclock, lockmode);
            ProcWakeup(waiter_no, PROC_WAIT_STATUS_OK);
        } else {
            ahead_requests |= LOCKBIT_ON(lockmode);
        }
        true
    });
}

/// Remove a proc that failed to get the lock from the wait queue it is on.
/// The partition lock for `hashcode` must be held by the caller.
pub fn RemoveFromWaitQueue(procno: ProcNumber, hashcode: u32) {
    let proc = lmgr_proc::GetPGProcByNumber(procno);
    // SAFETY: caller holds the partition lock (C contract).
    unsafe {
        let wait_lock: *mut LOCK = proc.waitLock.get();
        let proclock: *mut PROCLOCK = proc.waitProcLock.get();
        let lockmode = proc.waitLockMode.get();
        let lock_method = crate::GetLocksMethodTable(&*wait_lock);

        debug_assert_eq!(proc.waitStatus.load(Relaxed), PROC_WAIT_STATUS_WAITING);
        debug_assert!(!proc.links.get().is_detached());
        debug_assert!(!wait_lock.is_null());
        debug_assert!((*wait_lock).waitProcs.count > 0);

        wq_delete(wait_lock, procno);

        debug_assert!((*wait_lock).nRequested > (*wait_lock).nGranted);
        (*wait_lock).nRequested -= 1;
        debug_assert!((*wait_lock).requested[lockmode as usize] > 0);
        (*wait_lock).requested[lockmode as usize] -= 1;
        if (*wait_lock).granted[lockmode as usize] == (*wait_lock).requested[lockmode as usize] {
            (*wait_lock).waitMask &= LOCKBIT_OFF(lockmode);
        }

        proc.waitLock.set(std::ptr::null_mut());
        proc.waitProcLock.set(std::ptr::null_mut());
        proc.waitStatus.store(PROC_WAIT_STATUS_ERROR, Release);

        // Delete the proclock now if it holds nothing, and wake any newly
        // unblocked waiters.
        CleanUpLock(wait_lock, proclock, lock_method, hashcode, true)
            .expect("CleanUpLock failed in RemoveFromWaitQueue");
    }
}

/// DEADLOCK_TIMEOUT expiry: check for a deadlock and, if hard, boot ourselves
/// off the lock.
pub fn CheckDeadLock() -> PgResult<()> {
    let procno = my_procno();
    for i in 0..NUM_LOCK_PARTITIONS as usize {
        lwlock::LWLockAcquire(LockHashPartitionLockByIndex(i), lwlock::LW_EXCLUSIVE, procno)?;
    }

    let proc = lmgr_proc::GetPGProcByNumber(procno);
    // Awoken in the interim? (Unlinked from the queue == granted.)
    if !proc.links.get().is_detached() {
        let state = deadlock_seams::dead_lock_check::call(procno);
        lmgr_proc::SetDeadlockState(state);

        if state == DeadLockState::HardDeadLock {
            // SAFETY: all partition locks held exclusive.
            unsafe {
                let wait_lock = proc.waitLock.get();
                debug_assert!(!wait_lock.is_null());
                RemoveFromWaitQueue(procno, LockTagHashCode(&(*wait_lock).tag));
            }
        }
    }

    for i in (0..NUM_LOCK_PARTITIONS as usize).rev() {
        lwlock::LWLockRelease(LockHashPartitionLockByIndex(i))?;
    }
    Ok(())
}

/// PIDs of holders and waiters of the awaited lock, for wait logging.
/// The lock's partition lock must be held.
pub fn GetLockHoldersAndWaiters(localtag: &LOCALLOCKTAG, hashcode: u32) -> (String, String, i32) {
    debug_assert!(lwlock::LWLockHeldByMe(LockHashPartitionLock(hashcode)));
    let lock = with_local(|state| {
        state
            .table
            .get(localtag)
            .expect("missing LOCALLOCK")
            .lock
    });
    let mut holders = String::new();
    let mut waiters = String::new();
    let mut holders_num = 0;

    // SAFETY: partition lock held (asserted above).
    unsafe {
        crate::shared::foreach_proclock_on_lock(lock, |proclock| {
            let owner = lmgr_proc::GetPGProcByNumber((*proclock).tag.myProc);
            let pid = owner.pid.load(Relaxed);
            let is_waiter = owner.waitProcLock.get() == proclock;
            let buf = if is_waiter { &mut waiters } else { &mut holders };
            if !buf.is_empty() {
                buf.push_str(", ");
            }
            buf.push_str(&pid.to_string());
            if !is_waiter {
                holders_num += 1;
            }
            true
        });
    }
    (holders, waiters, holders_num)
}
