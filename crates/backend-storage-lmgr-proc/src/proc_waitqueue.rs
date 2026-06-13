//! Heavyweight-lock wait queue, sleeping, and wakeups (`storage/lmgr/proc.c`).
//!
//! When a backend cannot get a heavyweight lock immediately, `JoinWaitQueue`
//! inserts its `PGPROC` into the `LOCK`'s `waitProcs` queue at the position
//! dictated by lock-mode conflicts, then `ProcSleep` blocks on the process
//! latch until granted or a deadlock/timeout fires. `ProcWakeup` /
//! `ProcLockWakeup` move granted waiters off the queue and signal them.
//! `CheckDeadLock` runs on the deadlock timer; `LockErrorCleanup` unwinds a
//! partially-entered wait on error.
//!
//! `proc.c`'s OWN control flow — the priority-insertion decision, the
//! `ProcSleep` wait loop with its deadlock-timeout / autovac-cancel /
//! `log_lock_waits` branches, the partition-lock acquire/release ordering in
//! `CheckDeadLock`, and the queue bookkeeping in `ProcWakeup` /
//! `ProcLockWakeup` — is ported here.
//!
//! The genuinely-foreign callees are routed through their owners' per-owner
//! seam crates:
//!   * the `LOCK` / `PROCLOCK` substrate, the conflict table, the wait-queue
//!     splicing, and the holder/waiter walk (`lock.c`): the low-level slots in
//!     `backend-storage-lmgr-lock-seams`;
//!   * the deadlock checker (`deadlock.c`): `backend-storage-lmgr-deadlock-seams`;
//!   * the `PGPROC` array / `MyProc` (owned by the sibling `proc_lifecycle` /
//!     `proc_shmem` families, still unported on this branch): this unit's own
//!     inward seams in `backend-storage-lmgr-proc-seams`, which panic until
//!     `InitProcGlobal` / `InitProcess` land;
//!   * timeouts (`timeout.c`), the latch (`latch.c`), interrupt handling
//!     (`postgres.c`), recovery-conflict (`xlog.c` / `xlogrecovery.c` /
//!     `standby.c`), autovacuum cancellation (`autovacuum.c`), and the
//!     timestamp helpers (`timestamp.c`) through their respective seam crates.
//!
//! `MyProc`, the `DeadlockTimeout` / `LockTimeout` / `log_lock_waits` GUCs, and
//! the `deadlock_state` / `got_deadlock_timeout` bookkeeping are this unit's own
//! state ([`crate::globals`]).


use types_core::primitive::INVALID_PROC_NUMBER;
use types_core::ProcNumber;
use types_error::PgResult;
use types_storage::lock::{DeadLockState, DEFAULT_LOCKMETHOD, LOCKMASK, LOCKMODE, LOCKTAG};
use types_storage::storage::{
    ProcWaitStatus, NUM_LOCK_PARTITIONS, PROC_IS_AUTOVACUUM, PROC_VACUUM_FOR_WRAPAROUND,
    PROC_WAIT_STATUS_ERROR, PROC_WAIT_STATUS_OK, PROC_WAIT_STATUS_WAITING,
};

use crate::globals;
use crate::seam;

use backend_access_transam_xlog_seams as xlog;
use backend_access_transam_xlogrecovery_seams as xlogrecovery;
use backend_access_transam_xlogutils_seams as xlogutils;
use backend_storage_ipc_latch_seams as latch;
use backend_storage_ipc_standby_seams as standby;
use backend_storage_lmgr_lock_seams as lock;
use backend_storage_lmgr_lwlock_seams as lwlock;
use backend_storage_lmgr_proc_seams as proc;
use backend_tcop_postgres_seams as postgres;
use backend_utils_adt_timestamp_seams as timestamp;
use backend_utils_init_small_seams as initsmall;
use backend_utils_misc_timeout_seams as timeout;
use mcx::MemoryContext;
use types_storage::storage::ProcSignalReason;
use types_storage::lock::AccessExclusiveLock;
use types_storage::LWLockMode;
use types_timeout::{DisableTimeoutParams, EnableTimeoutParams, TimeoutId, TimeoutType};
use types_wal::xlogutils::in_hot_standby;

/// `WL_LATCH_SET | WL_EXIT_ON_PM_DEATH` and `PG_WAIT_LOCK` (latch.h / wait_event.h).
const WL_LATCH_SET: u32 = 1 << 0;
const WL_EXIT_ON_PM_DEATH: u32 = 1 << 5;
const PG_WAIT_LOCK: u32 = 0x0A00_0000;

/// `LOCKBIT_ON(lockmode)` (lock.h): `1 << lockmode`.
#[inline]
fn lockbit_on(mode: LOCKMODE) -> LOCKMASK {
    1 << mode
}

/// `LockHashPartition(hashcode)` (lock.h): the partition index of a hashcode
/// (`hashcode % NUM_LOCK_PARTITIONS`).
#[inline]
fn lock_hash_partition(hashcode: u32) -> i32 {
    (hashcode % (NUM_LOCK_PARTITIONS as u32)) as i32
}

/// `LockHashPartitionLock(hashcode)` (lock.h) — the `MainLWLockArray` offset of
/// the partition LWLock for a hashcode:
/// `LOCK_MANAGER_LWLOCK_OFFSET + LockHashPartition(hashcode)`.
#[inline]
fn lock_partition_lock_offset(hashcode: u32) -> usize {
    (types_storage::storage::LOCK_MANAGER_LWLOCK_OFFSET + lock_hash_partition(hashcode)) as usize
}

/// `LockHashPartitionLockByIndex(i)` (lock.h) — the `MainLWLockArray` offset of
/// the i-th partition LWLock.
#[inline]
fn lock_partition_lock_offset_by_index(i: i32) -> usize {
    (types_storage::storage::LOCK_MANAGER_LWLOCK_OFFSET + i) as usize
}

/// `JoinWaitQueue(locallock, lockMethodTable, dontWait)` — insert `MyProc` into
/// the lock's wait queue (or report it can be granted immediately / would
/// deadlock). The lock table's partition lock must be held at entry and is
/// still held at exit. Returns the resulting wait status.
///
/// `_lockMethodTable` is named in the signature for fidelity with the C; the
/// conflict table it points at is `lock.c`-owned and reached through the
/// `conflict_tab` / `lock_check_conflicts` seams keyed on the lock-method id in
/// the locallock's tag.
pub fn JoinWaitQueue(
    locallock: &mut types_storage::lock::LOCALLOCK,
    _lockMethodTable: &types_storage::lock::LockMethod,
    dontWait: bool,
) -> PgResult<ProcWaitStatus> {
    let lockmode = locallock.tag.mode;
    let lock_tag: LOCKTAG = locallock.tag.lock;
    let lockmethodid = lock_tag.locktag_lockmethodid;
    let _hashcode = locallock.hashcode;
    let myproc = proc::my_proc_number::call();

    // C: Assert(LWLockHeldByMeInMode(partitionLock, LW_EXCLUSIVE)) —
    // PG_USED_FOR_ASSERTS_ONLY; the held-by-me query is not part of the lwlock
    // seam surface, so the assert is dropped (the caller's contract stands).

    let leader = proc::proc_lock_group_leader::call(myproc);

    // myHeldLocks = MyProc->heldLocks = proclock->holdMask
    let my_proc_held_locks = lock::proclock_hold_mask::call(lock_tag, myproc);
    proc::set_proc_held_locks::call(myproc, my_proc_held_locks);
    let mut my_held_locks = my_proc_held_locks;

    // Group locking: include locks held by members of my locking group (the
    // seam walks lock->procLocks for PROCLOCKs whose groupLeader == leader).
    if leader != INVALID_PROC_NUMBER {
        my_held_locks |= lock::lock_group_held_locks::call(lock_tag, leader);
    }

    let mut insert_before: ProcNumber = INVALID_PROC_NUMBER;
    let mut early_deadlock = false;
    let mut grant_immediately = false;

    // Determine where to add myself in the wait queue: normally the tail, but
    // if I already hold locks that conflict with an earlier waiter, go just
    // ahead of the first such waiter (with the LockAcquire-style immediate-grant
    // special case). The scan over the lock.c-owned waitProcs queue is reclaimed
    // here, calling the conflict-table / LockCheckConflicts / GrantLock /
    // RememberSimpleDeadLock seams as it goes.
    if my_held_locks != 0 && !lock::lock_wait_queue_is_empty::call(lock_tag) {
        let waiters = lock::lock_wait_queue_waiters_snapshot::call(lock_tag);
        let mut ahead_requests: LOCKMASK = 0;
        for waiter in waiters {
            // Same locking group as this waiter: its locks neither conflict
            // with ours nor contribute to aheadRequests.
            if leader != INVALID_PROC_NUMBER
                && leader == proc::proc_lock_group_leader::call(waiter)
            {
                continue;
            }

            let waiter_wait_lock_mode = proc::proc_wait_lock_mode::call(waiter);
            let waiter_held_locks = proc::proc_held_locks::call(waiter);

            // Must he wait for me?
            if (lock::conflict_tab::call(lockmethodid, waiter_wait_lock_mode) & my_held_locks) != 0
            {
                // Must I wait for him?
                if (lock::conflict_tab::call(lockmethodid, lockmode) & waiter_held_locks) != 0 {
                    // Yes -> deadlock. Record it; clean-up happens once we're on
                    // the queue (CheckDeadLock's recovery code agrees).
                    seam::remember_simple_deadlock(myproc, lockmode, lock_tag, waiter);
                    early_deadlock = true;
                    break;
                }
                // I must go before this waiter. Check the immediate-grant case.
                if (lock::conflict_tab::call(lockmethodid, lockmode) & ahead_requests) == 0
                    && !lock::lock_check_conflicts::call(lockmethodid, lockmode, lock_tag, myproc)
                {
                    // Skip the wait and just grant myself the lock.
                    lock::grant_lock::call(lock_tag, myproc, lockmode);
                    grant_immediately = true;
                    break;
                }
                // Put myself into the wait queue before the conflicting process.
                insert_before = waiter;
                break;
            }
            // Nope, advance to the next waiter.
            ahead_requests |= lockbit_on(waiter_wait_lock_mode);
        }
    }

    if grant_immediately {
        return Ok(PROC_WAIT_STATUS_OK);
    }

    // Detected deadlock: give up without waiting. Must agree with CheckDeadLock.
    if early_deadlock {
        return Ok(PROC_WAIT_STATUS_ERROR);
    }

    // We'd really need to sleep. If commanded not to, bail out.
    if dontWait {
        return Ok(PROC_WAIT_STATUS_ERROR);
    }

    // Insert self into the queue at the position determined above.
    if insert_before != INVALID_PROC_NUMBER {
        lock::lock_wait_queue_insert_before::call(lock_tag, insert_before, myproc);
    } else {
        lock::lock_wait_queue_push_tail::call(lock_tag, myproc);
    }

    lock::lock_set_wait_mask_bit::call(lock_tag, lockmode);

    // Set up wait info in the PGPROC, too: heldLocks / waitLock / waitProcLock /
    // waitLockMode / waitStatus = WAITING.
    proc::set_proc_held_locks::call(myproc, my_proc_held_locks);
    proc::set_proc_wait_fields::call(myproc, lock_tag, myproc, lockmode);

    Ok(PROC_WAIT_STATUS_WAITING)
}

/// `ProcSleep(locallock)` — block on the process latch until the awaited lock is
/// granted, a deadlock is detected, or a timeout fires.
pub fn ProcSleep(
    locallock: &mut types_storage::lock::LOCALLOCK,
) -> PgResult<ProcWaitStatus> {
    let lockmode = locallock.tag.mode;
    let lock_tag: LOCKTAG = locallock.tag.lock;
    let lockmethodid = lock_tag.locktag_lockmethodid;
    let hashcode = locallock.hashcode;
    let myproc = proc::my_proc_number::call();
    let partition_offset = lock_partition_lock_offset(hashcode);
    let mut standby_wait_start: i64 = 0;
    let mut allow_autovacuum_cancel = true;
    let mut logged_recovery_conflict = false;

    // Transient context for ProcSleep's recovery-conflict vxid array and the
    // log/detail message buffers (the C `CurrentMemoryContext` StringInfos /
    // the GetLockConflicts result). Dropped on return.
    let msg_ctx = MemoryContext::new("ProcSleep");

    // C: Assert(GetAwaitedLock() == locallock); Assert(!LWLockHeldByMe(
    // partitionLock)) — assert-only; the caller's contract stands.

    let in_hs = in_hot_standby(xlogutils::standby_state::call());

    // Buffer-pin deadlock against the Startup process (Hot Standby, not Startup).
    if xlog::recovery_in_progress::call() && !xlogrecovery::in_recovery::call() {
        standby::check_recovery_conflict_deadlock::call()?;
    }

    // Reset deadlock_state before enabling the timeout handler.
    globals::set_deadlock_state(DeadLockState::NotYetChecked);
    globals::set_got_deadlock_timeout(false);

    let deadlock_timeout = globals::deadlock_timeout();
    let lock_timeout = globals::lock_timeout();

    // Set the deadlock (and, if set, lock) timeout; reuse the deadlock timer's
    // start time as waitStart to avoid an extra clock read.
    if !in_hs {
        if lock_timeout > 0 {
            let timeouts = [
                EnableTimeoutParams {
                    id: TimeoutId::DEADLOCK_TIMEOUT,
                    r#type: TimeoutType::TMPARAM_AFTER,
                    delay_ms: deadlock_timeout,
                    fin_time: 0,
                },
                EnableTimeoutParams {
                    id: TimeoutId::LOCK_TIMEOUT,
                    r#type: TimeoutType::TMPARAM_AFTER,
                    delay_ms: lock_timeout,
                    fin_time: 0,
                },
            ];
            timeout::enable_timeouts::call(&timeouts)?;
        } else {
            timeout::enable_timeout_after::call(TimeoutId::DEADLOCK_TIMEOUT, deadlock_timeout)?;
        }

        let start = timeout::get_timeout_start_time::call(TimeoutId::DEADLOCK_TIMEOUT);
        proc::set_proc_wait_start::call(myproc, start as u64);
    } else if xlog::log_recovery_conflict_waits::call() {
        // Set the wait start timestamp if logging is enabled and in hot standby.
        standby_wait_start = timestamp::get_current_timestamp::call();
    }

    let mut my_wait_status;
    loop {
        if in_hs {
            let maybe_log_conflict = standby_wait_start != 0 && !logged_recovery_conflict;

            // Set a timer and wait for that or for the lock to be granted.
            standby::resolve_recovery_conflict_with_lock::call(msg_ctx.mcx(), lock_tag, maybe_log_conflict)?;

            // Emit the log message if the startup process waited longer than
            // deadlock_timeout for recovery conflict on lock.
            if maybe_log_conflict {
                let now = timestamp::get_current_timestamp::call();
                if timestamp_difference_exceeds(standby_wait_start, now, deadlock_timeout) {
                    // Gather the conflicting backends and log the recovery conflict
                    // (LogRecoveryConflict(PROCSIG_RECOVERY_CONFLICT_LOCK,
                    // standbyWaitStart, now, cnt > 0 ? vxids : NULL, true)).
                    let vxids = lock::get_lock_conflicts::call(
                        msg_ctx.mcx(),
                        &lock_tag,
                        AccessExclusiveLock,
                    )?;
                    let wait_list = if vxids.is_empty() {
                        None
                    } else {
                        Some(vxids.as_slice())
                    };
                    standby::log_recovery_conflict::call(
                        msg_ctx.mcx(),
                        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOCK,
                        standby_wait_start,
                        now,
                        wait_list,
                        true,
                    )?;
                    logged_recovery_conflict = true;
                }
            }
        } else {
            latch::wait_latch_my_latch::call(
                WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
                0,
                PG_WAIT_LOCK | lock_tag.locktag_type as u32,
            )?;
            latch::reset_latch_my_latch::call();
            // Check for deadlocks first, as that's probably log-worthy.
            if globals::got_deadlock_timeout() {
                CheckDeadLock()?;
                globals::set_got_deadlock_timeout(false);
            }
            postgres::check_for_interrupts::call()?;
        }

        // waitStatus can change asynchronously; read it once per loop.
        my_wait_status = proc::proc_wait_status::call(myproc);

        // Not deadlocked but waiting on an autovacuum-induced task: signal it.
        if globals::deadlock_state() == DeadLockState::BlockedByAutoVacuum && allow_autovacuum_cancel
        {
            let autovac_proc = seam::get_blocking_autovacuum_pgproc();

            // Grab info under ProcArrayLock, then release immediately.
            lwlock::lwlock_acquire_proc_array::call(LWLockMode::LW_EXCLUSIVE)?;
            let pgxactoff = proc::proc_pgxactoff::call(autovac_proc);
            let status_flags = proc::proc_global_status_flags::call(pgxactoff);
            let lockmethod_copy = lockmethodid;
            let locktag_copy = lock_tag;
            lwlock::lwlock_release_proc_array::call()?;

            // Only if the worker is not protecting against Xid wraparound.
            if (status_flags & PROC_IS_AUTOVACUUM) != 0
                && (status_flags & PROC_VACUUM_FOR_WRAPAROUND) == 0
            {
                let pid = proc::proc_pid::call(autovac_proc);

                // Report the case, if configured to do so (DEBUG1).
                if backend_utils_error::message_level_is_interesting(types_error::DEBUG1) {
                    let locktagbuf = lock::describe_lock_tag::call(locktag_copy);
                    let modename =
                        lock::get_lockmode_name::call(lockmethod_copy as u16, lockmode);
                    let logbuf = format!(
                        "Process {} waits for {} on {}.",
                        initsmall::my_proc_pid::call(),
                        modename,
                        locktagbuf
                    );
                    postgres::report_autovac_cancel::call(pid, logbuf)?;
                }

                // Send the autovacuum worker Back to Old Kent Road. The race
                // (the worker exits before the kill) is handled inside the seam:
                // ESRCH is ignored, other errnos warn.
                postgres::signal_autovacuum_worker::call(pid)?;
            }

            // Prevent the signal from being sent again more than once.
            allow_autovacuum_cancel = false;
        }

        // If awoken after the deadlock check ran and log_lock_waits is on, report.
        let mut dlstate = globals::deadlock_state();
        if globals::log_lock_waits() && dlstate != DeadLockState::NotYetChecked {
            let buf = lock::describe_lock_tag::call(lock_tag);
            let modename = lock::get_lockmode_name::call(lockmethodid as u16, lockmode);
            let start = timeout::get_timeout_start_time::call(TimeoutId::DEADLOCK_TIMEOUT);
            let (secs, mut usecs) = timestamp_difference(start, timestamp::get_current_timestamp::call());
            let msecs = secs * 1000 + (usecs as i64) / 1000;
            usecs %= 1000;

            // Gather all lock holders and waiters under the partition lock (SHARED).
            let _part_guard = lwlock::lwlock_acquire_main::call(partition_offset, LWLockMode::LW_SHARED)?;
            let hw = lock::get_lock_holders_and_waiters::call(lock_tag);
            _part_guard.release()?;

            let detail_singular = format!(
                "Process holding the lock: {}. Wait queue: {}.",
                hw.holders,
                hw.waiters
            );
            let detail_plural = format!(
                "Processes holding the lock: {}. Wait queue: {}.",
                hw.holders,
                hw.waiters
            );
            let holders_num = hw.holders_num;
            let mypid = initsmall::my_proc_pid::call();

            if dlstate == DeadLockState::SoftDeadLock {
                postgres::report_lock_wait_log::call(
                    format!(
                        "process {mypid} avoided deadlock for {modename} on {buf} by rearranging queue order after {msecs}.{usecs:03} ms"
                    ),
                    Some(detail_singular.clone()),
                    Some(detail_plural.clone()),
                    holders_num,
                )?;
            } else if dlstate == DeadLockState::HardDeadLock {
                // Redundant with the error that follows, but the error might not
                // reach the log; ensure long-wait events are logged.
                postgres::report_lock_wait_log::call(
                    format!(
                        "process {mypid} detected deadlock while waiting for {modename} on {buf} after {msecs}.{usecs:03} ms"
                    ),
                    Some(detail_singular.clone()),
                    Some(detail_plural.clone()),
                    holders_num,
                )?;
            }

            if my_wait_status == PROC_WAIT_STATUS_WAITING {
                postgres::report_lock_wait_log::call(
                    format!(
                        "process {mypid} still waiting for {modename} on {buf} after {msecs}.{usecs:03} ms"
                    ),
                    Some(detail_singular.clone()),
                    Some(detail_plural.clone()),
                    holders_num,
                )?;
            } else if my_wait_status == PROC_WAIT_STATUS_OK {
                postgres::report_lock_wait_log::call(
                    format!(
                        "process {mypid} acquired {modename} on {buf} after {msecs}.{usecs:03} ms"
                    ),
                    None,
                    None,
                    holders_num,
                )?;
            } else {
                debug_assert_eq!(my_wait_status, PROC_WAIT_STATUS_ERROR);
                // The deadlock checker always kicks its own process, so ERROR
                // only co-occurs with HardDeadLock; print otherwise for
                // completeness if someone else kicked us off the lock.
                if dlstate != DeadLockState::HardDeadLock {
                    postgres::report_lock_wait_log::call(
                        format!(
                            "process {mypid} failed to acquire {modename} on {buf} after {msecs}.{usecs:03} ms"
                        ),
                        Some(detail_singular.clone()),
                        Some(detail_plural.clone()),
                        holders_num,
                    )?;
                }
            }

            // We might still need to wait; reset so we don't print again.
            dlstate = DeadLockState::NoDeadLock;
            globals::set_deadlock_state(dlstate);
        }

        if my_wait_status != PROC_WAIT_STATUS_WAITING {
            break;
        }
    }

    // Disable the timers if still running (preserve the LOCK_TIMEOUT indicator).
    if !in_hs {
        if lock_timeout > 0 {
            let timeouts = [
                DisableTimeoutParams {
                    id: TimeoutId::DEADLOCK_TIMEOUT,
                    keep_indicator: false,
                },
                DisableTimeoutParams {
                    id: TimeoutId::LOCK_TIMEOUT,
                    keep_indicator: true,
                },
            ];
            timeout::disable_timeouts::call(&timeouts)?;
        } else {
            timeout::disable_timeout::call(TimeoutId::DEADLOCK_TIMEOUT, false);
        }
    }

    // Emit the recovery-conflict resolution log if Startup waited longer than
    // deadlock_timeout for it. C: LogRecoveryConflict(PROCSIG_RECOVERY_CONFLICT_LOCK,
    // standbyWaitStart, GetCurrentTimestamp(), NULL, false).
    if in_hs && logged_recovery_conflict {
        standby::log_recovery_conflict::call(
            msg_ctx.mcx(),
            ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOCK,
            standby_wait_start,
            timestamp::get_current_timestamp::call(),
            None,
            false,
        )?;
    }

    // The awaker did all the lock-table / MyProc updates; nothing else to do.
    drop(msg_ctx);
    Ok(my_wait_status)
}

/// `ProcWakeup(proc, waitStatus)` — remove `proc` from its wait queue, stamp its
/// final status, and signal it. The partition lock must be held by the caller.
pub fn ProcWakeup(
    proc_arg: &mut types_storage::storage::PGPROC,
    waitStatus: ProcWaitStatus,
) {
    // GetNumberFromPGProc(proc): the proc's slot in ProcGlobal->allProcs.
    let procno = proc::pgproc_number::call(proc_arg);
    if proc::proc_wait_link_is_detached::call(procno) {
        return;
    }

    debug_assert_eq!(
        proc::proc_wait_status::call(procno),
        PROC_WAIT_STATUS_WAITING
    );

    // Remove the process from its lock's wait queue
    // (dclist_delete_from_thoroughly(&proc->waitLock->waitProcs, &proc->links)).
    lock::lock_wait_queue_delete::call(procno);

    // Clean up the proc's state and pass it the ok/fail signal: waitLock /
    // waitProcLock cleared, waitStatus set, the MyProc->waitStart = 0 quirk.
    proc::wakeup_proc_clear_wait::call(procno, waitStatus);

    // And awaken it.
    proc::set_proc_latch::call(procno);
}

/// `ProcLockWakeup(lockMethodTable, lock)` — wake every waiter on `lock` that can
/// now be granted. The partition lock must be held by the caller.
pub fn ProcLockWakeup(
    _lockMethodTable: &types_storage::lock::LockMethod,
    lock_arg: &mut types_storage::lock::LOCK,
) {
    let lock_tag: LOCKTAG = lock_arg.tag;
    let lockmethodid = lock_tag.locktag_lockmethodid;
    let mut ahead_requests: LOCKMASK = 0;

    if lock::lock_wait_queue_is_empty::call(lock_tag) {
        return;
    }

    // Walk the waiters front-to-back (dclist_foreach_modify); the queue is
    // lock.c-owned, so the iteration is an ordered snapshot of waiter slots.
    let waiters = lock::lock_wait_queue_waiters_snapshot::call(lock_tag);
    for waiter in waiters {
        let lockmode = proc::proc_wait_lock_mode::call(waiter);

        // Waken if it doesn't conflict with earlier waiters nor already-held locks.
        if (lock::conflict_tab::call(lockmethodid, lockmode) & ahead_requests) == 0
            && !lock::lock_check_conflicts::call(lockmethodid, lockmode, lock_tag, waiter)
        {
            // OK to waken.
            lock::grant_lock::call(lock_tag, waiter, lockmode);
            // Removes proc from the lock's waiting process queue.
            wakeup_waiter(waiter, PROC_WAIT_STATUS_OK);
        } else {
            // Conflicts: don't wake, but remember the requested mode for later.
            ahead_requests |= lockbit_on(lockmode);
        }
    }
}

/// `ProcWakeup` for a waiter identified by `ProcNumber` (the internal form used
/// by `ProcLockWakeup`, whose snapshot yields slot numbers).
fn wakeup_waiter(procno: ProcNumber, wait_status: ProcWaitStatus) {
    if proc::proc_wait_link_is_detached::call(procno) {
        return;
    }
    debug_assert_eq!(
        proc::proc_wait_status::call(procno),
        PROC_WAIT_STATUS_WAITING
    );
    lock::lock_wait_queue_delete::call(procno);
    proc::wakeup_proc_clear_wait::call(procno, wait_status);
    proc::set_proc_latch::call(procno);
}

/// `CheckDeadLock(void)` — deadlock-timer handler: run the deadlock checker and,
/// if a cycle is found, arrange for the victim to error out.
pub fn CheckDeadLock() -> PgResult<()> {
    let myproc = proc::my_proc_number::call();

    // Acquire all partition LWLocks in partition-number order (avoid LWLock
    // deadlock). LWLockAcquire creates a critical section, so we can't be
    // interrupted by cancel/die here.
    let mut guards = Vec::with_capacity(NUM_LOCK_PARTITIONS as usize);
    for i in 0..NUM_LOCK_PARTITIONS {
        guards.push(lwlock::lwlock_acquire_main::call(
            lock_partition_lock_offset_by_index(i),
            LWLockMode::LW_EXCLUSIVE,
        )?);
    }

    // Check whether we've been awoken in the interim. We have if we've been
    // unlinked from the wait queue (safe: we hold the partition locks).
    if !proc::proc_unlinked_from_wait_queue::call(myproc) {
        // Run the deadlock check; set deadlock_state for ProcSleep.
        let state = seam::deadlock_check(myproc);
        globals::set_deadlock_state(state);

        if state == DeadLockState::HardDeadLock {
            // Get this process out of the wait state. RemoveFromWaitQueue sets
            // MyProc->waitStatus = PROC_WAIT_STATUS_ERROR so ProcSleep reports an
            // error after we return from the signal handler.
            debug_assert!(proc::proc_is_waiting_on_lock::call(myproc));
            let tag = proc::proc_wait_lock_tag::call(myproc);
            let hashcode = lock::lock_tag_hash_code::call(tag);
            lock::remove_from_wait_queue::call(myproc, hashcode);
        }
    }

    // Release the locks in reverse order (1: anyone needing >1 lock takes them in
    // increasing order; 2: avoids O(N^2) inside LWLockRelease).
    while let Some(guard) = guards.pop() {
        guard.release()?;
    }
    Ok(())
}

/// `CheckDeadLockAlert(void)` — SIGALRM handler that sets the
/// deadlock-check-needed flag and the latch. Runs inside a signal handler.
pub fn CheckDeadLockAlert() {
    // NB: in C this saves/restores errno around the work; the latch set is the
    // only side effect with observable errno here, and the latch seam preserves
    // it, so there is nothing to save/restore in this translation.
    globals::set_got_deadlock_timeout(true);

    // Have to set the latch again, even if handle_sig_alarm already did (back
    // then got_deadlock_timeout wasn't yet set). Setting a set latch is cheap.
    latch::set_latch_my_latch::call();
}

/// `LockErrorCleanup(void)` — unwind a partially-entered lock wait when the
/// waiting backend errors out (remove `MyProc` from the wait queue, disable the
/// timers, reset wait state).
pub fn LockErrorCleanup() -> PgResult<()> {
    initsmall::hold_interrupts::call();

    lock::abort_strong_lock_acquire::call();

    // Nothing to do if we weren't waiting for a lock.
    let awaited = lock::get_awaited_lock_hashcode::call();
    if awaited < 0 {
        initsmall::resume_interrupts::call();
        return Ok(());
    }
    let hashcode = awaited as u32;

    // Turn off the deadlock and lock timeout timers, preserving the LOCK_TIMEOUT
    // indicator (this runs before ProcessInterrupts on SIGINT; else we'd lose
    // the knowledge that the SIGINT came from a lock timeout).
    let timeouts = [
        DisableTimeoutParams {
            id: TimeoutId::DEADLOCK_TIMEOUT,
            keep_indicator: false,
        },
        DisableTimeoutParams {
            id: TimeoutId::LOCK_TIMEOUT,
            keep_indicator: true,
        },
    ];
    timeout::disable_timeouts::call(&timeouts)?;

    // Unlink myself from the wait queue, if still on it (might not be anymore!).
    let myproc = proc::my_proc_number::call();
    let part_guard =
        lwlock::lwlock_acquire_main::call(lock_partition_lock_offset(hashcode), LWLockMode::LW_EXCLUSIVE)?;

    if !proc::proc_wait_link_is_detached::call(myproc) {
        // We could not have been granted the lock yet.
        lock::remove_from_wait_queue::call(myproc, hashcode);
    } else {
        // Somebody kicked us off the lock queue already. If they granted us the
        // lock, remember it in our local lock table.
        if proc::proc_wait_status::call(myproc) == PROC_WAIT_STATUS_OK {
            lock::grant_awaited_lock::call();
        }
    }

    lock::reset_awaited_lock::call();

    part_guard.release()?;

    initsmall::resume_interrupts::call();
    Ok(())
}

/// `ProcReleaseLocks(bool isCommit)` — release all locks at transaction end
/// (`LockReleaseAll` for the default lock method, then for user/advisory locks).
pub fn ProcReleaseLocks(isCommit: bool) -> PgResult<()> {
    if proc::my_proc_number::call() == INVALID_PROC_NUMBER {
        return Ok(());
    }
    // If waiting, get off the wait queue (should only be needed after error).
    LockErrorCleanup()?;
    // Release standard locks, including session-level if aborting.
    lock::lock_release_all::call(DEFAULT_LOCKMETHOD, !isCommit)?;
    // Release transaction-level advisory locks.
    lock::lock_release_all::call(USER_LOCKMETHOD, false)?;
    Ok(())
}

/// `USER_LOCKMETHOD` (lock.h) — the lock-method id for advisory locks.
const USER_LOCKMETHOD: u8 = 2;

/// `GetLockHoldersAndWaiters(locallock, lock_holders_sbuf, lock_waiters_sbuf,
/// lockHoldersNum)` — build the human-readable holder/waiter PID lists for a
/// lock-wait log message. The partition lock must be held on entry and exit.
pub fn GetLockHoldersAndWaiters(
    locallock: &types_storage::lock::LOCALLOCK,
    lock_holders_sbuf: &mut types_stringinfo::StringInfo<'_>,
    lock_waiters_sbuf: &mut types_stringinfo::StringInfo<'_>,
    lockHoldersNum: &mut i32,
) -> PgResult<()> {
    let lock_tag: LOCKTAG = locallock.tag.lock;
    let _ = locallock.hashcode;

    // C: Assert(LWLockHeldByMe(partitionLock)) — assert-only.

    *lockHoldersNum = 0;

    // The walk over lock->procLocks is lock.c-owned data; the seam produces the
    // holder/waiter PID strings and the holder count by classifying each
    // PROCLOCK (waiter iff myProc->waitProcLock == curproclock, else holder).
    // C appends each PID into the caller's StringInfo as it walks; here the seam
    // returns the assembled comma-joined lists, which are appended to the
    // caller's buffers (appendStringInfoString analog).
    let hw = lock::get_lock_holders_and_waiters::call(lock_tag);
    append_str(lock_holders_sbuf, &hw.holders)?;
    append_str(lock_waiters_sbuf, &hw.waiters)?;
    *lockHoldersNum = hw.holders_num;
    Ok(())
}

/// `appendStringInfoString(sbuf, s)` — append a UTF-8 string to a `StringInfo`'s
/// byte buffer, surfacing OOM as the `Err` (the C `enlargeStringInfo`
/// `out of memory` ereport).
fn append_str(sbuf: &mut types_stringinfo::StringInfo<'_>, s: &str) -> PgResult<()> {
    let bytes = s.as_bytes();
    sbuf.data
        .try_reserve(bytes.len())
        .map_err(|_| mcx::oom_named("GetLockHoldersAndWaiters", bytes.len()))?;
    sbuf.data.extend_from_slice(bytes);
    Ok(())
}

// --- helpers ---------------------------------------------------------------

/// `TimestampDifferenceExceeds(start, stop, msec)` (timestamp.c): does
/// `stop - start` microseconds exceed `msec` milliseconds?
#[inline]
fn timestamp_difference_exceeds(start: i64, stop: i64, msec: i32) -> bool {
    (stop - start) >= (msec as i64) * 1000
}

/// `TimestampDifference(start, stop, &secs, &usecs)` (timestamp.c) — the
/// difference as `(secs, microsecs)`; both zero when `stop <= start`.
#[inline]
fn timestamp_difference(start: i64, stop: i64) -> (i64, i32) {
    const USECS_PER_SEC: i64 = 1_000_000;
    let diff = stop - start;
    if diff <= 0 {
        (0, 0)
    } else {
        (diff / USECS_PER_SEC, (diff % USECS_PER_SEC) as i32)
    }
}
