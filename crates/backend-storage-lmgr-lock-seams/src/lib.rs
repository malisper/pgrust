//! Seam declarations for the `backend-storage-lmgr-lock` unit
//! (`storage/lmgr/lock.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

extern crate alloc;

seam_core::seam!(
    /// `lock_twophase_recover(xid, info, recdata, len)` — re-acquire a prepared
    /// transaction's locks at recovery startup (slot `TWOPHASE_RM_LOCK_ID` of
    /// `twophase_recover_callbacks`).
    pub fn lock_twophase_recover(
        xid: types_core::primitive::TransactionId,
        info: u16,
        recdata: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `lock_twophase_postcommit(xid, info, recdata, len)` — release a prepared
    /// transaction's locks on COMMIT PREPARED (slot `TWOPHASE_RM_LOCK_ID` of
    /// `twophase_postcommit_callbacks`).
    pub fn lock_twophase_postcommit(
        xid: types_core::primitive::TransactionId,
        info: u16,
        recdata: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `lock_twophase_postabort(xid, info, recdata, len)` — release a prepared
    /// transaction's locks on ROLLBACK PREPARED (slot `TWOPHASE_RM_LOCK_ID` of
    /// `twophase_postabort_callbacks`).
    pub fn lock_twophase_postabort(
        xid: types_core::primitive::TransactionId,
        info: u16,
        recdata: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `lock_twophase_standby_recover(xid, info, recdata, len)` — acquire a
    /// prepared transaction's AccessExclusiveLocks at hot-standby startup
    /// (slot `TWOPHASE_RM_LOCK_ID` of `twophase_standby_recover_callbacks`).
    pub fn lock_twophase_standby_recover(
        xid: types_core::primitive::TransactionId,
        info: u16,
        recdata: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `LockAcquire(locktag, lockmode, sessionLock, dontWait)` (lock.c) —
    /// the raw slot the lmgr-lock owner installs. `Err` carries the C
    /// `ereport(ERROR)` surface (deadlock detected, out of shared memory,
    /// unrecognized lock mode, ...).
    ///
    /// Consumers must not call this directly: use [`lock_acquire`], which
    /// wraps the held lock in a [`LockGuard`] so it cannot leak across a
    /// `?` / early return.
    pub fn lock_acquire_impl(
        locktag: &types_storage::lock::LOCKTAG,
        lockmode: types_storage::lock::LOCKMODE,
        session_lock: bool,
        dont_wait: bool,
    ) -> types_error::PgResult<types_storage::lock::LockAcquireResult>
);

seam_core::seam!(
    /// `LockRelease(locktag, lockmode, sessionLock)` (lock.c) — the raw slot
    /// the lmgr-lock owner installs: returns whether the lock was held and
    /// released; `Err` carries the C `elog(ERROR, "unrecognized lock mode")`
    /// / lock-table corruption errors (the "you don't own a lock of type"
    /// path is a WARNING and `false` in C, preserved as `Ok(false)`).
    ///
    /// Release authority lives in [`LockGuard`] ([`LockGuard::release`] /
    /// `Drop`), never in consumer hands — consumers must not call this
    /// directly.
    pub fn lock_release_impl(
        locktag: &types_storage::lock::LOCKTAG,
        lockmode: types_storage::lock::LOCKMODE,
        session_lock: bool,
    ) -> types_error::PgResult<bool>
);

/// A held heavyweight lock. Returned by [`lock_acquire`]; releasing is
/// dropping the guard (the silent abort/`?` path) or calling
/// [`LockGuard::release`] where the C code releases explicitly and its
/// `elog(ERROR)` surface must propagate.
#[derive(Debug)]
pub struct LockGuard {
    tag: types_storage::lock::LOCKTAG,
    mode: types_storage::lock::LOCKMODE,
    session_lock: bool,
    result: types_storage::lock::LockAcquireResult,
    held: bool,
}

impl LockGuard {
    /// The `LockAcquireResult` the acquisition returned
    /// (`LOCKACQUIRE_NOT_AVAIL` only when `dont_wait` was set; the guard
    /// then holds nothing and releasing is a no-op).
    pub fn result(&self) -> types_storage::lock::LockAcquireResult {
        self.result
    }

    /// Explicit release (`LockRelease`), propagating the C error surface.
    pub fn release(mut self) -> types_error::PgResult<bool> {
        if !self.held {
            return Ok(false);
        }
        self.held = false;
        lock_release_impl::call(&self.tag, self.mode, self.session_lock)
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if self.held {
            // The abort path: release is silent. The only Err surface here
            // ("unrecognized lock mode" / lock-table corruption) cannot arise
            // from a guard whose tag/mode were accepted at acquire time.
            let _ = lock_release_impl::call(&self.tag, self.mode, self.session_lock);
        }
    }
}

/// `LockAcquire(locktag, lockmode, sessionLock, dontWait)` returning the
/// held lock as a [`LockGuard`]. When `dont_wait` is set and the lock is not
/// available (`LOCKACQUIRE_NOT_AVAIL`), the returned guard holds nothing —
/// check [`LockGuard::result`].
pub fn lock_acquire(
    locktag: &types_storage::lock::LOCKTAG,
    lockmode: types_storage::lock::LOCKMODE,
    session_lock: bool,
    dont_wait: bool,
) -> types_error::PgResult<LockGuard> {
    let result = lock_acquire_impl::call(locktag, lockmode, session_lock, dont_wait)?;
    Ok(LockGuard {
        tag: *locktag,
        mode: lockmode,
        session_lock,
        result,
        held: result != types_storage::lock::LOCKACQUIRE_NOT_AVAIL,
    })
}

seam_core::seam!(
    /// `LockAcquire(locktag, lockmode, sessionLock, dontWait)` — can
    /// `ereport(ERROR)` (out of shared memory, deadlock).
    pub fn lock_acquire(
        locktag: &types_storage::LOCKTAG,
        lockmode: types_storage::LOCKMODE,
        session_lock: bool,
        dont_wait: bool,
    ) -> types_error::PgResult<types_storage::LockAcquireResult>
);

seam_core::seam!(
    /// `LockRelease(locktag, lockmode, sessionLock)` — false (with a WARNING)
    /// when the lock was not held.
    pub fn lock_release(
        locktag: &types_storage::LOCKTAG,
        lockmode: types_storage::LOCKMODE,
        session_lock: bool,
    ) -> bool
);

seam_core::seam!(
    /// `GetLockConflicts(locktag, lockmode, countp)` — VXIDs of transactions
    /// holding conflicting locks; the C terminator is dropped and `countp`
    /// folds into the length. The result array is allocated in `mcx` (C
    /// reuses a TopMemoryContext-static array; the owner copies into the
    /// caller's context instead).
    pub fn get_lock_conflicts<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        locktag: &types_storage::LOCKTAG,
        lockmode: types_storage::LOCKMODE,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, types_storage::VirtualTransactionId>>
);

seam_core::seam!(
    /// `GetRunningTransactionLocks(*nlocks)` — every held AccessExclusiveLock
    /// with an assigned xid, for snapshot logging. C pallocs the array in the
    /// caller's context (the caller pfrees it), so the seam takes the target
    /// context.
    pub fn get_running_transaction_locks<'mcx>(
        mcx: mcx::Mcx<'mcx>,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, types_storage::xl_standby_lock>>
);

seam_core::seam!(
    /// `VirtualXactLock(vxid, wait)` — true if the vxid has ended (or its
    /// lock was acquired); false when `wait == false` and it is still around.
    pub fn virtual_xact_lock(
        vxid: types_storage::VirtualTransactionId,
        wait: bool,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `VirtualXactLockTableInsert(vxid)`.
    pub fn virtual_xact_lock_table_insert(
        vxid: types_storage::VirtualTransactionId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `VirtualXactLockTableCleanup()`.
    pub fn virtual_xact_lock_table_cleanup() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `AtPrepare_Locks()` — collect lock data for the 2PC state file;
    /// errors out for cases 2PC cannot handle (e.g. session locks).
    pub fn at_prepare_locks() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `PostPrepare_Locks(xid)` — transfer the prepared transaction's locks
    /// to a dummy PGPROC.
    pub fn post_prepare_locks(
        xid: types_core::primitive::TransactionId,
    ) -> types_error::PgResult<()>
);

// --- low-level lock.c slots used by proc.c's wait-queue machinery ----------
//
// proc.c (JoinWaitQueue / ProcSleep / ProcWakeup / ProcLockWakeup /
// CheckDeadLock / LockErrorCleanup / GetLockHoldersAndWaiters) reaches into the
// shmem LOCK / PROCLOCK hash tables and the per-mode conflict table, all owned
// by lock.c.  A LOCK is identified by its LOCKTAG, a PROCLOCK holder by the
// owning backend's ProcNumber; the queue / list iteration is lock.c-owned data
// so the owner provides it as snapshots / decisions.

seam_core::seam!(
    /// `lockMethodTable->conflictTab[mode]` — the conflict bitmask for a lock
    /// mode under a given lock method.
    pub fn conflict_tab(
        lockmethodid: u8,
        mode: types_storage::lock::LOCKMODE,
    ) -> types_storage::lock::LOCKMASK
);

seam_core::seam!(
    /// `LockCheckConflicts(lockMethodTable, lockmode, lock, proclock)` — does
    /// the requested mode conflict with already-granted locks (excluding the
    /// requester's own holdings)?
    pub fn lock_check_conflicts(
        lockmethodid: u8,
        lockmode: types_storage::lock::LOCKMODE,
        lock: types_storage::lock::LOCKTAG,
        proclock_holder: types_core::ProcNumber,
    ) -> bool
);

seam_core::seam!(
    /// `GrantLock(lock, proclock, lockmode)` — record an immediate grant of
    /// `lockmode` to the holder's PROCLOCK on `lock`.
    pub fn grant_lock(
        lock: types_storage::lock::LOCKTAG,
        proclock_holder: types_core::ProcNumber,
        lockmode: types_storage::lock::LOCKMODE,
    )
);

seam_core::seam!(
    /// `RemoveFromWaitQueue(proc, hashcode)` — pull a proc off the lock's wait
    /// queue and clean up its accounting; sets the proc's waitStatus to
    /// `PROC_WAIT_STATUS_ERROR`.
    pub fn remove_from_wait_queue(
        procno: types_core::ProcNumber,
        hashcode: u32,
    )
);

seam_core::seam!(
    /// `GetLockmodeName(lockmethodid, mode)` — the human-readable lock mode
    /// name.
    pub fn get_lockmode_name(
        lockmethodid: u8,
        mode: types_storage::lock::LOCKMODE,
    ) -> alloc::string::String
);

seam_core::seam!(
    /// `LockReleaseAll(lockmethodid, allLocks)` — release all of this backend's
    /// locks for the given method.
    pub fn lock_release_all(
        lockmethodid: u8,
        all_locks: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `LockTagHashCode(locktag)` — the hash value of a LOCKTAG.
    pub fn lock_tag_hash_code(
        locktag: types_storage::lock::LOCKTAG,
    ) -> u32
);

seam_core::seam!(
    /// `AbortStrongLockAcquire()` — revert a strong-lock-count acquisition for
    /// a lock being acquired (called from LockErrorCleanup).
    pub fn abort_strong_lock_acquire()
);

seam_core::seam!(
    /// `GetAwaitedLock()` — the hashcode of the LOCALLOCK this backend is
    /// currently waiting on, or `-1` (`lockAwaited == NULL`).
    pub fn get_awaited_lock_hashcode() -> i64
);

seam_core::seam!(
    /// `GrantAwaitedLock()` — record in the local lock table that the awaited
    /// lock was granted.
    pub fn grant_awaited_lock()
);

seam_core::seam!(
    /// `ResetAwaitedLock()` — clear the `lockAwaited` pointer.
    pub fn reset_awaited_lock()
);

seam_core::seam!(
    /// `lock->procLocks` group-locking scan: OR together the holdMask of every
    /// PROCLOCK on `lock` whose groupLeader is `leader` (JoinWaitQueue's
    /// `myHeldLocks` augmentation).
    pub fn lock_group_held_locks(
        lock: types_storage::lock::LOCKTAG,
        leader: types_core::ProcNumber,
    ) -> types_storage::lock::LOCKMASK
);

seam_core::seam!(
    /// `proclock->holdMask` for the PROCLOCK held by `holder` on `lock`.
    pub fn proclock_hold_mask(
        lock: types_storage::lock::LOCKTAG,
        holder: types_core::ProcNumber,
    ) -> types_storage::lock::LOCKMASK
);

seam_core::seam!(
    /// `dclist_is_empty(&lock->waitProcs)`.
    pub fn lock_wait_queue_is_empty(lock: types_storage::lock::LOCKTAG) -> bool
);

seam_core::seam!(
    /// `dclist_insert_before(&lock->waitProcs, &insert_before->links,
    /// &MyProc->links)`.
    pub fn lock_wait_queue_insert_before(
        lock: types_storage::lock::LOCKTAG,
        insert_before: types_core::ProcNumber,
        myproc: types_core::ProcNumber,
    )
);

seam_core::seam!(
    /// `dclist_push_tail(&lock->waitProcs, &MyProc->links)`.
    pub fn lock_wait_queue_push_tail(
        lock: types_storage::lock::LOCKTAG,
        myproc: types_core::ProcNumber,
    )
);

seam_core::seam!(
    /// `lock->waitMask |= LOCKBIT_ON(lockmode)`.
    pub fn lock_set_wait_mask_bit(
        lock: types_storage::lock::LOCKTAG,
        lockmode: types_storage::lock::LOCKMODE,
    )
);

seam_core::seam!(
    /// `dclist_delete_from_thoroughly(&proc->waitLock->waitProcs,
    /// &proc->links)` — remove a granted/aborted waiter from its lock's queue.
    pub fn lock_wait_queue_delete(procno: types_core::ProcNumber)
);

seam_core::seam!(
    /// A front-to-back snapshot of the ProcNumbers in `lock->waitProcs`, for
    /// `ProcLockWakeup`'s `dclist_foreach_modify`.
    pub fn lock_wait_queue_waiters_snapshot(
        lock: types_storage::lock::LOCKTAG,
    ) -> alloc::vec::Vec<types_core::ProcNumber>
);

/// PID lists + holder count built from `lock->procLocks` by
/// [`get_lock_holders_and_waiters`].
#[derive(Clone, Debug, Default)]
pub struct LockHoldersAndWaiters {
    /// Comma-separated PIDs of processes holding the lock.
    pub holders: alloc::string::String,
    /// Comma-separated PIDs of processes waiting for the lock.
    pub waiters: alloc::string::String,
    /// Number of lock holders.
    pub holders_num: i32,
}

seam_core::seam!(
    /// `GetLockHoldersAndWaiters` inner walk over `lock->procLocks`: returns the
    /// holder / waiter PID strings and the holder count.
    pub fn get_lock_holders_and_waiters(
        lock: types_storage::lock::LOCKTAG,
    ) -> LockHoldersAndWaiters
);

seam_core::seam!(
    /// `DescribeLockTag(buf, tag)` — the human-readable description of a
    /// LOCKTAG (e.g. `relation 1234 of database 5`), as a string.
    pub fn describe_lock_tag(
        tag: types_storage::lock::LOCKTAG,
    ) -> alloc::string::String
);
