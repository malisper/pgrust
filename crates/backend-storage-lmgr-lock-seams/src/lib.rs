//! Seam declarations for the `backend-storage-lmgr-lock` unit
//! (`storage/lmgr/lock.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

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
    /// `LockAcquireExtended(locktag, lockmode, sessionLock, dontWait,
    /// reportMemoryError, &locallock, logLockFailure)` (lock.c). lmgr.c always
    /// passes `reportMemoryError = true`, so OOM is modeled by the `Err` leg
    /// rather than a parameter; the `LOCALLOCK*` out-parameter is internal to
    /// lock.c and re-keyed by [`mark_lock_clear`] on the (tag, mode) pair.
    /// `Err` carries the C `ereport(ERROR)` surface (deadlock detected, out of
    /// shared memory, unrecognized lock mode).
    pub fn lock_acquire_extended(
        locktag: &types_storage::lock::LOCKTAG,
        lockmode: types_storage::lock::LOCKMODE,
        session_lock: bool,
        dont_wait: bool,
        log_lock_failure: bool,
    ) -> types_error::PgResult<types_storage::lock::LockAcquireResult>
);

seam_core::seam!(
    /// `MarkLockClear(locallock)` (lock.c) — mark that the just-acquired lock
    /// has finished absorbing invalidation messages. Re-keyed on the
    /// (tag, mode) pair identifying the locallock the preceding
    /// `lock_acquire_extended` returned. Infallible.
    pub fn mark_lock_clear(
        locktag: &types_storage::lock::LOCKTAG,
        lockmode: types_storage::lock::LOCKMODE,
    )
);

seam_core::seam!(
    /// `LockHeldByMe(locktag, lockmode, orstronger)` (lock.c) — whether the
    /// current transaction holds `lockmode` (or, with `orstronger`, a
    /// numerically-higher mode) on `locktag`. Pure local lock-table lookup,
    /// infallible.
    pub fn lock_held_by_me(
        locktag: &types_storage::lock::LOCKTAG,
        lockmode: types_storage::lock::LOCKMODE,
        orstronger: bool,
    ) -> bool
);

seam_core::seam!(
    /// `LockHasWaiters(locktag, lockmode, sessionLock)` (lock.c) — whether
    /// anyone is waiting on a lock we hold. `Err` carries the C
    /// `elog(ERROR, "unrecognized lock mode")` / lock-table corruption surface.
    pub fn lock_has_waiters(
        locktag: &types_storage::lock::LOCKTAG,
        lockmode: types_storage::lock::LOCKMODE,
        session_lock: bool,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `LockWaiterCount(locktag)` (lock.c) — number of processes waiting for
    /// `locktag`. `Err` carries the C lock-table corruption `elog(ERROR)`
    /// surface.
    pub fn lock_waiter_count(
        locktag: &types_storage::lock::LOCKTAG,
    ) -> types_error::PgResult<i32>
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
