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

    /// Return with the lock held (the C default for the `pg_advisory_lock*`
    /// acquisitions: `(void) LockAcquire(...)` and return — the lock stays
    /// held until xact end or an explicit `pg_advisory_unlock*` /
    /// `pg_advisory_unlock_all`). Disarms the `Drop` release. Interim shape
    /// until a `TxnResources` owner exists to move the guard into.
    pub fn keep_held(mut self) {
        self.held = false;
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
    /// `LockReleaseSession(lockmethodid)` (lock.c) — release all session locks
    /// of the given lock method (used by `pg_advisory_unlock_all` with
    /// `USER_LOCKMETHOD`). The C is infallible (no `ereport`).
    pub fn lock_release_session(lockmethodid: types_core::primitive::uint8)
);

seam_core::seam!(
    /// `GetLockmodeName(lockmethodid, mode)` (lock.c) — the static lock-mode
    /// name string for the `pg_locks.mode` column. Infallible (constant table
    /// entry). The string is copied into `mcx`.
    pub fn get_lockmode_name<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        lockmethodid: types_core::primitive::uint8,
        mode: types_storage::LOCKMODE,
    ) -> mcx::PgString<'mcx>
);

seam_core::seam!(
    /// `GetLockStatusData()` (lock.c) — a snapshot of every PROCLOCK as
    /// `LockInstanceData`, for `pg_lock_status`. The C returns a `LockData`
    /// (count + palloc'd array); the seam folds the count into the vector
    /// length and allocates it in `mcx` (the SRF multi-call context). `Err`
    /// carries OOM.
    pub fn get_lock_status_data<'mcx>(
        mcx: mcx::Mcx<'mcx>,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, types_storage::lock::LockInstanceData>>
);

seam_core::seam!(
    /// `pg_blocking_pids(blocked_pid)` lock-table traversal (lockfuncs.c +
    /// lock.c): the leader PIDs blocking `blocked_pid`, computed from
    /// `GetBlockerStatusData` plus the lock-method conflict tables — all
    /// lock.c-internal state. The seam yields the PID list (duplicates kept,
    /// as in C) in `mcx`; the caller wraps it into the int4[] result. `Err`
    /// carries OOM.
    pub fn blocking_pids<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        blocked_pid: i32,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, i32>>
);

seam_core::seam!(
    /// `GetSafeSnapshotBlockingPids(blocked_pid, output, output_size)`
    /// (predicate.c, surfaced through lock.c's view function): the PIDs whose
    /// transactions block `blocked_pid` from getting a safe snapshot. The seam
    /// yields the list in `mcx`; the caller wraps it into the int4[] result.
    pub fn safe_snapshot_blocking_pids<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        blocked_pid: i32,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, i32>>
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
