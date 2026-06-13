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
